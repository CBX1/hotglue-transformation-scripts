#!/usr/bin/env python
# coding: utf-8

"""
HubSpot list -> CBX1 segment membership sync.

Membership is mirrored by diffing against a snapshot, so this module is the only place that
holds both the fresh fetch and the previous member set. Every changes record states the set
it was computed against and the set that results, so the backend can refuse anything lost,
duplicated or reordered instead of applying it on the wrong base. Every inventory entry states
the snapshot's fingerprint, which is how the backend knows a list is in sync.

The snapshot also keeps what the backend may still hold: the set it last confirmed and every
member sent since. A full delivery removes whoever of those is gone, so members who left while
records were lost never linger.
"""

import hashlib
import json
import logging
import os
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

from base_handler import BaseETLHandler
from utils import append_singer_records, iter_stream_chunks, prepare_for_singer

logger = logging.getLogger(__name__)

# Must match STREAM_OBJECT_TYPES / STREAM_LOOKUP_FIELDS in cbx1-target-hotglue; a name the
# target does not know raises "Unsupported stream type" at export.
SEGMENTS_STREAM = "segments"
MEMBERSHIP_CHANGES_STREAM = "segment_membership_changes"
MEMBERSHIP_FULL_BATCH_STREAM = "segment_membership_full_batch"

# The v2 HubSpot connector sets use_legacy_streams false, so lists_v3/list_membership_v3
# arrive renamed.
LISTS_INPUT_STREAM = "lists"
LIST_MEMBERSHIP_INPUT_STREAM = "list_membership"

# Other object types have no member entity on our side.
SUPPORTED_OBJECT_TYPE_IDS = frozenset({"0-1", "0-2"})

# ADHOC lists pass the object-type filter but their membership endpoint rejects with
# INVALID_PROCESSING_TYPE, so they must never become importable.
SUPPORTED_PROCESSING_TYPES = frozenset({"MANUAL", "DYNAMIC", "SNAPSHOT"})

# Must mirror the tap's own fingerprint — observed as {added, removed, updated, size} — because
# drift either way is a bug: less sensitive and we vouch for a snapshot a dropped fetch left
# stale, more sensitive and we read a legitimate skip as a dropped fetch and fail a Resync.
# (updatedAt tracks the list object, not its membership; it is here for that alignment only.)
CHANGE_SIGNAL_PROPERTIES = ("hs_last_record_added_at", "hs_last_record_removed_at", "hs_list_size", "updatedAt")

LIST_BATCH_SIZE = 100
# Small enough that the backend applies one record in a single short transaction.
MEMBER_BATCH_SIZE = 100

# cbx1-prefixed: lives in a namespace shared with tap-owned keys.
FULL_RESYNC_LIST_IDS_KEY = "cbx1_full_resync_list_ids"
CONFIRMED_FINGERPRINTS_KEY = "cbx1_confirmed_fingerprints"
MAX_MEMBERS_PER_LIST_KEY = "cbx1_max_members_per_list"

# Must equal the backend's default, so a tenant whose key was never written still has a limit.
DEFAULT_MAX_MEMBERS_PER_LIST = 10_000

# Not a documented contract, so a HotGlue change here would surface as clicked jobs looking unscoped.
TAP_RUN_CONFIG_PATH = "/tmp/hotglue/config.json"

FINGERPRINT_BYTES = 16
EMPTY_MEMBER_SET_FINGERPRINT = "0" * (FINGERPRINT_BYTES * 2)


def normalise_member_id(value: object) -> str:
    return str(value).strip()


def _member_hash(member_id: str) -> bytes:
    return hashlib.sha256(normalise_member_id(member_id).encode("utf-8")).digest()[:FINGERPRINT_BYTES]


def member_set_fingerprint(member_ids: Iterable[str]) -> str:
    """
    The only implementation: the backend stores and compares these as opaque strings.

    Contract: de-duplicate the ids, trim each, encode UTF-8, SHA-256 each, keep the first
    16 bytes, XOR them together, write as 32 lowercase hex characters. The empty set is 32
    zeros. De-duplicated because XOR is self-inverse: a repeated id would cancel itself out.
    """
    accumulator = bytearray(FINGERPRINT_BYTES)
    for member_id in {normalise_member_id(member_id) for member_id in member_ids}:
        digest = _member_hash(member_id)
        for index in range(FINGERPRINT_BYTES):
            accumulator[index] ^= digest[index]
    return accumulator.hex()


def _advance_fingerprint(fingerprint_hex: str, member_ids: Iterable[str]) -> str:
    """
    XOR folds in both directions, so this adds and removes alike. De-duplicated because
    folding one id twice cancels it and lands the chain on a wrong value, silently.
    """
    accumulator = bytearray(bytes.fromhex(fingerprint_hex))
    for member_id in {normalise_member_id(member_id) for member_id in member_ids}:
        digest = _member_hash(member_id)
        for index in range(FINGERPRINT_BYTES):
            accumulator[index] ^= digest[index]
    return accumulator.hex()


def member_snapshot_path(snapshot_dir: str, flow_id: str, list_id: str) -> str:
    return os.path.join(snapshot_dir, f"list_members_{list_id}_{flow_id}.json")


def read_list_snapshot(snapshot_dir: str, flow_id: str, list_id: str) -> Optional[Dict]:
    """
    ``{"members", "change_signals", "confirmed_members", "sent_since_confirmed",
    "is_membership_paused"}``, or None when never delivered. ``confirmed_members`` is None when the
    backend has confirmed nothing yet.

    None and an empty member set must not be conflated: None means "no baseline, send
    everything", empty means "last seen with no members".
    """
    path = member_snapshot_path(snapshot_dir, flow_id, list_id)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            stored = json.load(handle)
    except (OSError, ValueError):
        # Absent forces a full delivery, which re-establishes the baseline. Empty would look
        # like every member was removed.
        logger.warning("Unreadable member snapshot for list %s; treating as absent", list_id)
        return None

    if not isinstance(stored, dict):
        logger.warning("Unrecognised member snapshot for list %s; treating as absent", list_id)
        return None

    members = {normalise_member_id(m) for m in stored.get("members", [])}
    confirmed_fingerprint = stored.get("confirmed_fingerprint")
    if confirmed_fingerprint is None:
        confirmed_members = None
    elif "confirmed_members" in stored:
        confirmed_members = {normalise_member_id(m) for m in stored["confirmed_members"]}
    else:
        # Stored separately only when it differs, so absence means the latest set is confirmed.
        confirmed_members = set(members)
    sent_since_confirmed = {normalise_member_id(m) for m in stored.get("sent_since_confirmed", [])}
    is_membership_paused = bool(stored.get("is_membership_paused"))
    # A paused list's members were fetched but never sent, so they are not counted as sent.
    if confirmed_members is None and not is_membership_paused:
        # A snapshot written before histories existed recorded only its latest set, and every one
        # of those members was sent; without this, one lost removal would leave them held for good.
        sent_since_confirmed |= members
    return {
        "members": members,
        "change_signals": stored.get("change_signals") or None,
        "confirmed_members": confirmed_members,
        "sent_since_confirmed": sent_since_confirmed,
        "is_membership_paused": is_membership_paused,
    }


def write_list_snapshot(
    snapshot_dir: str,
    flow_id: str,
    list_id: str,
    member_ids: Iterable[str],
    change_signals: Optional[Dict] = None,
    confirmed_members: Optional[Iterable[str]] = None,
    sent_since_confirmed: Iterable[str] = (),
    is_membership_paused: bool = False,
) -> None:
    """
    Overwrites, never merges — which is why this does not use gluestick's snapshot_records.
    Under a merge a removed member stays in the file forever and no removal is ever computable.

    HotGlue re-uploads every snapshot with every job, so the confirmed set and the members sent
    since are written only when they add something beyond the latest set.
    """
    os.makedirs(snapshot_dir, exist_ok=True)
    members = sorted({normalise_member_id(member_id) for member_id in member_ids})
    payload: Dict[str, object] = {"members": members, "change_signals": change_signals}
    if confirmed_members is not None:
        confirmed = sorted({normalise_member_id(member_id) for member_id in confirmed_members})
        payload["confirmed_fingerprint"] = member_set_fingerprint(confirmed)
        if confirmed != members:
            payload["confirmed_members"] = confirmed
    sent = sorted({normalise_member_id(member_id) for member_id in sent_since_confirmed})
    if sent:
        payload["sent_since_confirmed"] = sent
    if is_membership_paused:
        payload["is_membership_paused"] = True
    with open(member_snapshot_path(snapshot_dir, flow_id, list_id), "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def apply_confirmation(snapshot: Dict, confirmed_fingerprint: Optional[str]) -> bool:
    """
    The backend confirms a set only when it holds exactly that set, so once the latest set is
    confirmed nothing older can still be held and the history is dropped. A confirmation naming
    anything else leaves the history alone: keeping more is only a longer removal list.
    """
    # A paused list's latest set was never sent, so no confirmation can be of it.
    if snapshot.get("is_membership_paused"):
        return False
    if confirmed_fingerprint is None or confirmed_fingerprint != member_set_fingerprint(snapshot["members"]):
        return False
    if snapshot["confirmed_members"] == snapshot["members"] and not snapshot["sent_since_confirmed"]:
        return False
    snapshot["confirmed_members"] = set(snapshot["members"])
    snapshot["sent_since_confirmed"] = set()
    return True


def members_possibly_held(snapshot: Optional[Dict]) -> Set[str]:
    """
    Only the transform's records add members to a mirror, so the backend can hold nothing
    outside the set it last confirmed and the members sent since. The latest set is included
    because every snapshot was sent when it was taken, including one written before the history
    was kept; a paused list's was not.
    """
    if snapshot is None:
        return set()
    possibly_held = set(snapshot["confirmed_members"] or set()) | snapshot["sent_since_confirmed"]
    if not snapshot.get("is_membership_paused"):
        possibly_held |= snapshot["members"]
    return possibly_held


def _load_json(path: str) -> Optional[object]:
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        logger.warning("Could not read %s", path)
        return None


def _string_id_set(value: object) -> Set[str]:
    if not isinstance(value, (list, tuple, set)):
        return set()
    return {str(item) for item in value if item is not None}


def read_source_config(root_dir: str) -> Dict:
    """
    A job's ``override_source_config`` reaches only the config the tap runs with; the container's
    ``ROOT_DIR/source-config.json`` holds the stored config (dev job sp7a12miqf3o5vf69g5xz). Read
    from there, an Import or Resync looks unscoped and its inventory would mark every list it did
    not fetch as deleted. A local run has no tap config, and its downloaded ``source-config.json``
    already carries the override.
    """
    for path in (TAP_RUN_CONFIG_PATH, os.path.join(root_dir, "source-config.json")):
        config = _load_json(path)
        if isinstance(config, dict):
            return config
    return {}


def _read_tenant_config_key(root_dir: str, snapshot_dir: str, key: str) -> Optional[object]:
    for candidate in (
        os.path.join(snapshot_dir, "tenant-config.json"),
        os.path.join(root_dir, "tenant-config.json"),
    ):
        config = _load_json(candidate)
        if isinstance(config, dict) and key in config:
            return config.get(key)
    return None


def read_full_resync_list_ids(root_dir: str, snapshot_dir: str) -> Set[str]:
    return _string_id_set(_read_tenant_config_key(root_dir, snapshot_dir, FULL_RESYNC_LIST_IDS_KEY))


def read_confirmed_fingerprints(root_dir: str, snapshot_dir: str) -> Dict[str, str]:
    confirmed = _read_tenant_config_key(root_dir, snapshot_dir, CONFIRMED_FINGERPRINTS_KEY)
    if not isinstance(confirmed, dict):
        return {}
    return {str(list_id): str(fingerprint) for list_id, fingerprint in confirmed.items() if fingerprint}


def read_max_members_per_list(root_dir: str, snapshot_dir: str) -> int:
    value = _as_optional_int(_read_tenant_config_key(root_dir, snapshot_dir, MAX_MEMBERS_PER_LIST_KEY))
    return value if value is not None and value > 0 else DEFAULT_MAX_MEMBERS_PER_LIST


def _wrap(data: Dict, lookup_key: str) -> Dict:
    """The target drops records with a null lookupKey, so every record must carry one."""
    return {
        "data": data,
        "sourceRecordId": lookup_key,
        "source": "HUBSPOT",
        "lookupKey": lookup_key,
    }


def is_supported_list(list_row: Dict) -> bool:
    return (
        str(list_row.get("objectTypeId")) in SUPPORTED_OBJECT_TYPE_IDS
        and str(list_row.get("processingType")).upper() in SUPPORTED_PROCESSING_TYPES
    )


def build_list_inventory_records(
    list_rows: Sequence[Dict],
    source_account_id: Optional[str],
    unscoped: bool,
    job_id: Optional[str],
    snapshot_fingerprints: Dict[str, str],
    over_limit_list_ids: Set[str] = frozenset(),
) -> List[Dict]:
    """
    ``allListIds`` rides the final batch of an unscoped run only. The backend reads absence
    from it as "deleted at the source", so a scoped run making that claim would delete every
    list it did not examine.
    """
    supported = [row for row in list_rows if is_supported_list(row)]
    skipped = len(list_rows) - len(supported)
    if skipped:
        logger.info("Skipping %d list(s): unsupported object type or processing type", skipped)

    if not supported:
        return []

    entries = []
    for row in supported:
        list_id = str(row.get("listId"))
        entry = {
            "sourceSegmentId": list_id,
            "name": row.get("name"),
            "objectTypeId": str(row.get("objectTypeId")),
            "sourceListType": row.get("processingType"),
            "sourceMemberCount": reported_member_count(row),
        }
        if list_id in snapshot_fingerprints:
            entry["snapshotMemberSetFingerprint"] = snapshot_fingerprints[list_id]
        if list_id in over_limit_list_ids:
            entry["isOverMemberLimit"] = True
        entries.append(entry)

    batches = [entries[i:i + LIST_BATCH_SIZE] for i in range(0, len(entries), LIST_BATCH_SIZE)]
    total_batches = len(batches)
    account_id = str(source_account_id) if source_account_id is not None else None

    records = []
    for index, batch in enumerate(batches, start=1):
        payload = {
            "jobId": job_id,
            "sourceAccountId": account_id,
            "batchNumber": index,
            "totalBatches": total_batches,
            "lists": batch,
        }
        if unscoped and index == total_batches:
            payload["allListIds"] = [entry["sourceSegmentId"] for entry in entries]
        records.append(_wrap(payload, f"{account_id or 'hubspot'}:lists:{index}"))
    return records


def _as_optional_int(value: object) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _additional_properties(list_row: Dict) -> Dict:
    """Arrives either parsed or as a JSON string."""
    additional = list_row.get("additionalProperties")
    if isinstance(additional, str):
        try:
            additional = json.loads(additional)
        except ValueError:
            additional = None
    return additional if isinstance(additional, dict) else {}


def reported_member_count(list_row: Dict) -> Optional[int]:
    """
    Feeds the empty-set guard: a fetch that came back empty for a list HubSpot says is
    populated is a failed fetch, never a mass removal.
    """
    additional = _additional_properties(list_row)
    if additional.get("hs_list_size") is not None:
        return _as_optional_int(additional.get("hs_list_size"))
    return _as_optional_int(list_row.get("hs_list_size"))


def change_signals(list_row: Dict) -> Dict[str, Optional[str]]:
    """
    Compared against the signals stored with our snapshot to tell "the tap skipped this list"
    from "the tap tried and the fetch was dropped". Both reach us as silence.
    """
    additional = _additional_properties(list_row)
    return {
        prop: _change_signal_value(additional.get(prop, list_row.get(prop)))
        for prop in CHANGE_SIGNAL_PROPERTIES
    }


def _change_signal_value(value: object) -> Optional[str]:
    if value is None:
        return None
    # Screened before isoformat: pandas' NaT has one, and it returns the string "NaT".
    text = str(value)
    if text in ("NaT", "nan", "None", ""):
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except (TypeError, ValueError):
            return text
    return text


def build_full_delivery_records(
    list_id: str,
    member_ids: Iterable[str],
    job_id: Optional[str],
    possibly_held_ids: Iterable[str] = (),
) -> List[Dict]:
    """
    Used when there is no snapshot, or when the backend asked for the whole set. The backend
    completes the delivery only once every numbered batch has arrived, so a partial delivery
    changes nothing it shows as in sync.

    Removals ride the same batches: whoever the backend may hold but is not in the set. The
    delivery is identified by its fingerprint and batch count, both fixed for a given set, so a
    delivery a lost request left unfinished is resumed by the next run instead of re-applied.
    """
    members = sorted({normalise_member_id(member_id) for member_id in member_ids})
    removals = sorted({normalise_member_id(member_id) for member_id in possibly_held_ids} - set(members))
    fingerprint = member_set_fingerprint(members)

    member_slices = [members[i:i + MEMBER_BATCH_SIZE] for i in range(0, len(members), MEMBER_BATCH_SIZE)]
    removal_slices = [removals[i:i + MEMBER_BATCH_SIZE] for i in range(0, len(removals), MEMBER_BATCH_SIZE)]
    # An empty set still needs one batch, or there is no delivery for the backend to complete.
    if not member_slices and not removal_slices:
        member_slices = [[]]
    total_batches = len(member_slices) + len(removal_slices)

    records = []
    for index, member_slice in enumerate(member_slices, start=1):
        records.append(_full_batch(list_id, job_id, index, total_batches, fingerprint, "memberIds", member_slice))
    for offset, removal_slice in enumerate(removal_slices, start=len(member_slices) + 1):
        records.append(
            _full_batch(list_id, job_id, offset, total_batches, fingerprint, "removedMemberIds", removal_slice)
        )
    return records


def _full_batch(
    list_id: str,
    job_id: Optional[str],
    batch_number: int,
    total_batches: int,
    fingerprint: str,
    ids_field: str,
    ids: List[str],
) -> Dict:
    return _wrap(
        {
            "jobId": job_id,
            "batchNumber": batch_number,
            "totalBatches": total_batches,
            "fullMemberSetFingerprint": fingerprint,
            ids_field: ids,
        },
        str(list_id),
    )


def build_change_records(
    list_id: str,
    previous_member_ids: Iterable[str],
    current_member_ids: Iterable[str],
    job_id: Optional[str],
) -> List[Dict]:
    """
    Each slice states the fingerprint it applies to and the one that results, so the backend
    can refuse anything arriving out of order or twice. The last slice lands on the current set.
    """
    previous = {normalise_member_id(member_id) for member_id in previous_member_ids}
    current = {normalise_member_id(member_id) for member_id in current_member_ids}

    added = sorted(current - previous)
    removed = sorted(previous - current)

    running_fingerprint = member_set_fingerprint(previous)
    records: List[Dict] = []
    for added_slice, removed_slice in _slice_changes(added, removed):
        base_fingerprint = running_fingerprint
        running_fingerprint = _advance_fingerprint(running_fingerprint, added_slice + removed_slice)
        records.append(
            _wrap(
                {
                    "jobId": job_id,
                    "baseMemberSetFingerprint": base_fingerprint,
                    "newMemberSetFingerprint": running_fingerprint,
                    "addedMemberIds": added_slice,
                    "removedMemberIds": removed_slice,
                },
                str(list_id),
            )
        )

    return records


def build_membership_records(
    list_id: str,
    current_member_ids: Iterable[str],
    snapshot: Optional[Dict],
    is_full_resync_forced: bool,
    job_id: Optional[str],
) -> Tuple[List[Dict], str, Set[str]]:
    """
    A paused list's snapshot follows the fetch, not what the backend holds. The backend still
    holds the set it last confirmed, so on return the changes are computed against that; with
    members sent since, it may hold any of them, so the list is sent whole.
    """
    current = {normalise_member_id(member_id) for member_id in current_member_ids}
    is_confirmed_set_held = (
        snapshot is not None
        and snapshot["confirmed_members"] is not None
        and not snapshot["sent_since_confirmed"]
    )
    if (
        snapshot is None
        or is_full_resync_forced
        or (snapshot["is_membership_paused"] and not is_confirmed_set_held)
    ):
        records = build_full_delivery_records(list_id, current, job_id, members_possibly_held(snapshot))
        return records, MEMBERSHIP_FULL_BATCH_STREAM, current
    held = snapshot["confirmed_members"] if snapshot["is_membership_paused"] else snapshot["members"]
    return build_change_records(list_id, held, current, job_id), MEMBERSHIP_CHANGES_STREAM, current - held


def _slice_changes(added: List[str], removed: List[str]):
    """Adds and removes are counted together so one record can never exceed the cap."""
    added_index = 0
    removed_index = 0
    while added_index < len(added) or removed_index < len(removed):
        added_slice = added[added_index:added_index + MEMBER_BATCH_SIZE]
        added_index += len(added_slice)

        remaining = MEMBER_BATCH_SIZE - len(added_slice)
        removed_slice = removed[removed_index:removed_index + remaining] if remaining else []
        removed_index += len(removed_slice)

        yield added_slice, removed_slice



class HubSpotListSyncHandler(BaseETLHandler):
    def __init__(self, *args, target_config: Optional[Dict] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_config = target_config or {}
        self.connector_id = "hubspot"
        self._segment_streams_started: Set[str] = set()

    def handle_write(self) -> None:
        raise NotImplementedError("The HubSpot list sync flow is read-only")

    def handle_read(self) -> None:
        data_streams = self.list_available_streams()
        if not data_streams:
            logger.warning("No streams available for read operation")
            return
        self._emit_segment_sync(data_streams)

    def _emit_segment_sync(self, data_streams: List[str]) -> None:
        root_dir = os.environ.get("ROOT_DIR", ".")
        job_id = os.environ.get("JOB_ID")
        source_config = read_source_config(root_dir)
        full_resync_ids = read_full_resync_list_ids(root_dir, self.snapshot_dir)
        confirmed_fingerprints = read_confirmed_fingerprints(root_dir, self.snapshot_dir)
        # A list outside this job's scope was never fetched by it, so stating its fingerprint would
        # let the backend hand the list to a job that will send nothing for it.
        scope_ids = _string_id_set(source_config.get("membership_list_ids"))

        # Completeness is governed by the lists scope, not membership: membership_list_ids is
        # re-stated on every import, so reading it here would make every run look scoped and no
        # deleted list would ever be swept.
        #
        # RULE: scope the lists stream only via `list_ids`. selectedFilters scoping is honoured
        # by the tap but invisible here, so it would report a partial inventory as complete.
        unscoped = not source_config.get("list_ids")

        list_rows: List[Dict] = []
        if LISTS_INPUT_STREAM in data_streams:
            lists_df = self.get_stream_data(LISTS_INPUT_STREAM)
            list_rows = [] if lists_df is None or lists_df.empty else lists_df.to_dict(orient="records")
        reported_counts = {str(row.get("listId")): reported_member_count(row) for row in list_rows}
        current_signals = {str(row.get("listId")): change_signals(row) for row in list_rows}

        max_members = read_max_members_per_list(root_dir, self.snapshot_dir)

        membership_writes: List[Tuple[List[Dict], str]] = []
        snapshot_fingerprints: Dict[str, str] = {}
        over_limit_ids: Set[str] = set()

        fetched_memberships = self._collect_list_memberships()
        for list_id, fetched_ids in sorted(fetched_memberships.items()):
            reported = reported_counts.get(list_id)
            if not fetched_ids and reported:
                # HubSpot claiming members while returning none is a dropped fetch, never a
                # mass removal.
                logger.warning(
                    "Skipping list %s: fetch returned no members but HubSpot reports %s", list_id, reported
                )
                continue

            snapshot = read_list_snapshot(self.snapshot_dir, self.flow_id, list_id)
            if snapshot is not None:
                apply_confirmation(snapshot, confirmed_fingerprints.get(list_id))
            if len(fetched_ids) > max_members:
                logger.info(
                    "Pausing list %s: %d member(s), over the limit of %d", list_id, len(fetched_ids), max_members
                )
                self._write_paused_snapshot(list_id, fetched_ids, current_signals.get(list_id), snapshot)
                over_limit_ids.add(list_id)
                continue

            records, stream, sent_as_present = build_membership_records(
                list_id, fetched_ids, snapshot, list_id in full_resync_ids, job_id
            )
            membership_writes.append((records, stream))
            write_list_snapshot(
                self.snapshot_dir,
                self.flow_id,
                list_id,
                fetched_ids,
                current_signals.get(list_id),
                confirmed_members=None if snapshot is None else snapshot["confirmed_members"],
                sent_since_confirmed=(set() if snapshot is None else snapshot["sent_since_confirmed"])
                | sent_as_present,
            )
            snapshot_fingerprints[list_id] = member_set_fingerprint(fetched_ids)

        for list_id in sorted(set(current_signals) - set(fetched_memberships)):
            snapshot = read_list_snapshot(self.snapshot_dir, self.flow_id, list_id)
            if snapshot is not None and apply_confirmation(snapshot, confirmed_fingerprints.get(list_id)):
                write_list_snapshot(
                    self.snapshot_dir,
                    self.flow_id,
                    list_id,
                    snapshot["members"],
                    snapshot["change_signals"],
                    confirmed_members=snapshot["confirmed_members"],
                    sent_since_confirmed=snapshot["sent_since_confirmed"],
                )
            if list_id not in scope_ids:
                continue
            reported = reported_counts.get(list_id)
            # A paused snapshot holds the set that crossed the limit, so HubSpot's count dipping
            # under it is not enough to resume.
            is_paused_set_over_limit = (
                snapshot is not None and snapshot["is_membership_paused"] and len(snapshot["members"]) > max_members
            )
            if (reported is not None and reported > max_members) or is_paused_set_over_limit:
                over_limit_ids.add(list_id)
                continue
            if snapshot is None:
                continue
            if snapshot.get("change_signals") is None or snapshot["change_signals"] != current_signals[list_id]:
                logger.warning(
                    "Not stating a snapshot fingerprint for list %s: its change signals moved but "
                    "no membership arrived, so the fetch was attempted and dropped",
                    list_id,
                )
                continue

            members = snapshot["members"]
            snapshot_fingerprints[list_id] = member_set_fingerprint(members)
            if snapshot["is_membership_paused"]:
                logger.info("Resuming list %s from its snapshot (%d member(s))", list_id, len(members))
                records, stream, sent_as_present = build_membership_records(
                    list_id, members, snapshot, list_id in full_resync_ids, job_id
                )
                membership_writes.append((records, stream))
                write_list_snapshot(
                    self.snapshot_dir,
                    self.flow_id,
                    list_id,
                    members,
                    snapshot["change_signals"],
                    confirmed_members=snapshot["confirmed_members"],
                    sent_since_confirmed=snapshot["sent_since_confirmed"] | sent_as_present,
                )
            elif list_id in full_resync_ids:
                logger.info("Full delivery for list %s from snapshot (%d member(s))", list_id, len(members))
                # Every member of the snapshot was sent when it was taken, so the history
                # already covers them and needs no update.
                membership_writes.append(
                    (
                        build_full_delivery_records(list_id, members, job_id, members_possibly_held(snapshot)),
                        MEMBERSHIP_FULL_BATCH_STREAM,
                    )
                )

        self._write_segment_records(
            build_list_inventory_records(
                list_rows,
                source_config.get("connection_org_id"),
                unscoped,
                job_id,
                snapshot_fingerprints,
                over_limit_ids,
            ),
            SEGMENTS_STREAM,
        )
        for records, stream in membership_writes:
            self._write_segment_records(records, stream)

    def _write_paused_snapshot(
        self, list_id: str, fetched_ids: Set[str], signals: Optional[Dict], snapshot: Optional[Dict]
    ) -> None:
        """
        Follows the fetch, so the tap and the snapshot agree on what was last seen and the list can
        resume without another fetch. A snapshot that was not paused was sent, so its members stay
        in the history of what the backend may hold.
        """
        confirmed = None if snapshot is None else snapshot["confirmed_members"]
        sent = set() if snapshot is None else set(snapshot["sent_since_confirmed"])
        if snapshot is not None and not snapshot["is_membership_paused"]:
            sent |= snapshot["members"] - (confirmed or set())
        write_list_snapshot(
            self.snapshot_dir,
            self.flow_id,
            list_id,
            fetched_ids,
            signals,
            confirmed_members=confirmed,
            sent_since_confirmed=sent,
            is_membership_paused=True,
        )

    def _collect_list_memberships(self) -> Dict[str, Set[str]]:
        """
        Unioned across rows: the memberships endpoint pages, so one list can span several rows.
        A list present with zero ids is meaningful — the empty-set guard inspects it.
        """
        memberships: Dict[str, Set[str]] = {}
        for chunk_df in iter_stream_chunks(self.input_dir, LIST_MEMBERSHIP_INPUT_STREAM):
            if chunk_df is None or chunk_df.empty or "list_id" not in chunk_df.columns:
                continue
            for row in chunk_df.to_dict(orient="records"):
                list_id = row.get("list_id")
                if list_id is None or (isinstance(list_id, float) and pd.isna(list_id)):
                    continue
                bucket = memberships.setdefault(str(list_id), set())
                bucket.update(self._extract_member_ids(row.get("results")))
        return memberships

    @staticmethod
    def _extract_member_ids(results) -> List[str]:
        """Observed as a JSON string of objects carrying recordId; typed array-or-string."""
        if results is None:
            return []
        if isinstance(results, str):
            try:
                results = json.loads(results)
            except ValueError:
                return []
        if isinstance(results, np.ndarray):
            results = results.tolist()
        if not isinstance(results, (list, tuple)):
            return []

        member_ids = []
        for entry in results:
            if isinstance(entry, dict):
                value = entry.get("recordId", entry.get("id"))
            else:
                value = entry
            if value is not None:
                member_ids.append(normalise_member_id(value))
        return member_ids

    def _write_segment_records(self, records: List[Dict], stream: str) -> None:
        """Each stream's SCHEMA must be written exactly once, across all callers in a run."""
        if not records:
            return
        first_chunk = stream not in self._segment_streams_started
        prepared = prepare_for_singer(pd.DataFrame(records))
        append_singer_records(prepared, stream, self.output_dir, first_chunk)
        self._segment_streams_started.add(stream)
        logger.info("Wrote %d record(s) to stream: %s", len(prepared), stream)
