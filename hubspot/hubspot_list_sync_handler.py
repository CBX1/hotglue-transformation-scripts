#!/usr/bin/env python
# coding: utf-8

"""
HubSpot list -> CBX1 segment membership sync.

Membership is mirrored by diffing against a snapshot, so this module is the only place that
holds both the fresh fetch and the previous member set. Every changes record states the set
it was computed against and the set that results, so the backend can refuse anything lost,
duplicated or reordered instead of applying it on the wrong base. Every inventory entry states
the snapshot's fingerprint, which is how the backend knows a list is in sync.
"""

import hashlib
import json
import logging
import os
import uuid
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
MEMBER_BATCH_SIZE = 5000

# cbx1-prefixed: lives in a namespace shared with tap-owned keys.
FULL_RESYNC_LIST_IDS_KEY = "cbx1_full_resync_list_ids"

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
    ``{"members": set, "change_signals": dict or None}``, or None when never delivered.

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

    if isinstance(stored, dict):
        return {
            "members": {normalise_member_id(m) for m in stored.get("members", [])},
            "change_signals": stored.get("change_signals") or None,
        }
    logger.warning("Unrecognised member snapshot for list %s; treating as absent", list_id)
    return None


def write_list_snapshot(
    snapshot_dir: str,
    flow_id: str,
    list_id: str,
    member_ids: Iterable[str],
    change_signals: Optional[Dict] = None,
) -> None:
    """
    Overwrites, never merges — which is why this does not use gluestick's snapshot_records.
    Under a merge a removed member stays in the file forever and no removal is ever computable.
    """
    os.makedirs(snapshot_dir, exist_ok=True)
    payload = {
        "members": sorted({normalise_member_id(member_id) for member_id in member_ids}),
        "change_signals": change_signals,
    }
    with open(member_snapshot_path(snapshot_dir, flow_id, list_id), "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


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
    """Where override_source_config lands (verified on dev job tx06nqlfrkmfdjwg21t8j)."""
    config = _load_json(os.path.join(root_dir, "source-config.json"))
    return config if isinstance(config, dict) else {}


def read_full_resync_list_ids(root_dir: str, snapshot_dir: str) -> Set[str]:
    """Set by the backend on a chain break; cleared once the forced full delivery lands."""
    for candidate in (
        os.path.join(snapshot_dir, "tenant-config.json"),
        os.path.join(root_dir, "tenant-config.json"),
    ):
        config = _load_json(candidate)
        if isinstance(config, dict) and FULL_RESYNC_LIST_IDS_KEY in config:
            return _string_id_set(config.get(FULL_RESYNC_LIST_IDS_KEY))
    return set()


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


def build_full_delivery_records(list_id: str, member_ids: Iterable[str], job_id: Optional[str]) -> List[Dict]:
    """
    Used when there is no snapshot, or when the backend forced a re-send. The backend applies
    the set only once every numbered batch has arrived, so a partial delivery changes nothing.
    """
    members = sorted({normalise_member_id(member_id) for member_id in member_ids})
    fingerprint = member_set_fingerprint(members)
    run_id = str(uuid.uuid4())

    # An empty set still needs one batch, or there is no delivery for the backend to complete.
    slices = [members[i:i + MEMBER_BATCH_SIZE] for i in range(0, len(members), MEMBER_BATCH_SIZE)] or [[]]
    total_batches = len(slices)

    return [
        _wrap(
            {
                "jobId": job_id,
                "fullSyncRunId": run_id,
                "batchNumber": index,
                "totalBatches": total_batches,
                "fullMemberSetFingerprint": fingerprint,
                "memberIds": member_slice,
            },
            str(list_id),
        )
        for index, member_slice in enumerate(slices, start=1)
    ]


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

        membership_writes: List[Tuple[List[Dict], str]] = []
        snapshot_fingerprints: Dict[str, str] = {}

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
            if snapshot is None or list_id in full_resync_ids:
                reason = "no snapshot" if snapshot is None else "full resync forced"
                logger.info("Full delivery for list %s (%s): %d member(s)", list_id, reason, len(fetched_ids))
                records = build_full_delivery_records(list_id, fetched_ids, job_id)
                stream = MEMBERSHIP_FULL_BATCH_STREAM
            else:
                records = build_change_records(list_id, snapshot["members"], fetched_ids, job_id)
                stream = MEMBERSHIP_CHANGES_STREAM

            membership_writes.append((records, stream))
            write_list_snapshot(
                self.snapshot_dir, self.flow_id, list_id, fetched_ids, current_signals.get(list_id)
            )
            snapshot_fingerprints[list_id] = member_set_fingerprint(fetched_ids)

        for list_id in sorted(set(current_signals) - set(fetched_memberships)):
            snapshot = read_list_snapshot(self.snapshot_dir, self.flow_id, list_id)
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
            if list_id in full_resync_ids:
                logger.info("Full delivery for list %s from snapshot (%d member(s))", list_id, len(members))
                membership_writes.append(
                    (
                        build_full_delivery_records(list_id, members, job_id),
                        MEMBERSHIP_FULL_BATCH_STREAM,
                    )
                )

        self._write_segment_records(
            build_list_inventory_records(
                list_rows, source_config.get("connection_org_id"), unscoped, job_id, snapshot_fingerprints
            ),
            SEGMENTS_STREAM,
        )
        for records, stream in membership_writes:
            self._write_segment_records(records, stream)

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
