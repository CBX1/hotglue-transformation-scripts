"""
Tests for HubSpotListSyncHandler and its dispatch.

The handler decides, per list, what to send and whether it can vouch for its snapshot. The
two silences — a list the tap skipped and a list whose fetch was dropped — both arrive as
"no membership rows"; only the first may carry a snapshot fingerprint on the inventory.
"""

import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import etl  # noqa: E402
import hubspot_list_sync_handler as ss  # noqa: E402
from hubspot_handler import HubSpotHandler  # noqa: E402
from hubspot_list_sync_handler import HubSpotListSyncHandler  # noqa: E402


JOB = "job-1"


def _handler(tmp_path):
    """Built without the constructor so the test needs no gluestick Reader or tenant config."""
    handler = HubSpotListSyncHandler.__new__(HubSpotListSyncHandler)
    handler.flow_id = "FLOW1"
    handler.snapshot_dir = str(tmp_path / "snapshots")
    handler.output_dir = str(tmp_path / "out")
    handler.neutralize_container_literals = False
    handler._segment_streams_started = set()
    os.makedirs(handler.snapshot_dir, exist_ok=True)
    os.makedirs(handler.output_dir, exist_ok=True)
    return handler


def _list_row(list_id, size, added_at="100"):
    return {
        "listId": list_id,
        "name": f"List {list_id}",
        "objectTypeId": "0-1",
        "processingType": "DYNAMIC",
        "additionalProperties": {"hs_last_record_added_at": added_at, "hs_list_size": size},
    }


# A scheduled run's scope is every imported list, so by default both test lists are in it.
DEFAULT_SOURCE_CONFIG = {"membership_list_ids": ["77", "88"]}


def _run(
    handler,
    monkeypatch,
    list_rows,
    memberships,
    full_resync_ids=(),
    source_config=None,
    confirmed=None,
    max_members=ss.DEFAULT_MAX_MEMBERS_PER_LIST,
):
    monkeypatch.setenv("JOB_ID", JOB)
    monkeypatch.setattr(ss, "read_max_members_per_list", lambda root, snap: max_members)
    monkeypatch.setattr(handler, "get_stream_data", lambda stream: pd.DataFrame(list_rows), raising=False)
    monkeypatch.setattr(handler, "_collect_list_memberships", lambda: memberships, raising=False)
    monkeypatch.setattr(ss, "read_source_config", lambda root: DEFAULT_SOURCE_CONFIG if source_config is None else source_config)
    monkeypatch.setattr(ss, "read_full_resync_list_ids", lambda root, snap: set(full_resync_ids))
    monkeypatch.setattr(ss, "read_confirmed_fingerprints", lambda root, snap: dict(confirmed or {}))
    handler._emit_segment_sync([ss.LISTS_INPUT_STREAM, ss.LIST_MEMBERSHIP_INPUT_STREAM])


def _emitted(handler):
    """Every record written to the shared Singer output, as (stream, record), in order."""
    path = os.path.join(handler.output_dir, "data.singer")
    if not os.path.isfile(path):
        return []
    records = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            message = json.loads(line)
            if message.get("type") == "RECORD":
                records.append((message["stream"], message["record"]))
    return records


def _membership(handler):
    return [(stream, record) for stream, record in _emitted(handler) if stream != ss.SEGMENTS_STREAM]


def _inventory_entries(handler):
    return {
        entry["sourceSegmentId"]: entry
        for stream, record in _emitted(handler)
        if stream == ss.SEGMENTS_STREAM
        for entry in record["data"]["lists"]
    }


def _write_snapshot(handler, list_id, members, row):
    ss.write_list_snapshot(handler.snapshot_dir, "FLOW1", list_id, members, ss.change_signals(row))


# --- A fetched list ---


def test_a_normal_fetch_delivers_changes_and_states_the_new_snapshot(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    before = _list_row("77", "1")
    _write_snapshot(handler, "77", ["1"], before)

    now = _list_row("77", "2", added_at="200")
    _run(handler, monkeypatch, [now], {"77": {"1", "2"}})

    records = _membership(handler)
    assert [stream for stream, _ in records] == [ss.MEMBERSHIP_CHANGES_STREAM]
    assert records[0][1]["data"]["addedMemberIds"] == ["2"]

    snapshot = ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")
    assert snapshot["members"] == {"1", "2"}
    assert snapshot["change_signals"] == ss.change_signals(now)

    fingerprint = ss.member_set_fingerprint({"1", "2"})
    assert _inventory_entries(handler)["77"]["snapshotMemberSetFingerprint"] == fingerprint
    assert records[-1][1]["data"]["newMemberSetFingerprint"] == fingerprint


def test_a_first_fetch_is_a_full_delivery(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "2")], {"77": {"1", "2"}})

    records = _membership(handler)
    assert [stream for stream, _ in records] == [ss.MEMBERSHIP_FULL_BATCH_STREAM]
    assert sorted(records[0][1]["data"]["memberIds"]) == ["1", "2"]
    assert _inventory_entries(handler)["77"]["snapshotMemberSetFingerprint"] == ss.member_set_fingerprint({"1", "2"})


def test_a_forced_list_that_was_fetched_is_a_full_delivery_from_the_fetch(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _write_snapshot(handler, "77", ["1"], _list_row("77", "1"))

    _run(handler, monkeypatch, [_list_row("77", "2", added_at="200")], {"77": {"1", "2"}}, full_resync_ids={"77"})

    records = _membership(handler)
    assert [stream for stream, _ in records] == [ss.MEMBERSHIP_FULL_BATCH_STREAM]
    assert sorted(records[0][1]["data"]["memberIds"]) == ["1", "2"]


def test_an_unchanged_fetch_sends_no_membership_but_states_the_snapshot(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    row = _list_row("77", "2")
    _write_snapshot(handler, "77", ["1", "2"], row)

    _run(handler, monkeypatch, [row], {"77": {"1", "2"}})

    assert _membership(handler) == []
    assert _inventory_entries(handler)["77"]["snapshotMemberSetFingerprint"] == ss.member_set_fingerprint({"1", "2"})


# --- The zero-member guard: an empty fetch is trusted unless HubSpot reports members ---


def test_an_empty_fetch_is_refused_when_the_source_reports_members(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _write_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))

    _run(handler, monkeypatch, [_list_row("77", "2", added_at="200")], {"77": set()})

    assert _membership(handler) == []
    assert ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")["members"] == {"1", "2"}
    assert "snapshotMemberSetFingerprint" not in _inventory_entries(handler)["77"]


def test_an_empty_fetch_is_accepted_when_the_count_is_unknown(tmp_path, monkeypatch):
    # hs_list_size can be absent, and the likeliest reason is a list with no members.
    # A dropped fetch cannot reach this branch: the tap emits no row at all for it.
    handler = _handler(tmp_path)
    _write_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))

    _run(handler, monkeypatch, [_list_row("77", None, added_at="200")], {"77": set()})

    records = _membership(handler)
    assert records[-1][1]["data"]["newMemberSetFingerprint"] == ss.EMPTY_MEMBER_SET_FINGERPRINT


def test_an_empty_fetch_is_accepted_when_the_source_reports_zero(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _write_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))

    _run(handler, monkeypatch, [_list_row("77", "0", added_at="200")], {"77": set()})

    records = _membership(handler)
    assert records[-1][1]["data"]["newMemberSetFingerprint"] == ss.EMPTY_MEMBER_SET_FINGERPRINT
    assert ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")["members"] == set()
    assert _inventory_entries(handler)["77"]["snapshotMemberSetFingerprint"] == ss.EMPTY_MEMBER_SET_FINGERPRINT


# --- A list with nothing fetched: skipped by the tap, or dropped ---


def test_a_quiet_run_emits_the_inventory_with_the_snapshot_fingerprint(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    row = _list_row("77", "2")
    _write_snapshot(handler, "77", ["1", "2"], row)

    _run(handler, monkeypatch, [row], {})

    assert _membership(handler) == []
    assert _inventory_entries(handler)["77"]["snapshotMemberSetFingerprint"] == ss.member_set_fingerprint({"1", "2"})


def test_an_empty_snapshot_is_still_stated(tmp_path, monkeypatch):
    # A list legitimately emptied earlier is in sync at the empty fingerprint.
    handler = _handler(tmp_path)
    row = _list_row("77", "0")
    _write_snapshot(handler, "77", [], row)

    _run(handler, monkeypatch, [row], {})

    assert _inventory_entries(handler)["77"]["snapshotMemberSetFingerprint"] == ss.EMPTY_MEMBER_SET_FINGERPRINT


def test_a_dropped_fetch_leaves_the_snapshot_unstated(tmp_path, monkeypatch):
    # Change signals moved, so the tap would have fetched — and nothing arrived. Stating the
    # old snapshot would report an outage as "in sync".
    handler = _handler(tmp_path)
    _write_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))

    _run(handler, monkeypatch, [_list_row("77", "3", added_at="200")], {})

    assert _membership(handler) == []
    assert "snapshotMemberSetFingerprint" not in _inventory_entries(handler)["77"]


def test_a_snapshot_without_change_signals_is_unstated(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    ss.write_list_snapshot(handler.snapshot_dir, "FLOW1", "77", ["1"], None)

    _run(handler, monkeypatch, [_list_row("77", "1")], {})

    assert "snapshotMemberSetFingerprint" not in _inventory_entries(handler)["77"]


def test_a_list_never_delivered_has_no_snapshot_fingerprint(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "1")], {})

    assert "snapshotMemberSetFingerprint" not in _inventory_entries(handler)["77"]


# --- Repair from the snapshot (force key, nothing fetched) ---


def test_a_forced_unchanged_list_is_delivered_in_full_from_the_snapshot(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    row = _list_row("77", "2")
    _write_snapshot(handler, "77", ["1", "2"], row)

    _run(handler, monkeypatch, [row], {}, full_resync_ids={"77"})

    records = _membership(handler)
    assert [stream for stream, _ in records] == [ss.MEMBERSHIP_FULL_BATCH_STREAM]
    payload = records[0][1]["data"]
    assert sorted(payload["memberIds"]) == ["1", "2"]
    assert payload["fullMemberSetFingerprint"] == ss.member_set_fingerprint({"1", "2"})
    assert _inventory_entries(handler)["77"]["snapshotMemberSetFingerprint"] == payload["fullMemberSetFingerprint"]


def test_a_forced_list_whose_fetch_was_dropped_is_not_repaired_from_a_stale_snapshot(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _write_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))

    _run(handler, monkeypatch, [_list_row("77", "3", added_at="200")], {}, full_resync_ids={"77"})

    assert _membership(handler) == []


def test_a_forced_list_with_no_snapshot_sends_nothing(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "2")], {}, full_resync_ids={"77"})

    assert _membership(handler) == []


# --- Scope, history and removals ---


def test_a_list_outside_the_jobs_scope_gets_no_fingerprint(tmp_path, monkeypatch):
    # Imported while this job ran, so the job never fetched it: a fingerprint would let the
    # backend hand the list to a job that sends nothing for it.
    handler = _handler(tmp_path)
    row = _list_row("77", "2")
    _write_snapshot(handler, "77", ["1", "2"], row)

    _run(handler, monkeypatch, [row], {}, source_config={"membership_list_ids": ["88"]})

    assert "snapshotMemberSetFingerprint" not in _inventory_entries(handler)["77"]


def test_a_first_delivery_records_every_member_as_sent(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "2")], {"77": {"1", "2"}})

    snapshot = ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")
    assert snapshot["confirmed_members"] is None
    assert snapshot["sent_since_confirmed"] == {"1", "2"}


def test_changes_after_a_confirmation_keep_the_confirmed_set_and_the_adds(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    before = _list_row("77", "3")
    ss.write_list_snapshot(handler.snapshot_dir, "FLOW1", "77", ["1", "2", "3"], ss.change_signals(before), confirmed_members=["1", "2", "3"])

    _run(handler, monkeypatch, [_list_row("77", "4", added_at="200")], {"77": {"1", "2", "3", "4"}})

    snapshot = ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")
    assert snapshot["members"] == {"1", "2", "3", "4"}
    assert snapshot["confirmed_members"] == {"1", "2", "3"}
    assert snapshot["sent_since_confirmed"] == {"4"}


def test_a_repair_removes_everyone_possibly_held_who_left(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    before = _list_row("77", "5")
    ss.write_list_snapshot(
        handler.snapshot_dir, "FLOW1", "77", ["1", "2", "3", "4", "5"], ss.change_signals(before),
        confirmed_members=["1", "2", "3"], sent_since_confirmed=["4", "5"],
    )

    _run(handler, monkeypatch, [_list_row("77", "3", added_at="300")], {"77": {"1", "2", "6"}}, full_resync_ids={"77"})

    payloads = [record["data"] for _, record in _membership(handler)]
    assert [p.get("memberIds") for p in payloads] == [["1", "2", "6"], None]
    assert payloads[1]["removedMemberIds"] == ["3", "4", "5"]


def test_a_confirmation_of_the_latest_set_drops_the_history(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    row = _list_row("77", "2")
    ss.write_list_snapshot(
        handler.snapshot_dir, "FLOW1", "77", ["1", "2"], ss.change_signals(row), confirmed_members=["1"], sent_since_confirmed=["2"]
    )

    _run(handler, monkeypatch, [row], {}, confirmed={"77": ss.member_set_fingerprint({"1", "2"})})

    snapshot = ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")
    assert snapshot["confirmed_members"] == {"1", "2"}
    assert snapshot["sent_since_confirmed"] == set()


# --- Ordering and job id ---


def test_the_inventory_is_written_before_any_membership(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "1"), _list_row("88", "1")], {"77": {"1"}, "88": {"2"}})

    streams = [stream for stream, _ in _emitted(handler)]
    assert streams[0] == ss.SEGMENTS_STREAM
    assert ss.SEGMENTS_STREAM not in streams[1:]


def test_every_record_carries_the_job_id(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _write_snapshot(handler, "88", ["1"], _list_row("88", "1"))
    _run(handler, monkeypatch, [_list_row("77", "1"), _list_row("88", "2", added_at="200")], {"77": {"1"}, "88": {"1", "2"}})

    emitted = _emitted(handler)
    assert {stream for stream, _ in emitted} == {
        ss.SEGMENTS_STREAM,
        ss.MEMBERSHIP_FULL_BATCH_STREAM,
        ss.MEMBERSHIP_CHANGES_STREAM,
    }
    assert all(record["data"]["jobId"] == JOB for _, record in emitted)


def test_a_scoped_run_does_not_claim_every_list(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "1")], {}, source_config={"list_ids": ["77"]})

    assert all("allListIds" not in record["data"] for _, record in _emitted(handler))


# --- Read-only, and the dispatch ---


# --- The member limit ---


def _confirmed_snapshot(handler, list_id, members, row):
    ss.write_list_snapshot(
        handler.snapshot_dir, "FLOW1", list_id, members, ss.change_signals(row), confirmed_members=members
    )


def test_a_list_fetched_over_the_limit_is_paused_and_sends_no_membership(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _confirmed_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))

    now = _list_row("77", "4", added_at="200")
    _run(handler, monkeypatch, [now], {"77": {"1", "2", "3", "4"}}, max_members=3)

    assert _membership(handler) == []
    entry = _inventory_entries(handler)["77"]
    assert entry["isOverMemberLimit"] is True
    assert "snapshotMemberSetFingerprint" not in entry
    snapshot = ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")
    assert snapshot["is_membership_paused"]
    assert snapshot["members"] == {"1", "2", "3", "4"}
    assert snapshot["confirmed_members"] == {"1", "2"}
    assert snapshot["sent_since_confirmed"] == set()


def test_a_list_under_the_limit_carries_no_limit_flag(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "2")], {"77": {"1", "2"}}, max_members=3)

    assert "isOverMemberLimit" not in _inventory_entries(handler)["77"]


def test_a_list_not_fetched_but_reported_over_the_limit_is_flagged_and_unstated(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    row = _list_row("77", "5")
    _confirmed_snapshot(handler, "77", ["1", "2"], row)

    _run(handler, monkeypatch, [row], {}, max_members=3)

    entry = _inventory_entries(handler)["77"]
    assert entry["isOverMemberLimit"] is True
    assert "snapshotMemberSetFingerprint" not in entry
    assert _membership(handler) == []


def test_a_paused_list_trimmed_under_the_limit_sends_changes_from_the_confirmed_set_in_the_same_run(
    tmp_path, monkeypatch
):
    handler = _handler(tmp_path)
    _confirmed_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))
    _run(handler, monkeypatch, [_list_row("77", "4", added_at="200")], {"77": {"1", "2", "3", "4"}}, max_members=3)

    handler = _handler(tmp_path)
    trimmed = _list_row("77", "3", added_at="300")
    _run(handler, monkeypatch, [trimmed], {"77": {"2", "3", "4"}}, max_members=3)

    records = _membership(handler)
    assert [stream for stream, _ in records] == [ss.MEMBERSHIP_CHANGES_STREAM]
    change = records[0][1]["data"]
    assert change["baseMemberSetFingerprint"] == ss.member_set_fingerprint({"1", "2"})
    assert sorted(change["addedMemberIds"]) == ["3", "4"]
    assert change["removedMemberIds"] == ["1"]
    assert _inventory_entries(handler)["77"]["snapshotMemberSetFingerprint"] == ss.member_set_fingerprint(
        {"2", "3", "4"}
    )
    assert not ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")["is_membership_paused"]


def test_a_paused_list_under_a_raised_limit_resumes_from_its_snapshot_without_a_fetch(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _confirmed_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))
    grown = _list_row("77", "4", added_at="200")
    _run(handler, monkeypatch, [grown], {"77": {"1", "2", "3", "4"}}, max_members=3)

    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [grown], {}, max_members=10)

    records = _membership(handler)
    assert [stream for stream, _ in records] == [ss.MEMBERSHIP_CHANGES_STREAM]
    assert sorted(records[0][1]["data"]["addedMemberIds"]) == ["3", "4"]


def test_a_paused_set_over_the_limit_stays_paused_when_the_reported_count_dips(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _confirmed_snapshot(handler, "77", ["1", "2"], _list_row("77", "2"))
    _run(handler, monkeypatch, [_list_row("77", "4", added_at="200")], {"77": {"1", "2", "3", "4"}}, max_members=3)

    handler = _handler(tmp_path)
    lagging = _list_row("77", "3", added_at="200")
    _run(handler, monkeypatch, [lagging], {}, max_members=3)

    assert _membership(handler) == []
    assert _inventory_entries(handler)["77"]["isOverMemberLimit"] is True


def test_a_list_paused_before_it_was_ever_confirmed_resumes_with_a_full_delivery(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "4")], {"77": {"1", "2", "3", "4"}}, max_members=3)
    paused = ss.read_list_snapshot(handler.snapshot_dir, "FLOW1", "77")
    assert paused["sent_since_confirmed"] == set()
    assert ss.members_possibly_held(paused) == set()

    handler = _handler(tmp_path)
    _run(handler, monkeypatch, [_list_row("77", "3", added_at="300")], {"77": {"1", "2", "3"}}, max_members=3)

    records = _membership(handler)
    assert [stream for stream, _ in records] == [ss.MEMBERSHIP_FULL_BATCH_STREAM]
    assert sorted(records[0][1]["data"]["memberIds"]) == ["1", "2", "3"]
    assert all("removedMemberIds" not in record["data"] for _, record in records)


def test_the_scope_is_read_from_the_config_the_tap_ran_with(tmp_path, monkeypatch):
    tap_config = tmp_path / "tap-config.json"
    tap_config.write_text(json.dumps({"membership_list_ids": ["25"], "list_ids": ["25"]}))
    (tmp_path / "source-config.json").write_text(json.dumps({"membership_list_ids": ["none"]}))
    monkeypatch.setattr(ss, "TAP_RUN_CONFIG_PATH", str(tap_config))

    assert ss.read_source_config(str(tmp_path))["list_ids"] == ["25"]


def test_without_a_tap_config_the_scope_is_read_from_the_root_dir(tmp_path, monkeypatch):
    (tmp_path / "source-config.json").write_text(json.dumps({"membership_list_ids": ["25"]}))
    monkeypatch.setattr(ss, "TAP_RUN_CONFIG_PATH", str(tmp_path / "missing.json"))

    assert ss.read_source_config(str(tmp_path))["membership_list_ids"] == ["25"]


def test_the_limit_defaults_when_the_key_was_never_written(tmp_path):
    assert ss.read_max_members_per_list(str(tmp_path), str(tmp_path)) == ss.DEFAULT_MAX_MEMBERS_PER_LIST


def test_the_limit_is_read_from_the_tenant_config(tmp_path):
    with open(tmp_path / "tenant-config.json", "w", encoding="utf-8") as handle:
        json.dump({ss.MAX_MEMBERS_PER_LIST_KEY: 250}, handle)
    assert ss.read_max_members_per_list(str(tmp_path), str(tmp_path)) == 250


def test_the_flow_is_read_only(tmp_path):
    with pytest.raises(NotImplementedError):
        _handler(tmp_path).handle_write()


def _dispatch(flow_id):
    return etl._get_handler("hubspot", flow_id, None, None, None, {})


def test_a_listed_flow_goes_to_the_list_sync_handler(monkeypatch):
    monkeypatch.setenv("LIST_SYNC_FLOW_IDS", "LKILIiNQo, OTHER")
    assert isinstance(_dispatch("LKILIiNQo"), HubSpotListSyncHandler)
    assert isinstance(_dispatch("OTHER"), HubSpotListSyncHandler)


def test_any_other_flow_keeps_the_hubspot_handler(monkeypatch):
    monkeypatch.setenv("LIST_SYNC_FLOW_IDS", "LKILIiNQo")
    handler = _dispatch("AJ3x0LMYI")
    assert isinstance(handler, HubSpotHandler)
    assert not isinstance(handler, HubSpotListSyncHandler)


def test_with_the_variable_unset_every_flow_keeps_the_hubspot_handler(monkeypatch):
    monkeypatch.delenv("LIST_SYNC_FLOW_IDS", raising=False)
    assert isinstance(_dispatch("LKILIiNQo"), HubSpotHandler)
