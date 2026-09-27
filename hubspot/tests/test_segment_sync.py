"""
Tests for the HubSpot list -> CBX1 segment mirror.

Pins the properties whose breakage is silent rather than loud — a fingerprint that changes,
a chain that does not add up, a snapshot that merges instead of overwriting.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import hubspot_list_sync_handler as ss  # noqa: E402


# --- The fingerprint contract ---

JOB = "job-1"


@pytest.mark.parametrize(
    "member_ids, expected",
    [
        (["1"], "6b86b273ff34fce19d6b804eff5a3f57"),
        (["1", "2", "3"], "f1f2acccbbd431841d9adcfeaa0e81f9"),
        (["101", "202"], "d7a2ec240f5029d8106a8db4b950a1ca"),
        ([], "0" * 32),
    ],
)
def test_fingerprint_matches_the_pinned_vectors(member_ids, expected):
    assert ss.member_set_fingerprint(member_ids) == expected


def test_empty_member_set_fingerprint_is_32_zeros():
    assert ss.EMPTY_MEMBER_SET_FINGERPRINT == "0" * 32


def test_fingerprint_is_32_lowercase_hex_characters():
    fingerprint = ss.member_set_fingerprint(["a", "b"])
    assert len(fingerprint) == 32
    assert fingerprint == fingerprint.lower()
    int(fingerprint, 16)


def test_fingerprint_ignores_order_and_duplicates():
    # XOR is self-inverse, so a duplicate would cancel itself out if ids were not
    # de-duplicated first — ["1","1","2"] would collide with ["2"].
    assert ss.member_set_fingerprint(["1", "2", "3"]) == ss.member_set_fingerprint(["3", "1", "2"])
    assert ss.member_set_fingerprint(["1", "1", "2"]) == ss.member_set_fingerprint(["1", "2"])
    assert ss.member_set_fingerprint(["1", "1", "2"]) != ss.member_set_fingerprint(["2"])


def test_ids_are_hashed_as_strings_so_numeric_and_text_ids_agree():
    # The tap can hand us ints; the same member must fingerprint the same either way.
    assert ss.member_set_fingerprint([1, 2]) == ss.member_set_fingerprint(["1", "2"])


def test_member_ids_are_trimmed_strings():
    assert ss.normalise_member_id(" 123 ") == "123"
    assert ss.normalise_member_id(123) == "123"


# --- The fingerprint chain ---


def _data(record):
    return record["data"]


def test_change_records_chain_from_previous_to_current():
    previous = ["1", "2", "3"]
    current = ["2", "3", "4"]

    records = ss.build_change_records("77", previous, current, JOB)
    payloads = [_data(r) for r in records]

    # Each link starts where the last one ended.
    assert payloads[0]["baseMemberSetFingerprint"] == ss.member_set_fingerprint(previous)
    for earlier, later in zip(payloads, payloads[1:]):
        assert later["baseMemberSetFingerprint"] == earlier["newMemberSetFingerprint"]

    # And the chain lands exactly on the fingerprint of the new member set.
    assert payloads[-1]["newMemberSetFingerprint"] == ss.member_set_fingerprint(current)


def test_change_records_carry_the_diff():
    records = ss.build_change_records("77", ["1", "2", "3"], ["2", "3", "4"], JOB)
    payloads = [_data(r) for r in records]

    assert len(payloads) == 1
    assert payloads[0]["addedMemberIds"] == ["4"]
    assert payloads[0]["removedMemberIds"] == ["1"]


def test_an_unchanged_list_produces_no_change_records():
    assert ss.build_change_records("77", ["1", "2"], ["2", "1"], JOB) == []


def test_every_membership_record_carries_the_job_id():
    for record in ss.build_change_records("77", ["1"], ["2"], JOB):
        assert _data(record)["jobId"] == JOB
    for record in ss.build_full_delivery_records("77", ["1"], JOB):
        assert _data(record)["jobId"] == JOB


def test_emptying_a_list_chains_to_the_empty_fingerprint():
    records = ss.build_change_records("77", ["1", "2"], [], JOB)
    payloads = [_data(r) for r in records]

    assert payloads[0]["removedMemberIds"] == ["1", "2"]
    assert payloads[-1]["newMemberSetFingerprint"] == ss.EMPTY_MEMBER_SET_FINGERPRINT


def test_change_slices_respect_the_record_cap_and_still_chain():
    previous = [f"old-{n}" for n in range(ss.MEMBER_BATCH_SIZE + 1200)]
    current = [f"new-{n}" for n in range(ss.MEMBER_BATCH_SIZE + 800)]

    records = ss.build_change_records("77", previous, current, JOB)
    payloads = [_data(r) for r in records]

    assert len(payloads) > 2, "a diff this size must span several records"
    for payload in payloads:
        assert len(payload["addedMemberIds"]) + len(payload["removedMemberIds"]) <= ss.MEMBER_BATCH_SIZE

    for earlier, later in zip(payloads, payloads[1:]):
        assert later["baseMemberSetFingerprint"] == earlier["newMemberSetFingerprint"]
    assert payloads[-1]["newMemberSetFingerprint"] == ss.member_set_fingerprint(current)

    # Every id must appear exactly once across the slices — no drops, no repeats.
    added = [i for p in payloads for i in p["addedMemberIds"]]
    removed = [i for p in payloads for i in p["removedMemberIds"]]
    assert sorted(added) == sorted(set(current) - set(previous))
    assert sorted(removed) == sorted(set(previous) - set(current))


def test_every_membership_record_is_addressed_by_list_id():
    # The payload has no list id of its own: the backend reads it off the envelope's
    # lookupKey, and the target drops any record whose lookupKey is null.
    for record in ss.build_change_records("77", ["1"], ["2"], JOB):
        assert record["lookupKey"] == "77"
        assert record["source"] == "HUBSPOT"
    for record in ss.build_full_delivery_records("77", ["1"], JOB):
        assert record["lookupKey"] == "77"


# --- Full deliveries ---


def test_full_delivery_numbers_its_batches_under_one_run_id():
    members = [str(n) for n in range(ss.MEMBER_BATCH_SIZE * 2 + 5)]
    records = ss.build_full_delivery_records("77", members, JOB)
    payloads = [_data(r) for r in records]

    assert len(payloads) == 3
    assert {p["fullSyncRunId"] for p in payloads} == {payloads[0]["fullSyncRunId"]}
    assert [p["batchNumber"] for p in payloads] == [1, 2, 3]
    assert all(p["totalBatches"] == 3 for p in payloads)

    # The fingerprint describes the whole set, so it is identical on every batch — that is what
    # lets the backend verify the assembled result once the last batch lands.
    assert {p["fullMemberSetFingerprint"] for p in payloads} == {ss.member_set_fingerprint(members)}
    assert sorted(i for p in payloads for i in p["memberIds"]) == sorted(members)


def test_full_delivery_of_an_empty_list_still_sends_one_batch():
    # Zero batches would leave the backend with a delivery that can never complete.
    records = ss.build_full_delivery_records("77", [], JOB)
    assert len(records) == 1

    payload = _data(records[0])
    assert payload["memberIds"] == []
    assert payload["totalBatches"] == 1
    assert payload["fullMemberSetFingerprint"] == ss.EMPTY_MEMBER_SET_FINGERPRINT


# --- List inventory ---


def _list_row(list_id, object_type="0-1", processing="DYNAMIC", size=None):
    return {
        "listId": list_id,
        "name": f"List {list_id}",
        "objectTypeId": object_type,
        "processingType": processing,
        "hs_list_size": size,
    }


def test_adhoc_and_unsupported_object_types_never_become_segments():
    rows = [
        _list_row("1"),
        _list_row("2", processing="ADHOC"),        # membership endpoint rejects these
        _list_row("3", object_type="0-3"),         # no member entity on our side
    ]
    records = ss.build_list_inventory_records(rows, "244660268", unscoped=True, job_id=JOB, snapshot_fingerprints={})
    entries = [e["sourceSegmentId"] for r in records for e in _data(r)["lists"]]
    assert entries == ["1"]


def test_processing_type_match_is_case_insensitive():
    records = ss.build_list_inventory_records([_list_row("1", processing="dynamic")], None, unscoped=True, job_id=JOB, snapshot_fingerprints={})
    assert len(records) == 1


def test_all_list_ids_rides_only_the_final_batch_of_an_unscoped_run():
    rows = [_list_row(str(n)) for n in range(ss.LIST_BATCH_SIZE + 10)]
    records = ss.build_list_inventory_records(rows, "244660268", unscoped=True, job_id=JOB, snapshot_fingerprints={})
    payloads = [_data(r) for r in records]

    assert len(payloads) == 2
    assert "allListIds" not in payloads[0]
    assert payloads[1]["allListIds"] == [str(n) for n in range(ss.LIST_BATCH_SIZE + 10)]


def test_a_scoped_run_never_claims_to_know_every_list():
    # allListIds is a complete statement — absence from it means "deleted at the source".
    # A scoped run only looked at some lists, so making that claim would delete the rest.
    rows = [_list_row("1"), _list_row("2")]
    records = ss.build_list_inventory_records(rows, "244660268", unscoped=False, job_id=JOB, snapshot_fingerprints={})
    assert all("allListIds" not in _data(r) for r in records)


def test_inventory_batches_are_numbered_and_carry_the_portal_id():
    rows = [_list_row(str(n)) for n in range(ss.LIST_BATCH_SIZE + 1)]
    payloads = [_data(r) for r in ss.build_list_inventory_records(rows, "244660268", unscoped=True, job_id=JOB, snapshot_fingerprints={})]

    assert [p["batchNumber"] for p in payloads] == [1, 2]
    assert all(p["totalBatches"] == 2 for p in payloads)
    assert all(p["sourceAccountId"] == "244660268" for p in payloads)
    assert len(payloads[0]["lists"]) == ss.LIST_BATCH_SIZE


def test_inventory_states_snapshot_fingerprints_only_where_given():
    rows = [_list_row("1"), _list_row("2")]
    fingerprint = ss.member_set_fingerprint(["9"])
    records = ss.build_list_inventory_records(
        rows, "244660268", unscoped=True, job_id=JOB, snapshot_fingerprints={"1": fingerprint}
    )
    payload = _data(records[0])
    entries = {e["sourceSegmentId"]: e for e in payload["lists"]}

    assert payload["jobId"] == JOB
    assert entries["1"]["snapshotMemberSetFingerprint"] == fingerprint
    assert "snapshotMemberSetFingerprint" not in entries["2"]


def test_no_supported_lists_emits_nothing():
    assert ss.build_list_inventory_records([_list_row("1", processing="ADHOC")], None, unscoped=True, job_id=JOB, snapshot_fingerprints={}) == []


def test_member_count_is_read_from_either_shape():
    # hs_list_size arrives top level or nested under additionalProperties, which itself
    # can arrive as a JSON string.
    assert ss.reported_member_count({"hs_list_size": 12}) == 12
    assert ss.reported_member_count({"additionalProperties": {"hs_list_size": 34}}) == 34
    assert ss.reported_member_count({"additionalProperties": json.dumps({"hs_list_size": 56})}) == 56
    assert ss.reported_member_count({}) is None


# --- Snapshots ---


SIGNALS_A = {"hs_last_record_added_at": "100", "hs_last_record_removed_at": None, "hs_list_size": "4"}
SIGNALS_B = {"hs_last_record_added_at": "200", "hs_last_record_removed_at": None, "hs_list_size": "5"}


def test_snapshot_round_trips(tmp_path):
    ss.write_list_snapshot(str(tmp_path), "FLOW1", "77", ["2", "1"], SIGNALS_A)
    snapshot = ss.read_list_snapshot(str(tmp_path), "FLOW1", "77")
    assert snapshot["members"] == {"1", "2"}
    assert snapshot["change_signals"] == SIGNALS_A


def test_snapshot_overwrites_and_never_merges(tmp_path):
    # The property the whole removal path depends on: under a merge, "1" would survive
    # forever and its removal could never be computed again.
    ss.write_list_snapshot(str(tmp_path), "FLOW1", "77", ["1", "2"], SIGNALS_A)
    ss.write_list_snapshot(str(tmp_path), "FLOW1", "77", ["2"], SIGNALS_B)
    snapshot = ss.read_list_snapshot(str(tmp_path), "FLOW1", "77")
    assert snapshot["members"] == {"2"}
    assert snapshot["change_signals"] == SIGNALS_B


def test_a_missing_snapshot_is_none_not_empty(tmp_path):
    # None means "no baseline, send everything"; an empty set means "last seen with no
    # members". Conflating them turns a first sync into a mass removal.
    assert ss.read_list_snapshot(str(tmp_path), "FLOW1", "missing") is None

    ss.write_list_snapshot(str(tmp_path), "FLOW1", "empty", [], SIGNALS_A)
    assert ss.read_list_snapshot(str(tmp_path), "FLOW1", "empty")["members"] == set()


def test_a_corrupt_snapshot_is_treated_as_absent(tmp_path):
    # Forces a full delivery, which re-establishes the baseline. Reading it as empty would
    # look like every member had been removed.
    path = ss.member_snapshot_path(str(tmp_path), "FLOW1", "77")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("{not json")
    assert ss.read_list_snapshot(str(tmp_path), "FLOW1", "77") is None


def test_snapshot_format_change_needs_a_rebaseline(tmp_path):
    import pandas as pd
    row = {
        "listId": "25",
        "additionalProperties": {
            "hs_last_record_added_at": "1790064260874",
            "hs_last_record_removed_at": "1790064271877",
            "hs_list_size": "5",
        },
        "updatedAt": pd.Timestamp("2026-09-22 07:48:55.946000"),
    }
    ss.write_list_snapshot(str(tmp_path), "FLOW1", "25", ["2", "1"], ss.change_signals(row))

    with open(ss.member_snapshot_path(str(tmp_path), "FLOW1", "25"), encoding="utf-8") as handle:
        assert handle.read() == (
            '{"members": ["1", "2"], "change_signals": {'
            '"hs_last_record_added_at": "1790064260874", '
            '"hs_last_record_removed_at": "1790064271877", '
            '"hs_list_size": "5", '
            '"updatedAt": "2026-09-22T07:48:55.946000"}}'
        )


def test_snapshots_are_scoped_per_list_and_per_flow(tmp_path):
    ss.write_list_snapshot(str(tmp_path), "FLOW1", "77", ["1"], SIGNALS_A)
    ss.write_list_snapshot(str(tmp_path), "FLOW2", "77", ["2"], SIGNALS_A)
    assert ss.read_list_snapshot(str(tmp_path), "FLOW1", "77")["members"] == {"1"}
    assert ss.read_list_snapshot(str(tmp_path), "FLOW2", "77")["members"] == {"2"}


# --- Change change_signals ---


def test_change_signals_are_read_from_additional_properties():
    row = {
        "listId": "9",
        "additionalProperties": {
            "hs_last_record_added_at": "1766031258308",
            "hs_list_size": "4",
        },
    }
    assert ss.change_signals(row) == {
        "hs_last_record_added_at": "1766031258308",
        "hs_last_record_removed_at": None,
        "hs_list_size": "4",
        "updatedAt": None,
    }


def test_change_signals_parse_additional_properties_sent_as_a_json_string():
    row = {"listId": "9", "additionalProperties": json.dumps({"hs_list_size": "4"})}
    assert ss.change_signals(row)["hs_list_size"] == "4"


def test_change_signals_compare_equal_across_runs_for_an_unchanged_list():
    row = {"additionalProperties": {"hs_last_record_added_at": "100", "hs_list_size": "4"}}
    assert ss.change_signals(row) == ss.change_signals(dict(row))


def test_change_signals_differ_when_the_source_moved():
    before = ss.change_signals({"additionalProperties": {"hs_last_record_added_at": "100", "hs_list_size": "4"}})
    after = ss.change_signals({"additionalProperties": {"hs_last_record_added_at": "200", "hs_list_size": "5"}})
    assert before != after


def test_change_signals_are_all_none_when_the_list_reports_nothing():
    # Unknown change_signals must stay distinguishable from a real reading, since unknown is
    # treated as "cannot vouch for the snapshot".
    assert ss.change_signals({"listId": "9"}) == {p: None for p in ss.CHANGE_SIGNAL_PROPERTIES}


def test_change_signals_include_updated_at_to_mirror_the_tap():
    # The tap's fingerprint is {added, removed, updated, size}; ours must match exactly. Less
    # sensitive vouches for a snapshot a dropped fetch left stale; more sensitive reads a
    # legitimate skip as a dropped fetch and fails a Resync.
    import datetime as dt
    before = ss.change_signals({"additionalProperties": {"hs_list_size": "2"},
                                "updatedAt": dt.datetime(2026, 9, 22, 3, 8, 39)})
    after = ss.change_signals({"additionalProperties": {"hs_list_size": "2"},
                               "updatedAt": dt.datetime(2026, 9, 22, 4, 1, 0)})
    assert before != after, "a same-size change must still move the signals"


def test_change_signals_are_stable_across_runs_for_an_untouched_list():
    import datetime as dt
    row = {"additionalProperties": {"hs_list_size": "2"}, "updatedAt": dt.datetime(2026, 9, 22, 3, 8, 39)}
    assert ss.change_signals(row) == ss.change_signals(dict(row))


def test_a_missing_updated_at_does_not_read_as_a_string():
    # pandas renders a missing datetime as NaT; str(NaT) would be a stable-looking value that
    # silently differs from None and could flip between runs.
    import pandas as pd
    signals = ss.change_signals({"additionalProperties": {"hs_list_size": "2"}, "updatedAt": pd.NaT})
    assert signals["updatedAt"] is None


# --- Config inputs ---


def test_missing_config_reads_as_no_instruction(tmp_path):
    assert ss.read_full_resync_list_ids(str(tmp_path), str(tmp_path)) == set()


def test_full_resync_ids_come_from_tenant_config(tmp_path):
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    with open(snapshots / "tenant-config.json", "w", encoding="utf-8") as handle:
        json.dump({ss.FULL_RESYNC_LIST_IDS_KEY: ["77"]}, handle)
    assert ss.read_full_resync_list_ids(str(tmp_path), str(snapshots)) == {"77"}
