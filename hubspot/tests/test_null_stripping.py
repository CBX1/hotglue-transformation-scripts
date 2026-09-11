#!/usr/bin/env python
# coding: utf-8

"""Tests that nulls are stripped at every depth of a Singer record.

The read path nests every source row under ``data``, so a top-level-only filter
leaves one explicit ``"prop": null`` per absent HubSpot property. On a portal
with ~1,030 properties that is ~69% of the emitted payload.

Run standalone:  python hubspot/tests/test_null_stripping.py
Run via pytest:  pytest hubspot/tests/test_null_stripping.py
"""

import json
import os
import sys
import tempfile

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import _strip_nulls, append_singer_records  # noqa: E402


def _records(output_dir):
    with open(os.path.join(output_dir, "data.singer"), encoding="utf-8") as fh:
        return [json.loads(l) for l in fh if l.strip() and json.loads(l)["type"] == "RECORD"]


def test_nulls_inside_data_are_dropped():
    """The regression: absent HubSpot properties must not ship as explicit nulls."""
    chunk = pd.DataFrame([{
        "data": {"email": "a@b.com", "jobtitle": None, "territory_ae_email__c": None},
        "sourceRecordId": "1",
        "source": "HUBSPOT",
        "lookupKey": "a@b.com",
    }])
    with tempfile.TemporaryDirectory() as out:
        append_singer_records(chunk, "contacts", out, first_chunk=True)
        data = _records(out)[0]["record"]["data"]

    assert data == {"email": "a@b.com"}
    assert "jobtitle" not in data
    assert "territory_ae_email__c" not in data


def test_pandas_and_numpy_missing_scalars_are_dropped():
    assert _strip_nulls({"a": np.nan, "b": pd.NaT, "c": pd.NA, "d": None, "e": 1}) == {"e": 1}


def test_falsy_but_real_values_survive():
    """Empty string, 0 and False are data, not absence — dropping them loses signal."""
    record = {"empty": "", "zero": 0, "false": False, "empty_list": [], "empty_dict": {}}
    assert _strip_nulls(record) == record


def test_nulls_inside_nested_lists_are_dropped():
    record = {"crmListMembershipDetails": [{"id": "1", "name": None}, None, {"id": "2"}]}
    assert _strip_nulls(record) == {
        "crmListMembershipDetails": [{"id": "1"}, {"id": "2"}]
    }


def test_nested_dicts_are_walked():
    assert _strip_nulls({"a": {"b": {"c": None, "d": 1}}}) == {"a": {"b": {"d": 1}}}


def test_top_level_nulls_still_dropped():
    """Behaviour the previous top-level filter provided must not regress."""
    chunk = pd.DataFrame([{
        "data": {"email": "a@b.com"},
        "sourceRecordId": None,
        "source": "HUBSPOT",
        "lookupKey": "a@b.com",
    }])
    with tempfile.TemporaryDirectory() as out:
        append_singer_records(chunk, "contacts", out, first_chunk=True)
        rec = _records(out)[0]["record"]

    assert "sourceRecordId" not in rec
    assert rec["lookupKey"] == "a@b.com"


def test_strip_is_not_applied_to_the_schema_message():
    """SCHEMA must still declare every column, including all-null ones."""
    chunk = pd.DataFrame([{
        "data": {"email": "a@b.com"}, "sourceRecordId": None,
        "source": "HUBSPOT", "lookupKey": "a@b.com",
    }])
    with tempfile.TemporaryDirectory() as out:
        append_singer_records(chunk, "contacts", out, first_chunk=True)
        with open(os.path.join(out, "data.singer"), encoding="utf-8") as fh:
            schema = json.loads(fh.readline())

    assert schema["type"] == "SCHEMA"
    assert set(schema["schema"]["properties"]) == {
        "data", "sourceRecordId", "source", "lookupKey",
    }


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} tests passed.")
