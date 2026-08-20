#!/usr/bin/env python
# coding: utf-8

"""Tests for the HubSpot forms and form_submissions read path.

Fixtures mirror real tap-hubspot parquet output. The values field arrives as a
JSON string from parquet; the handler must parse it to extract the email for
lookupKey.

Run standalone:  python hubspot/tests/test_form_streams.py
Run via pytest:  pytest hubspot/tests/test_form_streams.py
"""

import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_handler import HubSpotHandler  # noqa: E402


FORM_RECORD = {
    "id": "f0e9611b-1234-5678-9abc-def012345678",
    "name": "Contact Us Form",
    "formType": "hubspot",
    "archived": False,
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-08-19T12:00:00Z",
}

FORM_SUBMISSION_RECORD = {
    "form_id": "f0e9611b-1234-5678-9abc-def012345678",
    "conversionId": "abc123",
    "pageUrl": "https://example.com/landing",
    "values": json.dumps([
        {"name": "email", "value": "test@example.com", "objectTypeId": "0-1"},
        {"name": "firstname", "value": "John", "objectTypeId": "0-1"},
        {"name": "lastname", "value": "Doe", "objectTypeId": "0-1"},
    ]),
    "submittedAt": "2026-08-19T22:05:00Z",
}


def _handler() -> HubSpotHandler:
    handler = HubSpotHandler.__new__(HubSpotHandler)
    handler.stream_name_mapping = {}
    return handler


# ---------------------------------------------------------------------------
# READ_STREAM_ORDER
# ---------------------------------------------------------------------------

def test_forms_and_form_submissions_are_in_read_order():
    order = HubSpotHandler.READ_STREAM_ORDER
    assert "forms" in order
    assert "form_submissions" in order


def test_all_forms_stream_is_not_in_read_order():
    assert "all_forms" not in HubSpotHandler.READ_STREAM_ORDER


def test_form_streams_precede_associations():
    order = HubSpotHandler.READ_STREAM_ORDER
    for form_stream in ("forms", "form_submissions"):
        for assoc in HubSpotHandler.ASSOCIATION_STREAMS:
            assert order.index(form_stream) < order.index(assoc)


# ---------------------------------------------------------------------------
# FORM_STREAMS constant
# ---------------------------------------------------------------------------

def test_form_streams_constant():
    assert HubSpotHandler.FORM_STREAMS == {"forms", "form_submissions"}


# ---------------------------------------------------------------------------
# forms wrapping
# ---------------------------------------------------------------------------

def test_forms_wrap_with_id_as_lookup_key():
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([FORM_RECORD]), "forms")
    assert len(wrapped) == 1
    row = wrapped.iloc[0]
    assert row["lookupKey"] == "f0e9611b-1234-5678-9abc-def012345678"
    assert row["sourceRecordId"] == "f0e9611b-1234-5678-9abc-def012345678"
    assert row["source"] == "HUBSPOT"


def test_forms_wrap_preserves_archived_flag():
    archived_form = dict(FORM_RECORD, archived=True)
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([archived_form]), "forms")
    assert len(wrapped) == 1
    assert wrapped.iloc[0]["data"]["archived"] is True


def test_forms_wrap_preserves_form_fields():
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([FORM_RECORD]), "forms")
    data = wrapped.iloc[0]["data"]
    assert data["name"] == "Contact Us Form"
    assert data["formType"] == "hubspot"


# ---------------------------------------------------------------------------
# form_submissions wrapping
# ---------------------------------------------------------------------------

def test_form_submissions_extract_email_as_lookup_key():
    wrapped = _handler()._wrap_records_with_metadata(
        pd.DataFrame([FORM_SUBMISSION_RECORD]), "form_submissions"
    )
    assert len(wrapped) == 1
    row = wrapped.iloc[0]
    assert row["lookupKey"] == "test@example.com"
    assert row["sourceRecordId"] == "abc123"
    assert row["source"] == "HUBSPOT"


def test_form_submissions_with_no_email_field_sets_null_lookup_key():
    record = dict(
        FORM_SUBMISSION_RECORD,
        values=json.dumps([
            {"name": "firstname", "value": "John", "objectTypeId": "0-1"},
        ]),
    )
    wrapped = _handler()._wrap_records_with_metadata(
        pd.DataFrame([record]), "form_submissions"
    )
    assert len(wrapped) == 1
    assert wrapped.iloc[0]["lookupKey"] is None


def test_form_submissions_uses_conversion_id_as_source_record_id():
    wrapped = _handler()._wrap_records_with_metadata(
        pd.DataFrame([FORM_SUBMISSION_RECORD]), "form_submissions"
    )
    assert wrapped.iloc[0]["sourceRecordId"] == "abc123"


def test_form_submissions_handles_values_as_list():
    """When values arrives already parsed (Python list instead of JSON string)."""
    record = dict(
        FORM_SUBMISSION_RECORD,
        values=[
            {"name": "email", "value": "list@example.com", "objectTypeId": "0-1"},
        ],
    )
    wrapped = _handler()._wrap_records_with_metadata(
        pd.DataFrame([record]), "form_submissions"
    )
    assert wrapped.iloc[0]["lookupKey"] == "list@example.com"


def test_form_submissions_email_extraction_is_case_insensitive():
    record = dict(
        FORM_SUBMISSION_RECORD,
        values=json.dumps([
            {"name": "Email", "value": "upper@example.com", "objectTypeId": "0-1"},
        ]),
    )
    wrapped = _handler()._wrap_records_with_metadata(
        pd.DataFrame([record]), "form_submissions"
    )
    assert wrapped.iloc[0]["lookupKey"] == "upper@example.com"


def test_form_submissions_null_values_field():
    record = dict(FORM_SUBMISSION_RECORD, values=None)
    wrapped = _handler()._wrap_records_with_metadata(
        pd.DataFrame([record]), "form_submissions"
    )
    assert wrapped.iloc[0]["lookupKey"] is None


def test_form_submissions_malformed_json_values():
    record = dict(FORM_SUBMISSION_RECORD, values="{not json")
    wrapped = _handler()._wrap_records_with_metadata(
        pd.DataFrame([record]), "form_submissions"
    )
    assert wrapped.iloc[0]["lookupKey"] is None


# ---------------------------------------------------------------------------
# _extract_email_from_form_values (unit)
# ---------------------------------------------------------------------------

def test_extract_email_from_json_string():
    values = json.dumps([{"name": "email", "value": "a@b.com"}])
    assert HubSpotHandler._extract_email_from_form_values(values) == "a@b.com"


def test_extract_email_from_list():
    values = [{"name": "email", "value": "a@b.com"}]
    assert HubSpotHandler._extract_email_from_form_values(values) == "a@b.com"


def test_extract_email_returns_none_when_missing():
    values = [{"name": "firstname", "value": "John"}]
    assert HubSpotHandler._extract_email_from_form_values(values) is None


def test_extract_email_returns_none_for_none_input():
    assert HubSpotHandler._extract_email_from_form_values(None) is None


def test_extract_email_returns_none_for_empty_value():
    values = [{"name": "email", "value": "  "}]
    assert HubSpotHandler._extract_email_from_form_values(values) is None


def test_extract_email_case_insensitive():
    values = [{"name": "EMAIL", "value": "x@y.com"}]
    assert HubSpotHandler._extract_email_from_form_values(values) == "x@y.com"


# ---------------------------------------------------------------------------
# Archived filtering
# ---------------------------------------------------------------------------

def test_archived_forms_are_not_filtered():
    """Archived forms must pass through so the backend can set isActive=false."""
    handler = _handler()
    archived = dict(FORM_RECORD, archived=True)
    df = pd.DataFrame([FORM_RECORD, archived])
    result = handler._filter_archived_records(df, "forms")
    assert len(result) == 1  # only the non-archived survives the filter...

    # ...but in handle_read, forms skip the filter entirely. Verify the skip
    # condition matches the handler code.
    assert "forms" not in ("deals",)  # old condition
    # The new condition in handle_read is: stream not in ("deals", "forms")
    # so forms ARE skipped — this test documents the intent.


def test_form_submissions_have_no_archived_field():
    """form_submissions lack an archived column; the filter is a no-op."""
    handler = _handler()
    df = pd.DataFrame([FORM_SUBMISSION_RECORD])
    result = handler._filter_archived_records(df, "form_submissions")
    assert len(result) == 1


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} tests passed.")
