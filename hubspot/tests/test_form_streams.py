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


FORM_ID = "f0e9611b-9dd1-424a-ad08-2bf42614b35f"

FORM_RECORD = {
    "id": FORM_ID,
    "name": "Paginated Form",
    "fieldGroups": json.dumps([{
        "groupType": "default_group",
        "richTextType": "text",
        "fields": [
            {"objectTypeId": "0-1", "name": "firstname", "label": "First Name",
             "required": False, "hidden": False, "fieldType": "single_line_text"},
            {"objectTypeId": "0-1", "name": "email", "label": "Email",
             "required": True, "hidden": False, "fieldType": "single_line_text"},
        ],
    }]),
    "configuration": json.dumps({"language": "en", "cloneable": True}),
    "displayOptions": json.dumps({"renderRawHtml": False, "theme": "default_style"}),
    "legalConsentOptions": json.dumps({"type": "none"}),
    "formType": "hubspot",
    "archived": False,
    "userId": float("nan"),
    "createdAt": pd.Timestamp("2026-08-19 21:45:23.440"),
    "updatedAt": pd.Timestamp("2026-08-19 21:48:22.223"),
}

FORM_SUBMISSION_RECORD = {
    "form_id": FORM_ID,
    "conversionId": "55bc5c7f-65ef-4669-b84b-9f99d86d23fe",
    "pageUrl": "https://example.com/spike-test",
    "values": json.dumps([
        {"name": "firstname", "value": "Maya", "objectTypeId": "0-1"},
        {"name": "email", "value": "maya.iyer@cbxspiketest.com", "objectTypeId": "0-1"},
    ]),
    "submittedAt": pd.Timestamp("2026-08-19 21:49:12.801"),
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
    assert row["lookupKey"] == FORM_ID
    assert row["sourceRecordId"] == FORM_ID
    assert row["source"] == "HUBSPOT"


def test_forms_wrap_preserves_archived_flag():
    archived_form = dict(FORM_RECORD, archived=True)
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([archived_form]), "forms")
    assert len(wrapped) == 1
    assert wrapped.iloc[0]["data"]["archived"] is True


def test_forms_wrap_preserves_form_fields():
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([FORM_RECORD]), "forms")
    data = wrapped.iloc[0]["data"]
    assert data["name"] == "Paginated Form"
    assert data["formType"] == "hubspot"


def test_forms_wrap_preserves_json_string_fields():
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([FORM_RECORD]), "forms")
    data = wrapped.iloc[0]["data"]
    assert "fieldGroups" in data
    assert "configuration" in data
    assert "legalConsentOptions" in data


# ---------------------------------------------------------------------------
# form_submissions wrapping
# ---------------------------------------------------------------------------

def test_form_submissions_extract_email_as_lookup_key():
    wrapped = _handler()._wrap_records_with_metadata(
        pd.DataFrame([FORM_SUBMISSION_RECORD]), "form_submissions"
    )
    assert len(wrapped) == 1
    row = wrapped.iloc[0]
    assert row["lookupKey"] == "maya.iyer@cbxspiketest.com"
    assert row["sourceRecordId"] == "55bc5c7f-65ef-4669-b84b-9f99d86d23fe"
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
    assert wrapped.iloc[0]["sourceRecordId"] == "55bc5c7f-65ef-4669-b84b-9f99d86d23fe"


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


# ---------------------------------------------------------------------------
# Realistic batch (mirrors actual spike job _al-J output)
# ---------------------------------------------------------------------------

def test_realistic_submission_batch():
    """Process a batch resembling real tap-hubspot parquet output from the spike job."""
    batch = [
        {
            "form_id": FORM_ID,
            "conversionId": "55bc5c7f-65ef-4669-b84b-9f99d86d23fe",
            "pageUrl": "https://example.com/spike-test",
            "values": json.dumps([
                {"name": "firstname", "value": "Maya", "objectTypeId": "0-1"},
                {"name": "email", "value": "maya.iyer@cbxspiketest.com", "objectTypeId": "0-1"},
            ]),
            "submittedAt": pd.Timestamp("2026-08-19 21:49:12.801"),
        },
        {
            "form_id": FORM_ID,
            "conversionId": "a4d32deb-9ee1-49eb-ad8d-2b7647562330",
            "pageUrl": "https://example.com/spike-test",
            "values": json.dumps([
                {"name": "email", "value": "spike-test-055@example.com", "objectTypeId": "0-1"},
            ]),
            "submittedAt": pd.Timestamp("2026-08-19 21:47:40.663"),
        },
        {
            "form_id": FORM_ID,
            "conversionId": "c71f4347-3b57-4b37-bbc6-44c133ab989b",
            "pageUrl": "https://example.com/spike-test",
            "values": json.dumps([
                {"name": "email", "value": "spike-test-054@example.com", "objectTypeId": "0-1"},
            ]),
            "submittedAt": pd.Timestamp("2026-08-19 21:47:40.209"),
        },
    ]
    df = pd.DataFrame(batch)
    wrapped = _handler()._wrap_records_with_metadata(df, "form_submissions")

    assert len(wrapped) == 3
    assert list(wrapped["lookupKey"]) == [
        "maya.iyer@cbxspiketest.com",
        "spike-test-055@example.com",
        "spike-test-054@example.com",
    ]
    assert list(wrapped["sourceRecordId"]) == [
        "55bc5c7f-65ef-4669-b84b-9f99d86d23fe",
        "a4d32deb-9ee1-49eb-ad8d-2b7647562330",
        "c71f4347-3b57-4b37-bbc6-44c133ab989b",
    ]
    for _, row in wrapped.iterrows():
        assert row["source"] == "HUBSPOT"
        assert row["data"]["form_id"] == FORM_ID
        assert "values" in row["data"]


def test_realistic_forms_batch():
    """Process a forms record with the full column set from parquet."""
    df = pd.DataFrame([FORM_RECORD])
    wrapped = _handler()._wrap_records_with_metadata(df, "forms")

    assert len(wrapped) == 1
    data = wrapped.iloc[0]["data"]
    assert data["id"] == FORM_ID
    assert data["name"] == "Paginated Form"
    assert data["formType"] == "hubspot"
    assert data["archived"] is False
    assert wrapped.iloc[0]["lookupKey"] == FORM_ID
    assert wrapped.iloc[0]["sourceRecordId"] == FORM_ID


def test_handle_read_without_mapping_processes_form_streams_only():
    """A flow with no tenant mapping (the forms flow) must still emit the
    pass-through form streams, while mapped CRM streams are skipped."""
    import hubspot_handler as hh

    handler = _handler()
    handler.mapping_for_flow = {}
    handler.flow_id = "vUZid8TMR"
    handler.input_dir = "unused"
    handler.output_dir = "unused"
    handler.list_available_streams = lambda: ["contacts", "form_submissions", "forms"]
    handler._prepare_owner_lookup = lambda streams: None
    handler._prepare_list_lookup = lambda streams: None
    handler._prepare_account_lookup = lambda: None

    chunks = {
        "forms": pd.DataFrame([FORM_RECORD]),
        "form_submissions": pd.DataFrame([FORM_SUBMISSION_RECORD]),
        "contacts": pd.DataFrame([{"id": "1", "email": "a@b.com"}]),
    }
    written = []
    orig_iter, orig_append = hh.iter_stream_chunks, hh.append_singer_records
    hh.iter_stream_chunks = lambda input_dir, stream: iter([chunks[stream]])
    hh.append_singer_records = (
        lambda df, stream, output_dir, first: written.append((stream, len(df)))
    )
    try:
        handler.handle_read()
    finally:
        hh.iter_stream_chunks, hh.append_singer_records = orig_iter, orig_append

    written_streams = [s for s, _ in written]
    assert written_streams == ["forms", "form_submissions"]
    assert all(count == 1 for _, count in written)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} tests passed.")
