#!/usr/bin/env python
# coding: utf-8

"""Tests for the HubSpot READ path (CRM -> CBX1): HubSpotHandler.handle_read.

Covers the four contracts cbx1-target-hotglue and the CBX1 backend depend on:

  * Contacts/companies are wrapped as {data, sourceRecordId, source, lookupKey}
    keyed on email/domain; a record without its lookup field is dropped, because
    cbx1-target skips null lookupKeys anyway and would otherwise log noise.
  * Archived contacts/companies are dropped; archived deals are kept, since the
    backend mirrors ``archived`` to ``isDeleted``.
  * Contact ``associatedcompanyid`` (HubSpot id) resolves to ``accountId`` (CBX1
    UUID) through the accounts snapshot, and degrades to null when it can't.
  * A flow with no tenant mapping still emits the pass-through streams (forms,
    association edges) while skipping the mapped CRM streams.

The end-to-end tests drive handle_read against parquet files in a tmp dir and
parse the real ``data.singer`` it writes — no network, no committed fixtures.

Run via pytest:  pytest hubspot/tests/test_hubspot_read_path.py
"""

import json
import logging
import os
import sys

import gluestick as gs
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_handler import HubSpotHandler  # noqa: E402

FLOW_ID = "AJ3x0LMYI"

# Same shape as the committed snapshots/tenant-config.json for a HubSpot read.
READ_MAPPING = {
    "accounts/companies": {"name": "name", "domain": "domain"},
    "contacts/contacts": {"email": "email", "firstName": "firstname"},
    "deals/deals": {"dealName": "dealname"},
    "deal_company_links/associations_deals_companies": {},
    "deal_contact_links/associations_deals_contacts": {},
}

# HubSpot company id -> CBX1 account UUID, the read-path snapshot convention.
ACCOUNTS_SNAPSHOT = pd.DataFrame(
    {
        "InputId": ["233854320339", "239437936334"],
        "RemoteId": [
            "18a17f06-8452-4a31-9e12-8a8dc509d0ff",
            "a5a514b3-5840-4676-9903-4f4e50079056",
        ],
    }
)

COMPANIES = pd.DataFrame(
    [
        {"id": "233854320339", "name": "Acme", "domain": "acme.com", "archived": False},
        {"id": "239437936334", "name": "Globex", "domain": "globex.com", "archived": False},
        {"id": "300000000001", "name": "Gone Co", "domain": "gone.com", "archived": True},
        {"id": "300000000002", "name": "No Domain", "domain": None, "archived": False},
    ]
)

CONTACTS = pd.DataFrame(
    [
        {"id": "501", "email": "ada@acme.com", "firstname": "Ada",
         "associatedcompanyid": "233854320339", "archived": False},
        {"id": "502", "email": "bo@unknown.com", "firstname": "Bo",
         "associatedcompanyid": "999999999999", "archived": False},
        {"id": "503", "email": "cy@acme.com", "firstname": "Cy",
         "associatedcompanyid": None, "archived": False},
        {"id": "504", "email": "del@acme.com", "firstname": "Del",
         "associatedcompanyid": "233854320339", "archived": True},
        {"id": "505", "email": None, "firstname": "Eve",
         "associatedcompanyid": "233854320339", "archived": False},
    ]
)

DEALS = pd.DataFrame(
    [
        {"id": "700", "dealname": "Live deal", "archived": False},
        {"id": "701", "dealname": "Closed and deleted", "archived": True},
    ]
)

DEAL_COMPANY_EDGES = pd.DataFrame(
    [
        {
            "from_id": "700",
            "to_id": "233854320339",
            "associationTypes": '[{"category": "HUBSPOT_DEFINED", "typeId": 5, "label": "Primary"}]',
        }
    ]
)

FORMS = pd.DataFrame(
    [{"id": "f0e9611b-9dd1-424a-ad08-2bf42614b35f", "name": "Demo form", "archived": False}]
)


def _handler() -> HubSpotHandler:
    """A handler with no reader/mapping — for methods that only touch the frame."""
    handler = HubSpotHandler.__new__(HubSpotHandler)
    handler.stream_name_mapping = {}
    return handler


def _stream_name_mapping(mapping):
    """Mirror etl.py: {"contacts/contacts": ...} -> {"contacts": "contacts"}."""
    return {key.split("/")[0]: key.split("/")[1] for key in mapping}


def _run_read(tmp_path, streams, mapping=None, accounts_snapshot=None):
    """Lay streams out as HotGlue sync-output parquet, run handle_read, parse the output.

    Returns {output_stream: [record, ...]} in emission order, plus the stream order.
    """
    input_dir = tmp_path / "sync-output"
    snapshot_dir = tmp_path / "snapshots"
    output_dir = tmp_path / "etl-output"
    for directory in (input_dir, snapshot_dir, output_dir):
        directory.mkdir()

    for stream, df in streams.items():
        df.to_parquet(input_dir / f"{stream}-20260101T000000.parquet", index=False)
    if accounts_snapshot is not None:
        accounts_snapshot.to_csv(snapshot_dir / f"accounts_{FLOW_ID}.snapshot.csv", index=False)

    mapping = mapping or {}
    handler = HubSpotHandler(
        connector_id="hubspot",
        flow_id=FLOW_ID,
        reader=gs.Reader(str(input_dir)),
        mapping_for_flow=mapping,
        stream_name_mapping=_stream_name_mapping(mapping),
        input_dir=str(input_dir),
        snapshot_dir=str(snapshot_dir),
        output_dir=str(output_dir),
    )
    handler.handle_read()

    records, order = {}, []
    singer_path = output_dir / "data.singer"
    if not singer_path.exists():
        return records, order
    for line in singer_path.read_text(encoding="utf-8").splitlines():
        message = json.loads(line)
        if message["type"] == "SCHEMA":
            order.append(message["stream"])
        elif message["type"] == "RECORD":
            records.setdefault(message["stream"], []).append(message["record"])
    return records, order


# ---------------------------------------------------------------------------
# 1. Record wrapping: {data, sourceRecordId, source, lookupKey}
# ---------------------------------------------------------------------------

def test_contacts_wrap_with_email_as_lookup_key_and_id_as_source_record_id():
    contacts = pd.DataFrame([{"id": "501", "email": "ada@acme.com", "firstname": "Ada"}])

    wrapped = _handler()._wrap_records_with_metadata(contacts, "contacts")

    assert list(wrapped.columns) == ["data", "sourceRecordId", "source", "lookupKey"]
    row = wrapped.iloc[0]
    assert row["lookupKey"] == "ada@acme.com"
    assert row["sourceRecordId"] == "501"
    assert row["source"] == "HUBSPOT"
    # The full record is nested under data, and also stamped with source.
    assert row["data"] == {
        "id": "501",
        "email": "ada@acme.com",
        "firstname": "Ada",
        "source": "HUBSPOT",
    }


def test_companies_wrap_with_domain_as_lookup_key():
    companies = pd.DataFrame([{"id": "233854320339", "name": "Acme", "domain": "acme.com"}])

    row = _handler()._wrap_records_with_metadata(companies, "companies").iloc[0]

    assert row["lookupKey"] == "acme.com"
    assert row["sourceRecordId"] == "233854320339"
    assert row["source"] == "HUBSPOT"
    assert row["data"]["name"] == "Acme"


@pytest.mark.parametrize(
    "stream, lookup_field",
    [("contacts", "email"), ("companies", "domain")],
)
def test_records_with_null_or_blank_lookup_field_are_dropped(stream, lookup_field):
    """cbx1-target skips null lookupKeys, so the ETL drops them up front — and
    whitespace-only counts as missing, not as a key."""
    df = pd.DataFrame(
        [
            {"id": "1", lookup_field: "keep"},
            {"id": "2", lookup_field: None},
            {"id": "3", lookup_field: ""},
            {"id": "4", lookup_field: "   "},
        ]
    )

    wrapped = _handler()._wrap_records_with_metadata(df, stream)

    assert list(wrapped["sourceRecordId"]) == ["1"]
    assert list(wrapped["lookupKey"]) == ["keep"]


def test_chunk_where_every_lookup_is_null_wraps_to_empty():
    df = pd.DataFrame([{"id": "1", "email": None}, {"id": "2", "email": ""}])

    wrapped = _handler()._wrap_records_with_metadata(df, "contacts")

    assert wrapped.empty


@pytest.mark.parametrize(
    "stream, row",
    [
        ("contacts", {"id": "1", "firstname": "No email column"}),
        ("companies", {"id": "1", "name": "No domain column"}),
        ("contacts", {"email": "no-id@acme.com"}),
    ],
)
def test_missing_lookup_or_id_column_yields_empty_wrapped_schema(stream, row):
    """The chunk is skipped with the wrapped column set — never emitted unwrapped,
    which would poison the SCHEMA for every later correctly-wrapped chunk."""
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([row]), stream)

    assert wrapped.empty
    assert list(wrapped.columns) == ["data", "sourceRecordId", "source", "lookupKey"]


def test_wrapped_data_is_json_serializable():
    """NaN/NaT become null, numpy scalars become Python natives, and HubSpot's
    "true"/"false" strings become real booleans."""
    df = pd.DataFrame(
        [
            {
                "id": "501",
                "email": "ada@acme.com",
                "hs_email_optout": "true",
                "num_employees": 42,
                "lastmodifieddate": pd.NaT,
                "jobtitle": float("nan"),
            }
        ]
    )

    data = _handler()._wrap_records_with_metadata(df, "contacts").iloc[0]["data"]

    assert data["hs_email_optout"] is True
    assert data["num_employees"] == 42 and type(data["num_employees"]) is int
    assert data["lastmodifieddate"] is None
    assert data["jobtitle"] is None
    json.dumps(data)


# ---------------------------------------------------------------------------
# 2. Archived rows: dropped for contacts/companies, kept for deals
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stream", ["contacts", "companies"])
def test_filter_archived_records_drops_archived_rows(stream):
    df = pd.DataFrame(
        [
            {"id": "1", "archived": False},
            {"id": "2", "archived": True},
            {"id": "3", "archived": False},
        ]
    )

    filtered = _handler()._filter_archived_records(df, stream)

    assert list(filtered["id"]) == ["1", "3"]
    assert len(df) == 3, "the input frame must not be mutated"


def test_filter_archived_records_is_a_no_op_without_an_archived_column():
    df = pd.DataFrame([{"id": "1"}, {"id": "2"}])

    assert list(_handler()._filter_archived_records(df, "contacts")["id"]) == ["1", "2"]


@pytest.mark.xfail(
    int(pd.__version__.split(".")[0]) >= 3,
    raises=KeyError,
    strict=True,
    reason=(
        "Latent bug, reported in CB-19833: a nullable `archived` column (object dtype) is "
        "no longer downcast to bool by fillna(False) on pandas >= 3, so `~mask` yields "
        "ints (-2/-1) and df.loc raises KeyError, failing the whole read job. "
        "pandas is unpinned (>=1.3.5) in hubspot/requirements.txt."
    ),
)
def test_filter_archived_records_tolerates_null_archived_values():
    df = pd.DataFrame(
        [
            {"id": "1", "archived": False},
            {"id": "2", "archived": True},
            {"id": "3", "archived": None},
        ]
    )

    filtered = _handler()._filter_archived_records(df, "contacts")

    assert list(filtered["id"]) == ["1", "3"]


def test_handle_read_drops_archived_contacts_and_companies_but_keeps_archived_deals(tmp_path):
    records, _ = _run_read(
        tmp_path,
        {"companies": COMPANIES, "contacts": CONTACTS, "deals": DEALS},
        mapping=READ_MAPPING,
        accounts_snapshot=ACCOUNTS_SNAPSHOT,
    )

    company_ids = [r["sourceRecordId"] for r in records["accounts"]]
    contact_ids = [r["sourceRecordId"] for r in records["contacts"]]
    deals = {r["sourceRecordId"]: r for r in records["deals"]}

    # Archived company 300000000001 and archived contact 504 are gone; the
    # domain-less company and email-less contact are dropped by wrapping.
    assert company_ids == ["233854320339", "239437936334"]
    assert contact_ids == ["501", "502", "503"]

    # Both deals survive, and the archived flag reaches the backend for isDeleted.
    assert set(deals) == {"700", "701"}
    assert deals["701"]["data"]["archived"] is True
    assert deals["700"]["data"]["archived"] is False
    assert deals["701"]["lookupKey"] == "701"


# ---------------------------------------------------------------------------
# 3. Contact -> CBX1 account id resolution via the accounts snapshot
# ---------------------------------------------------------------------------

def _snapshot_handler(tmp_path, snapshot: pd.DataFrame = None) -> HubSpotHandler:
    handler = _handler()
    handler.flow_id = FLOW_ID
    handler.snapshot_dir = str(tmp_path)
    if snapshot is not None:
        snapshot.to_csv(tmp_path / f"accounts_{FLOW_ID}.snapshot.csv", index=False)
    return handler


def test_account_lookup_maps_hubspot_company_id_to_cbx1_uuid(tmp_path):
    lookup = _snapshot_handler(tmp_path, ACCOUNTS_SNAPSHOT)._prepare_account_lookup()

    # read_csv parses the numeric InputId as int64; keys must still be strings to
    # match the string associatedcompanyid on contacts.
    assert lookup == {
        "233854320339": "18a17f06-8452-4a31-9e12-8a8dc509d0ff",
        "239437936334": "a5a514b3-5840-4676-9903-4f4e50079056",
    }


def test_account_lookup_skips_rows_without_a_remote_id(tmp_path):
    snapshot = pd.DataFrame(
        {"InputId": ["233854320339", "239437936334"], "RemoteId": ["uuid-1", None]}
    )

    lookup = _snapshot_handler(tmp_path, snapshot)._prepare_account_lookup()

    assert lookup == {"233854320339": "uuid-1"}


def test_account_lookup_is_none_without_a_snapshot(tmp_path):
    assert _snapshot_handler(tmp_path)._prepare_account_lookup() is None


def test_account_lookup_is_none_when_snapshot_columns_are_wrong(tmp_path):
    """A write-path-shaped snapshot must not be misread as a read-path lookup."""
    snapshot = pd.DataFrame({"externalId": ["1"], "id": ["uuid-1"]})

    assert _snapshot_handler(tmp_path, snapshot)._prepare_account_lookup() is None


def test_contact_account_id_resolves_known_company_and_nulls_the_rest():
    contacts = pd.DataFrame(
        {
            "id": ["501", "502", "503"],
            "associatedcompanyid": ["233854320339", "999999999999", None],
        }
    )
    lookup = {"233854320339": "18a17f06-8452-4a31-9e12-8a8dc509d0ff"}

    result = _handler()._resolve_contact_account_ids(contacts, lookup)

    assert result.loc[0, "accountId"] == "18a17f06-8452-4a31-9e12-8a8dc509d0ff"
    assert pd.isna(result.loc[1, "accountId"])
    assert pd.isna(result.loc[2, "accountId"])
    # Kept so the backend can resolve the account itself on a lookup miss (CB-12898).
    assert list(result["associatedcompanyid"][:2]) == ["233854320339", "999999999999"]


def test_contact_account_id_resolves_an_integer_company_id():
    contacts = pd.DataFrame({"id": ["501"], "associatedcompanyid": [233854320339]})

    result = _handler()._resolve_contact_account_ids(contacts, {"233854320339": "uuid-1"})

    assert result.loc[0, "accountId"] == "uuid-1"


@pytest.mark.parametrize("lookup", [None, {}])
def test_contact_account_id_is_null_when_no_lookup_is_available(lookup):
    contacts = pd.DataFrame({"id": ["501"], "associatedcompanyid": ["233854320339"]})

    result = _handler()._resolve_contact_account_ids(contacts, lookup)

    assert result.loc[0, "accountId"] is None


def test_contact_account_id_is_null_without_associatedcompanyid_column():
    contacts = pd.DataFrame({"id": ["501"], "email": ["ada@acme.com"]})

    result = _handler()._resolve_contact_account_ids(contacts, {"233854320339": "uuid-1"})

    assert result.loc[0, "accountId"] is None


def test_handle_read_emits_resolved_account_ids_on_contacts(tmp_path):
    records, _ = _run_read(
        tmp_path,
        {"contacts": CONTACTS},
        mapping=READ_MAPPING,
        accounts_snapshot=ACCOUNTS_SNAPSHOT,
    )

    by_id = {r["sourceRecordId"]: r["data"] for r in records["contacts"]}
    assert by_id["501"]["accountId"] == "18a17f06-8452-4a31-9e12-8a8dc509d0ff"
    assert by_id["502"]["accountId"] is None
    assert by_id["503"]["accountId"] is None
    assert by_id["502"]["associatedcompanyid"] == "999999999999"


def test_handle_read_emits_null_account_ids_when_no_snapshot_exists(tmp_path):
    records, _ = _run_read(tmp_path, {"contacts": CONTACTS}, mapping=READ_MAPPING)

    assert records["contacts"], "contacts must still flow without a snapshot"
    assert all(r["data"]["accountId"] is None for r in records["contacts"])


# ---------------------------------------------------------------------------
# 4. Flow with no tenant mapping: only pass-through streams are processed
# ---------------------------------------------------------------------------

def test_handle_read_without_mapping_skips_mapped_streams_but_passes_through_the_rest(
    tmp_path, caplog
):
    streams = {
        "companies": COMPANIES,
        "contacts": CONTACTS,
        "deals": DEALS,
        "forms": FORMS,
        "associations_deals_companies": DEAL_COMPANY_EDGES,
    }

    with caplog.at_level(logging.WARNING, logger="hubspot_handler"):
        records, order = _run_read(tmp_path, streams, mapping=None)

    # Pass-through streams keep their connector names (nothing to invert) and are
    # emitted in READ_STREAM_ORDER.
    assert order == ["forms", "associations_deals_companies"]
    assert set(records) == {"forms", "associations_deals_companies"}

    edge = records["associations_deals_companies"][0]
    assert edge["lookupKey"] == "700:233854320339"
    assert edge["data"]["isPrimary"] is True
    assert records["forms"][0]["lookupKey"] == FORMS.iloc[0]["id"]

    skipped = [
        r.getMessage() for r in caplog.records if r.getMessage().startswith("Skipping mapped stream")
    ]
    assert skipped == [
        "Skipping mapped stream companies: no mapping for flow",
        "Skipping mapped stream contacts: no mapping for flow",
        "Skipping mapped stream deals: no mapping for flow",
    ]


def test_handle_read_with_mapping_emits_every_stream_under_its_target_name(tmp_path, caplog):
    """Control for the no-mapping case: the same input with a mapping emits the CRM
    streams too, renamed to their target stream and in dependency order."""
    streams = {
        "companies": COMPANIES,
        "contacts": CONTACTS,
        "deals": DEALS,
        "associations_deals_companies": DEAL_COMPANY_EDGES,
    }

    with caplog.at_level(logging.WARNING, logger="hubspot_handler"):
        records, order = _run_read(
            tmp_path, streams, mapping=READ_MAPPING, accounts_snapshot=ACCOUNTS_SNAPSHOT
        )

    assert order == ["accounts", "contacts", "deals", "deal_company_links"]
    assert not any("Skipping mapped stream" in r.getMessage() for r in caplog.records)


def test_handle_read_with_no_input_streams_writes_nothing(tmp_path):
    records, order = _run_read(tmp_path, {}, mapping=READ_MAPPING)

    assert records == {} and order == []
    assert not (tmp_path / "etl-output" / "data.singer").exists()
