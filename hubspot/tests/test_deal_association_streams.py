#!/usr/bin/env python
# coding: utf-8

"""Tests for the HubSpot deal + association read path.

Fixtures are verbatim payloads from a real tap-hubspot sync, because two of the
behaviours here are impossible to guess from the API docs:

  * ``associationTypes`` arrives as a JSON-encoded *string*, not a nested object.
  * The primacy / role signal lives inside that list. The top-level ``typeId`` and
    ``label`` mirror ``associationTypes[0]`` — the base HUBSPOT_DEFINED type, whose
    label is always null — so reading them marks every edge non-primary.

Run standalone:  python hubspot/tests/test_deal_association_streams.py
Run via pytest:  pytest hubspot/tests/test_deal_association_streams.py
"""

import os
import sys

import numpy as np
import pandas as pd

# Make the connector modules (hubspot_handler.py lives at hubspot/) importable whether
# the tests are run from the repo root or from hubspot/.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hubspot_handler import HubSpotHandler  # noqa: E402

COMPANY_EDGE = {
    "from_id": "339564136164",
    "to_id": "239697885890",
    "typeId": 341,
    "category": "HUBSPOT_DEFINED",
    "label": None,
    "associationTypes": (
        '[{"category": "HUBSPOT_DEFINED", "typeId": 341, "label": null,'
        ' "fromObjectTypeId": null, "toObjectTypeId": null},'
        ' {"category": "HUBSPOT_DEFINED", "typeId": 5, "label": "Primary",'
        ' "fromObjectTypeId": null, "toObjectTypeId": null}]'
    ),
}

CONTACT_EDGE = {
    "from_id": "339564136164",
    "to_id": "356855121603",
    "typeId": 3,
    "category": "HUBSPOT_DEFINED",
    "label": None,
    "associationTypes": (
        '[{"category": "HUBSPOT_DEFINED", "typeId": 3, "label": null,'
        ' "fromObjectTypeId": null, "toObjectTypeId": null},'
        ' {"category": "USER_DEFINED", "typeId": 1, "label": "contacts",'
        ' "fromObjectTypeId": null, "toObjectTypeId": null}]'
    ),
}


def _handler() -> HubSpotHandler:
    """A handler with no reader/mapping — the methods under test only touch the frame."""
    handler = HubSpotHandler.__new__(HubSpotHandler)
    handler.stream_name_mapping = {}
    return handler


def test_read_order_puts_edges_after_their_endpoints():
    order = HubSpotHandler.READ_STREAM_ORDER
    assert order.index("companies") < order.index("deals")
    assert order.index("contacts") < order.index("deals")
    for edge_stream in HubSpotHandler.ASSOCIATION_STREAMS:
        assert order.index("deals") < order.index(edge_stream)


def test_read_order_is_not_alphabetical():
    """Guard against someone replacing the tuple with a filtered set.

    Sorting alphabetically puts associations_* first, which would ingest every link
    with an unresolved accountId / contactId / dealId.
    """
    assert list(HubSpotHandler.READ_STREAM_ORDER) != sorted(HubSpotHandler.READ_STREAM_ORDER)


def test_lookup_key_is_the_from_to_composite():
    df = _handler()._build_association_lookup_key(pd.DataFrame([COMPANY_EDGE, CONTACT_EDGE]))
    assert list(df["lookupKey"]) == [
        "339564136164:239697885890",
        "339564136164:356855121603",
    ]


def test_lookup_key_is_null_when_an_endpoint_is_missing():
    """A half-edge must not get a usable key — cbx1-target skips null lookupKeys."""
    broken = dict(COMPANY_EDGE, to_id=None)
    df = _handler()._build_association_lookup_key(pd.DataFrame([broken]))
    assert pd.isna(df["lookupKey"].iloc[0])


def test_company_edge_is_primary_with_no_role():
    df = _handler()._derive_association_flags(pd.DataFrame([COMPANY_EDGE]))
    assert bool(df["isPrimary"].iloc[0]) is True
    assert df["roleLabel"].iloc[0] is None


def test_contact_edge_carries_the_user_defined_label_and_is_not_primary():
    df = _handler()._derive_association_flags(pd.DataFrame([CONTACT_EDGE]))
    assert bool(df["isPrimary"].iloc[0]) is False
    assert df["roleLabel"].iloc[0] == "contacts"


def test_top_level_label_is_never_the_signal():
    """Both real edges have a null top-level label, yet the company edge IS primary.

    This is the bug the derivation exists to avoid: keying off ``label`` would mark
    every edge non-primary and silently break account roll-up.
    """
    for edge in (COMPANY_EDGE, CONTACT_EDGE):
        assert edge["label"] is None
    company = _handler()._derive_association_flags(pd.DataFrame([COMPANY_EDGE]))
    assert bool(company["isPrimary"].iloc[0]) is True


def test_flags_key_off_category_and_label_not_type_id():
    """Same labels under different numeric typeIds must behave identically.

    HubSpot's type ids are not guaranteed identical across portals, so the rule must
    not depend on 341 / 5 / 3 / 1.
    """
    remapped = dict(
        COMPANY_EDGE,
        typeId=999,
        associationTypes=(
            '[{"category": "HUBSPOT_DEFINED", "typeId": 998, "label": null},'
            ' {"category": "HUBSPOT_DEFINED", "typeId": 997, "label": "Primary"}]'
        ),
    )
    df = _handler()._derive_association_flags(pd.DataFrame([remapped]))
    assert bool(df["isPrimary"].iloc[0]) is True


def test_malformed_association_types_defaults_without_raising():
    """One bad edge must not fail a tenant's whole sync."""
    rows = [
        dict(COMPANY_EDGE, associationTypes="{not json"),
        dict(CONTACT_EDGE, associationTypes=None),
        dict(CONTACT_EDGE, associationTypes=""),
        dict(CONTACT_EDGE, associationTypes='{"category": "USER_DEFINED"}'),
    ]
    df = _handler()._derive_association_flags(pd.DataFrame(rows))
    assert list(df["isPrimary"]) == [False, False, False, False]
    assert df["roleLabel"].isna().all()


def test_missing_association_types_column_defaults():
    stripped = {k: v for k, v in COMPANY_EDGE.items() if k != "associationTypes"}
    df = _handler()._derive_association_flags(pd.DataFrame([stripped]))
    assert bool(df["isPrimary"].iloc[0]) is False
    assert df["roleLabel"].iloc[0] is None


def test_association_records_wrap_on_the_composite_key():
    """Edges have no ``id`` column, so wrapping must key on lookupKey for both fields."""
    handler = _handler()
    df = handler._build_association_lookup_key(pd.DataFrame([COMPANY_EDGE]))
    df = handler._derive_association_flags(df)
    wrapped = handler._wrap_records_with_metadata(df, "associations_deals_companies")

    assert len(wrapped) == 1
    row = wrapped.iloc[0]
    assert row["lookupKey"] == "339564136164:239697885890"
    assert row["sourceRecordId"] == "339564136164:239697885890"
    assert row["source"] == "HUBSPOT"
    assert row["data"]["isPrimary"] is True
    # Raw HubSpot names are forwarded as-is; the CBX1 rename happens in the backend.
    assert row["data"]["from_id"] == "339564136164"
    assert row["data"]["to_id"] == "239697885890"
    assert row["data"]["associationTypes"] == COMPANY_EDGE["associationTypes"]


def test_deal_records_wrap_on_their_own_id():
    deal = {"id": "339564136164", "dealname": "vinay_bss", "archived": False}
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([deal]), "deals")

    assert len(wrapped) == 1
    row = wrapped.iloc[0]
    assert row["lookupKey"] == "339564136164"
    assert row["sourceRecordId"] == "339564136164"
    assert row["data"]["dealname"] == "vinay_bss"


def test_archived_deals_survive_wrapping():
    """isDeleted is mirrored from ``archived``; the row must not be filtered out."""
    deal = {"id": "1", "dealname": "gone", "archived": True}
    wrapped = _handler()._wrap_records_with_metadata(pd.DataFrame([deal]), "deals")
    assert len(wrapped) == 1
    assert wrapped.iloc[0]["data"]["archived"] is True


def test_deals_are_in_the_list_membership_gate():
    # Deal inherits crmListMembershipDetails from BaseTargetEntity on the backend, same as
    # AccountV2/ContactV2 — leaving deals out of this set would silently keep the field null.
    assert HubSpotHandler.LIST_MEMBERSHIP_STREAMS == {"contacts", "companies", "deals"}


def test_deal_list_memberships_resolve_to_names():
    deal = {
        "id": "339564136164",
        "dealname": "vinay_bss",
        "_hg_list_memberships": ["101", "202"],
    }
    list_lookup = {"101": "Enterprise Pipeline", "202": "Q3 Renewals"}

    result = _handler()._populate_list_memberships(pd.DataFrame([deal]), "deals", list_lookup)

    details = result.iloc[0]["crmListMembershipDetails"]
    assert details == [
        {"id": "101", "name": "Enterprise Pipeline"},
        {"id": "202", "name": "Q3 Renewals"},
    ]


def test_deal_list_memberships_resolve_from_a_numpy_array():
    # Regression: a parquet-sourced multi-value column commonly loads as a numpy array, not
    # a plain Python list. pd.isna() on an array-like raises "truth value of an array is
    # ambiguous" if called unconditionally — this must not crash the whole chunk.
    deal = {
        "id": "339564136164",
        "dealname": "vinay_bss",
        "_hg_list_memberships": np.array(["101"]),
    }

    result = _handler()._populate_list_memberships(pd.DataFrame([deal]), "deals", {"101": "Enterprise Pipeline"})

    assert result.iloc[0]["crmListMembershipDetails"] == [{"id": "101", "name": "Enterprise Pipeline"}]


def test_deal_without_list_memberships_column_gets_a_null_field_not_an_error():
    deal = {"id": "339564136164", "dealname": "vinay_bss"}

    result = _handler()._populate_list_memberships(pd.DataFrame([deal]), "deals", {})

    assert result.iloc[0]["crmListMembershipDetails"] is None


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} tests passed.")
