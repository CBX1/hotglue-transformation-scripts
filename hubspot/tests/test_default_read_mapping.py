#!/usr/bin/env python
# coding: utf-8

"""Tests for the default read stream mapping on the CRM sync flow.

A tenant onboarded without a hotglue_mapping entry must still have its HubSpot
companies and contacts read into CBX1, emitted under the target stream names.

Run via pytest:  pytest hubspot/tests/test_default_read_mapping.py
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hubspot_handler as hh  # noqa: E402
from hubspot_handler import HubSpotHandler  # noqa: E402


def _run_read(flow_id, mapping_for_flow, stream_name_mapping, chunks):
    handler = HubSpotHandler.__new__(HubSpotHandler)
    handler.mapping_for_flow = mapping_for_flow
    handler.stream_name_mapping = stream_name_mapping
    handler.flow_id = flow_id
    handler.input_dir = "unused"
    handler.output_dir = "unused"
    handler.list_available_streams = lambda: sorted(chunks)
    handler._prepare_owner_lookup = lambda streams: None
    handler._prepare_list_lookup = lambda streams: None
    handler._prepare_account_lookup = lambda: None

    written = []
    orig_iter, orig_append = hh.iter_stream_chunks, hh.append_singer_records
    hh.iter_stream_chunks = lambda input_dir, stream: iter([chunks[stream].copy()])
    hh.append_singer_records = (
        lambda df, stream, output_dir, first: written.append((stream, len(df)))
    )
    try:
        handler.handle_read()
    finally:
        hh.iter_stream_chunks, hh.append_singer_records = orig_iter, orig_append
    return written


def _crm_chunks():
    return {
        "companies": pd.DataFrame([{"id": "10", "name": "Acme", "domain": "acme.com"}]),
        "contacts": pd.DataFrame([{"id": "1", "email": "a@acme.com"}]),
    }


def test_crm_sync_flow_without_mapping_uses_default_stream_names():
    written = _run_read("AJ3x0LMYI", {}, {}, _crm_chunks())

    assert written == [("accounts", 1), ("contacts", 1)]


def test_crm_sync_flow_with_none_mapping_uses_default_stream_names():
    written = _run_read("AJ3x0LMYI", None, None, _crm_chunks())

    assert written == [("accounts", 1), ("contacts", 1)]


def test_tenant_mapping_takes_precedence_over_default():
    mapping = {"companies_out/companies": {}, "contacts/contacts": {}}
    stream_names = {"companies_out": "companies", "contacts": "contacts"}

    written = _run_read("AJ3x0LMYI", mapping, stream_names, _crm_chunks())

    assert written == [("companies_out", 1), ("contacts", 1)]


def test_other_flow_without_mapping_still_skips_crm_streams():
    written = _run_read("vUZid8TMR", {}, {}, _crm_chunks())

    assert written == []
