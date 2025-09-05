#!/usr/bin/env python
# coding: utf-8

import ast
import json
import logging
import os
from typing import Dict, List, Optional, Tuple

import gluestick as gs
import numpy as np
import pandas as pd

from utils import (
    drop_sent_records,
    get_contact_data,
    map_stream_data,
    split_contacts_by_account,
)


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Standard directories for HotGlue
ROOT_DIR = os.environ.get("ROOT_DIR", ".")
INPUT_DIR = f"{ROOT_DIR}/sync-output"
SNAPSHOT_DIR = f"{ROOT_DIR}/snapshots"
OUTPUT_DIR = f"{ROOT_DIR}/etl-output"


def _load_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _load_tenant_mapping(flow_id: str) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Returns (mapping_for_flow, stream_name_mapping) or (None, None)
    mapping_for_flow is keyed by "target_api/connector" for each stream.
    stream_name_mapping maps target_api_stream -> connector_stream.
    """
    tenant_cfg = _load_json(f"{SNAPSHOT_DIR}/tenant-config.json")
    if not tenant_cfg:
        return None, None

    mapping_root = tenant_cfg.get("hotglue_mapping", {}).get("mapping", {})
    mapping_for_flow = mapping_root.get(flow_id)
    if not mapping_for_flow:
        return None, None

    stream_name_mapping = {
        s.split("/")[0]: s.split("/")[1] for s in mapping_for_flow.keys()
    }
    return mapping_for_flow, stream_name_mapping


def _load_tap_config() -> Dict:
    cfg = _load_json(f"{ROOT_DIR}/config.json")
    if not cfg:
        logger.info("No tap config found")
        return {}
    return cfg


def _list_streams(reader: gs.Reader) -> List[str]:
    streams = ast.literal_eval(str(reader))
    streams.sort()
    return streams


def _build_write_mapping(mapping_for_flow: Dict) -> Dict:
    """
    Convert mapping from "target/connector" entries into dict keyed by target.
    Example: {"contacts/Contact": {...}} -> {"contacts": {...}}
    """
    result = {}
    for k, v in mapping_for_flow.items():
        target = k.split("/")[0]
        result[target] = v
    return result


def _handle_standard_write_stream(
    reader: gs.Reader,
    stream: str,
    new_mapping: Dict,
    stream_name_mapping: Dict,
    flow_id: str,
) -> None:
    sent_data = gs.read_snapshots(f"{stream_name_mapping[stream]}_{flow_id}", SNAPSHOT_DIR)

    # Map and filter
    stream_data = reader.get(stream)
    stream_columns, stream_data = map_stream_data(stream_data, stream, new_mapping)
    stream_data = drop_sent_records(stream, stream_data, sent_data)

    gs.to_singer(
        stream_data,
        stream_name_mapping[stream],
        OUTPUT_DIR,
        allow_objects=True,
    )


def _handle_contacts_write_stream(
    reader: gs.Reader,
    connector_id: str,
    tap_config: Dict,
    new_mapping: Dict,
    stream_name_mapping: Dict,
    flow_id: str,
) -> None:
    # Sent contacts snapshot (used for dedupe later)
    sent_contacts = gs.read_snapshots(
        f"{stream_name_mapping['contacts']}_{flow_id}", SNAPSHOT_DIR
    )

    # Accounts must exist to link contacts
    sent_accounts = gs.read_snapshots(
        f"{stream_name_mapping['accounts']}_{flow_id}", SNAPSHOT_DIR
    )
    if sent_accounts is None:
        logger.info("No accounts have been sent yet, skipping contacts export.")
        return

    sent_accounts = sent_accounts.rename(
        columns={"InputId": "AccountId", "RemoteId": "RemoteAccountId"}
    )
    # Keep only accounts with a valid remote id
    sent_accounts = sent_accounts[sent_accounts["RemoteAccountId"].notna()]

    contacts_df = reader.get("contacts")
    leads_df = None

    # Optionally split into Contact/Lead for Salesforce
    if connector_id == "salesforce" and tap_config.get("dynamic_contact_mapping"):
        contacts_df, leads_df = split_contacts_by_account(contacts_df, sent_accounts)

    for sfdc_stream in ["Contact", "Lead"]:
        df = get_contact_data(connector_id, sfdc_stream, contacts_df, leads_df)
        if df is None or df.empty:
            continue

        # Find target mapping name for this connector stream
        inverse = {v: k for k, v in stream_name_mapping.items()}
        mapping_name = "contacts" if connector_id == "hubspot" else inverse.get(sfdc_stream)
        if not mapping_name:
            # No mapping defined for this stream in tenant config; skip gracefully
            continue

        stream_columns, df = map_stream_data(df, mapping_name, new_mapping)
        new_data = df.copy()

        # Snapshot contacts by externalId
        snap = gs.snapshot_records(df, mapping_name, SNAPSHOT_DIR, pk="externalId")
        if "hash" in snap.columns:
            snap = snap.drop(columns=["hash"])  # cleaner payload

        # For true Contacts (not Leads) merge in account remote ids
        if sfdc_stream != "Lead":
            snap = (
                snap.merge(sent_accounts, on="AccountId")
                .rename(columns={"AccountId": "InputAccountId", "RemoteAccountId": "AccountId"})
            )

        # HubSpot association formatting
        if connector_id == "hubspot":
            snap["AccountId"] = snap["AccountId"].apply(
                lambda x: [
                    {
                        "to": {"id": x},
                        "types": [
                            {
                                "associationCategory": "HUBSPOT_DEFINED",
                                "associationTypeId": 279,
                            }
                        ],
                    }
                ]
            )
            snap = snap.rename(columns={"AccountId": "associations"})
            if "AccountId" in stream_columns:
                stream_columns.remove("AccountId")
            stream_columns.append("associations")

        # Finalize payload and filter out already sent
        df_out = snap[list(set(stream_columns))]
        df_out = drop_sent_records("contacts", df_out, sent_contacts, new_data)

        gs.to_singer(
            df_out,
            stream_name_mapping[mapping_name],
            OUTPUT_DIR,
            allow_objects=True,
        )


def _handle_write_job(
    reader: gs.Reader,
    mapping_for_flow: Dict,
    stream_name_mapping: Dict,
    flow_id: str,
    connector_id: str,
    tap_config: Dict,
) -> None:
    if not mapping_for_flow:
        return

    new_mapping = _build_write_mapping(mapping_for_flow)
    streams = _list_streams(reader)

    for stream in streams:
        if not new_mapping.get(stream):
            # Pass-through streams without mapping
            df = reader.get(stream)
            gs.to_singer(df, stream_name_mapping.get(stream, stream), OUTPUT_DIR, allow_objects=True)
            continue

        if stream == "contacts":
            _handle_contacts_write_stream(reader, connector_id, tap_config, new_mapping, stream_name_mapping, flow_id)
        else:
            _handle_standard_write_stream(reader, stream, new_mapping, stream_name_mapping, flow_id)


def _build_read_mapping(mapping_for_flow: Dict) -> Dict:
    """
    Build mapping keyed by connector stream -> target stream mapping dict,
    and include remote_id mapping to "Id" without mutating input mapping.
    """
    new_mapping: Dict[str, Dict] = {}
    for k, v in mapping_for_flow.items():
        target, connector = k.split("/")
        m = dict(v)
        m["remote_id"] = "Id"
        new_mapping[connector] = m
    return new_mapping


def _inject_crm_system(df: pd.DataFrame, connector_id: str) -> pd.DataFrame:
    crm_system = (connector_id or "").upper()
    if crm_system not in ["SALESFORCE", "HUBSPOT"]:
        if str(connector_id).lower() == "salesforce":
            crm_system = "SALESFORCE"
        elif str(connector_id).lower() == "hubspot":
            crm_system = "HUBSPOT"
    df["crmSystem"] = crm_system
    return df


def _handle_read_job(
    reader: gs.Reader,
    mapping_for_flow: Dict,
    stream_name_mapping: Dict,
    flow_id: str,
    connector_id: str,
) -> None:
    data_streams = _list_streams(reader)
    if not data_streams or not mapping_for_flow:
        return

    new_mapping = _build_read_mapping(mapping_for_flow)

    for stream in data_streams:
        stream_data = reader.get(stream)
        output_stream = None

        if new_mapping.get(stream):
            connector_columns = list(new_mapping[stream].keys())
            target_api_columns = list(new_mapping[stream].values())
            columns_to_rename = [c for c in stream_data.columns if c in target_api_columns]
            gc = np.array(target_api_columns)

            # Rename columns to their target_api version
            for column in columns_to_rename:
                for ind in np.where(gc == column)[0]:
                    stream_data[connector_columns[ind]] = stream_data[column]

            cols = [c for c in connector_columns if c in stream_data.columns]
            if "remote_id" in stream_data.columns:
                cols.append("remote_id")
            stream_data = stream_data[list(set(cols))]

            # Determine output (target) stream name
            output_stream = list(stream_name_mapping.keys())[list(stream_name_mapping.values()).index(stream)]

            # Inject CBX1 id when available
            sent_data = gs.read_snapshots(f"{stream}_{flow_id}", SNAPSHOT_DIR)
            if sent_data is not None:
                sent_data = sent_data.rename(columns={"InputId": "id", "RemoteId": "remote_id"})
                stream_data = stream_data.merge(sent_data, how="left", on="remote_id")

            # Replace Salesforce AccountId with CBX1 account ID for contacts
            if output_stream == "contacts" and "accountId" in stream_data.columns:
                account_snapshots = gs.read_snapshots(f"Account_{flow_id}", SNAPSHOT_DIR)
                if account_snapshots is not None:
                    acc = account_snapshots.rename(
                        columns={"InputId": "cbx1_account_id", "RemoteId": "salesforce_account_id"}
                    ).copy()
                    # Normalize identifiers as strings
                    stream_data["accountId"] = stream_data["accountId"].astype(str)
                    acc["salesforce_account_id"] = acc["salesforce_account_id"].astype(str)
                    # Only exact match (do not fallback to SF id)
                    stream_data = stream_data.merge(
                        acc[["salesforce_account_id", "cbx1_account_id"]],
                        left_on="accountId",
                        right_on="salesforce_account_id",
                        how="left",
                    )
                    # Set accountId strictly to CBX1 id; leave null if no match
                    stream_data["accountId"] = stream_data["cbx1_account_id"]
                    stream_data = stream_data.drop(
                        columns=["salesforce_account_id", "cbx1_account_id"],
                        errors="ignore",
                    )

        # Inject CRM system + rename remote_id -> crmAssociationId
        stream_data = _inject_crm_system(stream_data, connector_id)
        stream_data = stream_data.rename(columns={"remote_id": "crmAssociationId"})

        stream_name = output_stream or stream
        gs.to_singer(stream_data, stream_name, OUTPUT_DIR, allow_objects=True)


def main() -> None:
    reader = gs.Reader(INPUT_DIR)

    job_type = os.environ.get("JOB_TYPE", "write")
    flow_id = os.environ.get("FLOW", "AJ3x0LMYI")
    connector_id = os.environ.get("CONNECTOR_ID")
    logger.info(f"Running job: {job_type}")

    mapping_for_flow, stream_name_mapping = _load_tenant_mapping(flow_id)
    tap_config = _load_tap_config()

    if job_type == "write":
        _handle_write_job(reader, mapping_for_flow or {}, stream_name_mapping or {}, flow_id, connector_id, tap_config)
    else:
        _handle_read_job(reader, mapping_for_flow or {}, stream_name_mapping or {}, flow_id, connector_id)


if __name__ == "__main__":
    main()
