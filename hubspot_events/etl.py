#!/usr/bin/env python
# coding: utf-8

"""HubSpot Custom Event Sync ETL - transforms CBX1 events to HubSpot custom events."""

import ast
import json
import logging
import os
from typing import Dict, List, Optional, Tuple

import gluestick as gs
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ROOT_DIR = os.environ.get("ROOT_DIR", ".")
INPUT_DIR = f"{ROOT_DIR}/sync-output"
SNAPSHOT_DIR = f"{ROOT_DIR}/snapshots"
OUTPUT_DIR = f"{ROOT_DIR}/etl-output"

# Event stream name -> HubSpot custom event name prefix
EVENT_STREAM_CONFIG = {
    "email_events": {
        "event_name_field": "event_action",  # open -> diff_email_open, click -> diff_email_click
        "event_name_prefix": "diff_email_",
        "properties": [
            "diff_email_id", "diff_email_subject", "diff_campaign_id",
            "diff_campaign_name", "diff_is_bot", "diff_bot_filtered",
            "diff_in_business_hours",
        ],
    },
    "form_events": {
        "event_name_static": "diff_form_fill",
        "properties": [
            "diff_form_id", "diff_form_name", "diff_page_url",
            "diff_is_leadgen_form", "diff_campaign_name",
            "diff_utm_source", "diff_utm_medium", "diff_utm_campaign",
        ],
    },
    "page_visits": {
        "event_name_static": "diff_page_visit",
        "properties": [
            "diff_page_url", "diff_page_title", "diff_referrer",
            "diff_session_id", "diff_action", "diff_device",
            "diff_browser", "diff_bot_filtered",
        ],
    },
}


def _load_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _load_tenant_config() -> dict:
    """Load tenant config from snapshots."""
    cfg = _load_json(f"{SNAPSHOT_DIR}/tenant-config.json")
    return cfg or {}


def _build_hubspot_event_name(stream_config: dict, record: dict) -> str:
    """Build the HubSpot custom event name for a record."""
    if "event_name_static" in stream_config:
        return stream_config["event_name_static"]
    # Dynamic: based on event_action field (e.g., "open" -> "diff_email_open")
    action = record.get(stream_config.get("event_name_field", "event_action"), "unknown")
    return f"{stream_config['event_name_prefix']}{action}"


def _transform_to_hubspot_event(record: dict, stream_config: dict) -> dict:
    """Transform a single egestion record to HubSpot custom event format."""
    event_name = _build_hubspot_event_name(stream_config, record)

    # Extract properties (only include fields that exist in the record)
    properties = {}
    for prop in stream_config["properties"]:
        if prop in record and record[prop] is not None:
            properties[prop] = str(record[prop])

    return {
        "eventName": event_name,
        "objectType": "contacts",
        "email": record.get("email", record.get("contact_email", "")),
        "occurredAt": record.get("occurredAt", record.get("occurred_at", "")),
        "properties": properties,
    }


def _drop_sent_records(stream: str, stream_data: pd.DataFrame, flow_id: str) -> pd.DataFrame:
    """Filter out records that have already been sent."""
    sent_data = gs.read_snapshots(f"{stream}_{flow_id}", SNAPSHOT_DIR)
    if sent_data is None or sent_data.empty:
        return stream_data
    if "externalId" not in stream_data.columns or "InputId" not in sent_data.columns:
        return stream_data
    return stream_data[~stream_data["externalId"].isin(sent_data["InputId"])]


def _handle_write_job(reader: gs.Reader, flow_id: str) -> None:
    """Process event streams and output HubSpot custom event Singer records."""
    streams = ast.literal_eval(str(reader))

    for stream_name in streams:
        if stream_name not in EVENT_STREAM_CONFIG:
            logger.info(f"Skipping unknown stream: {stream_name}")
            continue

        stream_config = EVENT_STREAM_CONFIG[stream_name]
        logger.info(f"Processing event stream: {stream_name}")

        # Read stream data
        stream_data = reader.get(stream_name)
        if stream_data is None or stream_data.empty:
            logger.info(f"No data for stream: {stream_name}")
            continue

        # Deduplicate against previously sent records
        stream_data = _drop_sent_records(stream_name, stream_data, flow_id)
        if stream_data.empty:
            logger.info(f"No new records for stream: {stream_name}")
            continue

        logger.info(f"Processing {len(stream_data)} new records for {stream_name}")

        # Transform to HubSpot custom event format
        events = []
        for _, record in stream_data.iterrows():
            event = _transform_to_hubspot_event(record.to_dict(), stream_config)
            events.append(event)

        # Convert to DataFrame for Singer output
        events_df = pd.DataFrame(events)

        # Create snapshot for deduplication
        if "externalId" in stream_data.columns:
            gs.snapshot_records(stream_data, stream_name, SNAPSHOT_DIR, pk="externalId")

        # Output as Singer RECORD messages
        # Output stream name includes "custom_events_" prefix for HubSpot target connector
        output_stream = f"custom_events_{stream_name}"
        gs.to_singer(events_df, output_stream, OUTPUT_DIR, allow_objects=True)

        logger.info(f"Wrote {len(events)} events for {output_stream}")


def main() -> None:
    reader = gs.Reader(INPUT_DIR)
    flow_id = os.environ.get("FLOW", "")
    job_type = os.environ.get("JOB_TYPE", "write")

    logger.info(f"Running HubSpot event sync ETL - job_type={job_type}, flow_id={flow_id}")

    if job_type == "write":
        _handle_write_job(reader, flow_id)
    else:
        logger.warning("Read jobs not supported for event sync ETL (events are one-way to HubSpot)")


if __name__ == "__main__":
    main()
