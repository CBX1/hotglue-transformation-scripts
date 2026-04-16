#!/usr/bin/env python
# coding: utf-8

"""HubSpot Custom Event Sync ETL.

Reads batch event records from cbx1-tap event streams and outputs them
for the HubSpot Custom Events batch API (POST /events/v3/send/batch).

Input format (from cbx1-tap, one Singer RECORD per page/batch):
    {
        "inputs": [
            {"eventName": "diff_send", "email": "...", "objectId": "...", "occurredAt": "...", "uuid": "...", "properties": {...}},
            ...
        ],
        "uuid": "<last_event_uuid>",
        "occurredAt": "<last_event_timestamp>"
    }

Output format (for HubSpot target connector, stream: /events/v3/send/batch):
    Same structure — each record is a batch payload ready for HubSpot's bulk API.
"""

import ast
import json
import logging
import os
from typing import Optional

import gluestick as gs
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ROOT_DIR = os.environ.get("ROOT_DIR", ".")
INPUT_DIR = f"{ROOT_DIR}/sync-output"
SNAPSHOT_DIR = f"{ROOT_DIR}/snapshots"
OUTPUT_DIR = f"{ROOT_DIR}/etl-output"

# Event streams that this ETL processes
SUPPORTED_STREAMS = {"email_events", "page_events"}

OUTPUT_STREAM = "/events/v3/send/batch"


def _load_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _read_sent_uuids(stream_name: str, flow_id: str) -> set:
    """Read previously sent event UUIDs from snapshot."""
    sent_data = gs.read_snapshots(f"{stream_name}_{flow_id}", SNAPSHOT_DIR)
    if sent_data is None or sent_data.empty:
        return set()
    if "InputId" in sent_data.columns:
        return set(sent_data["InputId"].tolist())
    return set()


def _save_sent_uuids(stream_name: str, flow_id: str, uuids: list) -> None:
    """Save sent event UUIDs to snapshot for deduplication."""
    if not uuids:
        return
    df = pd.DataFrame({"InputId": uuids})
    # Append to existing snapshot
    existing = gs.read_snapshots(f"{stream_name}_{flow_id}", SNAPSHOT_DIR)
    if existing is not None and not existing.empty:
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates(subset=["InputId"])
    snapshot_path = f"{SNAPSHOT_DIR}/{stream_name}_{flow_id}.snapshot.csv"
    df.to_csv(snapshot_path, index=False)
    logger.info(f"Saved {len(uuids)} UUIDs to snapshot for {stream_name}")


def _handle_write_job(reader: gs.Reader, flow_id: str) -> None:
    """Process batch event records and output for HubSpot bulk API."""
    streams = ast.literal_eval(str(reader))

    for stream_name in streams:
        if stream_name not in SUPPORTED_STREAMS:
            logger.info(f"Skipping unsupported stream: {stream_name}")
            continue

        logger.info(f"Processing event stream: {stream_name}")

        stream_data = reader.get(stream_name)
        if stream_data is None or stream_data.empty:
            logger.info(f"No data for stream: {stream_name}")
            continue

        # Load previously sent UUIDs for deduplication
        sent_uuids = _read_sent_uuids(stream_name, flow_id)
        logger.info(f"Loaded {len(sent_uuids)} previously sent UUIDs for {stream_name}")

        output_batches = []
        new_uuids = []
        total_events = 0
        skipped_events = 0

        for _, row in stream_data.iterrows():
            inputs = row.get("inputs")
            if inputs is None:
                continue

            # inputs is a list of event dicts (already in HubSpot format)
            if isinstance(inputs, str):
                try:
                    inputs = json.loads(inputs)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse inputs as JSON, skipping batch")
                    continue

            if not isinstance(inputs, list):
                logger.warning(f"Expected inputs to be a list, got {type(inputs)}, skipping")
                continue

            # Filter out already-sent events
            new_events = []
            for event in inputs:
                event_uuid = event.get("uuid", "")
                if event_uuid and event_uuid in sent_uuids:
                    skipped_events += 1
                    continue
                new_events.append(event)
                if event_uuid:
                    new_uuids.append(event_uuid)

            if not new_events:
                continue

            total_events += len(new_events)

            # Output batch record — ready for HubSpot POST /events/v3/send/batch
            output_batches.append({"inputs": new_events})

        if not output_batches:
            logger.info(f"No new events for {stream_name} (skipped {skipped_events} already-sent)")
            continue

        logger.info(
            f"Outputting {len(output_batches)} batches with {total_events} events "
            f"for {stream_name} (skipped {skipped_events} already-sent)"
        )

        # Write as Singer output
        output_df = pd.DataFrame(output_batches)
        gs.to_singer(output_df, OUTPUT_STREAM, OUTPUT_DIR, allow_objects=True)

        # Save new UUIDs to snapshot
        _save_sent_uuids(stream_name, flow_id, new_uuids)


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
