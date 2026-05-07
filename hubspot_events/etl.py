#!/usr/bin/env python
# coding: utf-8

"""HubSpot Custom Event Sync ETL.

Reads batch event records from cbx1-tap parquet files (via gluestick Reader)
and outputs them for the HubSpot Custom Events batch API.

Input: sync-output/email_events-<timestamp>.parquet
       sync-output/page_events-<timestamp>.parquet

Each row in the parquet is a batch record with columns:
    - inputs: list of event dicts (HubSpot format) or JSON string
    - uuid: last event UUID (for replication tracking)
    - occurredAt: last event timestamp (for replication tracking)

Output: etl-output/data.singer
    Stream: /events/v3/send/batch
    Each RECORD has {"inputs": [{event1}, {event2}, ...]}
    Ready for HubSpot POST /events/v3/send/batch
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

SUPPORTED_STREAMS = {"email_events", "page_events"}

OUTPUT_STREAM = "/events/v3/send/batch"


def _parse_inputs(raw) -> list:
    """Parse the inputs column which may be a list, JSON string, or ast-parseable string."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        try:
            parsed = ast.literal_eval(raw)
            if isinstance(parsed, list):
                return parsed
        except (ValueError, SyntaxError):
            pass
        logger.warning("Failed to parse inputs column, skipping batch")
    return []


def _read_sent_uuids(stream_name: str, flow_id: str) -> set:
    """Read previously sent event UUIDs from snapshot."""
    snapshot_path = f"{SNAPSHOT_DIR}/{stream_name}_{flow_id}.snapshot.csv"
    if not os.path.exists(snapshot_path):
        return set()
    try:
        df = pd.read_csv(snapshot_path)
        if "InputId" in df.columns:
            return set(df["InputId"].dropna().astype(str).tolist())
    except Exception as e:
        logger.warning(f"Failed to read snapshot {snapshot_path}: {e}")
    return set()


def _save_sent_uuids(stream_name: str, flow_id: str, new_uuids: list) -> None:
    """Append sent event UUIDs to snapshot for deduplication."""
    if not new_uuids:
        return
    snapshot_path = f"{SNAPSHOT_DIR}/{stream_name}_{flow_id}.snapshot.csv"
    new_df = pd.DataFrame({"InputId": new_uuids})

    if os.path.exists(snapshot_path):
        try:
            existing = pd.read_csv(snapshot_path)
            new_df = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(subset=["InputId"])
        except Exception:
            pass

    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    new_df.to_csv(snapshot_path, index=False)
    logger.info(f"Saved {len(new_uuids)} new UUIDs to snapshot for {stream_name} (total: {len(new_df)})")


def _process_stream(stream_name: str, stream_data: pd.DataFrame, flow_id: str) -> None:
    """Process a single event stream — deduplicate and output batch records."""
    sent_uuids = _read_sent_uuids(stream_name, flow_id)
    logger.info(f"Loaded {len(sent_uuids)} previously sent UUIDs for {stream_name}")

    output_batches = []
    new_uuids = []
    total_events = 0
    skipped_events = 0

    for _, row in stream_data.iterrows():
        inputs = _parse_inputs(row.get("inputs"))
        if not inputs:
            continue

        # Filter out already-sent events by uuid
        new_events = []
        for event in inputs:
            if not isinstance(event, dict):
                continue
            event_uuid = str(event.get("uuid", ""))
            if event_uuid and event_uuid in sent_uuids:
                skipped_events += 1
                continue
            new_events.append(event)
            if event_uuid:
                new_uuids.append(event_uuid)

        if not new_events:
            continue

        total_events += len(new_events)
        output_batches.append({"inputs": new_events})

    if not output_batches:
        logger.info(f"No new events for {stream_name} (skipped {skipped_events} already-sent)")
        return

    logger.info(
        f"Outputting {len(output_batches)} batch(es) with {total_events} events "
        f"for {stream_name} (skipped {skipped_events} already-sent)"
    )

    # Write as Singer output — stream name is the HubSpot API path
    output_df = pd.DataFrame(output_batches)
    gs.to_singer(output_df, OUTPUT_STREAM, OUTPUT_DIR, allow_objects=True)

    # Save new UUIDs to snapshot for next run
    _save_sent_uuids(stream_name, flow_id, new_uuids)


def _handle_write_job(reader: gs.Reader, flow_id: str) -> None:
    """Process all event streams from parquet input."""
    streams = ast.literal_eval(str(reader))
    logger.info(f"Available streams in sync-output: {streams}")

    for stream_name in streams:
        if stream_name not in SUPPORTED_STREAMS:
            logger.info(f"Skipping unsupported stream: {stream_name}")
            continue

        logger.info(f"Processing event stream: {stream_name}")

        stream_data = reader.get(stream_name)
        if stream_data is None or stream_data.empty:
            logger.info(f"No data for stream: {stream_name}")
            continue

        logger.info(f"Read {len(stream_data)} batch record(s) from {stream_name} parquet")
        _process_stream(stream_name, stream_data, flow_id)


def main() -> None:
    flow_id = os.environ.get("FLOW", "")
    job_type = os.environ.get("JOB_TYPE", "write")

    logger.info(f"Running HubSpot event sync ETL - job_type={job_type}, flow_id={flow_id}")
    logger.info(f"Input dir: {INPUT_DIR}, Output dir: {OUTPUT_DIR}, Snapshot dir: {SNAPSHOT_DIR}")

    if job_type != "write":
        logger.warning("Read jobs not supported for event sync ETL (events are one-way to HubSpot)")
        return

    reader = gs.Reader(INPUT_DIR)
    _handle_write_job(reader, flow_id)

    logger.info("ETL complete")


if __name__ == "__main__":
    main()
