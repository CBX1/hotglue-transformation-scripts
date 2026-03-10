#!/usr/bin/env python
# coding: utf-8

"""
Marketo-specific ETL handler.

This module implements the Marketo-specific logic for both read and write operations.
Marketo's primary person entity is the "Lead", which maps to our CONTACT entity.

Key differences from HubSpot:
- Marketo uses "leads" as the stream name (not "contacts")
- No separate unsubscribe API (uses `unsubscribed` field directly on the lead)
- No owner enrichment or list membership enrichment needed
- Companies stream is not available in the standard Singer tap-marketo
"""

import logging
from typing import Dict, Optional, Set

import pandas as pd
import numpy as np
import gluestick as gs

from base_handler import BaseETLHandler
from utils import (
    map_stream_data,
    drop_sent_records,
)

logger = logging.getLogger(__name__)


class MarketoHandler(BaseETLHandler):
    """
    Handler for Marketo ETL operations.

    This handler manages the specific requirements for Marketo data transformations.
    It follows the HubSpot pattern (metadata wrapping for reads, thin pipe for writes)
    since backend owns field mappings (per PR #3308 architecture).
    """

    # Marketo read-only / system / inferred fields that must not be written back
    READ_ONLY_FIELDS: Set[str] = {
        "createdAt",
        "updatedAt",
        "isAnonymous",
        "inferredCompany",
        "inferredCountry",
        "inferredCity",
        "inferredMetropolitanArea",
        "inferredPhoneAreaCode",
        "inferredPostalCode",
        "inferredStateRegion",
        "anonymousIP",
    }

    def __init__(self, *args, target_config: Optional[Dict] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_config = target_config or {}
        self.connector_id = "marketo"

    def handle_write(self) -> None:
        """
        Handle the write operation for Marketo.

        Processes contacts stream and writes as leads to Marketo.
        Companies are not supported in write (no companies stream in tap-marketo).
        """
        if not self.mapping_for_flow:
            raise ValueError("No write mapping found for Marketo flow")

        mapping = self.build_write_mapping()
        streams = self.list_available_streams()

        logger.info(f"Marketo write: evaluating {len(streams)} streams; only 'contacts' will be written as leads")

        for stream in streams:
            if stream == "contacts":
                self._handle_contacts_write(mapping)
            else:
                logger.info(f"Marketo write: skipping stream '{stream}' by policy")

    def _handle_contacts_write(self, mapping: Dict) -> None:
        """
        Handle contacts write for Marketo.

        Processes contacts data, strips Marketo read-only fields,
        deduplicates against snapshots, and outputs in Singer format.
        The output stream name is determined by stream_name_mapping
        (typically maps contacts -> leads for Marketo).
        """
        logger.info("Processing contacts for Marketo")

        contacts_df_raw = self.get_stream_data("contacts")

        mapping_name = "contacts"
        stream_columns, contacts_df = map_stream_data(contacts_df_raw, mapping_name, mapping)

        # Remove Marketo read-only fields
        stream_columns = [col for col in stream_columns if col not in self.READ_ONLY_FIELDS]
        contacts_df = contacts_df.drop(columns=list(self.READ_ONLY_FIELDS), errors="ignore")

        new_data = contacts_df.copy()

        # Deduplicate columns
        ordered_columns = []
        for col in stream_columns:
            if col not in ordered_columns:
                ordered_columns.append(col)

        # Remove account-related fields (not applicable for Marketo leads)
        account_fields = {"AccountId", "accountId", "InputAccountId", "RemoteAccountId"}
        ignored_fields = account_fields.union(self.READ_ONLY_FIELDS)
        output_columns = [col for col in ordered_columns if col not in ignored_fields]
        df_out = contacts_df[output_columns].copy()

        # Filter out already sent records
        snapshot_name = f"{self.stream_name_mapping[mapping_name]}_{self.flow_id}"
        sent_contacts = gs.read_snapshots(snapshot_name, self.snapshot_dir)
        df_out = drop_sent_records("contacts", df_out, sent_contacts, new_data)

        # Write to output (stream_name_mapping maps contacts -> leads for Marketo)
        self.write_to_singer(df_out, self.stream_name_mapping[mapping_name])
        logger.info(f"Processed {len(df_out)} contact records for Marketo (as leads)")

    def handle_read(self) -> None:
        """
        Handle the read operation for Marketo data.

        Reads leads stream from Marketo, wraps records with metadata
        (source=MARKETO, lookupKey=email), and outputs in Singer format
        for backend ingestion.
        """
        logger.info("Starting Marketo read operation (passthrough + metadata wrapping)")

        data_streams = self.list_available_streams()
        if not data_streams or not self.mapping_for_flow:
            logger.warning("No streams or mapping available for read operation")
            return

        # Only process leads stream (Marketo's primary entity)
        target_streams = [s for s in data_streams if s in {"leads"}]

        for stream in target_streams:
            logger.info(f"Processing read stream: {stream}")
            stream_data = self.get_stream_data(stream)

            # Wrap records in metadata structure for backend ingestion API
            stream_data = self._wrap_records_with_metadata(stream_data, stream)

            # Determine output stream name
            inverse_mapping = {v: k for k, v in self.stream_name_mapping.items()}
            output_stream = inverse_mapping.get(stream, stream)

            self.write_to_singer(stream_data, output_stream)
            logger.info(f"Wrote {len(stream_data)} records for read stream: {stream}")

    def _wrap_records_with_metadata(self, df: pd.DataFrame, stream: str) -> pd.DataFrame:
        """
        Wrap each record in a structure with data, lookupKey, sourceRecordId, and source.

        Transforms each record to:
        {
            data: {field1: value1, ...},
            sourceRecordId: <id>,
            source: 'MARKETO',
            lookupKey: <email for leads>
        }

        Records with null lookupKey values are filtered out.
        """
        if df is None or df.empty:
            return df

        # Marketo leads use email as the lookup key
        if stream == "leads":
            lookup_field = "email"
        else:
            logger.warning(f"Unknown stream '{stream}' for record wrapping")
            return df

        if "id" not in df.columns:
            logger.warning(f"Missing 'id' field in {stream}, cannot wrap records")
            return df

        if lookup_field not in df.columns:
            logger.warning(f"Missing '{lookup_field}' field in {stream}, cannot wrap records")
            return df

        # Filter out records with null lookup key
        df = df.copy()
        initial_count = len(df)
        df = df[df[lookup_field].notna() & (df[lookup_field].astype(str).str.strip() != "")]
        filtered_count = initial_count - len(df)

        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} {stream} records with null or empty {lookup_field}")

        if df.empty:
            logger.warning(f"All {stream} records filtered out due to null {lookup_field}")
            return df

        # Replace pandas NaT and NA with None for JSON serialization
        df = df.replace({pd.NaT: None, pd.NA: None})

        records = df.to_dict(orient="records")

        wrapped_records = []
        for record in records:
            cleaned_record = self._clean_record_for_serialization(record)
            wrapped = {
                "data": cleaned_record,
                "sourceRecordId": record.get("id"),
                "source": "MARKETO",
                "lookupKey": record.get(lookup_field),
            }
            wrapped_records.append(wrapped)

        wrapped_df = pd.DataFrame(wrapped_records)

        logger.info(f"Wrapped {len(wrapped_df)} {stream} records with metadata structure and source='MARKETO'")
        return wrapped_df

    def _clean_record_for_serialization(self, obj):
        """
        Recursively clean a record by converting pandas NaT/NA and numpy types
        to JSON-serializable values.
        """
        if isinstance(obj, dict):
            return {k: self._clean_record_for_serialization(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_record_for_serialization(item) for item in obj]
        elif pd.isna(obj):
            return None
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        else:
            return obj
