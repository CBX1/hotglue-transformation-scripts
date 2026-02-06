#!/usr/bin/env python
# coding: utf-8

"""
HubSpot-specific ETL handler.

This module implements the HubSpot-specific logic for both read and write operations.
Note: HubSpot associations are handled by HubSpot's own system based on configured rules,
so this handler does not include association logic.
"""

import logging
from typing import Dict, List, Optional, Set

import pandas as pd
import numpy as np
import gluestick as gs

from base_handler import BaseETLHandler
from utils import (
    map_stream_data,
    drop_sent_records,
    get_contact_data,
    transform_dot_notation_to_nested,
)

logger = logging.getLogger(__name__)


class HubSpotHandler(BaseETLHandler):
    """
    Handler for HubSpot ETL operations.
    
    This handler manages the specific requirements for HubSpot data transformations,
    including:
    - Owner information enrichment
    - HubSpot-specific field transformations
    - Data standardization for warehouse ingestion
    
    Note: HubSpot associations are managed by HubSpot itself based on configured rules,
    so this handler does not handle association logic.
    """
    
    # Fields that are read-only in HubSpot and should not be synced from Different to HubSpot
    READ_ONLY_FIELDS: Set[str] = {
        "hs_email_optout",
        "createdate",
        "notes_last_updated",
        "hs_last_sales_activity_timestamp",
        "notes_last_contacted",
        "hs_latest_source_timestamp",
        "hs_email_last_click_date",
        "crmListMembershipDetails",
    }

    def __init__(self, *args, target_config: Optional[Dict] = None, **kwargs):
        """
        Initialize the HubSpot handler.
        
        Args:
            *args: Arguments passed to parent class
            target_config: Target configuration containing HubSpot-specific settings
            **kwargs: Additional keyword arguments passed to parent class
        """
        super().__init__(*args, **kwargs)
        self.target_config = target_config or {}
        self.connector_id = "hubspot"
    
    def handle_write(self) -> None:
        """
        Handle the write operation for HubSpot.
        
        This method:
        1. Processes the contacts stream from the source
        2. Applies HubSpot-specific transformations
        3. Writes transformed contact data in HubSpot-compatible format
        
        Policy: Write jobs only push Contacts. Companies and other objects are not
        written from this pipeline. Association handling is delegated to HubSpot's
        internal systems.
        """
        if not self.mapping_for_flow:
            raise ValueError("No write mapping found for HubSpot flow")
        
        mapping = self.build_write_mapping()
        streams = self.list_available_streams()
        
        logger.info(f"HubSpot write: evaluating {len(streams)} streams; only 'contacts' will be written")
        
        for stream in streams:
            if stream == "contacts":
                # Contacts are the only object we write to HubSpot
                self._handle_contacts_write(mapping)
            else:
                logger.info(f"HubSpot write: skipping non-contact stream '{stream}' by policy")
    
    def _handle_passthrough_stream(self, stream: str) -> None:
        """
        Handle streams that don't have mapping (pass through as-is).
        
        Args:
            stream: Name of the stream to pass through
        """
        logger.info(f"Passing through unmapped stream: {stream}")
        df = self.get_stream_data(stream)
        output_stream = self.stream_name_mapping.get(stream, stream)
        self.write_to_singer(df, output_stream)
    
    def _handle_standard_stream_write(self, stream: str, mapping: Dict) -> None:
        """
        Handle standard stream write with mapping transformations.
        
        Args:
            stream: Name of the stream to process
            mapping: Field mapping configuration for transformation
        """
        logger.info(f"Processing standard stream: {stream}")
        
        # Read any previously sent data for deduplication
        sent_data = self.read_snapshot(self.stream_name_mapping[stream])
        
        # Get and transform the data
        stream_data = self.get_stream_data(stream)
        stream_columns, stream_data = map_stream_data(stream_data, stream, mapping)
        
        # Filter out already sent records
        stream_data = drop_sent_records(stream, stream_data, sent_data)
        
        # Write to output
        self.write_to_singer(stream_data, self.stream_name_mapping[stream])
    
    def _handle_contacts_write(self, mapping: Dict) -> None:
        """
        Handle contacts write for HubSpot.
        
        This method processes contacts without handling associations,
        as associations are managed by HubSpot's internal systems.
        
        Additionally, for contacts with globalUnsubscribe=true, this method
        calls the HubSpot communication preferences API to unsubscribe them
        from all email communications.
        
        Args:
            mapping: Field mapping configuration
        """
        logger.info("Processing contacts for HubSpot")
        
        # Get contact data
        contacts_df_raw = self.get_stream_data("contacts")

        
        # Apply mapping for contacts
        mapping_name = "contacts"
        stream_columns, contacts_df = map_stream_data(contacts_df_raw, mapping_name, mapping)

        # Remove HubSpot read-only fields so we never attempt to update them
        # Filter from stream_columns list
        stream_columns = [col for col in stream_columns if col not in self.READ_ONLY_FIELDS]
        # Also drop directly from DataFrame to catch any columns not tracked in stream_columns
        contacts_df = contacts_df.drop(columns=list(self.READ_ONLY_FIELDS), errors="ignore")

        new_data = contacts_df.copy()

        # Prepare final output (without association formatting)
        # Note: For WRITE operations, we don't create a merged snapshot to avoid
        # mixing READ (HubSpot->CBX1) and WRITE (CBX1->HubSpot) data.
        # The CSV snapshot tracks previously sent records correctly.
        ordered_columns = []
        for col in stream_columns:
            if col not in ordered_columns:
                ordered_columns.append(col)

        account_fields = {"AccountId", "accountId", "InputAccountId", "RemoteAccountId"}
        ignored_fields = account_fields.union(self.READ_ONLY_FIELDS)
        output_columns = [col for col in ordered_columns if col not in ignored_fields]
        df_out = contacts_df[output_columns].copy()

        # Filter out already sent records
        # Read the CSV snapshot directly (with flow_id suffix) to avoid loading
        # the parquet snapshot which may contain mixed READ/WRITE data
        import gluestick as gs
        snapshot_name = f"{self.stream_name_mapping[mapping_name]}_{self.flow_id}"
        sent_contacts = gs.read_snapshots(snapshot_name, self.snapshot_dir)
        df_out = drop_sent_records("contacts", df_out, sent_contacts, new_data)
        
        # Write to output
        self.write_to_singer(df_out, self.stream_name_mapping[mapping_name])
        logger.info(f"Processed {len(df_out)} contact records for HubSpot")
                
        # Call unsubscribe API for contacts with globalUnsubscribe=true
        # Only processes contacts where globalUnsubscribe is NOT pending approval or rejected
        # (CB-7840: CRM Sync Approval System gates critical field changes)
        self._handle_global_unsubscribe(contacts_df_raw)
    
    def _handle_global_unsubscribe(self, contacts_df: pd.DataFrame) -> None:
        """
        Handle global unsubscribe for contacts with globalUnsubscribe field set to true.
        
        This method identifies contacts that should be unsubscribed from all email
        communications and writes them to the HubSpot communication preferences API
        individually (not using batch operations, as batch requires Marketing Enterprise).
        
        Args:
            contacts_df: DataFrame containing contact data with potential globalUnsubscribe field
        """
        if contacts_df is None or contacts_df.empty:
            logger.info("No contacts to check for global unsubscribe")
            return
        
        # Check if globalUnsubscribe field exists
        if "globalUnsubscribe" not in contacts_df.columns:
            logger.info("No globalUnsubscribe field found in contacts data")
            return
        
        # Filter contacts with globalUnsubscribe=true
        unsubscribe_mask = contacts_df["globalUnsubscribe"].fillna(False)
        # Handle various truthy values (True, "true", "True", 1, "1")
        if unsubscribe_mask.dtype == "object":
            unsubscribe_mask = unsubscribe_mask.astype(str).str.strip().str.lower().isin(["true", "1"])
        else:
            unsubscribe_mask = unsubscribe_mask.astype(bool)
        
        contacts_to_unsubscribe = contacts_df[unsubscribe_mask].copy()

        # Filter out contacts where globalUnsubscribe is pending approval or was rejected
        # (CB-7840: CRM Sync Approval System gates critical field changes)
        contacts_to_unsubscribe = self._filter_pending_or_rejected_contacts(
            contacts_to_unsubscribe, "globalUnsubscribe"
        )

        if contacts_to_unsubscribe.empty:
            logger.info("No contacts with globalUnsubscribe=true found")
            return
        
        # Extract email addresses for unsubscribe API call
        if "email" not in contacts_to_unsubscribe.columns:
            logger.warning(
                f"Found {len(contacts_to_unsubscribe)} contacts with globalUnsubscribe=true "
                "but no email field available. Cannot process unsubscribe."
            )
            return
        
        # Get valid email addresses (non-null, non-empty)
        emails_series = contacts_to_unsubscribe["email"].dropna()
        emails_series = emails_series[emails_series.astype(str).str.strip() != ""]
        emails_to_unsubscribe = emails_series.tolist()
        
        if not emails_to_unsubscribe:
            logger.warning(
                f"Found {len(contacts_to_unsubscribe)} contacts with globalUnsubscribe=true "
                "but no valid email addresses. Cannot process unsubscribe."
            )
            return
        
        logger.info(
            f"Processing global unsubscribe for {len(emails_to_unsubscribe)} contacts "
            "via HubSpot communication preferences API (individual requests)"
        )
        
        # Process each email individually to avoid requiring Marketing Enterprise
        # Using individual API calls instead of batch operations
        for email in emails_to_unsubscribe:
            # Create stream name with email embedded in the path
            stream_name = f"communication-preferences/v4/statuses/{email}/unsubscribe-all?channel=EMAIL"
            
            # Create a DataFrame with a single row but no columns (as per documentation)
            # The email is embedded in the URL path, so no data payload is needed
            empty_df = pd.DataFrame(index=pd.Index([0]))
            
            # Bypass the empty check and write directly using gs.to_singer
            # since write_to_singer would reject this as empty
            output_df = empty_df.copy()
            gs.to_singer(
                output_df,
                stream_name,
                self.output_dir,
                allow_objects=True,
            )
            logger.debug(f"Wrote unsubscribe request for: {email}")
        
        logger.info(
            f"Successfully queued {len(emails_to_unsubscribe)} contacts for global unsubscribe "
            "via individual API calls"
        )

    def _filter_pending_or_rejected_contacts(
        self, df: pd.DataFrame, field_name: str
    ) -> pd.DataFrame:
        """
        Filter out contacts where a specific field is pending CRM sync approval or was rejected.

        The CRM Sync Approval System (CB-7840/PR #3053) gates critical field changes
        before syncing to external CRMs. This method filters out contacts where:
        1. The field is in pendingCriticalFieldsForCrmSync (awaiting approval)
        2. The contact has a rejectionReason set (change was rejected)

        Note: Callers should pre-filter for truthy field values before calling this method.

        Args:
            df: DataFrame of contacts to filter (should already be filtered for field=true)
            field_name: The field name to check (e.g., "globalUnsubscribe")

        Returns:
            DataFrame with contacts pending approval or rejected removed
        """
        if df is None or df.empty:
            return df

        pending_field_col = "pendingCriticalFieldsForCrmSync"
        rejection_reason_col = "rejectionReason"

        # Check if approval system columns exist (backward compatibility)
        has_pending_col = pending_field_col in df.columns
        has_rejection_col = rejection_reason_col in df.columns

        if not has_pending_col and not has_rejection_col:
            logger.debug(
                "CRM sync approval columns not found in data; "
                "proceeding with all contacts (pre-approval system data)"
            )
            return df

        def is_not_pending_approval(pending_fields) -> bool:
            """Check if the field is NOT in the pending approval list."""
            # Handle null/NA values - not pending
            if pending_fields is None:
                return True
            if isinstance(pending_fields, float) and pd.isna(pending_fields):
                return True

            # Handle list type
            if isinstance(pending_fields, list):
                return field_name not in pending_fields

            # Handle string type (could be JSON string or comma-separated)
            if isinstance(pending_fields, str):
                pending_str = pending_fields.strip()
                if not pending_str or pending_str.lower() in ("null", "none", "[]"):
                    return True

                # Try JSON parsing first
                if pending_str.startswith("["):
                    try:
                        import json
                        fields_list = json.loads(pending_str)
                        return field_name not in fields_list
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Fallback: check if field name is in the string
                return field_name not in pending_str

            # Default: not pending (unknown type)
            return True

        def has_no_rejection_reason(rejection_reason) -> bool:
            """Check if there is no rejection reason set."""
            if rejection_reason is None:
                return True
            if isinstance(rejection_reason, float) and pd.isna(rejection_reason):
                return True
            if isinstance(rejection_reason, str):
                return not rejection_reason.strip() or rejection_reason.strip().lower() in ("null", "none")
            return True

        original_count = len(df)

        # Build mask for contacts that should be synced
        # (not pending AND no rejection reason)
        if has_pending_col:
            not_pending_mask = df[pending_field_col].apply(is_not_pending_approval)
        else:
            not_pending_mask = pd.Series([True] * len(df), index=df.index)

        if has_rejection_col:
            not_rejected_mask = df[rejection_reason_col].apply(has_no_rejection_reason)
        else:
            not_rejected_mask = pd.Series([True] * len(df), index=df.index)

        # Combine masks: must pass BOTH checks
        should_sync_mask = not_pending_mask & not_rejected_mask
        filtered_df = df[should_sync_mask].copy()

        # Log details about what was filtered
        pending_count = (~not_pending_mask).sum() if has_pending_col else 0
        rejected_count = (~not_rejected_mask).sum() if has_rejection_col else 0
        total_skipped = original_count - len(filtered_df)

        if total_skipped > 0:
            logger.info(
                f"Skipped {total_skipped} contacts for '{field_name}' sync: "
                f"{pending_count} pending approval, {rejected_count} rejected"
            )

        return filtered_df

    def handle_read(self) -> None:
        """
        Handle the read operation for HubSpot data.
        
        This method passes through HubSpot data as-is without transformations,
        preserving all native HubSpot field names and values.
        """
        logger.info("Starting HubSpot read operation (passthrough mode)")
        
        data_streams = self.list_available_streams()
        if not data_streams:
            logger.warning("No streams available for read operation")
            return
        
        # Only process relevant streams for data warehouse
        target_streams = [s for s in data_streams if s in {"contacts", "companies"}]
        
        for stream in target_streams:
            logger.info(f"Processing read stream (passthrough): {stream}")
            stream_data = self.get_stream_data(stream)
            
            # Determine output stream name
            inverse_mapping = {v: k for k, v in self.stream_name_mapping.items()}
            output_stream = inverse_mapping.get(stream, stream)
            
            self.write_to_singer(stream_data, output_stream)
            logger.info(f"Wrote {len(stream_data)} records for read stream: {stream}")
