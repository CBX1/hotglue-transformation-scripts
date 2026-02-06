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

        This method:
        1. Passes through raw HubSpot fields as-is (no heavy normalization)
        2. Enriches with context needed by Different:
           - Owner details (from owners stream)
           - List membership details (from _hg_list_memberships + lists stream)
           - df_accountId (from snapshots for contacts)
        """
        logger.info("Starting HubSpot read operation (passthrough + context enrichment)")

        data_streams = self.list_available_streams()
        if not data_streams:
            logger.warning("No streams available for read operation")
            return

        # Prepare owner lookup for enrichment
        owner_lookup = self._prepare_owner_lookup(data_streams)

        # Prepare list lookup for membership enrichment
        list_lookup = self._prepare_list_lookup(data_streams)

        # Only process relevant streams for data warehouse
        target_streams = [s for s in data_streams if s in {"contacts", "companies"}]

        for stream in target_streams:
            logger.info(f"Processing read stream (passthrough + enrichment): {stream}")
            stream_data = self.get_stream_data(stream)

            # Enrich with owner context
            stream_data = self._enrich_with_owner_details(stream_data, stream, owner_lookup)

            # Enrich with list membership context
            stream_data = self._enrich_with_membership_details(stream_data, stream, list_lookup)

            # For contacts: add df_accountId from snapshots
            if stream == "contacts":
                stream_data = self._enrich_with_account_id(stream_data)

            # Wrap records in {data, lookupKey, sourceRecordId} structure
            stream_data = self._wrap_records_with_metadata(stream_data, stream)

            # Determine output stream name
            inverse_mapping = {v: k for k, v in self.stream_name_mapping.items()}
            output_stream = inverse_mapping.get(stream, stream)

            self.write_to_singer(stream_data, output_stream)
            logger.info(f"Wrote {len(stream_data)} records for read stream: {stream}")

    def _prepare_owner_lookup(self, streams: List[str]) -> Optional[pd.DataFrame]:
        """
        Create a lookup table for HubSpot owner information enrichment.

        Args:
            streams: List of available stream names

        Returns:
            DataFrame with owner lookup information or None if not available
        """
        if "owners" not in streams:
            logger.info("No owners stream available for enrichment")
            return None

        owners_df = self.get_stream_data("owners")
        if owners_df is None or owners_df.empty:
            logger.info("Owners stream is empty")
            return None

        if "id" not in owners_df.columns:
            logger.warning("Owners stream missing 'id' column")
            return None

        length = len(owners_df)

        # Helper function to create string series with proper NA handling
        def _string_series(column: str) -> pd.Series:
            if column not in owners_df.columns:
                return pd.Series([pd.NA] * length, dtype="string")
            return owners_df[column].astype("string")

        # Extract owner information
        first_name = _string_series("firstName").fillna("")
        last_name = _string_series("lastName").fillna("")
        email = _string_series("email")

        # Construct full name
        full_name = (first_name.str.strip() + " " + last_name.str.strip()).str.strip()
        full_name = full_name.mask(full_name == "", pd.NA)
        full_name = full_name.fillna(email)

        # Create lookup DataFrame
        lookup = pd.DataFrame({
            "owner_key": owners_df["id"].astype("string"),
            "owner_name": full_name,
            "owner_email": email,
        })

        logger.info(f"Created owner lookup with {len(lookup)} entries")
        return lookup

    def _prepare_list_lookup(self, streams: List[str]) -> Optional[Dict[str, str]]:
        """
        Create a lookup dictionary for HubSpot list information.

        Maps list IDs to list names from the 'lists' stream.

        Args:
            streams: List of available stream names

        Returns:
            Dictionary mapping list ID (string) to list name, or None if not available
        """
        if "lists" not in streams:
            logger.info("No lists stream available for list membership enrichment")
            return None

        lists_df = self.get_stream_data("lists")
        if lists_df is None or lists_df.empty:
            logger.info("Lists stream is empty")
            return None

        if "listId" not in lists_df.columns:
            logger.warning("Lists stream missing 'listId' column")
            return None

        if "name" not in lists_df.columns:
            logger.warning("Lists stream missing 'name' column")
            return None

        lookup = {}
        for _, row in lists_df.iterrows():
            list_id = str(row["listId"]) if pd.notna(row["listId"]) else None
            list_name = str(row["name"]) if pd.notna(row["name"]) else None
            if list_id and list_name:
                lookup[list_id] = list_name

        logger.info(f"Created list lookup with {len(lookup)} entries")
        return lookup

    def _enrich_with_owner_details(
        self,
        df: pd.DataFrame,
        stream: str,
        owner_lookup: Optional[pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Enrich data with owner details as a nested object.

        Adds ownerDetails field with structure:
        {
            "name": "Owner Name",
            "email": "owner@example.com"
        }

        Args:
            df: DataFrame to enrich
            stream: Stream name (contacts or companies)
            owner_lookup: Owner lookup DataFrame

        Returns:
            DataFrame with ownerDetails field added
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        # Check if hubspot_owner_id field exists
        if "hubspot_owner_id" not in df.columns:
            logger.debug(f"No hubspot_owner_id field in {stream}, skipping owner enrichment")
            df["ownerDetails"] = None
            return df

        if owner_lookup is None or owner_lookup.empty:
            logger.debug(f"No owner lookup available for {stream}")
            df["ownerDetails"] = None
            return df

        # Merge with owner lookup
        df["__owner_key"] = df["hubspot_owner_id"].astype("string")
        merged = df.merge(
            owner_lookup,
            how="left",
            left_on="__owner_key",
            right_on="owner_key"
        )

        # Build ownerDetails object
        def build_owner_details(row):
            """Build owner details object from row data."""
            name = row.get("owner_name")
            email = row.get("owner_email")

            # Only create object if we have at least one value
            if pd.notna(name) or pd.notna(email):
                details = {}
                if pd.notna(name):
                    details["name"] = str(name)
                if pd.notna(email):
                    details["email"] = str(email)
                return details if details else None
            return None

        merged["ownerDetails"] = merged.apply(build_owner_details, axis=1)

        # Clean up temporary columns
        merged = merged.drop(columns=["__owner_key", "owner_key", "owner_name", "owner_email"], errors="ignore")

        non_null_count = merged["ownerDetails"].notna().sum()
        logger.info(f"Enriched {non_null_count} {stream} records with owner details")

        return merged

    def _enrich_with_membership_details(
        self,
        df: pd.DataFrame,
        stream: str,
        list_lookup: Optional[Dict[str, str]]
    ) -> pd.DataFrame:
        """
        Enrich data with list membership details from _hg_list_memberships field.

        Adds membershipListDetails field with structure:
        [
            {"id": "123", "name": "List Name"},
            ...
        ]

        Args:
            df: DataFrame to enrich
            stream: Stream name (contacts or companies)
            list_lookup: Dictionary mapping list ID to list name

        Returns:
            DataFrame with membershipListDetails field added
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        if "_hg_list_memberships" not in df.columns:
            logger.debug(f"No _hg_list_memberships field in {stream}")
            df["membershipListDetails"] = None
            return df

        def build_membership_details(list_memberships):
            """Convert list IDs to list of {id, name} objects."""
            if pd.isna(list_memberships) or list_memberships is None:
                return None

            list_ids = []
            if isinstance(list_memberships, list):
                list_ids = [str(lid) for lid in list_memberships if lid is not None]
            elif isinstance(list_memberships, str):
                try:
                    import json
                    parsed = json.loads(list_memberships)
                    if isinstance(parsed, list):
                        list_ids = [str(lid) for lid in parsed if lid is not None]
                    else:
                        list_ids = [str(list_memberships)]
                except (json.JSONDecodeError, TypeError):
                    list_ids = [lid.strip() for lid in str(list_memberships).split(",") if lid.strip()]
            else:
                list_ids = [str(list_memberships)]

            if not list_ids:
                return None

            details = []
            for lid in list_ids:
                name = list_lookup.get(lid, "") if list_lookup else ""
                details.append({"id": lid, "name": name})

            return details if details else None

        df["membershipListDetails"] = df["_hg_list_memberships"].apply(build_membership_details)

        non_null_count = df["membershipListDetails"].notna().sum()
        logger.info(f"Enriched {non_null_count} {stream} records with membership list details")

        return df

    def _enrich_with_account_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        For HubSpot contacts, add df_accountId by mapping associatedcompanyid to
        CBX1 company ID using the companies snapshot.

        Mapping logic:
        - associatedcompanyid (HubSpot company remote id) -> match to companies snapshot RemoteId
        - df_accountId output field <- companies snapshot InputId (CBX1 id)

        If no snapshot or no match, df_accountId will be null.

        Args:
            df: Contacts DataFrame

        Returns:
            DataFrame with df_accountId field added
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        if "associatedcompanyid" not in df.columns:
            logger.debug("No associatedcompanyid field in contacts, skipping account enrichment")
            df["df_accountId"] = None
            return df

        # Read companies snapshot (stores InputId and RemoteId)
        companies_snap = self.read_snapshot("companies")
        if companies_snap is None or companies_snap.empty:
            logger.info("No companies snapshot available for account ID enrichment")
            df["df_accountId"] = None
            return df

        # Prepare for join
        tmp = companies_snap.rename(columns={"RemoteId": "__company_remote_id", "InputId": "df_accountId"}).copy()
        # Normalize types for safe join
        df["associatedcompanyid"] = df["associatedcompanyid"].astype("string")
        tmp["__company_remote_id"] = tmp["__company_remote_id"].astype("string")

        merged = df.merge(
            tmp[["__company_remote_id", "df_accountId"]],
            left_on="associatedcompanyid",
            right_on="__company_remote_id",
            how="left",
        )
        merged = merged.drop(columns=["__company_remote_id"], errors="ignore")

        non_null_count = merged["df_accountId"].notna().sum()
        logger.info(f"Enriched {non_null_count} contacts with df_accountId")

        return merged

    def _wrap_records_with_metadata(self, df: pd.DataFrame, stream: str) -> pd.DataFrame:
        """
        Wrap each record in a structure with data, lookupKey, and sourceRecordId.

        Transforms each record from:
        {field1: value1, field2: value2, ...}

        To:
        {
            data: {field1: value1, field2: value2, ...},
            lookupKey: <email for contacts, domain for companies>,
            sourceRecordId: <id>
        }

        Args:
            df: DataFrame to wrap
            stream: Stream name (contacts or companies)

        Returns:
            DataFrame with wrapped records
        """
        if df is None or df.empty:
            return df

        # Determine lookup key field based on stream
        if stream == "contacts":
            lookup_field = "email"
        elif stream == "companies":
            lookup_field = "domain"
        else:
            logger.warning(f"Unknown stream '{stream}' for record wrapping")
            return df

        # Check if required fields exist
        if "id" not in df.columns:
            logger.warning(f"Missing 'id' field in {stream}, cannot wrap records")
            return df

        if lookup_field not in df.columns:
            logger.warning(f"Missing '{lookup_field}' field in {stream}, cannot wrap records")
            return df

        # Replace pandas NaT and NA with None for JSON serialization
        df = df.copy()
        df = df.replace({pd.NaT: None, pd.NA: None})

        # Convert DataFrame to list of dictionaries
        records = df.to_dict(orient='records')

        # Wrap each record
        wrapped_records = []
        for record in records:
            # Clean the record data recursively
            cleaned_record = self._clean_record_for_serialization(record)

            wrapped = {
                "data": cleaned_record,
                "lookupKey": record.get(lookup_field),
                "sourceRecordId": record.get("id")
            }
            wrapped_records.append(wrapped)

        # Convert back to DataFrame
        wrapped_df = pd.DataFrame(wrapped_records)

        logger.info(f"Wrapped {len(wrapped_df)} {stream} records with metadata structure")
        return wrapped_df

    def _clean_record_for_serialization(self, obj):
        """
        Recursively clean a record by converting pandas NaT/NA and numpy types to JSON-serializable values.

        Args:
            obj: Object to clean (can be dict, list, or scalar)

        Returns:
            Cleaned object safe for JSON serialization
        """
        if isinstance(obj, dict):
            return {k: self._clean_record_for_serialization(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_record_for_serialization(item) for item in obj]
        elif pd.isna(obj):
            # Handles pd.NaT, pd.NA, np.nan, None
            return None
        elif isinstance(obj, (np.integer, np.floating)):
            # Convert numpy types to Python native types
            return obj.item()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        else:
            return obj
