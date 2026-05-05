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
        "updatedAt",
        "pendingCriticalFieldsForCrmSync",
        "globalUnsubscribe",
    }

    # HubSpot company properties that are read-only and must not be pushed
    COMPANY_READ_ONLY_FIELDS: Set[str] = {
        "createdate",
        "hs_lastmodifieddate",
        "hs_object_id",
        "updatedAt",
        "pendingCriticalFieldsForCrmSync",
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

        Writes contacts unconditionally. Writes accounts (HubSpot companies) only when
        a tenant mapping for accounts is present in stream_name_mapping — enabling
        per-tenant opt-in without a feature flag. Association handling is delegated to
        HubSpot's internal systems.
        """
        if not self.mapping_for_flow:
            raise ValueError("No write mapping found for HubSpot flow")

        mapping = self.build_write_mapping()
        streams = self.list_available_streams()

        logger.info(f"HubSpot write: evaluating {len(streams)} streams")

        for stream in streams:
            if stream == "contacts":
                self._handle_contacts_write(mapping)
            elif stream == "accounts":
                self._handle_accounts_write(mapping)
            else:
                logger.info(f"HubSpot write: skipping stream '{stream}' by policy")
    
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

        contacts_df_raw = self.get_stream_data("contacts")
        mapping_name = "contacts"

        # Slim the raw DataFrame to only the columns needed by the unsubscribe handler
        # before dropping read-only fields, so we can free the full raw copy early.
        _unsub_cols = {"globalUnsubscribe", "pendingCriticalFieldsForCrmSync", "rejectionReason", "email"}
        contacts_for_unsub = contacts_df_raw[[c for c in _unsub_cols if c in contacts_df_raw.columns]]

        # Remove HubSpot read-only fields; drop() returns a new DataFrame — no copy() needed.
        df_out = contacts_df_raw.drop(columns=list(self.READ_ONLY_FIELDS), errors="ignore")
        del contacts_df_raw

        # Filter out already sent records
        snapshot_name = f"{self.stream_name_mapping[mapping_name]}_{self.flow_id}"
        sent_contacts = gs.read_snapshots(snapshot_name, self.snapshot_dir)
        df_out = drop_sent_records("contacts", df_out, sent_contacts, None)

        # Write to output
        self.write_to_singer(df_out, self.stream_name_mapping[mapping_name])
        logger.info(f"Processed {len(df_out)} contact records for HubSpot")

        # Call unsubscribe API for contacts with globalUnsubscribe=true
        # Only processes contacts where globalUnsubscribe is NOT pending approval or rejected
        # (CB-7840: CRM Sync Approval System gates critical field changes)
        self._handle_global_unsubscribe(contacts_for_unsub)
    
    def _handle_accounts_write(self, mapping: Dict) -> None:
        """
        Handle accounts write for HubSpot (maps to HubSpot companies object).

        Opt-in per tenant: if no accounts mapping exists in stream_name_mapping
        (i.e., the backend has not configured TenantEgestionMapping for ACCOUNT→HUBSPOT
        for this tenant), the stream is skipped gracefully.
        """
        if "accounts" not in self.stream_name_mapping:
            logger.info("No accounts mapping configured for this tenant, skipping accounts write")
            return

        logger.info("Processing accounts (companies) for HubSpot")

        accounts_df = self.get_stream_data("accounts")
        if accounts_df is None or accounts_df.empty:
            logger.info("No account records to write")
            return

        # drop() returns a new DataFrame — no copy() needed.
        df_out = accounts_df.drop(columns=list(self.COMPANY_READ_ONLY_FIELDS), errors="ignore")
        del accounts_df

        # drop_sent_records requires externalId; the egestion mapping emits it ($.id→$.externalId)
        # but guard defensively in case of misconfigured mappings
        if "externalId" not in df_out.columns:
            df_out["externalId"] = df_out["id"] if "id" in df_out.columns else None

        # Snapshot is written back by HotGlue's target connector after upsert (same as contacts)
        snapshot_name = f"{self.stream_name_mapping['accounts']}_{self.flow_id}"
        sent_accounts = gs.read_snapshots(snapshot_name, self.snapshot_dir)
        df_out = drop_sent_records("accounts", df_out, sent_accounts, None)

        self.write_to_singer(df_out, self.stream_name_mapping["accounts"])
        logger.info(f"Processed {len(df_out)} account records for HubSpot")

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
        if not data_streams or not self.mapping_for_flow:
            logger.warning("No streams or mapping available for read operation")
            return

        mapping = self.build_read_mapping()
        # Prepare owner lookup for enrichment
        owner_lookup = self._prepare_owner_lookup(data_streams)

        # Prepare list lookup for CRM list membership enrichment
        list_lookup = self._prepare_list_lookup(data_streams)

        # Only process relevant streams for data warehouse
        target_streams = [s for s in data_streams if s in {"contacts", "companies"}]

        for stream in target_streams:
            logger.info(f"Processing read stream (passthrough + enrichment): {stream}")
            stream_data = self.get_stream_data(stream)
            stream_data = self._filter_archived_records(stream_data, stream)
            owner_column = None

          
            stream_data, owner_column = self._apply_read_mapping(
                    stream_data, stream
                )   

            # Enrich with owner information for contacts and companies
            if stream in {"contacts", "companies"}:
                stream_data = self._merge_owner_details(stream_data, owner_lookup, owner_column)
            

            # Enrich with list membership context
            stream_data = self._populate_list_memberships(stream_data, stream, list_lookup)
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
        owners_df = self._filter_archived_records(owners_df, "owners")
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
            "crmOwnerName": full_name,
            "crmOwnerEmail": email,
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


    def _populate_list_memberships(
        self,
        df: pd.DataFrame,
        stream: str,
        list_lookup: Optional[Dict[str, str]]
    ) -> pd.DataFrame:
        """
        Populate crmListMembershipDetails from _hg_list_memberships field.

        The _hg_list_memberships field contains list IDs. This method maps those
        IDs to list names using the list_lookup and creates the crmListMembershipDetails
        field as a list of objects with 'id' and 'name' properties.

        Args:
            df: DataFrame containing records with potential _hg_list_memberships
            list_lookup: Dictionary mapping list ID to list name

        Returns:
            DataFrame with crmListMembershipDetails populated
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        if "_hg_list_memberships" not in df.columns:
            logger.debug("No _hg_list_memberships field found")
            df["crmListMembershipDetails"] = None
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

        df["crmListMembershipDetails"] = df["_hg_list_memberships"].apply(build_membership_details)
        df = df.drop(columns=["_hg_list_memberships"], errors="ignore")

        non_null_count = df["crmListMembershipDetails"].notna().sum()
        logger.info(f"Populated crmListMembershipDetails for {non_null_count} records")

        return df
    
    def _filter_archived_records(
        self,
        df: Optional[pd.DataFrame],
        stream: str
    ) -> Optional[pd.DataFrame]:
        """Remove archived records from the provided DataFrame."""
        if df is None or df.empty or "archived" not in df.columns:
            return df

        df = df.copy()
        archived_mask = df["archived"].fillna(False)
        filtered_df = df.loc[~archived_mask].copy()
        removed = len(df) - len(filtered_df)
        if removed:
            logger.debug(
                f"HubSpot read: skipped {removed} archived records from '{stream}'"
            )
        return filtered_df
    


    def _apply_read_mapping(
        self,
        df: pd.DataFrame,
        stream: str,
    ) -> tuple[pd.DataFrame, Optional[str]]:
        """
        Apply read mapping to transform field names.
        
        Args:
            df: DataFrame to transform
            stream: Name of the stream
            mapping: Field mapping configuration
            
        Returns:
            Tuple of (transformed DataFrame, owner column name if present)
        """
        df = df.copy()
        owner_column = None
        
        # Check for owner column
        if "hubspot_owner_id" in df.columns:
            owner_column = "hubspot_owner_id"
        else:
            # Add hubspot_owner_id column if not present
            df["hubspot_owner_id"] = None

        # Preserve associatedcompanyid for contacts so we can derive accountId later
        if stream == "contacts":
            if "associatedcompanyid" not in df.columns:
                df["associatedcompanyid"] = None

        # Preserve is_public for companies so we can derive ownershipType later
        if stream == "companies":
            if "is_public" not in df.columns:
                df["is_public"] = None

        # Preserve _hg_list_memberships for list membership enrichment
        if stream in {"contacts", "companies"}:
            if "_hg_list_memberships" not in df.columns:
                df["_hg_list_memberships"] = None
        
        return df, owner_column

    def _wrap_records_with_metadata(self, df: pd.DataFrame, stream: str) -> pd.DataFrame:
        """
        Wrap each record in a structure with data, lookupKey, sourceRecordId, and source.

        Transforms each record from:
        {field1: value1, field2: value2, ...}

        To:
        {
            data: {field1: value1, field2: value2, ...},
            sourceRecordId: <id>,
            source: 'HUBSPOT',
            lookupKey: <email for contacts, domain for companies>
        }

        Records with null lookupKey values are filtered out.

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

        # Filter out records with null lookup key (email or domain)
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

        # Convert DataFrame to list of dictionaries
        records = df.to_dict(orient='records')

        # Wrap each record
        wrapped_records = []
        for record in records:
            # Clean the record data recursively
            cleaned_record = self._clean_record_for_serialization(record)

            wrapped = {
                "data": cleaned_record,
                "sourceRecordId": record.get("id"),
                "source": "HUBSPOT",
                "lookupKey": record.get(lookup_field)
            }
            wrapped_records.append(wrapped)

        # Convert back to DataFrame
        wrapped_df = pd.DataFrame(wrapped_records)

        logger.info(f"Wrapped {len(wrapped_df)} {stream} records with metadata structure and source='HUBSPOT'")
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
 
 
    def _merge_owner_details(
        self,
        df: pd.DataFrame,
        owner_lookup: Optional[pd.DataFrame],
        owner_column: Optional[str]
    ) -> pd.DataFrame:
        """
        Merge owner details into the DataFrame.
        
        Args:
            df: DataFrame to enrich with owner details
            owner_lookup: Owner lookup table
            owner_column: Name of the owner ID column
            
        Returns:
            DataFrame with owner details merged
        """
        df = df.copy()
        
        # Add default columns if no owner lookup is available
        if owner_lookup is None or not owner_column or owner_column not in df.columns:
            if "crmOwnerName" not in df.columns:
                df["crmOwnerName"] = pd.Series([pd.NA] * len(df), dtype="string")
            if "crmOwnerEmail" not in df.columns:
                df["crmOwnerEmail"] = pd.Series([pd.NA] * len(df), dtype="string")
            return df
        
        # Merge owner information
        df["__owner_key"] = df[owner_column].astype("string")
        merged = df.merge(
            owner_lookup,
            how="left",
            left_on="__owner_key",
            right_on="owner_key"
        )
        
        # Clean up temporary columns
        merged = merged.drop(columns=["__owner_key", "owner_key"], errors="ignore")
        if owner_column in merged.columns:
            merged = merged.drop(columns=[owner_column], errors="ignore")
        
        # Ensure owner columns exist
        if "crmOwnerName" not in merged.columns:
            merged["crmOwnerName"] = pd.Series([pd.NA] * len(merged), dtype="string")
        if "crmOwnerEmail" not in merged.columns:
            merged["crmOwnerEmail"] = pd.Series([pd.NA] * len(merged), dtype="string")
        
        return merged       
