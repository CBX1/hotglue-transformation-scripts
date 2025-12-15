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
                
        # This checks the raw source data for globalUnsubscribe field
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
    
    def handle_read(self) -> None:
        """
        Handle the read operation for HubSpot data.
        
        This method:
        1. Reads data from HubSpot format
        2. Enriches with owner information when available
        3. Transforms to standardized data warehouse format
        4. Adds CBX1 IDs where available
        5. For contacts, adds accountId by mapping associatedcompanyid -> CBX1 company id
        """
        logger.info("Starting HubSpot read operation")
        
        data_streams = self.list_available_streams()
        if not data_streams or not self.mapping_for_flow:
            logger.warning("No streams or mapping available for read operation")
            return
        
        mapping = self.build_read_mapping()
        
        # Prepare owner lookup for enrichment
        owner_lookup = self._prepare_owner_lookup(data_streams)
        
        # Only process relevant streams for data warehouse
        target_streams = [s for s in data_streams if s in {"accounts", "contacts", "companies"}]
        
        for stream in target_streams:
            self._process_read_stream(stream, mapping, owner_lookup)
    
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
    
    def _process_read_stream(
        self,
        stream: str,
        mapping: Dict,
        owner_lookup: Optional[pd.DataFrame]
    ) -> None:
        """
        Process a single stream for read operation.
        
        Args:
            stream: Name of the stream to process
            mapping: Field mapping configuration
            owner_lookup: Owner information lookup table
        """
        logger.info(f"Processing read stream: {stream}")
        
        stream_data = self.get_stream_data(stream)
        stream_data = self._filter_archived_records(stream_data, stream)
        owner_column = None

        if stream not in mapping:
            logger.info(f"No mapping for stream {stream}, passing through")
        else:
            # Apply field mapping and transformations
            stream_data, owner_column = self._apply_read_mapping(
                stream_data, stream, mapping[stream]
            )
            
            # Enrich with CBX1 IDs from snapshots
            stream_data = self._enrich_with_cbx1_ids(stream_data, stream)
        
        # Enrich with owner information for contacts and companies
        if stream in {"contacts", "companies"}:
            stream_data = self._merge_owner_details(stream_data, owner_lookup, owner_column)

        # For companies: derive ownershipType from is_public
        if stream == "companies":
            stream_data = self._apply_company_ownership_type(stream_data)

        # For contacts: post-process to attach accountId and drop helper columns
        if stream == "contacts":
            stream_data = self._post_process_contacts(stream_data)

        # Add CRM system identifier
        stream_data = self._add_crm_system(stream_data)
        
        # Transform dot notation to nested structures
        stream_data = transform_dot_notation_to_nested(stream_data)
        
        # Determine output stream name
        inverse_mapping = {v: k for k, v in self.stream_name_mapping.items()}
        output_stream = inverse_mapping.get(stream, stream)
        
        self._write_read_output(stream_data, output_stream)

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
        mapping: Dict
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
        
        connector_columns = list(mapping.keys())
        target_api_columns = list(mapping.values())
        
        # Rename columns based on mapping
        columns_to_rename = [c for c in df.columns if c in target_api_columns]
        gc = np.array(target_api_columns)
        
        for column in columns_to_rename:
            for ind in np.where(gc == column)[0]:
                df[connector_columns[ind]] = df[column]
        
        # Build list of columns to keep
        cols = [c for c in connector_columns if c in df.columns]
        if "remote_id" in df.columns:
            cols.append("remote_id")
        
        # Check for owner column
        if "hubspot_owner_id" in df.columns:
            owner_column = "hubspot_owner_id"
            if owner_column not in cols:
                cols.append(owner_column)

        # Preserve associatedcompanyid for contacts so we can derive accountId later
        if stream == "contacts" and "associatedcompanyid" in df.columns:
            cols.append("associatedcompanyid")
        # Preserve is_public for companies so we can derive ownershipType later
        if stream == "companies" and "is_public" in df.columns:
            cols.append("is_public")
        
        # Ensure unique columns
        unique_cols = []
        for col in cols:
            if col not in unique_cols:
                unique_cols.append(col)
        
        return df[unique_cols].copy(), owner_column
    
    def _attach_contact_account_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        For HubSpot contacts, append accountId by mapping associatedcompanyid to
        CBX1 company ID using the companies snapshot.

        Mapping logic:
        - associatedcompanyid (HubSpot company remote id) -> match to companies snapshot RemoteId
        - accountId output field <- companies snapshot InputId (CBX1 id)

        If no snapshot or no match, accountId will be null.
        """
        if df is None or df.empty or "associatedcompanyid" not in df.columns:
            return df

        # Read companies snapshot (stores InputId and RemoteId)
        companies_snap = self.read_snapshot("companies")
        if companies_snap is None or companies_snap.empty:
            return df

        # Prepare for join
        tmp = companies_snap.rename(columns={"RemoteId": "__company_remote_id", "InputId": "accountId"}).copy()
        # Normalize types for safe join
        df["associatedcompanyid"] = df["associatedcompanyid"].astype("string")
        tmp["__company_remote_id"] = tmp["__company_remote_id"].astype("string")

        merged = df.merge(
            tmp[["__company_remote_id", "accountId"]],
            left_on="associatedcompanyid",
            right_on="__company_remote_id",
            how="left",
        )
        merged = merged.drop(columns=["__company_remote_id"], errors="ignore")
        return merged
    
    def _post_process_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Post-process contacts stream after mapping/enrichment:
        - Attach accountId by mapping associatedcompanyid -> companies snapshot
        - Derive emailBounceType from HubSpot email bounce fields
        - Drop the temporary associatedcompanyid column from the final output

        Args:
            df: Contacts DataFrame

        Returns:
            DataFrame with accountId attached, emailBounceType derived, and helper columns removed
        """
        df = self._attach_contact_account_id(df)
        df = self._derive_bounce_type(df)
        if "associatedcompanyid" in df.columns:
            df = df.drop(columns=["associatedcompanyid"], errors="ignore")
        return df

    def _derive_bounce_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Derive emailBounceType field from HubSpot email bounce fields.

        Maps to backend enum: ai.cbx1.targetmanagement.enums.EmailBounceType

        Logic:
            - If hs_email_hard_bounce_reason_enum is not null → HARD
            - Else if hs_email_bounce > 0 → SOFT
            - Else → NONE

        Args:
            df: Contacts DataFrame with HubSpot bounce fields

        Returns:
            DataFrame with emailBounceType field added
        """
        if df is None or df.empty:
            return df

        df = df.copy()

        def determine_bounce_type(row):
            hard_bounce_reason = row.get("hs_email_hard_bounce_reason_enum")
            email_bounce = row.get("hs_email_bounce")

            # Check for hard bounce first
            if pd.notna(hard_bounce_reason) and hard_bounce_reason not in [None, "", "null"]:
                return "HARD"

            # Check for soft bounce
            if pd.notna(email_bounce):
                try:
                    if float(email_bounce) > 0:
                        return "SOFT"
                except (ValueError, TypeError):
                    pass

            # Return NONE to match EmailBounceType enum
            return "NONE"

        df["emailBounceType"] = df.apply(determine_bounce_type, axis=1)
        return df

    def _apply_company_ownership_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Derive account ownershipType from company's is_public field.
        - If is_public is true-like -> ownershipType = "Public"
        - Else -> ownershipType = "Private"

        This expects that the `is_public` column is available after mapping for the
        companies stream. If it is missing, defaults to Private.
        """
        if df is None or df.empty:
            return df

        df = df.copy()
        # Initialize as nulls by default
        ownership = pd.Series([pd.NA] * len(df), dtype="string")

        if "is_public" in df.columns:
            ser = df["is_public"].astype("string").str.strip().str.lower()
            notna_mask = ~df["is_public"].isna()
            true_mask = ser == "true"
            false_mask = ser == "false"
            # Assign PUBLIC/PRIVATE for explicit true/false, leave others as null
            ownership.loc[notna_mask & true_mask] = "Public"
            ownership.loc[notna_mask & false_mask] = "Private"
            # Drop helper column from final output
            df = df.drop(columns=["is_public"], errors="ignore")
        else:
            # If column is missing, keep ownershipType as nulls
            logger.info("companies stream missing 'is_public' column; leaving ownershipType null")

        df["ownershipType"] = ownership
        return df
    
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
    
    def _enrich_with_cbx1_ids(self, df: pd.DataFrame, stream: str) -> pd.DataFrame:
        """
        Enrich data with CBX1 IDs from previous snapshots.
        
        Args:
            df: DataFrame to enrich
            stream: Name of the stream
            
        Returns:
            DataFrame with CBX1 IDs added where available
        """
        if "remote_id" not in df.columns:
            return df
        
        sent_data = self.read_snapshot(stream)
        if sent_data is None:
            return df
        
        sent_data = sent_data.rename(columns={"InputId": "id", "RemoteId": "remote_id"})
        return df.merge(sent_data[["id", "remote_id"]], how="left", on="remote_id")
    
    def _add_crm_system(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add CRM system identifier to the data.
        
        Args:
            df: DataFrame to add CRM system to
            
        Returns:
            DataFrame with crmSystem column added
        """
        df = df.copy()
        df["crmSystem"] = "HUBSPOT"
        df = df.rename(columns={"remote_id": "crmAssociationId"})
        return df
    
    def _write_read_output(self, df: pd.DataFrame, stream_name: str) -> None:
        """
        Write the processed data for read operation.
        
        Args:
            df: DataFrame to write
            stream_name: Name of the output stream
        """
        self.write_to_singer(df, stream_name)
        logger.info(f"Wrote {len(df)} records for read stream: {stream_name}")
