#!/usr/bin/env python
# coding: utf-8

"""
HubSpot-specific ETL handler.

This module implements the HubSpot-specific logic for both read and write operations.
Note: HubSpot associations are handled by HubSpot's own system based on configured rules,
so this handler does not include association logic.
"""

import logging
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

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
        
        Args:
            mapping: Field mapping configuration
        """
        logger.info("Processing contacts for HubSpot")
        
        # Check if we have sent accounts (required for contact processing)
        sent_accounts = self.read_snapshot(self.stream_name_mapping.get('accounts', 'companies'))
        if sent_accounts is None:
            logger.warning("No accounts have been sent yet, skipping contacts export")
            return
        
        # Prepare account data
        sent_accounts = sent_accounts.rename(
            columns={"InputId": "AccountId", "RemoteId": "RemoteAccountId"}
        )
        sent_accounts = sent_accounts[sent_accounts["RemoteAccountId"].notna()]
        
        # Get contact data
        contacts_df = self.get_stream_data("contacts")
        
        # Apply mapping for contacts
        mapping_name = "contacts"
        stream_columns, contacts_df = map_stream_data(contacts_df, mapping_name, mapping)
        new_data = contacts_df.copy()
        
        # Create snapshot for tracking
        snap = self.write_snapshot(contacts_df, mapping_name)
        if "hash" in snap.columns:
            snap = snap.drop(columns=["hash"])
        
        # Merge in account remote IDs (for reference only, not for associations)
        snap = snap.merge(sent_accounts, on="AccountId", how="left")
        snap = snap.rename(
            columns={
                "AccountId": "InputAccountId",
                "RemoteAccountId": "AccountId"
            }
        )
        
        # Prepare final output (without association formatting)
        df_out = snap[list(set(stream_columns))].copy()
        
        # Filter out already sent records
        sent_contacts = self.read_snapshot(self.stream_name_mapping[mapping_name])
        df_out = drop_sent_records("contacts", df_out, sent_contacts, new_data)
        
        # Write to output
        self.write_to_singer(df_out, self.stream_name_mapping[mapping_name])
        logger.info(f"Processed {len(df_out)} contact records for HubSpot")
    
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
        - Drop the temporary associatedcompanyid column from the final output

        Args:
            df: Contacts DataFrame

        Returns:
            DataFrame with accountId attached and helper columns removed
        """
        df = self._attach_contact_account_id(df)
        if "associatedcompanyid" in df.columns:
            df = df.drop(columns=["associatedcompanyid"], errors="ignore")
        return df

    def _apply_company_ownership_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Derive account ownershipType from company's is_public field.
        - If is_public is true-like -> ownershipType = "PUBLIC"
        - Else -> ownershipType = "PRIVATE"

        This expects that the `is_public` column is available after mapping for the
        companies stream. If it is missing, defaults to PRIVATE.
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
            ownership.loc[notna_mask & true_mask] = "PUBLIC"
            ownership.loc[notna_mask & false_mask] = "PRIVATE"
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
