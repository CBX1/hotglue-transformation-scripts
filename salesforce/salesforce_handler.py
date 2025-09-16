#!/usr/bin/env python
# coding: utf-8

"""
Salesforce-specific ETL handler.

This module implements the Salesforce-specific logic for both read and write operations,
including the Contact/Lead split functionality and field mapping transformations.
"""

import logging
from typing import Dict, Optional

import pandas as pd

from base_handler import BaseETLHandler
from utils import (
    map_stream_data,
    drop_sent_records,
    split_contacts_by_account,
    get_contact_data,
    transform_dot_notation_to_nested,
)

logger = logging.getLogger(__name__)


class SalesforceHandler(BaseETLHandler):
    """
    Handler for Salesforce ETL operations.
    
    This handler manages the specific requirements for Salesforce data transformations,
    including:
    - Dynamic contact mapping (splitting contacts into Contacts and Leads)
    - Account association management
    - Salesforce-specific field transformations
    """
    
    def __init__(self, *args, target_config: Optional[Dict] = None, **kwargs):
        """
        Initialize the Salesforce handler.
        
        Args:
            *args: Arguments passed to parent class
            target_config: Target configuration containing Salesforce-specific settings
            **kwargs: Additional keyword arguments passed to parent class
        """
        super().__init__(*args, **kwargs)
        self.target_config = target_config or {}
        self.connector_id = "salesforce"
    
    def handle_write(self) -> None:
        """
        Handle the write operation for Salesforce.
        
        This method:
        1. Processes all available streams from the source
        2. Applies Salesforce-specific transformations
        3. Handles the Contact/Lead split based on account associations
        4. Writes transformed data in Salesforce-compatible format
        """
        if not self.mapping_for_flow:
            raise ValueError("No write mapping found for Salesforce flow")
        
        mapping = self.build_write_mapping()
        streams = self.list_available_streams()
        
        logger.info(f"Processing {len(streams)} streams for Salesforce write operation")
        
        for stream in streams:
            if stream not in mapping:
                # Pass through unmapped streams without transformation
                self._handle_passthrough_stream(stream)
            elif stream == "contacts":
                # Special handling for contacts (split into Contacts/Leads)
                self._handle_contacts_write(mapping)
            else:
                # Standard stream processing with mapping
                self._handle_standard_stream_write(stream, mapping)
    
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
        Handle contacts write with Salesforce-specific Contact/Lead split.
        
        This method implements the business logic to split contacts into:
        - Contacts: Records with existing account associations
        - Leads: Records without account associations
        
        Args:
            mapping: Field mapping configuration
        """
        logger.info("Processing contacts with Contact/Lead split for Salesforce")
        
        # Check if we have sent accounts (required for contact processing)
        sent_accounts = self.read_snapshot(self.stream_name_mapping.get('accounts', 'Account'))
        if sent_accounts is None:
            logger.warning("No accounts have been sent yet, skipping contacts export")
            return
        
        # Prepare account data for joining
        sent_accounts = sent_accounts.rename(
            columns={"InputId": "AccountId", "RemoteId": "RemoteAccountId"}
        )
        sent_accounts = sent_accounts[sent_accounts["RemoteAccountId"].notna()]
        
        # Get contact data
        contacts_df = self.get_stream_data("contacts")
        leads_df = None
        
        # Split contacts based on account association if configured
        if self.target_config.get("dynamic_contact_mapping"):
            logger.info("Applying dynamic contact mapping (Contact/Lead split)")
            contacts_df, leads_df = split_contacts_by_account(contacts_df, sent_accounts)
        
        # Process both Contact and Lead streams
        for sfdc_stream in ["Contact", "Lead"]:
            self._process_contact_stream(
                sfdc_stream, contacts_df, leads_df, sent_accounts, mapping
            )
    
    def _process_contact_stream(
        self,
        stream_type: str,
        contacts_df: Optional[pd.DataFrame],
        leads_df: Optional[pd.DataFrame],
        sent_accounts: pd.DataFrame,
        mapping: Dict,
    ) -> None:
        """
        Process a specific contact stream (Contact or Lead).
        
        Args:
            stream_type: Either "Contact" or "Lead"
            contacts_df: DataFrame containing contact records
            leads_df: DataFrame containing lead records
            sent_accounts: DataFrame of previously sent accounts
            mapping: Field mapping configuration
        """
        # Get the appropriate data for this stream type
        df = get_contact_data(self.connector_id, stream_type, contacts_df, leads_df)
        if df is None or df.empty:
            logger.info(f"No data for {stream_type}, skipping")
            return
        
        # Find the mapping name for this stream
        inverse = {v: k for k, v in self.stream_name_mapping.items()}
        mapping_name = inverse.get(stream_type)
        if not mapping_name:
            logger.warning(f"No mapping found for {stream_type}, skipping")
            return
        
        # Apply field mapping
        stream_columns, df = map_stream_data(df, mapping_name, mapping)
        new_data = df.copy()
        
        # Create snapshot for tracking
        snap = self.write_snapshot(df, mapping_name)
        if "hash" in snap.columns:
            snap = snap.drop(columns=["hash"])
        
        # For Contacts (not Leads), merge in account remote IDs
        if stream_type == "Contact":
            snap = snap.merge(sent_accounts, on="AccountId")
            snap = snap.rename(
                columns={
                    "AccountId": "InputAccountId",
                    "RemoteAccountId": "AccountId"
                }
            )
        
        # Prepare final output
        df_out = snap[list(set(stream_columns))].copy()
        
        # Filter out already sent records
        sent_contacts = self.read_snapshot(self.stream_name_mapping[mapping_name])
        df_out = drop_sent_records("contacts", df_out, sent_contacts, new_data)
        
        # Write to output
        self.write_to_singer(df_out, self.stream_name_mapping[mapping_name])
        logger.info(f"Processed {len(df_out)} {stream_type} records")
    
    def handle_read(self) -> None:
        """
        Handle the read operation for Salesforce data.
        
        This method:
        1. Reads data from Salesforce format
        2. Transforms it to a standardized data warehouse format
        3. Enriches with CBX1 IDs where available
        4. Applies field transformations and nested structure handling
        """
        logger.info("Starting Salesforce read operation")
        
        data_streams = self.list_available_streams()
        if not data_streams or not self.mapping_for_flow:
            logger.warning("No streams or mapping available for read operation")
            return
        
        mapping = self.build_read_mapping()
        
        # Only process relevant streams for data warehouse
        target_streams = [s for s in data_streams if s in {"accounts", "contacts", "companies"}]
        
        for stream in target_streams:
            self._process_read_stream(stream, mapping)
    
    def _process_read_stream(self, stream: str, mapping: Dict) -> None:
        """
        Process a single stream for read operation.
        
        Args:
            stream: Name of the stream to process
            mapping: Field mapping configuration
        """
        logger.info(f"Processing read stream: {stream}")
        
        stream_data = self.get_stream_data(stream)
        
        if stream not in mapping:
            logger.info(f"No mapping for stream {stream}, passing through")
            self._write_read_output(stream_data, stream)
            return
        
        # Apply field mapping and transformations
        stream_data = self._apply_read_mapping(stream_data, stream, mapping[stream])
        
        # Enrich with CBX1 IDs from snapshots
        stream_data = self._enrich_with_cbx1_ids(stream_data, stream)
        
        # Handle special case for contacts with account IDs
        if stream == "contacts":
            stream_data = self._update_contact_account_ids(stream_data)
        
        # Add CRM system identifier
        stream_data = self._add_crm_system(stream_data)
        
        # Transform dot notation to nested structures
        stream_data = transform_dot_notation_to_nested(stream_data)
        
        # Determine output stream name
        inverse_mapping = {v: k for k, v in self.stream_name_mapping.items()}
        output_stream = inverse_mapping.get(stream, stream)
        
        self._write_read_output(stream_data, output_stream)
    
    def _apply_read_mapping(self, df: pd.DataFrame, stream: str, mapping: Dict) -> pd.DataFrame:
        """
        Apply read mapping to transform field names.
        
        Args:
            df: DataFrame to transform
            stream: Name of the stream
            mapping: Field mapping configuration
            
        Returns:
            Transformed DataFrame with mapped field names
        """
        df = df.copy()
        
        # Rename columns based on mapping
        for source_col, target_col in mapping.items():
            if target_col in df.columns:
                df[source_col] = df[target_col]
        
        # Keep only mapped columns plus remote_id
        columns = list(mapping.keys())
        if "remote_id" in df.columns:
            columns.append("remote_id")
        
        # Ensure unique columns
        unique_columns = []
        for col in columns:
            if col not in unique_columns and col in df.columns:
                unique_columns.append(col)
        
        return df[unique_columns].copy()
    
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
    
    def _update_contact_account_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Update contact account IDs with CBX1 account IDs.
        
        Args:
            df: Contacts DataFrame
            
        Returns:
            DataFrame with updated account IDs
        """
        if "accountId" not in df.columns:
            return df
        
        account_snapshots = self.read_snapshot("Account")
        if account_snapshots is None:
            return df
        
        # Prepare account mapping
        acc = account_snapshots.rename(
            columns={
                "InputId": "cbx1_account_id",
                "RemoteId": "salesforce_account_id"
            }
        ).copy()
        
        # Normalize IDs as strings for matching
        df["accountId"] = df["accountId"].astype(str)
        acc["salesforce_account_id"] = acc["salesforce_account_id"].astype(str)
        
        # Merge to get CBX1 account IDs
        df = df.merge(
            acc[["salesforce_account_id", "cbx1_account_id"]],
            left_on="accountId",
            right_on="salesforce_account_id",
            how="left",
        )
        
        # Replace account ID with CBX1 ID
        df["accountId"] = df["cbx1_account_id"]
        df = df.drop(columns=["salesforce_account_id", "cbx1_account_id"], errors="ignore")
        
        return df
    
    def _add_crm_system(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add CRM system identifier to the data.
        
        Args:
            df: DataFrame to add CRM system to
            
        Returns:
            DataFrame with crmSystem column added
        """
        df = df.copy()
        df["crmSystem"] = "SALESFORCE"
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
