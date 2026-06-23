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
        1. Processes contact stream from the source
        2. Applies Salesforce-specific transformations
        3. Handles the Contact/Lead split based on account associations
        4. Writes transformed data in Salesforce-compatible format

        Note: As per policy, write jobs only push Contacts (no Accounts or other objects).
        """
        if not self.mapping_for_flow:
            raise ValueError("No write mapping found for Salesforce flow")

        streams = self.list_available_streams()
        logger.info(f"Salesforce write: evaluating {len(streams)} streams; only 'contacts' will be written")

        for stream in streams:
            if stream == "contacts":
                # Special handling for contacts (split into Contacts/Leads)
                self._handle_contacts_write()
            else:
                logger.info(f"Salesforce write: skipping non-contact stream '{stream}' by policy")

    def _handle_contacts_write(self) -> None:
        """
        Handle contacts write with Salesforce-specific Contact/Lead split.
        
        This method implements the business logic to split contacts into:
        - Contacts: Records with existing account associations
        - Leads: Records without account associations
        
        Args:
            mapping: Field mapping configuration
        """
        logger.info("Processing contacts with Contact/Lead split for Salesforce")
        
        # A missing/empty accounts snapshot must not block contacts: with
        # dynamic_contact_mapping on, accountless contacts route to Lead instead.
        sent_accounts = self.read_snapshot(self.stream_name_mapping.get('accounts', 'Account'))
        if sent_accounts is None or sent_accounts.empty:
            logger.info(
                "No accounts sent yet; proceeding with contacts "
                "(accountless contacts route to Lead when dynamic_contact_mapping is enabled)"
            )
            sent_accounts = pd.DataFrame(columns=["AccountId", "RemoteAccountId"])
        else:
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

        # Connector-keyed mapping so Contact and Lead keep distinct field maps
        # (build_write_mapping collapses both onto the `contacts` target).
        mapping_by_connector = {}
        for key, fields in self.mapping_for_flow.items():
            parts = key.split("/")
            if len(parts) == 2:
                mapping_by_connector[parts[1]] = fields

        # Process both Contact and Lead streams
        for sfdc_stream in ["Contact", "Lead"]:
            if sfdc_stream not in mapping_by_connector:
                logger.info(f"No mapping configured for {sfdc_stream}, skipping")
                continue
            self._process_contact_stream(
                sfdc_stream, contacts_df, leads_df, sent_accounts, mapping_by_connector
            )
    
    def _process_contact_stream(
        self,
        stream_type: str,
        contacts_df: Optional[pd.DataFrame],
        leads_df: Optional[pd.DataFrame],
        sent_accounts: pd.DataFrame,
        mapping_by_connector: Dict,
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

        if stream_type not in mapping_by_connector:
            logger.warning(f"No mapping found for {stream_type}, skipping")
            return

        # Apply field mapping (CBX1 -> Salesforce field names) using this object's map
        stream_columns, df = map_stream_data(df, stream_type, mapping_by_connector)
        new_data = df.copy()

        # Create snapshot for tracking (keyed by Salesforce object: Contact / Lead)
        snap = self.write_snapshot(df, stream_type)
        if "hash" in snap.columns:
            snap = snap.drop(columns=["hash"])

        # For Contacts (not Leads), attach the Salesforce account id. Left-join so a Contact
        # whose account hasn't synced isn't silently dropped.
        if stream_type == "Contact" and "AccountId" in snap.columns and not sent_accounts.empty:
            snap = snap.merge(sent_accounts, on="AccountId", how="left")
            snap = snap.rename(
                columns={"AccountId": "InputAccountId", "RemoteAccountId": "AccountId"}
            )

        # Prepare final output
        available = [c for c in set(stream_columns) if c in snap.columns]
        df_out = snap[available].copy()

        # Filter out already sent records (snapshot keyed by Salesforce object)
        sent_contacts = self.read_snapshot(stream_type)
        df_out = drop_sent_records("contacts", df_out, sent_contacts, new_data)

        # Write to output (Salesforce object stream name)
        self.write_to_singer(df_out, stream_type)
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
        
        # Route connector streams (Account/Contact/Lead) to their CBX1 target from the mapping
        # keys. Contact and Lead both map to `contacts`; the backend owns field mapping.
        connector_to_target = {}
        for key in self.mapping_for_flow:
            parts = key.split("/")
            if len(parts) == 2:
                target, connector = parts
                connector_to_target[connector] = target

        target_streams = [s for s in data_streams if s in connector_to_target]
        if not target_streams:
            logger.warning(
                "No Salesforce streams matched the mapping (received=%s, mapped=%s)",
                data_streams, list(connector_to_target.keys()),
            )
        for stream in target_streams:
            self._process_read_stream(stream, connector_to_target[stream])
    
    def _process_read_stream(self, stream: str, output_stream: str) -> None:
        """
        Process a single Salesforce connector stream for the read (CRM -> CBX1) path.

        Mirrors the HubSpot read path: enrich + wrap with RAW Salesforce field names; the
        backend owns SF->CBX1 field mapping. The only field we synthesise here is `domain`
        (Salesforce has no domain field where HubSpot companies do), because the account
        lookupKey requires it.

        Args:
            stream: Salesforce connector stream name (e.g. "Account", "Contact", "Lead")
            output_stream: CBX1 target stream the records belong to ("accounts"/"contacts")
        """
        logger.info(f"Processing read stream: {stream} -> {output_stream}")

        stream_data = self.get_stream_data(stream)

        # Drop Salesforce soft-deleted records (parallels HubSpot's archived filter)
        stream_data = self._filter_deleted_records(stream_data)

        if output_stream == "accounts":
            # Salesforce has no domain field; synthesise one from Website for the lookupKey.
            stream_data = self._derive_domain_from_website(stream_data)
            lookup_field = "domain"
        else:
            stream_data = self._update_contact_account_ids(stream_data)
            lookup_field = "Email"  # contacts/leads match by email

        # Normalise any dot-notation fields into nested objects
        stream_data = transform_dot_notation_to_nested(stream_data)

        # Wrap for cbx1-target; raw Salesforce field names stay inside `data` for the backend to map.
        wrapped = self.wrap_records_with_metadata(
            stream_data, output_stream, source="SALESFORCE", id_field="Id", lookup_field=lookup_field
        )
        self._write_read_output(wrapped, output_stream)

    def _filter_deleted_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove Salesforce soft-deleted records (IsDeleted == true)."""
        if df is None or df.empty or "IsDeleted" not in df.columns:
            return df
        mask = df["IsDeleted"].fillna(False).astype(bool)
        removed = int(mask.sum())
        if removed:
            logger.info("Salesforce read: skipped %d IsDeleted record(s)", removed)
        return df.loc[~mask].copy()
    
    def _derive_domain_from_website(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Derive a clean `domain` from the raw Salesforce `Website` field.

        Salesforce Accounts have no domain field, only Website (e.g. "http://edgecomm.com",
        "www.burlington.com"). CBX1 matches accounts by domain, so we normalise Website into
        a bare host ("edgecomm.com", "burlington.com") and add it as `domain`, leaving the
        raw Website in place for the backend mapping.
        """
        import re

        if df is None or df.empty or "Website" not in df.columns:
            return df

        def _clean(value):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            host = str(value).strip()
            if not host:
                return None
            host = re.sub(r"^https?://", "", host, flags=re.IGNORECASE)
            host = host.split("/")[0].split("?")[0]
            if host.lower().startswith("www."):
                host = host[4:]
            return host.lower() or None

        df = df.copy()
        df["domain"] = df["Website"].apply(_clean)
        return df

    def _update_contact_account_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Update Contact/Lead account IDs with CBX1 account IDs.

        Args:
            df: Contact or Lead DataFrame with a Salesforce AccountId field

        Returns:
            DataFrame with updated account IDs
        """
        # Resolve the raw Salesforce AccountId -> CBX1 account id via the accounts
        # snapshot, exposed as `accountId`; the raw AccountId is left untouched.
        if df is None or df.empty or "AccountId" not in df.columns:
            return df

        account_snapshots = self.read_snapshot("Account")
        if account_snapshots is None or account_snapshots.empty:
            return df

        acc = account_snapshots.rename(
            columns={"InputId": "cbx1_account_id", "RemoteId": "salesforce_account_id"}
        ).copy()

        df = df.copy()
        df["_sf_account_id"] = df["AccountId"].astype(str)
        acc["salesforce_account_id"] = acc["salesforce_account_id"].astype(str)

        df = df.merge(
            acc[["salesforce_account_id", "cbx1_account_id"]],
            left_on="_sf_account_id",
            right_on="salesforce_account_id",
            how="left",
        )
        df["accountId"] = df["cbx1_account_id"]
        df = df.drop(
            columns=["_sf_account_id", "salesforce_account_id", "cbx1_account_id"], errors="ignore"
        )
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
