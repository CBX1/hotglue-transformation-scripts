#!/usr/bin/env python
# coding: utf-8

"""
Utility functions for HotGlue ETL operations.

This module provides common utility functions used across the ETL pipeline for:
- Data reading and transformation
- Field mapping and column renaming
- Deduplication and snapshot management
- Data type conversions and formatting
- Nested structure transformations

These utilities are designed to work with the HotGlue framework and support
both Salesforce and HubSpot connector operations.
"""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
import gluestick as gs

logger = logging.getLogger(__name__)


def get_stream_data(reader: gs.Reader, stream: str, catalog_types: bool = True) -> pd.DataFrame:
    """
    Retrieve data for a specific stream from the HotGlue reader.
    
    This wrapper ensures consistent behavior across all data reading operations
    by defaulting catalog_types to True, which preserves data type information
    from the source catalog.
    
    Args:
        reader: The gluestick Reader instance for accessing input data
        stream: Name of the stream to read (e.g., 'contacts', 'accounts')
        catalog_types: Whether to use catalog-defined types (defaults to True)
        
    Returns:
        DataFrame containing the stream data with appropriate typing
        
    Example:
        >>> reader = gs.Reader('/path/to/data')
        >>> contacts_df = get_stream_data(reader, 'contacts')
    """
    return reader.get(stream, catalog_types=catalog_types)


def map_stream_data(
    stream_data: pd.DataFrame, 
    stream: str, 
    mapping: Dict[str, Dict]
) -> Tuple[List[str], pd.DataFrame]:
    """
    Transform data columns based on field mapping configuration.
    
    This function applies field mappings to transform source data into the target
    format required by the destination system. It handles column renaming,
    external ID generation, and ensures all required fields are present.
    
    Args:
        stream_data: DataFrame containing the source data to be transformed
        stream: Name of the data stream being processed (e.g., 'contacts', 'accounts')
        mapping: Nested dictionary with stream-specific field mappings
                Format: {stream: {source_field: target_field, ...}}
        
    Returns:
        Tuple containing:
        - List of column names in the transformed data
        - DataFrame with renamed columns and external ID added
    
    Raises:
        ValueError: If mapping is missing or no mapping found for the stream
        
    Example:
        >>> mapping = {'contacts': {'firstName': 'FirstName', 'email': 'Email'}}
        >>> columns, df = map_stream_data(contacts_df, 'contacts', mapping)
    """
    # Validate inputs
    if not mapping:
        raise ValueError("Mapping configuration is required")
    
    if stream not in mapping:
        raise ValueError(f"No mapping found for stream: {stream}")
    
    # Create a copy to avoid mutating the original mapping
    stream_mapping = dict(mapping.get(stream))
    
    # Ensure remote_id mapping is present (standard field for ID tracking)
    stream_mapping.setdefault("remote_id", "Id")

    target_api_columns = list(stream_mapping.keys())
    connector_columns = list(stream_mapping.values())

    # Preserve the original 'id' column value BEFORE mapping
    # This is critical for write operations where we need to track the source system ID
    original_id = stream_data["id"].copy() if "id" in stream_data.columns else None

    # Identify columns that need to be renamed
    columns_to_rename = [
        column for column in stream_data.columns
        if column in target_api_columns
    ]

    # Apply column renaming based on mapping
    for column in columns_to_rename:
        try:
            target_column = connector_columns[target_api_columns.index(column)]
            stream_data[target_column] = stream_data[column]
        except Exception as e:
            logger.warning(f"Error renaming column {column}: {str(e)}")

    # Build list of columns present in the data
    stream_columns = [
        col for col in connector_columns if col in stream_data.columns
    ]

    # Add external ID for tracking (maps internal ID to external system)
    # Use the ORIGINAL id value (before mapping), not the mapped value
    if original_id is not None:
        stream_columns.append("externalId")
        stream_data["externalId"] = original_id

    # Remove duplicate column names while preserving order
    unique_columns = []
    for col in stream_columns:
        if col not in unique_columns:
            unique_columns.append(col)
    
    # Return only the mapped columns
    stream_data = stream_data[unique_columns].copy()

    return unique_columns, stream_data


def prepare_for_singer(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Prepare DataFrame for Singer format output.
    
    Singer is a specification for data exchange that requires specific formatting,
    particularly for datetime fields. This function ensures all datetime columns
    are properly serialized to ISO 8601 format strings.
    
    Args:
        df: DataFrame to prepare for Singer output
        
    Returns:
        DataFrame with datetime columns converted to Singer-compatible strings
        
    Raises:
        ValueError: If input is None instead of a DataFrame
        
    Note:
        Singer format requires datetime fields to be in ISO 8601 format:
        YYYY-MM-DDTHH:MM:SS.ffffffZ
    """
    if df is None:
        raise ValueError("Expected DataFrame, received None")

    prepared_df = df.copy()
    
    # Find all datetime columns
    datetime_cols = prepared_df.select_dtypes(include=["datetime", "datetimetz"]).columns

    # Convert datetime columns to ISO 8601 string format
    for col in datetime_cols:
        series = prepared_df[col]
        formatted = series.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ").where(series.notna(), None)
        prepared_df = prepared_df.astype({col: "object"})
        prepared_df.loc[:, col] = formatted

    return prepared_df


def drop_sent_records(
    stream: str,
    stream_data: pd.DataFrame,
    sent_data: Optional[pd.DataFrame],
    new_data: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Filter out records that have already been sent to prevent duplicates.
    
    This deduplication function compares current data against historical snapshots
    to identify records that have not been sent yet. For contacts, it also
    includes records that have been updated since the last sync.
    
    Args:
        stream: Name of the data stream being processed (e.g., 'contacts', 'accounts')
        stream_data: DataFrame containing the current batch of data
        sent_data: DataFrame containing previously sent records from snapshots
        new_data: Optional DataFrame containing records with updates (used for contacts)
        
    Returns:
        DataFrame containing only records that haven't been sent or have updates
        
    Raises:
        ValueError: If required ID columns are missing
        
    Note:
        - Uses 'externalId' in stream_data and 'InputId' in sent_data for comparison
        - For contacts stream, includes updated records even if previously sent
    """
    # If no sent data, return all stream data
    if sent_data is None:
        return stream_data
    
    # Validate required columns exist
    if "externalId" not in stream_data.columns:
        raise ValueError("externalId column is required in stream_data")
    
    if "InputId" not in sent_data.columns:
        raise ValueError("InputId column is required in sent_data")
    
    # Filter out records that have already been sent
    condition = ~stream_data["externalId"].isin(sent_data["InputId"])

    # For contacts stream, also include records with updates
    if stream == "contacts" and new_data is not None:
        if "externalId" in new_data.columns:
            condition = condition | (stream_data["externalId"].isin(new_data["externalId"]))

    return stream_data.loc[condition].copy()


def split_contacts_by_account(
    contacts: pd.DataFrame, 
    sent_accounts: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split contacts into Contacts and Leads based on account associations.
    
    Salesforce distinguishes between Contacts (associated with accounts) and
    Leads (not yet associated with accounts). This function performs that split
    based on whether a contact has a valid account association.
    
    Args:
        contacts: DataFrame containing all contact records
        sent_accounts: DataFrame containing accounts that have been synced
                      Must contain 'AccountId' and 'RemoteAccountId' columns
        
    Returns:
        Tuple of (contacts_df, leads_df):
        - contacts_df: Records with valid account associations
        - leads_df: Records without account associations (will become Leads)
    
    Raises:
        ValueError: If required columns are missing from input DataFrames
        
    Note:
        This is specific to Salesforce's Contact/Lead model. Other CRMs
        may not make this distinction.
    """
    # Validate required columns
    if "accountId" not in contacts.columns:
        raise ValueError("accountId column is required in contacts DataFrame")
    
    if "AccountId" not in sent_accounts.columns or "RemoteAccountId" not in sent_accounts.columns:
        raise ValueError("AccountId and RemoteAccountId columns are required in sent_accounts DataFrame")
    
    # Join contacts with accounts to identify associations
    contacts_data = contacts.copy()
    contacts_data = contacts_data.merge(
        sent_accounts, left_on="accountId", right_on="AccountId", how="left"
    )
    
    # Mark records without account associations
    contacts_data["RemoteAccountId"] = contacts_data["RemoteAccountId"].fillna("missing")
    
    # Split based on account association
    contacts_df = contacts_data[contacts_data["RemoteAccountId"] != "missing"].copy()
    leads_df = contacts_data[contacts_data["RemoteAccountId"] == "missing"].copy()
    
    # Clean up temporary columns
    if "AccountId" in contacts_df.columns:
        contacts_df.drop(columns=['AccountId', 'RemoteAccountId'], inplace=True, errors="ignore")
    
    if "AccountId" in leads_df.columns:
        leads_df.drop(columns=['AccountId', 'RemoteAccountId'], inplace=True, errors="ignore")
    
    logger.info(f"Split {len(contacts)} contacts into {len(contacts_df)} Contacts and {len(leads_df)} Leads")
    
    return contacts_df, leads_df


def transform_dot_notation_to_nested(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform dot-notation and array notation fields to nested objects and arrays.
    
    Supports:
    - Simple dot notation: "hqLocation.city" -> {"hqLocation": {"city": "value"}}
    - Array notation: "fundingHistory[*].amount" -> {"fundingHistory": [{"amount": "value"}]}
    - Nested arrays: "fundingHistory[*].investors[*]" -> {"fundingHistory": [{"investors": ["value"]}]}
    
    Args:
        df: DataFrame containing columns with dot/array notation
        
    Returns:
        DataFrame with nested objects and arrays properly structured
    """
    if df.empty:
        return df
    
    # Find columns with dot notation or array notation
    special_columns = [col for col in df.columns if '.' in col or '[*]' in col]
    
    if not special_columns:
        return df
    
    # Group columns by their structure
    nested_groups = {}
    array_groups = {}
    
    for col in special_columns:
        if '[*]' in col:
            # Handle array notation: "fundingHistory[*].amount" or "fundingHistory[*].investors[*]"
            _parse_array_column(col, array_groups)
        else:
            # Handle simple dot notation: "hqLocation.city"
            _parse_dot_column(col, nested_groups)
    
    # Create result DataFrame
    result_df = df.copy()
    
    # Process simple nested objects first
    for parent, children in nested_groups.items():
        nested_data = []
        for _, row in df.iterrows():
            nested_obj = {}
            for child_key, original_col in children.items():
                if original_col in row and pd.notna(row[original_col]):
                    nested_obj[child_key] = row[original_col]
            nested_data.append(nested_obj if nested_obj else None)
        
        result_df[parent] = nested_data
        result_df = result_df.drop(columns=list(children.values()))
    
    # Process array structures
    for array_parent, array_structure in array_groups.items():
        array_data = []
        for _, row in df.iterrows():
            array_obj = _build_array_structure(row, array_structure)
            array_data.append(array_obj if array_obj else None)
        
        result_df[array_parent] = array_data
        # Drop all original array columns
        cols_to_drop = _get_array_columns_to_drop(array_structure)
        result_df = result_df.drop(columns=cols_to_drop, errors='ignore')
    
    return result_df


def _parse_dot_column(col: str, nested_groups: dict):
    """Parse simple dot notation columns."""
    parts = col.split('.', 1)
    parent = parts[0]
    child = parts[1]
    
    if parent not in nested_groups:
        nested_groups[parent] = {}
    nested_groups[parent][child] = col


def _parse_array_column(col: str, array_groups: dict):
    """Parse array notation columns like 'fundingHistory[*].amount' or 'fundingHistory[*].investors[*]'."""
    # Split on first [*] to get the array parent
    if '[*]' not in col:
        return
    
    parts = col.split('[*]', 1)
    array_parent = parts[0]
    remainder = parts[1]
    
    if array_parent not in array_groups:
        array_groups[array_parent] = {'fields': {}, 'nested_arrays': {}}
    
    if remainder.startswith('.'):
        # Remove leading dot
        remainder = remainder[1:]
        
        if '[*]' in remainder:
            # Nested array case: "investors[*]"
            nested_parts = remainder.split('[*]', 1)
            nested_array_name = nested_parts[0]
            
            if nested_array_name not in array_groups[array_parent]['nested_arrays']:
                array_groups[array_parent]['nested_arrays'][nested_array_name] = []
            
            array_groups[array_parent]['nested_arrays'][nested_array_name].append(col)
        else:
            # Simple field case: "amount"
            array_groups[array_parent]['fields'][remainder] = col


def _build_array_structure(row: pd.Series, array_structure: dict) -> list:
    """Build array structure for a single row."""
    result = []
    
    # Collect all non-null values for this array
    all_values = {}
    
    # Process simple fields
    for field_name, original_col in array_structure['fields'].items():
        if original_col in row and pd.notna(row[original_col]):
            all_values[field_name] = row[original_col]
    
    # Process nested arrays
    for nested_array_name, columns in array_structure['nested_arrays'].items():
        nested_values = []
        for col in columns:
            if col in row and pd.notna(row[col]):
                nested_values.append(row[col])
        if nested_values:
            all_values[nested_array_name] = nested_values
    
    # If we have any values, create an array with one object containing all values
    # This is a simplified approach - in reality, you might need more complex logic
    # to handle multiple array items from different columns
    if all_values:
        result.append(all_values)
    
    return result


def _get_array_columns_to_drop(array_structure: dict) -> list:
    """Get list of all original columns that should be dropped after array transformation."""
    cols_to_drop = []
    
    # Add simple field columns
    cols_to_drop.extend(array_structure['fields'].values())
    
    # Add nested array columns
    for columns in array_structure['nested_arrays'].values():
        cols_to_drop.extend(columns)
    
    return cols_to_drop

def get_contact_data(
    connector_id: str,
    stream: str,
    contacts: Optional[pd.DataFrame],
    leads: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """
    Select the appropriate contact dataset based on connector and stream type.
    
    Different CRM systems handle contacts differently:
    - Salesforce: Distinguishes between Contacts (with accounts) and Leads (without)
    - HubSpot: Has a single contacts stream
    
    This function returns the correct dataset based on these requirements.
    
    Args:
        connector_id: CRM connector identifier ('salesforce', 'hubspot', etc.)
        stream: Stream type - 'Contact' or 'Lead' (only relevant for Salesforce)
        contacts: DataFrame containing contact records
        leads: DataFrame containing lead records (only used for Salesforce)
        
    Returns:
        DataFrame with the appropriate contact/lead data for the connector
        Returns empty DataFrame if the requested data is None
    
    Raises:
        ValueError: If stream type is invalid for Salesforce connector
        
    Example:
        >>> # For Salesforce Contact stream
        >>> df = get_contact_data('salesforce', 'Contact', contacts_df, leads_df)
        >>> # For HubSpot (stream parameter is ignored)
        >>> df = get_contact_data('hubspot', 'any', contacts_df, None)
    """
    if connector_id == "salesforce":
        if stream not in ["Contact", "Lead"]:
            raise ValueError(
                f"Invalid stream type for Salesforce: {stream}. "
                f"Expected 'Contact' or 'Lead'."
            )
        
        # Return appropriate data based on stream type
        if stream == "Contact":
            result = contacts if contacts is not None else pd.DataFrame()
            logger.debug(f"Returning {len(result)} Contact records for Salesforce")
            return result
        else:  # stream == "Lead"
            result = leads if leads is not None else pd.DataFrame()
            logger.debug(f"Returning {len(result)} Lead records for Salesforce")
            return result
    else:
        # For non-Salesforce connectors, always return contacts
        result = contacts if contacts is not None else pd.DataFrame()
        logger.debug(f"Returning {len(result)} contact records for {connector_id}")
        return result
