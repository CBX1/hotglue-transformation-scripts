import pandas as pd
from typing import Tuple, Optional
import gluestick as gs


def get_stream_data(reader: gs.Reader, stream: str, catalog_types: bool = True):
    """
    Wrapper function to get stream data with catalog_types=True by default.
    
    This ensures consistent behavior across all reader.get() calls without
    requiring developers to remember to pass catalog_types=True.
    
    Args:
        reader: The gluestick Reader instance
        stream: Name of the stream to read
        catalog_types: Whether to use catalog types (defaults to True)
        
    Returns:
        DataFrame with the stream data
    """
    return reader.get(stream, catalog_types=catalog_types)


def map_stream_data(stream_data: pd.DataFrame, stream: str, mapping: dict) -> Tuple[list, pd.DataFrame]:
    """
    Maps data from source format to target format based on provided mapping configuration.
    
    Args:
        stream_data: DataFrame containing the source data
        stream: Name of the data stream being processed
        mapping: Mapping configuration that defines field mappings between source and target
        
    Returns:
        (stream_columns, stream_data) where stream_columns is list of mapped column names
        and stream_data is the transformed DataFrame.
    
    Raises:
        ValueError: If mapping configuration is missing or invalid
    """
    # Validate inputs
    if not mapping:
        raise ValueError("Mapping configuration is required")
    
    if stream not in mapping:
        raise ValueError(f"No mapping found for stream: {stream}")
    
    stream_mapping = dict(mapping.get(stream))  # shallow copy to avoid side-effects
    
    # Ensure remote_id is set in mapping (do not mutate caller's dict)
    stream_mapping.setdefault("remote_id", "Id")

    target_api_columns = list(stream_mapping.keys())
    connector_columns = list(stream_mapping.values())

    columns_to_rename = [
        column
        for column in stream_data.columns
        if column in target_api_columns
    ]
    
    # Rename columns using mapping configuration
    for column in columns_to_rename:
        try:
            target_column = connector_columns[target_api_columns.index(column)]
            stream_data[target_column] = stream_data[column]
        except Exception as e:
            print(f"Warning: Error renaming column {column}: {str(e)}")

    stream_columns = [
        col for col in connector_columns if col in stream_data.columns
    ]

    # Map id => external_id
    if "id" in stream_data.columns:
        stream_columns.append("externalId")
        stream_data["externalId"] = stream_data["id"]

    # Remove duplicates while preserving order
    unique_columns = []
    for col in stream_columns:
        if col not in unique_columns:
            unique_columns.append(col)
    
    stream_data = stream_data[unique_columns]

    return unique_columns, stream_data


def drop_sent_records(
    stream: str,
    stream_data: pd.DataFrame,
    sent_data: Optional[pd.DataFrame],
    new_data: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Filters out records that have already been sent to avoid duplicates.
    
    Args:
        stream (str): Name of the data stream being processed
        stream_data (pd.DataFrame): DataFrame containing the current data
        sent_data (pd.DataFrame): DataFrame containing previously sent records
        new_data (pd.DataFrame, optional): DataFrame containing new/updated records
        
    Returns:
        Filtered DataFrame containing only unsent/updated records
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

    return stream_data[condition]


def split_contacts_by_account(contacts: pd.DataFrame, sent_accounts: pd.DataFrame):
    """
    Splits contact records into contacts and leads based on account association.
    
    Args:
        contacts (pd.DataFrame): DataFrame containing all contact records
        sent_accounts (pd.DataFrame): DataFrame containing account records that have been sent
        
    Returns:
        (contacts_df, leads_df) where contacts_df contains records with existing accounts
        and leads_df contains records with missing accounts
    
    Raises:
        ValueError: If required columns are missing from input DataFrames
    """
    # Validate required columns
    if "accountId" not in contacts.columns:
        raise ValueError("accountId column is required in contacts DataFrame")
    
    if "AccountId" not in sent_accounts.columns or "RemoteAccountId" not in sent_accounts.columns:
        raise ValueError("AccountId and RemoteAccountId columns are required in sent_accounts DataFrame")
    
    contacts_data = contacts.copy()
    contacts_data = contacts_data.merge(
        sent_accounts, left_on="accountId", right_on="AccountId", how="left"
    )
    
    # Handle cases where account mapping might be missing
    contacts_data["RemoteAccountId"] = contacts_data["RemoteAccountId"].fillna("missing")
    
    contacts_df = contacts_data[contacts_data["RemoteAccountId"] != "missing"].copy()
    leads_df = contacts_data[contacts_data["RemoteAccountId"] == "missing"].copy()
    
    # Drop AccountId columns to avoid duplication
    if "AccountId" in contacts_df.columns:
        contacts_df.drop(columns=['AccountId', 'RemoteAccountId'], inplace=True, errors="ignore")
    
    if "AccountId" in leads_df.columns:
        leads_df.drop(columns=['AccountId', 'RemoteAccountId'], inplace=True, errors="ignore")
    
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
    Gets the appropriate contact data based on connector ID and stream type.
    
    Args:
        connector_id (str): ID of the connector being used (e.g. 'salesforce', 'hubspot')
        stream (str): Type of stream ('Contact' or 'Lead') # only for salesforce
        contacts (pd.DataFrame): DataFrame containing contact records
        leads (pd.DataFrame): DataFrame containing lead records
        
    Returns:
        pd.DataFrame: Contact or lead data based on connector and stream type.
        Returns contacts for non-Salesforce connectors, and the appropriate
        stream data (contacts/leads) for Salesforce.
    
    Raises:
        ValueError: If stream type is invalid for Salesforce connector
    """
    if connector_id == "salesforce":
        if stream not in ["Contact", "Lead"]:
            raise ValueError(f"Invalid stream type for Salesforce: {stream}. Expected 'Contact' or 'Lead'.")
        
        # Return appropriate data based on stream type
        if stream == "Contact":
            return contacts if contacts is not None else pd.DataFrame()
        else:  # stream == "Lead"
            return leads if leads is not None else pd.DataFrame()
    else:
        # For non-Salesforce connectors, always return contacts
        return contacts if contacts is not None else pd.DataFrame()
