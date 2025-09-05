import pandas as pd
from typing import Tuple, Optional


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
