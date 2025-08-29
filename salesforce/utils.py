

def map_stream_data(stream_data, stream, mapping):
    """
    Maps data from source format to target format based on provided mapping configuration.
    
    Args:
        stream_data (pd.DataFrame): DataFrame containing the source data
        stream (str): Name of the data stream being processed
        mapping (dict): Mapping configuration that defines field mappings between source and target
        
    Returns:
        tuple: (stream_columns, stream_data) where stream_columns is list of mapped column names 
        and stream_data is the transformed DataFrame
    """
    stream_mapping = mapping.get(stream)
    connector_columns = list(stream_mapping.keys())

    if not mapping.get(stream).get("remote_id"):
        mapping[stream]["remote_id"] = "Id"

    target_api_columns = list(mapping.get(stream).keys())
    connector_columns = list(mapping.get(stream).values())

    columns_to_rename = [
        column
        for column in stream_data.columns
        if column in target_api_columns
    ]
    for column in columns_to_rename:
        try:
            stream_data[
                connector_columns[target_api_columns.index(column)]
            ] = stream_data[column]
        except:
            print("Error renaming columns")

    stream_columns = [
        col for col in connector_columns if col in stream_data.columns
    ]

    # Map id => external_id
    if "id" in stream_data.columns:
        stream_columns = stream_columns + ["externalId"]
        stream_data["externalId"] = stream_data["id"]

    stream_data = stream_data[list(set(stream_columns))]

    return stream_columns, stream_data


def drop_sent_records(stream=None, stream_data=None, sent_data=None, new_data=None):
    """
    Filters out records that have already been sent to avoid duplicates.
    
    Args:
        stream (str): Name of the data stream being processed
        stream_data (pd.DataFrame): DataFrame containing the current data
        sent_data (pd.DataFrame): DataFrame containing previously sent records
        new_data (pd.DataFrame): DataFrame containing new/updated records
        
    Returns:
        pd.DataFrame: Filtered DataFrame containing only unsent/updated records
    """
    if sent_data is not None:
        condition = ~stream_data["externalId"].isin(sent_data["InputId"])

        # We want to send contacts that have not been sent before OR have any updates
        if stream in ["contacts"]:
            condition = condition | (stream_data["externalId"].isin(new_data["externalId"]))

        stream_data = stream_data[condition]
    return stream_data


def split_contacts_by_account(contacts, sent_accounts):
    """
    Splits contact records into contacts and leads based on account association.
    
    Args:
        contacts (pd.DataFrame): DataFrame containing all contact records
        sent_accounts (pd.DataFrame): DataFrame containing account records that have been sent
        
    Returns:
        tuple: (contacts_df, leads_df) where contacts_df contains records with existing accounts 
        and leads_df contains records with missing accounts
    """
    contacts_data = contacts.copy()
    contacts_data = contacts_data.merge(
        sent_accounts, left_on="accountId", right_on="AccountId"
    )
    
    contacts = contacts_data[contacts_data["RemoteAccountId"] != "missing"]
    leads = contacts_data[contacts_data["RemoteAccountId"] == "missing"]
    
    # Drop AccountId columns
    for df in [contacts, leads]:
        df.drop(columns=['AccountId', 'RemoteAccountId'], inplace=True)
    
    return contacts, leads

def get_contact_data(connector_id, stream, contacts, leads):
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
    """
    if connector_id == "salesforce":
        return contacts if stream == "Contact" else leads
    else:
        return contacts