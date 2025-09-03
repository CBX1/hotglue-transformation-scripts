# Salesforce Integration - CBX1 Transformation Scripts

This directory contains the Salesforce-specific transformation scripts for integrating CBX1 with Salesforce CRM using HotGlue.

## Files Overview

- **`etl.py`** - Main transformation pipeline for Salesforce integration
- **`utils.py`** - Utility functions for data processing and transformation
- **`requirements.txt`** - Python dependencies
- **`snapshots/`** - Directory containing tracking data and configuration
- **`sync-output/`** - Input data from HotGlue sync process
- **`etl-output/`** - Transformed data output in Singer format

## Salesforce-Specific Features

### Dynamic Contact/Lead Mapping

When `dynamic_contact_mapping` is enabled in the tap configuration, the system automatically splits contact records:

- **Contacts**: Records with valid account associations
- **Leads**: Records without account associations (AccountId marked as "missing")

This allows proper handling of Salesforce's distinct Contact and Lead objects.

### Account Dependency Management

The transformation ensures proper account-contact relationships:

1. **Account Processing First**: Accounts are processed and sent before contacts
2. **Account Validation**: Only contacts with valid account references are processed as Contacts
3. **Lead Creation**: Contacts without valid accounts become Leads
4. **Relationship Mapping**: Maintains AccountId relationships between systems

### Field Mapping Configuration

Example mapping configuration for Salesforce:

```json
{
  "accounts/Account": {
    "name": "Name",
    "domain": "Website", 
    "summary": "Description"
  },
  "contacts/Contact": {
    "firstName": "FirstName",
    "lastName": "LastName",
    "email": "Email",
    "accountId": "AccountId"
  }
}
```

## Data Flow

### Write Jobs (CBX1 → Salesforce)

1. **Load Configuration**: Read tenant mapping and tap configuration
2. **Process Accounts**: Transform and send account data first
3. **Split Contacts**: Separate contacts and leads based on account availability
4. **Transform Data**: Apply field mappings for each stream
5. **Filter Duplicates**: Skip previously sent records using snapshots
6. **Output Data**: Generate Singer format for HotGlue consumption

### Read Jobs (Salesforce → CBX1)

1. **Load Data**: Read Salesforce data from sync-output
2. **Apply Mappings**: Transform Salesforce fields to CBX1 format
3. **Inject Metadata**: Add CRM system identifier and association IDs
4. **Merge Snapshots**: Include CBX1 IDs from previous sync operations
5. **Output Data**: Generate CBX1-compatible format

## Configuration Files

### Tenant Configuration (`snapshots/tenant-config.json`)

Contains the field mapping configuration:

```json
{
  "hotglue_mapping": {
    "mapping": {
      "FLOW_ID": {
        "accounts/Account": { /* field mappings */ },
        "contacts/Contact": { /* field mappings */ }
      }
    }
  }
}
```

### Tap Configuration (`config.json`)

Optional configuration for Salesforce tap:

```json
{
  "dynamic_contact_mapping": true,
  "other_salesforce_settings": "..."
}
```

## Stream Processing Logic

### Accounts Stream
- Direct field mapping and transformation
- Snapshot tracking for duplicate prevention
- Required before contact processing

### Contacts Stream (with dynamic mapping)
- **Step 1**: Validate account dependencies
- **Step 2**: Split into Contact and Lead streams
- **Step 3**: Process each stream separately
- **Step 4**: Apply appropriate field mappings
- **Step 5**: Handle account relationships

### Standard Streams
- Apply field mappings directly
- Filter duplicates using snapshots
- Output in Singer format

## Error Handling

### Missing Accounts
If no accounts have been sent yet:
```
"No accounts have been sent yet, skipping contacts export."
```

### Configuration Issues
- Graceful fallback when tenant-config.json is missing
- Default processing when mappings are unavailable
- Comprehensive logging for debugging

### Data Validation
- Checks for required fields before processing
- Validates account relationships
- Handles missing or malformed data gracefully

## Monitoring and Debugging

### Key Log Messages
- Job type and flow identification
- Account processing status
- Contact/lead splitting results
- Mapping application success/failure
- Duplicate filtering statistics

### Snapshot Files
- `Account_{FLOW_ID}.snapshot.csv` - Sent account records
- `contacts_{FLOW_ID}.snapshot.csv` - Sent contact records
- Used for duplicate detection and relationship mapping

## Usage Examples

### Enable Dynamic Contact Mapping
```bash
# Add to config.json
{
  "dynamic_contact_mapping": true
}
```

### Run Salesforce Write Job
```bash
export JOB_TYPE=write
export FLOW=AJ3x0LMYI
export CONNECTOR_ID=salesforce
python etl.py
```

### Run Salesforce Read Job
```bash
export JOB_TYPE=read
export FLOW=AJ3x0LMYI  
export CONNECTOR_ID=salesforce
python etl.py
```

## Troubleshooting

### Common Issues

1. **Contacts Skipped**: Ensure accounts are processed first
2. **Missing Leads**: Check if `dynamic_contact_mapping` is enabled
3. **Field Mapping Errors**: Verify tenant-config.json format
4. **Account Relationships**: Validate AccountId fields in source data

### Debug Steps

1. Check if accounts snapshot exists
2. Verify dynamic_contact_mapping setting
3. Review field mapping configuration
4. Validate input data structure
5. Monitor log output for specific errors
