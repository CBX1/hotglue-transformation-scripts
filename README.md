# HotGlue Transformation Scripts

This repository contains transformation scripts for integrating CBX1 App with CRM systems (HubSpot and Salesforce) using the HotGlue platform.

## Overview

The transformation scripts handle bidirectional data synchronization between CBX1 and CRM systems, with support for:
- **Salesforce** integration with dynamic contact/lead mapping
- **HubSpot** integration with association handling
- Field mapping and data transformation
- Duplicate prevention and incremental sync
- Account-contact relationship management

## Architecture

### Core Components

1. **`etl.py`** - Main transformation pipeline
2. **`utils.py`** - Utility functions for data processing
3. **Configuration files** - Mapping and connector configurations

### Data Flow

The system supports two job types:

#### Write Jobs (`JOB_TYPE=write`)
**CBX1 → CRM Systems**
- Reads data from CBX1 via sync-output
- Applies field mappings from tenant configuration
- Handles account-contact relationships
- Outputs to CRM systems via Singer format

#### Read Jobs (`JOB_TYPE=read`)
**CRM Systems → CBX1**
- Reads data from CRM systems
- Transforms to CBX1 format
- Injects CRM system identifiers
- Maintains remote ID associations

## Directory Structure

```
salesforce/
├── etl.py              # Main transformation script
├── utils.py            # Utility functions
├── requirements.txt    # Python dependencies
├── sync-output/        # Input data from HotGlue
├── snapshots/          # Tracking previously sent records
├── etl-output/         # Output data in Singer format
└── config.json         # Tap configuration (optional)
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `JOB_TYPE` | Job type: `write` or `read` | `write` |
| `FLOW` | Flow identifier | `AJ3x0LMYI` |
| `ROOT_DIR` | Root directory path | `.` |
| `CONNECTOR_ID` | Connector identifier | `salesforce` |
| `JOB_ROOT` | S3 root path | - |

### Tenant Configuration

Located in `snapshots/tenant-config.json`:

```json
{
  "hotglue_mapping": {
    "mapping": {
      "FLOW_ID": {
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
    }
  }
}
```

## Data Processing Features

### 1. Field Mapping System

The system uses a flexible mapping configuration to transform fields between CBX1 and CRM systems:

- **Target API → Connector** (Write jobs)
- **Connector → Target API** (Read jobs)
- Automatic `remote_id` field handling
- Support for nested field mappings

### 2. Contact/Lead Splitting (Salesforce)

For Salesforce with `dynamic_contact_mapping` enabled:

- **Contacts**: Records with valid account associations
- **Leads**: Records without account associations (marked as "missing")
- Automatic stream separation based on account availability

### 3. Account-Contact Relationships

- Validates account existence before processing contacts
- Maintains account ID mappings between systems
- Handles HubSpot associations with proper formatting
- Skips contact processing if no accounts are available

### 4. Duplicate Prevention

- Tracks previously sent records in snapshots
- Compares record hashes to detect changes
- Only processes new or updated records
- Maintains sync state across runs

### 5. Data Transformation

#### Write Jobs (CBX1 → CRM)
- Maps CBX1 fields to CRM fields
- Adds `externalId` for tracking
- Handles account associations
- Formats HubSpot associations as nested objects

#### Read Jobs (CRM → CBX1)
- Maps CRM fields to CBX1 fields
- Injects `crmSystem` identifier (SALESFORCE/HUBSPOT)
- Renames `remote_id` to `crmAssociationId`
- Maintains bidirectional ID mapping

## Utility Functions

### `map_stream_data(stream_data, stream, mapping)`
Transforms data fields based on mapping configuration.

**Parameters:**
- `stream_data`: Source DataFrame
- `stream`: Stream name
- `mapping`: Field mapping configuration

**Returns:** `(stream_columns, transformed_data)`

### `drop_sent_records(stream, stream_data, sent_data, new_data)`
Filters out previously sent records to prevent duplicates.

**Parameters:**
- `stream`: Stream name
- `stream_data`: Current data
- `sent_data`: Previously sent records
- `new_data`: New/updated records

**Returns:** Filtered DataFrame

### `split_contacts_by_account(contacts, sent_accounts)`
Splits contact records into contacts and leads based on account association.

**Parameters:**
- `contacts`: All contact records
- `sent_accounts`: Available account records

**Returns:** `(contacts_df, leads_df)`

### `get_contact_data(connector_id, stream, contacts, leads)`
Returns appropriate contact data based on connector and stream type.

**Parameters:**
- `connector_id`: Connector identifier
- `stream`: Stream type (Contact/Lead)
- `contacts`: Contact records
- `leads`: Lead records

**Returns:** Appropriate DataFrame

## Supported CRM Systems

### Salesforce
- **Streams**: Account, Contact, Lead
- **Features**: Dynamic contact/lead mapping, field-level mapping
- **Special Handling**: Account-based contact splitting

### HubSpot
- **Streams**: Account (Company), Contact
- **Features**: Association handling, nested object formatting
- **Special Handling**: HubSpot-specific association format

## Error Handling

- Graceful handling of missing configurations
- Fallback to default values when configs are unavailable
- Comprehensive logging for debugging
- Continues processing even with partial failures

## Dependencies

- **gluestick**: HotGlue SDK for data processing
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical operations
- **json/os**: Configuration and file handling

## Usage Examples

### Running a Write Job
```bash
export JOB_TYPE=write
export FLOW=AJ3x0LMYI
export CONNECTOR_ID=salesforce
python etl.py
```

### Running a Read Job
```bash
export JOB_TYPE=read
export FLOW=AJ3x0LMYI
export CONNECTOR_ID=hubspot
python etl.py
```

## Monitoring and Debugging

### Log Output
The script provides detailed logging for:
- Job type and flow identification
- Mapping configuration status
- Stream processing progress
- Error conditions and warnings

### Snapshot Tracking
- Records are tracked in the `snapshots/` directory
- Each stream maintains its own snapshot file
- Snapshots include input/remote ID mappings
- Used for duplicate detection and incremental sync

## Best Practices

1. **Configuration Management**: Ensure tenant-config.json is properly configured
2. **Account Dependencies**: Process accounts before contacts
3. **Error Monitoring**: Monitor logs for mapping and processing errors
4. **Incremental Sync**: Leverage snapshots for efficient data processing
5. **Testing**: Test with small datasets before full sync operations

## Troubleshooting

### Common Issues

1. **No accounts sent**: Contacts processing skipped if no accounts available
2. **Missing mapping**: Streams processed without transformation if mapping unavailable
3. **Configuration errors**: Check tenant-config.json format and field mappings
4. **Connector mismatch**: Verify CONNECTOR_ID matches expected values

### Debug Steps

1. Check environment variables
2. Verify configuration files exist and are valid
3. Review log output for specific error messages
4. Validate input data format and structure
5. Ensure proper account-contact relationships
