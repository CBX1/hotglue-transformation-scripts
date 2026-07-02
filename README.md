# HotGlue Transformation Scripts

This repository contains transformation scripts for integrating CBX1 App with CRM systems (Salesforce, HubSpot, Marketo) using the HotGlue platform.

> **⚠️ All code lives under `hubspot/`** (historical name — it hosts ALL connectors). Run `etl.py` from inside `hubspot/`, not the repo root.
>
> **End-to-end pipeline (tap → this ETL → target):** [`docs/architecture.md`](docs/architecture.md).

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
.
├── docs/
│   └── architecture.md         # End-to-end pipeline documentation
└── hubspot/                    # ← ALL connectors live here (historical name)
    ├── etl.py                  # Main orchestrator/entrypoint
    ├── base_handler.py         # Abstract base for connector handlers
    ├── salesforce_handler.py   # Salesforce write/read business logic
    ├── hubspot_handler.py      # HubSpot write/read business logic
    ├── marketo_handler.py      # Marketo write/read business logic
    ├── utils.py                # Shared helpers
    ├── requirements.txt        # Python dependencies
    ├── .hotgluerc              # HotGlue flow/env/tap config
    ├── tests/                  # pytest suite (run from repo root)
    ├── sync-output/            # Input data from HotGlue (sample fixtures committed)
    ├── snapshots/              # Tracking previously sent records + tenant-config.json
    └── etl-output/             # Output data in Singer format
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `JOB_TYPE` | Job type: `write` (CBX1 → CRM) or `read` (CRM → CBX1) | `write` |
| `FLOW` | Flow identifier used for mapping/snapshots | `AJ3x0LMYI` |
| `ROOT_DIR` | Base dir for `sync-output/`/`snapshots/`/`etl-output/` | `.` |
| `CONNECTOR_ID` | `salesforce`, `hubspot`, or `marketo` (required) | — |
| `JOB_ROOT` | S3 root path of the HotGlue job | — |

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

## Write Policy

- **Salesforce**: `contacts` are always written, split into `Contact` (account-linked) and `Lead` (accountless) by account linkage. `accounts` are written to the Salesforce `Account` object **when the flow has an `accounts/Account` mapping**. Contacts whose account has not yet synced to Salesforce are held back (not written as Leads) so they sync as `Contact`s once the account lands and the `Account` snapshot maps `CBX1-account-id → SF-Account-Id`. Other objects are not written.
- **HubSpot**: `contacts` are always written. `accounts` (HubSpot companies object) are written **only when the tenant has a `TenantEgestionMapping` configured for `ACCOUNT → HUBSPOT`** — the presence of an `"accounts"` key in `stream_name_mapping` is the opt-in signal. Other objects are not written.

## Supported CRM Systems

### Salesforce
- **Streams**: Account, Contact, Lead
- **Features**: Dynamic contact/lead mapping, field-level mapping
- **Special Handling**: Account-based contact splitting

### HubSpot
- **Streams**: Account (Company), Contact
- **Features**: Association handling, nested object formatting
- **Special Handling**: HubSpot-specific association format

### Marketo
- **Streams**: Contact-centric (see `hubspot/marketo_handler.py`)
- **Features**: Same handler interface as Salesforce/HubSpot

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

### Setup

```bash
cd hubspot
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**Important**: the HotGlue directories (`sync-output/`, `snapshots/`, `etl-output/`) must remain intact — idempotency and dedupe depend on them. Mapping configuration is tenant-driven and read from `snapshots/tenant-config.json` at runtime.

### Running a Write Job
```bash
cd hubspot
export JOB_TYPE=write
export FLOW=AJ3x0LMYI
export CONNECTOR_ID=salesforce
python etl.py
```

### Running a Read Job
```bash
cd hubspot
export JOB_TYPE=read
export FLOW=AJ3x0LMYI
export CONNECTOR_ID=hubspot
python etl.py
```

### Replicating a real HotGlue job locally

To debug a failed production/QA job with its exact input data and env vars, use the hotglue CLI (`hotglue etl setup-local-run` + `hotglue etl local-run`) — workflow documented in `.claude/skills/local-job-debugging/`.

### Running the tests
```bash
pytest hubspot/tests/    # from the repo root
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

## Contributing

- **Style**: PEP 8 — four-space indentation, snake_case functions/variables, CapWords classes. Add type hints to new helpers (present in critical paths). Reuse the shared logger configured in `etl.py`; prefer descriptive, imperative function names (e.g., `split_contacts_by_account`).
- **Commits**: brief, imperative titles (e.g., "Refactor into connector handlers").
- **PRs** should:
  - Summarize the scenario and expected outcomes
  - List files touched and mapping/config changes (especially `snapshots/tenant-config.json`)
  - Call out new/changed environment variables
  - Include before/after snippets or command output demonstrating correct behavior
