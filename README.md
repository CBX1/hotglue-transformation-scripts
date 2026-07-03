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

### How a job executes (mental model)

1. `etl.py` loads `snapshots/tenant-config.json` → `hotglue_mapping.mapping.{FLOW}` and derives `stream_name_mapping` from its `target/connector` keys (e.g. `"contacts/Contact"` for a Salesforce write, `"contacts/contacts"` for a HubSpot read). **A stream absent from the mapping is not written** — that's the write-policy opt-in signal.
2. The connector handler (`{connector}_handler.py`, subclass of `base_handler.py`) implements `handle_write()` / `handle_read()`.
3. Write path: `gs.Reader(sync-output)` → `map_stream_data()` (field mapping) → `drop_sent_records()` (dedupe vs `snapshots/{stream}_{FLOW}.snapshot.csv`) → connector-specific logic (e.g. `split_contacts_by_account()` for SF Contact/Lead) → `write_to_singer()` → `etl-output/data.singer`.
4. Read path: CRM parquet → field normalization → inject `crmSystem`, rename `remote_id`→`crmAssociationId`, set `lookupKey` → Singer output for `cbx1-target-hotglue`.
5. All output goes through `prepare_for_singer()`: datetimes → ISO strings, exact NaN/Infinity string tokens nulled (case-sensitive — see `hubspot/tests/`).

## Directory Structure

```
.
├── docs/
│   └── architecture.md         # End-to-end pipeline documentation
├── .claude/skills/             # AI workflows (local-job-debugging)
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

Mapping keys are `{target_stream}/{connector_stream}` and their shape depends on the connector and direction — the example above is a **Salesforce write** mapping (`Account`/`Contact` objects); the **committed** `tenant-config.json` is a **HubSpot read** mapping (`accounts/companies`, `contacts/contacts`, HubSpot property names). The file also carries a `hotglue_metadata` block (tenant name, `OrgId`) alongside `hotglue_mapping`.

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

### Running a Read Job (works out of the box)

The committed fixtures are a **HubSpot read** setup: `hubspot/sync-output/` holds HubSpot tap output as parquet (companies, contacts, owners, lists — plus `projects` and `contact_subscription_status`, which no code currently reads), and `snapshots/tenant-config.json` maps `accounts/companies` + `contacts/contacts` for flow `AJ3x0LMYI`. So a HubSpot read job runs with no further setup:

```bash
cd hubspot
export JOB_TYPE=read
export FLOW=AJ3x0LMYI
export CONNECTOR_ID=hubspot
python etl.py
head -3 etl-output/data.singer     # inspect the result
```

### Running a Write Job

A write job needs CBX1-shaped input in `sync-output/` and a write mapping (e.g. `contacts/Contact` for Salesforce) in `tenant-config.json` — the committed fixtures don't include these, so craft inputs or pull a real write job's data (see below):

```bash
cd hubspot
export JOB_TYPE=write
export FLOW=<flow-with-write-mapping>
export CONNECTOR_ID=salesforce   # or hubspot / marketo
python etl.py
```

### Replicating a real HotGlue job locally

A failed production/QA job can be reproduced locally with its exact input data and env vars (hotglue CLI: `setup-local-run` + `local-run`). This is an AI-assisted workflow — ask Claude to debug the failed job and it will follow the [`local-job-debugging` skill](.claude/skills/local-job-debugging/SKILL.md); the skill file documents the full manual procedure if you ever need it yourself.

### Running the tests
```bash
pytest hubspot/tests/    # from the repo root
```

### Development workflow (making a change)

- **Mapping logic change:** update `snapshots/tenant-config.json` locally to exercise it; craft minimal parquet inputs in `sync-output/` if the fixtures don't cover the case (`pd.DataFrame(...).to_parquet('sync-output/contacts-<ts>.parquet')`).
- **Dedupe-sensitive change:** `drop_sent_records()` consults the snapshot — delete/edit the local `snapshots/{stream}_{FLOW}.snapshot.csv` to force records through, and check the snapshot is written back correctly after the run.
- **New connector:** subclass `base_handler.py`, register in `etl.py::_get_handler`, follow the existing handler layout.

Verify before opening a PR:

1. `pytest hubspot/tests/` (from repo root) — serialization regressions.
2. Inspect `etl-output/data.singer`: SCHEMA line per stream, RECORD lines carry `lookupKey` + `sourceRecordId` (CBX1-bound), datetimes are ISO strings.
3. Check `snapshots/` diffs: updated, nothing unintentionally deleted.
4. Run **both** job types if the change touches shared code (`utils.py`, `base_handler.py`).

## Deployment

Deployment is automated via [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) — there is no manual deploy step in normal operation.

**Branch → environment mapping (know this before merging):**

| Trigger | Target HotGlue environment |
|---|---|
| Push to `main` | `dev.different.ai` |
| Push to `production` | `prod.different.ai` |
| Manual `workflow_dispatch` | Chosen env; a guard job **rejects** `prod.different.ai` unless dispatched from the `production` branch |

⚠️ **Merging to `main` deploys to dev immediately** (and `production` to prod). Doc-only changes (`README.md`, `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`, `.gitignore`) are excluded via `paths-ignore` and do not trigger a deploy.

**What a deploy does:** uploads `./hubspot` with `npx @hotglue/cli@1.1.0 etl deploy` for flow `AJ3x0LMYI` (hardcoded), once **per connector slot** — a `for TAP in hubspot salesforce` loop. The flow is bidirectional, so the ETL must land in *each* supported connector's slot; deploying only one previously left the other running stale code.

> **Note:** `marketo` is **not** in the deploy loop even though `marketo_handler.py` exists — a Marketo slot would run whatever was last deployed manually. Add it to the loop before relying on Marketo in any environment.

**Mechanics:** runs on the self-hosted `cbx1-gcp-runner-small` runner, authenticates with the `HOTGLUE_API_KEY` repository secret (verified present before deploying), and deploys are serialized per target env via a concurrency group so concurrent prod deploys can't interleave. Manual deploys: Actions → "Deploy ETL to HotGlue" → Run workflow.

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

1. **Ran from repo root** → `sync-output` not found / empty output. `cd hubspot` first.
2. **No accounts sent**: Contacts processing skipped if no accounts available
3. **Contacts silently held back (Salesforce)**: their account hasn't synced yet — by design (see [Write Policy](#write-policy)), not a bug
4. **Stream "ignored"**: no `{stream}/{Object}` key in the flow's mapping — the write-policy opt-in signal
5. **Everything re-sent**: missing/blown-away snapshot file (dedupe state lives in `snapshots/`)
6. **Missing mapping**: Streams processed without transformation if mapping unavailable
7. **Configuration errors**: Check tenant-config.json format and field mappings
8. **Connector mismatch**: Verify CONNECTOR_ID matches expected values
9. **`"NaN"` vs `"Nan"`**: the serialization token scrub is exact and case-sensitive on purpose (prod incident) — don't "generalize" it

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
