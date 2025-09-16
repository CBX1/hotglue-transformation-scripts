# Repository Guidelines

## Project Structure & Module Organization
The ETL has been refactored into a clear, modular architecture with strict separation of read/write paths and per-connector logic.

- `etl.py` — Main orchestrator/entrypoint. Loads env/config, resolves the connector, and delegates to the right handler. No business rules live here.
- `base_handler.py` — Abstract base class providing shared utilities:
  - Stream listing, snapshot read/write, Singer output (`write_to_singer()`), and mapping helpers
  - Enforces a common interface: `handle_write()` and `handle_read()`
- `salesforce_handler.py` — Salesforce-specific business logic:
  - Write: standard stream mapping and Contact/Lead split via `split_contacts_by_account()`
  - Read: field normalization for DW, CBX1 id enrichment, AccountId fix-up for contacts
- `hubspot_handler.py` — HubSpot-specific business logic:
  - Write: standard stream mapping for contacts/accounts/companies
  - Read: optional owner enrichment (name/email) when `owners` stream is present
  - Associations: deliberately NOT handled here; HubSpot applies its own association rules
- `utils.py` — Shared helpers with comprehensive docstrings:
  - `get_stream_data()`, `map_stream_data()`, `drop_sent_records()`, `transform_dot_notation_to_nested()`
  - Contact helpers: `split_contacts_by_account()`, `get_contact_data()`

HotGlue input/output directories must remain intact for idempotency and dedupe to work:

- `sync-output/` — connector/tap output consumed by the transformation
- `snapshots/` — historical snapshots used for dedupe and id mapping
- `etl-output/` — final Singer-formatted records emitted by the transformation

Mapping configuration is tenant-driven and read from `snapshots/tenant-config.json` at runtime. Update this snapshot when mappings change.

## Build, Test, and Development Commands
Create an isolated environment before editing:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Run jobs from the `salesforce/` directory by exporting the required environment variables:

- `JOB_TYPE`: `write` (to CRM) or `read` (from CRM to DW). Default: `write`.
- `FLOW`: Flow identifier used for mapping/snapshots. Example: `AJ3x0LMYI`.
- `CONNECTOR_ID`: `salesforce` or `hubspot`.

Examples:

```bash
# Write job to Salesforce
JOB_TYPE=write FLOW=AJ3x0LMYI CONNECTOR_ID=salesforce python etl.py

# Read job from Salesforce
JOB_TYPE=read FLOW=AJ3x0LMYI CONNECTOR_ID=salesforce python etl.py

# Write job to HubSpot
JOB_TYPE=write FLOW=AJ3x0LMYI CONNECTOR_ID=hubspot python etl.py

# Read job from HubSpot
JOB_TYPE=read FLOW=AJ3x0LMYI CONNECTOR_ID=hubspot python etl.py
```

## Coding Style & Naming Conventions
Follow PEP 8 conventions: four-space indentation, snake_case for functions/variables, and CapWords for classes. Type hints are present in critical paths—add them to new helpers. Reuse the shared logger configured in `etl.py` and prefer descriptive, imperative function names (e.g., `split_contacts_by_account`).

## Testing Guidelines
No automated test suite exists yet—use targeted job runs:

1. Run both `write` and `read` jobs against representative `sync-output/` payloads.
2. Inspect generated Singer files under `etl-output/` and verify schema/values.
3. Confirm snapshots under `snapshots/` are updated and no unintended deletions occurred.
4. When changing mapping logic, update `snapshots/tenant-config.json` and prepare minimal input samples in `sync-output/` to demonstrate new behavior.

Additional expectations:

- Salesforce write path may split `contacts` into `Contact` and `Lead` depending on account linkage.
- HubSpot associations are NOT formatted by this transformation; HubSpot applies its own association rules downstream.
- All outgoing data is serialized using `prepare_for_singer()` so datetime fields become Singer-friendly ISO strings.

## Commit & Pull Request Guidelines
Write brief, imperative commit titles (e.g., "Refactor into connector handlers"). PR descriptions should:

- Summarize the scenario and expected outcomes
- List files touched and mapping/config changes (especially `snapshots/tenant-config.json`)
- Call out new/changed environment variables
- Include before/after snippets or command output demonstrating correct behavior
