# Repository Guidelines

## Project Structure & Module Organization
Python transformation logic lives in `etl.py`, with shared helpers (stream access, mapping, dedupe, nested object handling) in `utils.py`. HotGlue-provided data and outputs stay in `sync-output/`, `etl-output/`, and `snapshots/`; keep these directories intact so snapshot-based deduplication continues to work. JSON fixtures such as `catalog-selected.json`, `fieldMap.json`, and `state.json` document current tap behavior—update them when upstream schemas evolve and note the change in your PR.

## Build, Test, and Development Commands
Create an isolated environment before editing: `python -m venv .venv && source .venv/bin/activate`. Install runtime dependencies with `pip install -r requirements.txt`. Execute jobs via `python etl.py`, exporting `JOB_TYPE`, `FLOW`, and `CONNECTOR_ID` as needed (for example: `JOB_TYPE=write FLOW=AJ3x0LMYI CONNECTOR_ID=salesforce python etl.py`). Use the same command with `JOB_TYPE=read` to validate inbound transformations against refreshed `sync-output/` payloads.

## Coding Style & Naming Conventions
Follow PEP 8 conventions: four-space indentation, snake_case for functions and variables, and CapWords for classes. Type hints are already present in critical paths; add them to new helpers for clarity. Reuse the shared logger configured in `etl.py` for consistent observability, and prefer descriptive, imperative function names (`split_contacts_by_account`) that mirror existing patterns.

## Testing Guidelines
No automated test suite exists yet, so rely on targeted job runs. After changes, run both write and read jobs against representative snapshots, inspect generated Singer files in `etl-output/`, and confirm no unintended deletions in `snapshots/`. When altering mapping logic, craft minimal mock payloads in `sync-output/` to demonstrate the new case and attach the sample or its gist in the PR.

## Commit & Pull Request Guidelines
Match the git history by writing brief, imperative commit titles (e.g., "Handle nested objects in the transformation"). PR descriptions should summarize the scenario, list configuration or mapping files touched, and call out any new environment variables. Link to relevant support tickets or customer escalations and include before/after snippets or command output that proves the transformation behaves as intended.
