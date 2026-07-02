# Agent Guide — hotglue-transformation-scripts

**`README.md` is the authoritative documentation** for this repo: architecture, directory structure, env vars, tenant mapping config, write policy, usage, troubleshooting, and contributing guidelines. Read it first; don't duplicate its content here. End-to-end pipeline (tap → this ETL → target): `docs/architecture.md`.

Agent-specific ground rules:

- **⚠️ Working directory:** all code and job data live under `hubspot/` (historical name — it hosts ALL connectors including Salesforce and Marketo). Run `etl.py` from inside `hubspot/`; from the repo root it finds no `sync-output/` and produces nothing. Tests run from the repo root: `pytest hubspot/tests/`.
- **Never delete or bypass `sync-output/`, `snapshots/`, `etl-output/`** — snapshots are the dedupe/idempotency state; blowing one away causes a full re-send to the tenant's CRM.
- **Respect the Write Policy** (README → Write Policy): streams absent from the flow's mapping are intentionally not written; Salesforce contacts without a synced account are intentionally held back. Neither is a bug.
- **Don't "generalize" the NaN/Infinity token scrub** in `prepare_for_singer` — matching is exact and case-sensitive on purpose (prod incident: `"NaN"` broke the HubSpot export, `"Nan"` is a valid name). The tests in `hubspot/tests/` encode this.
- Run `pytest hubspot/tests/` before and after touching `utils.py`/serialization paths.
- To reproduce a real HotGlue job locally: use the `local-job-debugging` skill (`.claude/skills/local-job-debugging/`). For mapping/handler development: README → "How a job executes" and "Development workflow".
