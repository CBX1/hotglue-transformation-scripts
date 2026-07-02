---
name: etl-development
description: Develop and test the CRM transformation scripts locally — read vs write job semantics, env vars, tenant-config mapping shape, snapshot/dedupe mechanics, handler pattern, running against the committed fixtures. Use when changing mapping logic, adding a stream or connector, or investigating why the ETL emitted/dropped/mismapped records.
---

# ETL Development

Authoritative reference: `AGENTS.md` (structure, write policy, testing guidelines) and `docs/architecture.md` (end-to-end pipeline). This skill is the operational workflow.

**⚠️ Everything runs from `hubspot/`** — code, `sync-output/`, `snapshots/`, `etl-output/` all live there (historical dir name; it hosts ALL connectors). Only pytest runs from the repo root.

## Setup

```bash
cd hubspot
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pytest
```

## Run against the committed fixtures

`hubspot/sync-output/` ships curated QA parquet fixtures (companies, contacts, owners, lists, …) and `snapshots/` a matching `tenant-config.json` for flow `AJ3x0LMYI`, so a job runs out of the box:

```bash
JOB_TYPE=write FLOW=AJ3x0LMYI CONNECTOR_ID=hubspot python etl.py
head -3 etl-output/data.singer
```

Env vars: `JOB_TYPE` (`write`=CBX1→CRM | `read`=CRM→CBX1), `FLOW` (mapping/snapshot key), `CONNECTOR_ID` (`salesforce`|`hubspot`|`marketo`), `ROOT_DIR` (default `.`).

To run against a **real job's data** instead of fixtures, use the `local-job-debugging` skill (hotglue CLI `setup-local-run`).

## The mental model

1. `etl.py` loads `snapshots/tenant-config.json` → `hotglue_mapping.mapping.{FLOW}` and derives `stream_name_mapping` from its `target/connector` keys (e.g. `"contacts/Contact"` → contacts maps to SF Contact). **A stream absent from the mapping is not written** — that's the write-policy opt-in signal.
2. The connector handler (`{connector}_handler.py`, subclass of `base_handler.py`) implements `handle_write()` / `handle_read()`.
3. Write path: `gs.Reader(sync-output)` → `map_stream_data()` (field mapping) → `drop_sent_records()` (dedupe vs `snapshots/{stream}_{FLOW}.snapshot.csv`) → connector-specific logic (e.g. `split_contacts_by_account()` for SF Contact/Lead) → `write_to_singer()` → `etl-output/data.singer`.
4. Read path: CRM parquet → field normalization → inject `crmSystem`, rename `remote_id`→`crmAssociationId`, set `lookupKey` → Singer output for `cbx1-target-hotglue`.
5. All output goes through `prepare_for_singer()`: datetimes → ISO strings, exact NaN/Infinity string tokens nulled (case-sensitive — see `hubspot/tests/`).

## Making a change

- **Mapping logic change:** update `snapshots/tenant-config.json` locally to exercise it; craft minimal parquet inputs in `sync-output/` if the fixtures don't cover the case (`pd.DataFrame(...).to_parquet('sync-output/contacts-<ts>.parquet')`).
- **Dedupe-sensitive change:** remember `drop_sent_records()` consults the snapshot — delete/edit the local `snapshots/{stream}_{FLOW}.snapshot.csv` to force records through, and check the snapshot is written back correctly after the run.
- **New connector:** subclass `base_handler.py`, register in `etl.py::_get_handler`, follow the existing handler layout.

## Verify

1. `pytest hubspot/tests/` (from repo root) — serialization regressions.
2. Inspect `etl-output/data.singer`: SCHEMA line per stream, RECORD lines carry `lookupKey` + `sourceRecordId` (CBX1-bound), datetimes are ISO strings.
3. Check `snapshots/` diffs: updated, nothing unintentionally deleted.
4. Run **both** job types if the change touches shared code (`utils.py`, `base_handler.py`).

## Common gotchas

- Ran from repo root → `sync-output` not found / empty output. `cd hubspot` first.
- Contacts silently held back (Salesforce): their account hasn't synced yet — by design (see `AGENTS.md` → Write Policy), not a bug.
- Stream "ignored": no `{stream}/{Object}` key in the flow's mapping.
- Everything re-sent: missing/blown-away snapshot file.
- `"NaN"` vs `"Nan"`: the token scrub is exact and case-sensitive on purpose (prod incident) — don't "generalize" it.
