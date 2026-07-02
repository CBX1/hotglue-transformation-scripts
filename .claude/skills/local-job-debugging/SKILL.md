---
name: local-job-debugging
description: Replicate a real HotGlue job locally with the hotglue CLI and debug the ETL against its exact input data, snapshots, and env vars — setup-local-run, local-run, output diffing against the reference. Use when a HotGlue sync job failed or produced wrong data and you need to reproduce/fix it locally.
---

# Debugging a HotGlue Job Locally

The hotglue CLI can download a specific job's data + environment and re-run `etl.py` locally, auto-diffing your output against what the job actually produced. This turns "debug a failed sync from logs" into a local reproduce-fix-verify loop.

CLI reference: https://docs.hotglue.com/cli/etl

## One-time setup

```bash
npm install -g @hotglue/cli
hotglue config set apikey <key>     # from hotglue dashboard → Account → Login & Security → Personal API Key
```

Project config lives in `hubspot/.hotgluerc` (keys: `env`, `flow`, `tap`; optional `tenant`). Verify with `hotglue config`.

⚠️ **The checked-in `.hotgluerc` defaults to `env: prod.different.ai`** — with no overrides, `jobs list` / `setup-local-run` operate against **production**. When you mean QA/dev, point `env` at `dev.different.ai` (edit `.hotgluerc` locally — don't commit it — or use the CLI's env override) and select the right `--tenant`. Treat anything downloaded from prod as sensitive tenant data.

## Workflow

All commands from inside `hubspot/` (that's where `.hotgluerc` and `etl.py` live).

### 1. Find the job

```bash
hotglue jobs list --tenant <tenant_id> --status JOB_FAILED --count 5
```

Grab the `s3_root` from the output (format: `{tenant}/flows/{flow}/jobs/YYYY/MM/…/{job_id}`) — or copy it from the job details page in the hotglue UI.

### 2. Pull the job environment

```bash
hotglue etl setup-local-run <s3_root> [--include-configs]
```

This downloads the job's data and writes a `.env` with the exact env vars from the job (JOB_TYPE, FLOW, CONNECTOR_ID, JOB_ROOT, …). Directory layout after setup:

| Local dir | Contents |
|---|---|
| `sync-output/` | The job's actual tap output (parquet) — your ETL input |
| `etl-output-reference/` | What the job's ETL actually produced (comparison baseline) |
| `snapshots-reference/` | The job's snapshot state |
| `.env` | The job's env vars |

`--include-configs` also pulls connector configs and sets `API_KEY`. Use `-o` to overwrite a previous setup, `-d <dir>` to download elsewhere.

⚠️ Job data is real tenant data — treat it as sensitive; don't commit it (sync-output fixtures already in git are curated QA samples only).

### 3. Run locally & diff

```bash
hotglue etl local-run
```

Executes the ETL in a job-like environment and **compares your `etl-output/` against `etl-output-reference/`**: missing files, extra files, content diffs in CSV and Singer files. Tune the comparator with a `test-config.json` (`sort_config`, `ignore_columns`, `rename_config`) when ordering or volatile columns cause false diffs. `--dockerPlatform` if the Docker image arch complains on Apple Silicon.

Alternatively, run bare-metal against the downloaded data (faster iteration, no comparator):

```bash
set -a; source .env; set +a
python etl.py
```

### 4. Fix → verify → deploy

Iterate on the handler/`utils.py` until the diff is clean (or intentionally different, for a bug fix). Then ship via PR; deployment to HotGlue happens from CI (`.github/workflows/deploy.yml`), or manually with `hotglue etl deploy` when instructed.

## Interpreting common diffs

| Diff | Usual cause |
|---|---|
| Records missing from your `etl-output` | `drop_sent_records()` deduped them — check `snapshots-reference/` state vs what you expected |
| Whole stream missing | Write policy: stream not in the flow's `stream_name_mapping` (see `README.md` → Write Policy), or contacts held back waiting for their account to sync (Salesforce) |
| Field values differ | Mapping change in `snapshots/tenant-config.json` (`hotglue_mapping.mapping.{FLOW}`) between the job and your local reference |
| Datetime/NaN serialization diffs | `prepare_for_singer()` behavior — see `hubspot/tests/` for the exact rules |
