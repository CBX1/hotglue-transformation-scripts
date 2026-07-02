---
name: local-job-debugging
description: Replicate a real HotGlue job locally with the hotglue CLI and debug the ETL against its exact input data, snapshots, and env vars. Use when a HotGlue sync job failed or produced wrong data and you need to reproduce/fix it locally.
---

# Debugging a HotGlue Job Locally

Follow the workflow in [`docs/local-job-debugging.md`](../../../docs/local-job-debugging.md) step by step: find the job's `s3_root` → `hotglue etl setup-local-run` → `hotglue etl local-run` → interpret the diff against the reference output → fix → re-run until clean.

Work from inside `hubspot/` (where `.hotgluerc` and `etl.py` live). Treat downloaded job data as sensitive tenant data — never commit it.
