# CBX1 ↔ CRM Sync — End-to-End Architecture

Single authoritative description of how the three HotGlue repos compose. Repo-specific detail lives in each repo's `README.md`; this doc covers the pipeline as a whole.

## The three repos

| Repo | Role | Runs as |
|---|---|---|
| [`cbx1-tap-hotglue`](https://github.com/CBX1/cbx1-tap-hotglue) | Singer **tap**: extracts accounts/contacts from the CBX1 Java backend (egestion list API, keyset pagination) | HotGlue source connector |
| `hotglue-transformation-scripts` (this repo) | **In-flow transformation** (`hubspot/etl.py`): field mapping, dedupe via snapshots, contact/lead splitting, Singer serialization | HotGlue ETL step, between tap and target |
| [`cbx1-target-hotglue`](https://github.com/CBX1/cbx1-target-hotglue) | Singer **target**: writes records to the CBX1 integration records API (bulk upsert) | HotGlue destination connector |

## The two flow directions

Each tenant flow is bidirectional; HotGlue runs two kinds of jobs:

### Write job (`JOB_TYPE=write`) — CBX1 → CRM

```
cbx1-tap-hotglue                          # reads CBX1 egestion API
  → sync-output/*.parquet                 # HotGlue lands tap output as parquet
    → etl.py (this repo)                  # maps CBX1 fields → CRM fields per tenant-config,
      |                                   # drops already-sent records (snapshots),
      |                                   # splits contacts/leads (Salesforce)
      → etl-output/data.singer            # Singer SCHEMA/RECORD messages
        → CRM target (HotGlue-provided    # writes to Salesforce/HubSpot/Marketo
          target-salesforce / target-hubspot / …)
```

### Read job (`JOB_TYPE=read`) — CRM → CBX1

```
CRM tap (HotGlue-provided tap-salesforce / tap-hubspot / …)
  → sync-output/*.parquet
    → etl.py (this repo)                  # maps CRM fields → CBX1 shape, injects crmSystem,
      |                                   # renames remote_id → crmAssociationId,
      |                                   # sets lookupKey (domain/email)
      → etl-output/data.singer
        → cbx1-target-hotglue             # POST …/integrations/{SOURCE}/{TYPE}/records
          → CBX1 Java backend
```

## Data contracts at the boundaries

| Boundary | Format | Key fields |
|---|---|---|
| tap → ETL | parquet files in `sync-output/`, one per stream, timestamped (`contacts-20260127T045246.parquet`) | stream-shaped columns from the tap |
| ETL → target | newline-delimited Singer messages in `etl-output/data.singer` | For CBX1-bound records: `lookupKey` (**required** — domain for accounts, email for contacts; records without it are skipped by the target), `sourceRecordId` (CRM id, becomes `externalId` in HotGlue UI state), optional `source` (SALESFORCE/HUBSPOT/MARKETO) |
| target → CBX1 | `POST {BASE_URL}api/t/v1/targets/integrations/{SOURCE}/{OBJECT_TYPE}/records`, `{"records": […]}` | Response: `data.results[]` with `status` enum (SUCCESS/FAILED/SKIPPED), `entityId`, `lookupKey` |

All ETL output passes through `prepare_for_singer()` (datetimes → ISO strings; NaN/Infinity literal tokens scrubbed — see `hubspot/tests/`).

## Where configuration lives

| Config | Location | Owner |
|---|---|---|
| Tenant field mappings | `snapshots/tenant-config.json` → `hotglue_mapping.mapping.{FLOW_ID}.{target_stream}/{connector_stream}` | Synced into the job's snapshot dir by HotGlue; per tenant |
| Flow id | `FLOW` env var (HotGlue sets it per job) | HotGlue job env |
| Connector | `CONNECTOR_ID` env var (`salesforce` \| `hubspot` \| `marketo`) | HotGlue job env |
| Tap/target credentials (`Code`, `OrgId`) | each connector's `config.json` (HotGlue-managed; Descope access key per tenant) | HotGlue connector settings |
| Backend URL | `BASE_URL` env var (trailing slash), per deployment env | HotGlue environment |
| Self-write filtering | `HOTGLUE_PRINCIPAL_ID` env var on the tap — drops records CBX1-side that HotGlue itself wrote | HotGlue environment |

## Dedupe & idempotency (why snapshots matter)

- **Write path:** `snapshots/{stream}_{FLOW}.snapshot.csv` records what was already sent; `drop_sent_records()` diffs against it so only new/changed records are emitted. Deleting a snapshot causes a full re-send.
- **Read path:** the tap's Singer **state** (`replication_key_value` bookmark) provides incrementality; the CBX1 target upserts by `lookupKey`, so replays are safe.
- **Echo prevention:** without `HOTGLUE_PRINCIPAL_ID`, records HotGlue wrote into CBX1 get re-read by the tap on the next cycle and bounce back to the CRM.

## Debugging a job — where to start

1. **Which stage failed?** HotGlue job logs show tap, ETL, and target phases separately.
2. **ETL stage:** replicate the exact job locally with the hotglue CLI (`hotglue etl setup-local-run <job>` then `hotglue etl local-run`) — see `docs/local-job-debugging.md` in this repo.
3. **Tap stage (CBX1 read):** run `tap-cbx1` locally against QA — see `cbx1-tap-hotglue/README.md` debugging playbook (auth, keyset pagination, state).
4. **Target stage (CBX1 write):** feed the job's `etl-output/data.singer` into `target-cbx1` locally against QA — see `cbx1-target-hotglue/README.md` (lookupKey skips, per-record errors, replayable cURL).

## Credentials quick reference

- `Code` / `OrgId`: from the tenant's Descope access key for the CBX1 IDM (`api/g/v1/auth/tokens`). Same pair works for tap and target. Ask in #eng-crm-self-serve for QA-tenant credentials.
- Both tap and target **write the JWT session token back into `config.json`** after auth — never commit a used config file (gitignored in both repos).
- hotglue CLI auth: `hotglue config set apikey <key>` (Personal API Key from the hotglue dashboard → Account → Login & Security) — needed for the local-job-debugging workflow.
