# app_etl — ingestion pipelines

Bespoke **dlt** pipelines: Postgres → GCS staging → BigQuery batch loads
(the free path). **One pipeline per source database** — LiteCore runs one
database per service and a dlt pipeline holds exactly one connection — so
each database is one file under `ingestion/`, one Cloud Run job, one
schedule. A pipeline file states only what is source-specific (database,
tables, cursor, write disposition, partition column); shared plumbing is
`utils/dlt_helpers.py`. New tables copy an existing table block — no
factories.

| Pipeline | Source (db: tables, all `public`) | Mode |
|---|---|---|
| `ingestion/payment_v2.py` | `payment_v2`: `payments`, `payment_operations` | incremental append on `updated_at` — one row per version |
| `ingestion/ledger.py` | `ledger`: `account`, `entry` | incremental append on `updated_at`, as above |
| `ingestion/smart_routing.py` | `smart_routing`: `profile`, `routing_rule` / `transaction_evaluation` | full replace (interim) / incremental append |
| `ingestion/business_management.py` | `business_management`: `business_entities` | full replace (interim; snapshots below are the target) |

All pipelines run through the one daily workflow
(`workflows/daily_pipeline.yaml`) — no staggered per-job schedules.
Targets land in `BQ_DATASET_RAW` (`raw_litecore`; local runs keep
`raw_test`), named `<database>__<table>`: every pipeline loads the same
dataset (dataset = source system, LiteCore), so the double-underscore
prefix namespaces per-service databases that will eventually carry
same-named tables — see `docs/schema-management.md`.

## Running

```bash
uv run python -m app_etl.ingestion.payment_v2            # from the repo root
uv run python -m app_etl.ingestion.payment_v2 --refresh  # drop this pipeline's BQ tables + cursor state, reload from scratch
```

`--refresh` is needed whenever a create-time-only BigQuery property
(partitioning, clustering) changes. Config is env vars / a repo-root
`.env`: `GCP_PROJECT`, `GCS_BUCKET`, `BQ_DATASET_RAW`, and one Postgres
connection mode (Auth Proxy locally, Cloud SQL Connector on Cloud Run);
the *database* is stated in each pipeline file, never in the env. Full
map: `docs/configuration.md`.

## Deploying to Cloud Run — Private Service Connect

The Postgres source (`lite-litecore-dev`) lives in a different, unpeered
GCP project/VPC than this app (`lite-data-dev`) — there's no private-IP
(PSA) path between them, so Cloud Run jobs must reach it over **Private
Service Connect (PSC)**, not the `private` Cloud SQL Connector mode. Set
`PG_IP_TYPE=psc` (see `.env.example` / `config.py`); `private` targets
PSA and will never connect across projects.

PSC also means the job needs an actual network path: the PSC endpoint's
reserved internal IP and the private DNS zone that resolves the
instance's PSC DNS name both live in `lite-data-dev`'s `default` VPC
(provisioned in `Litecore-IaC`'s
`units/modules/data-platform/psc-endpoint`). Cloud Run jobs get there via
**Direct VPC egress** — no separate VPC connector resource, just three
flags on the job itself:

```bash
gcloud run jobs create ingest-payment-v2 \
  --project=lite-data-dev --region=me-central2 \
  --image=me-central2-docker.pkg.dev/lite-data-dev/<AR_REPO>/app-etl:<TAG> \
  --service-account=sa-app-etl@lite-data-dev.iam.gserviceaccount.com \
  --network=default --subnet=default --vpc-egress=private-ranges-only \
  --env-vars-file=deploy/ingest-payment-v2.env \
  --command=python --args="-m,app_etl.ingestion.payment_v2"
```

`--env-vars-file` takes a local YAML or `.env`-style file (`KEY=value` per
line) — don't reuse the repo-root `.env` used for local dev, since it
targets different values (`raw_test`, `PG_HOST` mode); write a separate
file per job, e.g.:

```
GCP_PROJECT=lite-data-dev
GCS_BUCKET=lite-data-dev-raw
BQ_DATASET_RAW=raw_litecore
PG_INSTANCE_CONNECTION_NAME=lite-litecore-dev:me-central2:non-cde-postgres
PG_IAM_USER=sa-app-etl@lite-data-dev.iam
PG_IP_TYPE=psc
```

Note: `--subnet`, not `--subnetwork` — Cloud Run's Direct VPC egress flags
are `--network`/`--subnet`/`--network-tags`, unlike Compute Engine
resources which use `--subnetwork`. `--env-vars-file` only applies at
`create`/`update` time (it fully replaces the job's env vars); triggering
a run via `gcloud run jobs execute` doesn't accept a file — only inline
`--update-env-vars=KEY=VALUE,...` overrides merged with what's already
on the job.

`--vpc-egress=private-ranges-only` routes only RFC1918 traffic (the PSC
endpoint's IP) through the VPC; public-internet egress (GCS, BigQuery,
the Cloud SQL Admin API) stays direct — no NAT needed, and no reason for
`all-traffic` here.

Repeat per source database (`ledger`, `business_management`,
`smart_routing`), swapping the job name and the `--args` module path.
These per-database jobs don't exist yet as of this writing — the live
`ingest-*` jobs are stale per-table ones being retired; ingestion from
Cloud Run was previously blocked on this exact PSC/network gap.

## Watermark design (incremental pipelines)

Sources are mutable; every update bumps `updated_at`, the cursor. dlt
persists the max cursor seen, but not the Postgres commit-order race:
`now()` is transaction *start* time, so a row can commit *after* the
watermark has passed its `updated_at` — and be skipped forever. The guard
is `cap_upper_bound`, a plain predicate via the query adapter (never
`incremental(end_value=...)`, which bypasses dlt's persisted cursor):

```sql
WHERE updated_at > :last_value
  AND updated_at <= now() - SAFETY_LAG  -- 10 min for in-flight tx to commit
```

Consequences, all deliberate:

- **Assumption to keep true:** no source write transaction outlives
  `SAFETY_LAG`; if one ever does, raise the lag.
- **Raw stores versions, not current state** — grain
  `(primary key, updated_at)`; dbt staging dedups to the latest
  (`QUALIFY ROW_NUMBER()`), which also absorbs crash-rerun duplicates.
  Deletes, and updates that don't bump `updated_at`, are invisible —
  accepted in v1.
- **State is entirely dlt's**, stored in the destination
  (`_dlt_pipeline_state`, `_dlt_loads`); it advances only with a
  successful load. A first run extracts from `EPOCH` through the same
  windowed path — there is no separate full-load branch to trigger.

## Snapshot design (config tables)

Target: one full row-set per `snapshot_date` partition, idempotent per day
(merge/delete-insert; MERGE compute is pennies on config tables), so
point-in-time joins are a partition filter. Interim: plain `replace`, no
history. Moving to snapshots means new partitioning, i.e. one `--refresh`
run (partitioning is immutable at CREATE).

## Schema

dlt owns raw DDL and additive evolution; resources ingest every source
column — the deny-by-default allowlist stance was reversed 2026-08-12
(PII boundary = raw IAM + dbt staging column lists + staging-bucket
lifecycle; each pipeline file's docstring keeps its trim map); the
exported schema YAML under `schemas/` is the changelog. Rules:
`docs/schema-management.md`.

jsonb/array source columns can't load into BigQuery from Parquet as a
declared JSON type, so `bq_resource` sets `autodetect_schema` and they land
as `STRING` — query via `JSON_EXTRACT`/`PARSE_JSON` in staging.
