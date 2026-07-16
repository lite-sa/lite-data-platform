# app_etl — ingestion pipelines

Bespoke **dlt** pipelines: Postgres → GCS staging → BigQuery batch loads
(the free path). **One pipeline per source database** — LiteCore runs one
database per service and a dlt pipeline holds exactly one connection — so
each database is one file under `ingestion/`, one Cloud Run job, one
schedule. A pipeline file states only what is source-specific (database,
tables, cursor, write disposition, partition column); shared plumbing is
`utils/dlt_helpers.py`. New tables copy an existing table block — no
factories.

| Pipeline | Source (db: tables, all `public`) | Mode | Cadence |
|---|---|---|---|
| `ingestion/payment_v2.py` | `payment_v2`: `payments`, `payment_operations` | incremental append on `updated_at` — one row per version | hourly |
| `ingestion/user.py` | `user`: `merchants` | full replace (interim; snapshots below are the target) | daily |
| `ingestion/business_management.py` | `business_management`: `business_entities` | full replace (interim, as above) | daily |

Targets land in `BQ_DATASET_RAW` (`raw_litecore`; local runs keep
`raw_test`), named after the source table.

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

dlt owns raw DDL and additive evolution; every resource declares a PII
deny-by-default column allowlist (TODO while the dev source is all dummy
data); the exported schema YAML under `schemas/` is the changelog. Rules:
`docs/schema-management.md`.

jsonb/array source columns can't load into BigQuery from Parquet as a
declared JSON type, so `bq_resource` sets `autodetect_schema` and they land
as `STRING` — query via `JSON_EXTRACT`/`PARSE_JSON` in staging.
