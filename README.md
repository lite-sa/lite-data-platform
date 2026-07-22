# Lite Data Platform

The initial repository for Lite's data platform that will hold the foundation for the ML/AI capabilities across payment acceptance, issuing risk, AML,
merchant risk, feature store, etc. 
The first milestone is to build the data foundation: getting operational data out of Postgres and into BigQuery,
cheaply and correctly in order to enable future analytics and usecases. 

## Principles

- **Bespoke over generic — on a standard framework.** Ingestion uses **dlt**
  (data load tool), the de-facto standard Python EL framework, but as one
  explicit pipeline per table (`payments`, `merchants`), one explicit schedule
  per job. dlt replaces plumbing we'd otherwise hand-roll — extraction,
  GCS staging, BQ loading, watermark state, schema DDL — not the per-table
  explicitness. No config-driven job factories; we'll generalize only when
  duplication actually hurts.
- **Apps, one per domain.** Code lives in `apps/app_*` (uv workspace members
  sharing one lockfile): `app_etl` now; `app_aml`, `app_analytics`, … later.
- **Cheapest thing that is correct.** dlt with filesystem staging = Parquet on
  GCS + free BigQuery batch load jobs; Cloud Scheduler over Composer; no
  streaming until something needs it.

## Layout

```
lite-data-platform/
  pyproject.toml            # uv workspace root
  docs/                     # cross-cutting design docs (schema management, provisioning)
  apps/
    app_etl/                # dlt pipelines: Postgres → GCS (Parquet) → BigQuery
      app_etl/
        config.py           # env-driven settings (project, bucket, datasets, DSN)
        ingestion/          # bespoke pipelines: payments, merchants, ...
        schemas/            # dlt exported schema YAML, committed (the changelog)
      tests/
```

## Getting started

```bash
uv sync           # creates .venv, provisions Python 3.13, installs the workspace
uv run pytest     # scaffold sanity tests
```

Jobs read their settings from env vars (see `app_etl/config.py`): `GCP_PROJECT`,
`GCS_BUCKET`, one Postgres connection mode (`PG_HOST`/`PG_PORT`/`PG_USER` for
the Auth Proxy locally, `PG_INSTANCE_CONNECTION_NAME`/`PG_IAM_USER` on Cloud
Run), and optionally `BQ_DATASET_RAW`. The source *database* is not env
config — one pipeline per database, stated in each pipeline file.
Single dev GCP project for now; staging environments can be added later

## Architecture decisions (v1)

| Decision | Choice | Why |
|---|---|---|
| Movement | dlt pipelines: PG read replica (`sql_database` source) → Parquet staged on GCS → BQ batch load | Standard framework, and still the free-load path (staging + batch load jobs cost nothing); no CDC infra pre-launch. Revisit CDC (Debezium et al.) when volume/latency demands it. |
| Incremental capture | dlt incremental cursor on `updated_at` (sources are mutable; updates bump it), extraction query capped at `now() − safety lag` | dlt tracks the watermark, but not the Postgres commit-order race (`now()` is transaction *start* time); the capped upper bound closes it. Raw stores one row per version — grain `(primary key, updated_at)` — and dbt staging dedups to the latest; deletes and updates that skip `updated_at` are invisible. See `apps/app_etl/README.md`. |
| Watermark state | dlt pipeline state, stored in the destination (`_dlt_pipeline_state`; load history in `_dlt_loads`) | Advances only with a successful load; survives redeploys; nothing hand-rolled. The earlier `ops.ingestion_runs` run log is dropped for v1 — add observability back only if we miss it. |
| Schema | Column allowlist on every resource (PII deny-by-default); dlt owns raw DDL + evolution; dbt contracts once marts have consumers | One source of truth, no drift to police. See `docs/schema-management.md`. |
| Config tables | Daily snapshot rows keyed by snapshot date into a date-partitioned table, idempotent per day | Point-in-time history for as-of joins; exact dlt write strategy (merge on small config tables is cheap) settled in step 2. |
| Memory | Chunked extraction via dlt's `sql_database` backends (arrow batches) | A job's footprint is one chunk regardless of table size. |
| Orchestration | Cloud Scheduler → Cloud Run jobs | Composer is ~$400/mo idle; our one known dependency (transforms gate on today's merchant snapshot) is a data-availability check in code. Revisit when the dependency graph stops fitting in a head. |
| Transformation | dbt on BigQuery (planned, `app_aml` milestone) | AML v1 is aggregations/rules — dbt fits; Spark is overkill at our volume. |

## Roadmap

1. **Ingestion scaffold** *(done)* — repo structure, contracts, docs.
2. **Ingestion pipelines** — dlt pipelines for `payments` (incremental) and
   `merchants` (daily snapshot); replaces the scaffold's hand-rolled
   `utils/` + watermark stubs; verified locally against a seeded Postgres.
3. **Packaging & deploy** — Dockerfile + Cloud Build → Cloud Run jobs.
4. **Orchestration** — Cloud Scheduler schedules in code, data-availability gates.
5. **Notebooks** — BigQuery notebooks with `app_etl` utils importable; user branches.
6. **AML v1** (`app_aml`) — minimal dbt scenarios on BigQuery, pre-go-live.
