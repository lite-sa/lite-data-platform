# Lite Data Platform

The initial repository for Lite's data platform that will hold the foundation for the ML/AI capabilities across payment acceptance, issuing risk, AML,
merchant risk, feature store, etc. 
The first milestone is to build the data foundation: getting operational data out of Postgres and into BigQuery,
cheaply and correctly in order to enable future analytics and usecases. 

## Principles

- **Bespoke over generic.** One explicit job per table (`ingest_payments`,
  `ingest_merchants`), one explicit schedule per job. No factory patterns, no
  config-driven job frameworks. We pay the duplication cost to keep every job
  readable and independently changeable; we'll generalize only when the
  duplication actually hurts.
- **Apps, one per domain.** Code lives in `apps/app_*` (uv workspace members
  sharing one lockfile): `app_etl` now; `app_aml`, `app_analytics`, … later.
- **Cheapest thing that is correct.** Parquet on GCS + free BigQuery batch load
  jobs; Cloud Scheduler over Composer; no streaming until something needs it.


## Layout

```
lite-data-platform/
  pyproject.toml            # uv workspace root
  apps/
    app_etl/                # Postgres → GCS (Parquet) → BigQuery ingestion jobs
      app_etl/
        config.py           # env-driven settings (project, bucket, datasets, DSN)
        utils/              # plain shared helpers: pg, gcs, bq, state
        ingestion/          # bespoke jobs: ingest_payments, ingest_merchants, ...
      tests/
```

## Getting started

```bash
uv sync           # creates .venv, provisions Python 3.14, installs the workspace
uv run pytest     # scaffold sanity tests
```

Jobs read their settings from env vars (see `app_etl/config.py`): `GCP_PROJECT`,
`GCS_BUCKET`, `PG_DSN`, and optionally `BQ_DATASET_RAW` / `BQ_DATASET_OPS`.
Single dev GCP project for now; staging environments can be added later

## Architecture decisions (v1)

| Decision | Choice | Why |
|---|---|---|
| Movement | Pure-Python jobs: PG read replica → Parquet on GCS → BQ batch load | Load jobs are free; no CDC infra pre-launch. Revisit CDC (Debezium et al.) when volume/latency demands it. |
| Incremental capture | `created_at` watermark + safety lag (no overlap, no dedup) | Append-only tables still race on commit order; a trailing window bound closes it with zero duplicates, assuming no source transaction outlives the lag. See `apps/app_etl/README.md`. |
| Watermark state | `ops.ingestion_runs` — append-only run log in BigQuery, one INSERT per run | The COMPLETED insert is the watermark advance; replay = insert a correction row; doubles as run observability until an orchestrator exists. |
| Config tables | Daily full snapshot into a date partition (`table$YYYYMMDD`, WRITE_TRUNCATE) | Idempotent reruns + free point-in-time history for as-of joins. |
| Memory | Server-side cursors, fixed-size chunks, streamed Parquet row groups | A job's footprint is one chunk regardless of table size. ~10 lines, not a framework. |
| Orchestration | Cloud Scheduler → Cloud Run jobs | Composer is ~$400/mo idle; our one known dependency (transforms gate on today's merchant snapshot) is a data-availability check in code. Revisit when the dependency graph stops fitting in a head. |
| Transformation | dbt on BigQuery (planned, `app_aml` milestone) | AML v1 is aggregations/rules — dbt fits; Spark is overkill at our volume. |

## Roadmap

1. **Ingestion scaffold** *(this change)* — repo structure, contracts, docs.
2. **Ingestion jobs** — implement `utils/` + `ingest_payments`, `ingest_merchants`;
   verified locally against a seeded Postgres.
3. **Packaging & deploy** — Dockerfile + Cloud Build → Cloud Run jobs.
4. **Orchestration** — Cloud Scheduler schedules in code, data-availability gates.
5. **Notebooks** — BigQuery notebooks with `app_etl` utils importable; user branches.
6. **AML v1** (`app_aml`) — minimal dbt scenarios on BigQuery, pre-go-live.
