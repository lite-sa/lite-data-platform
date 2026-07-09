# app_etl — ingestion pipelines

Bespoke, per-table **dlt** pipelines moving data from the Postgres read
replica to BigQuery via GCS (filesystem staging → free batch load jobs).
One file per pipeline, one schedule per job. dlt supplies the plumbing
(extraction, staging, loading, state, schema DDL); each pipeline file states
only what is specific to its table: the column allowlist, the incremental
cursor, partition/cluster keys, and the write strategy.

## Pipelines

| Pipeline | Mode | Source | Target | Cadence (planned) |
|---|---|---|---|---|
| `ingestion/payments.py` | Incremental (cursor on `created_at`) | `payments` (append-only) | `raw_litecore.payments` (append) | hourly |
| `ingestion/merchants.py` | Daily snapshot | `merchants` (mutable config table) | `raw_litecore.merchants` (snapshot-date partitions) | daily |

Next tables (businesses, payment_operations, …) follow one of these two shapes —
copy the pipeline file, don't parameterize it.

## The watermark design (incremental pipelines)

dlt's incremental cursor tracks the max `created_at` seen and filters each
extraction — but it does **not** know about the Postgres commit-order race,
which is ours to close:

Append-only does **not** make `created_at` watermarking safe by itself: in
Postgres `now()` is the transaction *start* time, so a row with
`created_at = 10:00` can *commit* at 10:03, after a 10:02 run has already
advanced the watermark past it — and it would be skipped silently, forever.

One guard closes that race — the extraction query's upper bound trails wall
clock (via the `sql_database` source's query adapter):

```
WHERE created_at > :last_value
  AND created_at <= now() - SAFETY_LAG   -- leaves room for in-flight
                                         -- transactions to commit
```

Because no extracted row ever exceeds the capped bound, dlt's stored cursor
can never advance past it, and a late-committing row inside the lag is picked
up by the next run.

- **Correctness assumption (stated, ours to keep true):** no write transaction
  on the source outlives `SAFETY_LAG` (10 min). At hourly cadence the lag costs
  nothing in freshness. If long-running writers ever appear upstream, raise the
  lag — or switch to dlt's `lag` + merge disposition (the overlap+dedup design
  we rejected as premature; it also trades free batch loads for MERGE compute).
- **No duplicates produced, no dedup contract downstream.** Append disposition
  + batch loads. The one residual duplicate window — a crash after the data
  load lands but before dlt's state load commits, so the rerun re-extracts the
  same window — is accepted in v1. When dbt staging arrives it closes for free
  with a one-line `QUALIFY ROW_NUMBER()`; primary keys always land in raw so
  that stays possible.
- The stored watermark is `MAX(created_at)` of extracted rows, so an empty
  run does not advance it. Harmless: the capped bound guarantees every
  not-yet-seen row is still ahead of the cursor.

## Run state

Entirely dlt's: pipeline state (including the incremental cursor) is stored
in the destination and restored each run — `_dlt_pipeline_state`, plus
`_dlt_loads` (per-load history) and `_dlt_version` (every schema version
applied). State advances only as part of a successful load; a failed or
crashed run advances nothing — rerun and the same window extracts again.

The scaffold's `ops.ingestion_runs` run log and `utils/state.py`
(`get_watermark` / `commit_watermark`) are superseded by this and dropped.
If we later miss a queryable run-observability table beyond `_dlt_loads`,
we add one back as observability only — never as the correctness mechanism.

- First run: no stored cursor → extraction starts from the resource's
  `initial_value` (launch date), through the same windowed code path —
  pre-launch that window IS the full table, so there is no separate
  full-load branch to accidentally trigger.
- Replay/backfill: an explicit dlt backfill run over a fixed
  `initial_value`/`end_value` range — deliberate, and append-safe only if
  the target window was empty; otherwise dedup downstream first.

## Snapshot design (config tables)

Full extract each run, landing as **one row-set per snapshot date** in a
table partitioned on `snapshot_date` — point-in-time joins ("what did this
merchant's config look like when the payment happened") are a filter on the
snapshot date.

Reruns must be idempotent per day (an append rerun would double every
merchant). The dlt-idiomatic way is merge/delete-insert keyed on
`snapshot_date` — MERGE costs query compute, but config tables are small,
so this is pennies; the exact strategy is settled in step 2.

## Landing layout (GCS)

Staging files are written by dlt under its layout in the raw bucket; one
immutable file set per load, nothing overwritten. (The scaffold's bespoke
`raw/litecore/{table}/ingest_date=...` layout is superseded; we configure
dlt's filesystem layout rather than hand-rolling paths.)

## Memory

Extraction streams through dlt's `sql_database` source in arrow-batch chunks;
footprint is one chunk regardless of table size.

## Configuration

`config.py` reads env vars: `GCP_PROJECT`, `GCS_BUCKET`, `PG_DSN` (required),
`BQ_DATASET_RAW` (default `raw_litecore`), `BQ_DATASET_OPS` (default `ops`).
dlt's own credentials/config are fed from the same env (env vars are dlt's
native config provider) — no `secrets.toml` files in the repo.

## Schema

Column allowlists, DDL ownership, evolution, and the changelog are in
`docs/schema-management.md`. Short version: every resource declares an
explicit column list (PII deny-by-default — `payments` excludes the
customer/device/3DS JSONB blobs), dlt owns raw DDL and additive evolution,
and the exported schema YAML committed under `schemas/` is the changelog.

## Status

Scaffold predates the dlt decision: `utils/` (pg/gcs/bq/state) and the job
stubs encode the hand-rolled design this README used to describe. Step 2
replaces them with the dlt pipelines described here; the stubs'
`NotImplementedError("step 2")` markers still map to that milestone.
