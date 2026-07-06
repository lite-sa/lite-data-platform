# app_etl — ingestion jobs

Bespoke, per-table jobs moving data from the Postgres read replica to BigQuery
via GCS. One file per job, one schedule per job, shared *dumb* helpers in
`utils/` (a helper takes arguments and does IO; it never decides what a job does).

## Jobs

| Job | Mode | Source | Target | Cadence (planned) |
|---|---|---|---|---|
| `ingestion/ingest_payments.py` | Incremental (watermark) | `payments` (append-only) | `raw_litecore.payments` (WRITE_APPEND) | hourly |
| `ingestion/ingest_merchants.py` | Full snapshot | `merchants` (mutable config table) | `raw_litecore.merchants$YYYYMMDD` (partition TRUNCATE) | daily |

Next tables (businesses, payment_operations, …) follow one of these two shapes —
copy the job file, don't parameterize it.

## The watermark design (incremental jobs)

Append-only does **not** make `created_at` watermarking safe by itself: in
Postgres `now()` is the transaction *start* time, so a row with
`created_at = 10:00` can *commit* at 10:03, after a 10:02 run has already
advanced the watermark past it — and it would be skipped silently, forever.

One guard closes that race — the window's upper bound trails wall clock:

```
window = (watermark,  now() - SAFETY_LAG]
                      └ leaves room for in-flight
                        transactions to commit
```

- **Correctness assumption (stated, ours to keep true):** no write transaction
  on the source outlives `SAFETY_LAG` (10 min). At hourly cadence the lag costs
  nothing in freshness. If long-running writers ever appear upstream, raise the
  lag — or revisit the overlap+dedup design this replaced.
- **No duplicates produced, no dedup contract downstream.** BQ load jobs are
  atomic, so a failed run retried is clean. The one residual duplicate window —
  the job crashes *after* the load succeeds but *before* the watermark
  advances, so the rerun re-loads the same window — is accepted in v1. When dbt
  staging arrives it closes for free with a one-line `QUALIFY ROW_NUMBER()`;
  primary keys always land in raw so that stays possible.
- The watermark is the window upper bound the job **computed** — not
  `MAX(created_at)` fished out of the extracted data. An empty window still
  advances it; a no-op run is not an error.

How the watermark is stored, advanced, and rewound is entirely the run log's
business — next section.

## Run state (`ops.ingestion_runs`)

State is an **append-only run log** in BigQuery — one INSERT per run, no
UPDATE/MERGE ever:

```sql
CREATE TABLE ops.ingestion_runs (
  pipeline_id  STRING    NOT NULL,   -- "payments", "merchants", ...
  run_at       TIMESTAMP NOT NULL,
  watermark    TIMESTAMP,            -- window upper bound; NULL for snapshots
  status       STRING    NOT NULL,   -- COMPLETED (only value written today)
  rows_loaded  INT64,
  gcs_uri      STRING                -- ties the run to its landed file
);
```

- Two functions in `utils/state.py`: `get_watermark` (latest `COMPLETED` row
  per pipeline, else `initial_default`) and `commit_watermark` (insert the
  `COMPLETED` row). The commit **is** the watermark advance — one write, only
  after the load succeeds.
- Only successful runs are written. A failed or crashed run writes nothing —
  absence is the signal; Cloud Run logs hold the autopsy. Never read `status`
  as proof of health.
- First run: no `COMPLETED` row → the job starts from `initial_default`
  (launch date), through the same windowed code path — pre-launch that window
  IS the full table, so there is no separate full-load branch to accidentally
  trigger.
- Replay/backfill: INSERT a correction row with an older watermark — the
  rewind stays visible in history instead of being overwritten.
- Snapshot jobs (merchants) record runs too, with `watermark NULL` —
  observability only.
- Until an orchestrator exists, this table is the run observability layer:
  durations, failure streaks, volume trends, and `rows_loaded`/`gcs_uri` for
  recon and debugging.

## Snapshot design (config tables)

Full extract each run, loaded into **today's date partition** of the target:

```
destination: raw_litecore.merchants$20260706    # partition decorator
disposition: WRITE_TRUNCATE
```

`WRITE_TRUNCATE` is a BigQuery load-job *write disposition* — the setting that
says what to do with data already in the destination. `WRITE_APPEND` (what
payments uses) adds the loaded rows to whatever is there; `WRITE_TRUNCATE`
atomically **replaces** the destination's contents with the loaded file.
Pointed at a partition decorator (`table$YYYYMMDD`) rather than the bare
table, it replaces *only that day's partition* and leaves every other day
untouched. The combination gives snapshots both properties we want:

- **Idempotent per day** — a rerun replaces today's snapshot instead of
  duplicating it (rerunning an APPEND snapshot would double every merchant).
- **History for free** — one partition per day, so point-in-time joins
  ("what did this merchant's config look like when the payment happened")
  are a filter on the snapshot date.

## Landing layout (GCS)

```
gs://{bucket}/raw/litecore/{table}/ingest_date=YYYY-MM-DD/{run_ts}.parquet
```

One immutable file per run. Reruns write a new `run_ts`; nothing is overwritten.

## Memory

Every extract streams through a server-side cursor in 50k-row chunks and is
written as one Parquet row group per chunk (`utils/pg.py`, `utils/gcs.py`).
Footprint is one chunk regardless of table size.

## Configuration

`config.py` reads env vars: `GCP_PROJECT`, `GCS_BUCKET`, `PG_DSN` (required),
`BQ_DATASET_RAW` (default `raw_litecore`), `BQ_DATASET_OPS` (default `ops`).

## Status

Scaffold only — module contracts are in the docstrings; implementations land in
the next step (`raise NotImplementedError("step 2")` marks each one).
