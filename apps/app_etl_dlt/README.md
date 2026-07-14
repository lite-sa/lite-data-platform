# app_etl_dlt — dlt-based ingestion jobs

Same goal as `app_etl` (Postgres read replica → GCS Parquet → BigQuery,
cheaply and correctly), letting dlt own the boilerplate `app_etl` hand-rolled:
chunked cursor reads, streaming Parquet writes, BigQuery batch load jobs, and
incremental-cursor state tracking.

This is a fresh app, not a rewrite of `app_etl` in place — both exist so the
approaches can be compared before deciding which one stays.

## Design: config registry + generic runner

One table = one `TableConfig` entry in `tables.py`. `runner.py` has exactly
two execution paths — `run_incremental` and `run_snapshot` — and dispatches
to whichever the table's `mode` says. Adding a table means adding a
`TableConfig`, not a new job file:

```python
# tables.py
TABLES: list[TableConfig] = [
    TableConfig(
        name="payments",
        mode="incremental",
        cadence="hourly",
        cursor_column="created_at",
        initial_value=datetime(2025, 1, 1, tzinfo=timezone.utc),
        primary_key="id",
    ),
    TableConfig(name="merchants", mode="snapshot", cadence="daily"),
]
```

Run a table:

```bash
uv run python -m app_etl_dlt.runner payments
```

`TABLES` ships **empty** — add whichever tables you're actually ingesting;
there's no fixed pair this app expects.

`cadence` is documentation only for now: it says how the Cloud Scheduler job
invoking `runner.py <name>` should be set up. Nothing in code reads it —
there's no scheduler wired up yet.

## What dlt replaces from `app_etl`

| `app_etl` hand-rolled | dlt equivalent |
|---|---|
| `utils/pg.py` — named-cursor chunked reads into pyarrow batches | `sql_table(..., chunk_size=...)` |
| `utils/gcs.py` — streaming Parquet writer to a fixed GCS layout | `staging=dlt.destinations.filesystem(bucket_url=...)` |
| `utils/bq.py` — `WRITE_APPEND` / `WRITE_TRUNCATE` load jobs | `pipeline.run(..., write_disposition=...)` (still a free batch load job, not streaming inserts) |
| `utils/state.py` + `ops.ingestion_runs` watermark | dlt's own pipeline state, versioned in the destination, advanced only after a successful load |
| one job file per table | one `TableConfig` entry per table |

The incremental-window correctness contract carries over unchanged: Postgres
`now()` is transaction *start* time, so a cursor column alone isn't a safe
watermark. `run_incremental` in `runner.py` keeps the same guard `app_etl`
uses — the window's upper bound trails wall clock by `safety_lag_minutes`
(default 10) instead of extracting up to `now()`. See `app_etl`'s README for
the full race-condition writeup; the reasoning is identical here.

## jsonb/json columns

BigQuery can't load dlt's `json` data type from Parquet files (only from
`jsonl`/`model`) — confirmed against dlt 1.28's `ensure_supported_type`,
which raises rather than silently mis-loading or dropping the column. Any
table with `jsonb`/`json` Postgres columns (e.g. `payments.risk`,
`payments.customer`) needs `has_json_columns=True` set on its `TableConfig`
— `runner.py` uses that to apply BigQuery's `autodetect_schema` hint, which
lets BigQuery infer column types straight from the Parquet file instead of
dlt pre-declaring them. Those columns land as BigQuery `STRING` (raw JSON
text, queryable via `JSON_EXTRACT`/`PARSE_JSON`), not a native `JSON` column.
Forgetting this flag fails the whole load, loudly, not silently — so it's
hard to miss when adding a new table.

## Known gaps vs. `app_etl` (deliberate, not accidental)

- **No per-day snapshot history.** `app_etl`'s `ingest_merchants` loads into
  `merchants$YYYYMMDD` (a BigQuery partition decorator) with `WRITE_TRUNCATE`,
  so every day's snapshot survives for point-in-time joins. `run_snapshot`'s
  `write_disposition="replace"` only replaces the whole table each run —
  idempotent per run, but no history. Closing this gap means a
  `bigquery_adapter` partition hint plus a partition-scoped load call, which
  re-introduces the hand-rolled BigQuery API code this app exists to avoid.
  Not implemented; revisit per-table if point-in-time history turns out to
  matter for a given snapshot table.
- **No queryable run log.** `app_etl` writes an explicit, append-only
  `ops.ingestion_runs` row per run (`rows_loaded`, `gcs_uri`, `watermark`) for
  recon and debugging before an orchestrator exists. dlt's equivalent is the
  `LoadInfo` object `run_table()` returns (printed by the CLI, not persisted).
  If that observability is needed, write it into a BigQuery table shaped like
  `ops.ingestion_runs` — not implemented here.

## Configuration

`config.py` reads env vars: `GCP_PROJECT`, `GCS_BUCKET`, `BQ_DATASET_RAW`
(default `raw_litecore`), `BQ_LOCATION` (default `me-central2` — **must
match the GCS bucket's region**; BigQuery load jobs require the dataset and
the source bucket to be co-located, otherwise you'll hit a confusing
"dataset not found in location US" error even though the dataset exists),
and Postgres as four separate parts — `PG_HOST` (default `127.0.0.1`),
`PG_PORT` (default `5432`), `PG_USER`, `PG_DATABASE` — rather than one DSN
string, so switching databases (this instance hosts one per service) is a
one-line env change. `Settings.pg_dsn` assembles the SQLAlchemy DSN from
these at call time.

No password field: this assumes Cloud SQL IAM database auth through a
locally running Cloud SQL Auth Proxy —

```bash
cloud-sql-proxy --auto-iam-authn --port=5432 <project>:<region>:<instance>
```

`PG_USER` is your IAM identity (e.g. your `@lite.sa` email) — the proxy
handles the actual auth handshake with Cloud SQL, so nothing here ever
holds a static DB password. See `.env` for a filled-in example.

## Status

Working — `run_incremental` has been run end-to-end against real
infrastructure (`payment_v2.payments` in Cloud SQL → `lite-data-dev-raw` GCS
staging → `lite-data-dev.raw_litecore.payments` in BigQuery, `me-central2`),
including the `has_json_columns` path. `run_snapshot` is implemented the same
way but hasn't had a real run yet. `TABLES` currently has one entry
(`payments`) — add more as needed.

Required IAM on the target GCP project, learned the hard way: dataset
creation alone isn't enough — you need `roles/bigquery.dataEditor` (tables,
not just datasets), `roles/bigquery.jobUser` (to actually run load jobs;
`dataEditor` doesn't include this), and `roles/storage.objectAdmin` (or at
least `objectCreator`) on the staging bucket. Missing any one of these fails
at a different, confusingly-specific step rather than up front.
