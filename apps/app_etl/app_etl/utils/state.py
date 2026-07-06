"""Ingestion run log, stored in BigQuery. Append-only — one INSERT per run.

    CREATE TABLE {ops}.ingestion_runs (
      pipeline_id  STRING    NOT NULL,   -- "payments", "merchants", ...
      run_at       TIMESTAMP NOT NULL,
      watermark    TIMESTAMP,            -- window upper bound; NULL for snapshots
      status       STRING    NOT NULL,   -- COMPLETED (only value written today)
      rows_loaded  INT64,
      gcs_uri      STRING                -- ties the run to its landed file
    );

Rules:
- Only successful runs are written: commit_watermark inserts the COMPLETED
  row, and that insert IS the watermark advance — written only after the
  BQ load succeeds. One write, no MERGE/UPDATE, INSERT-only fits BQ.
- A failed or crashed run writes nothing — absence is the signal; Cloud Run
  logs hold the autopsy. Never read `status` as proof of health.
- The current watermark is the latest COMPLETED row per pipeline.
- No history for a pipeline → the job starts from its `initial_default`
  (launch date). Same windowed code path as every other run — pre-launch
  that window IS the full table, so there is no separate full-load branch.
- Replay/backfill = INSERT a correction row with an older watermark. The
  rewind itself stays visible in history instead of being overwritten.
- Until an orchestrator exists, this table is the run observability:
  cadence, volume trends, rows_loaded/gcs_uri for recon and debugging.
"""

from __future__ import annotations

from datetime import datetime


def get_watermark(
    project: str,
    ops_dataset: str,
    pipeline_id: str,
    initial_default: datetime,
) -> datetime:
    """Return the watermark of the latest COMPLETED run for `pipeline_id`,
    or `initial_default` if the pipeline has never completed."""
    raise NotImplementedError("step 2")


def commit_watermark(
    project: str,
    ops_dataset: str,
    pipeline_id: str,
    watermark: datetime | None,
    rows_loaded: int | None = None,
    gcs_uri: str | None = None,
) -> None:
    """Insert the COMPLETED row for this run (run_at = now, set here).
    Call only after the BQ load succeeds — this insert IS the advance.

    watermark=None → snapshot pipeline (merchants): no watermark to advance,
    the run is recorded for observability only."""
    raise NotImplementedError("step 2")
