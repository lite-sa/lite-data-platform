"""BigQuery load helpers.

Only *batch load jobs* from GCS — they are free, so ingestion cost is
GCS + BQ storage only. No streaming inserts, no Storage Write API.
"""

from __future__ import annotations


def load_parquet_append(
    project: str,
    dataset: str,
    table: str,
    gcs_uri: str,
) -> int:
    """Load a Parquet file into `dataset.table` with WRITE_APPEND.
    Used by incremental jobs (payments). Returns rows loaded.

    Load jobs are atomic (all-or-nothing), so a failed run retried is
    clean — no duplicates, no dedup contract downstream. The one residual
    duplicate window (crash after load succeeds, before the watermark
    advances) is accepted in v1; see the README.
    """
    raise NotImplementedError("step 2")


def load_parquet_snapshot(
    project: str,
    dataset: str,
    table: str,
    gcs_uri: str,
    snapshot_date: str,  # YYYYMMDD
) -> int:
    """Load a Parquet file into the `dataset.table$snapshot_date` partition
    with WRITE_TRUNCATE. Used by full-snapshot jobs (merchants, businesses):
    idempotent per day — reruns replace the partition, history is kept as
    one partition per day.
    """
    raise NotImplementedError("step 2")
