"""Table registry — the one place per-table ingestion is configured.

Add a table by appending one `TableConfig` here; `runner.py`'s two generic
paths (incremental / snapshot) do the rest. dlt already absorbs the
per-table plumbing (cursor chunking, parquet staging, load jobs), so the
only thing that varies table-to-table is this config — no new job file
per table.

`cadence` is documentation only right now: it says how the Cloud Scheduler
job invoking `runner.py <name>` should be set up. Nothing in code reads it
yet — there's no scheduler wired up.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal


@dataclass(frozen=True)
class TableConfig:
    name: str  # Postgres table name; also the BigQuery destination table name
    mode: Literal["incremental", "snapshot"]
    cadence: str  # informational only, e.g. "hourly", "daily", "*/5 * * * *"

    # incremental-only fields
    cursor_column: str | None = None  # e.g. "created_at" or "updated_at"
    initial_value: datetime | None = None  # launch date / earliest row to backfill from
    primary_key: str | None = None
    safety_lag_minutes: int = 10  # see runner.py's module docstring for why this exists

    # Set True if the table has jsonb/json columns. BigQuery can't load
    # dlt's "json" data_type from Parquet files (only from jsonl/model) —
    # this tells runner.py to apply BigQuery's autodetect_schema hint,
    # which lets BigQuery infer types from the Parquet file directly instead
    # of dlt pre-declaring them. Verified against dlt 1.28's actual source
    # (dlt/destinations/impl/bigquery/factory.py's ensure_supported_type):
    # without it, loading fails outright, not silently.
    has_json_columns: bool = False

    def __post_init__(self) -> None:
        if self.mode == "incremental" and (self.cursor_column is None or self.initial_value is None):
            raise ValueError(f"{self.name}: incremental tables need cursor_column and initial_value")


# One entry per ingested table. Empty by default — add your own.
TABLES: list[TableConfig] = [
    TableConfig(
        name="payments",
        mode="incremental",
        cadence="hourly",
        cursor_column="created_at",
        # earliest row currently in payment_v2.payments is 2026-05-06;
        # start a little before that so a first run backfills everything.
        initial_value=datetime(2026, 5, 1, tzinfo=timezone.utc),
        primary_key="id",
        # risk, customer, order_data, device, threeds_input, threeds_result,
        # return_url, metadata, routing_result, risk_result are all jsonb.
        has_json_columns=True,
    ),
]
