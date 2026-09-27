"""terminal_management database -> {BQ_DATASET_RAW}.terminal_management__terminal.

Incremental append on `updated_at` with the safety-lag cap. Every heartbeat
and presence change bumps `updated_at`, so an online terminal lands about one
row per run; readers take the latest version per `terminal_id`.

Joins: payment_operations.terminal_id -> `tid` (not the uuid `terminal_id`);
`merchant_id` -> business_entities.business_id; `channel_id` -> channels.id.
Soft delete via `deleted_at`. `tags` and `metadata` land as JSON strings.

Sensitive columns: vendor_jwt (bearer token), encrypted_secret_key /
kms_key_version (key material), trsm (terminal auth code), user_phone,
device_fingerprint, mqtt_client_id, notes, created_by / updated_by.

Skipped: terminal_command, terminal_connection_log, terminal_profile,
terminal_profile_change_request, terminal_refresh_token, databasechangelog*.
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import (
    bq_pipeline,
    bq_resource,
    cap_upper_bound,
    capped_incremental,
    pg_credentials,
    refresh_mode,
)

DATABASE = "terminal_management"


def run() -> None:
    settings = Settings.from_env()

    terminal = bq_resource(
        sql_table(
            credentials=pg_credentials(settings, DATABASE),
            schema="public",
            table="terminal",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__terminal",
            primary_key="terminal_id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(terminal, loader_file_format="parquet", refresh=refresh_mode())
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
