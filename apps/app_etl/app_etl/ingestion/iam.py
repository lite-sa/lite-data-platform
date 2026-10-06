"""iam database -> {BQ_DATASET_RAW}.iam__{identities,users,user_assignments}.

Small tables, full extract every run, `replace` disposition, no history.
Removing a role hard-deletes its `user_assignments` row, which an
incremental cursor would never see.

Joins: `users.id` = identities.id (only `type = 'user'` identities have a
users row); `user_assignments.identity_id` -> identities.id, `resource_id`
-> the organization, business unit or business account named by
`resource_type`. `users.yakeen_data`, `national_address` and `metadata`
land as JSON strings.

Sensitive columns: users.email, phone_number, full_name, national_id,
hijri_birth_date, yakeen_data (the full Yakeen payload, plaintext),
national_address, last_login_lat / last_login_long, metadata (never
reviewed against real data).

Skipped: invitations, api_keys, password_history, trusted_devices,
api_calls, `databasechangelog*`.
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import bq_pipeline, bq_resource, pg_credentials, refresh_mode

DATABASE = "iam"


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    identities = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="identities",
        ).apply_hints(
            table_name=f"{DATABASE}__identities",
            write_disposition="replace",
        ),
    )

    users = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="users",
        ).apply_hints(
            table_name=f"{DATABASE}__users",
            write_disposition="replace",
        ),
    )

    user_assignments = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="user_assignments",
        ).apply_hints(
            table_name=f"{DATABASE}__user_assignments",
            write_disposition="replace",
        ),
        cluster="resource_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [identities, users, user_assignments],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
