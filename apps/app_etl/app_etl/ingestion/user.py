"""user database -> {BQ_DATASET_RAW}.user__{users,user_invitations,merchants,
merchants_users,users_info}.

Small tables, full extract every run, `replace` disposition, no history.
`users.created_at` / `updated_at` are nullable, so an incremental cursor
would skip rows.

Joins: `user_id` on merchants_users, user_invitations and users_info ->
users.id; `merchant_id` on merchants_users and user_invitations ->
merchants.id. `users.user_id` is the identity-provider id, not users.id.
`users_info.national_address` lands as a JSON string.

Sensitive columns: users.email, phone_number, full_name, last_login_lat /
last_login_long; user_invitations.token (plaintext bearer token until used
or expired); merchants_users.email; users_info.national_id,
higri_birth_date, id_expiration_date, national_address, yakeen_*.

Skipped: `audit_logs` (event trail, add when a consumer needs status
history), roles, tokens, passwords, `databasechangelog*`.
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import bq_pipeline, bq_resource, pg_credentials, refresh_mode

DATABASE = "user"


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

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

    user_invitations = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="user_invitations",
        ).apply_hints(
            table_name=f"{DATABASE}__user_invitations",
            write_disposition="replace",
        ),
        cluster="merchant_id",
    )

    merchants = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="merchants",
        ).apply_hints(
            table_name=f"{DATABASE}__merchants",
            write_disposition="replace",
        ),
    )

    merchants_users = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="merchants_users",
        ).apply_hints(
            table_name=f"{DATABASE}__merchants_users",
            write_disposition="replace",
        ),
        cluster="merchant_id",
    )

    users_info = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="users_info",
        ).apply_hints(
            table_name=f"{DATABASE}__users_info",
            write_disposition="replace",
        ),
        cluster="user_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [users, user_invitations, merchants, merchants_users, users_info],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
