"""lem database (Legal Entity Management) →
{BQ_DATASET_RAW}.lem__<table> — full replace.

LEM is litecore's successor model to business_management (schema deployed
2026-08-17, empty at pipeline creation 2026-08-18): organizations →
legal_entities / business_units → business_accounts, with `agreements`
(source_business_account_id → target_business_account_id, typed rules) as
the first-class partner/aggregator linkage and the future home of
Qlub-style attribution (`payments.acting_business_account_id` is its
transaction-side counterpart). Until litecore starts writing to it, this
pipeline's daily zero-row snapshots double as a go-live monitor.

Seven small mutable config tables, full extract every run, `replace`
disposition (same interim stance as business_management — snapshot_date
partitioning is the intended end state). The KYC/PII-heavy tables
(individual_records, identity_documents, ubos, compliance_profiles,
corporate_records, screening_checks, addresses) and the log tables
(api_calls, requests) are deliberately not ingested — add them when a
consumer exists.

No column allowlists — ingest-everything posture. Known-sensitive columns
(trim map for dbt staging, not for extraction): legal_entities
contact_phone_number / contact_mobile_number / contact_email.
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import bq_pipeline, bq_resource, pg_credentials, refresh_mode

DATABASE = "lem"


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    organizations = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="organizations",
        ).apply_hints(
            table_name=f"{DATABASE}__organizations",
            write_disposition="replace",
        ),
        cluster="id",
    )

    legal_entities = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="legal_entities",
        ).apply_hints(
            table_name=f"{DATABASE}__legal_entities",
            write_disposition="replace",
        ),
        cluster="organization_id",
    )

    business_units = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="business_units",
        ).apply_hints(
            table_name=f"{DATABASE}__business_units",
            write_disposition="replace",
        ),
        cluster="organization_id",
    )

    business_accounts = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="business_accounts",
        ).apply_hints(
            table_name=f"{DATABASE}__business_accounts",
            write_disposition="replace",
        ),
        cluster="legal_entity_id",
    )

    # The partner/aggregator edge — the expected filter column is the
    # partner-side (source) account.
    agreements = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="agreements",
        ).apply_hints(
            table_name=f"{DATABASE}__agreements",
            write_disposition="replace",
        ),
        cluster="source_business_account_id",
    )

    channels = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="channels",
        ).apply_hints(
            table_name=f"{DATABASE}__channels",
            write_disposition="replace",
        ),
        cluster="business_account_id",
    )

    activities = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="activities",
        ).apply_hints(
            table_name=f"{DATABASE}__activities",
            write_disposition="replace",
        ),
        cluster="reference_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [
            organizations,
            legal_entities,
            business_units,
            business_accounts,
            agreements,
            channels,
            activities,
        ],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
