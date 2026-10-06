"""business_management database ->
{BQ_DATASET_RAW}.business_management__{business_entities,channels,
business_representatives}.

Small tables, full extract every run, `replace` disposition. A business
can be hard-deleted, which an incremental cursor would never see.
`channels.business_id` and `business_representatives.business_id` ->
business_entities.id.

Sensitive columns: channels.created_by / updated_by (operator identifiers);
business_representatives.phone_number, name, national_id, date_of_birth,
yakeen_* (the person's verified name parts).
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import bq_pipeline, bq_resource, pg_credentials, refresh_mode

DATABASE = "business_management"


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    business_entities = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="business_entities",
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            table_name=f"{DATABASE}__business_entities",
            write_disposition="replace",
        ),
        # `business_id`, not the PK `id`, is what payments.merchant_id
        # references.
        cluster="business_id",
    )

    channels = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="channels",
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            table_name=f"{DATABASE}__channels",
            write_disposition="replace",
        ),
        # Cluster on the parent FK (-> business_entities.id).
        cluster="business_id",
    )

    business_representatives = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="business_representatives",
        ).apply_hints(
            table_name=f"{DATABASE}__business_representatives",
            write_disposition="replace",
        ),
        cluster="business_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [business_entities, channels, business_representatives],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
