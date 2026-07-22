"""business_management database → {BQ_DATASET_RAW}.business_entities — full
replace.

One pipeline per source database; `business_entities` is the only table we
take from `business_management` today. Small mutable config table: full
extract every run, `replace` disposition. (The snapshot_date-partitioned
design in the README is the intended end state; `replace` is the interim
until that strategy lands.)
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import bq_pipeline, bq_resource, pg_credentials, refresh_mode

DATABASE = "business_management"

# Deny-by-default: anything not listed here never leaves the source. Cut
# from the 73 source columns down to these 52 — the excluded 21 are direct
# PII/contact info (contact_*, wathq_contact_email, agent_email,
# sales_email), Nafath identity-verification artifacts (nafath_trans_id,
# nafath_random, nafath_request_id, nafath_initiated_at,
# nafath_national_id — an actual national ID number), a device fingerprint
# (high_risk_device_id), and a handful of columns kept out pending a look
# at real data rather than confirmed PII: partners_nationality_name
# (unclear if aggregate or per-individual), contribution_capital/
# stock_capital/fiscal_year (jsonb — could list individual shareholders),
# business_sequence, ticket_id, and the focal_* integration bookkeeping
# columns (focal_sync_fail_reason is free text — same "could contain
# anything" risk as payment_operations.raw_provider_response).
BUSINESS_ENTITIES_COLUMNS = [
    "id",
    "business_id",
    "status",
    "cr_number",
    "created_at",
    "updated_at",
    "wathq_data_retrieved",
    "cr_national_number",
    "name",
    "cr_capital",
    "company_duration",
    "is_main",
    "issue_date_gregorian",
    "issue_date_hijri",
    "main_cr_national_number",
    "main_cr_number",
    "in_liquidation_process",
    "has_ecommerce",
    "headquarter_city_name",
    "is_license_based",
    "license_issuer_national_number",
    "license_issuer_name",
    "type",
    "type_form",
    "wathq_status",
    "wathq_status_confirmation_date_gregorian",
    "wathq_status_confirmation_date_hijri",
    "wathq_status_reactivation_date_gregorian",
    "wathq_status_reactivation_date_hijri",
    "wathq_status_suspension_date_gregorian",
    "wathq_status_suspension_date_hijri",
    "wathq_status_deletion_date_gregorian",
    "wathq_status_deletion_date_hijri",
    "website_url",
    "capital_currency_id",
    "capital_currency_name",
    "management_structure_name",
    "entity_characters",
    "annual_revenue_from",
    "annual_revenue_to",
    "is_high_risk_country_onboarding",
    "kyb_approved_at",
    "number_of_employees_from",
    "number_of_employees_to",
    "risk_score",
    "is_stakeholders_pep_compliant",
    "is_manually_created",
    "mccs",
    "is_pci_compliant",
    "product_type",
    "aml_risk_score",
    "wathq_legal_name",
]


def run() -> None:
    settings = Settings.from_env()

    business_entities = bq_resource(
        sql_table(
            credentials=pg_credentials(settings, DATABASE),
            schema="public",
            table="business_entities",
            included_columns=BUSINESS_ENTITIES_COLUMNS,
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            write_disposition="replace"
        ),
        # `business_id` (not `id`, the source PK) is the natural join key:
        # it's the only other uniquely-indexed column and reads as this
        # row's external identifier, whereas `id` is only targeted by this
        # DB's own child tables (channels, channel_sequence) — confirm
        # against how `merchants`/`payments.merchant_id` key before relying
        # on it in a downstream join.
        cluster="business_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        business_entities, loader_file_format="parquet", refresh=refresh_mode()
    )
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
