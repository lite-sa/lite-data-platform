"""pricing_engine database → {BQ_DATASET_RAW}.pricing_engine__rule_evaluation
(one pipeline per source database: a pipeline connects to exactly one DB),
incremental append.

`rule_evaluation` is pricing-engine's per-operation fee audit trail: one
row per pricing request, written in two phases by
`processPricingEvaluation` (insert the request, then update the same row
with matched_rules / winning_rule / winning_rule_id / applied_price;
dev p50 94 ms later, max 2.8 s). Mutable source, watermarked on
`updated_at` with the safety-lag cap: every update re-extracts the row,
so raw holds one appended row per source-row version and downstream
dedups to the latest (see utils/dlt_helpers.py and the README's
watermark design). Both phases stamp `updated_at` from the app clock
(`new Date().toISOString()`), not Postgres `now()`; the safety lag
absorbs that skew too. No index on `updated_at` at source (only the PK
and `origin_reference`), so every run scans the table: same
deferred-index situation as settlement and checkout_session.

Join contract (docs/ecom-payment-milestones.md §6): `origin` names the
calling service and the meaning of `origin_reference` depends on it.
`payment-v2-service` rows (operation authorize / capture / pay / refund)
carry `origin_reference` = payment_operations.id and
`parent_origin_reference` = payments.id (dev 2026-09-08: 333 of 337
distinct references match an op, the 4 misses are July refund
evaluations whose op is gone; 296 of 296 parents match a payment).
`payout-service` rows (operation transfer / payout, from HOLD_TRANSFER /
HOLD_EXTERNAL_SETTLEMENT) send their request correlation id as
`origin_reference` (payout-service/src/clients/pricing/pricing.service.ts),
which matches no id in payout, settlement or ledger; the payout-side
fee link is `payout.transfer.fees`, not this table. One reference can
be evaluated more than once (445 distinct references over 461 dev rows),
so readers pick the latest evaluation per reference, not only the
latest version per id. `winning_rule_id` -> profile_rule.id; NULL means
no rule matched, which is the zero-fee skip seen on the settlement side
(nb 035). `matched_rules` is jsonb[] and lands as a STRING holding a
JSON array, like settlement.transaction.fees; `winning_rule` and
`applied_price` land as JSON strings with snake_case keys
(`applied_price.fees[].fee_type` / `.amount`, incl. the `vat` leg, and
`applied_price.total_amount` = amount + Σ fees; the camelCase names in
the milestones doc §11 are the gRPC response, not the stored row).
Amounts are minor units.

The four config tables (`profile`, `profile_rule`, `merchant_profile`,
`profile_status`) are deliberately not ingested yet: the winning rule is
denormalised into `winning_rule`, so fee analysis needs no join. Add
them as replace snapshots (smart_routing's profile / routing_rule
blocks) when a consumer needs `merchant_profile.product_type` (the
settlement product-type gate) or rule history. `databasechangelog*` is
Liquibase's own bookkeeping, never ingested.

No column allowlist: ingest-everything posture. Nothing here is shopper
PII; the JSON blobs are the merchant's commercial fee terms (trim map in
docs/schema-management.md §1).
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

DATABASE = "pricing_engine"


def run() -> None:
    settings = Settings.from_env()

    rule_evaluation = bq_resource(
        sql_table(
            credentials=pg_credentials(settings, DATABASE),
            schema="public",
            table="rule_evaluation",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__rule_evaluation",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        # parent-FK convention, like payment_operations -> payment_id:
        # the join spine reaches this table on origin_reference = op id
        cluster="origin_reference",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        rule_evaluation, loader_file_format="parquet", refresh=refresh_mode()
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
