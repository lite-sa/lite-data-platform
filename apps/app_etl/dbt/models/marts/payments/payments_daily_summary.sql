{{
    config(
        materialized="incremental",
        incremental_strategy="microbatch",
        event_time="run_date",
        begin="2026-07-01",
        batch_size="day",
        lookback=0,
        full_refresh=false,
        partition_by={"field": "run_date", "data_type": "date", "granularity": "day"},
        cluster_by=["merchant_id"],
        on_schema_change="append_new_columns",
    )
}}

-- Merchant x run_date payment activity summary — the canonical, non-AML
-- mart: pure payment volume, no rule thresholds. Same run_date-keyed,
-- lookback=0, never --full-refresh in prod shape as aml_merchant_features
-- (docs/dbt-primer.md §5). Future payments features land in
-- int_payments__daily_activity or a sibling intermediate model, not by
-- re-aggregating raw here.
--
-- Unlike aml_merchant_features (which reads staging and hand-derives its
-- date window), int_payments__daily_activity already declares
-- event_time="run_date" itself, so dbt wraps the ref below in the
-- current batch's window automatically — every remaining row shares one
-- run_date, hence group by run_date alongside merchant_id. This is the
-- microbatch "automatic upstream filtering" case docs/dbt-primer.md §5
-- describes; no hand-rolled date logic needed here.
--
-- total_amount_minor sums across currencies (99.5% SAR today, same
-- accepted simplification as aml_merchant_features; currency-mix policy
-- is still an open TODO). Attempt-grained: a payment authorized in
-- several attempts contributes each attempt.

select
    run_date,
    merchant_id,
    count(distinct payment_id) as payment_count,
    count(*) as operation_count,
    countif(operation_type = 'AUTHORIZE') as authorize_count,
    countif(operation_type = 'CAPTURE') as capture_count,
    countif(operation_type = 'REFUND') as refund_count,
    sum(amount_minor) as total_amount_minor,
    current_timestamp() as evaluated_at
from {{ ref('int_payments__daily_activity') }}
group by run_date, merchant_id
