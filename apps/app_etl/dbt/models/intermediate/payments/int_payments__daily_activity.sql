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

-- Every payment operation from the last complete local day, widened with
-- its parent payment's dimensions (merchant_id, payment status/method/
-- channel). The reusable canonical building block for payments features —
-- same run_date-keyed microbatch shape as aml_merchant_features
-- (lookback=0, never --full-refresh in prod; begin is an arbitrary
-- bootstrap start, revisit before promotion), just general payment
-- activity instead of an AML rule. See docs/dbt-primer.md §5.
--
-- Grain: one row per payment_operation_id. An operation's status/the
-- parent payment's status here is whatever staging's dedup saw as of
-- this run — a later change (e.g. AUTHORIZED -> CAPTURED the next day)
-- is not retroactively applied to an already-built partition, same
-- accepted tradeoff as every other model in this run_date-keyed layer
-- (docs/dbt-primer.md §5, "Rebuildable table vs generation record").
--
-- Staging refs deliberately declare no event_time (they'd be batch-
-- filtered on the wrong column); the date filter below is the only input
-- window.

{% if model.batch %}
    {% set run_date = "date('" ~ model.batch.event_time_start.strftime("%Y-%m-%d") ~ "')" %}
{% else %}
    {# parse/compile-time only (no batch context, e.g. `dbt parse`) #}
    {% set run_date = "current_date('" ~ var("local_timezone") ~ "')" %}
{% endif %}

{% set activity_day = "date_sub(" ~ run_date ~ ", interval 1 day)" %}

with

ops as (

    select
        op.payment_operation_id,
        op.payment_id,
        pay.merchant_id,
        op.operation_type,
        op.status as operation_status,
        op.amount_minor,
        op.currency,
        op.terminal_id,
        op.entry_mode,
        op.created_at_local,
        pay.status as payment_status,
        pay.payment_method,
        pay.channel_id
    from {{ ref('stg_litecore__payment_operations') }} as op
    inner join {{ ref('stg_litecore__payments') }} as pay using (payment_id)
    where date(op.created_at_local) = {{ activity_day }}

)

select
    {{ run_date }} as run_date,
    payment_operation_id,
    payment_id,
    merchant_id,
    operation_type,
    operation_status,
    amount_minor,
    currency,
    terminal_id,
    entry_mode,
    created_at_local,
    payment_status,
    payment_method,
    channel_id,
    -- wall-clock generation moment (run_date is the logical date): written
    -- once per partition, restamped only if the partition is regenerated
    current_timestamp() as evaluated_at
from ops
