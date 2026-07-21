{{
    config(
        materialized="incremental",
        incremental_strategy="microbatch",
        event_time="run_date",
        begin="2026-07-01",
        batch_size="day",
        lookback=0,
        full_refresh=false,
        concurrent_batches=false,
        partition_by={"field": "run_date", "data_type": "date", "granularity": "day"},
        cluster_by=["target_id"],
        on_schema_change="append_new_columns",
    )
}}

-- AML-014 evaluation log — copied from aml_alerts_008 (house style: one
-- table per scenario, spine identical, only the breach predicate and the
-- `evidence` content are this scenario's own; see that file and
-- docs/aml-alert-design.md for the shared mechanics: microbatch semantics,
-- suppression, why concurrent_batches=false).
--
-- AML-014 "Dormant account sudden high activity": no AUTHORIZE ops in the
-- dormancy baseline [D−91, D−2] (90 complete days, the `_90d` feature)
-- AND a burst on the last complete day
-- [D−1] — op count >= threshold OR summed amount >= threshold (minor
-- units; the currency-mix policy is an open TODO, 99.5% SAR today). The
-- baseline deliberately excludes the burst day, so "dormant" can't be
-- contradicted by the burst being tested (docs/aml-alert-design.md,
-- window anchoring).
--
-- Two known v1 gaps, accepted (design doc TODO): (1) baseline = 0 can't
-- distinguish dormant-and-reawakened from newly-onboarded — an
-- establishment clause needs merchant creation date, which isn't in the
-- features yet; (2) the catalog lists this scenario's target as
-- "Business", but the id we emit is merchant_id, so target_level says
-- 'merchant' truthfully — business-entity-level aggregation needs the
-- merchant→business mapping.

{% set scenario_id = "AML-014" %}
{% set scenario_name = "Dormant account sudden high activity" %}

{% if model.batch %}
    {% set run_date = "date('" ~ model.batch.event_time_start.strftime("%Y-%m-%d") ~ "')" %}
{% else %}
    {# parse/compile-time only (no batch context, e.g. `dbt parse`) #}
    {% set run_date = "current_date('" ~ var("aml_local_timezone") ~ "')" %}
{% endif %}

with

-- The ref is also auto-filtered to the batch window by dbt (both models
-- declare event_time='run_date'); the explicit filter states the intent.
features as (

    select *
    from {{ ref('aml_merchant_features') }}
    where run_date = {{ run_date }}

),

breaches as (

    select
        '{{ scenario_id }}' as scenario_id,
        to_hex(md5(concat(
            '{{ scenario_id }}', '|', merchant_id, '|', cast(run_date as string)
        ))) as alert_id,
        '{{ scenario_name }}' as scenario_name,
        'merchant' as target_level,
        merchant_id as target_id,
        run_date,
        -- All three clauses ship with their observed values — the burst leg
        -- is an OR, so one of the two threshold clauses may show a value
        -- below its threshold; which passed is readable from the values.
        to_json(struct(
            [
                struct(
                    'authorize_count_90d' as feature,
                    authorize_count_90d as value,
                    '=' as operator,
                    0 as threshold
                ),
                struct(
                    'prev_day_authorize_count' as feature,
                    prev_day_authorize_count as value,
                    '>=' as operator,
                    {{ var("aml_014_txn_count_threshold") }} as threshold
                ),
                struct(
                    'prev_day_authorize_amount_minor' as feature,
                    prev_day_authorize_amount_minor as value,
                    '>=' as operator,
                    {{ var("aml_014_amount_minor_threshold") }} as threshold
                )
            ] as rule,
            struct(
                -- the burst day, not run_date: the behavior evaluated is
                -- yesterday's
                date_sub(run_date, interval 1 day) as behavior_date,
                example_prev_day_payment_id
            ) as context
        )) as evidence
    from features
    where authorize_count_90d = 0
        and (
            prev_day_authorize_count >= {{ var("aml_014_txn_count_threshold") }}
            or prev_day_authorize_amount_minor >= {{ var("aml_014_amount_minor_threshold") }}
        )

),

-- Cooldown: an unsuppressed alert for the same target within the trailing
-- window suppresses today's row (the table is single-scenario, so no
-- scenario key in the join). Anchoring on *unsuppressed* priors means a
-- persistent breach re-raises once per cooldown period instead of being
-- silenced forever by its own suppressed echoes.
prior_alerts as (

{% if is_incremental() %}
    select target_id, run_date, alert_id
    from {{ this }}
    where not is_suppressed
        and run_date >= date_sub({{ run_date }}, interval {{ var("aml_014_cooldown_days") }} day)
        and run_date < {{ run_date }}
{% else %}
    -- first-ever batch: nothing to suppress against
    select
        cast(null as string) as target_id,
        cast(null as date) as run_date,
        cast(null as string) as alert_id
    from unnest([])
{% endif %}

),

-- The most recent anchoring alert; its run_date also fixes the suppression
-- period below.
suppressor as (

    select
        breaches.alert_id,
        prior_alerts.alert_id as suppressed_by_alert_id,
        prior_alerts.run_date as anchor_run_date
    from breaches
    inner join prior_alerts
        on prior_alerts.target_id = breaches.target_id
    qualify
        row_number() over (
            partition by breaches.alert_id
            order by prior_alerts.run_date desc
        ) = 1

)

select
    breaches.scenario_id,
    breaches.alert_id,
    breaches.scenario_name,
    breaches.target_level,
    breaches.target_id,
    breaches.run_date,
    current_timestamp() as evaluated_at,
    breaches.evidence,
    -- The anchor's cooldown window as it applied to this row: an anchor at
    -- day A suppresses run_dates in [A+1, A+cooldown]. Materialized (though
    -- derivable from the anchor + the var) because the cooldown var will
    -- change — the log freezes the window as evaluated, same as `rule` in
    -- evidence. NULL on unsuppressed rows.
    date_add(suppressor.anchor_run_date, interval 1 day) as suppression_period_start,
    date_add(suppressor.anchor_run_date, interval {{ var("aml_014_cooldown_days") }} day)
        as suppression_period_end,
    suppressor.suppressed_by_alert_id,
    suppressor.suppressed_by_alert_id is not null as is_suppressed
from breaches
left join suppressor using (alert_id)
