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

-- AML-015 evaluation log — copied from aml_alerts_014 (house style: one
-- table per scenario, spine identical, only the breach predicate and the
-- `evidence` content are this scenario's own; see aml_alerts_008 and
-- docs/aml-alert-design.md for the shared mechanics: microbatch semantics,
-- suppression, why concurrent_batches=false).
--
-- AML-015 "Low-activity account sudden high activity" (scenario_name is a
-- placeholder — confirm against the catalog before go-live): trailing
-- daily average <= threshold over the 30d window (authorize_count_avg_30d,
-- [D−31, D−2]) AND a burst on the last complete day [D−1] — op count >=
-- threshold OR summed amount >= threshold (minor units, 30,000 SAR; the
-- currency-mix TODO applies here exactly as in AML-014).
--
-- Known overlaps, accepted (scenarios are separate catalog entries and
-- co-fire by design): a *dormant* merchant (90d count 0 ⇒ 30d avg 0) whose
-- burst clears both scenarios' thresholds raises AML-014 and AML-015 for
-- the same behavior; and AML-014's establishment gap applies here too — a
-- newly-onboarded merchant has a low average by construction, which makes
-- this rule *more* likely to fire for them (the avg denominator is a fixed
-- 30, not tenure — see the features model).

{% set scenario_id = "AML-015" %}
{% set scenario_name = "Low-activity account sudden high activity" %}

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
        -- The int values are cast to float64 because every element of a
        -- struct array must agree on field types and the avg is float64.
        to_json(struct(
            [
                struct(
                    'authorize_count_avg_30d' as feature,
                    authorize_count_avg_30d as value,
                    '<=' as operator,
                    {{ var("aml_015_avg_30d_threshold") }} as threshold
                ),
                struct(
                    'prev_day_authorize_count' as feature,
                    cast(prev_day_authorize_count as float64) as value,
                    '>=' as operator,
                    {{ var("aml_015_txn_count_threshold") }} as threshold
                ),
                struct(
                    'prev_day_authorize_amount_minor' as feature,
                    cast(prev_day_authorize_amount_minor as float64) as value,
                    '>=' as operator,
                    {{ var("aml_015_amount_minor_threshold") }} as threshold
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
    where authorize_count_avg_30d <= {{ var("aml_015_avg_30d_threshold") }}
        and (
            prev_day_authorize_count >= {{ var("aml_015_txn_count_threshold") }}
            or prev_day_authorize_amount_minor >= {{ var("aml_015_amount_minor_threshold") }}
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
        and run_date >= date_sub({{ run_date }}, interval {{ var("aml_015_cooldown_days") }} day)
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
    date_add(suppressor.anchor_run_date, interval {{ var("aml_015_cooldown_days") }} day)
        as suppression_period_end,
    suppressor.suppressed_by_alert_id,
    suppressor.suppressed_by_alert_id is not null as is_suppressed
from breaches
left join suppressor using (alert_id)
