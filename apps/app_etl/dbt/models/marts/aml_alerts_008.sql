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

-- AML-008 evaluation log — one table per scenario (docs/aml-alert-design.md,
-- revised 2026-07-16): every scenario table carries exactly the spine
-- columns (scenario_id, alert_id, scenario_name, target_level, target_id,
-- run_date, evaluated_at, evidence, suppression block) — column-identical
-- across scenarios, unioned by the aml_alerts view; what varies per
-- scenario is only the *content* of `evidence`. A new scenario copies this
-- file: new scenario_id/scenario_name, its own target_level, breach
-- predicate and evidence content, its own begin (the scenario's go-live —
-- the log starts where the record starts). Extract the suppression block
-- into a macro when the copies start to hurt.
--
-- Every breach day gets a row (the tuning record), cooldown lands as
-- is_suppressed — the exporter filters `not is_suppressed` and carries no
-- state. Append-only across days, partition-replace within a day; never
-- --full-refresh the prod table (that stamps historical run_dates with
-- today's rules — rewriting the audit log).
--
-- concurrent_batches=false is required, not an optimization: suppression
-- reads this model's own earlier partitions ({{ this }}), so batches must
-- land in run_date order during bootstrap/backfill.
--
-- AML-008 (v1-simplified 2026-07-16): >= threshold AUTHORIZE operations in
-- the night window ([00:00, 05:00) local) of run_date for one merchant.
-- The catalog name says "POS"; the v1 rule counts ALL AUTHORIZE ops (the
-- POS/per-terminal leg is parked with the credit-ops formulation until the
-- operation_type enum is confirmed — see the design doc TODO).

{% set scenario_id = "AML-008" %}
{% set scenario_name = "POS transactions outside business hours" %}

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
        -- target_level stays out of the hash: a scenario_id determines its
        -- level, and hash inputs value-identical to the pre-target schema
        -- (scenario | merchant | date) keep historical alert_ids stable.
        to_hex(md5(concat(
            '{{ scenario_id }}', '|', merchant_id, '|', cast(run_date as string)
        ))) as alert_id,
        '{{ scenario_name }}' as scenario_name,
        -- Free-form string, no pinned enum yet; this scenario's subject is
        -- the merchant.
        'merchant' as target_level,
        merchant_id as target_id,
        run_date,
        -- `evidence` is spine, its content is the scenario's own — and the
        -- ONLY place scenario-specific data lives (no flat copies: tuning
        -- reads aml_merchant_features at the same grain, or BQ JSON
        -- functions over this column). Shared shape
        -- (docs/aml-alert-design.md): `rule` = one entry per clause
        -- {feature, value, operator, threshold} — the rule as evaluated,
        -- frozen with the alert even if the vars change later; `context` =
        -- non-rule pointers for the investigator; the night evaluated is
        -- the night of run_date itself.
        to_json(struct(
            [
                struct(
                    'night_authorize_count' as feature,
                    night_authorize_count as value,
                    '>=' as operator,
                    {{ var("aml_008_txn_count_threshold") }} as threshold
                )
            ] as rule,
            struct(
                run_date as behavior_date,
                example_night_pos_payment_id,
                example_night_pos_terminal_id
            ) as context
        )) as evidence
    from features
    where night_authorize_count >= {{ var("aml_008_txn_count_threshold") }}

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
        and run_date >= date_sub({{ run_date }}, interval {{ var("aml_008_cooldown_days") }} day)
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
    date_add(suppressor.anchor_run_date, interval {{ var("aml_008_cooldown_days") }} day)
        as suppression_period_end,
    suppressor.suppressed_by_alert_id,
    suppressor.suppressed_by_alert_id is not null as is_suppressed
from breaches
left join suppressor using (alert_id)
