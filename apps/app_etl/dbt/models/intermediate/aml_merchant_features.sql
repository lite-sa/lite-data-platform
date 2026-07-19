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

-- Merchant × run_date evaluation log: one partition per knowledge date,
-- point-in-time features (also the future ML feature surface).
-- lookback=0 is load-bearing — the default of 1 would rewrite yesterday's
-- partition every morning. Never --full-refresh the prod table; backfills
-- are explicit --event-time-start/--event-time-end runs.
--
-- Window anchoring (docs/aml-alert-design.md): night features anchor to
-- run_date's own night [00:00, 05:00) local — the night that closed just
-- before the ~06:45 run; the burst/tested day is the last complete local
-- day [D−1]; trailing windows end at D−2 so they exclude the day being
-- tested. Naming convention for trailing windows: a `_Nd` suffix means
-- exactly N complete days, [D−(N+1), D−2]. Window lengths are part of a
-- feature's name and therefore hardcoded here, not dbt vars — a tunable
-- var under a column named `_30d` would let the two silently disagree
-- (thresholds/cooldowns stay vars; they don't name columns).
--
-- The spine is any merchant with an AUTHORIZE op visible in the longest
-- trailing window or the night; a merchant with no visibility anywhere
-- has no row (absence, not zero) — within a row, a 0 count is a genuine
-- observed zero for that window.
--
-- v1 feature set: night_authorize_count (+ example night POS op) for
-- AML-008; the dormancy/burst trio (authorize_count_90d,
-- prev_day_authorize_count/_amount_minor + example payment) for AML-014;
-- authorize_count_avg_30d for the upcoming spike scenario.
-- The credit-mapped features live in git history and
-- notebooks/003-intermediate-features.ipynb; re-add per scenario need,
-- here, never by re-aggregating raw in scenario SQL.
--
-- Deliberately no status filter: counts are attempts, not successes —
-- the simplest thing that exercises the pipeline end to end on dummy
-- data. Revisit alongside the status-enum confirmation (see
-- docs/aml-alert-design.md TODO). count(*) counts operations, so a
-- payment authorized in several attempts counts each attempt — and the
-- amount sum likewise re-counts retried attempts.
--
-- Staging refs deliberately declare no event_time (they'd be batch-filtered
-- on the wrong column); the window below is the only input filter.

{% if model.batch %}
    {% set run_date = "date('" ~ model.batch.event_time_start.strftime("%Y-%m-%d") ~ "')" %}
{% else %}
    {# parse/compile-time only (no batch context, e.g. `dbt parse`) #}
    {% set run_date = "current_date('" ~ var("aml_local_timezone") ~ "')" %}
{% endif %}

{% set prev_day = "date_sub(" ~ run_date ~ ", interval 1 day)" %}

with

-- Every AUTHORIZE op any feature window can see: the trailing complete
-- days [D−91, D−1] (91 = longest trailing window, 90d, plus the D−2
-- offset) plus run_date's own night hours — so op_date = run_date
-- rows are night ops by construction. Ops need the payments join for
-- merchant_id. Both sides are deduped in staging, so no fan-out.
authorize_ops as (

    select
        pay.merchant_id,
        op.payment_operation_id,
        payment_id,
        op.terminal_id,
        op.amount_minor,
        op.created_at_local,
        date(op.created_at_local) as op_date
    from {{ ref('stg_litecore__payment_operations') }} as op
    inner join {{ ref('stg_litecore__payments') }} as pay using (payment_id)
    where op.operation_type = 'AUTHORIZE'
        and (
            date(op.created_at_local)
                between date_sub({{ run_date }}, interval 91 day)
                and {{ prev_day }}
            or (
                date(op.created_at_local) = {{ run_date }}
                and extract(hour from op.created_at_local) >= {{ var("aml_night_start_hour") }}
                and extract(hour from op.created_at_local) < {{ var("aml_night_end_hour") }}
            )
        )

),

aggregated as (

    select
        merchant_id,

        -- Night of run_date ([00:00, 05:00) local): AML-008's feature.
        countif(op_date = {{ run_date }}) as night_authorize_count,
        -- Investigator entry point: ONE concrete op to pull up, so the
        -- payment and terminal ids come from the same row (independent
        -- min()s could name a payment and a terminal that never met).
        -- Earliest POS op (terminal_id present) of the night, tiebroken on
        -- id — deterministic, so reruns diff cleanly. NULL when the night
        -- had no POS op (non-POS ops still count above).
        array_agg(
            if(
                op_date = {{ run_date }} and terminal_id is not null,
                struct(payment_id, terminal_id),
                null
            )
            ignore nulls
            order by created_at_local, payment_operation_id
            limit 1
        )[safe_offset(0)] as example_night_pos_op,

        -- Dormancy baseline [D−91, D−2] (90 complete days) — excludes the
        -- burst day [D−1] so "dormant" can't be contradicted by the burst
        -- being tested. AML-014's feature. No explicit lower bound: the
        -- CTE filter already starts at D−91.
        countif(op_date <= date_sub({{ run_date }}, interval 2 day))
            as authorize_count_90d,

        -- Trailing daily average over [D−31, D−2] (30 complete days), for
        -- the upcoming spike scenario. Denominator fixed at 30 — not
        -- days-with-activity, not merchant tenure — so a young merchant's
        -- average is diluted and spike ratios fire more easily for them;
        -- accepted v1, revisit with AML-014's establishment clause.
        countif(
            op_date between date_sub({{ run_date }}, interval 31 day)
                and date_sub({{ run_date }}, interval 2 day)
        ) / 30 as authorize_count_avg_30d,

        -- The last complete local day [D−1]: AML-014's burst window.
        -- Amount is minor units summed across currencies (99.5% SAR today;
        -- currency-mix policy is an open TODO in the design doc).
        countif(op_date = {{ prev_day }}) as prev_day_authorize_count,
        sum(if(op_date = {{ prev_day }}, amount_minor, 0))
            as prev_day_authorize_amount_minor,
        -- Earliest op of the burst day, same determinism as the night one.
        array_agg(
            if(op_date = {{ prev_day }}, payment_id, null)
            ignore nulls
            order by created_at_local, payment_operation_id
            limit 1
        )[safe_offset(0)] as example_prev_day_payment_id

    from authorize_ops
    group by merchant_id

)

select
    {{ run_date }} as run_date,
    merchant_id,
    night_authorize_count,
    example_night_pos_op.payment_id as example_night_pos_payment_id,
    example_night_pos_op.terminal_id as example_night_pos_terminal_id,
    authorize_count_90d,
    authorize_count_avg_30d,
    prev_day_authorize_count,
    prev_day_authorize_amount_minor,
    example_prev_day_payment_id,
    -- wall-clock generation moment (run_date is the logical date): written
    -- once per partition, restamped only if the partition is regenerated —
    -- which is exactly what it's for, detecting reruns/backfills in-data
    current_timestamp() as evaluated_at
from aggregated
