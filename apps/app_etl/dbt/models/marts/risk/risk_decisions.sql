{{
    config(
        materialized="table",
        partition_by={"field": "first_evaluated_at", "data_type": "timestamp", "granularity": "day"},
        cluster_by=["mode", "merchant_id"],
    )
}}

-- One row per payment and rule mode: the decision the live rules made
-- (mode = live) and the one the shadow rules would have made (mode =
-- shadow). Shadow rules are is_test rules: evaluated on every payment, never
-- applied. A rule going live moves its matches from shadow rows to live rows.
--
-- decision is the strictest outcome across a payment's evaluations, reject
-- then review then accept: a live reject at any phase stops the payment.
-- Live reads the engine's final_outcome. Shadow has no decision row, so it
-- takes each matched rule's action, accept where none matched. The inner
-- join to payments drops test, hard-deleted and incident-excluded payments.
with

    rules as (

        select id as rule_id, name as rule_name, status as rule_action
        from {{ source('litecore', 'risk_management__risk_rule') }}
        qualify row_number() over (partition by id order by updated_at desc, _dlt_load_id desc) = 1

    ),

    rule_evaluations as (

        select
            evaluation.transaction_id as payment_id,
            if(evaluation.is_test, 'shadow', 'live') as mode,
            evaluation.evaluation_time,
            evaluation.result in ('Matched Rule', 'Test Rule Matched') as is_match,
            rules.rule_name,
            rules.rule_action

        from {{ source('litecore', 'risk_management__transaction_rule_evaluation') }} as evaluation
        left join rules on evaluation.rule_id = rules.rule_id

    ),

    outcomes as (

        select transaction_id as payment_id, 'live' as mode, decided_at as evaluated_at, final_outcome as outcome
        from {{ source('litecore', 'risk_management__transaction_rule_decision') }}

        union all

        select payment_id, mode, evaluation_time as evaluated_at, if(is_match, rule_action, 'accept') as outcome
        from rule_evaluations
        where mode = 'shadow'

    ),

    decisions as (

        select
            payment_id,
            mode,
            min(evaluated_at) as first_evaluated_at,
            case
                when logical_or(outcome = 'reject') then 'reject'
                when logical_or(outcome = 'review') then 'review'
                when logical_and(outcome = 'accept') then 'accept'
                else 'unknown'
            end as decision

        from outcomes
        group by payment_id, mode

    ),

    matched_rules as (

        select payment_id, mode, string_agg(distinct rule_name, ', ' order by rule_name) as matched_rules
        from rule_evaluations
        where is_match
        group by payment_id, mode

    )

select
    decisions.payment_id,
    decisions.mode,
    payments.merchant_id,
    payments.merchant_name,
    payments.channel,
    payments.entry_mode,
    payments.card_brand,
    payments.instrument_type,
    decisions.first_evaluated_at,
    datetime(decisions.first_evaluated_at, '{{ var("local_timezone") }}') as first_evaluated_at_local,
    decisions.decision,
    matched_rules.matched_rules,
    payments.outcome,
    payments.amount,
    payments.currency

from decisions
left join
    matched_rules
    on decisions.payment_id = matched_rules.payment_id
    and decisions.mode = matched_rules.mode
inner join {{ ref('payments') }} as payments on decisions.payment_id = payments.payment_id
