{{
    config(
        materialized="table",
        partition_by={
            "field": "payment_creation_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["merchant_id"],
    )
}}

-- Daily payment funnel: one row per payment_creation_date × merchant ×
-- channel × entry_mode × gateway × card_brand × currency ×
-- payment_method. Reads the payments fact — never staging/ops — and
-- FULLY RESTATES with it every run (late mutations restate history,
-- reruns are idempotent, --full-refresh is a no-op by construction;
-- graduates together with payments if that goes incremental).
-- payment_creation_date is the payment's own local calendar date, not a
-- run date.
--
-- Dimensions extended 2026-08-24 with the nb 023 fact port: entry_mode
-- (the slice the auth-rate story splits on — pos vs wallet vs card vs
-- link are different conversations) and card_brand (the product ask;
-- 'unknown' before the instrument_data backfill horizon ~2026-08-17, so
-- brand splits only mean something from then on). gateway changed
-- meaning in the same pass: it is now the fact's decision connector —
-- the gateway that said the final yes or no — not the first-routed
-- connector; 'not_routed' marks payments where no connector ever
-- answered.
--
-- Funnel counts come from the fact's outcome column, so authorized /
-- declined / no_decision PARTITION request_count by construction
-- (singular test), and sum(request_count) reconciles exactly to the
-- fact table's row count (second singular test) — the aggregate may
-- never lose payments. gateway_declined_count enables the net auth
-- rate downstream: net = authorized / (authorized + gateway_declined),
-- gross = authorized / (authorized + declined); the reached/not-reached
-- split is the fact's gateway_reached flag. currency is a dimension, so
-- every amount here sums a single currency.
--
-- Amounts are major units (NUMERIC — exact), reusing the fact's
-- amount/captured_amount/refunded_amount/reversed_amount so the
-- minor→major rule lives in one place. captured/refunded/reversed are
-- booked-funds totals (INSTANT AUTHORIZE fold), attributed to the
-- payment's creation date — not the capture/refund day.

select
    date(created_at_local) as payment_creation_date,
    merchant_id,
    merchant_name,
    channel,
    entry_mode,
    coalesce(gateway, 'not_routed') as gateway,
    coalesce(card_brand, 'unknown') as card_brand,
    currency,
    coalesce(payment_method, 'unknown') as payment_method,
    count(*) as request_count,
    countif(outcome = 'authorized') as authorized_count,
    countif(outcome = 'declined') as declined_count,
    countif(outcome = 'no_decision') as no_decision_count,
    countif(outcome = 'declined' and gateway_reached) as gateway_declined_count,
    sum(amount) as request_amount,
    sum(if(outcome = 'authorized', amount, 0)) as authorized_amount,
    sum(if(outcome = 'declined', amount, 0)) as declined_amount,
    sum(captured_amount) as captured_amount,
    sum(refunded_amount) as refunded_amount,
    sum(reversed_amount) as reversed_amount
from {{ ref('payments') }}
group by
    payment_creation_date,
    merchant_id,
    merchant_name,
    channel,
    entry_mode,
    gateway,
    card_brand,
    currency,
    payment_method
