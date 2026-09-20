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
-- channel × entry_mode × gateway × card_brand × currency × payment_method.
-- Reads the payments fact only and is fully restated with it every run.
-- payment_creation_date is the payment's local calendar date.
--
-- gateway is the decision connector (the one that gave the final answer);
-- 'not_routed' means no connector answered. card_brand is 'unknown' before
-- ~2026-08-17.
--
-- authorized / declined / no_decision partition request_count, and
-- sum(request_count) equals the fact's row count (singular tests).
-- gross = authorized / (authorized + declined);
-- net = authorized / (authorized + gateway_declined).
--
-- Amounts are major units from the fact. captured / refunded / reversed are
-- attributed to the payment's creation date, not the capture or refund day.

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
