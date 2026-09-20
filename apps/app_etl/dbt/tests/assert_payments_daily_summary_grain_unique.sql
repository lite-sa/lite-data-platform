-- One row per payment_creation_date × merchant × channel × entry_mode ×
-- gateway × card_brand × currency × payment_method. merchant_name stays out
-- of the grouping, so a merchant with two names also fails here.
select
    payment_creation_date,
    merchant_id,
    channel,
    entry_mode,
    gateway,
    card_brand,
    currency,
    payment_method,
    count(*) as row_count
from {{ ref('payments_daily_summary') }}
group by
    payment_creation_date,
    merchant_id,
    channel,
    entry_mode,
    gateway,
    card_brand,
    currency,
    payment_method
having count(*) > 1
