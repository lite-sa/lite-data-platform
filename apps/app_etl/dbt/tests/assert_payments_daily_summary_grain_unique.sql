-- One row per payment_creation_date × merchant × channel × entry_mode ×
-- gateway × card_brand × currency × payment_method. merchant_name is
-- deliberately left OUT of the grouping: it must be m:1 from merchant_id
-- within a build, so grouping without it is the stricter check — a
-- duplicate here means either the group by regressed or the
-- business_entities join fanned one merchant into two names.
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
