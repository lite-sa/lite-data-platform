-- Summed request_count must equal the fact table's row count: the summary
-- never loses or invents payments.
with

    summary as (

        select coalesce(sum(request_count), 0) as summary_payments
        from {{ ref('payments_daily_summary') }}

    ),

    fact as (select count(*) as fact_payments from {{ ref('payments') }})

select
    summary_payments,
    fact_payments
from summary
cross join fact
where summary_payments != fact_payments
