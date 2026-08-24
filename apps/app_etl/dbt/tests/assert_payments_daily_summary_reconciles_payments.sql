-- The aggregate may never lose or invent payments: summed request_count
-- must equal the fact table's row count exactly (nb 018's reconciliation
-- assert). A mismatch means a dimension filtered rows (e.g. a join gone
-- inner) or the summary drifted off payment grain.
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
