-- The log's contract is one row per payment_operation_id per run_date; a
-- fan-out here means the payments join or batch idempotency regressed
-- (e.g. a rerun appended instead of replacing its partition).

select
    payment_operation_id,
    run_date,
    count(*) as row_count
from {{ ref('int_payments__daily_activity') }}
group by payment_operation_id, run_date
having count(*) > 1
