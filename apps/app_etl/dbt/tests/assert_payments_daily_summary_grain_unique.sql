-- The mart's contract is one row per merchant_id per run_date; a fan-out
-- here means the group by regressed or batch idempotency broke (e.g. a
-- rerun appended instead of replacing its partition).

select
    merchant_id,
    run_date,
    count(*) as row_count
from {{ ref('payments_daily_summary') }}
group by merchant_id, run_date
having count(*) > 1
