-- The log's contract is one row per merchant per run_date; a fan-out here
-- means the payments join, an aggregation, or batch idempotency regressed
-- (e.g. a rerun appended instead of replacing its partition).

select
    merchant_id,
    run_date,
    count(*) as row_count
from {{ ref('aml_merchant_features') }}
group by merchant_id, run_date
having count(*) > 1
