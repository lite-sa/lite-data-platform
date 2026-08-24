-- authorized / declined / no_decision count the fact's outcome column,
-- which is mutually exclusive and exhaustive by construction; a row here
-- means a bucket definition drifted and the funnel no longer partitions
-- request_count.
select *
from {{ ref('payments_daily_summary') }}
where authorized_count + declined_count + no_decision_count != request_count
