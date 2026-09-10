-- Litecore mirror of raw payment_v2__payment_link_consumption:
-- the latest version of every row (raw appends one row per source-row
-- version), PII columns removed, one table for the payments team in
-- Metabase. The column list is the allowlist
-- (docs/schema-management.md §1): a new source column reaches this table
-- only by being added here after a look at the trim map.
--
-- Left out: ip_address, user_agent: device fingerprint; session_id, hosted_url:
-- session-scoped tokens (trim map).
select
    {{
        keep_columns(
            source('litecore', 'payment_v2__payment_link_consumption'),
            [
                'id',
                'link_id',
                'status',
                'consumed_at',
                'completed_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payment_v2__payment_link_consumption') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
