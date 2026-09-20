-- Latest version per merchant. payments.merchant_id references business_id,
-- not the PK id; rows without one are internal and dropped.
--
-- Two-column allowlist: only the display name belongs in core. Raw is
-- `replace` today, so the dedup is defensive.
select business_id as merchant_id, name as merchant_name

from {{ source('litecore', 'business_management__business_entities') }}
where business_id is not null
qualify
    row_number() over (
        partition by business_id order by updated_at desc, _dlt_load_id desc
    )
    = 1
