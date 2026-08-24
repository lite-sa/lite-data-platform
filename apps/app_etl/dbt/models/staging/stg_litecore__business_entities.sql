-- Latest version per merchant. business_id — not id, the source PK — is
-- the external identifier payments.merchant_id points at (join contract
-- verified in nb 018); rows without one are internal-only and dropped.
--
-- Deliberate two-column allowlist: business_entities is the merchant's
-- registration profile and none of it belongs in core beyond the display
-- name — add columns consciously. Raw is `replace` disposition today (no
-- version history), so the dedup is defensive; it also stays correct if
-- ingestion moves to the planned snapshot_date design.
select business_id as merchant_id, name as merchant_name

from {{ source('litecore', 'business_management__business_entities') }}
where business_id is not null
qualify
    row_number() over (
        partition by business_id order by updated_at desc, _dlt_load_id desc
    )
    = 1
