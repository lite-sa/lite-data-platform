-- Latest version per sales channel: a merchant's POS store, or an ecom
-- API / hosted-checkout / payment-link channel. `id` is what
-- payments.channel_id and checkout_sessions.channel_id point at. Raw is
-- `replace` disposition today (no version history), so the dedup is
-- defensive; it stays correct if ingestion moves to the snapshot_date
-- design.
--
-- Deliberate allowlist: display name, type/sub_type, status, is_default.
-- Store addresses, lat/long, the receipt footers, terminal counts and the
-- created_by/updated_by operator ids stay behind raw IAM
-- (docs/schema-management.md §1). The owning business is
-- `business_id` -> business_entities.id (the source PK, not the
-- business_id merchants are keyed by): left out until a consumer needs
-- the mapping.
select
    id as channel_id,
    name as channel_name,
    type as channel_type,
    sub_type as channel_sub_type,
    status as channel_status,
    is_default,
    created_at,
    updated_at

from {{ source('litecore', 'business_management__channels') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
