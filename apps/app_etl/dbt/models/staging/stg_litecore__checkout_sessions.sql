-- Rename/select only, deduped to the latest version per key: the standard
-- stg_ dedup contract. Grain: checkout_session_id (one hosted checkout
-- session). Nothing links a session to its payment except
-- (merchant_id, order_id) = payments.(merchant_id, order_id), nb 026's
-- verified, order-grain contract; a merchant can open several sessions
-- for one order_id.
--
-- THE PII ALLOWLIST for this table (docs/schema-management.md §1):
-- `customer` (shopper contact JSONB), `metadata` and `order_data`
-- (merchant-supplied JSONB, may carry anything) and `payment_url` (a live
-- checkout capability URL until expires_on) never leave this view; only
-- presence booleans do. `redirect_urls` is the merchant's success/failure
-- return-URL pair and 90% of prod rows carry a query-string token (probe
-- 2026-09-06), so only each URL's host passes. `idempotency_key` is
-- merchant free text (2 prod keys look like an email or a phone number):
-- exposed as a sha256, so duplicates stay detectable without the key.
with
    latest as (

        select
            id as checkout_session_id,
            merchant_id,
            {{ column_or_null(source('litecore', 'checkout_session__checkout_sessions'), 'acting_business_account_id', 'string') }},
            order_id,
            status,
            amount as amount_minor,
            currency,
            {{ column_or_null(source('litecore', 'checkout_session__checkout_sessions'), 'processing_type', 'string') }},
            {{ column_or_null(source('litecore', 'checkout_session__checkout_sessions'), 'three_ds_required', 'bool') }},
            {{ column_or_null(source('litecore', 'checkout_session__checkout_sessions'), 'channel_id', 'string') }},
            {{ column_or_null(source('litecore', 'checkout_session__checkout_sessions'), 'idempotency_key', 'string') }},
            redirect_urls,
            {{ column_or_null(source('litecore', 'checkout_session__checkout_sessions'), 'customer', 'string') }},
            {{ column_or_null(source('litecore', 'checkout_session__checkout_sessions'), 'metadata', 'string') }},
            {{ column_or_null(source('litecore', 'checkout_session__checkout_sessions'), 'order_data', 'string') }},
            expires_on,
            created_at,
            updated_at

        from {{ source('litecore', 'checkout_session__checkout_sessions') }}
        qualify
            row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            )
            = 1

    )

select
    checkout_session_id,
    merchant_id,
    acting_business_account_id,
    order_id,
    status,
    amount_minor,
    currency,
    processing_type,
    three_ds_required,
    channel_id,
    -- host only: the path and query of a merchant return URL carry tokens
    net.host(json_value(redirect_urls, '$.success')) as redirect_success_host,
    net.host(json_value(redirect_urls, '$.failure')) as redirect_failure_host,
    -- presence only: the blobs themselves stay behind raw IAM
    customer is not null as has_customer,
    metadata is not null as has_metadata,
    order_data is not null as has_order_data,
    to_hex(sha256(idempotency_key)) as idempotency_key_sha256,
    expires_on,
    datetime(expires_on, '{{ var("local_timezone") }}') as expires_on_local,
    created_at,
    datetime(created_at, '{{ var("local_timezone") }}') as created_at_local,
    updated_at
from latest
