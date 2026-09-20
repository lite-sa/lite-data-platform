-- Litecore mirror of raw checkout_session__checkout_sessions:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: customer: shopper contact JSONB; metadata, order_data: merchant JSONB;
-- payment_url: a live capability URL until expires_on; redirect_urls: 90% carry
-- a query-string token; idempotency_key: free text, 2 prod values look like an
-- email or a phone (trim map). splits: unreviewed, NULL on every prod row.
select
    {{
        keep_columns(
            source('litecore', 'checkout_session__checkout_sessions'),
            [
                'id',
                'merchant_id',
                'acting_business_account_id',
                'order_id',
                'status',
                'amount',
                'currency',
                'three_ds_required',
                'processing_type',
                'channel_id',
                'expires_on',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'checkout_session__checkout_sessions') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
