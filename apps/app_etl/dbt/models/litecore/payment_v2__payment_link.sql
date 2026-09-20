-- Litecore mirror of raw payment_v2__payment_link:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: public_token: the link's capability token; redirect_urls: token-bearing
-- return URLs; description: merchant free text; customer, metadata: payer
-- contact and merchant JSONB; created_by: a staff identifier (trim map).
select
    {{
        keep_columns(
            source('litecore', 'payment_v2__payment_link'),
            [
                'id',
                'merchant_id',
                'acting_business_account_id',
                'merchant_reference_id',
                'mode',
                'status',
                'amount',
                'currency',
                'three_ds_required',
                'processing_type',
                'is_hierarchy',
                'channel_id',
                'expires_at',
                'max_uses',
                'reserved_count',
                'paid_count',
                'failed_count',
                'last_paid_at',
                'reservation_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payment_v2__payment_link') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
