-- Litecore mirror of raw payment_v2__payments:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: customer, order_data, device, threeds_input, threeds_result, return_url, metadata:
-- shopper, card and request blobs (trim map). splits: unreviewed, NULL on every
-- prod row 2026-09-09. instrument_data stays: its keys are card_brand, type,
-- last_four, expiry_month, expiry_year, future_usage, agreement_id (prod 2026-09-09),
-- receipt-grade facts the payments mart already exposes.
select
    {{
        keep_columns(
            source('litecore', 'payment_v2__payments'),
            [
                'id',
                'merchant_id',
                'acting_business_account_id',
                'amount',
                'currency',
                'status',
                'processing_type',
                'capture_mode',
                'payment_method',
                'channel_id',
                'channel_type',
                'order_id',
                'payment_link_id',
                'payment_link_consumption_id',
                'payment_instrument_id',
                'gateway_reference_id',
                'instrument_data',
                'risk',
                'risk_result',
                'routing_result',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payment_v2__payments') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
