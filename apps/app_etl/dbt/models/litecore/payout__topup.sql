-- Litecore mirror of raw payout__topup:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: checkout_session_data: the hosted-checkout session as
-- returned by checkout-session-service, with payment_url (a live
-- checkout capability URL until expires_at) and the raw payload
-- including the customer block (trim map).
select
    {{
        keep_columns(
            source('litecore', 'payout__topup'),
            [
                'id',
                'merchant_id',
                'wallet_id',
                'checkout_session_id',
                'amount',
                'currency',
                'status',
                'expires_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payout__topup') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
