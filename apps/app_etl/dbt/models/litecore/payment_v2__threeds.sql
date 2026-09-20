-- Litecore mirror of raw payment_v2__threeds:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: customer, device, order_data: cardholder, browser and basket blobs;
-- authentication_value: the CAVV cryptogram; xid, creq, acs_url, return_url,
-- redirect_url: the rest of the EMV 3DS exchange and token-bearing URLs (trim
-- map); idempotency_key: a uuid nobody joins on.
select
    {{
        keep_columns(
            source('litecore', 'payment_v2__threeds'),
            [
                'id',
                'merchant_id',
                'acting_business_account_id',
                'payment_id',
                'instrument_id',
                'status',
                'version',
                'authentication_scheme',
                'trans_status',
                'trans_status_reason',
                'eci',
                'acs_challenge_mandated',
                'acs_authentication_type',
                'acs_challenge_cancel_reason',
                'authentication_time',
                'threeds_server_trans_id',
                'ds_transaction_id',
                'acs_transaction_id',
                'acs_reference_number',
                'ds_reference_number',
                'acs_operator_id',
                'provider_authentication_reference',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payment_v2__threeds') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
