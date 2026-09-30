-- Litecore mirror of raw terminal_management__terminal:
-- the latest version of every terminal, PII and secrets removed. The column
-- list is an allowlist: a new source column reaches this table only when
-- added here. The key is terminal_id: the table has no id column.
--
-- Left out: vendor_jwt, encrypted_secret_key, kms_key_version, trsm: the
-- NearPay bearer token, the KMS ciphertext of the terminal secret and the
-- code a terminal authenticates with; user_phone: the operator's phone
-- number; device_fingerprint, mqtt_client_id: device and connection
-- identifiers; notes, tags, metadata: free text and JSON never reviewed
-- (NULL on every prod row 2026-09-30); created_by, updated_by: staff
-- identifiers (trim map); terminal_name: a label the merchant types, one
-- prod value reads as a person's first name (probe 2026-09-30);
-- vendor_terminal_id, vendor_merchant_id: unreviewed, NULL on every prod
-- row 2026-09-30.
select
    {{
        keep_columns(
            source('litecore', 'terminal_management__terminal'),
            [
                'terminal_id',
                'tid',
                'merchant_id',
                'business_account_id',
                'channel_id',
                'terminal_type',
                'terminal_source',
                'make',
                'model',
                'operating_system',
                'software_version',
                'status',
                'network_status',
                'connection_status',
                'last_active_date',
                'last_heartbeat',
                'last_seen_at',
                'created_at',
                'updated_at',
                'deleted_at',
            ]
        )
    }}
from {{ source('litecore', 'terminal_management__terminal') }}
qualify
    row_number() over (
        partition by terminal_id order by updated_at desc, _dlt_load_id desc
    )
    = 1
