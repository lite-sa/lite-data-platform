-- Litecore mirror of raw payment_v2__payment_operations:
-- the latest version of every row (raw appends one row per source-row
-- version), PII columns removed, one table for the payments team in
-- Metabase. The column list is the allowlist
-- (docs/schema-management.md §1): a new source column reaches this table
-- only by being added here after a look at the trim map.
--
-- Left out: metadata: raw provider request and response payloads (trim map). The POS
-- receipt scalars it carries are on the payments mart (auth_external_op,
-- auth_verification, failure_outcome). idempotency_key stays: on POS it is the
-- receipt reference triple (RRN;STAN;TID), and no prod value looks like an
-- email or a phone number (probe 2026-09-09).
select
    {{
        keep_columns(
            source('litecore', 'payment_v2__payment_operations'),
            [
                'id',
                'payment_id',
                'acting_business_account_id',
                'idempotency_key',
                'operation_type',
                'status',
                'amount',
                'currency',
                'reason',
                'initiated_by',
                'connector_type',
                'connector_response_code',
                'connector_response_message',
                'connector_transaction_id',
                'http_status_code',
                'processing_duration_ms',
                'retry_count',
                'max_retries',
                'next_retry_at',
                'terminal_id',
                'entry_mode',
                'reversed_operation_id',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payment_v2__payment_operations') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
