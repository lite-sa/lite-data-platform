-- Rename/select only, plus the local wall-clock timestamp.
--
-- POS receipt facts live inside the metadata JSONB and are extracted as
-- scalars; the blob itself never leaves staging. receipt_outcome reads one
-- level deeper: raw_provider_response is a JSON string inside the JSON.
with
    latest as (

        select
            id as payment_operation_id,
            payment_id,
            operation_type,
            status,
            amount as amount_minor,
            currency,
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'reason', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'initiated_by', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'terminal_id', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'entry_mode', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'connector_type', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'connector_response_code', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'connector_response_message', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'connector_transaction_id', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'http_status_code', 'int64') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'processing_duration_ms', 'int64') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'reversed_operation_id', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payment_operations'), 'metadata', 'string') }},
            created_at,
            datetime(created_at, '{{ var("local_timezone") }}') as created_at_local,
            updated_at,
            datetime(updated_at, '{{ var("local_timezone") }}') as updated_at_local

        from {{ source('litecore', 'payment_v2__payment_operations') }}
        qualify
            row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            )
            = 1

    )

select
    * except (metadata),
    json_value(metadata, '$.external_operation_type') as external_operation_type,
    json_value(metadata, '$.verification_method') as verification_method,
    json_value(metadata, '$.reconciliation_status') as reconciliation_status,
    json_query(metadata, '$.frontend_error') is not null as frontend_error,
    json_value(
        json_value(metadata, '$.raw_provider_response'), '$.normalizedReceipt.outcome'
    ) as receipt_outcome
from latest
