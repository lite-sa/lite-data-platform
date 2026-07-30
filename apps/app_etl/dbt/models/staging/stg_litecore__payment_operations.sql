-- Rename/select only, plus the one derived column every time-of-day rule
-- needs: the local wall-clock timestamp. DATETIME() converts the stored
-- UTC instant into the business timezone and drops the offset.
select
    id as payment_operation_id,
    payment_id,
    operation_type,
    {{ column_or_null(source('litecore', 'payment_operations'), 'external_operation_type', 'string') }},
    status,
    amount as amount_minor,
    currency,
    {{ column_or_null(source('litecore', 'payment_operations'), 'terminal_id', 'string') }},
    {{ column_or_null(source('litecore', 'payment_operations'), 'entry_mode', 'string') }},
    created_at,
    datetime(created_at, '{{ var("local_timezone") }}') as created_at_local,
    updated_at,
    datetime(updated_at, '{{ var("local_timezone") }}') as updated_at_local

from {{ source('litecore', 'payment_operations') }}
qualify
    row_number() over (partition by id order by updated_at desc, _dlt_load_id desc) = 1
