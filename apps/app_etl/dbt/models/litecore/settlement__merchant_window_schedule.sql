-- Litecore mirror of raw settlement__merchant_window_schedule:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: nothing. created_by and updated_by hold service and tool names
-- (system, system-migration, system-0018, Retool-App), never a person.
select
    {{
        keep_columns(
            source('litecore', 'settlement__merchant_window_schedule'),
            [
                'id',
                'merchant_id',
                'acceptance_account_reference',
                'start_times',
                'created_by',
                'updated_by',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'settlement__merchant_window_schedule') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
