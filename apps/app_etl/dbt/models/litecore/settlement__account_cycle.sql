-- Litecore mirror of raw settlement__account_cycle:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: nothing. created_by and updated_by hold service and tool names
-- (system-default-config, Retool-App), never a person.
select
    {{
        keep_columns(
            source('litecore', 'settlement__account_cycle'),
            [
                'id',
                'account_id',
                'cycle_id',
                'window_schedule_id',
                'percentage',
                'is_deleted',
                'created_by',
                'updated_by',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'settlement__account_cycle') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
