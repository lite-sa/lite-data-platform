-- Litecore mirror of raw settlement__instruction:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: nothing: no PII in this table.
select
    {{
        keep_columns(
            source('litecore', 'settlement__instruction'),
            [
                'id',
                'settlement_window_id',
                'account_cycle_id',
                'destination_account_id',
                'amount',
                'currency',
                'status',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'settlement__instruction') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
