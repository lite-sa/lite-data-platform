-- Litecore mirror of raw settlement__settlement_window:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: created_by, updated_by: staff identifiers (trim map).
select
    {{
        keep_columns(
            source('litecore', 'settlement__settlement_window'),
            [
                'id',
                'merchant_id',
                'cycle_id',
                'value_date',
                'closing_date',
                'window_start',
                'window_end',
                'collection_date',
                'status',
                'total_instructions',
                'total_amount',
                'settled_amount',
                'currency',
                'reviewed_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'settlement__settlement_window') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
