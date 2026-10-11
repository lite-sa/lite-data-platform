-- Litecore mirror of raw lem__organizations:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: nothing.
select
    {{
        keep_columns(
            source('litecore', 'lem__organizations'),
            [
                'id',
                'name',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'lem__organizations') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
