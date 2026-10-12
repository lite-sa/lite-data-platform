-- Litecore mirror of raw lem__activities:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: nothing.
select
    {{
        keep_columns(
            source('litecore', 'lem__activities'),
            [
                'id',
                'reference_id',
                'reference_type',
                'name',
                'activity_id',
                'mcc',
                'mcc_title',
                'is_selected',
                'is_deleted',
                'deleted_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'lem__activities') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
