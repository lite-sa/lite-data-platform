-- Litecore mirror of raw settlement__account:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: nothing. created_by and updated_by hold service and tool names
-- (ledger-service, payout-service, Retool-App), never a person.
select
    {{
        keep_columns(
            source('litecore', 'settlement__account'),
            [
                'id',
                'merchant_id',
                'type',
                'entity_id',
                'status',
                'currency',
                'is_deleted',
                'created_by',
                'updated_by',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'settlement__account') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
