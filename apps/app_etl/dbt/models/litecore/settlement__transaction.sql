-- Litecore mirror of raw settlement__transaction:
-- the latest version of every row (raw appends one row per source-row
-- version), PII columns removed, one table for the payments team in
-- Metabase. The column list is the allowlist
-- (docs/schema-management.md §1): a new source column reaches this table
-- only by being added here after a look at the trim map.
--
-- Left out: nothing: no PII in this table.
select
    {{
        keep_columns(
            source('litecore', 'settlement__transaction'),
            [
                'id',
                'settlement_window_id',
                'parent_payment_id',
                'external_reference_id',
                'operation',
                'amount',
                'currency',
                'fees',
                'settled_amount',
                'is_settled',
                'status',
                'hold_at',
                'refunded_at',
                'removed_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'settlement__transaction') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
