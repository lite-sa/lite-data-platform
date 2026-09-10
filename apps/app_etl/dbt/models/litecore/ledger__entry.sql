-- Litecore mirror of raw ledger__entry:
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
            source('litecore', 'ledger__entry'),
            [
                'id',
                'source',
                'method',
                'external_reference_id',
                'type',
                'amount',
                'direction',
                'currency',
                'exchange_rate',
                'available_balance_after',
                'account_id',
                'parent_transaction_id',
                'order_id',
                'hold_at',
                'captured_at',
                'released_at',
                'expires_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'ledger__entry') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
