-- Litecore mirror of raw ledger__account:
-- the latest version of every row (raw appends one row per source-row
-- version), PII columns removed, one table for the payments team in
-- Metabase. The column list is the allowlist
-- (docs/schema-management.md §1): a new source column reaches this table
-- only by being added here after a look at the trim map.
--
-- Left out: account_number, virtual_iban: bank identifiers that enable payment
-- initiation; opened_by: a staff identifier (trim map).
select
    {{
        keep_columns(
            source('litecore', 'ledger__account'),
            [
                'id',
                'name',
                'direction',
                'status',
                'tag',
                'type',
                'owner_type',
                'owner_product_type',
                'owner',
                'currency',
                'available_balance',
                'description',
                'reference',
                'last_transaction_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'ledger__account') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
