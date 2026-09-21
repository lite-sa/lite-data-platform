-- Litecore mirror of raw business_management__channels:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: created_by, updated_by: operator identifiers, 88 rows hold a
-- staff email (trim map).
select
    {{
        keep_columns(
            source('litecore', 'business_management__channels'),
            [
                'id',
                'business_id',
                'name',
                'type',
                'sub_type',
                'is_default',
                'status',
                'terminal_count',
                'terminal_total',
                'sequence',
                'arabic_address1',
                'arabic_address2',
                'english_address1',
                'english_address2',
                'latitude',
                'longitude',
                'arabic_receipt_footer1',
                'arabic_receipt_footer2',
                'english_receipt_footer1',
                'english_receipt_footer2',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'business_management__channels') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
