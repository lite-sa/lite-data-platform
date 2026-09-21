-- Litecore mirror of raw pricing_engine__rule_evaluation:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: nothing. No shopper data; matched_rules, winning_rule and
-- applied_price are the merchant's fee terms (trim map).
select
    {{
        keep_columns(
            source('litecore', 'pricing_engine__rule_evaluation'),
            [
                'id',
                'origin',
                'origin_reference',
                'parent_origin_reference',
                'merchant_id',
                'operation',
                'original_amount',
                'matched_rules',
                'winning_rule',
                'winning_rule_id',
                'applied_price',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'pricing_engine__rule_evaluation') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
