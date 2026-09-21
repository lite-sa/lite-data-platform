-- Litecore mirror of raw smart_routing__transaction_evaluation:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: transaction_initiated_message: the raw inbound payment event
-- (trim map); published_event: an event payload, NULL on every row, shape
-- never reviewed.
select
    {{
        keep_columns(
            source('litecore', 'smart_routing__transaction_evaluation'),
            [
                'id',
                'main_transaction_evaluation_id',
                'transaction_id',
                'payment_id',
                'merchant_id',
                'operation_type',
                'status',
                'gateway_id',
                'routing_obj',
                'matched_rules',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'smart_routing__transaction_evaluation') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
