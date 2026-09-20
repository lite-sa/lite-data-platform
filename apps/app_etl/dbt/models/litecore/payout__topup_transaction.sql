-- Litecore mirror of raw payout__topup_transaction:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: request, response: the checkout-session and ledger-limit
-- payloads (customer block, tokenised redirect_urls, hosted_url);
-- comment: free text (trim map).
select
    {{
        keep_columns(
            source('litecore', 'payout__topup_transaction'),
            [
                'id',
                'topup_id',
                'event_type',
                'status',
                'external_ref',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payout__topup_transaction') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
