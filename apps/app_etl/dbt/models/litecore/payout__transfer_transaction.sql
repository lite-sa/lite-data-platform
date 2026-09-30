-- Litecore mirror of raw payout__transfer_transaction:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: request, response: the ANB and ledger API payloads (request
-- headers, account numbers, IBANs, party names, statement narratives);
-- comment: free text (trim map). One request key passes as its own column:
-- channel, the rail ANB was asked to use (IPS, SARIE or ANB), which the
-- SAMA EMI IPS/SARIE split needs.
{% set events = source('litecore', 'payout__transfer_transaction') -%}
{%- set events_columns = adapter.get_columns_in_relation(events) | map(attribute="name") | map("lower") | list -%}
select
    {{
        keep_columns(
            events,
            [
                'id',
                'transfer_id',
                'event_type',
                'status',
                'external_status',
                'external_ref',
                'created_at',
                'updated_at',
            ]
        )
    }},
    {% if "request" in events_columns -%}
        json_value(request, '$.payload.channel')
    {%- else -%}
        cast(null as string)
    {%- endif %} as channel
from {{ events }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
