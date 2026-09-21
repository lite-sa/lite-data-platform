-- Litecore mirror of raw smart_routing__routing_rule:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: nothing. Routing configuration, no person or secret.
select
    {{
        keep_columns(
            source('litecore', 'smart_routing__routing_rule'),
            [
                'id',
                'profile_id',
                'name',
                'type',
                'scope',
                'priority',
                'rule_definition',
                'is_deleted',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'smart_routing__routing_rule') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
