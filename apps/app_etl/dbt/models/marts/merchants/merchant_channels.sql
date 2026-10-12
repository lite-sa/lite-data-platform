-- One row per sales channel, across both onboarding systems, with the
-- merchant that owns it: payments.channel_id resolves here. A
-- business_management channel points at business_entities.id (the PK), so
-- its owner maps to that row's business_id. A lem channel points at its
-- business account directly. lem__channels lands in raw with its first
-- row; until then the lem half is skipped.
{%- set lem_channels = source('litecore', 'lem__channels') -%}
{%- set lem_channels_landed = adapter.get_relation(
    database=lem_channels.database,
    schema=lem_channels.schema,
    identifier=lem_channels.identifier,
) is not none %}

with

    bms_entities as (

        select id, business_id
        from {{ source('litecore', 'business_management__business_entities') }}
        qualify
            row_number() over (partition by id order by updated_at desc, _dlt_load_id desc)
            = 1

    ),

    bms_channels as (

        select id, business_id, name
        from {{ source('litecore', 'business_management__channels') }}
        qualify
            row_number() over (partition by id order by updated_at desc, _dlt_load_id desc)
            = 1

    ),

    {% if lem_channels_landed %}
    lem_channels as (

        select
            id,
            business_account_id,
            {{ column_or_null(lem_channels, 'name', 'string') }}
        from {{ lem_channels }}
        qualify
            row_number() over (partition by id order by updated_at desc, _dlt_load_id desc)
            = 1

    ),
    {% endif %}

    channels as (

        {% if lem_channels_landed %}
        select
            id as channel_id,
            business_account_id as merchant_id,
            nullif(trim(name), '') as channel_name,
            'lem' as source_system
        from lem_channels
        union all
        {% endif %}
        select
            channels.id as channel_id,
            entities.business_id as merchant_id,
            nullif(trim(channels.name), '') as channel_name,
            'business_management' as source_system
        from bms_channels as channels
        left join bms_entities as entities on channels.business_id = entities.id

    )

select channel_id, merchant_id, channel_name, source_system
from channels
qualify
    row_number() over (
        partition by channel_id order by if(source_system = 'lem', 0, 1)
    )
    = 1
