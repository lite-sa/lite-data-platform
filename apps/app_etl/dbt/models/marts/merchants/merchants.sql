-- One row per merchant_id, across both onboarding systems: a
-- business_management merchant is keyed on business_entities.business_id,
-- a lem merchant on business_accounts.id. The two share no key, so a
-- business onboarded in both is two merchants. Should an id appear in
-- both, the lem row wins (a warn-level singular test reports it).
--
-- merchant_name is the business_management display name or the lem legal
-- entity's legal_name. Readers group by merchant_id and attach the name
-- afterwards. A lem wallet belongs to the legal entity, not the account:
-- name it through legal_entity_id. Every source here is `replace`; the
-- dedups are defensive.
with

    test_merchants as (select merchant_id from {{ ref('test_merchants') }}),

    deleted_merchants as (select merchant_id from {{ ref('deleted_merchants') }}),

    business_management as (

        select
            business_id as merchant_id,
            'business_management' as source_system,
            nullif(trim(name), '') as merchant_name,
            cast(null as string) as legal_entity_id,
            cast(null as string) as business_unit_name,
            cast(null as string) as business_account_name,
            status,
            coalesce(is_deleted, false) as is_deleted_at_source,
            created_at
        from {{ source('litecore', 'business_management__business_entities') }}
        where business_id is not null
        qualify
            row_number() over (
                partition by business_id order by updated_at desc, _dlt_load_id desc
            )
            = 1

    ),

    lem_accounts as (

        select *
        from {{ source('litecore', 'lem__business_accounts') }}
        qualify
            row_number() over (partition by id order by updated_at desc, _dlt_load_id desc)
            = 1

    ),

    lem_units as (

        select id, name
        from {{ source('litecore', 'lem__business_units') }}
        qualify
            row_number() over (partition by id order by updated_at desc, _dlt_load_id desc)
            = 1

    ),

    lem_entities as (

        select
            id,
            {{ column_or_null(source('litecore', 'lem__legal_entities'), 'legal_name', 'string') }}
        from {{ source('litecore', 'lem__legal_entities') }}
        qualify
            row_number() over (partition by id order by updated_at desc, _dlt_load_id desc)
            = 1

    ),

    lem as (

        select
            accounts.id as merchant_id,
            'lem' as source_system,
            nullif(trim(entities.legal_name), '') as merchant_name,
            accounts.legal_entity_id,
            nullif(trim(units.name), '') as business_unit_name,
            nullif(trim(accounts.name), '') as business_account_name,
            accounts.status,
            false as is_deleted_at_source,
            accounts.created_at
        from lem_accounts as accounts
        left join lem_units as units on accounts.business_unit_id = units.id
        left join lem_entities as entities on accounts.legal_entity_id = entities.id

    ),

    registry as (

        select *
        from lem
        union all
        select *
        from business_management

    )

select
    merchant_id,
    merchant_name,
    source_system,
    legal_entity_id,
    business_unit_name,
    business_account_name,
    status,
    merchant_id in (select merchant_id from test_merchants) as is_test,
    is_deleted_at_source
    or merchant_id in (select merchant_id from deleted_merchants) as is_deleted,
    created_at
from registry
qualify
    row_number() over (
        partition by merchant_id order by if(source_system = 'lem', 0, 1)
    )
    = 1
