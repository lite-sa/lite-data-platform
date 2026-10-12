"""Building blocks for the PI sheet: the KYB-approved merchants, the POS
terminal inventory and the authorized payments.
"""

from app_etl.export.common import merchant_directory
from app_etl.export.sama.common import CORE, RAW, lv, not_deleted, scheme_corrections

# Merchants with KYB approved on or before month end; each one gets an
# ECOM account at approval. status holds the current state only. Manually
# created merchants have no kyb_approved_at, so created_at stands in.
KYB_MERCHANTS = f"""(
    select
        business_id as merchant_id,
        name as merchant_name,
        date(coalesce(kyb_approved_at, created_at), @tz) as kyb_approved_on,
        kyb_approved_at is null as kyb_date_is_created_at,
        coalesce(business_id in unnest(@test_ids), false) as is_test
    from {lv("business_management__business_entities")}
    where status = 'KYB approved'
      and date(coalesce(kyb_approved_at, created_at), @tz) <= @month_end
      and (not coalesce(is_deleted, false) or date(deleted_at, @tz) > @month_end)
      and {not_deleted("business_id")}
)"""

# Terminals in service at some point in the month: created by month end,
# not removed before it started. The inventory keeps no history, so a
# terminal moved to another merchant shows its current merchant only.
TERMINALS = f"""(
    select
        t.terminal_id,
        t.tid,
        t.merchant_id,
        be.merchant_name,
        coalesce(t.merchant_id in unnest(@test_ids), false) as is_test,
        t.terminal_type,
        t.status as terminal_status,
        t.created_at,
        t.deleted_at,
        coalesce(date(t.deleted_at, @tz) <= @month_end, false) as is_pulled_out,
        coalesce(date(t.deleted_at, @tz) > @month_end, true) as is_installed
    from (
        select * from `{RAW}.terminal_management__terminal`
        qualify row_number() over (
            partition by terminal_id order by updated_at desc, _dlt_load_id desc
        ) = 1
    ) as t
    left join {merchant_directory(CORE)} as be on be.merchant_id = t.merchant_id
    where date(t.created_at, @tz) <= @month_end
      and coalesce(date(t.deleted_at, @tz) >= @month_start, true)
      and {not_deleted("t.merchant_id")}
)"""

# One row per payment authorized in the report month. Authorized = first SUCCESS on AUTHORIZE (ECOM) or on
# AUTHORIZE, CAPTURE or PURCHASE (POS), dated in Riyadh time. PENDING ops
# never count; reversed and refunded payments stay in, nothing is netted.
# Channel: channel_type, else the ops (only POS ops carry a terminal), else
# a 3DS row means ECOM. Scheme: a hand correction, else
# instrument_data.card_brand, else the EMV application id on a POS op, else
# the 3DS scheme, else UNKNOWN.
AUTHORIZED_PAYMENTS = f"""(
    with
    p as (
        select
            id as payment_id,
            merchant_id,
            amount as amount_minor,
            currency,
            channel_type,
            status,
            nullif(upper(json_value(instrument_data, '$.card_brand')), 'UNKNOWN')
                as card_brand
        from {lv("payment_v2__payments")}
        where id not in unnest(@excluded_payment_ids)
          and {not_deleted("merchant_id")}
    ),
    o as (
        select
            payment_id,
            operation_type,
            status,
            terminal_id,
            json_value(metadata, '$.emv_aid') as emv_aid,
            updated_at
        from {lv("payment_v2__payment_operations")}
    ),
    threeds as (
        select payment_id, min(nullif(upper(authentication_scheme), '')) as scheme
        from {lv("payment_v2__threeds")}
        where payment_id is not null
        group by payment_id
    ),
    pos_ops as (
        select
            payment_id,
            min(terminal_id) as tid,
            min(
                case substr(emv_aid, 1, 10)
                    when 'A000000228' then 'MADA'
                    when 'A000000003' then 'VISA'
                    when 'A000000004' then 'MASTERCARD'
                    when 'A000000025' then 'AMEX'
                end
            ) as emv_scheme
        from o
        where terminal_id is not null
        group by payment_id
    ),
    op_flags as (
        select
            payment_id,
            logical_or(terminal_id is not null) as any_pos_op,
            logical_or(terminal_id is null) as any_ecom_op
        from o
        group by payment_id
    ),
    chan as (
        select
            p.payment_id,
            case
                when p.channel_type = 'IN-PERSON' or coalesce(f.any_pos_op, false)
                then 'POS'
                when p.channel_type = 'ECOM'
                    or coalesce(f.any_ecom_op, false)
                    or d.payment_id is not null
                then 'ECOM'
                else 'UNKNOWN'
            end as channel
        from p
        left join op_flags as f using (payment_id)
        left join threeds as d using (payment_id)
    ),
    decision as (
        select o.payment_id, min(if(o.status = 'SUCCESS', o.updated_at, null)) as authorized_at
        from o
        join chan as c using (payment_id)
        where if(
            c.channel = 'POS',
            o.operation_type in ('AUTHORIZE', 'CAPTURE', 'PURCHASE'),
            o.operation_type = 'AUTHORIZE'
        )
        group by o.payment_id
    ),
    scheme as (
        select
            p.payment_id,
            coalesce(x.scheme, p.card_brand, s.emv_scheme, d.scheme, 'UNKNOWN')
                as scheme,
            case
                when x.scheme is not null then 'correction'
                when p.card_brand is not null then 'instrument_data'
                when s.emv_scheme is not null then 'emv_aid'
                when d.scheme is not null then 'threeds'
                else 'unknown'
            end as scheme_source
        from p
        left join {scheme_corrections()} as x using (payment_id)
        left join pos_ops as s using (payment_id)
        left join threeds as d using (payment_id)
    )
    select
        p.payment_id,
        p.merchant_id,
        p.status as payment_status,
        be.merchant_name,
        coalesce(p.merchant_id in unnest(@test_ids), false) as is_test,
        c.channel,
        s.tid,
        k.authorized_at,
        p.amount_minor,
        p.amount_minor / 100 as amount_sar,
        p.currency,
        g.scheme,
        g.scheme_source,
        if(g.scheme in ('MADA', 'VISA', 'MASTERCARD'), g.scheme, 'Other')
            as scheme_reported
    from p
    join chan as c using (payment_id)
    join decision as k using (payment_id)
    join scheme as g using (payment_id)
    left join pos_ops as s using (payment_id)
    left join {merchant_directory(CORE)} as be on be.merchant_id = p.merchant_id
    where date(k.authorized_at, @tz) between @month_start and @month_end
)"""

# One row per terminal in service in the month, plus any TID that took a
# POS payment in the month but is missing from the inventory.
TERMINAL_ACTIVITY = f"""
with pos as (
    select
        tid,
        any_value(merchant_id) as payment_merchant_id,
        count(*) as payments,
        sum(amount_minor) as volume_minor,
        min(authorized_at) as first_payment_at,
        max(authorized_at) as last_payment_at
    from {AUTHORIZED_PAYMENTS}
    where channel = 'POS' and tid is not null
    group by tid
)
select
    coalesce(t.tid, p.tid) as tid,
    t.terminal_id,
    coalesce(t.merchant_id, p.payment_merchant_id) as merchant_id,
    t.merchant_name,
    coalesce(t.is_test, p.payment_merchant_id in unnest(@test_ids), false) as is_test,
    t.terminal_type,
    t.terminal_status,
    t.created_at,
    t.deleted_at,
    coalesce(t.is_installed, false) as is_installed,
    coalesce(t.is_pulled_out, false) as is_pulled_out,
    t.terminal_id is not null as in_inventory,
    p.payment_merchant_id,
    coalesce(p.payments, 0) as payments_in_month,
    coalesce(p.volume_minor, 0) / 100 as volume_in_month_sar,
    p.first_payment_at,
    p.last_payment_at,
    p.tid is not null as is_active
from {TERMINALS} as t
full join pos as p on p.tid = t.tid
order by merchant_name, tid
"""
