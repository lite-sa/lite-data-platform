"""Building blocks for the EMI sheet: the wallet population, the movement
rule, per-wallet activity and wallet payouts with their rail.
"""

from app_etl.export.common import merchant_directory
from app_etl.export.sama.common import RAW, lv, not_deleted

# ANB routes a payout below this amount (halalas) on IPS, the rest on SARIE.
IPS_LIMIT_MINOR = 2_000_000


# E-money population: ledger accounts tagged WALLET that existed at month end.
WALLETS = f"""(
    select
        a.id as account_id,
        a.owner as merchant_id,
        be.name as merchant_name,
        a.owner_type,
        a.status as account_status,
        coalesce(a.owner in unnest(@test_ids), false) as is_test,
        a.created_at as account_created_at
    from {lv("ledger__account")} as a
    left join {merchant_directory(RAW)} as be on be.business_id = a.owner
    where a.tag = 'WALLET'
      and date(a.created_at, @tz) <= @month_end
      and {not_deleted("a.owner")}
)"""

# An entry `e` that moved money: not a hold, not a released hold, not the
# credit leg that a capture, refund or release posts against its hold, and
# not a move between two compartments of the same wallet.
IS_MOVEMENT = """(
    e.hold_at is null
    and e.released_at is null
    and e.type != 'RELEASE_INCOMING_TRANSFER'
    and not (
        e.direction = 'CREDIT'
        and e.type in (
            'CAPTURE_BENEFICIARY_TRANSFER',
            'CAPTURE_WITHDRAWAL',
            'EXTERNAL_SETTLEMENT',
            'REFUND_PAYMENT',
            'RELEASE_HOLD',
            'RELEASE_WITHDRAWAL',
            'RELEASE_EXTERNAL_SETTLEMENT',
            'RELEASE_REFUND_PAYMENT',
            'REVERSE_ISSUING',
            'INTERNAL_CLOSE_ISSUING',
            'CAPTURE_ISSUING'
        )
    )
)"""

# One row per wallet in the population, with its movements in the month.
WALLET_ACTIVITY = f"""
with moves as (
    select
        e.account_id,
        count(*) as movements,
        string_agg(distinct concat(e.type, ' ', e.direction), ', ') as movement_types,
        min(e.created_at) as first_movement_at,
        max(e.created_at) as last_movement_at
    from {lv("ledger__entry")} as e
    where date(e.created_at, @tz) between @month_start and @month_end
      and {IS_MOVEMENT}
    group by e.account_id
)
select
    w.*,
    coalesce(m.movements, 0) as movements,
    m.movement_types,
    m.first_movement_at,
    m.last_movement_at,
    m.account_id is not null as is_active
from {WALLETS} as w
left join moves as m using (account_id)
order by movements desc, merchant_name
"""

# Wallet payouts booked in the month, ledger first: the wallet debit that
# captured the payout hold, linked to its payout transfer by
# parent_transaction_id. `matched` shows rows found on one side only.
WALLET_PAYOUTS = f"""
with ledger as (
    select
        e.id as entry_id,
        e.parent_transaction_id as transfer_id,
        e.amount as entry_amount_minor,
        e.created_at as booked_at,
        w.account_id,
        w.merchant_id,
        w.merchant_name,
        w.is_test
    from {lv("ledger__entry")} as e
    join {WALLETS} as w using (account_id)
    where e.type = 'CAPTURE_BENEFICIARY_TRANSFER' and e.direction = 'DEBIT'
),
payout as (
    select
        t.id as transfer_id,
        t.merchant_id,
        t.status as transfer_status,
        t.comment,
        t.amount as transfer_amount_minor,
        t.processed_at
    from {lv("payout__transfer")} as t
    where t.direction = 'OUTGOING'
      and t.status = 'completed'
      and t.comment like '%Transfer%'
      and {not_deleted("t.merchant_id")}
),
channel as (
    -- the ANB initiation request is the only event that carries the channel
    select
        transfer_id,
        array_agg(
            json_value(request, '$.payload.channel') ignore nulls
            order by created_at desc limit 1
        )[safe_offset(0)] as channel
    from {lv("payout__transfer_transaction")}
    where event_type = 'BANK_TRANSFER_INITIATION_RECORDED'
    group by transfer_id
),
linked as (
    select
        coalesce(l.transfer_id, p.transfer_id) as transfer_id,
        l.entry_id,
        l.account_id,
        coalesce(l.merchant_id, p.merchant_id) as merchant_id,
        l.merchant_name,
        coalesce(l.is_test, p.merchant_id in unnest(@test_ids), false) as is_test,
        coalesce(l.booked_at, p.processed_at) as booked_at,
        coalesce(l.entry_amount_minor, p.transfer_amount_minor) as amount_minor,
        p.transfer_amount_minor,
        p.transfer_status,
        p.comment,
        case
            when p.transfer_id is null then 'ledger_only'
            when l.entry_id is null then 'payout_only'
            else 'both'
        end as matched
    from ledger as l
    full join payout as p on p.transfer_id = l.transfer_id
)
select
    k.*,
    k.amount_minor / 100 as amount_sar,
    c.channel,
    case
        when c.channel in ('IPS', 'SARIE') then c.channel
        when k.amount_minor < {IPS_LIMIT_MINOR} then 'IPS'
        else 'SARIE'
    end as rail,
    if(c.channel in ('IPS', 'SARIE'), 'channel', 'amount') as rail_basis
from linked as k
left join channel as c using (transfer_id)
where date(k.booked_at, @tz) between @month_start and @month_end
order by k.booked_at
"""
