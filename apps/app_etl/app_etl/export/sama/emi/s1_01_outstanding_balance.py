"""Section I.1: total outstanding e-money balance at the end of the month.

Point in time. Per WALLET account, the `available_balance_after` of its
last ledger entry on or before month end (ordered by created_at, then
order_id); a wallet with no entry by then holds 0.
Memo columns: wallets with a negative balance, and holds still open at
month end (reserved for a pending payout, so outside the available
balance).
"""

from app_etl.export.sama.common import lv, run, show
from app_etl.export.sama.emi.common import WALLETS

REPORT_MONTH = "2026-08"

DETAIL_SQL = f"""
with last_entry as (
    select
        e.account_id,
        e.id as last_entry_id,
        e.type as last_entry_type,
        e.created_at as last_entry_at,
        e.available_balance_after
    from {lv("ledger__entry")} as e
    where date(e.created_at, @tz) <= @month_end
    qualify row_number() over (
        partition by e.account_id order by e.created_at desc, e.order_id desc
    ) = 1
),
open_holds as (
    select e.account_id, sum(e.amount) as open_holds_minor, count(*) as open_holds
    from {lv("ledger__entry")} as e
    where e.direction = 'DEBIT'
      and e.hold_at is not null
      and date(e.hold_at, @tz) <= @month_end
      and (
          coalesce(e.captured_at, e.released_at) is null
          or date(coalesce(e.captured_at, e.released_at), @tz) > @month_end
      )
    group by e.account_id
)
select
    w.*,
    l.last_entry_id,
    l.last_entry_type,
    l.last_entry_at,
    coalesce(l.available_balance_after, 0) as balance_minor,
    coalesce(l.available_balance_after, 0) / 100 as balance_sar,
    coalesce(h.open_holds_minor, 0) as open_holds_minor,
    coalesce(h.open_holds, 0) as open_holds
from {WALLETS} as w
left join last_entry as l using (account_id)
left join open_holds as h using (account_id)
order by balance_minor desc
"""

ANSWER_SQL = f"""
select
    round(sum(balance_minor) / 100, 2) as outstanding_balance_sar,
    count(*) as wallets,
    countif(balance_minor < 0) as negative_balance_wallets,
    round(sum(open_holds_minor) / 100, 2) as memo_open_holds_sar
from ({DETAIL_SQL})
where not is_test
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
