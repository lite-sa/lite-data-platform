"""Section III.3: SADAD payments into wallets, count and value.

Monthly. Filed as zero: Lite has no SADAD product, and no ledger entry
type or payout table records one.
"""

from app_etl.export.sama.common import run, show

REPORT_MONTH = "2026-08"

DETAIL_SQL = """
select cast(null as string) as entry_id
limit 0
"""

ANSWER_SQL = """
select 0 as volume, 0.0 as value_sar
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
