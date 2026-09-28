"""Section I.3: SADAD billers.

Filed as zero: Lite has no SADAD biller integration, so no source table
exists. Replace this with a real query once one does.
"""

from app_etl.export.sama.common import run, show

REPORT_MONTH = "2026-08"

DETAIL_SQL = "select cast(null as string) as merchant_id limit 0"

ANSWER_SQL = "select 0 as sadad_billers, 0 as active, 0 as inactive"

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
