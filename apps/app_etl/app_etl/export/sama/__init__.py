"""SAMA monthly report, one module per data point.

emi/ holds the e-money (EMI) sheet, pi/ the payment institution (PI)
sheet. Each module holds DETAIL_SQL (the supporting rows) and ANSWER_SQL
(the filed number, aggregated from DETAIL_SQL). Set REPORT_MONTH at the
top, then paste the module into a notebook, or run
`python -m app_etl.export.sama.<sheet>.<module>`. Both leave `answer` and
`detail` DataFrames behind. Month boundaries are Asia/Riyadh.
`python -m app_etl.export.sama --month YYYY-MM` runs every answer into one
CSV.
"""
