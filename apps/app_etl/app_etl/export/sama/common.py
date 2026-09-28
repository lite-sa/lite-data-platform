"""Shared pieces for the SAMA data points: report-month parameters, the
latest-version reader and the query runner.

Every query can use @month_start, @month_end (Riyadh DATEs, inclusive),
@tz, @test_ids and @excluded_payment_ids. Answers exclude test merchants;
detail rows keep them, flagged `is_test`.
"""

from __future__ import annotations

import calendar
from datetime import date

import pandas as pd
from google.cloud import bigquery

from app_etl.export.common import EXCLUDED_PAYMENT_IDS, LOCAL_TIMEZONE, latest_version

PROJECT = "lite-data-prod"
RAW = f"{PROJECT}.raw_litecore"

# The dbt seed test_merchants.csv is canonical; a parity test keeps this
# copy equal to it.
TEST_MERCHANT_IDS = [
    "97476ad7-37a0-4288-8f7e-57d0988d3ec8",
    "e8a61e1b-f4b1-4a14-b630-cf08b0656576",
    "2e2a8eab-0e8e-4d95-9a48-917e42249e3a",
    "fc8bf7fe-da2a-4327-afde-641a5929f05e",
]


def lv(table: str) -> str:
    return latest_version(f"{RAW}.{table}")


def month_params(report_month: str) -> list:
    year, month = map(int, report_month.split("-"))
    return [
        bigquery.ScalarQueryParameter("month_start", "DATE", date(year, month, 1)),
        bigquery.ScalarQueryParameter(
            "month_end", "DATE", date(year, month, calendar.monthrange(year, month)[1])
        ),
        bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
        bigquery.ArrayQueryParameter("test_ids", "STRING", TEST_MERCHANT_IDS),
        bigquery.ArrayQueryParameter(
            "excluded_payment_ids", "STRING", EXCLUDED_PAYMENT_IDS
        ),
    ]


def run(sql: str, report_month: str) -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT, location="me-central2")
    config = bigquery.QueryJobConfig(query_parameters=month_params(report_month))
    # frames are small; the REST download avoids the Storage API's flaky streams
    return client.query(sql, job_config=config).to_dataframe(
        create_bqstorage_client=False
    )


def show(answer: pd.DataFrame, detail: pd.DataFrame, report_month: str) -> None:
    print(f"report month {report_month}\n")
    print(
        answer.T.to_string(header=False)
        if len(answer) == 1
        else answer.to_string(index=False)
    )
    print(f"\n{len(detail)} supporting rows")
    with pd.option_context("display.max_columns", None, "display.width", 250):
        print(detail.head(20).to_string(index=False))
