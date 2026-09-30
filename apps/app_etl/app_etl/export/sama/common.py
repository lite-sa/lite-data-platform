"""Shared pieces for the SAMA data points: report-month parameters, the
latest-version reader and the query runner.

Every query can use @month_start, @month_end (Riyadh DATEs, inclusive),
@tz, @test_ids, @deleted_ids and @excluded_payment_ids. Answers include test
merchants; detail rows flag them `is_test`. Merchants hard-deleted at source
never appear, not even in detail rows.
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
]

# Merchants hard-deleted at source; raw keeps their history. The dbt seed
# deleted_merchants.csv is canonical; a parity test keeps this copy equal.
DELETED_MERCHANT_IDS = [
    "fc8bf7fe-da2a-4327-afde-641a5929f05e",
    "2e2a8eab-0e8e-4d95-9a48-917e42249e3a",
]


# Payments whose card_brand names the wrong scheme, mapped to the scheme ops
# confirmed. The dbt seed scheme_corrections.csv is canonical; a parity test
# keeps this copy equal to it.
SCHEME_CORRECTIONS = {
    "09362c4d-35a7-4f7d-a37a-0dc2428327cb": "MADA",
    "4af4cd20-675a-484e-b9ed-d0a46b98c4d6": "MADA",
    "851d457e-da36-43f6-b612-71a175825b6d": "MADA",
    "b04f8616-7473-49bf-b249-14da3b078396": "MADA",
    "f1888e91-dc53-431b-95a1-2229f36fb4b8": "MADA",
    "dd51884b-42b0-4ce2-aa23-e678f07be7aa": "MADA",
    "6e1465e3-65eb-4260-b3d4-f9d5668d8f48": "MADA",
    "a75e50b4-f57b-406e-bb64-2e7490e9d1a4": "MADA",
    "242255b6-1e31-40cb-89a5-e11508c7982f": "MADA",
    "5367d3cb-01d0-4c5a-8bb9-9e543e6e4ae4": "MADA",
    "42a8bfb8-5b71-441e-8493-93b2a625bac5": "MADA",
    "6dee25a0-f496-410d-ab8d-e7902372afd4": "MADA",
    "9e9922a0-c1b0-49ed-8c57-72fbfc42f022": "MADA",
    "c264b552-8714-4fee-9c3a-06ee0218b1cc": "MADA",
    "3735ebfe-0659-4735-8fd7-7c3c1e9b9a32": "MADA",
    "a37299c2-2cdd-447e-86d9-c6bba639087b": "MADA",
}


def scheme_corrections() -> str:
    """Subquery of SCHEME_CORRECTIONS: one row per payment_id, scheme."""
    rows = ",\n".join(
        f"struct('{pid}' as payment_id, '{scheme}' as scheme)"
        for pid, scheme in SCHEME_CORRECTIONS.items()
    )
    return f"(select * from unnest([\n{rows}\n]))"


def not_deleted(merchant_col: str) -> str:
    """Predicate that drops hard-deleted merchants and keeps NULL ids."""
    return f"coalesce({merchant_col} not in unnest(@deleted_ids), true)"


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
        bigquery.ArrayQueryParameter("deleted_ids", "STRING", DELETED_MERCHANT_IDS),
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
