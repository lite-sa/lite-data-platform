"""Daily per-merchant transaction report exported to the egress bucket.

One CSV per merchant per day: a payment-grain spine (every payment the
merchant created on the report day, Riyadh calendar) enriched with its
settlement transaction — fee breakdown, window status/value day, settled
amount. Productionizes `notebooks/021-merchant-daily-report.ipynb`; the
notebook remains the backfill / full-history version.

Each run covers exactly ONE day: the previous complete Riyadh day by
default (the job is scheduled 09:00 Asia/Riyadh, after the 06:45 ingest
DAG has landed that morning's raw), `--date` overrides for reruns. A
merchant with no payments that day still gets a file — header-only — so
the delivery side can distinguish "no activity" from "report missing".

Runs like the notify job: BigQuery reads + GCS writes, no Postgres, no
VPC flags. Needs `GCS_BUCKET_EGRESS` (plus the usual `GCP_PROJECT` /
`BQ_DATASET_RAW`); `--dry-run` writes local files and skips the upload.

Bucket layout (docs/merchant-reporting-design.md §J5 path convention):

    gs://<egress>/merchant-reports/<merchant_id>/daily_transactions/
        transactions_<report_day>_run_<run_date>.csv

The filename carries the run date (Riyadh) next to the report day so a
backfill is traceable: re-running an old day writes a NEW file beside the
original instead of silently replacing it, and the newest run date is the
authoritative version of that report day. Same-day reruns overwrite —
idempotent by design.

Refund / multi-window posture (v1): the file keeps payment grain. If a
payment ever carries more than one settlement transaction (refund or
chargeback settling in a second window), the row keeps the EARLIEST
transaction — the sale, whose fees stay correct — `n_settlement_txns`
counts what it carries, and the run logs a warning instead of failing:
a refund is an event of its own day and belongs in a report type of its
own, never a mutation of an already-delivered daily file. The notebook
keeps the blocking version of this check so the first refund on prod
gets a person's eyes before that report type exists.

Column posture matches the notebook: every source column plus derived
ones, minus columns that are entirely NaN for the merchant that day
(header-only files keep the full schema). This means the header can
vary between days until a fixed layout comes from product (Track J J1).
"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import bigquery, storage

from app_etl.config import Settings

# Mirrors dbt's local_timezone var: payment_creation_date is a local
# calendar date, and "previous day" is a local-midnight question.
LOCAL_TIMEZONE = "Asia/Riyadh"

# The merchants in scope: the original two (CEO ask, 2026-08-20 — same
# mids as notebooks/020/021) plus the 2026-08-24 KYB-approved batch.
# Widening to all merchants is a deliberate decision (refund posture,
# file-count, layout sign-off), not a config default.
MERCHANT_IDS = [
    "6b58b5d3-d381-4bf0-bce3-812c812e11d5",  # fine table Company
    "afc004ce-20a0-4dd0-86c3-e9de6324590b",  # altawsil alashhal Company Ltd.
    "e70d441e-678d-477c-840d-7eb486981369",  # bayt altawabil Company For Trading
    "ae53c269-6258-4b96-8cbe-1f1c4fa87b13",  # alruyat almutamayiza Company For Meals
    "4da9902e-baed-4eff-9743-d9cef5d29442",  # alquwa almuttalaqa Center Sport
    "1dd2f1eb-e0ff-41f2-b56b-48b7434e07f2",  # J hub
    "65de1bf8-20c2-4034-a42f-ef3ca04fb34d",  # Alatima Alraeia Company Ltd.
]

GCS_PREFIX = "merchant-reports"
REPORT_TYPE = "daily_transactions"

# Fee types always materialized as columns so a day without settled
# payments (or a header-only file) keeps a stable schema; genuinely new
# source fee types still appear dynamically alongside these.
BASE_FEE_TYPES = ("mdr", "vat", "flat")
BASE_FEE_COLS = [f"fee_{t}_minor" for t in BASE_FEE_TYPES]

TERMINAL_WINDOW_STATUSES = ("SUCCESS", "FAILED")

# Settlement columns the join contributes (post-rename). Ensured to exist
# even when the day has no settlement rows at all, so empty and quiet days
# keep the same header as busy ones.
SETTLE_EXPORT_COLS = [
    "settlement_transaction_id",
    "settlement_status",
    "is_settled",
    "settled_amount_minor",
    "settlement_window_id",
    "window_status",
    "value_day",
    "n_settlement_txns",
    *BASE_FEE_COLS,
    "fee_total_minor",
]


def fetch_payments(
    client: bigquery.Client, raw: str, report_day: date
) -> pd.DataFrame:
    """The day's payments for the in-scope merchants, latest version per id
    (the dedup a stg_ model would own), with the merchant's display name.
    """
    query = f"""
        select p.* except (_dlt_load_id, _dlt_id), b.name as merchant_name
        from (
            select * from `{raw}.payment_v2__payments`
            qualify row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            ) = 1
        ) p
        left join (
            select business_id, name
            from `{raw}.business_management__business_entities`
            where business_id is not null
            qualify row_number() over (
                partition by business_id order by updated_at desc, _dlt_load_id desc
            ) = 1
        ) b on b.business_id = p.merchant_id
        where p.merchant_id in unnest(@merchant_ids)
          and date(datetime(p.created_at, @tz)) = @report_day
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("merchant_ids", "STRING", MERCHANT_IDS),
            bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
            bigquery.ScalarQueryParameter("report_day", "DATE", report_day),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def fetch_settlement(
    client: bigquery.Client, raw: str, payment_ids: list[str]
) -> pd.DataFrame:
    """Settlement transactions for the day's payments, window context joined
    in (all window statuses — on a payment row this is "where does
    settlement stand", not a payout statement).
    """
    query = f"""
        select
            t.* except (_dlt_load_id, _dlt_id),
            w.merchant_id as window_merchant_id,
            w.status as window_status,
            format_date('%F', date(datetime(w.value_date, @tz))) as value_day
        from (
            select * from `{raw}.settlement__transaction`
            qualify row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            ) = 1
        ) t
        left join (
            select * from `{raw}.settlement__settlement_window`
            qualify row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            ) = 1
        ) w on w.id = t.settlement_window_id
        where t.parent_payment_id in unnest(@payment_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("payment_ids", "STRING", payment_ids),
            bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def _empty_settlement() -> pd.DataFrame:
    """Schema stand-in when the day has no payments (no settlement query)."""
    return pd.DataFrame(
        columns=[
            "id",
            "parent_payment_id",
            "settlement_window_id",
            "status",
            "is_settled",
            "amount",
            "settled_amount",
            "fees",
            "created_at",
            "window_merchant_id",
            "window_status",
            "value_day",
        ]
    )


def pivot_fees(settle: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """fees is a JSON string of [{fee_type, amount, ...}] — parse once,
    pivot to one column per fee type (missing type on a txn -> 0). Returns
    the frame plus the fee column list (base types always present).
    """
    settle = settle.copy()
    fees_long = (
        settle.loc[settle["fees"].notna(), ["id", "fees"]]
        .assign(fee=lambda d: d["fees"].apply(json.loads))
        .explode("fee")
    )
    if len(fees_long):
        fees_long["fee_type"] = fees_long["fee"].str["fee_type"]
        fees_long["fee_amount_minor"] = fees_long["fee"].str["amount"].astype("int64")
        fees_wide = (
            fees_long.pivot_table(
                index="id", columns="fee_type", values="fee_amount_minor", aggfunc="sum"
            )
            .add_prefix("fee_")
            .add_suffix("_minor")
        )
        settle = settle.merge(fees_wide, on="id", how="left")
    for col in BASE_FEE_COLS:
        if col not in settle.columns:
            settle[col] = 0
    extra = sorted(
        c
        for c in settle.columns
        if c.startswith("fee_") and c.endswith("_minor") and c not in BASE_FEE_COLS
    )
    fee_cols = [*BASE_FEE_COLS, *extra]
    settle[fee_cols] = settle[fee_cols].fillna(0).astype("int64")
    settle["fee_total_minor"] = settle[fee_cols].sum(axis=1)
    return settle, fee_cols


def dedupe_settlement(settle: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """At most one settlement row per payment: keep the earliest (the sale),
    count the rest in n_settlement_txns, and report the payments affected
    (refund/chargebacks — see module docstring for why this warns, not fails).
    """
    settle = settle.sort_values("created_at", kind="stable").copy()
    settle["n_settlement_txns"] = settle.groupby("parent_payment_id")[
        "id"
    ].transform("size")
    multi = settle.loc[
        settle["n_settlement_txns"] > 1, "parent_payment_id"
    ].unique()
    deduped = settle.drop_duplicates("parent_payment_id", keep="first")
    warnings = (
        [
            f"{len(multi)} payments carry >1 settlement transaction "
            f"(kept earliest): {sorted(multi)[:5]}"
        ]
        if len(multi)
        else []
    )
    return deduped, warnings


def run_checks(payments: pd.DataFrame, settle: pd.DataFrame) -> list[str]:
    """Report-blocking checks (nb 014/021 lineage) — non-empty return means
    nothing may be exported.
    """
    failures: list[str] = []
    if payments["id"].duplicated().any():
        failures.append("duplicate payment ids after dedup")

    # Tenant guard: the window's merchant must be the payment's merchant.
    guard = settle.merge(
        payments[["id", "merchant_id"]],
        left_on="parent_payment_id",
        right_on="id",
        how="left",
        suffixes=("", "_pay"),
    )
    mismatch = guard["window_merchant_id"].notna() & (
        guard["window_merchant_id"] != guard["merchant_id"]
    )
    if mismatch.any():
        failures.append(
            f"{int(mismatch.sum())} settlement txns whose window merchant "
            "!= payment merchant"
        )

    # Row tie-out, blocking only once the window is terminal (nb 021 stance).
    tie_bad = (settle["amount"] - settle["fee_total_minor"]) != settle[
        "settled_amount"
    ]
    terminal = settle["window_status"].isin(TERMINAL_WINDOW_STATUSES)
    if (tie_bad & terminal).any():
        failures.append(
            f"{int((tie_bad & terminal).sum())} terminal settlement txns "
            "failing amount - fees == settled"
        )
    return failures


def build_report(payments: pd.DataFrame, settle: pd.DataFrame) -> pd.DataFrame:
    """Pure: payments spine + deduped settlement rows -> the export frame.
    Payments without a settlement txn keep NaN in the settlement columns
    ("no settlement" is distinguishable from "zero fee").
    """
    payments = payments.copy()
    payments["payment_creation_date"] = (
        payments["created_at"].dt.tz_convert(LOCAL_TIMEZONE).dt.date
    )
    payments["created_at_local"] = (
        payments["created_at"].dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None)
    )
    # SAR exponent 2 — same minor->major divisor as the payments mart.
    payments["amount_sar"] = payments["amount"].astype("float") / 100
    payments["order_reference"] = payments["order_data"].map(
        lambda s: json.loads(s).get("reference") if isinstance(s, str) else None
    )

    fee_cols = [
        c
        for c in settle.columns
        if c.startswith("fee_")
        and c.endswith("_minor")
        and c != "fee_total_minor"
    ]
    extra_fee_cols = [c for c in fee_cols if c not in BASE_FEE_COLS]
    join = settle.rename(
        columns={
            "id": "settlement_transaction_id",
            "status": "settlement_status",
            "settled_amount": "settled_amount_minor",
        }
    )[["parent_payment_id", *SETTLE_EXPORT_COLS, *extra_fee_cols]]
    report = payments.merge(
        join,
        left_on="id",
        right_on="parent_payment_id",
        how="left",
        validate="1:1",
    ).drop(columns="parent_payment_id")

    for col in SETTLE_EXPORT_COLS:
        if col not in report.columns:
            report[col] = pd.NA
    report["settled_amount_sar"] = report["settled_amount_minor"].astype("float") / 100
    for c in [*fee_cols, "fee_total_minor"]:
        report[c.removesuffix("_minor") + "_sar"] = report[c].astype("float") / 100
    return report


def write_daily_files(
    report: pd.DataFrame,
    report_day: date,
    run_date: date,
    out_dir: Path,
    merchant_ids: list[str] | None = None,
) -> list[tuple[str, Path, int]]:
    """One CSV per merchant — header-only when the merchant had no payments
    (every in-scope merchant gets a file, that's the delivery contract).
    Columns all-NaN for the merchant that day are dropped; an empty frame
    keeps the full schema (a 0-row dropna would drop every column). The
    filename pairs report day with run date — see module docstring.
    """
    written: list[tuple[str, Path, int]] = []
    for mid in merchant_ids or MERCHANT_IDS:
        m_rows = report[report["merchant_id"] == mid]
        if len(m_rows):
            m_rows = m_rows.dropna(axis=1, how="all")
        m_dir = out_dir / mid
        m_dir.mkdir(parents=True, exist_ok=True)
        path = m_dir / f"transactions_{report_day}_run_{run_date}.csv"
        m_rows.sort_values("created_at").to_csv(
            path, index=False, float_format="%.2f"
        )
        written.append((mid, path, len(m_rows)))
    return written


def upload(
    files: list[tuple[str, Path, int]],
    bucket_name: str,
    project: str,
) -> list[str]:
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    uris: list[str] = []
    for mid, path, _ in files:
        # Blob name reuses the local filename so the two can never drift.
        blob_name = f"{GCS_PREFIX}/{mid}/{REPORT_TYPE}/{path.name}"
        bucket.blob(blob_name).upload_from_filename(path)
        uris.append(f"gs://{bucket_name}/{blob_name}")
    return uris


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="report day (Riyadh calendar); default: yesterday",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="write local files, skip the GCS upload (no bucket needed)",
    )
    args = parser.parse_args(argv)

    run_date = datetime.now(ZoneInfo(LOCAL_TIMEZONE)).date()
    report_day = args.date or run_date - timedelta(days=1)
    settings = Settings.from_env()
    # Explicit location, as everywhere: BigQuery defaults to the US
    # multi-region, which the org residency policy rejects.
    client = bigquery.Client(project=settings.gcp_project, location="me-central2")
    raw = f"{settings.gcp_project}.{settings.bq_dataset_raw}"

    payments = fetch_payments(client, raw, report_day)
    settle = (
        fetch_settlement(client, raw, payments["id"].tolist())
        if len(payments)
        else _empty_settlement()
    )
    settle, _ = pivot_fees(settle)
    settle, warnings = dedupe_settlement(settle)
    for warning in warnings:
        print(f"WARNING: {warning}")

    failures = run_checks(payments, settle)
    if failures:
        raise SystemExit(
            "report-blocking check failures:\n- " + "\n- ".join(failures)
        )

    report = build_report(payments, settle)
    out_dir = Path(tempfile.mkdtemp(prefix="merchant-daily-"))
    files = write_daily_files(report, report_day, run_date, out_dir)
    for mid, path, n_rows in files:
        print(f"{mid}: {n_rows} payments -> {path}")

    if args.dry_run:
        print(f"dry run — nothing uploaded, files under {out_dir}")
        return
    if not settings.gcs_bucket_egress:
        raise SystemExit("GCS_BUCKET_EGRESS is not set (required unless --dry-run)")
    for uri in upload(files, settings.gcs_bucket_egress, settings.gcp_project):
        print(f"uploaded {uri}")


if __name__ == "__main__":
    main()
