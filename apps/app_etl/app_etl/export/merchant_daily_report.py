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
        <merchant_slug>_daily_transaction_report_<report_day>_run_<run_date>.csv

The filename carries the run date (Riyadh) next to the report day so a
backfill is traceable: re-running an old day writes a NEW file beside the
original instead of silently replacing it, and the newest run date is the
authoritative version of that report day. Same-day reruns overwrite —
idempotent by design.

Refund / multi-window posture (v1): the file keeps payment grain. If a
payment ever carries more than one settlement transaction (refund or
chargeback settling in a second window), the row keeps the EARLIEST
transaction — the sale, whose fees stay correct — and the run logs a
warning instead of failing: a refund is an event of its own day and
belongs in a report type of its own, never a mutation of an
already-delivered daily file. The notebook keeps the blocking version of
this check so the first refund on prod gets a person's eyes before that
report type exists (the finance report now carries a reversals/refunds
file; the merchant-facing one is still pending layout sign-off).

Column layout (ops sample sign-off 2026-08-27, supersedes the 2026-08-26
finance keep-list): EXPORT_COLUMNS mirrors the sample report operations
shared, column for column, in order. Relative to the 08-26 list that
means: the source-blob block is gone (the trim flagged then is now
taken), minor-unit and internal columns (n_settlement_txns,
payment_method, processing_type, order_id, link/actor ids) are gone,
payment_creation_date is back, and four enrichments joined in —
channel_name (business_management channels on payments.channel_id),
last_four / card_brand (instrument_data JSON), and RRN / STAN / TID
(parsed from the sale op's idempotency_key, see pos_identifiers). The
keep-list rationale is unchanged: a drop-list would leak the next
upstream ADD COLUMN straight into merchant-facing files under the
ingest-everything posture. Genuinely new fee types still appear
dynamically (fee_<type>_sar). The header is fixed (2026-08-27, supersedes
the all-NaN column drop): every file carries the full keep-list schema
every day, empty columns included, so the delivered header never varies
between days or merchants.
"""

from __future__ import annotations

import json
import re
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from app_etl.config import Settings
from app_etl.export.common import (
    EXCLUDED_PAYMENT_IDS,
    LOCAL_TIMEZONE,
    latest_version,
    make_arg_parser,
    merchant_directory,
    resolve_days,
    upload_files,
)

__all__ = ["EXCLUDED_PAYMENT_IDS", "LOCAL_TIMEZONE"]  # re-exported from common

# The merchants in scope, id -> display name (the name feeds the report
# filename slug, so a rename here changes the filename, deliberately —
# and a KYB rename should be reflected here). The original two (CEO ask,
# 2026-08-20 — same mids as notebooks/020/021) plus the 2026-08-24
# KYB-approved batch. Widening to all merchants is a deliberate decision
# (refund posture, file-count, layout sign-off), not a config default.
MERCHANTS = {
    "6b58b5d3-d381-4bf0-bce3-812c812e11d5": "fine table Company",
    "afc004ce-20a0-4dd0-86c3-e9de6324590b": "altawsil alashhal Company Ltd.",
    "e70d441e-678d-477c-840d-7eb486981369": "bayt altawabil Company For Trading",
    "ae53c269-6258-4b96-8cbe-1f1c4fa87b13": "alruyat almutamayiza Company For Meals",
    "4da9902e-baed-4eff-9743-d9cef5d29442": "alquwa almuttalaqa Center Sport",
    "1dd2f1eb-e0ff-41f2-b56b-48b7434e07f2": "J hub",
    "65de1bf8-20c2-4034-a42f-ef3ca04fb34d": "Alatima Alraeia Company Ltd.",
    "22efe224-41da-4136-8cbb-39504ee4693c" :"alakhwa almutamayiza Company Ltd.",
}
MERCHANT_IDS = list(MERCHANTS)

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
# keep the same header as busy ones. Minor-unit columns stay internal
# (run_checks ties out on them); only the keep-list ships.
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

# The fixed keep-list: the ops sample layout (2026-08-27), column for
# column, in order. RRN/STAN/TID are upper-case because the delivered
# header is the contract. See the module docstring for what changed
# relative to the 08-26 finance list and why a keep-list at all.
EXPORT_COLUMNS = [
    "merchant_id",
    "merchant_name",
    "id",
    "status",
    "currency",
    "amount_sar",
    "fee_flat_sar",
    "fee_mdr_sar",
    "fee_vat_sar",
    "fee_total_sar",
    "capture_mode",
    "RRN",
    "STAN",
    "TID",
    "payment_instrument_id",
    "channel_id",
    "channel_name",
    "last_four",
    "card_brand",
    "channel_type",
    "payment_creation_date",
    "created_at_local",
    "order_reference",
    "settlement_transaction_id",
    "settlement_status",
    "is_settled",
    "settlement_window_id",
    "window_status",
    "value_day",
    "settled_amount_sar",
]


def export_columns(report: pd.DataFrame) -> list[str]:
    """The keep-list plus any genuinely new fee-type columns the day's
    fees JSON produced (the one sanctioned way the header grows) — their
    major-unit versions only, appended after the fixed layout."""
    extra_fees = sorted(
        c
        for c in report.columns
        if c.startswith("fee_") and c.endswith("_sar") and c not in EXPORT_COLUMNS
    )
    return [*EXPORT_COLUMNS, *extra_fees]


def merchant_slug(name: str, merchant_id: str) -> str:
    """Filename-safe slug of the display name; falls back to the id
    prefix when nothing survives (e.g. a fully non-Latin name)."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug or merchant_id[:8]


def fetch_payments(
    client: bigquery.Client, raw: str, report_day: date
) -> pd.DataFrame:
    """The day's payments for the in-scope merchants, latest version per id
    (the dedup a stg_ model would own), with the merchant's display name
    and the channel's display name (channels.id = payments.channel_id).
    channel_merchant_id is the channel's owning business mapped back to
    the natural merchant key — run_checks' cross-tenant guard, never
    exported. Incident-excluded payments (EXCLUDED_PAYMENT_IDS) never
    enter the spine.
    """
    query = f"""
        select
            p.* except (_dlt_load_id, _dlt_id),
            b.name as merchant_name,
            c.name as channel_name,
            cb.business_id as channel_merchant_id
        from {latest_version(f"{raw}.payment_v2__payments")} p
        left join {merchant_directory(raw)} b
            on b.business_id = p.merchant_id
        left join {latest_version(f"{raw}.business_management__channels")} c
            on c.id = p.channel_id
        left join {latest_version(f"{raw}.business_management__business_entities")} cb
            on cb.id = c.business_id
        where p.merchant_id in unnest(@merchant_ids)
          and p.id not in unnest(@excluded_payment_ids)
          and date(datetime(p.created_at, @tz)) = @report_day
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("merchant_ids", "STRING", MERCHANT_IDS),
            bigquery.ArrayQueryParameter(
                "excluded_payment_ids", "STRING", EXCLUDED_PAYMENT_IDS
            ),
            bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
            bigquery.ScalarQueryParameter("report_day", "DATE", report_day),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def fetch_operations(
    client: bigquery.Client, raw: str, payment_ids: list[str]
) -> pd.DataFrame:
    """The day's payments' operations, latest version per op — just the
    columns pos_identifiers() parses. The receipt identifiers live in
    idempotency_key (docs/payment-milestones-by-entry-mode.md §4); parsing
    stays in pandas so it's unit-testable.
    """
    query = f"""
        select o.payment_id, o.idempotency_key, o.created_at
        from {latest_version(f"{raw}.payment_v2__payment_operations")} o
        where o.payment_id in unnest(@payment_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("payment_ids", "STRING", payment_ids),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def _empty_operations() -> pd.DataFrame:
    """Schema stand-in when the day has no payments (no ops query)."""
    return pd.DataFrame(columns=["payment_id", "idempotency_key", "created_at"])


def pos_identifiers(ops: pd.DataFrame) -> pd.DataFrame:
    """One row per payment: RRN / STAN / TID from the earliest sale-shaped
    idempotency_key. Sale receipts (SDK/webhook/reconcile paths) write
    '<rrn>;<stan>;<tid>'; immediate reversals use ':' and the init /
    device-error rows a random UUID — none of those are the sale, so only
    ';'-delimited keys qualify, and the earliest one is the sale even when
    a later refund receipt carries its own rrn. Ecom payments have no such
    op and simply stay absent (NaN after the join).
    """
    keys = ops["idempotency_key"].astype("string")
    sale = ops.loc[keys.str.count(";").eq(2).fillna(False)]
    sale = sale.sort_values("created_at", kind="stable").drop_duplicates("payment_id")
    parts = sale["idempotency_key"].astype("string").str.split(";")
    return pd.DataFrame(
        {
            "payment_id": sale["payment_id"],
            "RRN": parts.str[0],
            "STAN": parts.str[1],
            "TID": parts.str[2],
        }
    )


def fetch_settlement(
    client: bigquery.Client, raw: str, payment_ids: list[str]
) -> pd.DataFrame:
    """Settlement transactions for the day's payments, window context joined
    in (all window statuses — on a payment row this is "where does
    settlement stand", not a payout statement). REMOVED transactions are
    excluded here, after the latest-version dedup, so a txn whose latest
    version is REMOVED disappears instead of resurfacing an older version;
    its payment then reports "no settlement" (NaN columns).
    """
    query = f"""
        select
            t.* except (_dlt_load_id, _dlt_id),
            w.merchant_id as window_merchant_id,
            w.status as window_status,
            format_date('%F', date(datetime(w.value_date, @tz))) as value_day
        from {latest_version(f"{raw}.settlement__transaction")} t
        left join {latest_version(f"{raw}.settlement__settlement_window")} w
            on w.id = t.settlement_window_id
        where t.parent_payment_id in unnest(@payment_ids)
          -- `is distinct from`, not `!=`: a NULL status must stay in scope.
          and t.status is distinct from 'REMOVED'
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

    # Tenant guard: a resolved channel must belong to the payment's
    # merchant — a mismatch would put another business's channel name in
    # a merchant-facing file. (Verified clean on prod 2026-08-27; an
    # unresolvable channel is NaN here and just ships a blank name.)
    chan_mismatch = payments["channel_merchant_id"].notna() & (
        payments["channel_merchant_id"] != payments["merchant_id"]
    )
    if chan_mismatch.any():
        failures.append(
            f"{int(chan_mismatch.sum())} payments whose channel belongs "
            "to another merchant"
        )

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


def build_report(
    payments: pd.DataFrame, settle: pd.DataFrame, pos_ids: pd.DataFrame
) -> pd.DataFrame:
    """Pure: payments spine + deduped settlement rows + per-payment POS
    identifiers -> the export frame. Payments without a settlement txn
    keep NaN in the settlement columns ("no settlement" is distinguishable
    from "zero fee"); payments without a sale receipt (ecom, device
    errors) keep NaN RRN/STAN/TID.
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
    # Card facts from the instrument_data JSON — same extraction as
    # stg_litecore__payments, sample-report column names.
    instrument = payments["instrument_data"].map(
        lambda s: json.loads(s) if isinstance(s, str) else {}
    )
    payments["last_four"] = instrument.map(lambda d: d.get("last_four"))
    payments["card_brand"] = instrument.map(lambda d: d.get("card_brand"))

    payments = payments.merge(
        pos_ids, left_on="id", right_on="payment_id", how="left", validate="1:1"
    ).drop(columns="payment_id")

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
    merchants: dict[str, str] | None = None,
) -> list[tuple[str, Path, int]]:
    """One CSV per merchant — header-only when the merchant had no payments
    (every in-scope merchant gets a file, that's the delivery contract).
    Every file carries the full keep-list schema, empty columns included —
    the header is fixed. The filename carries the merchant-name slug
    (finance ask 2026-08-26) and pairs report day with run date — see
    module docstring; the bucket path keeps the merchant_id, so uniqueness
    and the §J5 path contract never depend on display names.
    """
    cols = export_columns(report)
    written: list[tuple[str, Path, int]] = []
    for mid, name in (merchants or MERCHANTS).items():
        m_rows = report[report["merchant_id"] == mid]
        m_rows = m_rows.sort_values("created_at").reindex(columns=cols)
        m_dir = out_dir / mid
        m_dir.mkdir(parents=True, exist_ok=True)
        filename = (
            f"{merchant_slug(name, mid)}_daily_transaction_report_"
            f"{report_day}_run_{run_date}.csv"
        )
        m_rows.to_csv(m_dir / filename, index=False, float_format="%.2f")
        written.append((mid, m_dir / filename, len(m_rows)))
    return written


def main(argv: list[str] | None = None) -> None:
    args = make_arg_parser(__doc__).parse_args(argv)
    report_day, run_date = resolve_days(args)
    settings = Settings.from_env()
    # Explicit location, as everywhere: BigQuery defaults to the US
    # multi-region, which the org residency policy rejects.
    client = bigquery.Client(project=settings.gcp_project, location="me-central2")
    raw = f"{settings.gcp_project}.{settings.bq_dataset_raw}"

    payments = fetch_payments(client, raw, report_day)
    payment_ids = payments["id"].tolist()
    settle = (
        fetch_settlement(client, raw, payment_ids)
        if len(payments)
        else _empty_settlement()
    )
    ops = (
        fetch_operations(client, raw, payment_ids)
        if len(payments)
        else _empty_operations()
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

    report = build_report(payments, settle, pos_identifiers(ops))
    out_dir = Path(tempfile.mkdtemp(prefix="merchant-daily-"))
    files = write_daily_files(report, report_day, run_date, out_dir)
    for mid, path, n_rows in files:
        print(f"{mid}: {n_rows} payments -> {path}")

    if args.dry_run:
        print(f"dry run — nothing uploaded, files under {out_dir}")
        return
    if not settings.gcs_bucket_egress:
        raise SystemExit("GCS_BUCKET_EGRESS is not set (required unless --dry-run)")
    # Blob name reuses the local filename so the two can never drift.
    pairs = [
        (f"{GCS_PREFIX}/{mid}/{REPORT_TYPE}/{path.name}", path)
        for mid, path, _ in files
    ]
    for uri in upload_files(pairs, settings.gcs_bucket_egress, settings.gcp_project):
        print(f"uploaded {uri}")


if __name__ == "__main__":
    main()
