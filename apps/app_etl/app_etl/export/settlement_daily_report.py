"""Daily per-merchant transaction report, settlement-ledger spine (v2).

Ports `notebooks/025-settlement-daily-report-v2.ipynb` (DAT-27, review of
2026-08-30; design summary in docs/settlement-daily-report-v2-design.md).
Same delivered layout as `merchant_daily_report` (the ops-signed 08-27
sample, reused verbatim), different spine: instead of "payments created on
the report day", the file for day D carries the settlement transactions of
every window whose collection day is D. A refund books a new negative row
on the day it happens, so old-payment refunds appear as ordinary rows of
their own day; the payments-created spine cannot see them.

Grain: one row per INCLUDED settlement transaction of the day's windows,
plus one payment-grain row for each of the day's payments with no such
transaction (declines and failures, sales not yet booked, voided sales).
Payment operations are never rows here — they resolve enrichment
(RRN / STAN / TID) and feed the gate. A payment appears twice only when a
refund books: the sale row plus the negative refund row.

Side-by-side phase: this module lives NEXT TO merchant_daily_report until
the outputs are validated against each other and product signs off on the
spine change. Since the 2026-08-30 consolidation it is self-contained: the
merchant list, column layout, payment-grain build and file writer are
duplicated VERBATIM from the old module instead of imported, so the two
exporters share no code and the cutover is a file swap, not an
untangling. Until cutover, a layout change must land in BOTH files (the
comparison harness catches drift). It REFUSES a real upload (dry-run
only) because the filenames match the old report's and a non-dry-run
would overwrite delivered files in the bucket. At cutover this module
replaces merchant_daily_report and the guard goes.

Union spine, for the 1:1 acceptance criterion: the pure settlement spine
cannot see payments that never reached settlement (declined / failed
payments, sales not yet booked, same-day voids), which the delivered file
carries today with empty settlement columns. Removing rows from a
delivered report is a contract change nobody has ruled on, so v2 appends
those payments as payment-grain leftover rows through a verbatim copy of
the old module's build path (`build_payment_rows`). On a day with no
refunds, reversals or day-boundary rows the two exporters are
byte-identical (verified on prod 2026-08-29 and 08-28: 8/8 files). The
deltas that remain are the designed ones
(docs/settlement-daily-report-v2-design.md case matrix):

- A refund is its own extra row in its own day's file, negative,
  referencing the original payment (the old spine deduped it away).
- A transaction whose window collects on a different Riyadh day than the
  payment's creation moment shows settled on the window's day; the
  creation day keeps the payment-grain row with empty settlement
  columns (the old spine showed it settled on the creation day).
- A same-day voided sale (row flipped to REMOVED) ships as a
  payment-grain row with empty settlement columns, exactly like the old
  spine after its REMOVED filter.

TODO(finops sign-off): add `transaction_type` (SALE / REFUND) and the raw
settlement `operation` (pay | capture | authorize | refund) to the layout
as PROPOSED_ADDITIONS once finops signs off (nb 025 review 2026-08-30).
`operation` already travels the spine for the operation/sign gate check;
only the delivered header waits.
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
)

# ---------------------------------------------------------------------------
# Contract constants — duplicated verbatim from merchant_daily_report
# (2026-08-30 consolidation). A change here must land there too until
# cutover deletes the old module.
# ---------------------------------------------------------------------------

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
    "22efe224-41da-4136-8cbb-39504ee4693c": "alakhwa almutamayiza Company Ltd.",
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

# Settlement columns the join contributes (post-rename) on the
# payment-grain leftover path. Ensured to exist even when there are no
# settlement rows at all, so empty and quiet days keep the same header as
# busy ones. Minor-unit columns stay internal; only the keep-list ships.
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
# header is the contract.
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


# ---------------------------------------------------------------------------
# Extraction — the settlement spine and its gate inputs
# ---------------------------------------------------------------------------


def fetch_window_transactions(
    client: bigquery.Client,
    raw: str,
    report_day: date,
    end_day: date | None = None,
    all_merchants: bool = False,
) -> pd.DataFrame:
    """The spine: EVERY settlement transaction (latest version) in the
    in-scope merchants' windows collecting on the report day, REMOVED rows
    included and flagged `included_in_window` (a reversal adds no row, it
    flips the sale's row in place; filtering REMOVED in SQL would hide it
    from the checks). Enrichment per the verified contract:
    `external_reference_id` -> the operation, `parent_payment_id` -> the
    payment (for a refund row: the ORIGINAL payment, deliberately),
    channels -> display name, merchant_directory -> merchant name.
    Incident-excluded payments stay in the spine so the per-window
    tie-out sees complete windows; build_report drops them from the file.

    `end_day` widens the extraction to the closed collection-day range
    [report_day, end_day] for one-off range reports; the daily job never
    passes it. `all_merchants=True` lifts the MERCHANTS allowlist to the
    whole platform, test merchants included.
    """
    query = f"""
        select
            t.id as settlement_transaction_id,
            t.parent_payment_id as payment_id,
            t.external_reference_id as operation_id,
            t.operation,
            t.status as settlement_status,
            t.status is distinct from 'REMOVED' as included_in_window,
            t.is_settled,
            t.amount as amount_minor,
            t.settled_amount as settled_amount_minor,
            t.fees,
            t.currency,
            t.hold_at,
            t.created_at as txn_created_at,
            w.id as settlement_window_id,
            w.merchant_id,
            w.status as window_status,
            w.settled_amount as window_settled_amount_minor,
            w.currency as window_currency,
            format_date('%F', date(datetime(w.value_date, @tz))) as value_day,
            date(datetime(t.created_at, @tz))
                <> coalesce(w.collection_date, date(datetime(w.created_at, @tz)))
                as moved_across_days,
            b.name as merchant_name,
            o.id is not null as op_resolved,
            p.id is not null as payment_resolved,
            p.merchant_id as payment_merchant_id,
            p.status as payment_status,
            p.currency as payment_currency,
            p.capture_mode,
            p.payment_instrument_id,
            p.instrument_data,
            p.channel_id,
            p.channel_type,
            p.order_data,
            p.created_at,
            c.name as channel_name,
            cb.business_id as channel_merchant_id
        from {latest_version(f"{raw}.settlement__transaction")} t
        join {latest_version(f"{raw}.settlement__settlement_window")} w
            on w.id = t.settlement_window_id
        left join {merchant_directory(raw)} b
            on b.business_id = w.merchant_id
        left join {latest_version(f"{raw}.payment_v2__payment_operations")} o
            on o.id = t.external_reference_id
        left join {latest_version(f"{raw}.payment_v2__payments")} p
            on p.id = t.parent_payment_id
        left join {latest_version(f"{raw}.business_management__channels")} c
            on c.id = p.channel_id
        left join {latest_version(f"{raw}.business_management__business_entities")} cb
            on cb.id = c.business_id
        where coalesce(w.collection_date, date(datetime(w.created_at, @tz)))
              between @report_day and @end_day
          and (@all_merchants or w.merchant_id in unnest(@merchant_ids))
        order by t.created_at
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("merchant_ids", "STRING", MERCHANT_IDS),
            bigquery.ScalarQueryParameter("all_merchants", "BOOL", all_merchants),
            bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
            bigquery.ScalarQueryParameter("report_day", "DATE", report_day),
            bigquery.ScalarQueryParameter("end_day", "DATE", end_day or report_day),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def fetch_payments(
    client: bigquery.Client,
    raw: str,
    report_day: date,
    end_day: date | None = None,
    all_merchants: bool = False,
) -> pd.DataFrame:
    """The day's payments for the in-scope merchants, latest version per id
    (the dedup a stg_ model would own), with the merchant's display name
    and the channel's display name (channels.id = payments.channel_id).
    channel_merchant_id is the channel's owning business mapped back to
    the natural merchant key — the cross-tenant guard, never exported.
    Incident-excluded payments (EXCLUDED_PAYMENT_IDS) never enter this
    frame. `end_day` widens to a creation-day range for one-off reports;
    `all_merchants=True` lifts the allowlist, test merchants included.
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
        where (@all_merchants or p.merchant_id in unnest(@merchant_ids))
          and p.id not in unnest(@excluded_payment_ids)
          and date(datetime(p.created_at, @tz)) between @report_day and @end_day
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("merchant_ids", "STRING", MERCHANT_IDS),
            bigquery.ScalarQueryParameter("all_merchants", "BOOL", all_merchants),
            bigquery.ArrayQueryParameter(
                "excluded_payment_ids", "STRING", EXCLUDED_PAYMENT_IDS
            ),
            bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
            bigquery.ScalarQueryParameter("report_day", "DATE", report_day),
            bigquery.ScalarQueryParameter("end_day", "DATE", end_day or report_day),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def fetch_operations(
    client: bigquery.Client, raw: str, payment_ids: list[str]
) -> pd.DataFrame:
    """The report's payments' operations, latest version per op — just the
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


def fetch_refund_reverse_ops(
    client: bigquery.Client,
    raw: str,
    report_day: date,
    end_day: date | None = None,
    all_merchants: bool = False,
) -> pd.DataFrame:
    """Successful REFUND / REVERSE ops for in-scope merchants whose event
    day (op updated_at, Riyadh) is the report day, each flagged with
    whether ANY settlement transaction references it. Feeds the refund
    tripwire (booking is unreliable upstream: 9 of 15 successful refunds
    to date never produced a row) and the REVERSE notice. `end_day`
    widens to an event-day range for one-off reports; `all_merchants=True`
    lifts the allowlist, test merchants included.
    """
    query = f"""
        select o.operation_type, o.id as op_id, o.payment_id,
            t.id is not null as has_settlement_row
        from {latest_version(f"{raw}.payment_v2__payment_operations")} o
        join {latest_version(f"{raw}.payment_v2__payments")} p
            on p.id = o.payment_id
        left join {latest_version(f"{raw}.settlement__transaction")} t
            on t.external_reference_id = o.id
        where o.operation_type in ('REFUND', 'REVERSE')
          and o.status = 'SUCCESS'
          and (@all_merchants or p.merchant_id in unnest(@merchant_ids))
          and date(datetime(o.updated_at, @tz)) between @report_day and @end_day
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("merchant_ids", "STRING", MERCHANT_IDS),
            bigquery.ScalarQueryParameter("all_merchants", "BOOL", all_merchants),
            bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
            bigquery.ScalarQueryParameter("report_day", "DATE", report_day),
            bigquery.ScalarQueryParameter("end_day", "DATE", end_day or report_day),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


# ---------------------------------------------------------------------------
# Shared transforms (verbatim from merchant_daily_report)
# ---------------------------------------------------------------------------


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


def _empty_settlement() -> pd.DataFrame:
    """Schema stand-in feeding the payment-grain leftover build, which by
    design never sees a settlement row (a leftover payment is exactly one
    the spine does not cover)."""
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
    Keyed on `id` (the settlement transaction id in the source frame).
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


def pivot_fees_txn(spine: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """pivot_fees keyed on the settlement transaction id instead of a
    frame whose txn id column is `id` (here `id` is reserved for the
    payment, per the delivered layout)."""
    renamed = spine.rename(columns={"settlement_transaction_id": "id"})
    pivoted, fee_cols = pivot_fees(renamed)
    return pivoted.rename(columns={"id": "settlement_transaction_id"}), fee_cols


def dedupe_settlement(settle: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """At most one settlement row per payment: keep the earliest (the sale),
    count the rest in n_settlement_txns, report the payments affected.
    Only the empty stand-in passes through here in this module — kept
    verbatim so the leftover frame's schema matches the old exporter's.
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


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


def run_checks(spine: pd.DataFrame, day_ops: pd.DataFrame) -> list[str]:
    """Report-blocking checks (nb 025 gate) — non-empty return means
    nothing may be exported. Runs on the FULL spine (REMOVED rows and
    incident-excluded payments included) so the per-window tie-out sums
    complete windows.
    """
    failures: list[str] = []
    included = spine["included_in_window"].fillna(False).astype(bool)

    if spine["settlement_transaction_id"].duplicated().any():
        failures.append("duplicate settlement transaction ids after dedup")

    # Join contract: every row must resolve its operation and payment;
    # an unresolved row is money we cannot attribute or describe.
    if len(spine) and not spine["op_resolved"].all():
        failures.append(
            f"{int((~spine['op_resolved']).sum())} rows with no "
            "payment_operation for external_reference_id"
        )
    if len(spine) and not spine["payment_resolved"].all():
        failures.append(
            f"{int((~spine['payment_resolved']).sum())} rows with no "
            "payment for parent_payment_id"
        )

    # Tenant guards: the window's merchant must be the payment's, and a
    # resolved channel must belong to the payment's merchant.
    mismatch = spine["payment_resolved"] & (
        spine["merchant_id"] != spine["payment_merchant_id"]
    )
    if mismatch.any():
        failures.append(
            f"{int(mismatch.sum())} rows whose window merchant "
            "!= payment merchant"
        )
    chan_mismatch = spine["channel_merchant_id"].notna() & (
        spine["channel_merchant_id"] != spine["payment_merchant_id"]
    )
    if chan_mismatch.any():
        failures.append(
            f"{int(chan_mismatch.sum())} rows whose channel belongs "
            "to another merchant"
        )

    # Single-currency guard: the /100 minor->major conversion assumes SAR.
    bad_ccy = (
        set(spine["currency"].dropna())
        | set(spine["window_currency"].dropna())
        | set(spine["payment_currency"].dropna())
    ) - {"SAR"}
    if bad_ccy:
        failures.append(f"non-SAR currency in scope: {sorted(bad_ccy)}")

    # Row tie-out on included rows (verified on all non-REMOVED rows
    # 2026-08-26, hence the scope), sign-correct for refunds.
    tie_bad = included & (
        spine["amount_minor"] - spine["fee_total_minor"]
        != spine["settled_amount_minor"]
    )
    if tie_bad.any():
        failures.append(
            f"{int(tie_bad.sum())} included rows failing "
            "amount - fees == settled_amount"
        )

    # Refund shape: every negative row was born HELD (hold_at set).
    neg_no_hold = included & (spine["amount_minor"] < 0) & spine["hold_at"].isna()
    if neg_no_hold.any():
        failures.append(f"{int(neg_no_hold.sum())} negative rows without hold_at")

    # Operation/sign cross-check (operation exists since settlement
    # migration 0009; NULL on older rows, never checked).
    op_sign_bad = (
        included
        & spine["operation"].notna()
        & (
            spine["operation"].str.lower().eq("refund")
            != spine["amount_minor"].lt(0)
        )
    )
    if op_sign_bad.any():
        failures.append(
            f"{int(op_sign_bad.sum())} included rows where operation and "
            "amount sign disagree"
        )

    # Per-window tie-out: included rows must sum to the window's own
    # settled_amount (reassignment adjusts both windows in one service
    # transaction, so this holds mid-flight too).
    win_sums = (
        spine.assign(
            included_settled_minor=spine["settled_amount_minor"].where(included, 0)
        )
        .groupby("settlement_window_id")
        .agg(
            window_settled_amount_minor=("window_settled_amount_minor", "first"),
            included_settled_minor=("included_settled_minor", "sum"),
        )
    )
    win_bad = win_sums.loc[
        win_sums["window_settled_amount_minor"].isna()
        | win_sums["window_settled_amount_minor"].ne(
            win_sums["included_settled_minor"]
        )
    ]
    if len(win_bad):
        failures.append(
            f"{len(win_bad)} window(s) failing sum(included settled_amount) "
            f"== window.settled_amount: {win_bad.index.tolist()[:5]}"
        )

    # Refund tripwire: a successful refund with no settlement row means
    # the file would omit money movement — refuse loudly.
    unbooked = day_ops.query("operation_type == 'REFUND' and not has_settlement_row")
    if len(unbooked):
        failures.append(
            f"{len(unbooked)} successful REFUND op(s) with NO settlement "
            f"row: {unbooked['op_id'].tolist()[:5]}"
        )
    return failures


def run_payment_checks(payments: pd.DataFrame, settle: pd.DataFrame) -> list[str]:
    """Report-blocking checks on the payment-grain leftover path (verbatim
    merchant_daily_report.run_checks, nb 014/021 lineage) — non-empty
    return means nothing may be exported.
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


def informational_notes(spine: pd.DataFrame, day_ops: pd.DataFrame) -> list[str]:
    """Non-blocking observations, printed after the gate passes."""
    notes: list[str] = []
    included = spine["included_in_window"].fillna(False).astype(bool)
    open_windows = spine.loc[
        ~spine["window_status"].isin(TERMINAL_WINDOW_STATUSES),
        "settlement_window_id",
    ].nunique()
    if open_windows:
        notes.append(
            f"{open_windows} window(s) not yet terminal; statuses are "
            "as-of-run-time, the population is final"
        )
    n_removed = int((~included).sum())
    if n_removed:
        notes.append(
            f"{n_removed} REMOVED row(s) in the day's windows (reversed "
            "sales or failed refunds); in the spine, never in the file"
        )
    held = int((spine["settlement_status"] == "HELD").sum())
    if held:
        notes.append(
            f"{held} HELD row(s): refund holds, pending; REMOVED if the "
            "refund later fails"
        )
    moved = int(spine["moved_across_days"].fillna(False).sum())
    if moved:
        notes.append(
            f"{moved} row(s) whose transaction day differs from the window "
            "collection day (expected near midnight)"
        )
    null_operation = int(spine["operation"].isna().sum())
    if null_operation:
        notes.append(
            f"{null_operation} row(s) with NULL operation (pre-0009 rows)"
        )
    n_reverse = int((day_ops["operation_type"] == "REVERSE").sum())
    if n_reverse:
        notes.append(
            f"{n_reverse} successful REVERSE op(s) on the report day; "
            "visible only as the sale's REMOVED flip on its original day, "
            "never in this day's file"
        )
    return notes


# ---------------------------------------------------------------------------
# Build — transaction-grain spine rows + payment-grain leftovers
# ---------------------------------------------------------------------------


def build_report(
    spine: pd.DataFrame, pos_ids: pd.DataFrame, fee_cols: list[str]
) -> pd.DataFrame:
    """Pure: full spine -> the export frame in the delivered layout.
    Drops REMOVED rows and incident-excluded payments (the checks
    already ran on the full spine). Payment columns (`id`, `status`,
    `payment_creation_date`, `created_at_local`) describe the row's
    payment; `amount_sar` is the TRANSACTION's amount (equal to the
    payment amount on sale rows, negative on refunds).

    TODO(finops sign-off): append transaction_type + operation as
    PROPOSED_ADDITIONS once finops signs off — see module docstring.
    """
    keep = spine["included_in_window"].fillna(False).astype(bool) & ~spine[
        "payment_id"
    ].isin(EXCLUDED_PAYMENT_IDS)
    report = spine.loc[keep].copy()

    # Prod column names: id / status / creation dates are the payment's.
    report["id"] = report["payment_id"]
    report["status"] = report["payment_status"]
    report["payment_creation_date"] = (
        report["created_at"].dt.tz_convert(LOCAL_TIMEZONE).dt.date
    )
    report["created_at_local"] = (
        report["created_at"].dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None)
    )
    report["order_reference"] = report["order_data"].map(
        lambda s: json.loads(s).get("reference") if isinstance(s, str) else None
    )
    instrument = report["instrument_data"].map(
        lambda s: json.loads(s) if isinstance(s, str) else {}
    )
    report["last_four"] = instrument.map(lambda d: d.get("last_four"))
    report["card_brand"] = instrument.map(lambda d: d.get("card_brand"))

    # Per-payment POS identifiers, the old module's earliest-sale-receipt
    # semantics (identical values for sale rows). m:1 because a refund day
    # puts two rows on one payment.
    report = report.merge(
        pos_ids,
        on="payment_id",
        how="left",
        validate="m:1",
    )

    # SAR exponent 2 — same divisor as the marts; gated by the currency
    # check. Signed: a refund row is negative through amount, fees, net.
    report["amount_sar"] = report["amount_minor"].astype("float") / 100
    report["settled_amount_sar"] = (
        report["settled_amount_minor"].astype("float") / 100
    )
    for c in [*fee_cols, "fee_total_minor"]:
        report[c.removesuffix("_minor") + "_sar"] = report[c].astype("float") / 100
    return report


def build_payment_rows(
    payments: pd.DataFrame, settle: pd.DataFrame, pos_ids: pd.DataFrame
) -> pd.DataFrame:
    """Pure: leftover payments + the (empty) settlement stand-in +
    per-payment POS identifiers -> payment-grain export rows (verbatim
    merchant_daily_report.build_report). Payments without a settlement txn
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
    """One CSV per merchant — header-only when the merchant had no rows
    (every in-scope merchant gets a file, that's the delivery contract).
    Every file carries the full keep-list schema, empty columns included —
    the header is fixed. The filename carries the merchant-name slug
    (finance ask 2026-08-26) and pairs report day with run date; the
    bucket path keeps the merchant_id, so uniqueness and the §J5 path
    contract never depend on display names.
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


def write_combined_file(
    report: pd.DataFrame,
    report_day: date,
    run_date: date,
    out_dir: Path,
) -> tuple[Path, int]:
    """Every merchant's rows in one CSV, same fixed header (merchant_id
    and merchant_name lead the layout, so rows stay attributable), rows
    grouped by merchant. Internal review cut only — the delivery contract
    stays one file per merchant, and the filename matches no merchant
    slug, so it can never collide with a delivered file.
    """
    rows = report.sort_values(["merchant_name", "created_at"]).reindex(
        columns=export_columns(report)
    )
    filename = (
        f"all_merchants_daily_transaction_report_{report_day}_run_{run_date}.csv"
    )
    rows.to_csv(out_dir / filename, index=False, float_format="%.2f")
    return out_dir / filename, len(rows)


def main(argv: list[str] | None = None) -> None:
    parser = make_arg_parser(__doc__)
    parser.add_argument(
        "--all-merchants",
        action="store_true",
        help="every platform merchant, test merchants included, written as "
        "one combined csv instead of per-merchant files; never uploaded",
    )
    args = parser.parse_args(argv)
    report_day, run_date = resolve_days(args)
    settings = Settings.from_env()
    # Explicit location, as everywhere: BigQuery defaults to the US
    # multi-region, which the org residency policy rejects.
    client = bigquery.Client(project=settings.gcp_project, location="me-central2")
    raw = f"{settings.gcp_project}.{settings.bq_dataset_raw}"

    spine = fetch_window_transactions(
        client, raw, report_day, all_merchants=args.all_merchants
    )
    spine, fee_cols = pivot_fees_txn(spine)
    day_ops = fetch_refund_reverse_ops(
        client, raw, report_day, all_merchants=args.all_merchants
    )
    # The day's payments (v1 spine, incident-excluded already dropped):
    # whichever of them the settlement spine does not cover ships
    # payment-grain with empty settlement columns (see module docstring).
    payments = fetch_payments(
        client, raw, report_day, all_merchants=args.all_merchants
    )
    empty_settle, _ = pivot_fees(_empty_settlement())
    empty_settle, _ = dedupe_settlement(empty_settle)

    # Both gates: the settlement-spine gate, plus the payment-grain gate
    # (duplicate payment ids, channel tenant guard) for the leftovers.
    failures = run_checks(spine, day_ops) + run_payment_checks(payments, empty_settle)
    if failures:
        raise SystemExit(
            "report-blocking check failures:\n- " + "\n- ".join(failures)
        )
    for note in informational_notes(spine, day_ops):
        print(f"NOTE: {note}")

    payment_ids = sorted(
        set(spine["payment_id"].dropna()) | set(payments["id"])
    )
    ops = (
        fetch_operations(client, raw, payment_ids)
        if payment_ids
        else _empty_operations()
    )
    pos_ids = pos_identifiers(ops)

    txn_rows = build_report(spine, pos_ids, fee_cols)
    leftover = payments.loc[~payments["id"].isin(set(txn_rows["id"]))]
    if len(leftover):
        by_status = leftover["status"].value_counts().to_dict()
        print(
            f"NOTE: {len(leftover)} payment(s) without a settlement row in "
            f"the day's windows, kept payment-grain: {by_status}"
        )
    report = pd.concat(
        [txn_rows, build_payment_rows(leftover, empty_settle, pos_ids)],
        ignore_index=True,
    )
    out_dir = Path(tempfile.mkdtemp(prefix="settlement-daily-"))
    if args.all_merchants:
        path, n_rows = write_combined_file(report, report_day, run_date, out_dir)
        print(f"all merchants: {n_rows} rows -> {path}")
    else:
        files = write_daily_files(report, report_day, run_date, out_dir)
        for mid, path, n_rows in files:
            print(f"{mid}: {n_rows} rows -> {path}")

    if args.dry_run:
        print(f"dry run — nothing uploaded, files under {out_dir}")
        return
    # Side-by-side phase guard: filenames match merchant_daily_report's,
    # so a real upload would overwrite delivered files. Goes at cutover.
    raise SystemExit(
        "settlement_daily_report is in side-by-side validation: run with "
        "--dry-run; uploads stay with merchant_daily_report until cutover"
    )


if __name__ == "__main__":
    main()
