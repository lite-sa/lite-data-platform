"""Daily per-merchant transaction report on the settlement spine.

The file for day D carries the settlement transactions of every window whose
collection day is D. A refund or a claw-back is its own negative row on the
day it books.

Grain: one row per included settlement transaction, plus one payment-grain
row (empty settlement columns) for each of the day's payments with no such
transaction: declines, sales not yet booked, voided sales.

Two modes: per-merchant files under merchant-reports/, and
`--all-merchants`, one combined file (test merchants included) under
finance-reports/.

The gate (`run_checks`, `run_payment_checks`) runs on the whole spine before
the merchant split: one failing row blocks every file of the day.

TODO(finops sign-off): add `transaction_type` and the raw settlement
`operation` to the delivered layout.
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
    require_fresh_extraction,
    resolve_days,
    upload_files,
)

# ---------------------------------------------------------------------------
# Contract constants: the delivered-file contract
# ---------------------------------------------------------------------------

# Merchants in scope, id -> display name. The name feeds the filename slug,
# so a rename here renames the file.
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

# The --all-merchants file goes to its own finance folder, never under
# merchant-reports.
ALL_MERCHANTS_GCS_PREFIX = "finance-reports/daily_settlement"

# Fee types that always get a column, so the header is stable on empty
# days. New source fee types add columns after these.
BASE_FEE_TYPES = ("mdr", "vat", "flat")
BASE_FEE_COLS = [f"fee_{t}_minor" for t in BASE_FEE_TYPES]

TERMINAL_WINDOW_STATUSES = ("SUCCESS", "FAILED")

# Claw-back rows (LiteCore CTR-921): the `operation` value, and the suffix
# the service appends to the reversed capture's operation id to build the
# row's external_reference_id.
REVERSAL_ADJUSTMENT = "reversal_adjustment"
REVERSAL_ADJUSTMENT_SUFFIX = ":reversal-adjustment"
# The operations whose rows are negative by construction.
NEGATIVE_OPERATIONS = ("refund", REVERSAL_ADJUSTMENT)

# Gate waivers, pinned to investigated ids. Anything not listed blocks.
#
# A manual VAT-deduct remediation entry booked by the settlement team, with
# references that exist nowhere in payment_v2. It is in the payout, so it
# ships, with empty payment-level columns.
WAIVED_UNRESOLVED_TXNS = frozenset({"e719e386-e8d9-4d48-99c9-f13c11924a12"})

# A window paid out without deducting a refund hold that is still HELD (the
# refund moved through the wallet path). Only its tie-out failure is waived.
WAIVED_TIE_OUT_WINDOWS = frozenset({"deeff147-8301-44b0-9210-fecbdbf305de"})

# Settlement columns on the payment-grain leftover path. Always present, so
# empty days keep the same header. Minor-unit columns never ship.
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

# The delivered layout, in order. The header is the contract (hence the
# upper-case RRN/STAN/TID).
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
    """EXPORT_COLUMNS plus the major-unit column of any new fee type found
    in the day's fees JSON."""
    extra_fees = sorted(
        c
        for c in report.columns
        if c.startswith("fee_") and c.endswith("_sar") and c not in EXPORT_COLUMNS
    )
    return [*EXPORT_COLUMNS, *extra_fees]


def merchant_slug(name: str, merchant_id: str) -> str:
    """Filename-safe slug of the display name; the id prefix when nothing
    survives (a fully non-Latin name)."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug or merchant_id[:8]


# ---------------------------------------------------------------------------
# Extraction: the settlement spine and its gate inputs
# ---------------------------------------------------------------------------


def fetch_window_transactions(
    client: bigquery.Client,
    raw: str,
    report_day: date,
    end_day: date | None = None,
    all_merchants: bool = False,
) -> pd.DataFrame:
    """The spine: every settlement transaction in the in-scope merchants'
    windows collecting on the report day. REMOVED rows stay, flagged
    `included_in_window`, so the gate sees complete windows; so do
    incident-excluded payments, which build_report drops.

    `external_reference_id` resolves the operation and `parent_payment_id`
    the payment, with the operation's `payment_id` as fallback
    (`payment_resolved_via_op`). A claw-back row resolves its operation
    through `original_operation_id` (suffix stripped) and joins the sale row
    it mirrors (`original_*` columns; NULL on other rows).

    `end_day` widens to a collection-day range for one-off reports.
    `all_merchants=True` lifts the MERCHANTS allowlist.
    """
    query = f"""
        with txn as (
            select
                t.*,
                t.operation = @adjustment_operation as is_reversal_adjustment,
                if(
                    ends_with(t.external_reference_id, @adjustment_suffix),
                    left(
                        t.external_reference_id,
                        length(t.external_reference_id)
                            - length(@adjustment_suffix)
                    ),
                    t.external_reference_id
                ) as original_operation_id
            from {latest_version(f"{raw}.settlement__transaction")} t
        ),
        original as (
            -- The sale row a claw-back mirrors: same operation id, no
            -- suffix. The earliest, if a retry booked two.
            select external_reference_id, status, amount, settled_amount, fees
            from {latest_version(f"{raw}.settlement__transaction")}
            where operation is distinct from @adjustment_operation
            qualify row_number() over (
                partition by external_reference_id order by created_at
            ) = 1
        )
        select
            t.id as settlement_transaction_id,
            coalesce(t.parent_payment_id, o.payment_id) as payment_id,
            t.external_reference_id as operation_id,
            t.original_operation_id,
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
            t.parent_payment_id is null and p.id is not null
                as payment_resolved_via_op,
            orig.status as original_txn_status,
            orig.amount as original_amount_minor,
            orig.settled_amount as original_settled_amount_minor,
            orig.fees as original_fees,
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
        from txn t
        join {latest_version(f"{raw}.settlement__settlement_window")} w
            on w.id = t.settlement_window_id
        left join {merchant_directory(raw)} b
            on b.business_id = w.merchant_id
        left join {latest_version(f"{raw}.payment_v2__payment_operations")} o
            on o.id = t.original_operation_id
        left join original orig
            on t.is_reversal_adjustment
            and orig.external_reference_id = t.original_operation_id
        left join {latest_version(f"{raw}.payment_v2__payments")} p
            on p.id = coalesce(t.parent_payment_id, o.payment_id)
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
            bigquery.ScalarQueryParameter(
                "adjustment_operation", "STRING", REVERSAL_ADJUSTMENT
            ),
            bigquery.ScalarQueryParameter(
                "adjustment_suffix", "STRING", REVERSAL_ADJUSTMENT_SUFFIX
            ),
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
    """The day's payments for the in-scope merchants, with merchant and
    channel display names. `channel_merchant_id` (the channel's owning
    merchant) feeds the tenant guard and is never exported.
    Incident-excluded payments are filtered out. `end_day` and
    `all_merchants` as in fetch_window_transactions.
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
    """Operations of the report's payments: the columns pos_identifiers()
    parses.
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
    """Successful REFUND / REVERSE ops whose event day (op updated_at,
    local) is the report day, flagged with whether any settlement
    transaction references them. Feeds the notes only.
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
# Shared transforms
# ---------------------------------------------------------------------------


def pos_identifiers(ops: pd.DataFrame) -> pd.DataFrame:
    """One row per payment: RRN / STAN / TID from the earliest
    idempotency_key shaped '<rrn>;<stan>;<tid>' (the sale receipt).
    Reversals use ':' and other rows a UUID, so they never qualify. Ecom
    payments have no such op and stay absent.
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
    """Schema stand-in for the leftover build: a leftover payment has no
    settlement row by definition."""
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
    """Pivot the `fees` JSON string ([{fee_type, amount, ...}]) to one
    column per fee type, keyed on `id`; a missing type is 0. Returns the
    frame and the fee column list.
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
    """pivot_fees for the spine, where the transaction id column is
    `settlement_transaction_id` (`id` is the payment's)."""
    renamed = spine.rename(columns={"settlement_transaction_id": "id"})
    pivoted, fee_cols = pivot_fees(renamed)
    return pivoted.rename(columns={"id": "settlement_transaction_id"}), fee_cols


def dedupe_settlement(settle: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """At most one settlement row per payment: keep the earliest, count all
    in n_settlement_txns. Only the empty stand-in passes through here; it
    fixes the leftover frame's schema.
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


def is_reversal_adjustment(spine: pd.DataFrame) -> pd.Series:
    """Row mask of claw-back rows, from `operation` (NULL is never one)."""
    return spine["operation"].fillna("").str.lower().eq(REVERSAL_ADJUSTMENT)


def derive_adjustment_fees(spine: pd.DataFrame) -> pd.DataFrame:
    """Fill `fees` on claw-back rows with the original sale's fees, negated.
    Settlement books a claw-back net of fees but with no `fees` of its own,
    so without this the row tie-out fails. Sets `fees_derived_from_original`.
    Runs before the pivot.
    """
    spine = spine.copy()
    derive = (
        is_reversal_adjustment(spine)
        & spine["fees"].isna()
        & spine["original_fees"].notna()
    )
    spine["fees_derived_from_original"] = derive

    def negate(fees_json: str) -> str:
        return json.dumps(
            [{**fee, "amount": -int(fee["amount"])} for fee in json.loads(fees_json)]
        )

    spine.loc[derive, "fees"] = spine.loc[derive, "original_fees"].map(negate)
    return spine


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


def run_checks(spine: pd.DataFrame) -> list[str]:
    """Report-blocking checks on the full spine (REMOVED rows and
    incident-excluded payments included). A non-empty return blocks the
    export.
    """
    failures: list[str] = []
    included = spine["included_in_window"].fillna(False).astype(bool)
    adjustment = is_reversal_adjustment(spine)

    if spine["settlement_transaction_id"].duplicated().any():
        failures.append("duplicate settlement transaction ids after dedup")

    # Every row must resolve its operation and payment, waived ids aside.
    waived = spine["settlement_transaction_id"].isin(WAIVED_UNRESOLVED_TXNS)
    no_op = ~spine["op_resolved"].fillna(False).astype(bool) & ~waived
    if no_op.any():
        failures.append(
            f"{int(no_op.sum())} rows with no "
            "payment_operation for external_reference_id"
        )
    no_payment = ~spine["payment_resolved"].fillna(False).astype(bool) & ~waived
    if no_payment.any():
        failures.append(
            f"{int(no_payment.sum())} rows with no "
            "payment for parent_payment_id (nor via the operation's payment_id)"
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

    # Row tie-out on included rows, sign-correct for refunds.
    tie_bad = included & (
        spine["amount_minor"] - spine["fee_total_minor"]
        != spine["settled_amount_minor"]
    )
    if tie_bad.any():
        failures.append(
            f"{int(tie_bad.sum())} included rows failing "
            "amount - fees == settled_amount"
        )

    # Refund shape: every negative row was born HELD (hold_at set). A
    # claw-back is born ACTIVE, so it is exempt; its own contract is below.
    neg_no_hold = (
        included & (spine["amount_minor"] < 0) & spine["hold_at"].isna() & ~adjustment
    )
    if neg_no_hold.any():
        failures.append(f"{int(neg_no_hold.sum())} negative rows without hold_at")

    # Operation/sign cross-check. `operation` is NULL on rows older than
    # settlement migration 0009; those are skipped.
    op_sign_bad = (
        included
        & spine["operation"].notna()
        & (
            spine["operation"].str.lower().isin(NEGATIVE_OPERATIONS)
            != spine["amount_minor"].lt(0)
        )
    )
    if op_sign_bad.any():
        failures.append(
            f"{int(op_sign_bad.sum())} included rows where operation and "
            "amount sign disagree"
        )

    # Claw-back contract: the row mirrors a sale row the reversal flipped
    # to REMOVED, with both amounts negated.
    no_original = adjustment & ~spine["original_txn_status"].eq("REMOVED")
    if no_original.any():
        failures.append(
            f"{int(no_original.sum())} reversal_adjustment rows whose "
            "original sale row is missing or not REMOVED"
        )
    not_mirrored = (
        adjustment
        & spine["original_amount_minor"].notna()
        & (
            spine["amount_minor"].ne(-spine["original_amount_minor"])
            | spine["settled_amount_minor"].ne(
                -spine["original_settled_amount_minor"]
            )
        )
    )
    if not_mirrored.any():
        failures.append(
            f"{int(not_mirrored.sum())} reversal_adjustment rows whose "
            "amounts do not mirror the original sale row"
        )

    # Per-window tie-out: included rows sum to the window's settled_amount.
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
    win_bad = win_bad.loc[~win_bad.index.isin(WAIVED_TIE_OUT_WINDOWS)]
    if len(win_bad):
        failures.append(
            f"{len(win_bad)} window(s) failing sum(included settled_amount) "
            f"== window.settled_amount: {win_bad.index.tolist()[:5]}"
        )

    return failures


def run_payment_checks(payments: pd.DataFrame, settle: pd.DataFrame) -> list[str]:
    """Report-blocking checks on the payment-grain leftover path. A
    non-empty return blocks the export.
    """
    failures: list[str] = []
    if payments["id"].duplicated().any():
        failures.append("duplicate payment ids after dedup")

    # Tenant guard: a resolved channel must belong to the payment's
    # merchant. An unresolvable channel ships a blank name.
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

    # Row tie-out, blocking only once the window is terminal.
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
    # Name every waived id the run carries, so a waiver never applies
    # silently.
    waived_txns = sorted(
        set(spine.loc[
            spine["settlement_transaction_id"].isin(WAIVED_UNRESOLVED_TXNS),
            "settlement_transaction_id",
        ])
    )
    if waived_txns:
        notes.append(
            f"WAIVED join contract for {waived_txns}: manual adjustment "
            "entry, ships with empty payment-level columns"
        )
    waived_windows = sorted(
        set(spine.loc[
            spine["settlement_window_id"].isin(WAIVED_TIE_OUT_WINDOWS),
            "settlement_window_id",
        ])
    )
    if waived_windows:
        notes.append(
            f"WAIVED per-window tie-out for {waived_windows}: stuck HELD "
            "refund hold"
        )
    # Each op fallback means an upstream event lost its payment id.
    via_op = sorted(
        set(spine.loc[
            spine["payment_resolved_via_op"].fillna(False).astype(bool),
            "settlement_transaction_id",
        ])
    )
    if via_op:
        notes.append(
            f"{len(via_op)} row(s) with NULL parent_payment_id resolved "
            "through the operation's payment_id (upstream event lost the "
            f"payment id): {via_op[:5]}"
        )
    clawbacks = spine.loc[is_reversal_adjustment(spine)]
    if len(clawbacks):
        derived = clawbacks.get(
            "fees_derived_from_original", pd.Series(False, index=clawbacks.index)
        )
        notes.append(
            f"{len(clawbacks)} reversal_adjustment row(s) (CTR-921 claw-back "
            "of a reversal after payout), negative rows of this day; fees "
            f"derived from the original sale row on {int(derived.sum())} "
            f"of them: {sorted(clawbacks['settlement_transaction_id'])[:5]}"
        )
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
    # A refund with no settlement row is settled from the merchant's
    # wallet account, and is not in this file by design.
    wallet = day_ops.query("operation_type == 'REFUND' and not has_settlement_row")
    if len(wallet):
        notes.append(
            f"{len(wallet)} successful REFUND op(s) settled from the "
            f"merchant wallet (no settlement row, never in this file): "
            f"{wallet['op_id'].tolist()[:5]}"
        )
    return notes


# ---------------------------------------------------------------------------
# Build: transaction-grain spine rows + payment-grain leftovers
# ---------------------------------------------------------------------------


def build_report(
    spine: pd.DataFrame, pos_ids: pd.DataFrame, fee_cols: list[str]
) -> pd.DataFrame:
    """Full spine -> export frame. Drops REMOVED rows and incident-excluded
    payments. `id`, `status` and the creation dates describe the row's
    payment; `amount_sar` is the transaction's amount (negative on refunds).
    """
    keep = spine["included_in_window"].fillna(False).astype(bool) & ~spine[
        "payment_id"
    ].isin(EXCLUDED_PAYMENT_IDS)
    report = spine.loc[keep].copy()

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

    # m:1 because a refund day puts two rows on one payment.
    report = report.merge(
        pos_ids,
        on="payment_id",
        how="left",
        validate="m:1",
    )

    # SAR exponent 2, guarded by the currency check. Signed: a refund row
    # is negative through amount, fees and net.
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
    """Leftover payments -> payment-grain export rows. Settlement columns
    stay NaN ("no settlement" differs from "zero fee"), as do RRN/STAN/TID
    on payments without a sale receipt.
    """
    payments = payments.copy()
    payments["payment_creation_date"] = (
        payments["created_at"].dt.tz_convert(LOCAL_TIMEZONE).dt.date
    )
    payments["created_at_local"] = (
        payments["created_at"].dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None)
    )
    # SAR exponent 2.
    payments["amount_sar"] = payments["amount"].astype("float") / 100
    payments["order_reference"] = payments["order_data"].map(
        lambda s: json.loads(s).get("reference") if isinstance(s, str) else None
    )
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
    """One CSV per in-scope merchant, header-only when it had no rows. The
    header is fixed. The filename carries the merchant-name slug, the report
    day and the run date; the directory is the merchant_id.
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
    """Every merchant's rows in one CSV for finance, same header, grouped
    by merchant.
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
        "one combined csv (uploaded to the finance folder) instead of "
        "per-merchant files",
    )
    args = parser.parse_args(argv)
    report_day, run_date = resolve_days(args)
    settings = Settings.from_env()
    # BigQuery defaults to the US multi-region, which the org residency
    # policy rejects.
    client = bigquery.Client(project=settings.gcp_project, location="me-central2")
    raw = f"{settings.gcp_project}.{settings.bq_dataset_raw}"
    require_fresh_extraction(client, raw, report_day)

    spine = fetch_window_transactions(
        client, raw, report_day, all_merchants=args.all_merchants
    )
    spine = derive_adjustment_fees(spine)
    spine, fee_cols = pivot_fees_txn(spine)
    day_ops = fetch_refund_reverse_ops(
        client, raw, report_day, all_merchants=args.all_merchants
    )
    # Payments the settlement spine does not cover ship payment-grain.
    payments = fetch_payments(
        client, raw, report_day, all_merchants=args.all_merchants
    )
    empty_settle, _ = pivot_fees(_empty_settlement())
    empty_settle, _ = dedupe_settlement(empty_settle)

    failures = run_checks(spine) + run_payment_checks(payments, empty_settle)
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
    if not settings.gcs_bucket_egress:
        raise SystemExit("GCS_BUCKET_EGRESS is not set (required unless --dry-run)")
    if args.all_merchants:
        pairs = [(f"{ALL_MERCHANTS_GCS_PREFIX}/{path.name}", path)]
    else:
        # Blob name reuses the local filename so the two can never drift.
        pairs = [
            (f"{GCS_PREFIX}/{mid}/{REPORT_TYPE}/{path.name}", path)
            for mid, path, _ in files
        ]
    for uri in upload_files(pairs, settings.gcs_bucket_egress, settings.gcp_project):
        print(f"uploaded {uri}")


if __name__ == "__main__":
    main()
