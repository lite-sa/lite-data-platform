# SAMA monthly report

This package answers the two SAMA monthly sheets: PI (payment institution)
and EMI (e-money institution). Each data point on a sheet is one module,
`pi/sN_MM_*.py` or `emi/sN_MM_*.py`. Every module has two queries:

- `DETAIL_SQL`: one row per merchant, terminal, wallet, payment or ledger
  entry behind the number. Use it to check an answer or explain it.
- `ANSWER_SQL`: the value we file, aggregated from `DETAIL_SQL`.

This README records the populations and the exact predicates behind every
answer. The code is the source of truth; update this file in the same
change when a predicate moves.

## Running

```bash
# the whole report, one CSV row per value
uv run python -m app_etl.export.sama --month 2026-08

# one data point, answer plus the first 20 detail rows
uv run python -m app_etl.export.sama.pi.s1_02_pos_merchants
```

The single-module form runs the `REPORT_MONTH` set at the top of the file.
The runner skips `emi.s3_03_in_sadad` and `pi.s1_03_sadad_billers`: both
are hardcoded zeros that we file by hand.

## Definitions

- **Report month**: a calendar month in Riyadh time. Every query gets
  `@month_start` and `@month_end` (inclusive `DATE`s) and `@tz =
  'Asia/Riyadh'`. Every timestamp is compared as `date(ts, @tz)`.
- **Latest version**: raw keeps one row per source version. Every table is
  read through `lv(table)`, one row per `id`:

  ```sql
  qualify row_number() over (
      partition by id order by updated_at desc, _dlt_load_id desc
  ) = 1
  ```

  The terminal table has no `id` column, so it uses the same window over
  `terminal_id`.
- **Test merchant**: a merchant in `TEST_MERCHANT_IDS` (copy of the dbt seed
  `test_merchants.csv`). Answers **include** test merchants. Detail rows
  flag them `is_test` so you can see what they add.
- **Hard-deleted merchant**: a merchant deleted at source, listed in
  `DELETED_MERCHANT_IDS` (copy of the dbt seed `deleted_merchants.csv`).
  These never appear, not even in detail rows. The predicate keeps rows
  with a NULL merchant:

  ```sql
  coalesce(<merchant_col> not in unnest(@deleted_ids), true)
  ```

- **Excluded payment**: a payment deleted or reversed upstream during an
  incident, listed in `EXCLUDED_PAYMENT_IDS` (copy of the dbt seed
  `incident_excluded_payments.csv`). Raw never sees a source DELETE, so we
  filter these by id.
- **Scheme correction**: a payment whose `instrument_data.card_brand`
  names the wrong scheme, listed in `SCHEME_CORRECTIONS` (copy of the dbt
  seed `scheme_corrections.csv`) with the scheme the operations team
  confirmed. The correction wins over every other scheme source.
- **Total, active, inactive**: every count on both sheets follows one rule.
  Total is cumulative to month end. Active means activity inside the report
  month. Inactive is total minus active.

## PI sheet

### Authorized payment

`pi.common.AUTHORIZED_PAYMENTS`, one row per payment. It feeds every PI
section that mentions activity.

A payment is **authorized** when one of its operations reached `SUCCESS`
on the right operation type for its channel:

```sql
if(
    channel = 'POS',
    operation_type in ('AUTHORIZE', 'CAPTURE', 'PURCHASE'),
    operation_type = 'AUTHORIZE'
)
and status = 'SUCCESS'
```

- **Date**: the earliest `updated_at` of those `SUCCESS` operations,
  converted to Riyadh time. The payment belongs to the month that date
  falls in.
- **Amount**: `payments.amount`, in halalas (divide by 100 for SAR).
- **Pending operations** never count: they have no verdict yet.
- **Reversals and refunds** stay in. We net nothing. A reversed or
  refunded payment still had its successful capture in the month.
- **Payment filters**: `id not in unnest(@excluded_payment_ids)` and the
  hard-deleted predicate on `payments.merchant_id`.

Every payment in prod is single-message (`capture_mode = 'INSTANT'`). In
practice that means `AUTHORIZE` for ECOM and `CAPTURE` for POS. No
`PURCHASE` operation has appeared yet.

**Channel**, first match wins:

1. `POS` when `payments.channel_type = 'IN-PERSON'`, or any operation of
   the payment carries a `terminal_id`.
2. `ECOM` when `channel_type = 'ECOM'`, or any operation has no
   `terminal_id`, or the payment has a `threeds` row.
3. `UNKNOWN` otherwise.

**TID**: `min(terminal_id)` over the payment's operations where
`terminal_id is not null`.

**Scheme**, first non-null wins:

1. The scheme correction for the payment, if any (`scheme_source =
   'correction'`).
2. `upper(json_value(instrument_data, '$.card_brand'))`, with `UNKNOWN`
   treated as null.
3. The EMV application id on a POS operation, `substr(json_value(metadata,
   '$.emv_aid'), 1, 10)`: `A000000228` MADA, `A000000003` VISA,
   `A000000004` MASTERCARD, `A000000025` AMEX.
4. `upper(threeds.authentication_scheme)`.
5. `UNKNOWN`.

The sheet reports MADA, VISA and MASTERCARD. Every other scheme goes to
`Other`, including `UNKNOWN`.

### I.1 ECOM merchants

**Population** (`pi.common.KYB_MERCHANTS`): approving a merchant's KYB
opens its ECOM account, so every KYB-approved merchant counts. From the
latest version of `business_management__business_entities`:

```sql
where status = 'KYB approved'
  and date(coalesce(kyb_approved_at, created_at), @tz) <= @month_end
  and (not coalesce(is_deleted, false) or date(deleted_at, @tz) > @month_end)
  and coalesce(business_id not in unnest(@deleted_ids), true)
```

The merchant key is `business_id`, not the table's `id`. Merchants created
manually have no `kyb_approved_at`, so `created_at` stands in (flagged
`kyb_date_is_created_at`).

This population is also the **registered merchants** count for both
channels. Every merchant that owns a terminal is KYB approved.

**Active**: at least one authorized payment with `channel = 'ECOM'` in the
report month.

**Check column** `active_outside_population`: ECOM payers missing from the
population. It should be 0.

### I.2 POS merchants

**Population**: merchants that own a terminal in service in the month,
plus any merchant with an authorized POS payment in the month:

```
(TERMINALS grouped by merchant_id)
  FULL JOIN (merchants with an authorized POS payment in the month)
  ON merchant_id
```

`TERMINALS` is defined under [Terminals in service](#terminals-in-service).
The payment side adds merchants whose terminal has since moved to another
merchant.

**Active**: at least one authorized payment with `channel = 'POS'` in the
report month. The terminal's own `status` plays no part.

**Check column** `active_without_terminal`: active merchants with no
terminal in the inventory. It should be 0.

The merchant registry `core.merchants` does not filter POS merchants. It
supplies the name only, through a left join on `merchant_id`. KYB status
and soft deletes are never checked here.

### I.3 SADAD billers

Filed as zero. Lite has no SADAD biller integration.

### Terminals in service

`pi.common.TERMINALS`, from the latest version per `terminal_id` of
`terminal_management__terminal`:

```sql
where date(t.created_at, @tz) <= @month_end
  and coalesce(date(t.deleted_at, @tz) >= @month_start, true)
  and coalesce(t.merchant_id not in unnest(@deleted_ids), true)
```

In words: created by month end, and either never deleted or deleted in or
after the report month. A terminal pulled out during the month is still in
the population. There is no filter on `status` (`ASSIGNED`, `ACTIVE`,
`TERMINATED`) or on `terminal_type` (`POS`, `SOFTPOS`).

Two flags come from `deleted_at`:

- `is_pulled_out`: `coalesce(date(deleted_at, @tz) <= @month_end, false)`
- `is_installed`: `coalesce(date(deleted_at, @tz) > @month_end, true)`

### II.1 to II.3 POS devices

All three sections read `pi.common.TERMINAL_ACTIVITY`:

```
TERMINALS
  FULL JOIN (TIDs with an authorized POS payment in the month)
  ON tid
```

- **Total**: every row.
- **II.1 Active**: the TID took at least one authorized POS payment in the
  month.
- **II.2 Inactive**: total minus active. It includes terminals that are
  `ASSIGNED` (allocated but never activated) and terminals pulled out in
  the month without a payment.
- **II.3 Pulled out**: `is_pulled_out`. Terminal replacements and
  reassignments make this number unreliable, so review it by hand before
  filing.

Check columns on II.1: `not_in_inventory` (active TIDs missing from the
inventory, should be 0) and `merchant_moved` (the payment's merchant
differs from the terminal's current merchant).

### III Transactions by channel and scheme

Every authorized payment in the month, counted and summed per channel
(`POS`, `ECOM`) and reported scheme. The `All` rows must equal POS plus
ECOM. The `unknown_scheme` column counts payments whose scheme fell
through to `UNKNOWN`.

## EMI sheet

### Wallet

`emi.common.WALLETS`, from the latest version of `ledger__account`:

```sql
where a.tag = 'WALLET'
  and date(a.created_at, @tz) <= @month_end
  and coalesce(a.owner not in unnest(@deleted_ids), true)
```

The merchant is `ledger__account.owner`. Account `status` does not filter
wallets; every wallet in prod is `ACTIVE`. A lem merchant's wallet is owned
by its legal entity (`owner_type = 'LEGAL_ENTITY'`), so its name comes from
`core.merchants.legal_entity_id`, not `merchant_id`.

### Money movement

`emi.common.IS_MOVEMENT`, a predicate on one `ledger__entry` row `e`.
The ledger posts extra entries for holds and for the accounting side of a
capture. A movement is an entry that changed what the merchant owns:

```sql
e.hold_at is null
and e.released_at is null
and e.type != 'RELEASE_INCOMING_TRANSFER'
and not (
    e.direction = 'CREDIT'
    and e.type in (
        'CAPTURE_BENEFICIARY_TRANSFER', 'CAPTURE_WITHDRAWAL',
        'EXTERNAL_SETTLEMENT', 'REFUND_PAYMENT', 'RELEASE_HOLD',
        'RELEASE_WITHDRAWAL', 'RELEASE_EXTERNAL_SETTLEMENT',
        'RELEASE_REFUND_PAYMENT', 'REVERSE_ISSUING',
        'INTERNAL_CLOSE_ISSUING', 'CAPTURE_ISSUING'
    )
)
```

It drops:

- holds (`hold_at` set) and released holds (`released_at` set),
- the credit leg that a capture, refund or release posts against its hold,
- `RELEASE_INCOMING_TRANSFER`, which moves money between two compartments
  of the same wallet.

Entries are dated by `date(e.created_at, @tz)`.

### I.1 Outstanding e-money balance

Point in time at month end. For each wallet, we take
`available_balance_after` from its last entry on or before month end,
ordered by `created_at desc, order_id desc`. A wallet with no entry by
then holds 0. The answer is the sum over all wallets.

Memo columns:

- `negative_balance_wallets`: should be 0.
- `memo_open_holds_sar`: debit holds still open at month end. A hold is
  open when `hold_at` is on or before month end and
  `coalesce(captured_at, released_at)` is null or after month end.
  Holds sit outside the available balance, so they are not in the answer.

### II.1 and II.2 Wallets

- **Total**: the wallet population.
- **II.1 Active**: the wallet has at least one movement entry created in
  the report month.
- **II.2 Inactive**: total minus active.

### III.1 Incoming bank transfers

Credit entries on a wallet, created in the month, with
`type in ('INCOMING_TRANSFER', 'HOLD_INCOMING_TRANSFER')`. A transfer over
the wallet limit posts both types under one `parent_transaction_id`: the
allowed part and the pending rest. So volume is
`count(distinct parent_transaction_id)` and value is the sum of the
entries.

### III.2 Top-ups

`payout__topup` rows created in the month with `status = 'completed'`,
with the hard-deleted predicate on `merchant_id`. We file zero until the
product launches. The ledger entry type a completed top-up posts is still
unknown. Once one exists, exclude that type from III.4.

### III.3 SADAD

Filed as zero. Lite has no SADAD product.

### III.4 Other incoming

Every wallet credit created in the month that passes `IS_MOVEMENT` and is
not an incoming bank transfer:

```sql
e.direction = 'CREDIT'
and e.type not in ('INCOMING_TRANSFER', 'HOLD_INCOMING_TRANSFER')
```

Today this is `INTERNAL_SETTLEMENT`: acquiring proceeds settled into the
merchant's wallet. Its `external_reference_id` is the
`settlement__instruction` id.

### IV.1 and IV.2 Outgoing transfers on IPS and SARIE

`emi.common.WALLET_PAYOUTS` starts from the ledger. It takes the wallet
debit that captures a payout hold:

```sql
e.type = 'CAPTURE_BENEFICIARY_TRANSFER' and e.direction = 'DEBIT'
```

It then full-joins the payout transfer on
`payout__transfer.id = e.parent_transaction_id`, with the transfer side
filtered to:

```sql
t.direction = 'OUTGOING'
and t.status = 'completed'
and t.comment like '%Transfer%'
```

The booking date is the ledger entry's `created_at`, else the transfer's
`processed_at`. The amount is the entry amount, else the transfer amount.

**Rail**: the ANB initiation request is the only record of the channel. We
read `json_value(request, '$.payload.channel')` from the latest
`payout__transfer_transaction` with
`event_type = 'BANK_TRANSFER_INITIATION_RECORDED'`. When it says `IPS` or
`SARIE`, that is the rail. Otherwise ANB's routing rule decides: below
SAR 20,000 (`IPS_LIMIT_MINOR = 2_000_000` halalas) is IPS, the rest is
SARIE.

Check columns: `by_amount_rule` (rail chosen by amount, not channel) and
`unmatched` (found in only one of ledger and payout, should be 0).

### IV.3 Other outgoing

Every wallet debit created in the month that passes `IS_MOVEMENT` and is
not a payout:

```sql
e.direction = 'DEBIT'
and e.type != 'CAPTURE_BENEFICIARY_TRANSFER'
```

Today this is refunds and payment reversals funded from the wallet. Card
spend counts once captured (`CAPTURE_ISSUING`, `DEBIT_ISSUING`). An
authorization still open at month end shows in I.1's open-holds memo
instead.

## Known limits

- **No terminal history**: `terminal_management__terminal` keeps the
  current merchant only. A terminal moved between merchants counts under
  its current owner. The payment side of I.2 and II.1 still brings in the
  merchant it took payments for.
- **Current KYB status only**: `business_entities.status` holds today's
  state. A merchant approved in August and suspended in October drops out
  of August when you rerun it in November.
- **card_brand can name the wrong scheme**: some POS payments on mada
  cards carry VISA or MASTERCARD in `card_brand`. The cause upstream is
  not known yet. The first list (2026-09-30) moved 16 September POS payments, SAR 8,010.48,
  to MADA. Ten of them carried a mada EMV AID that `card_brand`
  outranked; six had no AID. A further 16 September POS payments show the
  reverse (`card_brand` MADA, AID VISA) and stay as MADA until someone
  confirms them.
- **Test merchants move the EMI balance**: in August 2026 one test wallet
  held SAR 48,612.23 of the SAR 48,657.45 total. Read the `is_test` rows
  in the I.1 detail before filing.
