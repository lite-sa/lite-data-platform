# Operational runbook — v1 test phase

Every manual operation we expect to run while testing the pipelines:
ingest → verify → dbt build → verify, plus the destructive resets at the
bottom. Commands are copy-pasteable from the **repo root**. Design context
lives elsewhere (`docs/aml-alert-design.md`, `docs/configuration.md`,
`apps/app_etl/README.md`) — this file is only *how to run and check
things*.

(The AML alert-export stage — `python -m app_etl.export.aml_alerts`,
egress bucket, `export-alerts` Cloud Run Job — was removed along with the
alert-mart stack it read from; recover from git history, `DAT-15-aml-p0`,
if it comes back. This runbook covers ingestion + dbt only.)

## 0. Session setup

```bash
cd ~/workspace/lite-data-platform

# Shell vars for the gcloud/bq snippets below (app config is .env, NOT this)
source infra.env            # DEV, OPS, REGION, SA, REPO, BUCKET

# Which datasets this session targets. Keep these consistent with what
# .env says — the python/dbt side reads .env, the bq/gcloud snippets read
# these. Test phase = raw_test/core_test/aml_test; flip to
# raw_litecore/core/aml only at deliberate promotion.
export RAW_DATASET=raw_test
export CORE_DATASET=core_test    # staging + canonical payments layer
export AML_DATASET=aml_test      # the one surviving AML model only

# dbt wrapper used throughout (dbt does not read .env by itself)
dbtx() { uv run --env-file .env dbt "$@" --project-dir apps/app_etl/dbt --profiles-dir apps/app_etl/dbt; }
```

Sanity checks before anything else (four "project" concepts can disagree —
`docs/gcp-auth-and-config.md`):

```bash
gcloud config get-value project        # CLI project
gcloud auth list                       # active principal
cat .env                               # what the app side will actually use
```

`gcloud auth list` shows the **CLI** principal only — it says nothing about
whether **ADC** (what dlt/client libraries actually use) is impersonating
`sa-app-etl`, which is a separate, machine-wide setting. Two ways to check:

```bash
# Authoritative: the ADC file's own type field.
grep '"type"' ~/.config/gcloud/application_default_credentials.json
# "authorized_user"              -> ADC is your own human identity
# "impersonated_service_account" -> ADC is impersonating (check .../impersonation_url for which SA)

# Quick human-readable check: the email attached to the live token.
# Confirmed 2026-07-21: prints your email when ADC is authorized_user
# (application-default login requests the userinfo.email scope by
# default). Expected to print no `email` field when impersonating,
# since impersonated tokens don't carry that scope — not yet
# independently confirmed; if it disagrees with the type-field check
# above, trust the type field.
curl -s "https://oauth2.googleapis.com/tokeninfo?access_token=$(gcloud auth application-default print-access-token)"
```

### 0.1 Toggle SA impersonation (`$SA`, local ingestion testing only)

Impersonating lets a laptop run ingestion (or `psql`) as `sa-app-etl`
instead of your own broader access — proves the SA's own grants are
sufficient, same identity the Cloud Run jobs use natively (no
impersonation there). One-time prerequisites (`roles/
iam.serviceAccountTokenCreator` on `$SA` for your user,
`iamcredentials.googleapis.com` enabled on `$DEV`) are provisioning, not
session setup — see `docs/provisioning.md` / `docs/iac-port.md` §1.

```bash
# Enable — writes a machine-wide impersonated_service_account ADC file,
# not shell/repo-scoped; stays impersonating in every other terminal too
# until you disable it.
gcloud auth application-default login --impersonate-service-account=$SA

# Disable — back to your own human ADC identity.
gcloud auth application-default login
```

For ingestion only, the Auth Proxy must be running in another terminal.
**Don't pass `--impersonate-service-account` to the proxy once ADC is
already impersonating** — that double-impersonates (the SA tries to
impersonate itself) and 403s on `iam.serviceAccounts.getAccessToken`,
which looks identical to the missing-`tokenCreator` error but isn't:

```bash
cloud-sql-proxy --auto-iam-authn --port=5432 lite-litecore-dev:me-central2:non-cde-postgres
```

## 1. Run the ingestion pipelines

One pipeline per source database. `Settings.from_env()` loads `.env`
itself (python-dotenv), so no `--env-file` here; shell-exported vars win
over `.env`.

```bash
uv run python -m app_etl.ingestion.payment_v2            # payments + payment_operations (incremental, watermarked)
uv run python -m app_etl.ingestion.business_management   # business_entities (snapshot, full replace)
uv run python -m app_etl.ingestion.ledger                # account + entry (incremental, watermarked)
uv run python -m app_etl.ingestion.smart_routing         # profile + routing_rule (snapshot) + transaction_evaluation (incremental)
```

`ingestion.user` (→ `merchants`, per `CLAUDE.md`'s pipeline list) is not
implemented yet — there is no `app_etl/ingestion/user.py` on disk despite
being documented as one of the per-database pipelines; don't run it until
it exists.

Each prints its `load_info` and exits non-zero on any failed load job.
Incremental pipelines are safe to re-run any time — the persisted
watermark (minus the 10-min safety lag) means a re-run picks up only new
row versions. `--refresh` exists but is destructive (drops tables **and**
cursor state) — see §6.

**In the cloud** (PARKED — ingestion from Cloud Run is blocked on the
read-replica/network path; the live `ingest-*` jobs are the stale
per-*table* ones slated for deletion, `docs/iac-port.md` §5). Once the
per-database jobs exist, it will be:

```bash
gcloud run jobs execute ingest-payment-v2 --region=$REGION --project=$DEV --wait
```

## 2. Verify ingestion state

### 2.1 BigQuery tables

```bash
bq --project_id=$DEV ls $RAW_DATASET

# Row counts + last-modified in one shot
bq --project_id=$DEV query --nouse_legacy_sql "
select table_id, row_count, timestamp_millis(last_modified_time) as last_modified
from \`$DEV.$RAW_DATASET.__TABLES__\`
order by table_id"
```

Watermark freshness — the high-water mark should trail wall clock by no
more than the safety lag (10 min) plus time since the last run:

```bash
bq --project_id=$DEV query --nouse_legacy_sql "
select 'payments' as table_name, max(updated_at) as high_water_mark, count(*) as row_versions
from \`$DEV.$RAW_DATASET.payments\`
union all
select 'payment_operations', max(updated_at), count(*)
from \`$DEV.$RAW_DATASET.payment_operations\`"
```

Remember the incremental grain is `(id, updated_at)` — `row_versions`
grows on every source update; dedup happens downstream in staging views.

### 2.2 dlt state tables in BigQuery

`_dlt_loads` is the load ledger (status `0` = fully loaded — anything
else means an interrupted load):

```bash
bq --project_id=$DEV query --nouse_legacy_sql "
select load_id, schema_name, status, inserted_at
from \`$DEV.$RAW_DATASET._dlt_loads\`
order by inserted_at desc
limit 10"
```

`_dlt_pipeline_state` holds the authoritative pipeline state (one row per
state version; the `state` blob is compressed, so read cursor values via
the local CLI in §2.4, not from here):

```bash
bq --project_id=$DEV query --nouse_legacy_sql "
select pipeline_name, version, created_at
from \`$DEV.$RAW_DATASET._dlt_pipeline_state\`
qualify row_number() over (partition by pipeline_name order by created_at desc) = 1"
```

### 2.3 GCS staging bucket

Staging prefix mirrors the target dataset
(`gs://$BUCKET/$RAW_DATASET/…`), so test runs can never collide with a
promoted landing area. Parquet files remain after a successful load —
they are disposable residue, not the source of truth:

```bash
gcloud storage ls --recursive "gs://$BUCKET/$RAW_DATASET/" | tail -30
```

### 2.4 dlt pipeline state — locally

Local runs keep their working dir under `~/.dlt/pipelines/<pipeline>`;
`info` prints the state including each resource's incremental
`last_value` (the watermark), `trace` the last run's timings and row
counts:

```bash
uv run dlt pipeline payment_v2 info
uv run dlt pipeline payment_v2 trace
ls ~/.dlt/pipelines/
```

### 2.5 dlt pipeline state — when it ran as a cloud job

Cloud Run containers are ephemeral: there is no local working dir to
inspect, and the BigQuery state tables (§2.2) **are** the state — dlt
restores from them at job start. To check the execution itself:

```bash
gcloud run jobs executions list --job=ingest-payment-v2 --region=$REGION --project=$DEV --limit=5

gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="ingest-payment-v2"' \
  --project=$DEV --freshness=1d --limit=100 --order=desc \
  --format='value(textPayload)'
```

And if the run came from the daily workflow:

```bash
gcloud workflows executions list litecore-daily --location=$REGION --project=$DEV --limit=5
gcloud workflows executions describe <EXECUTION_ID> \
  --workflow=litecore-daily --location=$REGION --project=$DEV
```

## 3. Run the dbt transform (canonical payments + the surviving AML model)

Every model in the project — staging aside — is a `run_date`-keyed
microbatch log (`lookback: 0`, `full_refresh: false`, `begin:
2026-07-01`): the canonical payments layer
(`int_payments__daily_activity`, `payments_daily_summary`, dataset
`$CORE_DATASET`) and `aml_merchant_features` (dataset `$AML_DATASET`)
share the exact same shape — see `docs/dbt-primer.md` §5. `dbt build`
includes the tests; use `dbt run` to skip them. **`--event-time-end` is
EXCLUSIVE** — one day is `[D, D+1)`.

```bash
# a) Daily run — the scheduled shape, builds every model above in one
#    pass. Table exists → builds only the current run_date batch
#    (lookback=0). First-ever run (empty dataset) → builds every day
#    from begin (2026-07-01) to today.
dbtx build

# b) One specific day (here: 2026-07-16 only)
dbtx build --event-time-start "2026-07-16" --event-time-end "2026-07-17"

# c) Backfill a window (here: Jul 10–15 inclusive)
dbtx build --event-time-start "2026-07-10" --event-time-end "2026-07-16"

# d) One model only, with its upstreams (staging views + intermediate)
dbtx build --select +payments_daily_summary --event-time-start "2026-07-16" --event-time-end "2026-07-17"

# e) Compile-only gate (what CI runs; no BQ credentials needed)
uv run pytest apps/app_etl/tests/test_dbt_parse.py
```

Reruns of an already-built day rewrite **only** that day's partition
(verified) — safe to repeat. Do not backfill a day whose *raw* data
wasn't ingested yet: the model would be computed over a partial extract.
Order is always ingest → transform, the workflow's guarantee; manual runs
inherit it.

**In the cloud** (job env pins the dataset vars — `gcloud run jobs
describe transform-aml` is the source of truth for what it targets; the
job name predates the payments layer and now runs the whole project, not
just AML — a rename is tracked as a to-do, not yet done):

```bash
gcloud run jobs execute transform-aml --region=$REGION --project=$DEV --wait
```

The cloud job runs the plain daily shape. Run backfills from the laptop —
per-execution arg overrides exist (`gcloud run jobs execute --args=…`)
but check `describe` for the command/args split before relying on them.

## 4. Verify the dbt build in BigQuery

Quick dbt-native peek at one model (`ref()` resolves against whatever
dataset `.env` points at; note `dbt show` takes no event-time flags —
those belong to `build`/`run` only):

```bash
dbtx show --inline "select * from {{ ref('payments_daily_summary') }} where run_date = date '2026-07-16'" --limit 20
```

The full checks, via bq — canonical payments layer (`$CORE_DATASET`):

```bash
# Intermediate coverage: one row-set per run_date, append-only across days.
# Gaps in the run_date sequence = days never evaluated.
bq --project_id=$DEV query --nouse_legacy_sql "
select run_date, count(*) as operation_rows
from \`$DEV.$CORE_DATASET.int_payments__daily_activity\`
group by run_date
order by run_date desc
limit 14"

# Mart: merchant × day volume for one day
bq --project_id=$DEV query --nouse_legacy_sql "
select merchant_id, payment_count, operation_count, total_amount_minor
from \`$DEV.$CORE_DATASET.payments_daily_summary\`
where run_date = date '2026-07-16'
order by total_amount_minor desc
limit 20"

# Invariant: one row per (merchant_id, run_date) in the mart (0 rows expected)
bq --project_id=$DEV query --nouse_legacy_sql "
select merchant_id, run_date, count(*) as n
from \`$DEV.$CORE_DATASET.payments_daily_summary\`
group by merchant_id, run_date
having n > 1"
```

The one surviving AML model (`$AML_DATASET`) — same shape, different
dataset:

```bash
bq --project_id=$DEV query --nouse_legacy_sql "
select run_date, count(*) as merchant_rows
from \`$DEV.$AML_DATASET.aml_merchant_features\`
group by run_date
order by run_date desc
limit 14"
```

## 5. End-to-end via the workflow

The daily workflow (`litecore-daily`) currently runs the dbt transform
alone — the ingest stage is commented out until the per-database Cloud
Run Jobs exist (§1's cloud form), and the AML alert-export stage that
used to run after transform was removed along with the alert-mart stack
(`apps/app_etl/workflows/daily_pipeline.yaml`). Scheduler attach is
deliberately still pending — trigger manually:

```bash
gcloud workflows run litecore-daily --location=$REGION --project=$DEV
gcloud workflows executions list litecore-daily --location=$REGION --project=$DEV --limit=5
```

No retry policy in v1: fix the cause, run it again.

---

## 6. ⚠️ Destructive operations

Rules of engagement: `echo $DEV $RAW_DATASET $CORE_DATASET $AML_DATASET`
**before every command in this section** and read what it says. Nothing
here may target `raw_litecore`, `core`, or `aml` (the promoted names)
casually — if the echo shows them, stop and get a second pair of eyes.
Prefer `bq rm` *without* `-f` so it prompts.

### 6.1 Reset an ingestion pipeline (the right way)

`--refresh` maps to dlt `drop_resources`: drops the pipeline's
destination tables **and** its persisted cursor state together, then
re-extracts from EPOCH. This is the only correct full-reload path, and
also what's needed whenever a create-time-only BQ property (partitioning,
clustering) changes:

```bash
uv run python -m app_etl.ingestion.payment_v2 --refresh
```

**Never** manually `bq rm` a raw table *instead* — that leaves the
watermark behind in `_dlt_pipeline_state`, and the next run silently
skips all history before the stale cursor. If a table was already
dropped by hand, running the pipeline with `--refresh` afterwards still
heals it.

### 6.2 Drop a single raw table

Only as a prelude to an immediate `--refresh` run (see above), or for a
table that is being retired entirely:

```bash
bq --project_id=$DEV rm -t "$DEV:$RAW_DATASET.payments"
```

### 6.3 Drop and re-bootstrap a whole test dataset

`-r` deletes every table in it, including the `_dlt_*` state tables —
every pipeline restarts from EPOCH on its next run (which is exactly the
point of a re-bootstrap). Dataset ACLs die with the dataset:

```bash
bq --project_id=$DEV rm -r -d "$DEV:$RAW_DATASET"          # prompts; add -f only when scripted
bq --project_id=$DEV mk --location=$REGION -d "$DEV:$RAW_DATASET"
```

Then re-grant `sa-app-etl` WRITER on the new dataset (grants are tracked
in `docs/provisioning.md` / `docs/iac-port.md` §3):

```bash
bq show --format=prettyjson "$DEV:$RAW_DATASET" > /tmp/ds.json
# edit /tmp/ds.json: add {"role": "WRITER", "userByEmail": "'$SA'"} to "access"
bq update --source /tmp/ds.json "$DEV:$RAW_DATASET"
```

Same recipe for `$CORE_DATASET` and `$AML_DATASET`.

### 6.4 Reset a run_date-keyed microbatch log

`full_refresh: false` is enforced in every model's config, so
`dbt build --full-refresh` will not do this for you — resetting means
dropping the incremental table(s) and rebuilding from `begin`. If a
downstream model reads a table's own earlier run_dates (rolling-window
features, cooldown-style logic), rebuild the **whole history from
2026-07-01**, not a partial window — a mid-history rebuild would compute
those against missing rows. None of the current models do this yet
(`payments_daily_summary` and `aml_merchant_features` are both
independent per-day aggregates), but the rule stands for whatever lands
next.

```bash
# Canonical payments layer
for t in int_payments__daily_activity payments_daily_summary; do
  bq --project_id=$DEV rm -f -t "$DEV:$CORE_DATASET.$t"
done

# The one surviving AML model
bq --project_id=$DEV rm -f -t "$DEV:$AML_DATASET.aml_merchant_features"

TOMORROW=$(date -v+1d +%F)   # macOS; linux: date -d tomorrow +%F
dbtx build --event-time-start "2026-07-01" --event-time-end "$TOMORROW"
```

### 6.5 Purge stale dlt staging files from GCS

Staging parquet under `gs://$BUCKET/<dataset>/` is disposable **after**
its load completed (check `_dlt_loads` status 0 first, §2.2; deleting
under a load in flight breaks it). The bucket also still holds stale
prefixes from retired layouts (`docs/iac-port.md` §3):

```bash
gcloud storage rm --recursive "gs://$BUCKET/$RAW_DATASET/"
```

### 6.6 Reset local dlt working state only

Harmless to the destination (state is restored from BigQuery on the next
run) — useful when a local working dir is corrupted or you want a clean
`trace`:

```bash
rm -rf ~/.dlt/pipelines/payment_v2
```
