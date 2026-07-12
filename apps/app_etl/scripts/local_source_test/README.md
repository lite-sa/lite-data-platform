# Local source test (disposable)

Proves the dlt Postgres -> GCS -> BigQuery wiring against your real
`lite-data-dev` resources, without waiting on the real read-replica
connection (still unresolved — see `docs/provisioning.md`). The source is a
local Docker Postgres seeded with the real shape of 4 LiteCore tables:

- `payment_v2.payments`, `payment_v2.payment_operations` (payment-v2-service)
- `user.merchants` (user-service)
- `business_management.business_entities` (business-management-service)

Everything here is throwaway. Delete this whole folder once the real
connection exists and step 2 of the roadmap starts for real.

## Run

```bash
cp .env.example .env          # at the repo root; fill in — .env is gitignored
cd apps/app_etl/scripts/local_source_test
docker compose up -d          # starts Postgres, applies seed.sql automatically
cd -                          # back to the repo root
gcloud auth application-default login   # once per workstation, needs access to lite-data-dev

uv run python apps/app_etl/scripts/local_source_test/pipeline.py
```

`pipeline.py` now just runs the real `app_etl/ingestion/` pipelines in
sequence; config comes from the repo-root `.env` (loaded by
`Settings.from_env()` via python-dotenv — shell-exported variables always
win). Keep `BQ_DATASET_RAW=raw_test` set so nothing lands in
`raw_litecore`. The `+psycopg` marker in `PG_DSN` matters: it tells
SQLAlchemy to use the `psycopg[binary]` (v3) driver already in this
workspace's dependencies, instead of `psycopg2`, which isn't installed.

## Verify

- BigQuery console, `lite-data-dev` project: dataset `raw_test` should have 4 tables with a few rows each.
- `gs://lite-data-dev-raw/raw_test/` should have the staged parquet files
  (the staging prefix mirrors `BQ_DATASET_RAW`).

## Teardown

```bash
cd apps/app_etl/scripts/local_source_test
docker compose down                                    # wipes local Postgres, no volume to clean up

bq rm -r -f -d lite-data-dev:raw_test                   # drop the test dataset
gsutil -m rm -r gs://lite-data-dev-raw/raw_test          # delete the staged files
rm -rf ~/.dlt/pipelines/{payments,payment_operations,merchants,business_entities}
rm -rf ~/.dlt/pipelines/pg_source_smoke_test*            # runs from before the port
```

## Re-create 

```bash
bq mk --project_id=lite-data-dev --location=me-central2 --dataset raw_test

```

## Connection configuration the real pipeline will need

This is the actual gap to hand to the platform/backend team — not solved by
this test, only worked around:

1. **This is not one connection — it's (at least) three.** `payments` /
   `payment_operations`, `merchants`, and `business_entities` are owned by
   three separate services (`payment-v2-service`, `user-service`,
   `business-management-service`). Unless backend confirms these share one
   physical Postgres instance, expect **three separate DSNs / three read
   replicas**, not one — `docs/provisioning.md` currently assumes a single
   replica and should be corrected once this is confirmed.
2. For each source DB: a **read-only role** (`SELECT` only, on exactly the
   tables being pulled), password in **Secret Manager**, not env vars in
   plaintext.
3. **Network path** from `lite-data-dev` (Cloud Run Jobs, later) into
   whatever VPC each service's DB lives in — Shared VPC, peering, or a
   bastion, platform team's call. This is still an open question in
   `docs/provisioning.md`.
4. DSN shape once granted: `postgresql+psycopg://<ro_user>:<password>@<private_ip_or_host>:5432/<db_name>`
   — swap into `PG_DSN` (or, once there are 3, into per-pipeline env vars);
   the real `app_etl/ingestion/` pipelines pick it up unchanged, just
   pointed at the real host instead of `localhost`.

## Known gaps in this stand-in (don't over-trust it)

- Local Postgres 16; real source version/replica type unconfirmed.
- Seed data is fabricated, not representative of real volume, skew, or edge
  cases (nulls, malformed JSONB, etc.).
- `business_entities`' PII-adjacent fields (contact email/phone) are
  included here because this is fake local data — the real ingestion will
  need its own deny-by-default column allowlist review for this table and
  `merchants`, same as `docs/schema-management.md` already did for
  `payments`. Nothing here should be read as "this is the approved
  allowlist."
