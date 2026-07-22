# dbt primer — concepts, commands, and decisions for this repo

Session notes from a hands-on dbt walkthrough (2026-07-12, extended
2026-07-15 and 2026-07-22), written for someone coming from pandas/PySpark. Everything here is grounded in
`apps/app_etl/dbt/` — open the referenced files alongside.

## 1. What dbt is (and isn't)

dbt is **not an execution engine** — it never touches data. Pandas and
Spark compute; dbt compiles. It is a **compiler + build system for SQL**:

1. renders Jinja-templated `.sql` files into plain SQL,
2. derives a dependency DAG from `ref()`/`source()` calls,
3. submits `CREATE TABLE/VIEW ... AS SELECT` to BigQuery in DAG order.

BigQuery does all the compute. "SQL with referencing" is genuinely ~80%
of it; the other 20% (DAG, materializations, tests, environments) is what
makes the 80% production-grade.

**Proof by inspection**: compare any model with its compiled output —
`models/marts/payments/payments_daily_summary.sql` (Jinja: `{{ ref(...) }}`,
`{{ config(...) }}`) vs `target/compiled/app_etl/models/marts/payments/
payments_daily_summary.sql` (fully-qualified table name, a literal date
range dbt injected for the batch). That rendering step is all dbt "is".

### Mapping for a pandas/PySpark brain

| PySpark / pandas                     | dbt                                        |
|--------------------------------------|--------------------------------------------|
| DataFrame variable                   | one `.sql` model file                      |
| referencing `df_a` in `df_b`         | `{{ ref('model_a') }}`                     |
| `spark.read.table(...)`              | `{{ source('litecore', 'payments') }}`     |
| write mode / `.cache()`              | materialization (`view`/`table`/`incremental`) |
| `df.show()`                          | `dbt show --select model` / `--inline`     |
| assertions                           | `dbt test` (a test = SELECT that must return 0 rows) |
| Spark engine                         | BigQuery                                   |

**The one conceptual trap**: Jinja runs at **compile time**, before any
data exists. There is no runtime logic — no loops over rows, no branching
on values. `{{ var("local_timezone") }}` is string substitution producing
a static SQL file.

## 2. When dbt vs pandas / bigframes / PySpark

- **dbt**: data already in BQ, set-based logic (joins, aggregations,
  windows), output is tables/views for SQL consumers. Data never leaves
  the warehouse; compute is BQ slots.
- **pandas / bigframes**: iterative algorithms, ML feature engineering
  needing Python libs, model scoring, APIs, exploration in notebooks.
- **PySpark**: no role here — it would mean Dataproc clusters doing work
  BigQuery already does.

**Decision (2026-07-12, broadened 2026-07-22)**: the raw→canonical
transform path stays in dbt. First decided for the AML raw→features→
alerts pipeline specifically; the same argument now governs the payments
canonical layer too. The deciding argument is not tooling preference —
it's that **business logic in SQL is an auditable artifact**: anyone
(analyst, auditor, another engineer) can read `payments_daily_summary.sql`
or `aml_merchant_features.sql` and verify it against what it claims to
compute; a PR diff shows exactly what changed and when. Bigframes inverts
that (readable Python, unreviewed generated SQL — same failure mode as
debugging Spark optimizer output). Long-term goal: the people who own a
domain's rules edit its model SQL directly. Bigframes owns everything
downstream and exploratory (notebooks, future ML features). The
deploy-story concern (entry point / Docker) is not a differentiator: a
dbt deployment is a container whose command is `dbt build`, run as a
Cloud Run Job — same shape as the dlt jobs, and `profiles.yml`
(`method: oauth`) already covers Cloud Run SA auth.

Revisit trigger: if in ~6 months the modeling layer is still tiny, one
person owns all of it, and a PR-diff/audit trail never actually mattered
— dbt was over-tooling; the SQL is portable.

## 3. Running dbt in this repo

dbt is installed in the shared workspace venv (the whole workspace is
pinned to Python 3.13 — dbt-core can't run on 3.14 yet), so plain
`uv run dbt` works from the repo root. dbt still needs pointing at its
project/profiles directory:

```bash
uv run --env-file .env dbt <cmd> \
  --project-dir apps/app_etl/dbt --profiles-dir apps/app_etl/dbt
```

Shorthand:

```bash
alias dbtx='uv run --env-file .env dbt'
export DBT_PROJECT_DIR=apps/app_etl/dbt DBT_PROFILES_DIR=apps/app_etl/dbt
```

### The commands that matter

```bash
dbtx build            # materialize models AND run tests, DAG order — the daily driver
dbtx run              # models only
dbtx test             # tests only
dbtx compile           # render Jinja → target/compiled/ (no writes to BQ)
dbtx parse            # syntax/DAG check, no BQ connection (what CI runs)
dbtx show --select stg_litecore__payment_operations --limit 20   # df.head()
dbtx show --inline "select count(*) from {{ ref('payments_daily_summary') }}"  # ad-hoc with refs
dbtx retry            # re-run only failed + skipped nodes from last run
dbtx build --full-refresh   # rebuild incremental models from scratch — NEVER on a run_date-keyed log, see §5
dbtx docs generate && dbtx docs serve   # lineage graph on localhost:8080
```

`dbt show` is **read-only**: it compiles the model's SELECT from the
**file on disk** (Jinja rendered), wraps it in `select * from (…) limit N`,
runs it as an ephemeral query, and prints the rows — nothing is written.
So an edited-but-not-rebuilt model previews the *edit*, while querying
the deployed object in BQ still serves the *last build*. File state vs
deployed state — `show` is the former, which is what makes it the
dev-loop tool.

### Node selection

```
--select payments_daily_summary     # one model (+ its tests under `build`)
--select marts                      # a folder
--select +payments_daily_summary    # model and everything upstream
--select stg_litecore__payments+    # model and everything downstream
--select state:modified+            # changed vs a previous manifest (CI pattern)
--exclude ...                       # subtract from any of the above
```

`build --select <model>` runs that model plus whatever tests are declared
against it in its `_*.yml` (or a singular test under `tests/` that
`ref()`s it) — nothing else in the DAG.

### Failure semantics (the PySpark comparison inverts)

Failures are **per-node with DAG-aware skipping**: a failed model marks
only its *descendants* SKIP; independent branches build to completion.
`dbt retry` resumes from the failure. A monolithic .py script needs
hand-written try/except orchestration to match this. When a domain grows
past one scenario/mart, make **one model per scenario feeding a thin
union view** — the AML alert stack did exactly this before it was
demoted (recoverable from git history, `DAT-15-aml-p0`), and the payments
marts under `models/marts/payments/` follow the same convention as more
land there. `dbt build` blocking downstream of a failed *test* is
deliberate (don't build on bad data); use `severity: warn` on a test to
demote it.

## 4. Project anatomy (`apps/app_etl/dbt/`)

| Path | Role |
|---|---|
| `dbt_project.yml` | Manifest: name, paths, folder-level materializations/schema routing, `vars:` (`local_timezone` used stack-wide; `aml_night_*` feed the one surviving AML model only) |
| `profiles.yml` | Connection: ADC oauth, project/dataset from env vars, me-central2. Separate from the project on purpose; committed env-var-driven |
| `models/staging/` | Thin 1:1 views over raw: rename, cast, derive `created_at_local`. Nothing else. Shared by every downstream domain, AML included |
| `models/intermediate/payments/` | Canonical payments building block: `int_payments__daily_activity` (operation-grain, run_date-keyed) |
| `models/intermediate/aml/` | The one surviving piece of the AML stack: `aml_merchant_features` (merchant × run_date feature log) — routed to the `aml` dataset via a folder-level `+schema` override; everything else in the project defaults to `core` |
| `models/marts/payments/` | Business-grain payments tables: `payments_daily_summary` (merchant × run_date volume) — the template for future non-AML marts |
| `models/staging/_sources.yml` | Declares raw dlt-landed tables as sources — the DAG's entry points |
| `models/*/_*.yml` (one per folder) | Schema tests (`unique`, `not_null`) + column docs |
| `macros/` | Jinja functions returning SQL text: `column_or_null` (sparse-column guard), `generate_schema_name` (routes `+schema` overrides to an exact dataset name instead of BigQuery-adapter's default `target_dataset_customschema` concatenation) |
| `tests/` | Singular tests: hand-written SELECTs that must return 0 rows |
| `target/compiled/` | Rendered pure SELECTs (build artifact) |
| `target/run/` | Same SQL wrapped in the executed DDL — read this to see materializations concretely |
| `target/manifest.json` | Parsed DAG; feeds `dbt docs` and `state:` selection |
| `pyproject.toml`, `uv.lock`, `.venv/` | Not dbt — the standalone py3.13 pin |

Standard dirs not present (unused): `seeds/` (CSVs as tables),
`snapshots/` (built-in SCD2), `analyses/`.

## 5. Views vs tables vs incremental

**BigQuery**: a table stores data (pay storage, scan its bytes); a view
stores only SQL text, macro-expanded into every query — never stale,
never saves compute.

**dbt**: materialization = which DDL wraps your SELECT.

- `view` → `create or replace view`. Staging default here.
- `table` → `create or replace table ... as select` (CTAS) — full
  recompute, **atomic replace**. Marts folder default here, though
  every current mart overrides it with `incremental` (see below) —
  this is the write disposition: repeated `dbt build` is idempotent
  (dlt analogy: `replace`). A failed run leaves the previous table
  intact.
- `incremental` → first run creates; later runs merge/insert only new
  data (dlt analogy: `append`/`merge`). On BQ,
  `incremental_strategy="insert_overwrite"` + `partition_by` replaces
  exactly the partitions present in the new result — idempotent per-day
  rebuild.
- `ephemeral` → no BQ object; inlined as a CTE downstream.

**Terminology trap**: `materialized: view` does *not* mean "not
materialized" — every model except `ephemeral` creates a real, queryable
BQ object. Mental model: `ephemeral` = no object; `view` = object without
data; `table` = object with data. Check `INFORMATION_SCHEMA.TABLES` in
whichever dataset you're pointed at (`core`/`core_test` by default,
`aml`/`aml_test` for the one surviving AML model) to see them concretely.
Staging views are queryable from anywhere
(`bpd.read_gbq("select * from …stg_litecore__payment_operations")`
works like any table) — but each read re-executes their SQL and scans
the underlying raw table's bytes.

Folder defaults in `dbt_project.yml` (`staging: view`, `marts: table`);
per-model `config()` overrides win over the folder default — that's how
`payments_daily_summary` and `aml_merchant_features` get their
microbatch/date partitioning despite living under folders whose default
materialization says otherwise.

### Incremental sketch (for when it's time)

```sql
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "activity_date", "data_type": "date"},
        cluster_by=["merchant_id"],
    )
}}
select ...
from {{ ref('stg_litecore__payment_operations') }}
{% if is_incremental() %}
where date(created_at_local) >= date_sub(
    current_date('{{ var("local_timezone") }}'), interval 2 day)
{% endif %}
```

> **Superseded 2026-07-15** — rolling-window scenarios (already in
> scope) flipped the marts to run_date-keyed evaluation logs; see
> "Rebuildable table vs generation record" below. Kept for the
> reasoning trail.

**Decision (2026-07-12): stay kill-and-fill for now.** Reasons (as they
stood for the AML stack this was first decided for):

1. **The incremental foot-gun**: changing a var or the SQL does *not*
   invalidate built partitions — past days silently keep old logic until
   `--full-refresh`. With an AML rule mapping still unconfirmed at the
   time, full rebuild guaranteed the whole table reflected current code.
2. The features model's contract ("new scenarios add columns here") means
   frequent schema changes during scenario buildout — each one a
   full-refresh event anyway.
3. Raw is still the local stand-in; first real-replica day is a
   `--full-refresh` regardless.

**Switch when** (checklist): rule mapping confirmed ∧ real replica
flowing ∧ feature columns stable ∧ (bytes-scanned cost visible ∨ a row
is under real investigation — the freeze-history/audit argument
activates). The consumption contract doesn't wait for this: the table is
already day-partitioned, so day-scoped exports work identically under
either build mode.

### Under the hood: `partition_by` / `cluster_by` (2026-07-15)

`partition_by` compiles to `PARTITION BY` in the DDL — the behavior is
pure BigQuery. The Hive/Spark mental model ("directory of files per
partition") is right in spirit, wrong in mechanics: BQ storage is fully
managed (no visible files); each partition is a physically separate set
of columnar blocks. Three things fall out of that separation:

- **Pruning**: a filter on the partition column scans only matching
  partitions, and on-demand billing is bytes *scanned* — a day-scoped
  query over a year of features bills one day.
- **Atomic partition-level DML**: a partition is replaceable as a unit —
  the primitive `insert_overwrite` builds on, also addressable directly
  via the decorator syntax (`payments_daily_summary$20260714`).
- **Per-partition storage lifecycle**: partitions untouched for 90 days
  drop to long-term storage pricing automatically.

(The raw tables already work this way — dlt created them day-partitioned
on `updated_at`.)

`cluster_by` is the second, finer level: *within* each partition, rows
are kept physically sorted/co-located on the cluster columns (≤4).
Partitioning prunes on one low-cardinality time column; clustering
prunes blocks on high-cardinality columns you can't partition by —
`where merchant_id = 'X'` reads only X's blocks instead of the whole
partition. Background re-clustering is automatic and free. Footnotes:
the dry-run cost estimate ignores clustering (it's an upper bound;
actual billed bytes come in lower), and below ~1GB/table the effect is
negligible — at our volume it's future-proofing that costs nothing.

### Incremental strategies and `--full-refresh` (2026-07-15)

`incremental_strategy` exists only under `materialized: incremental` —
the other materializations have nothing to configure since they don't
write into an existing object. dbt-bigquery has three:

| Strategy | Mechanics | dlt analogy |
|---|---|---|
| `merge` (default) | `MERGE` on `unique_key` (row-level upsert); without a `unique_key` it degenerates to a plain insert | `merge` / `append` |
| `insert_overwrite` | run the select, collect the distinct partitions present in the result, atomically replace exactly those | `replace`, per partition |
| `microbatch` (dbt ≥ 1.9) | dbt slices the run into per-period batches off an `event_time` column + `lookback` config | — |

`merge` pays MERGE compute — the same cost rejected on the ingestion
side; `microbatch` is the reprocess-window pattern productized, not
worth reaching for before plain `insert_overwrite` hurts. (There is no
named `append` strategy on BigQuery — merge-without-unique_key is how
you get it.)

`dbt build --full-refresh` is the `dlt --refresh` analog:
`is_incremental()` compiles to false, the incremental filters drop out,
and the table is rebuilt CTAS. One asymmetry worth internalizing: dlt
needs refresh partly because it has a *persisted cursor*
(`_dlt_pipeline_state`) that can desync from the data. dbt-incremental
has none — with `insert_overwrite` the watermark is
`_dbt_max_partition`, derived at runtime from the max partition already
in the destination table. The data *is* the state; the stale-cursor
failure class (cf. the retired-pipeline refresh caveat on the dlt side)
doesn't exist here.

### The trailing-window trap (2026-07-15)

The sketch above is single-filter and safe only because the features
are same-day grain — each `activity_date` needs only same-day input.
The moment a trailing-window feature lands ("ops in the last 7 days"),
an incremental run needs **two different filters**: input reaching back
to `oldest_rebuilt_day − 6 days`, output clipped to
`>= oldest_rebuilt_day`. Miss the clip and `insert_overwrite` silently
replaces a correct old partition with one computed from a truncated
window — no error, wrong counts. This is *the* classic
incremental-aggregation bug, a tax on every new feature while
definitions churn — by itself a sufficient reason to stay kill-and-fill
during scenario buildout.

### Rebuildable table vs generation record (2026-07-15)

Discussion outcome, re-affirming the 2026-07-12 decision. The framing
that settles "shouldn't incremental give us a clean generation date?":
**a table can be safely rerunnable, or it can be the generation record —
not both.** Three shapes:

1. **Kill-and-fill keyed by `activity_date`**: the whole table always
   means "current rules over current knowledge". Rebuilds are harmless
   because the table never claimed to be a record.
2. **Incremental `insert_overwrite` keyed by `activity_date`**: same
   schema, same contract — a pure compute saver, not a semantics
   change. It does *not* produce a generation date (a restamped
   `evaluated_at` column is meaningless whenever a partition
   reprocesses), and it quietly muddies shape 1: after a rule/var change
   only partitions inside the reprocess window reflect the new logic —
   mixed-rule-version history unless every change triggers
   `--full-refresh`, which is kill-and-fill again.
3. **Append-only evaluation log keyed by `run_date`** (knowledge date):
   each run reads one complete trailing input window and writes only
   today's partition — no clipping problem by construction. What
   follows from it:
   - Mechanically still `incremental` + `insert_overwrite` (or, as
     adopted here, `microbatch`), on `run_date`: a same-day rerun must
     replace today's partition, not append duplicates.
   - `--full-refresh` becomes *semantically forbidden*: a rebuild
     re-evaluates old windows with today's knowledge and today's code,
     stamped with historical run_dates — rewriting the audit log. New
     columns land as NULLs back-history, and that's correct, not a gap.
   - Rolling-window rules re-fire by construction: one Tuesday burst
     breaches "7-day count > 100" for up to seven consecutive
     run_dates, so suppression state (scenario × merchant within N
     days) arrives early wherever that matters.
   - Bonus: run_date-keyed trailing features are point-in-time correct —
     exactly the shape future ML training sets need.

**Decision: one shape, the run_date-keyed evaluation log, for every
model in this project**, not just the AML one it was first designed for.
This is the canonical transaction-monitoring/batch-processing design
(processing-date batch; grain = entity × run date — how Actimize /
Oracle FCCM work for detection, and equally how a plain daily activity
rollup should work, which is why dbt grew `microbatch`). It's now real
in two shapes, both worth reading side by side:

- `models/intermediate/aml/aml_merchant_features.sql` — reads
  **unfiltered** staging refs (they declare no `event_time`) and derives
  its own trailing-window date logic by hand from `model.batch`.
- `models/marts/payments/payments_daily_summary.sql` — refs
  `int_payments__daily_activity`, which itself declares
  `event_time="run_date"`, so dbt wraps that ref in the current batch's
  window **automatically**; the mart needs no date logic at all. This is
  the simpler, more common case — reach for it whenever the thing you're
  reading is itself already run_date-keyed.

The accepted bill (the same one Adyen-style PySpark setups pay):
logical-date plumbing (dbt has no `execution_date` — microbatch or a
`run_date` var supplies it); late data is never re-evaluated for past
run_dates (rolling windows partially self-heal, a missed same-day spike
stays missed); the prod table stops being a tuning surface (threshold
experiments = backtest into a scratch dataset — and since raw keeps
every row version, `updated_at <= run_date` approximately reconstructs
knowledge-at-the-time); `--full-refresh` flips from harmless to
destructive; new columns land as NULLs back-history
(`on_schema_change: append_new_columns`).

### Microbatch in practice (2026-07-15)

Every run_date model in this project is built as
`incremental_strategy='microbatch'` (dbt ≥ 1.9; the lockfile has
dbt-core 1.11.12). dbt computes which daily batches to process and runs
**one query per batch**, exposing each batch's window as
`model.batch.event_time_start/end` — the logical date comes from dbt
itself, answering "SQL models are date-agnostic" without threading a
var by hand. The docs' canonical use is event-grain models with
automatic upstream filtering (any `ref()` whose model also declares an
`event_time` gets wrapped in the batch window) — that's exactly the
`payments_daily_summary` case above. A model reading raw staging
instead (no `event_time` there) derives its window by hand, as
`aml_merchant_features` and `int_payments__daily_activity` both do:

```sql
{{ config(
    materialized="incremental",
    incremental_strategy="microbatch",
    event_time="run_date",          -- column in THIS model's output
    begin="2026-08-01",            -- first evaluation date ever
    batch_size="day",
    lookback=0,                    -- never reprocess past run_dates
    partition_by={"field": "run_date", "data_type": "date"},
    cluster_by=["merchant_id"],
) }}

{% set run_date = "date('" ~ model.batch.event_time_start ~ "')" %}

with ops_window as (
    select *
    from {{ ref('stg_litecore__payment_operations') }}  -- unfiltered ref
    where date(created_at_local) = date_sub({{ run_date }}, interval 1 day)
)
select {{ run_date }} as run_date, merchant_id, ...   -- daily aggregates
```

Runtime behavior: the first build generates every batch from `begin`
to today (the history bootstrap — computed from today's raw, honest
caveat); a daily run processes today's batch only; `dbt retry` re-runs
failed batches.

**Backfills are first-class CLI, not an orchestrator loop**:
`--event-time-start/--event-time-end` regenerates exactly those
batches; `--full-refresh` reprocesses from `begin`. Workflows stays a
dumb daily trigger, Airflow gains no new argument, and a backfill is a
deliberate manual command — appropriate, since on a log a backfill
stamps historical run_dates with today's knowledge and code. Optional
honesty upgrade: a `where updated_at <= '{{ model.batch.event_time_end
}}'` cap makes backfilled batches approximate knowledge-at-the-time
(raw keeps every row version). Note microbatch does **not**
auto-catchup by inspecting the destination — a normal run processes
today (+ lookback) regardless of holes; outage recovery is an explicit
backfill command.

Caveats: **`lookback: 0` is load-bearing** — every normal run
processes the current batch plus `lookback` prior ones, and the
default of 1 would rewrite yesterday's partition each morning (exactly
the restamping the log forbids); verified 2026-07-16 in `aml_test`
(INFORMATION_SCHEMA.PARTITIONS before/after a rerun: only the current
partition rewritten, prior partitions untouched). Two BigQuery
gotchas from that spike: `partition_by` must spell out
`"granularity": "day"` (the adapter validates the config key, not the
default), and a leftover table from a previous design counts as "the
relation exists" — dbt then runs only the current batch instead of
bootstrapping from `begin`; drop the old table first. **Batch boundaries are UTC** — Riyadh
is UTC+3 with no DST and the daily run is ~06:45 local = 03:45 UTC, so
dates align; never schedule between 00:00 and 03:00 local. Fallback if
microbatch surprises: `--vars '{run_date: …}'` + plain
`insert_overwrite` — same SQL body, config-level swap.

## 6. Sources and datasets: logical names, physical env vars

Source `name: litecore` is a **logical** label (name the source *system*,
not the dataset); the physical location is `database:`/`schema:` via
`env_var()`. The same source resolves to `raw_test` locally and
`raw_litecore` in prod — naming it after a dataset would be a lie half
the time. Staging models follow `stg_<source>__<table>`.

The read/write asymmetry: what dbt **reads** comes from `_sources.yml`
(`BQ_DATASET_RAW` → `raw_test` locally); what it **writes** to depends on
which folder a model lives in. Two datasets exist on the write side:

- **`core`** (`BQ_DATASET_CORE`, default `core`/`core_test` locally) —
  the connection's home dataset (`profiles.yml`'s `dataset:`). Staging
  views and every canonical, non-AML model (`models/intermediate/
  payments/`, `models/marts/payments/`) land here with no per-model
  config needed.
- **`aml`** (`BQ_DATASET_AML`, default `aml`/`aml_test` locally) — home
  to exactly one surviving model, `aml_merchant_features`, via a
  `+schema` override on the `models/intermediate/aml/` folder in
  `dbt_project.yml`. BigQuery-adapter's default custom-schema behavior
  *concatenates* (`+schema: aml` against target `core` would create
  `core_aml`, not `aml`) — `macros/generate_schema_name.sql` overrides
  that so a custom schema always names the dataset outright.

Chain: `.env` → `--env-file` puts it in the process env →
`env_var()` in YAML. dbt creates each target dataset on first run, with
`location: me-central2` from the profile.

## 7. Macros

Jinja functions returning SQL text, executed at compile time — a Python
function returning a query string. `ref()`, `source()`, `var()`,
`config()` are built-in macros; the "N macros" reported in compile
output is dbt's stdlib (including the DDL wrappers themselves).
Third-party macro libraries (e.g. `dbt_utils`) install via `packages.yml`.

Two house examples:

- `macros/column_or_null.sql` does compile-time **introspection**:
  `adapter.get_columns_in_relation()` runs a metadata query against BQ
  (why `compile` needs a live connection here), then emits either the
  real column or `cast(null as <type>) as <name>` depending on whether
  dlt has materialized the column yet. Same source, different SQL per
  environment — see it rendered in `target/compiled/`.
- `macros/generate_schema_name.sql` overrides dbt's built-in schema-name
  generation (§6): no introspection, just string logic, but it changes
  every model's DDL target, so it's worth reading in full — the
  four-line standard recipe for "a custom schema names a dataset
  outright, it never gets appended to the target's."

## 8. Development workflow

Explore the **data** in the notebook; write the **transformation** in
the model file — and move to the file earlier than feels natural.
Notebook SQL uses physical names, model SQL uses `ref()`/`var()`; every
round-trip between the two risks drift, and dbt's own loop is fast
enough once the question shifts from "what's in this data" to "what
should this model compute."

1. **Explore data** — notebook `read_gbq`, BQ console, or
   `dbtx show --inline`. Explore against **staging views, not raw**:
   they're real queryable objects (§5), and their vocabulary
   (`payment_operation_id`, `amount_minor`, `created_at_local`) carries
   into the model one-to-one instead of re-deriving renames/timezone
   logic twice.
2. **Draft the model file early** — rough SELECT into `models/`, swap
   physical names → `ref()`/`source()`, rule knobs → `var()`. Iterate
   with `dbtx show --select my_model` (preview, nothing written) or
   `build --select my_model` (materialize + test). Dev writes go to
   `core_test` (or `aml_test` for the AML folder), so iterate
   destructively without fear. Team version: dataset per developer via
   `BQ_DATASET_CORE`/`BQ_DATASET_AML` — profiles.yml/dbt_project.yml
   already support it unchanged.
3. **Promote manual checks to tests** — the queries you ran to convince
   yourself (grain unique, no null ids) become YAML tests or singular
   tests. The notebook exploration evaporates; tests are its residue
   (`assert_payments_daily_summary_grain_unique.sql` is exactly this).
4. **PR** — the artifact is a SQL + YAML diff. CI runs `dbt parse` here;
   bigger setups run slim CI (`build --select state:modified+`).
5. **Prod is boring** — the same `dbt build` on a schedule; only env
   vars differ between dev and prod.

`analyses/` (standard dir, not present yet) is the dbt-native
scratchpad: `.sql` that compiles with full `ref()` support but never
materializes — for investigation queries worth keeping, not shipping.

Notebooks sit at the **ends** of the workflow: profiling upstream of
modeling, consuming marts downstream. They validate the model; they
never become it. Concretely, for a new payments feature: profile
operations in the notebook → confirm the discriminator columns and any
thresholds → new column (or model) with `ref()`s from line one →
`dbtx show` until the numbers match the notebook → promote sanity checks
to tests → PR.

## 9. Notebook workflow

SQL in Jupyter: `bpd.read_gbq("...")` (already in use), or install
`bigquery-magics` for `%%bigquery df` SQL cells. Inspecting mart output
needs no dbt — they're real tables (`core_test.payments_daily_summary`).

Running dbt from a notebook (the kernel shares the workspace venv, so
`import dbt` works now, but shelling out keeps dbt's CLI state — logging,
global flags — out of the kernel):

```python
import subprocess
REPO = "/Users/madel/workspace/lite-data-platform"
def dbt(*args):
    r = subprocess.run(
        ["uv", "run", "--env-file", ".env", "dbt", *args,
         "--project-dir", "apps/app_etl/dbt", "--profiles-dir", "apps/app_etl/dbt"],
        cwd=REPO, capture_output=True, text=True)
    print(r.stdout or r.stderr)
```

Worth it for previewing *uncommitted* model changes (`dbt("show", ...)`)
or rebuilding one node without leaving the notebook.

## 10. VS Code (dbt Power User)

Red squiggles across dbt YAML/SQL ≠ broken files (CLI `parse`/`build`
pass) — it's the extension failing to initialize. Two causes:

1. Wrong Python: the extension has to find the venv with dbt installed
   (the workspace root `.venv`) and the in-repo `profiles.yml`. Fixed via
   `.vscode/settings.json` → `dbtPowerUser.dbtPythonPathOverride` +
   `profilesDirOverride`.
2. Missing env vars: `env_var('GCP_PROJECT')` has no default, and a
   GUI-launched VS Code has no shell env. Quit VS Code, then:

   ```bash
   cd ~/workspace/lite-data-platform
   set -a; source .env; set +a
   code .
   ```

If it stays flaky (nested project + env-var profiles is its worst case),
CLI-first is a complete dbt workflow; the extension is preview/lineage
sugar.

## Deferred concepts (learn when needed)

`snapshots` (SCD2), `seeds`, `exposures`, `packages.yml`/dbt_utils,
source freshness checks, and the scenario-per-model + union-view
refactor whenever a domain (payments included) outgrows one mart file.
(`incremental` graduated from this list 2026-07-15 — see §5.)
