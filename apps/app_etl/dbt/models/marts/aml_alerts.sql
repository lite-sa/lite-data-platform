{{ config(materialized="view") }}

-- Cross-scenario alert stream, one `union all` branch per scenario table.
-- Every scenario table is column-identical to this spine (the split is
-- about per-scenario reruns/begin and export units, not columns — see
-- docs/aml-alert-design.md, revised 2026-07-16); scenario-specific data
-- lives only inside the `evidence` JSON (fixed rule/context shape,
-- scenario-owned content), so this view is self-sufficient for case-level
-- reads. The exporter reads the scenario tables directly.

select
    scenario_id,
    alert_id,
    scenario_name,
    target_level,
    target_id,
    run_date,
    evaluated_at,
    evidence,
    suppression_period_start,
    suppression_period_end,
    suppressed_by_alert_id,
    is_suppressed
from {{ ref('aml_alerts_008') }}

union all

select
    scenario_id,
    alert_id,
    scenario_name,
    target_level,
    target_id,
    run_date,
    evaluated_at,
    evidence,
    suppression_period_start,
    suppression_period_end,
    suppressed_by_alert_id,
    is_suppressed
from {{ ref('aml_alerts_014') }}

union all

select
    scenario_id,
    alert_id,
    scenario_name,
    target_level,
    target_id,
    run_date,
    evaluated_at,
    evidence,
    suppression_period_start,
    suppression_period_end,
    suppressed_by_alert_id,
    is_suppressed
from {{ ref('aml_alerts_015') }}
