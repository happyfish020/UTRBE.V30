# V31 Ops Monitor Runbook

Version: v1.3.1  
Updated: 2026-02-25

## Goal
Provide a read-only operational health check after each production run.

## Prerequisite (Important)
V31 report can run independently, but if you want the report to include `V2` signal-state section, you must run `V2` daily output first and provide `latest_state.json`.

## Recommended Daily Command (Production)
```bash
python scripts/run_v31_prod_daily.py
```

This one command will:
1. If no CLI args are provided, run single-day only on last trading day (`start=end=last trading day`).
2. Refresh UTRBE daily data from DB.
3. Run V31 risk pipeline.
4. Generate ops report and strategy chart.
5. Auto-include `V2` state block when `latest_state.json` exists.
6. Write one-run-one-log file: `output/logs/UTRBEV30_log_YYYY-MM-DD.log`.

## Manual Command (Ops Monitor Only)
```bash
python scripts/run_v31_ops_monitor.py \
  --backtest-summary-json output/v31_backtest_eval_default_prod/summary.json \
  --risk-summary-json output/v31_risk_aggregate_default_prod/summary.json \
  --allocation-table v31_daily_allocation \
  --backtest-daily-table v30_backtest_daily \
  --reference-summary-json output/v31_backtest_eval_hardgate_gatepass/summary.json \
  --lowfreq-summary-json output/v31_lowfreq_recovery_weekly/summary.json \
  --v2-latest-state-json output/utrbe_prod_daily/latest_state.json \
  --output-dir output/v31_ops_monitor
```

## Outputs
- `output/logs/UTRBEV30_log_YYYY-MM-DD.log`
- `output/v31_ops_monitor/summary.json`
- `output/v31_ops_monitor/report.md`
- `output/v31_ops_monitor/daily_report.md`
- `output/v31_ops_monitor/UTRBEV3_daily_report_YYYY-MM-DD.md`
- `output/v31_ops_monitor/strategy_120d.png`

DB auxiliary tables (default on):
- `v31_ops_monitor_snapshot`
- `v31_ops_monitor_health_checks`
- `v31_ops_monitor_episodes`

Disable DB upsert only for debugging:
```bash
python scripts/run_v31_ops_monitor.py --skip-db-upsert ...
```

## SQL Audit
- 快速审计 SQL 模板见：`docs/V31_OPS_MONITOR_SQL_QUERIES.md`

## Health Rules
1. `dd_reduction >= 0.40`
2. `cagr_impact < 0.02`
3. average trigger density in `[2, 8]` per year
4. average trigger episode duration `< 15` trading days
5. no full liquidation days (`final_allocation <= 0.05`)

## Action Rules
1. If any of rule 1/2 fails, block release and re-run Step4 status check.
2. If only trigger density/duration fails, keep config frozen and investigate data quality first.
3. If drift vs reference exceeds:
   - `|cagr_impact_delta| > 0.005` or
   - `|dd_reduction_delta| > 0.03`,
   open incident note and require manual review before next deploy.
