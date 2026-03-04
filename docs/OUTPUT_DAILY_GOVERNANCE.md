# Output/Artifacts Governance (Daily)

## Goal
- `output/` stores temporary runtime artifacts and daily reports only.
- `data/backtest/v30_backtest.sqlite` stores long-lived V30 backtest/strategy data and model/artifact blobs.
- `artifacts/` is deprecated for persistent strategy assets.
- Production MySQL keeps production/ops tables only; V30 backtest/strategy core tables are removed from MySQL.

## Directory Policy
1. `output/` (temporary + reports)
   - Keep:
     - `output/utrbe_prod_daily`
     - `output/utrbe_compare_prod_daily`
     - `output/v31_unified_sentiment`
     - `output/v31_risk_aggregate_default_prod`
     - `output/v31_backtest_eval_default_prod`
     - `output/v31_lowfreq_recovery_weekly`
     - `output/v31_ops_monitor`
   - Everything else under `output/` should be archived or removed.

2. `data/backtest/` (persistent strategy storage)
   - Default SQLite path: `data/backtest/v30_backtest.sqlite`
   - Includes:
     - V30 backtest/strategy tables (`v30_*`, `v31_daily_allocation`, etc.)
     - `v30_model_store` (model bundles)
     - `v30_artifact_file_store` (archived file artifacts / baseline json)

## Daily Required File Outputs
These are registered into `v31_output_artifact_registry_daily`:
1. `output/v31_ops_monitor/summary.json`
2. `output/v31_ops_monitor/UTRBEV3_daily_report_YYYY-MM-DD.md`
3. `output/v31_ops_monitor/strategy_120d.png`
4. `output/v31_ops_monitor/strategy_120d_YYYYMMDD.png`
5. `output/v31_unified_sentiment/latest_unified_sentiment.json`
6. `output/v31_unified_sentiment/summary.json`
7. `output/v31_risk_aggregate_default_prod/latest_allocation.json`
8. `output/v31_risk_aggregate_default_prod/summary.json`
9. `output/v31_backtest_eval_default_prod/summary.json`
10. `output/v31_lowfreq_recovery_weekly/summary.json`

## Daily DB Tables (Core)
Production MySQL:
1. `sentiment_daily_unified`
2. `v31_lowfreq_recovery_summary`
3. `v31_lowfreq_recovery_events`
4. `v31_ops_monitor_snapshot`
5. `v31_ops_monitor_health_checks`
6. `v31_ops_monitor_episodes`
7. `v31_ops_monitor_summary_archive`
8. `v31_ops_monitor_report_archive`
9. `v31_output_artifact_registry_daily`

V30 SQLite (`data/backtest/v30_backtest.sqlite`):
1. `v30_features_daily`
2. `v30_structural_labels_daily`
3. `v30_shock_labels_daily`
4. `v30_structural_full_predictions_daily`
5. `v30_shock_full_predictions_daily`
6. `v31_daily_allocation`
7. `v30_backtest_daily`
8. `v30_model_store`
9. `v30_artifact_file_store`

## Runtime Command
Daily main command:

```bash
python scripts/run_v31_prod_daily.py
```

No-arg default day rule:
- Before 16:00 ET: run previous trading day.
- At/after 16:00 ET: run current trading day.

Optional persistent asset root override:

```bash
python scripts/run_v31_prod_daily.py --backtest-db-path data/backtest/v30_backtest.sqlite --artifact-db-path data/backtest/v30_backtest.sqlite
```

## Enforcement Notes
1. `run_v31_ops_monitor.py` writes daily report + summary and upserts archives.
2. Missing required daily artifacts should fail the run when DB upsert is enabled.
3. Intraday report text must be clearly marked as intraday snapshot when applicable.
