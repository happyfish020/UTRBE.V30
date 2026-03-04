# V31 Production Daily Runbook

Version: v1.5.2  
Updated: 2026-03-04

## Goal
Run production daily process from DB close data to final V31 ops report with one command.

## Production Order (Post-Close)
1. DB ingest is complete for the latest trading day (from upstream scraper/ETL).
2. Run V2 daily pipeline (reads DB, writes `latest_state.json` and DB tables).
3. Run V31 production daily (build unified sentiment from DB and upsert to DB, then run full V31 chain).

## One-Command Daily Run
```bash
python scripts/run_v31_prod_daily.py
```

Default SQLite paths:
- `--backtest-db-path data/backtest/v30_backtest.sqlite`
- `--artifact-db-path data/backtest/v30_backtest.sqlite`

Default behavior:
1. If no CLI args are provided, run single-day only: `start=end=last trading day` (not natural day, avoid intraday incomplete bars).
2. If args are provided, use explicit `--start/--end` (and `--end auto` resolves to last trading day).
3. Build unified sentiment from DB and upsert to DB table `sentiment_daily_unified`.
4. Run UTRBE daily pipeline first (DB -> `market_features_daily` + `latest_state.json`, unified sentiment read from DB table).
   - Default sentiment profile: `plus`
   - Default gray switch: `plus_force_reduce_on_signal = ON`
   - Default floor: `plus_force_reduce_min_prob = 0.12`
5. Run V31 full chain (features -> infer -> aggregate -> backtest -> lowfreq -> ops report).
6. Output dated daily report and strategy chart.
7. Write one-run-one-log file: `output/logs/UTRBEV30_log_YYYY-MM-DD.log`.

No CSV dependency in runtime data path (default):
- Upstream and intermediate data bus is DB tables.
- CSV files are optional artifacts only when explicitly enabled.

Storage split (current):
- Production MySQL: production feed + ops monitor tables (`v2_*`, `v31_ops_*`, `v31_lowfreq_*`, etc.)
- V30 SQLite: backtest/strategy core data and model/artifact store (`v30_*`, `v31_daily_allocation`, `v30_backtest_daily`, `v30_model_store`, `v30_artifact_file_store`)

## Dependency
If you want `V2` signal-state block in V31 report, `latest_state.json` from UTRBE output is required.
Unified sentiment layer is DB-first:
- Build script writes `sentiment_daily_unified` (includes `gdelt_risk`, `wiki_views_total` derived features).
- V2 reads unified sentiment from DB table by `--unified-sentiment-table sentiment_daily_unified`.

Market features are also DB-first:
- `build_market_features.py` upserts into `market_features_daily`.
- `run_dualtrack_daily.py` reads market features from `--market-features-table market_features_daily`.
- CSV export of market features is optional and not required in production.
- Gray biotech overlay (new): `IBB`-derived fields are generated when source data exists and mapped to
  `ibb_risk_score` in V31 aggregation (`ibb_risk_weight`, default `0.05` in prod tuning).

V30 feature/label persistence (SQLite-first):
- `run_v30_build_features.py` reads production market features from MySQL (`market_features_daily`) and upserts into SQLite `v30_features_daily`.
- `run_v30_build_labels.py` upserts into SQLite:
  - `v30_structural_labels_daily`
  - `v30_shock_labels_daily`

V30 model storage:
- Default runtime loads model bundles from SQLite `v30_model_store` (`v30_structural_model_prod`, `v30_shock_model_prod`).
- File `pkl` fallback exists for backward compatibility, but file persistence is no longer the primary path.

Low-frequency recovery auxiliary persistence (DB-first):
- `run_v31_lowfreq_recovery.py` upserts into:
  - `v31_lowfreq_recovery_summary`
  - `v31_lowfreq_recovery_events`
- `events.csv` is optional via `--write-csv` (default off).

Ops monitor auxiliary persistence (DB-first):
- `run_v31_ops_monitor.py` upserts into:
  - `v31_ops_monitor_snapshot`
  - `v31_ops_monitor_health_checks`
  - `v31_ops_monitor_episodes`

Unified sentiment artifact behavior:
- `run_v31_build_unified_sentiment.py` always writes JSON artifacts.
- `sentiment_daily_unified.csv` is optional via `--write-csv` (default off).

## Key Outputs
- `logs/UTRBEV30_log_YYYY-MM-DD_HHMMSS.log`
- `output/v31_ops_monitor/daily_report.md`
- `output/v31_ops_monitor/daily_report_YYYY-MM-DD.md`
- `output/v31_ops_monitor/UTRBEV3_daily_report_YYYY-MM-DD.md`
- `output/v31_ops_monitor/summary.json`
- `output/v31_ops_monitor/summary_YYYYMMDD.json`
- `output/v31_ops_monitor/strategy_120d.png`
- `output/v31_ops_monitor/strategy_120d_YYYYMMDD.png`

## Quick Health Check
Check these fields in `output/v31_ops_monitor/summary.json`:
1. `metrics.dd_reduction` should be `>= 0.40`
2. `metrics.cagr_impact` should be `< 0.02`
3. `health_checks.trigger_density_ok_2_to_8_per_year` should be `true`
4. `health_checks.episode_duration_ok_lt_15_days` should be `true`
5. `health_checks.full_liquidation_forbidden` should be `true`

## Common Issues
1. DB not updated (latest date not advanced)
- Symptom: report date unchanged.
- Action: re-run after DB close load completes; do not treat weekend/intraday as missing data.

2. UTRBE output path permission issue
- Symptom: `PermissionError` when writing UTRBE `output/...`
- Action: use writable output paths (current default in `run_v31_prod_daily.py` points to V30 workspace output).

3. Missing V2 block in report
- Symptom: no `V2信号状态（兼容视图）` section.
- Action: ensure `latest_state.json` exists in UTRBE output dir and daily run is not skipping UTRBE refresh.

4. Single-day sample causing distorted stats
- Symptom: near-10d track too short / dd or cagr looks 0.
- Action: run with historical range; avoid single-day dry run for performance judgment.

## Manual Fallback (Step-by-Step)
If one-command run fails, run in order:
1. `python scripts/run_v31_build_unified_sentiment.py --table-name sentiment_daily_unified`
2. `python ../UTRBE/scripts/run_daily_pipeline.py --market-features-table market_features_daily --unified-sentiment-table sentiment_daily_unified ...`
3. `python scripts/run_v30_build_features.py --input-table market_features_daily --input-table-source mysql --table-name v30_features_daily --db-path data/backtest/v30_backtest.sqlite ...`
4. `python scripts/run_v30_build_labels.py --features-table v30_features_daily --struct-table v30_structural_labels_daily --shock-table v30_shock_labels_daily --db-path data/backtest/v30_backtest.sqlite ...`
5. `python scripts/run_v30_full_infer.py --features-table v30_features_daily --artifact-db-path data/backtest/v30_backtest.sqlite --struct-model-key v30_structural_model_prod --shock-model-key v30_shock_model_prod --db-path data/backtest/v30_backtest.sqlite --struct-output-table v30_structural_full_predictions_daily --shock-output-table v30_shock_full_predictions_daily ...`
6. `python scripts/run_v30_risk_aggregate.py --features-table v30_features_daily --struct-pred-table v30_structural_full_predictions_daily --shock-pred-table v30_shock_full_predictions_daily --db-path data/backtest/v30_backtest.sqlite --output-table v31_daily_allocation ...`
7. `python scripts/run_v30_backtest_eval.py --allocation-table v31_daily_allocation --db-path data/backtest/v30_backtest.sqlite --output-daily-table v30_backtest_daily ...`
8. `python scripts/run_v31_lowfreq_recovery.py --daily-table v30_backtest_daily ...`
9. `python scripts/run_v31_ops_monitor.py --allocation-csv output/v31_risk_aggregate_default_prod/daily_allocation.csv --backtest-daily-csv output/v31_backtest_eval_default_prod/v30_backtest_daily.csv --reference-summary-sqlite-key v31_backtest_eval_hardgate_gatepass/summary.json --artifact-db-path data/backtest/v30_backtest.sqlite ...`

## Gray Rollback Switch
If you need to temporarily disable the cautious gray mode in V2 execution:
- add `--disable-plus-force-reduce-on-signal`
- optionally keep/raise `--plus-force-reduce-min-prob` (default `0.12`)

