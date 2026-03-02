# V30 Phase-D Runbook (V31 Default Production)

Version: v1.6.1  
Updated: 2026-02-25

## Goal
Run V31 Phase-D/Phase-E with the frozen hard-gate production baseline.

## Production Dependency
For daily production use, V31 consumes UTRBE daily outputs first (DB -> V2 artifacts), then runs V31 risk pipeline.
If you need V2 signal-state block in V31 report, `latest_state.json` from UTRBE must exist.
Unified sentiment is DB-first: `run_v31_build_unified_sentiment.py` writes to `sentiment_daily_unified`, and V2 reads this table directly.
Market features are DB-first: UTRBE daily updates `market_features_daily`, and V31 Phase-A reads from this table.
V30 features, labels, predictions, allocation, and backtest daily are also persisted to DB tables by default.

## One-Command Daily Production Run
```bash
python scripts/run_v31_prod_daily.py
```

Default behavior:
1. If no CLI args are provided, run single-day only: `start=end=last trading day` (not natural day).
2. If args are provided, use explicit `--start/--end` (and `--end auto` resolves to last trading day).
3. Build unified sentiment from DB and upsert to `sentiment_daily_unified`.
4. Run UTRBE daily pipeline first (reads unified sentiment from DB table).
5. Run V31 full chain and ops report.
6. Write one-run-one-log file: `output/logs/UTRBEV30_log_YYYY-MM-DD.log`.

## Default Production Config
- `config/v31_phase_d_default_prod_20260223.json`
- Notes:
  - hard-gate enabled
  - hard-force regime crisis disabled
  - recovery curve disabled (`recovery_enable=false`)

## End-to-End Production Commands
```bash
# 1) UTRBE daily output from DB (required in production)
python ../UTRBE/scripts/run_daily_pipeline.py \
  --start 2016-01-01 \
  --end YYYY-MM-DD \
  --output-dir output/utrbe_prod_daily \
  --compare-primary-tracks \
  --compare-output-dir output/utrbe_compare_prod_daily

# 2) V31 feature/risk chain
python scripts/run_v30_build_features.py \
  --input-table market_features_daily \
  --breadth-csv ../UTRBE/output/full_2006_2026.csv \
  --breadth-value-col breadth \
  --breadth-price-col price \
  --breadth-credit-col credit \
  --output-csv output/v30_features_daily.csv \
  --meta-json output/v30_features_build_meta.json \
  --table-name v30_features_daily

python scripts/run_v30_build_labels.py \
  --features-table v30_features_daily \
  --struct-table v30_structural_labels_daily \
  --shock-table v30_shock_labels_daily \
  --shock-adaptive-drop \
  --shock-use-stress-override

python scripts/run_v30_structural_train.py \
  --features-table v30_features_daily \
  --output-dir output/v30_structural_train \
  --calibration sigmoid \
  --test-years 2

python scripts/run_v30_shock_train.py \
  --features-table v30_features_daily \
  --output-dir output/v30_shock_train_step4 \
  --calibration sigmoid \
  --test-years 2 \
  --label-horizon-days 7 \
  --label-early-share-threshold 0.45 \
  --label-adaptive-drop \
  --label-target-positive-rate 0.10 \
  --label-min-drop-threshold 0.010 \
  --label-max-drop-threshold 0.10 \
  --label-use-stress-override \
  --label-stress-gate 0.40

python scripts/run_v30_risk_aggregate.py \
  --features-table v30_features_daily \
  --struct-pred-table v30_structural_full_predictions_daily \
  --shock-pred-table v30_shock_full_predictions_daily \
  --tuning-json config/v31_phase_d_default_prod_20260223.json \
  --output-dir output/v31_risk_aggregate_default_prod \
  --output-table v31_daily_allocation

python scripts/run_v30_backtest_eval.py \
  --allocation-table v31_daily_allocation \
  --output-dir output/v31_backtest_eval_default_prod \
  --execution-lag-days 1 \
  --transaction-cost-bps 2 \
  --output-daily-table v30_backtest_daily
```

## Expected Outputs
- `output/v31_risk_aggregate_default_prod/daily_allocation.csv`
- `output/v31_risk_aggregate_default_prod/summary.json`
- `output/v31_backtest_eval_default_prod/summary.json`

## Acceptance Checklist
1. `output/v31_backtest_eval_default_prod/summary.json` exists.
2. `comparison.max_drawdown_reduction >= 0.40`.
3. `comparison.cagr_impact < 0.02`.
4. `output/v31_risk_aggregate_default_prod/summary.json` shows reasonable hard-gate density (not high-frequency over-trigger).

## Release References
- Status doc: `docs/V31_PHASE_D_STEP4_STATUS_20260222.md`
- OOS walkforward: `output/v31_backtest_eval_hardgate_gatepass/oos_walkforward_summary.json`
- Diagnostics: `output/v31_backtest_eval_hardgate_gatepass/hardgate_diagnostics.json`
