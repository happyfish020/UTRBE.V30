# V30 Phase-C Shock Runbook

Version: v1.1.0
Updated: 2026-02-24

## Scope
Phase-C bootstrap implements Shock Engine training and rolling walk-forward evaluation.

## Label (Bootstrap Proxy)
Proxy shock label from drawdown path:
- Horizon: 5 trading days
- total drawdown increase over next 5d >= 7%
- first 2d share of increase >= 60%

## Train
```bash
python scripts/run_v30_shock_train.py \
  --features-table v30_features_daily \
  --output-dir output/v30_shock_train \
  --calibration sigmoid \
  --label-adaptive-drop \
  --label-target-positive-rate 0.04 \
  --label-use-stress-override \
  --label-stress-gate 0.55
```

## Eval
```bash
python scripts/run_v30_shock_eval.py   --features-csv output/v30_features_daily.csv   --output-dir output/v30_shock_eval   --calibration sigmoid   --train-years 3 --test-years 1 --step-years 1
```

## Outputs
- `output/v30_shock_train/shock_model.pkl`
- `output/v30_shock_train/shock_test_predictions.csv`
- `output/v30_shock_train/summary.json`
- `output/v30_shock_eval/walkforward_window_metrics.csv`
- `output/v30_shock_eval/summary.json`
- DB label table `v30_shock_labels_daily` (via `run_v30_build_labels.py`)

## Notes
- This is a bootstrap proxy label. Replace with dedicated shock-event labels after Phase-D data contracts.
- Step4 adds adaptive label threshold and stress override to avoid extreme positive sparsity.
