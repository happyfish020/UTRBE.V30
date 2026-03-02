# V30 Phase-B Structural Runbook

Version: v1.1.0
Updated: 2026-02-24

## Scope
Phase-B bootstrap implements Structural Engine training and rolling walk-forward evaluation.

## Inputs
- DB table `v30_features_daily` (from Phase-A)

## Label (Bootstrap Proxy)
- Structural label = 1 when in next 30 trading days:
  - max(drawdown_252) >= 10%
  - days(drawdown_252 >= 10%) >= 15

## Train Command
```bash
python scripts/run_v30_structural_train.py   --features-table v30_features_daily   --output-dir output/v30_structural_train   --calibration sigmoid
```

## Eval Command
```bash
python scripts/run_v30_structural_eval.py   --features-csv output/v30_features_daily.csv   --output-dir output/v30_structural_eval   --calibration sigmoid   --train-years 3 --test-years 1 --step-years 1
```

## Outputs
- `output/v30_structural_train/structural_model.pkl`
- `output/v30_structural_train/structural_test_predictions.csv`
- `output/v30_structural_train/summary.json`
- `output/v30_structural_eval/walkforward_window_metrics.csv`
- `output/v30_structural_eval/summary.json`

## Notes
- This is a bootstrap proxy label implementation for Phase-B.
- Phase-C/Phase-D should replace proxy label with finalized V30 target pipeline once data contracts are completed.
