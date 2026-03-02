# V30 Phase-E Runbook (Integrated Backtest Evaluation)

Version: v1.1.0
Updated: 2026-02-24

## Goal
Evaluate V30 allocation output with practical execution assumptions and compare with benchmark.

## Command
```bash
python scripts/run_v30_backtest_eval.py \
  --allocation-table v31_daily_allocation \
  --output-dir output/v30_backtest_eval \
  --execution-lag-days 1 \
  --transaction-cost-bps 2 \
  --output-daily-table v30_backtest_daily
```

## Outputs
- `output/v30_backtest_eval/v30_backtest_daily.csv`
- `output/v30_backtest_eval/v30_backtest_summary.csv`
- `output/v30_backtest_eval/summary.json`
- `output/v30_backtest_eval/v30_backtest_report.md`
- DB table `v30_backtest_daily`

## Acceptance Checks
- max drawdown reduction >= 40%
- cagr impact < 2%

## Notes
- Current pipeline still uses Phase-C shock proxy labels.
- Re-evaluate after replacing shock labels with production event definitions.
