# UTRBE.V30

Version: v30.bootstrap.2026-02-22

## Purpose
This repository is the dedicated V30 line, separated from `UTRBE` production branch.

## Source Specs
V30 design docs are copied to `docs/`:
- `UTRBE_V30_Backtest_Specification.docx`
- `UTRBE_V30_Code_Architecture_Blueprint.docx`
- `UTRBE_V30_Database_Schema.docx`
- `UTRBE_V30_Math_and_Calibration_Spec.docx`

## V30 Core Targets
- Max drawdown reduction >= 40%
- CAGR impact < 2%
- Worst-window AUC >= 0.60

## Architecture Skeleton
- `v30/data_layer`
- `v30/feature_engineering`
- `v30/structural_engine`
- `v30/shock_engine`
- `v30/tactical_engine`
- `v30/risk_aggregation`
- `v30/allocation`
- `v30/backtest`
- `v30/evaluation`

## Execution Pipeline
1. Load latest data
2. Compute features
3. Run structural model
4. Run shock model
5. Compute tactical level
6. Aggregate risk
7. Output allocation

## Next Step
See `docs/V30_BOOTSTRAP_PLAN.md` for phased migration and implementation.
