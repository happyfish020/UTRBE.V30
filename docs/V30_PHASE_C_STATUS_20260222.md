# V30 Phase-C Status (Shock Engine Bootstrap)

Version: v1.0.0
Updated: 2026-02-22

## Run Status
- Phase-C training/evaluation scripts run successfully.
- Outputs generated:
  - `output/v30_shock_train/summary.json`
  - `output/v30_shock_eval/summary.json`
  - `output/v30_shock_eval/walkforward_window_metrics.csv`

## Key Metrics (Current Bootstrap)
- Train/Test split metrics:
  - AUC: 0.8184
  - FPR: 0.0000
  - Recall: 0.0000
- Walk-forward:
  - windows: 3
  - worst_auc: 0.2553
  - mean_auc: 0.5758
  - mean_fpr: 0.0000

## Interpretation
- Current shock label is a **proxy** based on drawdown-path increase in next 5 days.
- Positive class is extremely sparse (~0.2%), causing:
  - unstable walk-forward AUC
  - near-zero recall at default threshold

## Required Next Step
1. Replace proxy label with dedicated shock-event labeling pipeline:
   - event table from high-frequency or event-aware source
   - explicit 5d drop + 2d concentration rule using return path
2. Add threshold policy / class-weight tuning for recall target.
3. Expand shock-specific features (vol compression, skew/gamma proxies, flow shocks).

## Decision
- Phase-C engineering pipeline: PASS
- Phase-C model readiness for production: NOT READY (label/data limitation)
