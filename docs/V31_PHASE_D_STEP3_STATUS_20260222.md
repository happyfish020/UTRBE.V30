# V31 Phase-D Step3 Status (2026-02-22)

## Objective
Integrate a real breadth data source into Phase-A (not in-table proxy fallback as primary path).

## What Changed
- Phase-A now supports external breadth merge:
  - script args added in `scripts/run_v30_build_features.py`
    - `--breadth-csv`
    - `--breadth-date-col`
    - `--breadth-value-col`
- Feature engineering now consumes real breadth series (`breadth_real_stress`) when available:
  - implemented in `v30/feature_engineering/build_features.py`
  - `breadth_source_flag=1` when external breadth coverage is sufficient
- Updated data contract:
  - `config/v30_data_contract.yaml`
- Phase-A runbook updated for breadth source usage:
  - `docs/V30_PHASE_A_RUNBOOK.md`

## Data Source Used
- Breadth csv: `../UTRBE/output/full_2006_2026.csv`
- Breadth field: `breadth`
- Coverage on V30 feature window: `0.9996`

## Re-run Pipeline
1. `run_v30_build_features.py` with external breadth csv  
2. `run_v30_structural_train.py`  
3. `run_v30_shock_train.py`  
4. `run_v30_risk_aggregate.py` (V31 init config)  
5. `run_v30_backtest_eval.py`

## Main Results
- Step2 (before real breadth source):
  - `cagr_impact = 0.038484`
  - `max_drawdown_reduction = 0.161794`
- Step3 (after real breadth source):
  - `cagr_impact = 0.030642`
  - `max_drawdown_reduction = 0.191251`
- Both gates still fail (`DD>=40%`, `CAGR<2%`), but efficiency improved.

## Interpretation
- Real breadth source integration is successful and active.
- Structural model quality improved materially (test recall now non-zero), but shock leg remains sparse-label constrained.
- Next bottleneck is Phase-C label/data definition rather than Phase-A ingestion mechanics.
