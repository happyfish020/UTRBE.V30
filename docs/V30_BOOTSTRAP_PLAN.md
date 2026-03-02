# V30 Bootstrap Plan

Version: v1.0.0
Updated: 2026-02-22

## Context
This plan translates the V30 docx specs into a practical migration path from `UTRBE` to `UTRBE.V30`.

## Spec Summary
1. Backtest and Risk:
- Walk-forward retrain every 3 years
- Mandatory out-of-sample evaluation
- Separate structural/shock event assessment

2. Models:
- Structural: logistic probability model + calibration (Platt/Isotonic)
- Shock: non-linear classifier (GBDT preferred)
- Tactical: multiplier layer after structural/shock

3. Data and Storage:
- Daily ingestion -> feature tables -> model outputs -> allocation snapshots
- Feature tables versioned; model outputs immutable

## Proposed Phases

### Phase A: Foundation (1-2 weeks)
- Build data contracts and table mapping
- Freeze V30 feature dictionary and target definitions
- Add reproducible training/evaluation entrypoints

Deliverables:
- `config/v30_data_contract.yaml`
- `config/v30_targets.yaml`
- `scripts/run_v30_build_features.py`

### Phase B: Structural Engine (1-2 weeks)
- Implement structural features and logistic model
- Add calibration pipeline (Platt + Isotonic selectable)
- Add decade stability checks

Deliverables:
- `v30/structural_engine/`
- `scripts/run_v30_structural_train.py`
- `scripts/run_v30_structural_eval.py`

### Phase C: Shock Engine (1-2 weeks)
- Implement shock features (vol compression, gamma exposure proxies)
- Train/evaluate GBDT-based shock model

Deliverables:
- `v30/shock_engine/`
- `scripts/run_v30_shock_train.py`
- `scripts/run_v30_shock_eval.py`

### Phase D: Allocation & Tactical (1 week)
- Piecewise base allocation from structural probability
- Shock override as multiplicative modifier
- Tactical multiplier finalization

Deliverables:
- `v30/allocation/`
- `v30/tactical_engine/`
- `scripts/run_v30_allocation_daily.py`

### Phase E: Integrated Backtest and Reporting (1 week)
- Unified walk-forward and stress window evaluation
- Include required stress periods: 2008, 2018, 2020, 2022
- Produce release-grade report artifacts

Deliverables:
- `v30/backtest/`
- `v30/evaluation/`
- `output/v30_backtest_report_*`

## Acceptance Gate (from V30 specs)
- Max drawdown reduction >= 40%
- CAGR impact < 2%
- Worst-window AUC >= 0.60

## Initial Migration Mapping from UTRBE
- Reuse candidate modules:
  - `scripts/run_hmm_walkforward.py` (evaluation flow patterns)
  - `scripts/run_dualtrack_daily.py` (operational state/output patterns)
  - `scripts/compare_primary_track_portfolio.py` (portfolio simulation shell)
- Replace model core with V30 structural/shock/tactical stack.

## Recommended Immediate Action
1. Freeze this scaffold as `UTRBE.V30 bootstrap` baseline.
2. Implement Phase A contracts and feature dictionary first.
3. Do not mix V30 development back into current UTRBE branch.
