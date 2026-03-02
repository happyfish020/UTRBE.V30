# V31 Phase-D Step1 Status (2026-02-22)

## What Was Implemented
- Added `v31_mode` in risk aggregation:
  - Structural Damage Score (SDS) gating
  - Regime context multiplier (BULL/NEUTRAL/BEAR)
  - Composite risk score and state mapping (`LOW/MEDIUM/HIGH/CRISIS`)
- Added `v31_step_mode` in allocation:
  - Step allocation mapping:
    - `LOW -> 1.00`
    - `MEDIUM -> 0.85`
    - `HIGH -> 0.60`
    - `CRISIS -> 0.20`
- Added runnable init config:
  - `config/v31_phase_d_init_20260222.json`

## Output Artifacts
- Phase-D output: `output/v31_risk_aggregate_init/`
- Phase-E backtest: `output/v31_backtest_eval_init/summary.json`

## Current Result
- `cagr_impact = 0.053935`
- `max_drawdown_reduction = 0.122152`
- Both gates still fail (`DD>=40%`, `CAGR<2%`)

## Interpretation
- V31 Step1 architecture is wired and executable.
- With current V30 feature set, structural-damage gate is too conservative (`LOW` dominates), so risk cuts are too shallow.
- This confirms next bottleneck is upstream structural features, not just downstream threshold tuning.

## Next Step (Step2)
- Implement true breadth/structure inputs into features:
  - New-lows ratio
  - Advance/decline spread proxy
  - Sector synchronous breakdown ratio
  - Leadership failure proxy
- Refit SDS using these features, then rerun Phase-D/E and tuning scan.
