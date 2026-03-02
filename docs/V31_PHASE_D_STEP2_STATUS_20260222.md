# V31 Phase-D Step2 Status (2026-02-22)

## Step2 Goal
Add structural breadth proxies into feature engineering and feed them into V31 SDS gating.

## Implemented
- Added proxy features in `v30/feature_engineering/build_features.py`:
  - `new_lows_ratio_proxy`
  - `ad_spread_proxy`
  - `sector_sync_breakdown_proxy`
  - `leadership_failure_proxy`
  - `breadth_damage_score`
- Updated SDS composition in `v30/risk_aggregation/aggregate.py` to prioritize breadth damage terms.
- Updated output contract in `config/v30_data_contract.yaml`.
- Rebuilt features and retrained structural/shock models.

## Main Artifacts
- Features: `output/v30_features_daily.csv`
- Structural train: `output/v30_structural_train/summary.json`
- Shock train: `output/v30_shock_train/summary.json`
- V31 Phase-D output: `output/v31_risk_aggregate_step2/summary.json`
- V31 Phase-E eval: `output/v31_backtest_eval_step2/summary.json`
- V31 grid scan: `output/v31_backtest_eval_step2/v31_grid_scan.csv`

## Step2 Result
- `cagr_impact = 0.038484`
- `max_drawdown_reduction = 0.161794`
- Acceptance gates still fail.

## Grid Scan Conclusion
- Tested 320 V31 parameter combinations (`SDS gates + regime multipliers + step allocation + tactical multipliers`).
- Pass count for both gates (`DD>=40%`, `CAGR<2%`) = `0`.
- Best drawdown reduction in scan is around `0.239`, with CAGR impact around `0.069~0.071`.

## Key Limitation Observed
- Upstream source file currently only provides market-level columns:
  - `date, vol20, hurst_100, drawdown_252, ret5d, external_event, is_trading_day`
- No true breadth inputs (A/D, new lows universe ratio, sector breakdown, leadership failure cross-section).
- Therefore Step2 can only use proxies; precision is still insufficient for dual-gate target.

## Recommended Step3
- Add real breadth/structure dataset to Phase-A input.
- Rebuild feature schema and replace proxy terms in SDS.
- Re-run Phase B/C and V31 Phase-D/E with same evaluation protocol.
