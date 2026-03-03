# V30 Phase-D Tuning Report

- candidate_count: 81
- acceptance_pass_count: 0
- selection_reason: No candidate passed both gates; selected best constrained trade-off.

## Best Candidate
- cagr_impact: 0.071156
- max_drawdown_reduction: 0.400989
- avg_final_allocation: 0.866992
- strategy_annualized_return: 0.130148
- strategy_max_drawdown: -0.112346

## Usage
- Apply with:
```bash
python scripts/run_v30_risk_aggregate.py \
  --features-csv output/v30_features_daily.csv \
  --struct-pred-csv output/v30_structural_train/structural_test_predictions.csv \
  --shock-pred-csv output/v30_shock_train/shock_test_predictions.csv \
  --tuning-json output\v30_phase_d_tuning\best_tuning.json \
  --output-dir output/v30_risk_aggregate
```