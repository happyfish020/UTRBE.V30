# V31 Step4 Efficient Frontier (2026-02-23)

- points_total: 189
- points_unique: 68
- pareto_points: 5

## Extremes
- max_dd_reduction: run=v31_backtest_eval_step4_dynamic_0, dd=0.344422, cagr=0.088882
- min_cagr_impact: run=v31_backtest_eval_step4_allocscan_48, cagr=0.026234, dd=0.289949
- min_gate_gap_score(4*dd_gap+cagr_gap): run=v31_backtest_eval_step4_dynamic_0, cagr=0.088882, dd=0.344422

## Lambda Choices
- lambda=0.5: run=v31_backtest_eval_step4_dynamic_0, efficiency=0.299981, cagr=0.088882, dd=0.344422
- lambda=0.6: run=v31_backtest_eval_step4_dynamic_0, efficiency=0.291093, cagr=0.088882, dd=0.344422
- lambda=0.7: run=v31_backtest_eval_step4_dynamic_0, efficiency=0.282205, cagr=0.088882, dd=0.344422

## 40%/2% Feasibility in Current Frontier
- points_dd_ge_40pct: 0
- points_cagr_lt_2pct: 0
- points_pass_both: 0
- conclusion: not observed in sampled frontier
