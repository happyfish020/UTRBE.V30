# V30 Phase-D/E Underperformance Report (Init Release)

Date: 2026-02-22  
Scope: Phase-D risk aggregation + Phase-E integrated backtest evaluation

## Acceptance Gates
- max_drawdown_reduction >= 40%
- cagr_impact < 2%

## Baseline Result (Before Tuning)
Source: `output/v30_backtest_eval/summary.json`
- window: 2024-02-12 to 2026-01-06 (477 rows)
- cagr_impact: 0.064650
- max_drawdown_reduction: 0.252296
- gate_dd_reduction_ge_40pct: false
- gate_cagr_impact_lt_2pct: false

## Tuned Result (Current Best Trade-off)
Source: `output/v30_backtest_eval_tuned/summary.json`
- cagr_impact: 0.071156
- max_drawdown_reduction: 0.400989
- gate_dd_reduction_ge_40pct: true
- gate_cagr_impact_lt_2pct: false

## Tuning Scan Summary
Source: `output/v30_phase_d_tuning/tuning_report.md`
- candidate_count: 81
- acceptance_pass_count: 0
- conclusion: Under current signal quality, no scanned parameter set meets both gates simultaneously.

## Diagnosis
- Current tuning can push drawdown control to target, but CAGR loss remains significantly above 2%.
- This indicates the bottleneck is not only threshold settings; signal quality/discrimination from upstream stages (especially shock modeling) is likely insufficient for the dual-objective gate.

## Release Note
- This report is included in the init release package as the formal record of "run below expectation".
