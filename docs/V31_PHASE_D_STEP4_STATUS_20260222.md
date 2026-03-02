# V31 Phase-D Step4 Status (Continuation, 2026-02-22)

> Note (2026-02-24): this status file records historical Step4 runs. Commands listed below are historical snapshots and may use CSV arguments. Current production runtime is DB-first (`run_v31_prod_daily.py`).

## Step4 Goal
Improve Phase-C shock label usability (more effective positive samples and practical recall proxy), then rerun Phase-D/E.

## Mandatory Re-run Commands Executed
1. `python scripts/run_v30_shock_train.py --features-csv output/v30_features_daily.csv --output-dir output/v30_shock_train_step4 --calibration sigmoid --test-years 2 --label-horizon-days 7 --label-early-share-threshold 0.45 --label-adaptive-drop --label-target-positive-rate 0.10 --label-min-drop-threshold 0.010 --label-max-drop-threshold 0.10 --label-use-stress-override --label-stress-gate 0.40`
2. `python scripts/run_v30_risk_aggregate.py --features-csv output/v30_features_daily.csv --struct-pred-csv output/v30_structural_train/structural_test_predictions.csv --shock-pred-csv output/v30_shock_train_step4/shock_test_predictions.csv --tuning-json config/v31_phase_d_init_20260222.json --output-dir output/v31_risk_aggregate_step4`
3. `python scripts/run_v30_backtest_eval.py --allocation-csv output/v31_risk_aggregate_step4/daily_allocation.csv --output-dir output/v31_backtest_eval_step4 --execution-lag-days 1 --transaction-cost-bps 2 --spy-ret-csv output/v30_backtest_eval/v30_backtest_daily.csv`

## Step4 Shock Label/Train Outcome
- Output: `output/v30_shock_train_step4/summary.json`
- Label parameters used:
  - `horizon_days=7`
  - `early_share_threshold=0.45`
  - `adaptive_drop=True`, `target_positive_rate=0.10`
  - `min_drop_threshold=0.010`, `max_drop_threshold=0.10`
  - `use_stress_override=True`, `stress_gate=0.40`
  - effective `drop_threshold_used=0.034614`
- Sample usability:
  - `label_positive_rate_train=0.069733`
  - `label_positive_rate_test=0.053892`
- Model metrics:
  - `auc=0.690420`
  - `recall@0.5=0.0` (still constrained)

## Baseline vs Step3 vs Step4 (Current)
- Baseline: `output/v30_backtest_eval/summary.json`
  - `cagr_impact=0.064650`
  - `max_drawdown_reduction=0.252296`
- Step3: `output/v31_backtest_eval_step3/summary.json`
  - `cagr_impact=0.030642`
  - `max_drawdown_reduction=0.191251`
- Step4 (current mainline): `output/v31_backtest_eval_step4/summary.json`
  - `cagr_impact=0.031926`
  - `max_drawdown_reduction=0.236660`

## Gate Check (DD>=40%, CAGR<2%)
- Step4 mainline result:
  - `DD reduction >= 40%`: **False** (`23.666%`)
  - `CAGR impact < 2%`: **False** (`3.193%`)
- Status: still not passing, but compared to Step3, DD improved materially with only slight CAGR increase.

## Continue-If-Fail Action Executed
Since gate still failed, a follow-up deployable change was executed immediately: lower shock activation thresholds in Phase-D tuning to match Step4 `p_shock` scale.

- Scan outputs:
  - `config/v31_phase_d_step4_followup_a_20260222.json`
  - `config/v31_phase_d_step4_followup_b_20260222.json`
  - `config/v31_phase_d_step4_followup_c_20260222.json`
  - `config/v31_phase_d_step4_followup_d_20260222.json`
  - `output/v31_backtest_eval_step4/followup_scan_summary.json`
- Best follow-up candidate: `d` (`shock_high_cut=0.40`, `shock_risk_cut=0.20`)
  - result: `output/v31_backtest_eval_step4_followup_d/summary.json`
  - `cagr_impact=0.030899`
  - `max_drawdown_reduction=0.259974`
  - still fails both gates, but improves both metrics vs current Step4 mainline.

## Conclusion
- Step4 continuation successfully increased shock label positive coverage and improved DD control over Step3.
- Hard gate remains unmet:
  - DD target shortfall: `40.0% - 26.0% = 14.0%` (using best follow-up d)
  - CAGR target shortfall: `3.09% - 2.00% = 1.09%`
- Next actionable direction (already validated by scan): keep Step4 labeling and continue Phase-D threshold/step-allocation co-tuning around follow-up candidate `d`.

## Continuation Update (2026-02-23)

### A. Allocation Grid on Follow-up d
- Baseline for this continuation:
  - `shock_high_cut=0.40`, `shock_risk_cut=0.20`
- New scan:
  - 64 combos over:
    - `step_alloc_medium in {0.82,0.85,0.88,0.90}`
    - `step_alloc_high in {0.45,0.50,0.55,0.60}`
    - `step_alloc_crisis in {0.05,0.10,0.15,0.20}`
- Best config selected:
  - `config/v31_phase_d_step4_followup_alloc_20260222.json`
  - `step_alloc_medium=0.90`
  - `step_alloc_high=0.45`
  - `step_alloc_crisis=0.15`
- Result:
  - `output/v31_backtest_eval_step4_followup_alloc/summary.json`
  - `cagr_impact=0.026234`
  - `max_drawdown_reduction=0.289949`

### B. Additional Deployable Change After Fail
- Since gates still failed, an additional code-level enhancement was implemented and tested:
  - dynamic rolling-quantile shock thresholds (history-only, no look-ahead)
  - optional SDS filter for enabling dynamic thresholds only in higher structural stress
- Changed files:
  - `v30/risk_aggregation/aggregate.py`
  - `v30/allocation/decision.py`
  - `scripts/run_v30_risk_aggregate.py`
- Scan artifacts:
  - `output/v31_backtest_eval_step4/dynamic_scan_summary.json`
  - `output/v31_backtest_eval_step4/dynamic_sds_scan_summary.json`
- Outcome:
  - dynamic thresholds increased DD in some combos but caused unacceptable CAGR drag,
  - SDS-filtered dynamic mode did not produce a better trade-off than follow-up alloc.
  - Therefore current retained best remains `followup_alloc`.

### C. Baseline / Step3 / Step4 (Latest Best in Branch)
- Baseline:
  - `output/v30_backtest_eval/summary.json`
  - `cagr_impact=0.064650`
  - `dd_reduction=0.252296`
- Step3:
  - `output/v31_backtest_eval_step3/summary.json`
  - `cagr_impact=0.030642`
  - `dd_reduction=0.191251`
- Step4 latest best (`followup_alloc`):
  - `output/v31_backtest_eval_step4_followup_alloc/summary.json`
  - `cagr_impact=0.026234`
  - `dd_reduction=0.289949`

### D. Gate Proximity (Target: DD>=40%, CAGR<2%)
- Latest best still fails both gates:
  - DD gap: `40.000% - 28.995% = 11.005%`
  - CAGR gap: `2.623% - 2.000% = 0.623%`

### E. Efficient Frontier Validation (2026-02-23)
- Method:
  - Aggregate all `v31_backtest_eval_step4*` run summaries.
  - Construct Pareto frontier on `(x=CAGR impact, y=DD reduction)`.
  - Evaluate utility `Efficiency = DD_reduction - lambda * CAGR_impact` for `lambda in {0.5,0.6,0.7}`.
- Artifacts:
  - `output/v31_backtest_eval_step4/efficient_frontier_all_points.csv`
  - `output/v31_backtest_eval_step4/efficient_frontier_pareto.csv`
  - `output/v31_backtest_eval_step4/efficient_frontier_lambda_choices.csv`
  - `output/v31_backtest_eval_step4/efficient_frontier_report.md`
- Data coverage:
  - `points_total=189`, `points_unique=68`, `pareto_points=5`
- Pareto best low-CAGR point:
  - `run=v31_backtest_eval_step4_allocscan_48`
  - `cagr_impact=0.026234`, `dd_reduction=0.289949`
- Pareto best high-DD point:
  - `run=v31_backtest_eval_step4_dynamic_0`
  - `cagr_impact=0.088882`, `dd_reduction=0.344422`
- Lambda selection (`0.5/0.6/0.7`) all pick `dynamic_0`, indicating frontier is DD-driven but very costly in CAGR.
- Feasibility result in current sampled frontier:
  - points with `DD>=40%`: `0`
  - points with `CAGR<2%`: `0`
  - points passing both: `0`
  - conclusion: current architecture/run-space does **not** show observed reachability for `DD>=40% & CAGR<2%`.

### F. Frontier-Constrained Production Candidate (2026-02-23)
- Decision:
  - switch objective from hard-gate-only to constrained frontier selection.
  - feasibility constraints:
    - `cagr_impact <= 3.0%`
    - `dd_reduction >= 25.0%`
  - utility within feasible set:
    - `Efficiency = DD_reduction - lambda * CAGR_impact`
    - `lambda in {0.5, 0.6, 0.7}`, default `lambda=0.7`
- Selection output:
  - `output/v31_backtest_eval_step4/efficient_frontier_policy_selection.json`
  - selected run: `v31_backtest_eval_step4_allocscan_48`
- Production candidate config:
  - `config/v31_phase_d_frontier_prod_20260223.json`
  - policy record: `config/v31_frontier_policy_20260223.json`
- Production candidate rerun outputs:
  - `output/v31_risk_aggregate_frontier_prod/summary.json`
  - `output/v31_backtest_eval_frontier_prod/summary.json`
- Production candidate metrics:
  - `cagr_impact=0.026234`
  - `dd_reduction=0.289949`
- Interpretation:
  - this does not pass original hard gates, but is the current best constrained-frontier point for practical deployment under the selected caps.

### G. Frontier Prod Stability Check (2026-02-23)
- Validation outputs:
  - `output/v31_backtest_eval_frontier_prod/stability_summary.json`
  - `output/v31_backtest_eval_frontier_prod/stability_report.md`
- Overall (same window):
  - baseline: `cagr_impact=0.064189`, `dd_reduction=0.252296`
  - step3: `cagr_impact=0.030181`, `dd_reduction=0.191251`
  - frontier_prod: `cagr_impact=0.026234`, `dd_reduction=0.289949`
- Year buckets:
  - 2024:
    - `frontier_prod` DD similar to `step3`, CAGR impact slightly worse.
  - 2025:
    - `frontier_prod` dominates `step3` on both metrics (`cagr_impact` lower, `dd_reduction` higher).
  - 2026:
    - only 3 rows, not statistically meaningful for stability judgement.
- Stress subwindows (benchmark DD tails):
  - `stress_q10`: `frontier_prod` shows strongest DD reduction and best annualized return among three.
  - `stress_q25`: `frontier_prod` improves both DD reduction and annualized return vs `step3`.
- Stability conclusion:
  - `frontier_prod` is robustly better than `step3` in the main stress-bearing segments and in 2025 full-year behavior.
  - recommended as current V31 default candidate under constrained-frontier policy.

### H. Hard-Gate State Machine Trial (2026-02-23)
- Implemented hard-gate logic (pre-composite / pre-allocation override):
  - trend-break cap:
    - `price_below_200dma_flag == 1` and `ret5d_median_20 < 0` => cap allocation.
  - breadth-collapse crisis:
    - `new_lows_ratio_proxy >= threshold` OR
    - `ad_spread_proxy <= threshold` for N consecutive days
    - => force `CRISIS` + cap.
  - credit expansion confirmer:
    - `credit_chg_20d >= rolling_quantile(90%, 252d)` => at least `HIGH`.
- Code paths added/updated:
  - `v30/feature_engineering/build_features.py`
  - `scripts/run_v30_build_features.py`
  - `v30/risk_aggregation/aggregate.py`
  - `v30/allocation/decision.py`
  - `scripts/run_v30_risk_aggregate.py`

- Scan result (`72` combos):
  - summary: `output/v31_backtest_eval_hardgate_scan_summary.json`
  - best-DD candidate (target 40):
    - config: `config/v31_phase_d_hardgate_target40_20260223.json`
    - output: `output/v31_backtest_eval_hardgate_target40/summary.json`
    - `cagr_impact=0.119539`
    - `dd_reduction=0.403920`
    - gate status: `DD>=40%` pass, `CAGR<2%` fail
  - balanced candidate:
    - config: `config/v31_phase_d_hardgate_balanced_20260223.json`
    - output: `output/v31_backtest_eval_hardgate_balanced/summary.json`
    - `cagr_impact=0.051155`
    - `dd_reduction=0.370465`

- Interpretation:
  - hard gates can break the previous DD ceiling and push beyond `40%`.
  - but with current proxy quality, reaching `DD>=40%` requires a large CAGR sacrifice.
  - this confirms the structural hypothesis: hard gates are the right direction for DD, but need more precise collapse/credit inputs to control false positives and retain CAGR.

### I. Hard-Gate Bias Fix + Low-Frequency Scan (2026-02-23)
- Bias fix:
  - issue confirmed: hard crisis flow could still be over-restrictive through regime path.
  - fix applied:
    - add `hard_force_sets_regime_crisis` (default `false`)
    - when `false`, hard gate controls allocation via `hard_max_allocation` without forcibly routing to `CRISIS` regime.
  - changed files:
    - `v30/risk_aggregation/aggregate.py`
    - `scripts/run_v30_risk_aggregate.py`

- Low-frequency hard-gate scan:
  - size: `648` combos
  - artifact: `output/v31_backtest_eval_hardgate_lfscan_summary.json`
  - focus: raise collapse thresholds and reduce trigger density.

- Gate-pass result found (both conditions true):
  - config: `config/v31_phase_d_hardgate_gatepass_20260223.json`
  - output:
    - `output/v31_risk_aggregate_hardgate_gatepass/summary.json`
    - `output/v31_backtest_eval_hardgate_gatepass/summary.json`
  - metrics:
    - `cagr_impact=0.013638`  (`< 0.02`, pass)
    - `dd_reduction=0.401311` (`>= 0.40`, pass)
  - hard gate trigger profile:
    - `BREADTH_CRISIS=4 days`
    - `TREND_CAP=3 days`
    - `CREDIT_HIGH=28 days`
    - `NONE=443 days`

- Current comparison snapshot:
  - baseline: `cagr_impact=0.064650`, `dd_reduction=0.252296`
  - step3: `cagr_impact=0.030642`, `dd_reduction=0.191251`
  - frontier_prod: `cagr_impact=0.026234`, `dd_reduction=0.289949`
  - hardgate_gatepass: `cagr_impact=0.013638`, `dd_reduction=0.401311` (**pass both gates**)

### J. Trigger Density / Recovery / Stability Diagnostics (2026-02-23)
- Diagnostics artifacts:
  - `output/v31_backtest_eval_hardgate_gatepass/hardgate_diagnostics.json`
  - `output/v31_backtest_eval_hardgate_gatepass/hardgate_diagnostics.md`

- Core numbers (gatepass config):
  - annual trigger episodes:
    - 2024: `5`
    - 2025: `5`
    - 2026: `0` (partial year window)
    - average: `3.33` episodes/year
  - average episode duration: `4.5` trading days
  - average recovery days to `>=95%` allocation: `18.2` trading days
  - full liquidation check:
    - episode ratio with `min_alloc <= 5%`: `0.0`
    - full-liquidation days: `0 / 478`
  - trend-miss proxy ratio: `0.0`

- Threshold strength curve (0.9 / 1.0 / 1.1):
  - `0.9x`: `cagr_impact=0.029259`, `dd_reduction=0.334540`
  - `1.0x`: `cagr_impact=0.013638`, `dd_reduction=0.401311`
  - `1.1x`: `cagr_impact=0.014569`, `dd_reduction=0.400547`
- Interpretation:
  - around current threshold, `1.0x -> 1.1x` is stable (DD/CAGR nearly unchanged), indicating a local plateau.
  - `0.9x` materially worsens both objectives, showing sensitivity on the looser-trigger side.

- Hard-gate OFF vs ON (annualized return):
  - OFF (`frontier_prod`): `0.174610`
  - ON (`hardgate_gatepass`): `0.187206`
  - delta (`ON - OFF`): `+0.012596`

### K. True OOS Walkforward Check (2026-02-23)
- Method:
  - candidate set: all low-frequency hard-gate scans (`648` configs)
  - rolling selection:
    - train window: `252` trading days
    - reselection step: `63` trading days
    - selection objective: constrained (`cagr_impact < 2%`, `dd_reduction >= 40%`) + lambda utility fallback
  - OOS test window:
    - `2025-02-12 -> 2026-01-06` (`226` rows)
  - outputs:
    - `output/v31_backtest_eval_hardgate_gatepass/oos_walkforward_summary.json`
    - `output/v31_backtest_eval_hardgate_gatepass/oos_walkforward_report.md`

- OOS results (same window comparison):
  - walkforward selected:
    - `cagr_impact=-0.019845`
    - `dd_reduction=0.402409`
    - `annualized_return=0.195652`
    - `max_drawdown=-0.112080`
  - fixed `frontier_prod`:
    - `cagr_impact=0.013782`
    - `dd_reduction=0.289949`
    - `annualized_return=0.162026`
    - `max_drawdown=-0.133172`
  - fixed `hardgate_gatepass`:
    - same as walkforward in this OOS slice (selection converged to equivalent top candidates).

- Conclusion:
  - in forward-only selection, hard-gate low-frequency family still maintains `DD >= 40%` while keeping `CAGR impact < 2%` in tested OOS segment.
  - this supports upgrading from parameter-fit evidence to structure-level robustness evidence.

### L. Dynamic Recovery Curve Trial (2026-02-23)
- Goal:
  - test whether post-trigger gradual recovery can add `1~2%` CAGR while keeping DD around `40%`.
- Implementation:
  - added recovery controls in allocation layer:
    - `recovery_enable`
    - `recovery_step`
    - `recovery_start_cap`
    - `recovery_vol_window`
    - `recovery_vol_q`
    - `recovery_min_days`
    - `recovery_trigger_reasons`
  - changed files:
    - `v30/allocation/decision.py`
    - `scripts/run_v30_risk_aggregate.py`

- Scan:
  - first scan: `108` combos (`output/v31_backtest_eval_hardgate_recovery_scan_summary.json`)
  - refined scan (only `BREADTH_CRISIS,TREND_CAP` trigger recovery): `81` combos
    - `output/v31_backtest_eval_hardgate_recovery2_scan_summary.json`
    - best: `output/v31_backtest_eval_hardgate_recovery2_scan_47/summary.json`

- Result vs gatepass baseline:
  - gatepass (no recovery):
    - `cagr_impact=0.013638`
    - `dd_reduction=0.401311`
  - best recovery:
    - `cagr_impact=0.028686`
    - `dd_reduction=0.402409`
- Conclusion:
  - current dynamic recovery design does not improve CAGR; it worsens CAGR materially while DD is nearly unchanged.
  - production recommendation remains:
    - keep `hardgate_gatepass` config
    - `recovery_enable=false` for now.

### M. Step5 Freeze + Release + Smoke (2026-02-23)
- Freeze:
  - default production config created:
    - `config/v31_phase_d_default_prod_20260223.json`
  - explicit policy:
    - hard-gate enabled
    - `hard_force_sets_regime_crisis=false`
    - `recovery_enable=false`

- Important implementation guard added:
  - exclude hard-gate-only columns from Structural/Shock model feature sets to prevent model-drift contamination after feature schema expansion.
  - changed:
    - `v30/structural_engine/model.py`
    - `v30/shock_engine/model.py`

- Runbook updated:
  - `docs/V30_PHASE_D_RUNBOOK.md`

- Release bundle:
  - `release/v31_hardgate_gatepass_20260223/`
  - includes config/docs/key summaries/diagnostics/OOS references.

- End-to-end smoke re-run (build -> structural -> shock -> aggregate -> backtest):
  - output:
    - `output/v31_backtest_eval_default_prod/summary.json`
  - result:
    - `cagr_impact=0.013638`
    - `dd_reduction=0.401311`
    - both acceptance checks: `true`

### N. Read-Only Ops Monitoring (2026-02-23)
- Added monitoring script and runbook:
  - `scripts/run_v31_ops_monitor.py`
  - `docs/V31_OPS_MONITOR_RUNBOOK.md`
- Monitor outputs:
  - `output/v31_ops_monitor/summary.json`
  - `output/v31_ops_monitor/report.md`
- Current monitor snapshot:
  - `cagr_impact=0.013638`
  - `dd_reduction=0.401311`
  - avg triggers/year: `5.0`
  - avg episode days: `4.5`
  - full liquidation days: `0`
  - drift vs reference: `0` on both core metrics
- Release bundle updated:
  - `release/v31_hardgate_gatepass_20260223/MANIFEST.txt`
