# V31 Continuation Prompt (No-Memory-Loss Handoff)

复制下面整段到新会话第一条消息：

```text
你现在是本项目的连续开发代理，请在 d:\LHJ\PythonWS\UTRBE.V30 继续工作，不要丢失上下文。

运行权限与行为：
1) 使用可写沙箱（workspace-write）执行，默认同意项目内非破坏性读写与验证命令。
2) 收到需求后直接执行，不先停在讨论阶段。
3) 仅在高风险操作前确认（大规模删除、reset、覆盖关键产物）。
4) 每个阶段结束输出：结果摘要 + 关键文件路径 + 下一步建议。

项目当前状态（截至 2026-02-22）：
- Step1: V31 Phase-D 架构已落地（SDS + Regime + Step Allocation）。
- Step2: 加入了 breadth 代理特征，但效果有限。
- Step3: 已接入真实 breadth 数据源（../UTRBE/output/full_2006_2026.csv 的 breadth 列），并完成 A/B/C/D/E 重跑。
- Step3 回测结果：
  - output/v31_backtest_eval_step3/summary.json
  - cagr_impact = 0.030641689540481964
  - max_drawdown_reduction = 0.1912505390198666
  - gate 仍未通过。
- 已定位瓶颈：Phase-C shock 标签过稀（历史 summary 显示 recall=0）。

必须优先检查的文件：
- v30/feature_engineering/build_features.py
- scripts/run_v30_build_features.py
- v30/risk_aggregation/aggregate.py
- v30/allocation/decision.py
- v30/shock_engine/labeling.py
- scripts/run_v30_shock_train.py
- scripts/run_v30_shock_eval.py
- docs/V31_PHASE_D_STEP3_STATUS_20260222.md

当前任务目标：
1) 继续 Step4：改进 Phase-C shock 标签定义与训练可用性（提高有效正样本和实用召回）。
2) 重跑：
   - scripts/run_v30_shock_train.py
   - scripts/run_v30_risk_aggregate.py (V31 config)
   - scripts/run_v30_backtest_eval.py
3) 产出 Step4 状态文档，记录参数、结果、结论。

标准命令（按需调整参数）：
- python scripts/run_v30_build_features.py --input-csv ../UTRBE/output/dualtrack_daily_pipeline_with_compare_2016/market_features.csv --breadth-csv ../UTRBE/output/full_2006_2026.csv --breadth-value-col breadth --output-csv output/v30_features_daily.csv --meta-json output/v30_features_build_meta.json
- python scripts/run_v30_structural_train.py --features-csv output/v30_features_daily.csv --output-dir output/v30_structural_train --calibration sigmoid --test-years 2
- python scripts/run_v30_shock_train.py --features-csv output/v30_features_daily.csv --output-dir output/v30_shock_train --calibration sigmoid --test-years 2 --label-adaptive-drop --label-target-positive-rate 0.04 --label-use-stress-override --label-stress-gate 0.55
- python scripts/run_v30_risk_aggregate.py --features-csv output/v30_features_daily.csv --struct-pred-csv output/v30_structural_train/structural_test_predictions.csv --shock-pred-csv output/v30_shock_train/shock_test_predictions.csv --tuning-json config/v31_phase_d_init_20260222.json --output-dir output/v31_risk_aggregate_step4
- python scripts/run_v30_backtest_eval.py --allocation-csv output/v31_risk_aggregate_step4/daily_allocation.csv --output-dir output/v31_backtest_eval_step4 --execution-lag-days 1 --transaction-cost-bps 2 --spy-ret-csv output/v30_backtest_eval/v30_backtest_daily.csv

输出要求：
- 给出 baseline / step3 / step4 三者对比。
- 明确说明是否逼近 gate（DD>=40%, CAGR<2%）。
- 如仍失败，继续执行下一个可落地改动，不要停在建议层。
```

## DB-First Update (2026-02-24)
- Production runtime is fully DB-first for data flow.
- Core tables:
  - `market_features_daily`
  - `sentiment_daily_unified`
  - `v30_features_daily`
  - `v30_structural_labels_daily`
  - `v30_shock_labels_daily`
  - `v30_structural_full_predictions_daily`
  - `v30_shock_full_predictions_daily`
  - `v31_daily_allocation`
  - `v30_backtest_daily`
- `run_v31_prod_daily.py` is the canonical full-chain command.
- Legacy command lines in this handoff block are historical context from 2026-02-22 and may still show CSV arguments.
- Current canonical DB-first commands:
  - `python scripts/run_v30_build_features.py --input-table market_features_daily --table-name v30_features_daily ...`
  - `python scripts/run_v30_build_labels.py --features-table v30_features_daily --struct-table v30_structural_labels_daily --shock-table v30_shock_labels_daily ...`
  - `python scripts/run_v30_full_infer.py --features-table v30_features_daily --struct-output-table v30_structural_full_predictions_daily --shock-output-table v30_shock_full_predictions_daily ...`
  - `python scripts/run_v30_risk_aggregate.py --features-table v30_features_daily --struct-pred-table v30_structural_full_predictions_daily --shock-pred-table v30_shock_full_predictions_daily --output-table v31_daily_allocation ...`
  - `python scripts/run_v30_backtest_eval.py --allocation-table v31_daily_allocation --output-daily-table v30_backtest_daily ...`
