# V31 实验报告_2026-03-02

## 1. 实验目标
- 固化 `A_ma100` 策略并验证全样本表现。
- 检查稳定性（Rolling 5Y、Forward Walk）。
- 验证杠杆扩展（`1.15x` 主版本、`1.2x` 上限）与极端冲击抗性。
- 将结果接入每日生产报告口径。

## 2. 当日回测流程汇总
1. 基线策略确认（`A_ma100`）
- 策略配置：`config/v31_phase_d_A_ma100_frozen_20260302.json`
- 关键规则：
  - `tactical_gate_mode = MA100`
  - `early_struct_gate_mode = MA100`
  - `early_struct_top20_mult = 0.90`
  - `early_struct_top10_mult = 0.80`

2. 全样本基线回测（2016-01-04 ~ 2026-03-02）
- 数据源：`output/exp_tactical_ma100_backtest/summary.json`
- 结果：
  - Strategy CAGR: `12.367%`
  - Strategy MDD: `-17.845%`
  - Benchmark CAGR: `14.736%`
  - Benchmark MDD: `-33.717%`
  - `cagr_impact = 2.369%`
  - `dd_reduction = 47.074%`

3. Rolling 5Y 稳定性（按月滚动）
- 文件：`output/analysis_stability_forwardwalk/rolling_5y_summary.json`
- 结果：
  - 窗口数：`66`
  - 平均 `cagr_impact = 1.450%`
  - 平均 `dd_reduction = 46.042%`
  - 平均 `sharpe_diff = +0.214`
  - 平均 `calmar_diff = +0.308`

4. Forward Walk（严格版：3Y expanding train + 1Y test，逐段重训）
- 文件：`output/analysis_stability_forwardwalk/forward_walk_retrain_summary.json`
- 结果（2020~2024）：
  - 平均 `cagr_impact = 2.467%`
  - 平均 `dd_reduction = 6.442%`
  - 平均 `sharpe_diff = -0.146`
  - 平均 `calmar_diff = -0.163`
  - OOS 拼接（2020-01-02~2024-12-31）：
    - Strategy CAGR: `3.112%`
    - Benchmark CAGR: `5.371%`
    - OOS `cagr_impact = 2.259%`
    - OOS `dd_reduction = 7.719%`

5. 杠杆测试（在 MDD 不劣于 benchmark 约束下）
- 文件：
  - `output/analysis_leverage/leverage_1p0_1p2_summary.json`
  - `output/analysis_leverage/leverage_rebacktest_1p15_main_1p2_cap_stress.json`
- 结果：
  - `1.15x`：CAGR `14.198%`，MDD `-20.323%`
  - `1.20x`：CAGR `14.805%`，MDD `-21.137%`
  - 两者 MDD 均显著小于 benchmark（`-33.717%`）

6. 极端压力测试（模拟单月 -40% 冲击）
- 冲击窗口：`2020-02-24 ~ 2020-03-23`（21 交易日）
- 结果（冲击窗口收益）：
  - Benchmark: `-40.00%`
  - `1.15x`: `-18.42%`
  - `1.20x`: `-19.14%`

## 3. 结论
- 生产基线确定为：`A_ma100_frozen`。
- 在当前样本下：
  - 风险控制目标（DD reduction）稳定满足（全样本约 `47%`）。
  - 收益影响仍略高于 `<2%` 门槛（当前约 `2.37%`），需继续优化触发密度。
- 杠杆建议：
  - 主版本：`1.15x`
  - 上限：`1.20x`
  - 已通过本次极端冲击模拟的相对抗压验证。

## 4. 产物索引
- 基线回测：`output/exp_tactical_ma100_backtest/summary.json`
- Rolling 5Y：`output/analysis_stability_forwardwalk/rolling_5y_summary.json`
- Forward Walk 重训：`output/analysis_stability_forwardwalk/forward_walk_retrain_summary.json`
- 杠杆主测试：`output/analysis_leverage/leverage_1p0_1p2_summary.json`
- 杠杆+压力测试：`output/analysis_leverage/leverage_rebacktest_1p15_main_1p2_cap_stress.json`
- 每日运维汇总：`output/v31_ops_monitor/summary.json`
