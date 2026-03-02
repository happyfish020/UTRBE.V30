# V31 总设计概要_2026-03-02

## 1. 文档目的
- 给出 V31 当前可运行版本的全局设计总览。
- 固化生产基线：`A_ma100_frozen`。
- 作为后续变更评审的主参考文档（先看总览，再看各子模块详细设计）。

## 2. 系统目标
1. 在中长期维持可接受收益。
2. 在极端风险阶段显著压降回撤。
3. 通过分层风控实现“提前降温 + 冲击防守 + 硬门槛兜底”。
4. 保持生产链路可观测、可回溯、可复现。

## 3. 总体架构
1. 数据层（DB-first）
- 市场特征：`market_features_daily`
- V30 特征：`v30_features_daily`
- 预测输出：`v30_structural_full_predictions_daily`, `v30_shock_full_predictions_daily`

2. 模型层
- Structural 模型：中期结构性风险概率 `p_structural`
- Shock 模型：短期冲击风险概率 `p_shock`

3. 聚合层（Risk Aggregation）
- 组合风险分层：`LOW / MEDIUM / HIGH / CRISIS`
- Hard Gate：趋势、广度、信用三类硬门槛
- Early-Structural：提前预警分层（Top15% / Top5%）

4. 决策层（Allocation）
- 基础仓位（step alloc）
- Shock 修正
- Tactical 修正
- Early 修正
- Hard Gate 上限约束
- 输出 `final_allocation`

5. 评估与监控层
- Backtest：`v30_backtest_daily`
- Ops Monitor：`output/v31_ops_monitor/`
- 每日解释报告 + 120d 图 + 健康检查

## 4. 风控分层设计（职责分离）
1. Early-Structural（提前层）
- 作用：顶部失稳阶段轻度降仓，不做 hard gate。
- 乘数：`1.0 / 0.9 / 0.8`

2. Structural（中期层）
- 作用：识别慢性结构恶化与趋势破坏。
- 决定中期风控方向。

3. Shock（短期层）
- 作用：识别突发波动与下跌冲击。
- 决定短期踩刹车强度。

4. Hard Gate（兜底层）
- 作用：在极端条件下强制仓位上限。
- 防止异常市场状态下失控暴露。

## 5. 冻结生产策略（当前有效）
- 策略名：`A_ma100_frozen`
- 配置：`config/v31_phase_d_A_ma100_frozen_20260302.json`
- 关键参数：
  - `tactical_gate_mode = MA100`
  - `early_struct_gate_mode = MA100`
  - `early_struct_q20 = 0.85`（Top15%）
  - `early_struct_q10 = 0.95`（Top5%）
  - `early_struct_top20_mult = 0.9`
  - `early_struct_top10_mult = 0.8`
  - `shock_risk_cut = 0.2`
  - `shock_high_cut = 0.4`

## 6. 生产运行总流程
1. 一键入口
```bash
python scripts/run_v31_prod_daily.py
```

2. 执行顺序
1. Build unified sentiment（DB upsert）
2. UTRBE 日更（DB -> 特征与状态）
3. V30 features / labels
4. Structural + Shock full infer
5. Risk aggregate + allocation
6. Backtest eval
7. Lowfreq recovery
8. Ops monitor report

3. 报告输出
- `output/v31_ops_monitor/UTRBEV3_daily_report_YYYY-MM-DD.md`
- `output/v31_ops_monitor/summary.json`
- `output/v31_ops_monitor/strategy_120d.png`

## 7. 可观测性与治理
1. 策略冻结信息入报告
- `policy.name`
- `policy.config_json`
- `policy.tactical_gate_mode`
- `policy.early_struct_gate_mode`

2. 健康检查阈值
- `dd_reduction >= 0.40`
- `cagr_impact < 0.02`
- 触发频率、持续时长、全清仓约束

3. 解释层
- 日报内置“解释版（更易懂）”五段口径：
  - 今天整体安全吗
  - 结构轨/冲击轨说明
  - Early-Structural 说明
  - 仓位原因说明
  - Tactical 说明

## 8. 当前基线结果（截至 2026-03-02）
- 全样本（2016-01-04 ~ 2026-03-02）：
  - `cagr_impact = 0.023688`
  - `dd_reduction = 0.470741`
- Rolling 5Y 平均：
  - `cagr_impact = 0.014499`
  - `dd_reduction = 0.460424`
- 杠杆扩展：
  - 主版本 `1.15x`
  - 上限 `1.2x`
  - 已完成单月 `-40%` 冲击模拟测试

## 9. 子文档索引
1. Early 详细设计：
- `docs/2026-03-02-独立的 Early-Structural 模型设计.md`
2. 当日实验报告：
- `docs/V31_EXPERIMENT_REPORT_2026-03-02.md`
3. 生产运行手册（冻结策略版）：
- `docs/V31_PROD_DAILY_RUNBOOK_2026-03-02.md`
4. 运维监控手册：
- `docs/V31_OPS_MONITOR_RUNBOOK.md`

## 10. 变更规则（建议）
1. 任何生产参数变更，先出实验报告（含回测与压力测试）。
2. 通过后再生成新日期版本的冻结配置与 runbook。
3. 未更新总览文档前，不执行生产策略切换。
