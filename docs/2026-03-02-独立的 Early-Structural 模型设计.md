# 2026-03-02 独立的 Early-Structural 模型设计（已实现版）

## 1. 目标与范围
- 目标：在不破坏主风控框架（Structural + Shock + Hard Gate）的前提下，引入一层“提前但温和”的降仓机制。
- 核心思想：Early 层不做 hard gate，不做二元清仓，只做轻度乘数（`1.0 -> 0.9 -> 0.8`）。
- 当前生产策略冻结为：`A_ma100_frozen`。

## 2. 总体架构（实施后）
1. 特征与预测层
- 输入：`v30_features_daily`（含 `vol20`, `ret5d`, `market_price` 等）。
- 模型输出：`p_structural`, `p_shock`。

2. 风险聚合层（Phase-D）
- 文件：`v30/risk_aggregation/aggregate.py`
- 新增 Early 分数计算与分位阈值机制，输出 `early_struct_level`。

3. 仓位决策层（Allocation）
- 文件：`v30/allocation/decision.py`
- 在 Tactical 与 Shock 修正后叠加 Early 乘数。
- 增加 Tactical 与 Early 的 gate 模式控制（`MA100/MA200/RET20D_NEG/NONE`）。

4. 生产编排与报告层
- 生产入口：`scripts/run_v31_prod_daily.py`
- 运维报告：`scripts/run_v31_ops_monitor.py`
- 报告展示：策略冻结状态、门控状态、Early 状态与解释版文本。

## 3. Early-Structural 详细设计
### 3.1 特征工程
- `z_vol20 = zscore(vol20, rolling_window)`
- `z_mom = -zscore(ret5d, rolling_window)`
- `ma20_slope = MA20(t) - MA20(t-5)`
- `z_slope = -zscore(ma20_slope, rolling_window)`

默认参数：
- `early_struct_z_window = 252`
- `early_struct_z_min_periods = 60`
- `early_struct_ma_window = 20`
- `early_struct_slope_lag = 5`

### 3.2 分数构造
```
early_score_raw =
    0.5 * z_vol20
  + 0.3 * z_mom
  + 0.2 * z_slope

early_score = sigmoid(early_score_raw)
```

### 3.3 分位阈值与分级
- 阈值使用历史分布，不用固定绝对值。
- 为避免未来函数，阈值基于 `score.shift(1)` 的 expanding quantile。

默认生产阈值（冻结策略）：
- `early_struct_q20 = 0.85`（Top15%）
- `early_struct_q10 = 0.95`（Top5%）

分级：
- `early_struct_level = 0`：常态
- `early_struct_level = 1`：Top15%
- `early_struct_level = 2`：Top5%

### 3.4 乘数映射
- `level=0 -> early_struct_multiplier=1.0`
- `level=1 -> early_struct_multiplier=0.9`
- `level=2 -> early_struct_multiplier=0.8`

设计约束：
- Early 层只用于“轻降”，不直接触发危机仓位。
- Early 层始终作为乘数层，不替代 Structural/Shock/Hard Gate。

## 4. Gate 机制详细设计
### 4.1 Early Gate
- 参数：`early_struct_gate_mode`
- 支持：
  - `NONE`
  - `MA100`（`close < MA100` 时 Early 生效）
  - `MA200`（`close < MA200` 时 Early 生效）

### 4.2 Tactical Gate
- 参数：`tactical_gate_mode`
- 支持：
  - `NONE`
  - `MA100`
  - `RET20D_NEG`（或别名 `MOM20_NEG`）

### 4.3 A_ma100 冻结策略定义
- 文件：`config/v31_phase_d_A_ma100_frozen_20260302.json`
- 固定项：
  - `early_struct_gate_mode = MA100`
  - `tactical_gate_mode = MA100`
  - `early_struct_top20_mult = 0.9`
  - `early_struct_top10_mult = 0.8`
  - `shock_risk_cut = 0.2`
  - `shock_high_cut = 0.4`

## 5. 输出字段与接口
### 5.1 聚合层输出新增
- `early_score_raw`
- `early_score`
- `early_score_q20_thr`
- `early_score_q10_thr`
- `early_struct_level`

### 5.2 决策层输出新增
- `early_struct_multiplier`
- `tactical_gate_on`

### 5.3 运维报告新增
- 传入策略参数：
  - `--policy-name`
  - `--policy-config-json`
- `summary.json` 新增：
  - `policy.name`
  - `policy.config_json`
  - `policy.tactical_gate_mode`
  - `policy.early_struct_gate_mode`
  - `policy.early_struct_top20_mult`
  - `policy.early_struct_top10_mult`
- 日报新增：
  - 冻结策略标识
  - 门控状态
  - Early 乘数说明
  - “解释版（更易懂）”五段口径

## 6. 生产流程详细设计
1. `run_v31_prod_daily.py` 默认配置
- `--tuning-json` 默认改为 `config/v31_phase_d_A_ma100_frozen_20260302.json`
- `--policy-name` 默认 `A_ma100_frozen`

2. 数据流
- DB -> unified sentiment -> UTRBE -> v30 feature/labels -> full infer -> risk aggregate -> backtest eval -> lowfreq -> ops monitor。

3. 报告产物
- `output/v31_ops_monitor/UTRBEV3_daily_report_YYYY-MM-DD.md`
- `output/v31_ops_monitor/summary.json`
- `output/v31_ops_monitor/strategy_120d.png`

## 7. 当日验证结果（2026-03-02）
### 7.1 全样本（A_ma100）
- 区间：`2016-01-04 ~ 2026-03-02`
- `cagr_impact = 0.023688`
- `dd_reduction = 0.470741`

### 7.2 Rolling 5Y 稳定性
- 窗口数：`66`
- 平均 `cagr_impact = 0.014499`
- 平均 `dd_reduction = 0.460424`

### 7.3 Forward Walk（重训）
- 测试段：`2020~2024`
- 平均 `cagr_impact = 0.024670`
- 平均 `dd_reduction = 0.064421`

### 7.4 杠杆测试
- 主版本：`1.15x`
- 上限：`1.2x`
- 极端压力（模拟单月 -40%）下，杠杆版本回撤仍显著小于 benchmark 同期冲击损失。

## 8. 设计决策结论
- Early 层定位正确：提前、温和、非 hard gate。
- MA100 gate 显著抑制牛市期不必要降仓，是当前冻结策略的关键约束。
- 报告层已接入策略冻结信息与解释文本，满足生产可观测性。

## 9. 后续优化方向
1. 将报告中的“趋势条件”从“无法判定”升级为明确的 `price vs MA100` 状态（需在报告输入保留 `market_price`）。
2. 对 Forward Walk 负迁移年份（2021~2024）做分层归因（shock/structural/early/tactical 各层贡献）。
3. 在不降低 `dd_reduction` 的前提下，继续压缩 `cagr_impact` 到 `<2%`。
