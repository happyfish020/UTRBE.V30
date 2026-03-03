# 每日风险报告（易读版）

## 今日结论（2026-03-02）
- 当前状态（模型原始）：正常模式
- 当前状态（执行后）：正常执行
- 操作建议：继续持有，不操作。
- 执行信号：NONE
- 仓位状态：NORMAL

## 核心指标
- 风险概率（lead5 / lead10）：0.103 / 0.204
- drawdown_252: 1.37%
- vol20: 0.008109
- hurst_100: 0.777617
- ret5d: 0.005276
- srs: 0.0
- srs_accel: 0.0
- sentiment_signal: 0
- trend_score: 0.2024

## 人话解释（怎么读这些数值）
- lead5：偏短窗口风险概率（更灵敏，容易先动）。
- lead10：偏长窗口风险概率（更稳健，用于防守确认）。
- 风险分级（按 reference_prob）：
  - 低风险：< 0.35（通常继续持有）
  - 观察区：0.35 - 0.55（先观察，必要时小幅减仓）
  - 高风险：>= 0.55（进入防守，考虑明显减仓）
- 当前 reference_prob=0.103，对应 action=HOLD_STRATEGIC。
- trend_score 分级：< 0.60 常规；0.60-0.70 关注；>= 0.70 趋势预警区。

## 证据链（双轨明细）
- mode(模型原始)=NORMAL, 执行状态=正常执行
- active_track=LEAD5, action=HOLD_STRATEGIC, execution_signal=NONE
- prewarning_type=NONE, warning_source=NONE
- warning_sources=NONE
- reference_prob=0.1034
- switch_reason=high_hurst

## 近10日分层信号轨迹
- dates(10d): 2026-03-02
- mode 轨迹（模型原始）: NORMAL
- 执行状态轨迹（执行后）: 正常执行
- smart warning tier (WATCH): none
- pre-warning tier (PREWARNING): none

## 数据新鲜度（Data Freshness）
- 报告日期: 2026-03-02
- 最近应有交易日: 2026-03-02
- 报告日期与应有交易日一致。
- 数据源明细：
  - lead5: latest=2026-03-02, 已覆盖当天
  - lead10: latest=2026-03-02, 已覆盖当天
  - market_features: latest=2026-03-02, 已覆盖当天
- 结论：关键数据源均已覆盖当天。