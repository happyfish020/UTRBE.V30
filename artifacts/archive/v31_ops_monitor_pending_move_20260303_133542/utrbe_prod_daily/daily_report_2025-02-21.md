# 每日风险报告（易读版）

## 今日结论（2025-02-21）
- 当前状态：正常模式
- 操作建议：观察层预警：有噪声风险，建议提高警惕。
- 执行信号：NONE
- 仓位状态：NORMAL

## 核心指标
- 风险概率（lead5 / lead10）：0.129 / 0.204
- drawdown_252: 2.12%
- vol20: 0.007392
- hurst_100: 0.761332
- ret5d: -0.016008
- srs: 1.1402
- srs_accel: 1.0295
- sentiment_signal: 1
- trend_score: 0.3891

## 人话解释（怎么读这些数值）
- lead5：偏短窗口风险概率（更灵敏，容易先动）。
- lead10：偏长窗口风险概率（更稳健，用于防守确认）。
- 风险分级（按 reference_prob）：
  - 低风险：< 0.35（通常继续持有）
  - 观察区：0.35 - 0.55（先观察，必要时小幅减仓）
  - 高风险：>= 0.55（进入防守，考虑明显减仓）
- 当前 reference_prob=0.129，对应 action=SHORT_TERM_WATCH。
- trend_score 分级：< 0.60 常规；0.60-0.70 关注；>= 0.70 趋势预警区。

## 证据链（双轨明细）
- mode=NORMAL, active_track=LEAD5, action=SHORT_TERM_WATCH, execution_signal=NONE
- prewarning_type=WATCH_NOISE, warning_source=shortterm_watch_guard
- warning_sources=high_hurst|sentiment_factor|shortterm_watch_guard
- reference_prob=0.1293
- switch_reason=high_hurst|shortterm_watch_guard

## 近10日分层信号轨迹
- dates(10d): 2025-02-07->2025-02-10->2025-02-11->2025-02-12->2025-02-13->2025-02-14->2025-02-18->2025-02-19->2025-02-20->2025-02-21
- smart warning tier (WATCH): none->none->none->none->none->none->none->none->none->warning
- pre-warning tier (PREWARNING): none->none->none->none->none->none->none->none->none->none

## 数据新鲜度（Data Freshness）
- 报告日期: 2025-02-21
- 最近应有交易日: 2026-02-23
- ⚠️ 报告晚于应有交易日 367 天。
- 数据源明细：
  - lead5: latest=2026-02-20, 缺少最近 3 天
  - lead10: latest=2026-02-20, 缺少最近 3 天
  - market_features: latest=2025-02-21, 缺少最近 367 天
- ⚠️ 结论：至少一个数据源缺失当天数据。