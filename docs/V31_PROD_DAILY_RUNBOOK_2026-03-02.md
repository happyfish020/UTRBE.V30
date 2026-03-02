# V31 生产运行手册_2026-03-02

## 1. 当前生产冻结策略
- 策略名：`A_ma100_frozen`
- 配置文件：`config/v31_phase_d_A_ma100_frozen_20260302.json`
- 核心门控：
  - `tactical_gate_mode = MA100`
  - `early_struct_gate_mode = MA100`
  - `early_struct_top20_mult = 0.90`
  - `early_struct_top10_mult = 0.80`

## 2. 一键生产运行（推荐）
```bash
python scripts/run_v31_prod_daily.py
```

默认行为（已切到冻结策略）：
1. 无参数时自动跑最近交易日（`start=end=last trading day`）。
2. 先跑 unified sentiment（DB-first），再跑 UTRBE 日更，再跑 V31 全链路。
3. 默认使用冻结配置：`config/v31_phase_d_A_ma100_frozen_20260302.json`。
4. 默认将策略标识写入每日报告（`A_ma100_frozen`）。

## 3. 指定区间运行（可选）
```bash
python scripts/run_v31_prod_daily.py \
  --start 2026-02-20 \
  --end 2026-03-02 \
  --tuning-json config/v31_phase_d_A_ma100_frozen_20260302.json \
  --policy-name A_ma100_frozen
```

## 4. 仅重跑 Ops Report（不重跑全链路）
```bash
python scripts/run_v31_ops_monitor.py \
  --backtest-summary-json output/exp_tactical_ma100_backtest/summary.json \
  --risk-summary-json output/v31_risk_aggregate_hardgate_lfscan_121_full_2016_now/summary.json \
  --allocation-csv output/v31_risk_aggregate_hardgate_lfscan_121_full_2016_now/daily_allocation.csv \
  --backtest-daily-csv output/exp_tactical_ma100_backtest/v30_backtest_daily.csv \
  --reference-summary-json output/v31_backtest_eval_hardgate_gatepass/summary.json \
  --policy-name A_ma100_frozen \
  --policy-config-json config/v31_phase_d_A_ma100_frozen_20260302.json \
  --output-dir output/v31_ops_monitor
```

调试模式（不写 DB）：
```bash
python scripts/run_v31_ops_monitor.py --skip-db-upsert ...
```

## 5. 每日输出检查清单
- 报告：
  - `output/v31_ops_monitor/UTRBEV3_daily_report_YYYY-MM-DD.md`
  - `output/v31_ops_monitor/daily_report.md`
- 指标：
  - `output/v31_ops_monitor/summary.json`
- 图表：
  - `output/v31_ops_monitor/strategy_120d.png`

关键字段必须出现：
1. `policy.name = A_ma100_frozen`
2. `policy.config_json = config/v31_phase_d_A_ma100_frozen_20260302.json`
3. `policy.tactical_gate_mode = MA100`
4. `policy.early_struct_gate_mode = MA100`

## 6. 当前健康阈值
1. `dd_reduction >= 0.40`
2. `cagr_impact < 0.02`
3. 年均触发次数建议在 `[2, 8]`
4. 平均触发持续 `< 15` 交易日
5. `full_liquidation_days = 0`

## 7. 备注
- 当前冻结策略在全样本下满足 DD 压缩目标，但 `cagr_impact` 仍略高于 2% 门槛。
- 生产阶段先保持配置冻结，后续优化必须走新实验报告和对比回测流程。
