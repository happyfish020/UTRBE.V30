# V31 Ops Monitor SQL Queries

Version: v1.0.0  
Updated: 2026-02-24

## 1) 指定交易日完整审计包
```sql
-- 替换日期
SET @d = '2026-02-23';

SELECT * FROM v31_ops_monitor_summary_archive WHERE date = @d;
SELECT * FROM v31_ops_monitor_snapshot WHERE date = @d;
SELECT * FROM v31_ops_monitor_health_checks WHERE date = @d ORDER BY check_name;
SELECT * FROM v31_ops_monitor_episodes WHERE date = @d ORDER BY episode_idx;
```

## 2) 最近20个交易日健康通过率
```sql
SELECT
  check_name,
  COUNT(*) AS obs_days,
  SUM(check_pass) AS pass_days,
  ROUND(SUM(check_pass) / COUNT(*), 4) AS pass_rate
FROM v31_ops_monitor_health_checks
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 40 DAY)
GROUP BY check_name
ORDER BY check_name;
```

## 3) 最近20个交易日关键风险轨迹
```sql
SELECT
  date,
  risk_regime,
  allocation_action,
  hard_gate_reason,
  final_allocation,
  p_structural_today,
  p_shock_today,
  cagr_impact,
  dd_reduction
FROM v31_ops_monitor_snapshot
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 40 DAY)
ORDER BY date DESC
LIMIT 20;
```

## 4) 检查“未通过项”日期
```sql
SELECT
  date,
  SUM(CASE WHEN check_pass = 0 THEN 1 ELSE 0 END) AS failed_checks
FROM v31_ops_monitor_health_checks
GROUP BY date
HAVING failed_checks > 0
ORDER BY date DESC;
```

## 5) 低频恢复层联查（同日）
```sql
SET @d = '2026-02-23';

SELECT * FROM v31_lowfreq_recovery_summary WHERE date = @d;
SELECT * FROM v31_lowfreq_recovery_events WHERE date = @d ORDER BY event_idx;
```

## 6) V2 与 V31 同日对照
```sql
SET @d = '2026-02-23';

SELECT * FROM v2_dualtrack_daily_states WHERE date = @d;
SELECT * FROM v31_ops_monitor_snapshot WHERE date = @d;
```

