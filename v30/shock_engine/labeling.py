from __future__ import annotations

import numpy as np
import pandas as pd


def add_shock_label_proxy(
    df: pd.DataFrame,
    horizon_days: int = 5,
    drop_threshold: float = 0.07,
    early_share_threshold: float = 0.60,
    adaptive_drop_threshold: bool = False,
    target_positive_rate: float = 0.04,
    min_drop_threshold: float = 0.02,
    max_drop_threshold: float = 0.10,
    use_stress_override: bool = True,
    stress_gate: float = 0.55,
) -> pd.DataFrame:
    """Bootstrap proxy for shock label using drawdown path changes.

    Proxy interpretation:
    - total_dd_increase_5d = max(dd in next 5d) - current dd
    - early_dd_increase_2d = max(dd in next 2d) - current dd
    Label=1 when:
      total_dd_increase_5d >= drop_threshold
      and early_dd_increase_2d / total_dd_increase_5d >= early_share_threshold

    Note: this is a proxy in absence of dedicated intraday/crash event table.
    """
    out = df.sort_values('date').reset_index(drop=True).copy()
    dd = pd.to_numeric(out['drawdown_252'], errors='coerce').to_numpy(dtype=float)
    stress = pd.to_numeric(
        out.get('breadth_damage_score', out.get('structural_pressure_score', 0.0)),
        errors='coerce',
    ).to_numpy(dtype=float)
    n = len(out)

    y = np.full(n, np.nan, dtype=float)
    fw_inc5 = np.full(n, np.nan, dtype=float)
    fw_inc2 = np.full(n, np.nan, dtype=float)
    fw_share2 = np.full(n, np.nan, dtype=float)

    for i in range(n):
        if i + int(horizon_days) >= n:
            continue
        cur = dd[i]
        if np.isnan(cur):
            continue

        w5 = dd[i + 1 : i + 1 + int(horizon_days)]
        w2 = dd[i + 1 : i + 1 + 2]
        if np.isnan(w5).all() or np.isnan(w2).all():
            continue

        inc5 = float(np.nanmax(w5) - cur)
        inc2 = float(np.nanmax(w2) - cur)
        inc5 = max(0.0, inc5)
        inc2 = max(0.0, inc2)
        share2 = (inc2 / inc5) if inc5 > 1e-12 else 0.0

        fw_inc5[i] = inc5
        fw_inc2[i] = inc2
        fw_share2[i] = share2

    valid = np.isfinite(fw_inc5) & np.isfinite(fw_share2)
    eff_drop = float(drop_threshold)
    if bool(adaptive_drop_threshold) and np.any(valid):
        target = min(max(float(target_positive_rate), 0.005), 0.30)
        share_mask = valid & (fw_share2 >= float(early_share_threshold))
        pool = fw_inc5[share_mask]
        if pool.size < 20:
            pool = fw_inc5[valid]
        if pool.size > 0:
            q = float(np.quantile(pool, max(0.0, min(1.0, 1.0 - target))))
            eff_drop = float(np.clip(q, float(min_drop_threshold), float(max_drop_threshold)))

    for i in range(n):
        if not np.isfinite(fw_inc5[i]) or not np.isfinite(fw_share2[i]):
            continue
        primary = bool(fw_inc5[i] >= eff_drop and fw_share2[i] >= float(early_share_threshold))
        stress_override = False
        if bool(use_stress_override):
            stress_i = stress[i] if i < len(stress) and np.isfinite(stress[i]) else 0.0
            stress_override = bool(stress_i >= float(stress_gate) and fw_inc5[i] >= 0.70 * eff_drop)
        y[i] = 1.0 if (primary or stress_override) else 0.0

    out['label_shock'] = y
    out['fw_dd_increase_5d'] = fw_inc5
    out['fw_dd_increase_2d'] = fw_inc2
    out['fw_dd_2d_share'] = fw_share2
    out['label_shock_drop_threshold_used'] = eff_drop
    return out
