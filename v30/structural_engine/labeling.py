from __future__ import annotations

import numpy as np
import pandas as pd


def add_structural_label(
    df: pd.DataFrame,
    horizon_days: int = 30,
    drawdown_threshold: float = 0.10,
    persistence_days: int = 15,
) -> pd.DataFrame:
    """Build Structural event label using forward window conditions.

    Label = 1 when within next horizon_days:
    - max(drawdown_252) >= drawdown_threshold
    - number of days drawdown_252 >= drawdown_threshold >= persistence_days
    """
    out = df.sort_values('date').reset_index(drop=True).copy()
    dd = pd.to_numeric(out['drawdown_252'], errors='coerce').to_numpy(dtype=float)
    n = len(out)

    y = np.full(n, np.nan, dtype=float)
    fw_max = np.full(n, np.nan, dtype=float)
    fw_persist = np.full(n, np.nan, dtype=float)

    for i in range(n):
        j0 = i + 1
        j1 = i + 1 + int(horizon_days)
        if j1 > n:
            continue
        w = dd[j0:j1]
        if np.isnan(w).all():
            continue
        w_max = np.nanmax(w)
        w_persist = float(np.nansum(w >= float(drawdown_threshold)))
        fw_max[i] = w_max
        fw_persist[i] = w_persist
        y[i] = 1.0 if (w_max >= float(drawdown_threshold) and w_persist >= float(persistence_days)) else 0.0

    out['label_structural'] = y
    out['fw_max_dd_30d'] = fw_max
    out['fw_persist_dd_30d'] = fw_persist
    return out
