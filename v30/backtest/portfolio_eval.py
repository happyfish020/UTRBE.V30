from __future__ import annotations

import numpy as np
import pandas as pd


def max_drawdown(nav: pd.Series) -> float:
    peak = nav.cummax()
    dd = nav / peak - 1.0
    return float(dd.min()) if len(dd) else 0.0


def ulcer_index(nav: pd.Series) -> float:
    peak = nav.cummax()
    dd = nav / peak - 1.0
    return float(np.sqrt(np.mean(np.square(dd.values)))) if len(dd) else 0.0


def recovery_time_days(nav: pd.Series) -> int:
    peak = nav.cummax()
    underwater = (nav < peak).astype(int).to_numpy()
    best = 0
    cur = 0
    for v in underwater:
        if v == 1:
            cur += 1
            if cur > best:
                best = cur
        else:
            cur = 0
    return int(best)


def annualized_return(nav: pd.Series, trading_days_per_year: int = 252) -> float:
    n = len(nav)
    if n <= 1:
        return 0.0
    final = float(nav.iloc[-1])
    if final <= 0:
        return 0.0
    return float(final ** (trading_days_per_year / n) - 1.0)


def tail_risk_5pct(daily_ret: pd.Series) -> float:
    if len(daily_ret) == 0:
        return 0.0
    q = float(daily_ret.quantile(0.05))
    tail = daily_ret[daily_ret <= q]
    return float(tail.mean()) if len(tail) else 0.0


def summarize(nav: pd.Series, daily_ret: pd.Series) -> dict:
    return {
        'days': int(len(nav)),
        'final_nav': float(nav.iloc[-1]) if len(nav) else 1.0,
        'total_return': float(nav.iloc[-1] - 1.0) if len(nav) else 0.0,
        'annualized_return': annualized_return(nav),
        'max_drawdown': max_drawdown(nav),
        'ulcer_index': ulcer_index(nav),
        'recovery_time_days': recovery_time_days(nav),
        'tail_risk_5pct_mean': tail_risk_5pct(daily_ret),
        'vol_annualized': float(daily_ret.std(ddof=0) * np.sqrt(252)) if len(daily_ret) > 1 else 0.0,
    }
