from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
import pandas as pd


@dataclass
class LowfreqConfig:
    freq: str = "W-FRI"  # W-FRI | M
    drawdown_threshold: float = 0.12
    max_horizon: int = 52
    recovery_target: float = 1.00
    min_event_len: int = 2
    gate_normal_rss: float = 0.55
    gate_normal_rsts: float = 0.75
    gate_caution_rss: float = 0.35
    gate_caution_rsts: float = 0.45


def build_lowfreq_prices(
    daily_df: pd.DataFrame,
    date_col: str = "date",
    price_col: str = "",
    ret_col: str = "spy_ret1d",
    freq: str = "W-FRI",
) -> pd.DataFrame:
    x = daily_df.copy()
    x[date_col] = pd.to_datetime(x[date_col]).dt.tz_localize(None)
    x = x.sort_values(date_col).reset_index(drop=True)

    if price_col and price_col in x.columns:
        px = pd.to_numeric(x[price_col], errors="coerce").ffill()
    elif ret_col in x.columns:
        ret = pd.to_numeric(x[ret_col], errors="coerce").fillna(0.0)
        px = (1.0 + ret).cumprod()
    else:
        raise ValueError(f"missing price source: {price_col or ret_col}")

    z = pd.DataFrame({"date": x[date_col], "price": px}).dropna().copy()
    z = z.set_index("date").resample(freq).last().dropna().reset_index()
    return z


def _fit_recovery_curve(y: np.ndarray, t: np.ndarray) -> tuple[float, float, float]:
    if len(y) < 3:
        return 0.0, 0.0, 0.0
    a = float(max(1e-6, np.nanmax(y)))
    valid = (y > 0.0) & (y < a * 0.98) & np.isfinite(y) & np.isfinite(t)
    if valid.sum() < 3:
        return a, 0.0, 0.0
    z = np.log(1.0 - (y[valid] / a))
    slope = np.polyfit(t[valid], z, 1)[0]
    b = float(max(0.0, -slope))
    y_hat = a * (1.0 - np.exp(-b * t))
    ss_res = float(np.nansum((y - y_hat) ** 2))
    ss_tot = float(np.nansum((y - np.nanmean(y)) ** 2))
    r2 = 0.0 if ss_tot <= 1e-12 else float(max(-1.0, min(1.0, 1.0 - ss_res / ss_tot)))
    return a, b, r2


def _detect_events(lowf: pd.DataFrame, cfg: LowfreqConfig) -> list[dict]:
    x = lowf.copy().sort_values("date").reset_index(drop=True)
    x["peak"] = x["price"].cummax()
    x["drawdown"] = x["price"] / x["peak"] - 1.0
    in_dd = x["drawdown"] <= -abs(float(cfg.drawdown_threshold))

    starts = x.index[in_dd & (~in_dd.shift(1, fill_value=False))]
    ends = x.index[(~in_dd) & in_dd.shift(1, fill_value=False)]
    if len(x) > 0 and bool(in_dd.iloc[-1]):
        ends = pd.Index(list(ends) + [len(x) - 1])

    ev = []
    for s in starts:
        e = next((j for j in ends if j >= s), len(x) - 1)
        seg = x.iloc[s : e + 1]
        if len(seg) < int(cfg.min_event_len):
            continue
        trough_i = int(seg["price"].idxmin())
        start_price = float(x.loc[s, "peak"])
        trough_price = float(x.loc[trough_i, "price"])
        if start_price - trough_price <= 1e-12:
            continue

        target_px = trough_price + (start_price - trough_price) * float(cfg.recovery_target)
        after = x.loc[trough_i:].copy()
        hit = after.index[after["price"] >= target_px]
        end_i = int(hit[0]) if len(hit) else min(len(x) - 1, trough_i + int(cfg.max_horizon))
        curve = x.loc[trough_i:end_i].copy()

        rec_ratio = (curve["price"] - trough_price) / (start_price - trough_price)
        rec_ratio = pd.to_numeric(rec_ratio, errors="coerce").clip(lower=0.0)
        t = np.arange(len(curve), dtype=float)
        y = rec_ratio.to_numpy(dtype=float)
        a, b, r2 = _fit_recovery_curve(y, t)
        if b > 0 and a > 0.50:
            t50 = float(-math.log(max(1e-9, 1.0 - 0.5 / a)) / b)
        else:
            t50 = float(cfg.max_horizon)
        rss = float(max(0.0, min(1.0, 1.0 - min(t50, cfg.max_horizon) / max(1.0, cfg.max_horizon))))
        rsts = float(max(0.0, min(1.2, y[-1] if len(y) else 0.0)))

        ev.append(
            {
                "start_date": str(pd.to_datetime(x.loc[s, "date"]).date()),
                "trough_date": str(pd.to_datetime(x.loc[trough_i, "date"]).date()),
                "end_date": str(pd.to_datetime(x.loc[end_i, "date"]).date()),
                "drawdown_depth": float((trough_price / start_price) - 1.0),
                "event_len": int(len(seg)),
                "recovery_len": int(len(curve)),
                "recovered_target": bool(len(hit) > 0),
                "a_recovery_amplitude": float(a),
                "b_recovery_speed": float(b),
                "curve_r2": float(r2),
                "rss": rss,
                "rsts": rsts,
                "t50": float(t50),
            }
        )
    return ev


def _persistence_adjustment(flow: pd.Series | None) -> float:
    if flow is None or len(flow.dropna()) < 5:
        return 0.0
    s = pd.to_numeric(flow, errors="coerce").dropna()
    if len(s) < 5:
        return 0.0
    z = (s.iloc[-1] - s.mean()) / max(1e-9, s.std(ddof=0))
    return float(max(-0.25, min(0.25, z * 0.08)))


def _gate_action(rss: float, rsts: float, pa: float, in_drawdown: bool, cfg: LowfreqConfig) -> tuple[str, str, str]:
    score_rss = float(rss + pa)
    score_rsts = float(rsts + pa)
    if in_drawdown:
        return "ALERT", "REDUCE", "CONFIRM_SHORT_TERM_WARNING"
    if score_rss >= float(cfg.gate_normal_rss) and score_rsts >= float(cfg.gate_normal_rsts):
        return "NORMAL", "ADD", "VETO_SHORT_TERM_WARNING"
    if score_rss >= float(cfg.gate_caution_rss) and score_rsts >= float(cfg.gate_caution_rsts):
        return "CAUTION", "HOLD", "NEUTRAL_CONFIRMATION"
    return "ALERT", "REDUCE", "CONFIRM_SHORT_TERM_WARNING"


def compute_lowfreq_recovery(
    lowf_price_df: pd.DataFrame,
    cfg: LowfreqConfig,
    flow_series: pd.Series | None = None,
) -> dict:
    x = lowf_price_df.copy().sort_values("date").reset_index(drop=True)
    x["peak"] = x["price"].cummax()
    x["drawdown"] = x["price"] / x["peak"] - 1.0
    events = _detect_events(x, cfg)

    if events:
        ev = pd.DataFrame(events)
        latest = ev.iloc[-1].to_dict()
        rss = float(latest["rss"])
        rsts = float(latest["rsts"])
    else:
        latest = {}
        rss = 0.0
        rsts = 0.0

    pa = _persistence_adjustment(flow_series)
    in_dd = bool(float(x["drawdown"].iloc[-1]) <= -abs(float(cfg.drawdown_threshold)))
    gate, action, confirm = _gate_action(rss, rsts, pa, in_dd, cfg)

    return {
        "freq": cfg.freq,
        "window": {
            "start": str(pd.to_datetime(x["date"].iloc[0]).date()),
            "end": str(pd.to_datetime(x["date"].iloc[-1]).date()),
            "rows": int(len(x)),
        },
        "thresholds": {
            "drawdown_threshold": float(cfg.drawdown_threshold),
            "max_horizon": int(cfg.max_horizon),
            "recovery_target": float(cfg.recovery_target),
        },
        "latest_state": {
            "drawdown_now": float(x["drawdown"].iloc[-1]),
            "in_major_drawdown": in_dd,
            "rss": rss,
            "rsts": rsts,
            "persistence_adjustment": float(pa),
            "gate": gate,
            "action_hint": action,
            "short_term_confirmation": confirm,
        },
        "events": events,
        "latest_event": latest,
    }

