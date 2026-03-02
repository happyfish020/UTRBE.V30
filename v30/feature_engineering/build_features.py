from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

import pandas as pd


@dataclass
class FeatureBuildConfig:
    low_vol_cut: float = 0.010
    high_hurst_cut: float = 0.60
    breadth_window: int = 20
    vol_regime_window: int = 60


def _clip01(x: pd.Series) -> pd.Series:
    return x.clip(lower=0.0, upper=1.0)


def load_market_data(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["date"])
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    if "is_trading_day" in df.columns:
        df = df[df["is_trading_day"].fillna(1).astype(int) == 1].copy()

    req = ["date", "vol20", "hurst_100", "drawdown_252", "ret5d"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for c in [
        "vol20",
        "hurst_100",
        "drawdown_252",
        "drawdown_126",
        "ret5d",
        "ret20d",
        "ibb_ret20d",
        "ibb_drawdown_126",
        "ibb_rel20d",
        "external_event",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.sort_values("date").reset_index(drop=True)


def load_breadth_data(
    csv_path: Path,
    date_col: str = "date",
    value_col: str = "breadth",
    price_col: str = "price",
    credit_col: str = "credit",
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    cols = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    df.columns = cols
    if date_col not in df.columns:
        raise ValueError(f"breadth date column not found: {date_col}")
    if value_col not in df.columns:
        raise ValueError(f"breadth value column not found: {value_col}")
    pick = [date_col, value_col]
    has_price = bool(price_col) and price_col in df.columns
    has_credit = bool(credit_col) and credit_col in df.columns
    if has_price:
        pick.append(price_col)
    if has_credit:
        pick.append(credit_col)
    out = df[pick].copy()
    ren = {date_col: "date", value_col: "breadth_real_stress"}
    if has_price:
        ren[price_col] = "market_price"
    if has_credit:
        ren[credit_col] = "credit_real"
    out = out.rename(columns=ren)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["breadth_real_stress"] = pd.to_numeric(out["breadth_real_stress"], errors="coerce")
    if "market_price" in out.columns:
        out["market_price"] = pd.to_numeric(out["market_price"], errors="coerce")
    if "credit_real" in out.columns:
        out["credit_real"] = pd.to_numeric(out["credit_real"], errors="coerce")
    out = out.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
    return out.sort_values("date").reset_index(drop=True)


def attach_breadth(df: pd.DataFrame, breadth_df: pd.DataFrame | None) -> pd.DataFrame:
    if breadth_df is None or breadth_df.empty:
        x = df.copy()
        x["breadth_real_stress"] = pd.NA
        x["market_price"] = pd.NA
        x["credit_real"] = pd.NA
        return x
    return df.merge(breadth_df, on="date", how="left")


def build_features(df: pd.DataFrame, cfg: FeatureBuildConfig) -> pd.DataFrame:
    out = df.copy()

    out["dd_delta_1d"] = out["drawdown_252"].diff()
    out["ret5d_delta_1d"] = out["ret5d"].diff()
    out["ret5d_median_20"] = pd.to_numeric(out["ret5d"], errors="coerce").rolling(20, min_periods=10).median()

    out["low_vol_flag"] = (out["vol20"] < float(cfg.low_vol_cut)).astype(int)
    out["high_hurst_flag"] = (out["hurst_100"] > float(cfg.high_hurst_cut)).astype(int)
    out["drawdown_gt5_flag"] = (out["drawdown_252"] >= 0.05).astype(int)

    out["shock_like_flag"] = (
        (out["ret5d"] <= -0.02)
        & (out["drawdown_252"] >= 0.03)
    ).astype(int)

    # Structural breadth features (Step3): use external breadth source when available.
    w = max(5, int(cfg.breadth_window))
    vw = max(w, int(cfg.vol_regime_window))
    b = pd.to_numeric(out.get("breadth_real_stress"), errors="coerce")
    breadth_available = b.notna().mean() >= 0.40
    out["breadth_source_flag"] = int(breadth_available)

    if breadth_available:
        b = b.ffill().bfill()
        b_base = b.rolling(126, min_periods=20).median()
        b_std = b.rolling(126, min_periods=20).std().replace(0, pd.NA)
        b_z = ((b - b_base) / b_std).fillna(0.0)
        b_up = b.diff().clip(lower=0.0).fillna(0.0)

        out["new_lows_ratio_proxy"] = _clip01((b - 0.05) / 0.20)
        out["ad_spread_proxy"] = (-(b_z / 2.5)).clip(-1.0, 1.0)
        out["sector_sync_breakdown_proxy"] = _clip01(
            0.70 * _clip01((b - 0.07) / 0.18) + 0.30 * _clip01(b_up / 0.03)
        )
        out["leadership_failure_proxy"] = _clip01(
            0.60 * out["sector_sync_breakdown_proxy"] + 0.40 * _clip01(b.rolling(5, min_periods=1).mean() / 0.20)
        )
    else:
        # Fallback when no external breadth is provided.
        dd_up = out["drawdown_252"].diff().clip(lower=0.0).fillna(0.0)
        high_vol_regime = out["vol20"] >= out["vol20"].rolling(vw, min_periods=10).median()

        out["new_lows_ratio_proxy"] = (
            ((out["drawdown_252"] >= 0.08) | (dd_up >= 0.003))
            .astype(float)
            .rolling(w, min_periods=5)
            .mean()
            .fillna(0.0)
        ).clip(0.0, 1.0)

        down_ratio = (out["ret5d"] <= -0.01).astype(float).rolling(w, min_periods=5).mean()
        up_ratio = (out["ret5d"] >= 0.01).astype(float).rolling(w, min_periods=5).mean()
        out["ad_spread_proxy"] = (up_ratio - down_ratio).fillna(0.0).clip(-1.0, 1.0)

        out["sector_sync_breakdown_proxy"] = (
            ((out["ret5d"] <= -0.015) & high_vol_regime.fillna(False))
            .astype(float)
            .rolling(w, min_periods=5)
            .mean()
            .fillna(0.0)
        ).clip(0.0, 1.0)

        out["leadership_failure_proxy"] = (
            ((out["hurst_100"] >= 0.78) & (out["ret5d"] <= -0.01))
            .astype(float)
            .rolling(w, min_periods=5)
            .mean()
            .fillna(0.0)
        ).clip(0.0, 1.0)

    ad_damage = _clip01((-out["ad_spread_proxy"] + 0.20) / 1.20)
    out["breadth_damage_score"] = pd.concat(
        [out["new_lows_ratio_proxy"], ad_damage, out["sector_sync_breakdown_proxy"], out["leadership_failure_proxy"]],
        axis=1,
    ).mean(axis=1, skipna=True).fillna(0.0).clip(0.0, 1.0)

    # lightweight structural pressure score [0,1]
    f_dd = _clip01((out["drawdown_252"] - 0.01) / 0.08)
    f_ret5 = _clip01((-out["ret5d"] - 0.005) / 0.03)
    f_hurst = _clip01((out["hurst_100"] - 0.55) / 0.20)
    out["structural_pressure_score"] = pd.concat([f_dd, f_ret5, f_hurst], axis=1).mean(axis=1, skipna=True).fillna(0.0)

    # Biotech exposure proxy (IBB): only contributes when upstream IBB data exists.
    ibb_rel20 = (
        pd.to_numeric(out["ibb_rel20d"], errors="coerce")
        if "ibb_rel20d" in out.columns
        else pd.Series(0.0, index=out.index, dtype=float)
    )
    ibb_dd126 = (
        pd.to_numeric(out["ibb_drawdown_126"], errors="coerce")
        if "ibb_drawdown_126" in out.columns
        else pd.Series(0.0, index=out.index, dtype=float)
    )
    ibb_rel_weak = _clip01(((-ibb_rel20.fillna(0.0)) - 0.01) / 0.08)
    ibb_dd_risk = _clip01((ibb_dd126.fillna(0.0) - 0.10) / 0.35)
    out["ibb_risk_score"] = (
        pd.concat([ibb_rel_weak, ibb_dd_risk], axis=1)
        .mean(axis=1, skipna=True)
        .fillna(0.0)
        .clip(0.0, 1.0)
    )

    # Hard-gate support features.
    px = pd.to_numeric(out.get("market_price"), errors="coerce")
    out["price_below_200dma_flag"] = (px < px.rolling(200, min_periods=50).mean()).astype(int) if px.notna().any() else 0
    cred = pd.to_numeric(out.get("credit_real"), errors="coerce")
    out["credit_chg_20d"] = cred - cred.shift(20) if cred.notna().any() else pd.NA

    keep = [
        "date",
        "vol20",
        "hurst_100",
        "drawdown_252",
        "ret5d",
        "ret20d",
        "drawdown_126",
        "ibb_ret20d",
        "ibb_drawdown_126",
        "ibb_rel20d",
        "ibb_risk_score",
        "external_event",
        "breadth_real_stress",
        "market_price",
        "credit_real",
        "breadth_source_flag",
        "dd_delta_1d",
        "ret5d_delta_1d",
        "ret5d_median_20",
        "low_vol_flag",
        "high_hurst_flag",
        "drawdown_gt5_flag",
        "shock_like_flag",
        "new_lows_ratio_proxy",
        "ad_spread_proxy",
        "sector_sync_breakdown_proxy",
        "leadership_failure_proxy",
        "breadth_damage_score",
        "structural_pressure_score",
        "price_below_200dma_flag",
        "credit_chg_20d",
    ]
    keep = [c for c in keep if c in out.columns]
    return out[keep].sort_values("date", ascending=False).reset_index(drop=True)


def write_metadata(path: Path, input_csv: Path, output_csv: Path, rows: int, extras: dict | None = None) -> None:
    payload = {
        "input_csv": str(input_csv),
        "output_csv": str(output_csv),
        "rows": int(rows),
    }
    if extras:
        payload.update(extras)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
