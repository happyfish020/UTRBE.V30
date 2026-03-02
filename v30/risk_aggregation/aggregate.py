from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass
class AggregationConfig:
    struct_high_cut: float = 0.85
    struct_risk_cut: float = 0.70
    shock_high_cut: float = 0.85
    shock_risk_cut: float = 0.60
    v31_mode: bool = False
    sds_gate_low: float = 0.35
    sds_gate_high: float = 0.60
    regime_bull_mult: float = 0.60
    regime_neutral_mult: float = 1.00
    regime_bear_mult: float = 1.50
    shock_dynamic_gate: bool = False
    shock_dynamic_window: int = 252
    shock_dynamic_high_q: float = 0.90
    shock_dynamic_risk_q: float = 0.75
    shock_dynamic_min_sds: float = -1.0
    hard_gate_enable: bool = False
    hard_trend_cap: float = 0.60
    hard_new_lows_ratio_thr: float = 0.15
    hard_ad_spread_thr: float = -0.40
    hard_ad_consecutive_days: int = 5
    hard_crisis_allocation: float = 0.20
    hard_credit_q: float = 0.90
    hard_credit_window: int = 252
    hard_force_sets_regime_crisis: bool = False
    ibb_risk_weight: float = 0.05


def derive_tactical_level(df: pd.DataFrame) -> pd.Series:
    ret5d = pd.to_numeric(df.get('ret5d', 0), errors='coerce').fillna(0.0)
    dd = pd.to_numeric(df.get('drawdown_252', 0), errors='coerce').fillna(0.0)
    dd_delta = pd.to_numeric(df.get('dd_delta_1d', 0), errors='coerce').fillna(0.0)
    shock_like = pd.to_numeric(df.get('shock_like_flag', 0), errors='coerce').fillna(0).astype(int)

    lvl = pd.Series(0, index=df.index, dtype=int)
    lvl[(ret5d <= -0.01) | (dd >= 0.03)] = 1
    lvl[(ret5d <= -0.02) | (dd_delta >= 0.005) | (shock_like == 1)] = 2
    lvl[(ret5d <= -0.03) & (dd >= 0.05)] = 3
    return lvl


def _structural_damage_score(df: pd.DataFrame) -> pd.Series:
    sp = pd.to_numeric(df.get('structural_pressure_score', 0), errors='coerce').fillna(0.0)
    bd = pd.to_numeric(df.get('breadth_damage_score', 0), errors='coerce').fillna(0.0)
    new_lows = pd.to_numeric(df.get('new_lows_ratio_proxy', 0), errors='coerce').fillna(0.0)
    sync_break = pd.to_numeric(df.get('sector_sync_breakdown_proxy', 0), errors='coerce').fillna(0.0)
    leader_fail = pd.to_numeric(df.get('leadership_failure_proxy', 0), errors='coerce').fillna(0.0)
    ad_spread = pd.to_numeric(df.get('ad_spread_proxy', 0), errors='coerce').fillna(0.0)
    dd = pd.to_numeric(df.get('drawdown_252', 0), errors='coerce').fillna(0.0)
    dd_delta = pd.to_numeric(df.get('dd_delta_1d', 0), errors='coerce').fillna(0.0)
    shock_like = pd.to_numeric(df.get('shock_like_flag', 0), errors='coerce').fillna(0).astype(int)
    drawdown_gt5 = pd.to_numeric(df.get('drawdown_gt5_flag', 0), errors='coerce').fillna(0).astype(int)
    ret5d = pd.to_numeric(df.get('ret5d', 0), errors='coerce').fillna(0.0)

    ad_damage = (-ad_spread).clip(lower=0.0, upper=1.0)
    score = (
        0.22 * sp.clip(0.0, 1.0)
        + 0.28 * bd.clip(0.0, 1.0)
        + 0.12 * new_lows.clip(0.0, 1.0)
        + 0.10 * sync_break.clip(0.0, 1.0)
        + 0.08 * leader_fail.clip(0.0, 1.0)
        + 0.05 * ad_damage
        + 0.07 * (dd >= 0.05).astype(float)
        + 0.05 * (dd_delta >= 0.005).astype(float)
        + 0.03 * shock_like.astype(float)
        + 0.00 * drawdown_gt5.astype(float)
        + 0.00 * (ret5d <= -0.02).astype(float)
    )
    return score.clip(0.0, 1.0)


def _regime_context(df: pd.DataFrame, cfg: AggregationConfig) -> tuple[pd.Series, pd.Series]:
    ret5d = pd.to_numeric(df.get('ret5d', 0), errors='coerce').fillna(0.0)
    dd = pd.to_numeric(df.get('drawdown_252', 0), errors='coerce').fillna(0.0)
    vol20 = pd.to_numeric(df.get('vol20', 0), errors='coerce').fillna(0.0)
    high_hurst = pd.to_numeric(df.get('high_hurst_flag', 0), errors='coerce').fillna(0).astype(int)

    is_bear = (ret5d <= -0.015) | (dd >= 0.08) | (vol20 >= 0.020)
    is_bull = (ret5d >= 0.005) & (dd <= 0.03) & (high_hurst == 1) & (~is_bear)

    label = pd.Series('NEUTRAL', index=df.index, dtype=object)
    label[is_bear] = 'BEAR'
    label[is_bull] = 'BULL'

    mult = pd.Series(float(cfg.regime_neutral_mult), index=df.index, dtype=float)
    mult[label == 'BULL'] = float(cfg.regime_bull_mult)
    mult[label == 'BEAR'] = float(cfg.regime_bear_mult)
    return label, mult


def aggregate_risk(df: pd.DataFrame, cfg: AggregationConfig) -> pd.DataFrame:
    x = df.copy()
    x['p_structural'] = pd.to_numeric(x['p_structural'], errors='coerce').fillna(0.0)
    x['p_shock'] = pd.to_numeric(x['p_shock'], errors='coerce').fillna(0.0)
    # Effective shock cuts can be dynamic (history-only rolling quantiles) or static.
    if bool(cfg.shock_dynamic_gate):
        w = max(20, int(cfg.shock_dynamic_window))
        qh = min(max(float(cfg.shock_dynamic_high_q), 0.50), 0.995)
        qr = min(max(float(cfg.shock_dynamic_risk_q), 0.50), 0.99)
        hist = x['p_shock'].shift(1)
        high_cut_eff = hist.rolling(w, min_periods=20).quantile(qh)
        risk_cut_eff = hist.rolling(w, min_periods=20).quantile(qr)
        x['shock_high_cut_eff'] = high_cut_eff.fillna(float(cfg.shock_high_cut)).clip(lower=0.0, upper=1.0)
        x['shock_risk_cut_eff'] = risk_cut_eff.fillna(float(cfg.shock_risk_cut)).clip(lower=0.0, upper=1.0)
    else:
        x['shock_high_cut_eff'] = float(cfg.shock_high_cut)
        x['shock_risk_cut_eff'] = float(cfg.shock_risk_cut)

    # Hard-gate state machine (pre-composite).
    x['hard_gate_reason'] = 'NONE'
    x['hard_force_crisis'] = 0
    x['hard_trend_cap_flag'] = 0
    x['hard_credit_high_flag'] = 0
    x['hard_max_allocation'] = 1.0
    if bool(cfg.hard_gate_enable):
        px_below_200 = pd.to_numeric(x.get('price_below_200dma_flag', 0), errors='coerce').fillna(0).astype(int) == 1
        med20 = pd.to_numeric(x.get('ret5d_median_20', 0.0), errors='coerce').fillna(0.0)
        trend_break = px_below_200 & (med20 < 0.0)

        new_lows = pd.to_numeric(x.get('new_lows_ratio_proxy', 0.0), errors='coerce').fillna(0.0)
        ad_spread = pd.to_numeric(x.get('ad_spread_proxy', 0.0), errors='coerce').fillna(0.0)
        ad_weak = ad_spread <= float(cfg.hard_ad_spread_thr)
        grp = (ad_weak != ad_weak.shift(1)).cumsum()
        run_len = ad_weak.groupby(grp).cumcount() + 1
        ad_weak_streak = ad_weak & (run_len >= max(1, int(cfg.hard_ad_consecutive_days)))
        breadth_crisis = (new_lows >= float(cfg.hard_new_lows_ratio_thr)) | ad_weak_streak

        credit_chg20 = pd.to_numeric(x.get('credit_chg_20d', pd.NA), errors='coerce')
        c_win = max(60, int(cfg.hard_credit_window))
        c_q = min(max(float(cfg.hard_credit_q), 0.70), 0.995)
        credit_q = credit_chg20.shift(1).rolling(c_win, min_periods=60).quantile(c_q)
        credit_expand = (credit_chg20 >= credit_q) & credit_q.notna() if credit_chg20.notna().any() else pd.Series(False, index=x.index)

        x.loc[trend_break, 'hard_trend_cap_flag'] = 1
        x.loc[credit_expand, 'hard_credit_high_flag'] = 1
        x.loc[trend_break, 'hard_max_allocation'] = x.loc[trend_break, 'hard_max_allocation'].clip(upper=float(cfg.hard_trend_cap))
        x.loc[breadth_crisis, 'hard_force_crisis'] = 1
        x.loc[breadth_crisis, 'hard_max_allocation'] = x.loc[breadth_crisis, 'hard_max_allocation'].clip(upper=float(cfg.hard_crisis_allocation))
        x.loc[trend_break, 'hard_gate_reason'] = 'TREND_CAP'
        x.loc[credit_expand & (~trend_break), 'hard_gate_reason'] = 'CREDIT_HIGH'
        x.loc[breadth_crisis, 'hard_gate_reason'] = 'BREADTH_CRISIS'

    lvl = derive_tactical_level(x)
    x['tactical_level'] = lvl

    if not bool(cfg.v31_mode):
        regime = []
        for _, r in x.iterrows():
            ps = float(r['p_structural'])
            pk = float(r['p_shock'])
            sh_high = float(r.get('shock_high_cut_eff', cfg.shock_high_cut))
            sh_risk = float(r.get('shock_risk_cut_eff', cfg.shock_risk_cut))
            if ps >= float(cfg.struct_high_cut):
                regime.append('HIGH_STRUCT')
            elif ps >= float(cfg.struct_risk_cut):
                regime.append('STRUCT_RISK')
            elif pk >= sh_high:
                regime.append('SHOCK_HIGH')
            elif pk >= sh_risk:
                regime.append('SHOCK_RISK')
            else:
                regime.append('NORMAL')
        x['risk_regime'] = regime
        return x

    sds = _structural_damage_score(x)
    x['structural_damage_score'] = sds
    if bool(cfg.shock_dynamic_gate) and float(cfg.shock_dynamic_min_sds) >= 0.0:
        low_stress = x['structural_damage_score'] < float(cfg.shock_dynamic_min_sds)
        x.loc[low_stress, 'shock_high_cut_eff'] = float(cfg.shock_high_cut)
        x.loc[low_stress, 'shock_risk_cut_eff'] = float(cfg.shock_risk_cut)

    regime_ctx, regime_mult = _regime_context(x, cfg)
    x['regime_context'] = regime_ctx
    x['regime_multiplier'] = regime_mult

    base_composite = (0.65 * x['p_structural'] + 0.35 * x['p_shock']) * x['regime_multiplier']
    ibb_score = pd.to_numeric(x.get('ibb_risk_score', 0.0), errors='coerce').fillna(0.0).clip(0.0, 1.0)
    ibb_w = min(max(float(cfg.ibb_risk_weight), 0.0), 0.30)
    composite = base_composite + ibb_w * ibb_score
    composite = composite.clip(0.0, 1.5)
    x['ibb_risk_score'] = ibb_score
    x['ibb_risk_weight'] = ibb_w
    x['composite_risk_score'] = composite

    risk_regime = []
    for _, r in x.iterrows():
        if int(r.get('hard_force_crisis', 0)) == 1 and bool(cfg.hard_force_sets_regime_crisis):
            risk_regime.append('CRISIS')
            continue
        s = float(r['structural_damage_score'])
        c = float(r['composite_risk_score'])
        if s < float(cfg.sds_gate_low):
            if c >= 0.85:
                risk_regime.append('MEDIUM_GATED')
            elif c >= 0.60:
                risk_regime.append('MEDIUM')
            else:
                risk_regime.append('LOW')
        elif s < float(cfg.sds_gate_high):
            if c >= 1.00:
                risk_regime.append('HIGH')
            elif c >= 0.70:
                risk_regime.append('MEDIUM')
            else:
                risk_regime.append('LOW')
        else:
            if c >= 1.05:
                risk_regime.append('CRISIS')
            elif c >= 0.75:
                risk_regime.append('HIGH')
            elif c >= 0.55:
                risk_regime.append('MEDIUM')
            else:
                risk_regime.append('LOW')
    if bool(cfg.hard_gate_enable):
        for i in range(len(risk_regime)):
            if int(pd.to_numeric(x.iloc[i].get('hard_credit_high_flag', 0), errors='coerce')) == 1 and risk_regime[i] in ('LOW', 'MEDIUM_GATED'):
                risk_regime[i] = 'HIGH'
    x['risk_regime'] = risk_regime
    return x
