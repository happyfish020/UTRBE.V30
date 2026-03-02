from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass
class AllocationConfig:
    base_bins: tuple = (0.30, 0.50, 0.70, 0.85)
    base_allocs: tuple = (1.00, 0.80, 0.60, 0.30, 0.10)
    shock_high_cut: float = 0.85
    shock_risk_cut: float = 0.70
    tactical_multipliers: tuple = (1.00, 0.90, 0.80, 0.70)
    tactical_gate_mode: str = "NONE"
    v31_step_mode: bool = False
    step_alloc_low: float = 1.00
    step_alloc_medium: float = 0.85
    step_alloc_high: float = 0.60
    step_alloc_crisis: float = 0.20
    recovery_enable: bool = False
    recovery_step: float = 0.08
    recovery_start_cap: float = 0.30
    recovery_vol_window: int = 126
    recovery_vol_q: float = 0.60
    recovery_min_days: int = 3
    recovery_trigger_reasons: str = 'BREADTH_CRISIS,TREND_CAP'
    recovery_accel_enable: bool = False
    recovery_accel_step: float = 0.16
    recovery_accel_struct_window: int = 20
    recovery_accel_struct_margin: float = 0.0
    recovery_accel_vol_q: float = 0.45
    recovery_boost_enable: bool = False
    recovery_boost_start_floor: float = 0.60
    recovery_boost_step: float = 0.12
    recovery_boost_max: float = 0.95
    early_struct_enable: bool = True
    early_struct_top20_mult: float = 0.90
    early_struct_top10_mult: float = 0.80
    early_struct_gate_mode: str = "NONE"


def base_allocation(p_struct: float, cfg: AllocationConfig) -> float:
    b = cfg.base_bins
    a = cfg.base_allocs
    if p_struct < b[0]:
        return float(a[0])
    if p_struct < b[1]:
        return float(a[1])
    if p_struct < b[2]:
        return float(a[2])
    if p_struct < b[3]:
        return float(a[3])
    return float(a[4])


def tactical_multiplier(level: int, cfg: AllocationConfig) -> float:
    idx = max(0, min(int(level), len(cfg.tactical_multipliers) - 1))
    return float(cfg.tactical_multipliers[idx])


def apply_allocation(df: pd.DataFrame, cfg: AllocationConfig) -> pd.DataFrame:
    x = df.copy()
    out_base = []
    out_shock_mod = []
    out_tac_mod = []
    out_tac_gate = []
    out_early_mod = []
    out_final = []
    actions = []
    out_recovery_active = []
    out_recovery_cap = []
    out_recovery_days = []
    out_recovery_floor = []

    # Optional gate for tactical layer.
    tac_gate_mode = str(cfg.tactical_gate_mode).strip().upper()
    px = pd.to_numeric(x.get('market_price', pd.Series([pd.NA] * len(x))), errors='coerce')
    ret20d = pd.to_numeric(x.get('ret20d', pd.Series([pd.NA] * len(x))), errors='coerce')
    if tac_gate_mode == "MA100":
        ma100 = px.rolling(100, min_periods=100).mean()
        tactical_gate_on = (px < ma100).fillna(False)
    elif tac_gate_mode in {"RET20D_NEG", "MOM20_NEG"}:
        tactical_gate_on = (ret20d < 0.0).fillna(False)
    else:
        tactical_gate_on = pd.Series(True, index=x.index)

    # Optional trend gate for Early-Structural multiplier (e.g., MA100 / MA200).
    gate_mode = str(cfg.early_struct_gate_mode).strip().upper()
    px = pd.to_numeric(x.get('market_price', pd.Series([pd.NA] * len(x))), errors='coerce')
    if gate_mode == "MA100":
        ma_gate = px.rolling(100, min_periods=100).mean()
        early_gate_on = (px < ma_gate).fillna(False)
    elif gate_mode == "MA200":
        ma_gate = px.rolling(200, min_periods=200).mean()
        early_gate_on = (px < ma_gate).fillna(False)
    else:
        early_gate_on = pd.Series(True, index=x.index)

    # Stateful recovery controller (date-ordered rows expected).
    recovery_active = False
    recovery_cap = 1.0
    recovery_floor = 0.0
    recovery_days = 0
    vol = pd.to_numeric(x.get('vol20'), errors='coerce').fillna(0.0)
    p_struct = pd.to_numeric(x.get('p_structural'), errors='coerce').fillna(0.0)
    rvw = max(20, int(cfg.recovery_vol_window))
    rvq = min(max(float(cfg.recovery_vol_q), 0.30), 0.90)
    vol_gate = vol.shift(1).rolling(rvw, min_periods=20).quantile(rvq).fillna(vol.expanding(min_periods=1).median())
    accel_vol_q = min(max(float(cfg.recovery_accel_vol_q), 0.20), 0.80)
    accel_vol_gate = vol.shift(1).rolling(rvw, min_periods=20).quantile(accel_vol_q).fillna(vol.expanding(min_periods=1).median())
    rsw = max(10, int(cfg.recovery_accel_struct_window))
    struct_ref = p_struct.shift(1).rolling(rsw, min_periods=max(5, rsw // 2)).mean().fillna(p_struct.expanding(min_periods=1).median())
    trigger_reasons = {s.strip().upper() for s in str(cfg.recovery_trigger_reasons).split(',') if s.strip()}

    for i, r in x.iterrows():
        ps = float(r['p_structural'])
        pk = float(r['p_shock'])
        tl = int(r['tactical_level'])
        shock_high_cut = float(r.get('shock_high_cut_eff', cfg.shock_high_cut))
        shock_risk_cut = float(r.get('shock_risk_cut_eff', cfg.shock_risk_cut))
        hard_cap = float(r.get('hard_max_allocation', 1.0))
        hard_force_crisis = int(pd.to_numeric(r.get('hard_force_crisis', 0), errors='coerce'))
        hard_reason = str(r.get('hard_gate_reason', 'NONE')).upper()
        hard_on = hard_reason != 'NONE'
        recovery_trigger = hard_reason in trigger_reasons

        risk_regime = str(r.get('risk_regime', '')).upper()
        if bool(cfg.v31_step_mode):
            if risk_regime in ('LOW',):
                base = float(cfg.step_alloc_low)
            elif risk_regime in ('MEDIUM', 'MEDIUM_GATED'):
                base = float(cfg.step_alloc_medium)
            elif risk_regime in ('HIGH',):
                base = float(cfg.step_alloc_high)
            elif risk_regime in ('CRISIS',):
                base = float(cfg.step_alloc_crisis)
            else:
                base = base_allocation(ps, cfg)
        else:
            base = base_allocation(ps, cfg)

        # shock override
        if pk >= shock_high_cut:
            shock_mod = min(0.5, 0.30 / base) if base > 1e-12 else 0.5
        elif pk >= shock_risk_cut:
            shock_mod = 0.7
        else:
            shock_mod = 1.0

        tac_mod = tactical_multiplier(tl, cfg) if bool(tactical_gate_on.iloc[i]) else 1.0
        early_level = int(pd.to_numeric(r.get('early_struct_level', 0), errors='coerce'))
        if bool(cfg.early_struct_enable) and bool(early_gate_on.iloc[i]):
            if early_level >= 2:
                early_mod = float(cfg.early_struct_top10_mult)
            elif early_level >= 1:
                early_mod = float(cfg.early_struct_top20_mult)
            else:
                early_mod = 1.0
        else:
            early_mod = 1.0

        final_alloc = max(0.0, min(1.0, base * shock_mod * tac_mod * early_mod))
        if hard_force_crisis == 1:
            # Crisis hard-gate is controlled by hard_max_allocation (from aggregation layer).
            final_alloc = min(final_alloc, max(0.0, min(1.0, hard_cap)))

        # Dynamic post-trigger recovery cap:
        # after hard-gate days, recover cap gradually only when volatility normalizes.
        if bool(cfg.recovery_enable):
            if hard_on and recovery_trigger:
                recovery_active = True
                recovery_days = 0
                recovery_cap = min(
                    max(0.0, min(1.0, float(hard_cap))),
                    max(0.0, min(1.0, float(cfg.recovery_start_cap))),
                )
                recovery_floor = max(0.0, min(1.0, float(cfg.recovery_boost_start_floor)))
            elif recovery_active and not hard_on:
                recovery_days += 1
                vol_ok = bool(float(vol.iloc[i]) <= float(vol_gate.iloc[i]))
                accel_ok = (
                    bool(cfg.recovery_accel_enable)
                    and bool(float(ps) <= float(struct_ref.iloc[i]) - float(cfg.recovery_accel_struct_margin))
                    and bool(float(vol.iloc[i]) <= float(accel_vol_gate.iloc[i]))
                )
                if recovery_days >= int(cfg.recovery_min_days):
                    step = 0.0
                    if vol_ok:
                        step = max(step, float(cfg.recovery_step))
                    if accel_ok:
                        step = max(step, float(cfg.recovery_accel_step))
                    if step > 0.0:
                        recovery_cap = min(1.0, float(recovery_cap + step))
                if bool(cfg.recovery_boost_enable) and recovery_days >= int(cfg.recovery_min_days) and accel_ok:
                    recovery_floor = min(float(cfg.recovery_boost_max), float(recovery_floor + float(cfg.recovery_boost_step)))
                if recovery_cap >= 0.999:
                    recovery_active = False
                    recovery_floor = 0.0
            cap_now = min(max(0.0, min(1.0, hard_cap)), recovery_cap if recovery_active else 1.0)
            if bool(cfg.recovery_boost_enable):
                floor_now = float(recovery_floor if recovery_active else 0.0)
                final_alloc = max(final_alloc, floor_now)
                final_alloc = min(final_alloc, max(0.0, min(1.0, hard_cap)))
                out_recovery_cap.append(float(max(0.0, min(1.0, hard_cap))))
            else:
                final_alloc = min(final_alloc, cap_now)
                out_recovery_cap.append(float(cap_now))
            out_recovery_active.append(int(recovery_active))
            out_recovery_days.append(int(recovery_days))
            out_recovery_floor.append(float(recovery_floor if recovery_active else 0.0))
        else:
            final_alloc = min(final_alloc, max(0.0, min(1.0, hard_cap)))
            out_recovery_active.append(0)
            out_recovery_cap.append(float(max(0.0, min(1.0, hard_cap))))
            out_recovery_days.append(0)
            out_recovery_floor.append(0.0)

        # action label for readability
        if final_alloc <= 0.15:
            action = 'DEFENSIVE_0_15'
        elif final_alloc <= 0.35:
            action = 'REDUCE_TO_15_35'
        elif final_alloc <= 0.60:
            action = 'REDUCE_TO_35_60'
        elif final_alloc < 1.0:
            action = 'LIGHT_REDUCE'
        else:
            action = 'FULL_RISK_ON'

        out_base.append(base)
        out_shock_mod.append(shock_mod)
        out_tac_mod.append(tac_mod)
        out_tac_gate.append(int(bool(tactical_gate_on.iloc[i])))
        out_early_mod.append(early_mod)
        out_final.append(final_alloc)
        actions.append(action)

    x['base_allocation'] = out_base
    x['shock_modifier'] = out_shock_mod
    x['tactical_multiplier'] = out_tac_mod
    x['tactical_gate_on'] = out_tac_gate
    x['early_struct_multiplier'] = out_early_mod
    x['final_allocation'] = out_final
    x['allocation_action'] = actions
    x['recovery_active'] = out_recovery_active
    x['recovery_cap'] = out_recovery_cap
    x['recovery_days'] = out_recovery_days
    x['recovery_floor'] = out_recovery_floor
    return x
