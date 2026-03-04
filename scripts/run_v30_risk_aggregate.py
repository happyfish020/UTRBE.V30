from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.allocation.decision import AllocationConfig, apply_allocation
from v30.risk_aggregation.aggregate import AggregationConfig, aggregate_risk
from v30.data_layer import connect_backtest_db, read_table, upsert_dataframe

def _read_table(conn, table_name: str, cols: list[str] | None = None, start: str = "", end: str = "") -> pd.DataFrame:
    df = read_table(conn, table_name=table_name, cols=cols, start=start, end=end)
    if df.empty:
        raise ValueError(f"no rows in table: {table_name}")
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def _upsert_df(conn, df: pd.DataFrame, table_name: str, int_cols: set[str] | None = None) -> int:
    if df.empty:
        return 0
    return upsert_dataframe(
        conn,
        df,
        table_name=table_name,
        key_cols=("date",),
        int_cols=int_cols or set(),
    )


def _maybe_load_tuning(path: str | None) -> dict:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f'missing tuning config: {p}')
    data = json.loads(p.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError('tuning config must be a JSON object')
    return data


def _load_json_optional(path_str: str) -> dict:
    p = Path(str(path_str).strip())
    if not str(path_str).strip() or (not p.exists()):
        return {}
    txt = p.read_text(encoding="utf-8-sig")
    data = json.loads(txt)
    return data if isinstance(data, dict) else {}


def _action_from_allocation(v: float) -> str:
    x = float(v)
    if x >= 0.95:
        return "FULL_RISK_ON"
    if x >= 0.60:
        return "LIGHT_REDUCE"
    if x >= 0.35:
        return "REDUCE_TO_35_60"
    if x >= 0.15:
        return "REDUCE_TO_15_35"
    return "DEFENSIVE_0_15"


def _apply_live_guardrails(
    x: pd.DataFrame,
    *,
    v2_state: dict,
    lowfreq_state: dict,
    us_summary: dict,
    cap_lowfreq_alert: float,
    cap_watch_down: float,
    cap_watch_down_extra: float,
    cap_hyoas_stale: float,
) -> pd.DataFrame:
    if x.empty or "date" not in x.columns:
        return x
    out = x.copy()
    out["guardrail_applied"] = 0
    out["guardrail_cap"] = pd.NA
    out["guardrail_reasons"] = ""

    latest_idx = out["date"].idxmax()
    cap = 1.0
    reasons: list[str] = []

    lf_latest = lowfreq_state.get("latest_state", {}) if isinstance(lowfreq_state, dict) else {}
    lf_gate = str(lf_latest.get("gate", "NA")).upper()
    lf_hint = str(lf_latest.get("action_hint", "NA")).upper()
    if lf_gate == "ALERT" and lf_hint == "REDUCE":
        cap = min(cap, float(cap_lowfreq_alert))
        reasons.append("lowfreq_alert_reduce")

    warn = str(v2_state.get("warning_level", "NA")).upper() if isinstance(v2_state, dict) else "NA"
    v2_action = str(v2_state.get("action", "NA")).upper() if isinstance(v2_state, dict) else "NA"
    ret5d = float(pd.to_numeric(v2_state.get("ret5d", 0.0), errors="coerce")) if isinstance(v2_state, dict) else 0.0
    watch_on = (warn in {"WATCH", "ALERT", "SHORT"}) or (v2_action in {"SHORT_TERM_WATCH", "SHORT_TERM_ALERT"})
    if watch_on and ret5d < 0.0:
        if cap <= float(cap_watch_down) + 1e-12:
            cap = min(cap, float(cap_watch_down_extra))
        else:
            cap = min(cap, float(cap_watch_down))
        reasons.append("v2_watch_with_negative_return")

    sf = us_summary.get("source_freshness", {}) if isinstance(us_summary, dict) else {}
    hy_fr = sf.get("hy_oas", {}) if isinstance(sf, dict) else {}
    if bool(hy_fr.get("is_stale", False)):
        cap = min(cap, float(cap_hyoas_stale))
        reasons.append("hy_oas_stale")

    cur_alloc = float(pd.to_numeric(out.loc[latest_idx, "final_allocation"], errors="coerce"))
    if cap < cur_alloc:
        out.loc[latest_idx, "final_allocation"] = cap
        out.loc[latest_idx, "allocation_action"] = _action_from_allocation(cap)
        out.loc[latest_idx, "guardrail_applied"] = 1
        out.loc[latest_idx, "guardrail_cap"] = cap
        out.loc[latest_idx, "guardrail_reasons"] = "|".join(reasons)
    else:
        out.loc[latest_idx, "guardrail_cap"] = cap
        out.loc[latest_idx, "guardrail_reasons"] = "|".join(reasons)
    return out


def _build_agg_cfg(tuning: dict) -> AggregationConfig:
    return AggregationConfig(
        struct_high_cut=float(tuning.get('struct_high_cut', AggregationConfig.struct_high_cut)),
        struct_risk_cut=float(tuning.get('struct_risk_cut', AggregationConfig.struct_risk_cut)),
        shock_high_cut=float(tuning.get('shock_high_cut', AggregationConfig.shock_high_cut)),
        shock_risk_cut=float(tuning.get('shock_risk_cut', AggregationConfig.shock_risk_cut)),
        v31_mode=bool(tuning.get('v31_mode', AggregationConfig.v31_mode)),
        sds_gate_low=float(tuning.get('sds_gate_low', AggregationConfig.sds_gate_low)),
        sds_gate_high=float(tuning.get('sds_gate_high', AggregationConfig.sds_gate_high)),
        regime_bull_mult=float(tuning.get('regime_bull_mult', AggregationConfig.regime_bull_mult)),
        regime_neutral_mult=float(tuning.get('regime_neutral_mult', AggregationConfig.regime_neutral_mult)),
        regime_bear_mult=float(tuning.get('regime_bear_mult', AggregationConfig.regime_bear_mult)),
        shock_dynamic_gate=bool(tuning.get('shock_dynamic_gate', AggregationConfig.shock_dynamic_gate)),
        shock_dynamic_window=int(tuning.get('shock_dynamic_window', AggregationConfig.shock_dynamic_window)),
        shock_dynamic_high_q=float(tuning.get('shock_dynamic_high_q', AggregationConfig.shock_dynamic_high_q)),
        shock_dynamic_risk_q=float(tuning.get('shock_dynamic_risk_q', AggregationConfig.shock_dynamic_risk_q)),
        shock_dynamic_min_sds=float(tuning.get('shock_dynamic_min_sds', AggregationConfig.shock_dynamic_min_sds)),
        hard_gate_enable=bool(tuning.get('hard_gate_enable', AggregationConfig.hard_gate_enable)),
        hard_trend_cap=float(tuning.get('hard_trend_cap', AggregationConfig.hard_trend_cap)),
        hard_new_lows_ratio_thr=float(tuning.get('hard_new_lows_ratio_thr', AggregationConfig.hard_new_lows_ratio_thr)),
        hard_ad_spread_thr=float(tuning.get('hard_ad_spread_thr', AggregationConfig.hard_ad_spread_thr)),
        hard_ad_consecutive_days=int(tuning.get('hard_ad_consecutive_days', AggregationConfig.hard_ad_consecutive_days)),
        hard_crisis_allocation=float(tuning.get('hard_crisis_allocation', AggregationConfig.hard_crisis_allocation)),
        hard_credit_q=float(tuning.get('hard_credit_q', AggregationConfig.hard_credit_q)),
        hard_credit_window=int(tuning.get('hard_credit_window', AggregationConfig.hard_credit_window)),
        hard_force_sets_regime_crisis=bool(tuning.get('hard_force_sets_regime_crisis', AggregationConfig.hard_force_sets_regime_crisis)),
        ibb_risk_weight=float(tuning.get('ibb_risk_weight', AggregationConfig.ibb_risk_weight)),
        early_struct_enable=bool(tuning.get('early_struct_enable', AggregationConfig.early_struct_enable)),
        early_struct_z_window=int(tuning.get('early_struct_z_window', AggregationConfig.early_struct_z_window)),
        early_struct_z_min_periods=int(tuning.get('early_struct_z_min_periods', AggregationConfig.early_struct_z_min_periods)),
        early_struct_ma_window=int(tuning.get('early_struct_ma_window', AggregationConfig.early_struct_ma_window)),
        early_struct_slope_lag=int(tuning.get('early_struct_slope_lag', AggregationConfig.early_struct_slope_lag)),
        early_struct_w_vol=float(tuning.get('early_struct_w_vol', AggregationConfig.early_struct_w_vol)),
        early_struct_w_mom=float(tuning.get('early_struct_w_mom', AggregationConfig.early_struct_w_mom)),
        early_struct_w_slope=float(tuning.get('early_struct_w_slope', AggregationConfig.early_struct_w_slope)),
        early_struct_q20=float(tuning.get('early_struct_q20', AggregationConfig.early_struct_q20)),
        early_struct_q10=float(tuning.get('early_struct_q10', AggregationConfig.early_struct_q10)),
    )


def _build_alloc_cfg(tuning: dict) -> AllocationConfig:
    base_bins = tuple(tuning.get('base_bins', AllocationConfig.base_bins))
    base_allocs = tuple(tuning.get('base_allocs', AllocationConfig.base_allocs))
    tactical_multipliers = tuple(tuning.get('tactical_multipliers', AllocationConfig.tactical_multipliers))
    if len(base_bins) != 4:
        raise ValueError('base_bins must have length 4')
    if len(base_allocs) != 5:
        raise ValueError('base_allocs must have length 5')
    if len(tactical_multipliers) < 4:
        raise ValueError('tactical_multipliers must have at least 4 values for levels 0..3')
    return AllocationConfig(
        base_bins=base_bins,
        base_allocs=base_allocs,
        shock_high_cut=float(tuning.get('alloc_shock_high_cut', AllocationConfig.shock_high_cut)),
        shock_risk_cut=float(tuning.get('alloc_shock_risk_cut', AllocationConfig.shock_risk_cut)),
        tactical_multipliers=tactical_multipliers,
        tactical_gate_mode=str(tuning.get('tactical_gate_mode', AllocationConfig.tactical_gate_mode)),
        v31_step_mode=bool(tuning.get('v31_step_mode', AllocationConfig.v31_step_mode)),
        step_alloc_low=float(tuning.get('step_alloc_low', AllocationConfig.step_alloc_low)),
        step_alloc_medium=float(tuning.get('step_alloc_medium', AllocationConfig.step_alloc_medium)),
        step_alloc_high=float(tuning.get('step_alloc_high', AllocationConfig.step_alloc_high)),
        step_alloc_crisis=float(tuning.get('step_alloc_crisis', AllocationConfig.step_alloc_crisis)),
        recovery_enable=bool(tuning.get('recovery_enable', AllocationConfig.recovery_enable)),
        recovery_step=float(tuning.get('recovery_step', AllocationConfig.recovery_step)),
        recovery_start_cap=float(tuning.get('recovery_start_cap', AllocationConfig.recovery_start_cap)),
        recovery_vol_window=int(tuning.get('recovery_vol_window', AllocationConfig.recovery_vol_window)),
        recovery_vol_q=float(tuning.get('recovery_vol_q', AllocationConfig.recovery_vol_q)),
        recovery_min_days=int(tuning.get('recovery_min_days', AllocationConfig.recovery_min_days)),
        recovery_trigger_reasons=str(tuning.get('recovery_trigger_reasons', AllocationConfig.recovery_trigger_reasons)),
        recovery_accel_enable=bool(tuning.get('recovery_accel_enable', AllocationConfig.recovery_accel_enable)),
        recovery_accel_step=float(tuning.get('recovery_accel_step', AllocationConfig.recovery_accel_step)),
        recovery_accel_struct_window=int(tuning.get('recovery_accel_struct_window', AllocationConfig.recovery_accel_struct_window)),
        recovery_accel_struct_margin=float(tuning.get('recovery_accel_struct_margin', AllocationConfig.recovery_accel_struct_margin)),
        recovery_accel_vol_q=float(tuning.get('recovery_accel_vol_q', AllocationConfig.recovery_accel_vol_q)),
        recovery_boost_enable=bool(tuning.get('recovery_boost_enable', AllocationConfig.recovery_boost_enable)),
        recovery_boost_start_floor=float(tuning.get('recovery_boost_start_floor', AllocationConfig.recovery_boost_start_floor)),
        recovery_boost_step=float(tuning.get('recovery_boost_step', AllocationConfig.recovery_boost_step)),
        recovery_boost_max=float(tuning.get('recovery_boost_max', AllocationConfig.recovery_boost_max)),
        early_struct_enable=bool(tuning.get('early_struct_enable', AllocationConfig.early_struct_enable)),
        early_struct_top20_mult=float(tuning.get('early_struct_top20_mult', AllocationConfig.early_struct_top20_mult)),
        early_struct_top10_mult=float(tuning.get('early_struct_top10_mult', AllocationConfig.early_struct_top10_mult)),
        early_struct_gate_mode=str(tuning.get('early_struct_gate_mode', AllocationConfig.early_struct_gate_mode)),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description='V30 Phase-D risk aggregation and allocation runner.')
    parser.add_argument('--features-csv', default='output/v30_features_daily.csv')
    parser.add_argument('--features-table', default='')
    parser.add_argument('--struct-pred-csv', default='output/v30_structural_train/structural_test_predictions.csv')
    parser.add_argument('--struct-pred-table', default='')
    parser.add_argument('--shock-pred-csv', default='output/v30_shock_train/shock_test_predictions.csv')
    parser.add_argument('--shock-pred-table', default='')
    parser.add_argument('--db-path', default='data/backtest/v30_backtest.sqlite')
    parser.add_argument('--output-dir', default='output/v30_risk_aggregate')
    parser.add_argument('--output-table', default='v31_daily_allocation')
    parser.add_argument('--skip-db-upsert', action='store_true')
    parser.add_argument('--skip-csv-output', action='store_true')
    parser.add_argument('--data-start', default='')
    parser.add_argument('--data-end', default='')
    parser.add_argument('--tuning-json', default='', help='Optional JSON with aggregation/allocation overrides.')
    parser.add_argument('--v2-latest-state-json', default='', help='Optional V2 latest_state json for live guardrail.')
    parser.add_argument('--lowfreq-summary-json', default='', help='Optional lowfreq summary json for live guardrail.')
    parser.add_argument('--unified-sentiment-summary-json', default='', help='Optional unified sentiment summary json for data-freshness guardrail.')
    parser.add_argument('--guardrail-cap-lowfreq-alert', type=float, default=0.60)
    parser.add_argument('--guardrail-cap-watch-down', type=float, default=0.60)
    parser.add_argument('--guardrail-cap-watch-down-extra', type=float, default=0.35)
    parser.add_argument('--guardrail-cap-hyoas-stale', type=float, default=0.90)
    args = parser.parse_args()

    f_csv = Path(args.features_csv)
    s_csv = Path(args.struct_pred_csv)
    k_csv = Path(args.shock_pred_csv)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with connect_backtest_db(str(args.db_path).strip() or None) as conn:
        if str(args.features_table).strip():
            feat = _read_table(conn, str(args.features_table).strip(), start=str(args.data_start), end=str(args.data_end))
        else:
            if not f_csv.exists():
                raise FileNotFoundError(f'missing input: {f_csv}')
            feat = pd.read_csv(f_csv, parse_dates=['date'])
        if str(args.struct_pred_table).strip():
            st = _read_table(
                conn,
                str(args.struct_pred_table).strip(),
                cols=["date", "p_structural"],
                start=str(args.data_start),
                end=str(args.data_end),
            )[["date", "p_structural"]]
        else:
            if not s_csv.exists():
                raise FileNotFoundError(f'missing input: {s_csv}')
            st = pd.read_csv(s_csv, parse_dates=['date'])[['date', 'p_structural']]
        if str(args.shock_pred_table).strip():
            sh = _read_table(
                conn,
                str(args.shock_pred_table).strip(),
                cols=["date", "p_shock"],
                start=str(args.data_start),
                end=str(args.data_end),
            )[["date", "p_shock"]]
        else:
            if not k_csv.exists():
                raise FileNotFoundError(f'missing input: {k_csv}')
            sh = pd.read_csv(k_csv, parse_dates=['date'])[['date', 'p_shock']]

    feat['date'] = pd.to_datetime(feat['date']).dt.normalize()
    st['date'] = pd.to_datetime(st['date']).dt.normalize()
    sh['date'] = pd.to_datetime(sh['date']).dt.normalize()

    x = feat.merge(st, on='date', how='inner').merge(sh, on='date', how='inner')
    x = x.sort_values('date').reset_index(drop=True)

    tuning = _maybe_load_tuning(args.tuning_json)
    agg_cfg = _build_agg_cfg(tuning)
    alloc_cfg = _build_alloc_cfg(tuning)

    x = aggregate_risk(x, agg_cfg)
    x = apply_allocation(x, alloc_cfg)
    cap_lowfreq_alert = float(tuning.get('guardrail_cap_lowfreq_alert', args.guardrail_cap_lowfreq_alert))
    cap_watch_down = float(tuning.get('guardrail_cap_watch_down', args.guardrail_cap_watch_down))
    cap_watch_down_extra = float(tuning.get('guardrail_cap_watch_down_extra', args.guardrail_cap_watch_down_extra))
    cap_hyoas_stale = float(tuning.get('guardrail_cap_hyoas_stale', args.guardrail_cap_hyoas_stale))
    x = _apply_live_guardrails(
        x,
        v2_state=_load_json_optional(args.v2_latest_state_json),
        lowfreq_state=_load_json_optional(args.lowfreq_summary_json),
        us_summary=_load_json_optional(args.unified_sentiment_summary_json),
        cap_lowfreq_alert=cap_lowfreq_alert,
        cap_watch_down=cap_watch_down,
        cap_watch_down_extra=cap_watch_down_extra,
        cap_hyoas_stale=cap_hyoas_stale,
    )

    keep = [
        'date',
        'p_structural',
        'p_shock',
        'early_score_raw',
        'early_score',
        'early_score_q20_thr',
        'early_score_q10_thr',
        'early_struct_level',
        'tactical_level',
        'risk_regime',
        'structural_damage_score',
        'breadth_damage_score',
        'new_lows_ratio_proxy',
        'ad_spread_proxy',
        'sector_sync_breakdown_proxy',
        'leadership_failure_proxy',
        'regime_context',
        'regime_multiplier',
        'ibb_risk_score',
        'ibb_risk_weight',
        'composite_risk_score',
        'shock_high_cut_eff',
        'shock_risk_cut_eff',
        'hard_gate_reason',
        'hard_force_crisis',
        'hard_trend_cap_flag',
        'hard_credit_high_flag',
        'hard_max_allocation',
        'recovery_active',
        'recovery_cap',
        'recovery_days',
        'recovery_floor',
        'base_allocation',
        'shock_modifier',
        'tactical_multiplier',
        'tactical_gate_on',
        'early_struct_multiplier',
        'final_allocation',
        'allocation_action',
        'guardrail_applied',
        'guardrail_cap',
        'guardrail_reasons',
        'drawdown_252',
        'ret5d',
        'vol20',
        'hurst_100',
    ]
    keep = [c for c in keep if c in x.columns]
    out = x[keep].sort_values('date', ascending=False).reset_index(drop=True)
    if not bool(args.skip_csv_output):
        out.to_csv(out_dir / 'daily_allocation.csv', index=False)
    if not bool(args.skip_db_upsert):
        int_cols = {"early_struct_level", "tactical_level", "tactical_gate_on", "hard_force_crisis", "hard_trend_cap_flag", "hard_credit_high_flag", "recovery_active", "recovery_days", "guardrail_applied"}
        with connect_backtest_db(str(args.db_path).strip() or None) as conn:
            rows = _upsert_df(conn, out, table_name=str(args.output_table), int_cols=int_cols)
        print(f"[OK] DB upsert rows: {rows} -> table `{args.output_table}`")

    latest = out.iloc[0].to_dict()
    latest['date'] = str(pd.to_datetime(latest['date']).date())
    (out_dir / 'latest_allocation.json').write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding='utf-8')

    summary = {
        'rows': int(len(out)),
        'date_min': str(pd.to_datetime(out['date']).min().date()),
        'date_max': str(pd.to_datetime(out['date']).max().date()),
        'avg_final_allocation': float(pd.to_numeric(out['final_allocation'], errors='coerce').mean()),
        'regime_counts': out['risk_regime'].value_counts().to_dict(),
        'action_counts': out['allocation_action'].value_counts().to_dict(),
        'hard_gate_counts': out['hard_gate_reason'].value_counts().to_dict() if 'hard_gate_reason' in out.columns else {},
        'early_struct_level_counts': out['early_struct_level'].value_counts().to_dict() if 'early_struct_level' in out.columns else {},
        'avg_early_score': float(pd.to_numeric(out.get('early_score', 0.0), errors='coerce').mean()) if 'early_score' in out.columns else 0.0,
        'guardrail_applied_count': int(pd.to_numeric(out.get('guardrail_applied', 0), errors='coerce').fillna(0).sum()) if 'guardrail_applied' in out.columns else 0,
        'guardrail_reasons_latest': str(out.iloc[0].get('guardrail_reasons', '')) if 'guardrail_reasons' in out.columns and len(out) > 0 else '',
    }
    summary['config'] = {
        'aggregation': {
            'struct_high_cut': float(agg_cfg.struct_high_cut),
            'struct_risk_cut': float(agg_cfg.struct_risk_cut),
            'shock_high_cut': float(agg_cfg.shock_high_cut),
            'shock_risk_cut': float(agg_cfg.shock_risk_cut),
            'v31_mode': bool(agg_cfg.v31_mode),
            'sds_gate_low': float(agg_cfg.sds_gate_low),
            'sds_gate_high': float(agg_cfg.sds_gate_high),
            'regime_bull_mult': float(agg_cfg.regime_bull_mult),
            'regime_neutral_mult': float(agg_cfg.regime_neutral_mult),
            'regime_bear_mult': float(agg_cfg.regime_bear_mult),
            'ibb_risk_weight': float(agg_cfg.ibb_risk_weight),
            'shock_dynamic_gate': bool(agg_cfg.shock_dynamic_gate),
            'shock_dynamic_window': int(agg_cfg.shock_dynamic_window),
            'shock_dynamic_high_q': float(agg_cfg.shock_dynamic_high_q),
            'shock_dynamic_risk_q': float(agg_cfg.shock_dynamic_risk_q),
            'shock_dynamic_min_sds': float(agg_cfg.shock_dynamic_min_sds),
            'hard_gate_enable': bool(agg_cfg.hard_gate_enable),
            'hard_trend_cap': float(agg_cfg.hard_trend_cap),
            'hard_new_lows_ratio_thr': float(agg_cfg.hard_new_lows_ratio_thr),
            'hard_ad_spread_thr': float(agg_cfg.hard_ad_spread_thr),
            'hard_ad_consecutive_days': int(agg_cfg.hard_ad_consecutive_days),
            'hard_crisis_allocation': float(agg_cfg.hard_crisis_allocation),
            'hard_credit_q': float(agg_cfg.hard_credit_q),
            'hard_credit_window': int(agg_cfg.hard_credit_window),
            'hard_force_sets_regime_crisis': bool(agg_cfg.hard_force_sets_regime_crisis),
            'early_struct_enable': bool(agg_cfg.early_struct_enable),
            'early_struct_z_window': int(agg_cfg.early_struct_z_window),
            'early_struct_z_min_periods': int(agg_cfg.early_struct_z_min_periods),
            'early_struct_ma_window': int(agg_cfg.early_struct_ma_window),
            'early_struct_slope_lag': int(agg_cfg.early_struct_slope_lag),
            'early_struct_w_vol': float(agg_cfg.early_struct_w_vol),
            'early_struct_w_mom': float(agg_cfg.early_struct_w_mom),
            'early_struct_w_slope': float(agg_cfg.early_struct_w_slope),
            'early_struct_q20': float(agg_cfg.early_struct_q20),
            'early_struct_q10': float(agg_cfg.early_struct_q10),
        },
        'allocation': {
            'base_bins': list(alloc_cfg.base_bins),
            'base_allocs': list(alloc_cfg.base_allocs),
            'shock_high_cut': float(alloc_cfg.shock_high_cut),
            'shock_risk_cut': float(alloc_cfg.shock_risk_cut),
            'tactical_multipliers': list(alloc_cfg.tactical_multipliers),
            'tactical_gate_mode': str(alloc_cfg.tactical_gate_mode),
            'v31_step_mode': bool(alloc_cfg.v31_step_mode),
            'step_alloc_low': float(alloc_cfg.step_alloc_low),
            'step_alloc_medium': float(alloc_cfg.step_alloc_medium),
            'step_alloc_high': float(alloc_cfg.step_alloc_high),
            'step_alloc_crisis': float(alloc_cfg.step_alloc_crisis),
            'recovery_enable': bool(alloc_cfg.recovery_enable),
            'recovery_step': float(alloc_cfg.recovery_step),
            'recovery_start_cap': float(alloc_cfg.recovery_start_cap),
            'recovery_vol_window': int(alloc_cfg.recovery_vol_window),
            'recovery_vol_q': float(alloc_cfg.recovery_vol_q),
            'recovery_min_days': int(alloc_cfg.recovery_min_days),
            'recovery_trigger_reasons': str(alloc_cfg.recovery_trigger_reasons),
            'recovery_accel_enable': bool(alloc_cfg.recovery_accel_enable),
            'recovery_accel_step': float(alloc_cfg.recovery_accel_step),
            'recovery_accel_struct_window': int(alloc_cfg.recovery_accel_struct_window),
            'recovery_accel_struct_margin': float(alloc_cfg.recovery_accel_struct_margin),
            'recovery_accel_vol_q': float(alloc_cfg.recovery_accel_vol_q),
            'recovery_boost_enable': bool(alloc_cfg.recovery_boost_enable),
            'recovery_boost_start_floor': float(alloc_cfg.recovery_boost_start_floor),
            'recovery_boost_step': float(alloc_cfg.recovery_boost_step),
            'recovery_boost_max': float(alloc_cfg.recovery_boost_max),
            'early_struct_enable': bool(alloc_cfg.early_struct_enable),
            'early_struct_top20_mult': float(alloc_cfg.early_struct_top20_mult),
            'early_struct_top10_mult': float(alloc_cfg.early_struct_top10_mult),
            'early_struct_gate_mode': str(alloc_cfg.early_struct_gate_mode),
        },
        'live_guardrail': {
            'cap_lowfreq_alert': float(cap_lowfreq_alert),
            'cap_watch_down': float(cap_watch_down),
            'cap_watch_down_extra': float(cap_watch_down_extra),
            'cap_hyoas_stale': float(cap_hyoas_stale),
        },
    }
    (out_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    lines = [
        '# V30 Daily Allocation Report',
        '',
        f"- date: {latest['date']}",
        f"- p_structural: {float(latest.get('p_structural', 0.0)):.4f}",
        f"- p_shock: {float(latest.get('p_shock', 0.0)):.4f}",
        f"- early_score: {float(latest.get('early_score', 0.0)):.4f}",
        f"- early_struct_level: {int(pd.to_numeric(latest.get('early_struct_level', 0), errors='coerce'))}",
        f"- tactical_level: {int(latest.get('tactical_level', 0))}",
        f"- tactical_gate_on: {int(pd.to_numeric(latest.get('tactical_gate_on', 0), errors='coerce'))}",
        f"- risk_regime: {latest.get('risk_regime', 'NA')}",
        f"- final_allocation: {float(latest.get('final_allocation', 0.0)):.2f}",
        f"- allocation_action: {latest.get('allocation_action', 'NA')}",
        f"- guardrail_applied: {int(pd.to_numeric(latest.get('guardrail_applied', 0), errors='coerce'))}",
        f"- guardrail_reasons: {latest.get('guardrail_reasons', '')}",
    ]
    (out_dir / 'daily_allocation_report.md').write_text('\n'.join(lines), encoding='utf-8')

    if not bool(args.skip_csv_output):
        print(f"[OK] Wrote: {out_dir / 'daily_allocation.csv'}")
    print(f"[OK] Wrote: {out_dir / 'latest_allocation.json'}")
    print(f"[OK] Wrote: {out_dir / 'summary.json'}")
    print(f"[OK] Wrote: {out_dir / 'daily_allocation_report.md'}")


if __name__ == '__main__':
    main()

