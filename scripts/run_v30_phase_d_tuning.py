from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

UTRBE_SRC = (PROJECT_ROOT.parent / 'UTRBE' / 'src').resolve()
if str(UTRBE_SRC) not in sys.path:
    sys.path.insert(0, str(UTRBE_SRC))

from v30.allocation.decision import AllocationConfig, apply_allocation
from v30.backtest.portfolio_eval import summarize
from v30.risk_aggregation.aggregate import AggregationConfig, aggregate_risk


def fetch_spy_ret(start: pd.Timestamp, end: pd.Timestamp, spy_ret_csv: str) -> pd.Series:
    p = Path(spy_ret_csv)
    if p.exists():
        x = pd.read_csv(p, parse_dates=['date'])
        if 'spy_ret1d' in x.columns:
            x['date'] = pd.to_datetime(x['date']).dt.normalize()
            s = pd.to_numeric(x.set_index('date')['spy_ret1d'], errors='coerce').fillna(0.0)
            s = s.sort_index()
            s.name = 'spy_ret1d'
            return s[(s.index >= start) & (s.index <= end)]

    from utrbe.data.fetcher import DataFetcher

    f = DataFetcher()
    px = pd.to_numeric(f.get_price('spy', start.date(), end.date()), errors='coerce')
    px.index = pd.to_datetime(px.index)
    r = px.pct_change().fillna(0.0)
    r.name = 'spy_ret1d'
    return r


def evaluate(allocation: pd.Series, spy_ret1d: pd.Series, lag_days: int, transaction_cost_bps: float) -> dict:
    x = pd.DataFrame({'final_allocation': allocation, 'spy_ret1d': spy_ret1d}).copy()
    x['applied_allocation'] = pd.to_numeric(x['final_allocation'], errors='coerce').shift(lag_days).fillna(1.0)
    x['turnover'] = x['applied_allocation'].diff().abs().fillna(0.0)
    x['trade_cost'] = x['turnover'] * float(transaction_cost_bps) / 10000.0
    x['strategy_ret1d'] = x['applied_allocation'] * x['spy_ret1d'] - x['trade_cost']
    x['strategy_nav'] = (1.0 + x['strategy_ret1d']).cumprod()
    x['benchmark_nav'] = (1.0 + x['spy_ret1d']).cumprod()

    s_strat = summarize(x['strategy_nav'], x['strategy_ret1d'])
    s_bench = summarize(x['benchmark_nav'], x['spy_ret1d'])

    cagr_impact = float(s_bench['annualized_return'] - s_strat['annualized_return'])
    dd_reduction = 0.0
    if abs(float(s_bench['max_drawdown'])) > 1e-12:
        dd_reduction = float((abs(float(s_bench['max_drawdown'])) - abs(float(s_strat['max_drawdown']))) / abs(float(s_bench['max_drawdown'])))
    return {
        'cagr_impact': cagr_impact,
        'max_drawdown_reduction': dd_reduction,
        'strategy_annualized_return': float(s_strat['annualized_return']),
        'strategy_max_drawdown': float(s_strat['max_drawdown']),
        'avg_final_allocation': float(pd.to_numeric(allocation, errors='coerce').mean()),
    }


def iter_candidate_configs() -> list[dict]:
    agg_default = asdict(AggregationConfig())
    candidates: list[dict] = []
    base_bins_space = [
        (0.20, 0.40, 0.60, 0.80),
        (0.25, 0.45, 0.65, 0.85),
        (0.30, 0.50, 0.70, 0.85),
    ]
    base_allocs_space = [
        (1.00, 0.90, 0.75, 0.55, 0.35),
        (1.00, 0.85, 0.65, 0.40, 0.20),
        (1.00, 0.80, 0.60, 0.30, 0.10),
    ]
    tactical_space = [
        (1.00, 0.95, 0.85, 0.75),
        (1.00, 0.90, 0.80, 0.70),
        (1.00, 0.85, 0.70, 0.55),
    ]
    alloc_shock_space = [
        (0.85, 0.70),
        (0.70, 0.50),
        (0.60, 0.40),
    ]
    for bins in base_bins_space:
        for allocs in base_allocs_space:
            for tac in tactical_space:
                for sh in alloc_shock_space:
                    candidates.append({
                        **agg_default,
                        'base_bins': list(bins),
                        'base_allocs': list(allocs),
                        'alloc_shock_high_cut': float(sh[0]),
                        'alloc_shock_risk_cut': float(sh[1]),
                        'tactical_multipliers': list(tac),
                    })
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser(description='V30 Phase-D tuning scan.')
    parser.add_argument('--features-csv', default='output/v30_features_daily.csv')
    parser.add_argument('--struct-pred-csv', default='output/v30_structural_train/structural_test_predictions.csv')
    parser.add_argument('--shock-pred-csv', default='output/v30_shock_train/shock_test_predictions.csv')
    parser.add_argument('--output-dir', default='output/v30_phase_d_tuning')
    parser.add_argument('--execution-lag-days', type=int, default=1)
    parser.add_argument('--transaction-cost-bps', type=float, default=2.0)
    parser.add_argument('--accept-dd-reduction-min', type=float, default=0.40)
    parser.add_argument('--accept-cagr-impact-max', type=float, default=0.02)
    parser.add_argument('--spy-ret-csv', default='output/v30_backtest_eval/v30_backtest_daily.csv')
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    feat = pd.read_csv(args.features_csv, parse_dates=['date'])
    st = pd.read_csv(args.struct_pred_csv, parse_dates=['date'])[['date', 'p_structural']]
    sh = pd.read_csv(args.shock_pred_csv, parse_dates=['date'])[['date', 'p_shock']]

    feat['date'] = pd.to_datetime(feat['date']).dt.normalize()
    st['date'] = pd.to_datetime(st['date']).dt.normalize()
    sh['date'] = pd.to_datetime(sh['date']).dt.normalize()
    merged = feat.merge(st, on='date', how='inner').merge(sh, on='date', how='inner').sort_values('date').reset_index(drop=True)

    ret = fetch_spy_ret(pd.Timestamp(merged['date'].min()), pd.Timestamp(merged['date'].max()), args.spy_ret_csv)
    merged = merged.merge(ret.rename('spy_ret1d'), left_on='date', right_index=True, how='left')
    merged['spy_ret1d'] = pd.to_numeric(merged['spy_ret1d'], errors='coerce').fillna(0.0)

    rows = []
    candidates = iter_candidate_configs()
    for cfg in candidates:
        agg_cfg = AggregationConfig(
            struct_high_cut=float(cfg['struct_high_cut']),
            struct_risk_cut=float(cfg['struct_risk_cut']),
            shock_high_cut=float(cfg['shock_high_cut']),
            shock_risk_cut=float(cfg['shock_risk_cut']),
        )
        alloc_cfg = AllocationConfig(
            base_bins=tuple(cfg['base_bins']),
            base_allocs=tuple(cfg['base_allocs']),
            shock_high_cut=float(cfg['alloc_shock_high_cut']),
            shock_risk_cut=float(cfg['alloc_shock_risk_cut']),
            tactical_multipliers=tuple(cfg['tactical_multipliers']),
        )

        x = aggregate_risk(merged, agg_cfg)
        x = apply_allocation(x, alloc_cfg)
        metric = evaluate(
            allocation=x['final_allocation'],
            spy_ret1d=x['spy_ret1d'],
            lag_days=max(0, int(args.execution_lag_days)),
            transaction_cost_bps=float(args.transaction_cost_bps),
        )
        ok = bool(
            metric['max_drawdown_reduction'] >= float(args.accept_dd_reduction_min)
            and metric['cagr_impact'] < float(args.accept_cagr_impact_max)
        )
        rows.append({
            **cfg,
            **metric,
            'acceptance_pass': ok,
        })

    res = pd.DataFrame(rows).sort_values(['acceptance_pass', 'max_drawdown_reduction', 'cagr_impact'], ascending=[False, False, True]).reset_index(drop=True)
    res.to_csv(out_dir / 'tuning_results.csv', index=False)

    passed = res[res['acceptance_pass'] == True].copy()
    if len(passed) > 0:
        best_row = passed.sort_values(['cagr_impact', 'max_drawdown_reduction'], ascending=[True, False]).iloc[0]
        selection_reason = 'Found candidate passing both acceptance gates.'
    else:
        # fallback: prioritize drawdown reduction while minimizing cagr impact
        res = res.copy()
        res['score'] = (
            (float(args.accept_dd_reduction_min) - res['max_drawdown_reduction']).clip(lower=0.0) * 4.0
            + (res['cagr_impact'] - float(args.accept_cagr_impact_max)).clip(lower=0.0) * 1.0
        )
        best_row = res.sort_values(['score', 'cagr_impact', 'max_drawdown_reduction'], ascending=[True, True, False]).iloc[0]
        selection_reason = 'No candidate passed both gates; selected best constrained trade-off.'

    best_cfg = {
        'struct_high_cut': float(best_row['struct_high_cut']),
        'struct_risk_cut': float(best_row['struct_risk_cut']),
        'shock_high_cut': float(best_row['shock_high_cut']),
        'shock_risk_cut': float(best_row['shock_risk_cut']),
        'base_bins': json.loads(best_row['base_bins']) if isinstance(best_row['base_bins'], str) else list(best_row['base_bins']),
        'base_allocs': json.loads(best_row['base_allocs']) if isinstance(best_row['base_allocs'], str) else list(best_row['base_allocs']),
        'alloc_shock_high_cut': float(best_row['alloc_shock_high_cut']),
        'alloc_shock_risk_cut': float(best_row['alloc_shock_risk_cut']),
        'tactical_multipliers': json.loads(best_row['tactical_multipliers']) if isinstance(best_row['tactical_multipliers'], str) else list(best_row['tactical_multipliers']),
        'scan_meta': {
            'candidate_count': int(len(rows)),
            'acceptance_pass_count': int((res['acceptance_pass'] == True).sum()),
            'selection_reason': selection_reason,
        },
    }
    (out_dir / 'best_tuning.json').write_text(json.dumps(best_cfg, ensure_ascii=False, indent=2), encoding='utf-8')

    lines = [
        '# V30 Phase-D Tuning Report',
        '',
        f"- candidate_count: {len(rows)}",
        f"- acceptance_pass_count: {int((res['acceptance_pass'] == True).sum())}",
        f"- selection_reason: {selection_reason}",
        '',
        '## Best Candidate',
        f"- cagr_impact: {float(best_row['cagr_impact']):.6f}",
        f"- max_drawdown_reduction: {float(best_row['max_drawdown_reduction']):.6f}",
        f"- avg_final_allocation: {float(best_row['avg_final_allocation']):.6f}",
        f"- strategy_annualized_return: {float(best_row['strategy_annualized_return']):.6f}",
        f"- strategy_max_drawdown: {float(best_row['strategy_max_drawdown']):.6f}",
        '',
        '## Usage',
        '- Apply with:',
        '```bash',
        'python scripts/run_v30_risk_aggregate.py \\',
        '  --features-csv output/v30_features_daily.csv \\',
        '  --struct-pred-csv output/v30_structural_train/structural_test_predictions.csv \\',
        '  --shock-pred-csv output/v30_shock_train/shock_test_predictions.csv \\',
        f'  --tuning-json {str((out_dir / "best_tuning.json")).replace("\\\\", "/")} \\',
        '  --output-dir output/v30_risk_aggregate',
        '```',
    ]
    (out_dir / 'tuning_report.md').write_text('\n'.join(lines), encoding='utf-8')

    print(f"[OK] Wrote: {out_dir / 'tuning_results.csv'}")
    print(f"[OK] Wrote: {out_dir / 'best_tuning.json'}")
    print(f"[OK] Wrote: {out_dir / 'tuning_report.md'}")


if __name__ == '__main__':
    main()
