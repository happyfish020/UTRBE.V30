from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.shock_engine.labeling import add_shock_label_proxy
from v30.shock_engine.model import default_shock_feature_columns, ShockModelConfig, train_shock_model, predict_proba
from v30.structural_engine.evaluate import compute_metrics


def rolling_eval(
    df: pd.DataFrame,
    feat_cols: list[str],
    calibration: str,
    train_years: int,
    test_years: int,
    step_years: int,
) -> pd.DataFrame:
    x = df.sort_values('date').reset_index(drop=True).copy()
    x = x[x['label_shock'].notna()].copy()
    x['date'] = pd.to_datetime(x['date'])

    start = pd.Timestamp(x['date'].min())
    end = pd.Timestamp(x['date'].max())
    rows = []
    cursor = start

    while True:
        tr0 = cursor
        tr1 = tr0 + pd.DateOffset(years=int(train_years))
        te1 = tr1 + pd.DateOffset(years=int(test_years))
        if te1 > end:
            break

        tr = x[(x['date'] >= tr0) & (x['date'] < tr1)].copy()
        te = x[(x['date'] >= tr1) & (x['date'] < te1)].copy()
        if len(tr) < 150 or len(te) < 60:
            cursor = cursor + pd.DateOffset(years=int(step_years))
            continue
        if tr['label_shock'].nunique() < 2 or te['label_shock'].nunique() < 2:
            cursor = cursor + pd.DateOffset(years=int(step_years))
            continue

        model = train_shock_model(tr, feature_cols=feat_cols, cfg=ShockModelConfig(calibration=calibration))
        p = predict_proba(model, te, feat_cols)
        m = compute_metrics(te['label_shock'].to_numpy(), p, threshold=0.5)

        rows.append(
            {
                'train_start': str(tr0.date()),
                'train_end': str((tr1 - pd.Timedelta(days=1)).date()),
                'test_start': str(tr1.date()),
                'test_end': str((te1 - pd.Timedelta(days=1)).date()),
                'n_train': int(len(tr)),
                'n_test': int(len(te)),
                'auc': m.auc,
                'brier': m.brier,
                'fpr': m.fpr,
                'recall': m.recall,
            }
        )

        cursor = cursor + pd.DateOffset(years=int(step_years))

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description='V30 shock rolling walk-forward eval (Phase C bootstrap).')
    parser.add_argument('--features-csv', default='output/v30_features_daily.csv')
    parser.add_argument('--output-dir', default='output/v30_shock_eval')
    parser.add_argument('--calibration', choices=['none', 'sigmoid', 'isotonic'], default='sigmoid')
    parser.add_argument('--train-years', type=int, default=3)
    parser.add_argument('--test-years', type=int, default=1)
    parser.add_argument('--step-years', type=int, default=1)
    parser.add_argument('--label-horizon-days', type=int, default=5)
    parser.add_argument('--label-drop-threshold', type=float, default=0.07)
    parser.add_argument('--label-early-share-threshold', type=float, default=0.60)
    parser.add_argument('--label-adaptive-drop', action='store_true')
    parser.add_argument('--label-target-positive-rate', type=float, default=0.04)
    parser.add_argument('--label-min-drop-threshold', type=float, default=0.02)
    parser.add_argument('--label-max-drop-threshold', type=float, default=0.10)
    parser.add_argument('--label-use-stress-override', action='store_true')
    parser.add_argument('--label-stress-gate', type=float, default=0.55)
    args = parser.parse_args()

    in_csv = Path(args.features_csv)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_csv.exists():
        raise FileNotFoundError(f'features csv not found: {in_csv}')

    df = pd.read_csv(in_csv, parse_dates=['date']).sort_values('date').reset_index(drop=True)
    labeled = add_shock_label_proxy(
        df,
        horizon_days=int(args.label_horizon_days),
        drop_threshold=float(args.label_drop_threshold),
        early_share_threshold=float(args.label_early_share_threshold),
        adaptive_drop_threshold=bool(args.label_adaptive_drop),
        target_positive_rate=float(args.label_target_positive_rate),
        min_drop_threshold=float(args.label_min_drop_threshold),
        max_drop_threshold=float(args.label_max_drop_threshold),
        use_stress_override=bool(args.label_use_stress_override),
        stress_gate=float(args.label_stress_gate),
    )

    feat_cols = default_shock_feature_columns(labeled)
    wf = rolling_eval(
        labeled,
        feat_cols=feat_cols,
        calibration=str(args.calibration),
        train_years=int(args.train_years),
        test_years=int(args.test_years),
        step_years=int(args.step_years),
    )

    wf = wf.sort_values('test_start', ascending=False).reset_index(drop=True)
    wf.to_csv(out_dir / 'walkforward_window_metrics.csv', index=False)

    summary = {
        'windows': int(len(wf)),
        'worst_auc': float(wf['auc'].min()) if not wf.empty else float('nan'),
        'mean_auc': float(wf['auc'].mean()) if not wf.empty else float('nan'),
        'mean_fpr': float(wf['fpr'].mean()) if not wf.empty else float('nan'),
        'calibration': str(args.calibration),
        'train_years': int(args.train_years),
        'test_years': int(args.test_years),
        'step_years': int(args.step_years),
        'label_note': 'proxy shock label from drawdown-path increase in next 5d',
        'labeling': {
            'adaptive_drop': bool(args.label_adaptive_drop),
            'target_positive_rate': float(args.label_target_positive_rate),
            'use_stress_override': bool(args.label_use_stress_override),
            'stress_gate': float(args.label_stress_gate),
        },
    }
    (out_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f"[OK] Wrote: {out_dir / 'walkforward_window_metrics.csv'}")
    print(f"[OK] Wrote: {out_dir / 'summary.json'}")


if __name__ == '__main__':
    main()
