from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.structural_engine.evaluate import rolling_walkforward_eval
from v30.structural_engine.labeling import add_structural_label
from v30.structural_engine.model import default_feature_columns


def main() -> None:
    parser = argparse.ArgumentParser(description='V30 structural rolling walk-forward eval (Phase B bootstrap).')
    parser.add_argument('--features-csv', default='output/v30_features_daily.csv')
    parser.add_argument('--output-dir', default='output/v30_structural_eval')
    parser.add_argument('--calibration', choices=['none', 'sigmoid', 'isotonic'], default='sigmoid')
    parser.add_argument('--train-years', type=int, default=3)
    parser.add_argument('--test-years', type=int, default=1)
    parser.add_argument('--step-years', type=int, default=1)
    parser.add_argument('--label-horizon-days', type=int, default=30)
    parser.add_argument('--label-dd-threshold', type=float, default=0.10)
    parser.add_argument('--label-persistence-days', type=int, default=15)
    args = parser.parse_args()

    in_csv = Path(args.features_csv)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_csv.exists():
        raise FileNotFoundError(f'features csv not found: {in_csv}')

    df = pd.read_csv(in_csv, parse_dates=['date'])
    df = df.sort_values('date').reset_index(drop=True)
    labeled = add_structural_label(
        df,
        horizon_days=int(args.label_horizon_days),
        drawdown_threshold=float(args.label_dd_threshold),
        persistence_days=int(args.label_persistence_days),
    )

    feat_cols = default_feature_columns(labeled)
    wf = rolling_walkforward_eval(
        labeled,
        feature_cols=feat_cols,
        calibration=str(args.calibration),
        train_years=int(args.train_years),
        test_years=int(args.test_years),
        step_years=int(args.step_years),
    )

    wf = wf.sort_values('test_start', ascending=False).reset_index(drop=True)
    wf.to_csv(out_dir / 'walkforward_window_metrics.csv', index=False)

    worst_auc = float(wf['auc'].min()) if not wf.empty else float('nan')
    mean_auc = float(wf['auc'].mean()) if not wf.empty else float('nan')
    mean_fpr = float(wf['fpr'].mean()) if not wf.empty else float('nan')

    summary = {
        'windows': int(len(wf)),
        'worst_auc': worst_auc,
        'mean_auc': mean_auc,
        'mean_fpr': mean_fpr,
        'calibration': str(args.calibration),
        'train_years': int(args.train_years),
        'test_years': int(args.test_years),
        'step_years': int(args.step_years),
    }
    (out_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f"[OK] Wrote: {out_dir / 'walkforward_window_metrics.csv'}")
    print(f"[OK] Wrote: {out_dir / 'summary.json'}")


if __name__ == '__main__':
    main()
