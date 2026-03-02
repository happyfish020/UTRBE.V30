from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

import joblib
import pandas as pd
import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.structural_engine.labeling import add_structural_label
from v30.structural_engine.model import (
    StructuralModelConfig,
    default_feature_columns,
    predict_proba,
    train_structural_model,
)
from v30.structural_engine.evaluate import compute_metrics


def _connect():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost")),
        port=int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306"))),
        user=os.getenv("MYSQL_USER", os.getenv("DB_USER", "us_opr")),
        password=os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "sec@Bobo123")),
        database=os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "us_market")),
        charset=os.getenv("MYSQL_CHARSET", "utf8mb4"),
    )


def _load_features_table(table_name: str) -> pd.DataFrame:
    with _connect() as conn:
        df = pd.read_sql(f"SELECT * FROM `{table_name}` ORDER BY `date`", conn)
    if df.empty:
        raise ValueError(f"no rows in features table: {table_name}")
    df["date"] = pd.to_datetime(df["date"])
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description='V30 structural model train (Phase B bootstrap).')
    parser.add_argument('--features-csv', default='output/v30_features_daily.csv')
    parser.add_argument('--features-table', default='')
    parser.add_argument('--output-dir', default='output/v30_structural_train')
    parser.add_argument('--calibration', choices=['none', 'sigmoid', 'isotonic'], default='sigmoid')
    parser.add_argument('--test-years', type=int, default=2)
    parser.add_argument('--label-horizon-days', type=int, default=30)
    parser.add_argument('--label-dd-threshold', type=float, default=0.10)
    parser.add_argument('--label-persistence-days', type=int, default=15)
    args = parser.parse_args()

    in_csv = Path(args.features_csv)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if str(args.features_table).strip():
        df = _load_features_table(str(args.features_table).strip())
    else:
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
    labeled = labeled[labeled['label_structural'].notna()].copy()
    labeled['label_structural'] = labeled['label_structural'].astype(int)

    split_date = labeled['date'].max() - pd.DateOffset(years=int(args.test_years))
    train_df = labeled[labeled['date'] < split_date].copy()
    test_df = labeled[labeled['date'] >= split_date].copy()

    if train_df.empty or test_df.empty:
        raise ValueError('Train/test split is empty. Adjust test-years or input range.')
    if train_df['label_structural'].nunique() < 2 or test_df['label_structural'].nunique() < 2:
        raise ValueError('Label has a single class in train/test. Cannot train robust classifier.')

    feat_cols = default_feature_columns(labeled)
    model = train_structural_model(train_df, feature_cols=feat_cols, cfg=StructuralModelConfig(calibration=args.calibration))

    p_test = predict_proba(model, test_df, feat_cols)
    m = compute_metrics(test_df['label_structural'].to_numpy(), p_test, threshold=0.5)

    pred = test_df[['date', 'label_structural']].copy()
    pred['p_structural'] = p_test
    pred = pred.sort_values('date', ascending=False).reset_index(drop=True)
    pred.to_csv(out_dir / 'structural_test_predictions.csv', index=False)

    summary = {
        'rows_total': int(len(labeled)),
        'rows_train': int(len(train_df)),
        'rows_test': int(len(test_df)),
        'label_positive_rate_train': float(train_df['label_structural'].mean()),
        'label_positive_rate_test': float(test_df['label_structural'].mean()),
        'metrics': {
            'auc': float(m.auc),
            'brier': float(m.brier),
            'fpr': float(m.fpr),
            'recall': float(m.recall),
        },
        'calibration': str(args.calibration),
        'feature_columns': feat_cols,
        'split_date': str(pd.Timestamp(split_date).date()),
    }
    (out_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    joblib.dump({'model': model, 'feature_columns': feat_cols, 'summary': summary}, out_dir / 'structural_model.pkl')

    print(f"[OK] Wrote: {out_dir / 'structural_test_predictions.csv'}")
    print(f"[OK] Wrote: {out_dir / 'summary.json'}")
    print(f"[OK] Wrote: {out_dir / 'structural_model.pkl'}")


if __name__ == '__main__':
    main()
