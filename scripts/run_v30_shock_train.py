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

from v30.shock_engine.labeling import add_shock_label_proxy
from v30.shock_engine.model import ShockModelConfig, default_shock_feature_columns, predict_proba, train_shock_model
from v30.structural_engine.evaluate import compute_metrics


def _connect():
    try:
        return pymysql.connect(
            host=os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost")),
            port=int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306"))),
            user=os.getenv("MYSQL_USER", os.getenv("DB_USER", "us_opr")),
            password=os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "sec@Bobo123")),
            database=os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "us_market")),
            charset=os.getenv("MYSQL_CHARSET", "utf8mb4"),
        )
    except RuntimeError as exc:
        if "cryptography" in str(exc) and "caching_sha2_password" in str(exc):
            raise RuntimeError(
                "MySQL auth plugin 'caching_sha2_password' requires Python package "
                "'cryptography' when not using TLS. Fix one of these: "
                "(1) install cryptography in this venv, or "
                "(2) change DB user auth plugin to mysql_native_password."
            ) from exc
        raise


def _load_features_table(table_name: str) -> pd.DataFrame:
    with _connect() as conn:
        df = pd.read_sql(f"SELECT * FROM `{table_name}` ORDER BY `date`", conn)
    if df.empty:
        raise ValueError(f"no rows in features table: {table_name}")
    df["date"] = pd.to_datetime(df["date"])
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description='V30 shock model train (Phase C bootstrap).')
    parser.add_argument('--features-csv', default='output/v30_features_daily.csv')
    parser.add_argument('--features-table', default='')
    parser.add_argument('--output-dir', default='output/v30_shock_train')
    parser.add_argument('--calibration', choices=['none', 'sigmoid', 'isotonic'], default='sigmoid')
    parser.add_argument('--test-years', type=int, default=2)
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

    if str(args.features_table).strip():
        df = _load_features_table(str(args.features_table).strip()).sort_values('date').reset_index(drop=True)
    else:
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
    labeled = labeled[labeled['label_shock'].notna()].copy()
    labeled['label_shock'] = labeled['label_shock'].astype(int)

    split_date = labeled['date'].max() - pd.DateOffset(years=int(args.test_years))
    train_df = labeled[labeled['date'] < split_date].copy()
    test_df = labeled[labeled['date'] >= split_date].copy()

    if train_df.empty or test_df.empty:
        raise ValueError('Train/test split is empty. Adjust test-years or input range.')
    if train_df['label_shock'].nunique() < 2 or test_df['label_shock'].nunique() < 2:
        raise ValueError('Shock label has a single class in train/test. Cannot train robust classifier.')

    feat_cols = default_shock_feature_columns(labeled)
    model = train_shock_model(train_df, feature_cols=feat_cols, cfg=ShockModelConfig(calibration=args.calibration))

    p_test = predict_proba(model, test_df, feat_cols)
    m = compute_metrics(test_df['label_shock'].to_numpy(), p_test, threshold=0.5)

    pred = test_df[['date', 'label_shock']].copy()
    pred['p_shock'] = p_test
    pred = pred.sort_values('date', ascending=False).reset_index(drop=True)
    pred.to_csv(out_dir / 'shock_test_predictions.csv', index=False)

    summary = {
        'rows_total': int(len(labeled)),
        'rows_train': int(len(train_df)),
        'rows_test': int(len(test_df)),
        'label_positive_rate_train': float(train_df['label_shock'].mean()),
        'label_positive_rate_test': float(test_df['label_shock'].mean()),
        'metrics': {
            'auc': float(m.auc),
            'brier': float(m.brier),
            'fpr': float(m.fpr),
            'recall': float(m.recall),
        },
        'calibration': str(args.calibration),
        'feature_columns': feat_cols,
        'split_date': str(pd.Timestamp(split_date).date()),
        'label_note': 'proxy shock label from drawdown-path increase in next 5d',
        'labeling': {
            'horizon_days': int(args.label_horizon_days),
            'drop_threshold': float(args.label_drop_threshold),
            'early_share_threshold': float(args.label_early_share_threshold),
            'adaptive_drop': bool(args.label_adaptive_drop),
            'target_positive_rate': float(args.label_target_positive_rate),
            'min_drop_threshold': float(args.label_min_drop_threshold),
            'max_drop_threshold': float(args.label_max_drop_threshold),
            'use_stress_override': bool(args.label_use_stress_override),
            'stress_gate': float(args.label_stress_gate),
            'drop_threshold_used': float(pd.to_numeric(labeled.get('label_shock_drop_threshold_used'), errors='coerce').dropna().iloc[0]) if 'label_shock_drop_threshold_used' in labeled.columns and pd.to_numeric(labeled.get('label_shock_drop_threshold_used'), errors='coerce').notna().any() else float(args.label_drop_threshold),
        },
    }
    (out_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    joblib.dump({'model': model, 'feature_columns': feat_cols, 'summary': summary}, out_dir / 'shock_model.pkl')

    print(f"[OK] Wrote: {out_dir / 'shock_test_predictions.csv'}")
    print(f"[OK] Wrote: {out_dir / 'summary.json'}")
    print(f"[OK] Wrote: {out_dir / 'shock_model.pkl'}")


if __name__ == '__main__':
    main()
