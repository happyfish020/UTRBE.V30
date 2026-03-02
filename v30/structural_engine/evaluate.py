from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, recall_score, roc_auc_score

from .model import StructuralModelConfig, train_structural_model, predict_proba


@dataclass
class EvalMetrics:
    auc: float
    brier: float
    fpr: float
    recall: float


def compute_metrics(y_true: np.ndarray, p: np.ndarray, threshold: float = 0.5) -> EvalMetrics:
    y_true = np.asarray(y_true, dtype=int)
    p = np.asarray(p, dtype=float)
    pred = (p >= float(threshold)).astype(int)

    auc = float('nan')
    if len(np.unique(y_true)) >= 2:
        auc = float(roc_auc_score(y_true, p))

    brier = float(brier_score_loss(y_true, p))
    recall = float(recall_score(y_true, pred, zero_division=0))

    neg = y_true == 0
    fpr = float(np.mean(pred[neg] == 1)) if np.any(neg) else float('nan')
    return EvalMetrics(auc=auc, brier=brier, fpr=fpr, recall=recall)


def rolling_walkforward_eval(
    df: pd.DataFrame,
    feature_cols: list[str],
    calibration: str = 'sigmoid',
    train_years: int = 3,
    test_years: int = 1,
    step_years: int = 1,
) -> pd.DataFrame:
    x = df.sort_values('date').reset_index(drop=True).copy()
    x = x[x['label_structural'].notna()].copy()
    x['date'] = pd.to_datetime(x['date'])

    start = pd.Timestamp(x['date'].min())
    end = pd.Timestamp(x['date'].max())

    rows = []
    cursor = start
    while True:
        train_start = cursor
        train_end = train_start + pd.DateOffset(years=int(train_years))
        test_end = train_end + pd.DateOffset(years=int(test_years))
        if test_end > end:
            break

        tr = x[(x['date'] >= train_start) & (x['date'] < train_end)].copy()
        te = x[(x['date'] >= train_end) & (x['date'] < test_end)].copy()
        if len(tr) < 150 or len(te) < 60:
            cursor = cursor + pd.DateOffset(years=int(step_years))
            continue
        if tr['label_structural'].nunique() < 2 or te['label_structural'].nunique() < 2:
            cursor = cursor + pd.DateOffset(years=int(step_years))
            continue

        model = train_structural_model(tr, feature_cols=feature_cols, cfg=StructuralModelConfig(calibration=calibration))
        p = predict_proba(model, te, feature_cols=feature_cols)
        m = compute_metrics(te['label_structural'].to_numpy(), p, threshold=0.5)

        rows.append(
            {
                'train_start': str(train_start.date()),
                'train_end': str((train_end - pd.Timedelta(days=1)).date()),
                'test_start': str(train_end.date()),
                'test_end': str((test_end - pd.Timedelta(days=1)).date()),
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
