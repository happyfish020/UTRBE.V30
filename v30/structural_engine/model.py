from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression


@dataclass
class StructuralModelConfig:
    calibration: str = 'sigmoid'  # none|sigmoid|isotonic
    random_state: int = 42


def default_feature_columns(df: pd.DataFrame) -> list[str]:
    blocked = {
        'date',
        'label_structural',
        'fw_max_dd_30d',
        'fw_persist_dd_30d',
        # hard-gate specific columns: keep out of structural model to avoid drift/leakage
        'market_price',
        'credit_real',
        'ret5d_median_20',
        'price_below_200dma_flag',
        'credit_chg_20d',
    }
    cols = []
    for c in df.columns:
        if c in blocked:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)
    return cols


def train_structural_model(
    df_train: pd.DataFrame,
    feature_cols: Iterable[str],
    cfg: StructuralModelConfig,
):
    x = df_train[list(feature_cols)].copy().fillna(0.0)
    y = pd.to_numeric(df_train['label_structural'], errors='coerce').astype(int)

    base = LogisticRegression(
        max_iter=1200,
        class_weight='balanced',
        random_state=int(cfg.random_state),
        solver='lbfgs',
    )

    method = str(cfg.calibration).lower()
    if method == 'none':
        model = base.fit(x, y)
    elif method in {'sigmoid', 'isotonic'}:
        model = CalibratedClassifierCV(estimator=base, method=method, cv=3)
        model.fit(x, y)
    else:
        raise ValueError(f'Unsupported calibration: {cfg.calibration}')

    return model


def predict_proba(model, df: pd.DataFrame, feature_cols: Iterable[str]) -> np.ndarray:
    x = df[list(feature_cols)].copy().fillna(0.0)
    p = model.predict_proba(x)[:, 1]
    return np.asarray(p, dtype=float)
