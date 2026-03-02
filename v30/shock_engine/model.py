from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression


@dataclass
class ShockModelConfig:
    calibration: str = 'sigmoid'  # none|sigmoid|isotonic
    random_state: int = 42


def default_shock_feature_columns(df: pd.DataFrame) -> list[str]:
    blocked = {
        'date',
        'label_shock',
        'fw_dd_increase_5d',
        'fw_dd_increase_2d',
        'fw_dd_2d_share',
        'label_shock_drop_threshold_used',
        'label_structural',
        'fw_max_dd_30d',
        'fw_persist_dd_30d',
        # hard-gate specific columns: reserve for rule engine, not shock classifier
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
    # keep short-horizon oriented subset first if present
    preferred = [
        'ret5d',
        'ret5d_delta_1d',
        'dd_delta_1d',
        'vol20',
        'high_hurst_flag',
        'shock_like_flag',
        'drawdown_252',
        'drawdown_gt5_flag',
        'structural_pressure_score',
    ]
    pref = [c for c in preferred if c in cols]
    rest = [c for c in cols if c not in pref]
    return pref + rest


def train_shock_model(df_train: pd.DataFrame, feature_cols: Iterable[str], cfg: ShockModelConfig):
    x = df_train[list(feature_cols)].copy().fillna(0.0)
    y = pd.to_numeric(df_train['label_shock'], errors='coerce').astype(int)

    # Prefer GBDT; fallback to logistic if local runtime can't load tree backend.
    try:
        base = GradientBoostingClassifier(
            random_state=int(cfg.random_state),
            n_estimators=200,
            learning_rate=0.05,
            max_depth=2,
            subsample=0.9,
        )
        base.fit(x, y)
    except Exception:
        base = LogisticRegression(
            max_iter=1200,
            class_weight='balanced',
            random_state=int(cfg.random_state),
            solver='lbfgs',
        )
        base.fit(x, y)

    method = str(cfg.calibration).lower()
    if method == 'none':
        return base

    if method not in {'sigmoid', 'isotonic'}:
        raise ValueError(f'Unsupported calibration: {cfg.calibration}')

    # Adaptive CV to handle low positive-count windows.
    class_counts = y.value_counts().to_dict()
    min_class = int(min(class_counts.values())) if class_counts else 0
    if min_class < 2:
        return base
    cv = 3 if min_class >= 3 else 2

    model = CalibratedClassifierCV(estimator=base, method=method, cv=cv)
    model.fit(x, y)
    return model


def predict_proba(model, df: pd.DataFrame, feature_cols: Iterable[str]) -> np.ndarray:
    x = df[list(feature_cols)].copy().fillna(0.0)
    return np.asarray(model.predict_proba(x)[:, 1], dtype=float)
