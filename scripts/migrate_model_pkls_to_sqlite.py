from __future__ import annotations

import argparse
from pathlib import Path
import sys

import joblib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.data_layer import connect_backtest_db, upsert_model_bundle


def _load_bundle(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"missing model pkl: {path}")
    obj = joblib.load(path)
    if not isinstance(obj, dict) or "model" not in obj or "feature_columns" not in obj:
        raise ValueError(f"invalid model bundle format: {path}")
    return obj


def main() -> None:
    p = argparse.ArgumentParser(description="Migrate model pkl bundles into SQLite model store.")
    p.add_argument("--db-path", default="data/backtest/v30_backtest.sqlite")
    p.add_argument("--struct-pkl", default="artifacts/v31_train_assets/v30_structural_train/structural_model.pkl")
    p.add_argument("--shock-pkl", default="artifacts/v31_train_assets/v30_shock_train_step4/shock_model.pkl")
    p.add_argument("--struct-key", default="v30_structural_model_prod")
    p.add_argument("--shock-key", default="v30_shock_model_prod")
    p.add_argument("--delete-after", action="store_true")
    args = p.parse_args()

    s_path = Path(str(args.struct_pkl)).resolve()
    k_path = Path(str(args.shock_pkl)).resolve()
    s_obj = _load_bundle(s_path)
    k_obj = _load_bundle(k_path)

    with connect_backtest_db(str(args.db_path).strip() or None) as conn:
        upsert_model_bundle(conn, str(args.struct_key), s_obj, metadata={"source": str(s_path)})
        upsert_model_bundle(conn, str(args.shock_key), k_obj, metadata={"source": str(k_path)})

    if bool(args.delete_after):
        s_path.unlink(missing_ok=True)
        k_path.unlink(missing_ok=True)

    print(f"[OK] SQLite model upsert: {args.struct_key}")
    print(f"[OK] SQLite model upsert: {args.shock_key}")
    if bool(args.delete_after):
        print(f"[OK] deleted: {s_path}")
        print(f"[OK] deleted: {k_path}")


if __name__ == "__main__":
    main()

