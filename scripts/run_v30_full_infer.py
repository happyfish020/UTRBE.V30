from __future__ import annotations

import argparse
from pathlib import Path
import sys

import joblib
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.structural_engine.model import predict_proba as struct_predict_proba
from v30.shock_engine.model import predict_proba as shock_predict_proba
from v30.data_layer import connect_backtest_db, load_model_bundle, quote_ident, upsert_dataframe


def _load_bundle(path: Path) -> tuple[object, list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"missing model bundle: {path}")
    obj = joblib.load(path)
    if not isinstance(obj, dict) or "model" not in obj or "feature_columns" not in obj:
        raise ValueError(f"invalid model bundle format: {path}")
    return obj["model"], list(obj["feature_columns"])


def _load_bundle_from_db(db_path: str, model_key: str) -> tuple[object, list[str]]:
    with connect_backtest_db(str(db_path).strip() or None) as conn:
        obj = load_model_bundle(conn, model_key=str(model_key).strip())
    if not isinstance(obj, dict) or "model" not in obj or "feature_columns" not in obj:
        raise ValueError(f"invalid sqlite model bundle format: {model_key}")
    return obj["model"], list(obj["feature_columns"])


def _load_features_from_db(conn, table_name: str, start: str = "", end: str = "") -> pd.DataFrame:
    q_table = quote_ident(table_name)
    cond = ""
    params: list[str] = []
    if str(start).strip() and str(end).strip():
        cond = ' WHERE "date" BETWEEN ? AND ?'
        params = [str(start), str(end)]
    sql = f'SELECT * FROM {q_table}{cond} ORDER BY "date"'
    df = pd.read_sql_query(sql, conn, params=params)
    if df.empty and str(end).strip():
        # Production fallback: if target day is not materialized yet, reuse
        # latest available feature row up to end-date and stamp target date.
        fb_sql = f'SELECT * FROM {q_table} WHERE "date" <= ? ORDER BY "date" DESC LIMIT 1'
        fb = pd.read_sql_query(fb_sql, conn, params=[str(end)])
        if not fb.empty:
            fb["feature_source_date"] = pd.to_datetime(fb["date"]).dt.normalize()
            fb["date"] = pd.to_datetime(str(end)).normalize()
            df = fb
    if df.empty:
        raise ValueError(f"no rows in features table: {table_name}")
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df.sort_values("date").reset_index(drop=True)


def _upsert_pred(conn, df: pd.DataFrame, table_name: str, value_col: str) -> int:
    if df.empty:
        return 0
    return upsert_dataframe(
        conn,
        df[["date", value_col]].copy(),
        table_name=table_name,
        key_cols=("date",),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Run full-range inference for structural/shock models.")
    p.add_argument("--features-csv", default="output/v30_features_daily.csv")
    p.add_argument("--features-table", default="")
    p.add_argument("--features-start", default="")
    p.add_argument("--features-end", default="")
    p.add_argument("--struct-model-pkl", default="output/v30_structural_train/structural_model.pkl")
    p.add_argument("--shock-model-pkl", default="output/v30_shock_train_step4/shock_model.pkl")
    p.add_argument("--artifact-db-path", default="data/backtest/v30_backtest.sqlite")
    p.add_argument("--struct-model-key", default="")
    p.add_argument("--shock-model-key", default="")
    p.add_argument("--struct-output-csv", default="output/v30_structural_train/structural_full_predictions.csv")
    p.add_argument("--shock-output-csv", default="output/v30_shock_train_step4/shock_full_predictions.csv")
    p.add_argument("--struct-output-table", default="v30_structural_full_predictions_daily")
    p.add_argument("--shock-output-table", default="v30_shock_full_predictions_daily")
    p.add_argument("--db-path", default="data/backtest/v30_backtest.sqlite")
    p.add_argument("--skip-db-upsert", action="store_true")
    p.add_argument("--skip-csv-output", action="store_true")
    args = p.parse_args()

    if str(args.features_table).strip():
        with connect_backtest_db(str(args.db_path).strip() or None) as conn:
            feat = _load_features_from_db(
                conn=conn,
                table_name=str(args.features_table).strip(),
                start=str(args.features_start).strip(),
                end=str(args.features_end).strip(),
            )
    else:
        feat_path = Path(args.features_csv)
        if not feat_path.exists():
            raise FileNotFoundError(f"missing features csv: {feat_path}")
        feat = pd.read_csv(feat_path, parse_dates=["date"]).sort_values("date").reset_index(drop=True)

    if str(args.struct_model_key).strip():
        try:
            s_model, s_cols = _load_bundle_from_db(str(args.artifact_db_path), str(args.struct_model_key))
        except Exception:
            s_model, s_cols = _load_bundle(Path(args.struct_model_pkl))
    else:
        s_model, s_cols = _load_bundle(Path(args.struct_model_pkl))
    if str(args.shock_model_key).strip():
        try:
            k_model, k_cols = _load_bundle_from_db(str(args.artifact_db_path), str(args.shock_model_key))
        except Exception:
            k_model, k_cols = _load_bundle(Path(args.shock_model_pkl))
    else:
        k_model, k_cols = _load_bundle(Path(args.shock_model_pkl))

    s_pred = pd.DataFrame({"date": feat["date"]})
    s_pred["p_structural"] = struct_predict_proba(s_model, feat, s_cols)
    s_out = Path(args.struct_output_csv)
    if not bool(args.skip_csv_output):
        s_out.parent.mkdir(parents=True, exist_ok=True)
        s_pred.sort_values("date", ascending=False).to_csv(s_out, index=False)

    k_pred = pd.DataFrame({"date": feat["date"]})
    k_pred["p_shock"] = shock_predict_proba(k_model, feat, k_cols)
    k_out = Path(args.shock_output_csv)
    if not bool(args.skip_csv_output):
        k_out.parent.mkdir(parents=True, exist_ok=True)
        k_pred.sort_values("date", ascending=False).to_csv(k_out, index=False)

    s_rows = 0
    k_rows = 0
    if not bool(args.skip_db_upsert):
        with connect_backtest_db(str(args.db_path).strip() or None) as conn:
            s_rows = _upsert_pred(conn, s_pred, table_name=str(args.struct_output_table), value_col="p_structural")
            k_rows = _upsert_pred(conn, k_pred, table_name=str(args.shock_output_table), value_col="p_shock")

    if not bool(args.skip_csv_output):
        print(f"[OK] Wrote: {s_out}")
        print(f"[OK] Wrote: {k_out}")
    if not bool(args.skip_db_upsert):
        print(f"[OK] DB upsert rows: {s_rows} -> table `{args.struct_output_table}`")
        print(f"[OK] DB upsert rows: {k_rows} -> table `{args.shock_output_table}`")


if __name__ == "__main__":
    main()

