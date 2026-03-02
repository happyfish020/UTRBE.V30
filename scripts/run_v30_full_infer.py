from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from urllib.parse import quote_plus

import joblib
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.structural_engine.model import predict_proba as struct_predict_proba
from v30.shock_engine.model import predict_proba as shock_predict_proba


def _load_bundle(path: Path) -> tuple[object, list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"missing model bundle: {path}")
    obj = joblib.load(path)
    if not isinstance(obj, dict) or "model" not in obj or "feature_columns" not in obj:
        raise ValueError(f"invalid model bundle format: {path}")
    return obj["model"], list(obj["feature_columns"])


def _create_engine() -> Engine:
    host = os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost"))
    port = int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306")))
    user = os.getenv("MYSQL_USER", os.getenv("DB_USER", "us_opr"))
    password = os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "sec@Bobo123"))
    database = os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "us_market"))
    charset = os.getenv("MYSQL_CHARSET", "utf8mb4")
    url = (
        f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(database)}?charset={quote_plus(charset)}"
    )
    return create_engine(url, pool_pre_ping=True)


def _quote(name: str) -> str:
    return f"`{str(name).replace('`', '``')}`"


def _load_features_from_db(engine: Engine, table_name: str, start: str = "", end: str = "") -> pd.DataFrame:
    cond = ""
    params: dict[str, str] = {}
    if str(start).strip() and str(end).strip():
        cond = " WHERE `date` BETWEEN :start AND :end"
        params = {"start": str(start), "end": str(end)}
    sql = f"SELECT * FROM {_quote(table_name)}{cond} ORDER BY `date`"
    with engine.connect() as conn:
        df = pd.read_sql_query(text(sql), conn, params=params)
        if df.empty and str(end).strip():
            # Production fallback: if target day is not materialized yet, reuse
            # latest available feature row up to end-date and stamp target date.
            fb_sql = f"SELECT * FROM {_quote(table_name)} WHERE `date` <= :end ORDER BY `date` DESC LIMIT 1"
            fb = pd.read_sql_query(text(fb_sql), conn, params={"end": str(end)})
            if not fb.empty:
                fb["feature_source_date"] = pd.to_datetime(fb["date"]).dt.normalize()
                fb["date"] = pd.to_datetime(str(end)).normalize()
                df = fb
    if df.empty:
        raise ValueError(f"no rows in features table: {table_name}")
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df.sort_values("date").reset_index(drop=True)


def _upsert_pred(engine: Engine, df: pd.DataFrame, table_name: str, value_col: str) -> int:
    if df.empty:
        return 0
    x = df.copy()
    x["date"] = pd.to_datetime(x["date"]).dt.date
    q_table = _quote(table_name)
    q_val = _quote(value_col)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"CREATE TABLE IF NOT EXISTS {q_table} (`date` DATE NOT NULL PRIMARY KEY, {q_val} DOUBLE NULL)"
        )
        existing = {str(r[0]) for r in conn.exec_driver_sql(f"SHOW COLUMNS FROM {q_table}")}
        if value_col not in existing:
            conn.exec_driver_sql(f"ALTER TABLE {q_table} ADD COLUMN {q_val} DOUBLE NULL")
        sql = (
            f"INSERT INTO {q_table} (`date`, {q_val}) VALUES (%s,%s) "
            f"ON DUPLICATE KEY UPDATE {q_val}=VALUES({q_val})"
        )
        rows = [
            tuple(r)
            for r in x[["date", value_col]]
            .astype(object)
            .where(pd.notna(x[["date", value_col]]), None)
            .to_numpy()
            .tolist()
        ]
        conn.exec_driver_sql(sql, rows)
        return len(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Run full-range inference for structural/shock models.")
    p.add_argument("--features-csv", default="output/v30_features_daily.csv")
    p.add_argument("--features-table", default="")
    p.add_argument("--features-start", default="")
    p.add_argument("--features-end", default="")
    p.add_argument("--struct-model-pkl", default="output/v30_structural_train/structural_model.pkl")
    p.add_argument("--shock-model-pkl", default="output/v30_shock_train_step4/shock_model.pkl")
    p.add_argument("--struct-output-csv", default="output/v30_structural_train/structural_full_predictions.csv")
    p.add_argument("--shock-output-csv", default="output/v30_shock_train_step4/shock_full_predictions.csv")
    p.add_argument("--struct-output-table", default="v30_structural_full_predictions_daily")
    p.add_argument("--shock-output-table", default="v30_shock_full_predictions_daily")
    p.add_argument("--skip-db-upsert", action="store_true")
    p.add_argument("--skip-csv-output", action="store_true")
    args = p.parse_args()
    engine = _create_engine()

    if str(args.features_table).strip():
        feat = _load_features_from_db(
            engine=engine,
            table_name=str(args.features_table).strip(),
            start=str(args.features_start).strip(),
            end=str(args.features_end).strip(),
        )
    else:
        feat_path = Path(args.features_csv)
        if not feat_path.exists():
            raise FileNotFoundError(f"missing features csv: {feat_path}")
        feat = pd.read_csv(feat_path, parse_dates=["date"]).sort_values("date").reset_index(drop=True)

    s_model, s_cols = _load_bundle(Path(args.struct_model_pkl))
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
        s_rows = _upsert_pred(engine, s_pred, table_name=str(args.struct_output_table), value_col="p_structural")
        k_rows = _upsert_pred(engine, k_pred, table_name=str(args.shock_output_table), value_col="p_shock")

    if not bool(args.skip_csv_output):
        print(f"[OK] Wrote: {s_out}")
        print(f"[OK] Wrote: {k_out}")
    if not bool(args.skip_db_upsert):
        print(f"[OK] DB upsert rows: {s_rows} -> table `{args.struct_output_table}`")
        print(f"[OK] DB upsert rows: {k_rows} -> table `{args.shock_output_table}`")


if __name__ == "__main__":
    main()
