from __future__ import annotations

import argparse
import os
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

from v30.data_layer import connect_backtest_db, quote_ident, resolve_backtest_db_path, upsert_dataframe


def _create_mysql_engine():
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


def _list_mysql_tables(engine) -> set[str]:
    with engine.connect() as conn:
        df = pd.read_sql_query(text("SHOW TABLES"), conn)
    return {str(v) for v in df.iloc[:, 0].tolist()}


def _copy_table(engine, sqlite_conn, table_name: str, chunksize: int) -> int:
    total = 0
    q = quote_ident(table_name)
    sql = f"SELECT * FROM {q} ORDER BY {quote_ident('date')}"
    with engine.connect() as conn:
        for chunk in pd.read_sql_query(text(sql), conn, chunksize=int(chunksize)):
            if chunk.empty:
                continue
            int_cols = {c for c in chunk.columns if c.endswith("_level") or c.endswith("_flag") or c in {"label_structural", "label_shock", "recovery_days", "guardrail_applied"}}
            key_cols = ("date",) if "date" in chunk.columns else tuple([str(chunk.columns[0])])
            total += upsert_dataframe(
                sqlite_conn,
                chunk,
                table_name=table_name,
                key_cols=key_cols,
                int_cols=int_cols,
            )
    return total


def main() -> None:
    p = argparse.ArgumentParser(description="Migrate V30 backtest tables from MySQL to SQLite.")
    p.add_argument("--db-path", default="data/backtest/v30_backtest.sqlite")
    p.add_argument(
        "--tables",
        nargs="*",
        default=[
            "market_features_daily",
            "v30_features_daily",
            "v30_structural_labels_daily",
            "v30_shock_labels_daily",
            "v30_structural_full_predictions_daily",
            "v30_shock_full_predictions_daily",
            "v31_daily_allocation",
            "v30_backtest_daily",
        ],
    )
    p.add_argument("--chunksize", type=int, default=2000)
    args = p.parse_args()

    target = resolve_backtest_db_path(str(args.db_path).strip() or None)
    mysql_engine = _create_mysql_engine()
    mysql_tables = _list_mysql_tables(mysql_engine)

    with connect_backtest_db(str(target)) as sqlite_conn:
        for t in [str(x).strip() for x in args.tables if str(x).strip()]:
            if t not in mysql_tables:
                print(f"[SKIP] missing mysql table: {t}")
                continue
            rows = _copy_table(mysql_engine, sqlite_conn, t, chunksize=int(args.chunksize))
            print(f"[OK] migrated rows: {rows} -> {t}")

    print(f"[OK] sqlite path: {target}")


if __name__ == "__main__":
    main()

