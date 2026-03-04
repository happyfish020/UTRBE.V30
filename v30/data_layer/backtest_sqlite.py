from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BACKTEST_DB_PATH = Path("data") / "backtest" / "v30_backtest.sqlite"


def _to_project_path(p: Path) -> Path:
    if p.is_absolute():
        return p
    return (PROJECT_ROOT / p).resolve()


def resolve_backtest_db_path(explicit_path: str | None = None) -> Path:
    has_explicit = bool(explicit_path and str(explicit_path).strip())
    env_db = str(os.getenv("V30_BACKTEST_DB_PATH", "")).strip()
    env_dir = str(os.getenv("V30_BACKTEST_DATA_DIR", "")).strip()
    if has_explicit:
        p = _to_project_path(Path(str(explicit_path).strip()))
    else:
        if env_db:
            p = _to_project_path(Path(env_db))
        else:
            data_dir = env_dir or "data/backtest"
            db_file = str(os.getenv("V30_BACKTEST_DB_FILE", "v30_backtest.sqlite")).strip() or "v30_backtest.sqlite"
            p = _to_project_path(Path(data_dir) / db_file)
    if (not has_explicit) and (not env_db) and (not env_dir):
        p = _to_project_path(DEFAULT_BACKTEST_DB_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def connect_backtest_db(db_path: str | None = None) -> sqlite3.Connection:
    path = resolve_backtest_db_path(db_path)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    q_table = quote_ident(table_name)
    rows = conn.execute(f"PRAGMA table_info({q_table})").fetchall()
    return [str(r[1]) for r in rows]


def _infer_col_type(df: pd.DataFrame, col: str, int_cols: set[str], text_cols: set[str]) -> str:
    if col in text_cols:
        return "TEXT"
    if col in int_cols:
        return "INTEGER"
    if col == "date" or col.endswith("_date"):
        return "TEXT"
    s = df[col]
    if pd.api.types.is_integer_dtype(s):
        return "INTEGER"
    if pd.api.types.is_numeric_dtype(s):
        return "REAL"
    return "TEXT"


def _normalize_df_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    for c in x.columns:
        if c == "date" or c.endswith("_date"):
            x[c] = pd.to_datetime(x[c], errors="coerce").dt.strftime("%Y-%m-%d")
    return x


def upsert_dataframe(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    table_name: str,
    key_cols: Iterable[str] = ("date",),
    int_cols: set[str] | None = None,
    text_cols: set[str] | None = None,
) -> int:
    if df.empty:
        return 0

    key_cols = tuple(str(c) for c in key_cols)
    int_cols = set(int_cols or set())
    text_cols = set(text_cols or set())
    x = _normalize_df_for_sqlite(df)
    cols = [str(c) for c in x.columns]
    for k in key_cols:
        if k not in cols:
            raise ValueError(f"missing key column `{k}` for table `{table_name}`")

    q_table = quote_ident(table_name)
    col_defs = []
    for c in cols:
        q_col = quote_ident(c)
        col_type = _infer_col_type(x, c, int_cols=int_cols, text_cols=text_cols)
        null_suffix = " NOT NULL" if c in key_cols else ""
        col_defs.append(f"{q_col} {col_type}{null_suffix}")
    pk = ", ".join([quote_ident(c) for c in key_cols])
    conn.execute(f"CREATE TABLE IF NOT EXISTS {q_table} ({', '.join(col_defs)}, PRIMARY KEY ({pk}))")

    existing = set(table_columns(conn, table_name))
    for c in cols:
        if c in existing:
            continue
        q_col = quote_ident(c)
        col_type = _infer_col_type(x, c, int_cols=int_cols, text_cols=text_cols)
        conn.execute(f"ALTER TABLE {q_table} ADD COLUMN {q_col} {col_type} NULL")

    q_cols = [quote_ident(c) for c in cols]
    placeholders = ", ".join(["?"] * len(cols))
    non_keys = [c for c in cols if c not in key_cols]
    if non_keys:
        updates = ", ".join([f"{quote_ident(c)}=excluded.{quote_ident(c)}" for c in non_keys])
        conflict = ", ".join([quote_ident(c) for c in key_cols])
        sql = (
            f"INSERT INTO {q_table} ({', '.join(q_cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict}) DO UPDATE SET {updates}"
        )
    else:
        conflict = ", ".join([quote_ident(c) for c in key_cols])
        sql = f"INSERT INTO {q_table} ({', '.join(q_cols)}) VALUES ({placeholders}) ON CONFLICT({conflict}) DO NOTHING"

    rows = x[cols].astype(object).where(pd.notna(x[cols]), None).to_numpy().tolist()
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def read_table(
    conn: sqlite3.Connection,
    table_name: str,
    cols: list[str] | None = None,
    start: str = "",
    end: str = "",
    date_col: str = "date",
) -> pd.DataFrame:
    q_table = quote_ident(table_name)
    if cols:
        pick = ", ".join([quote_ident(c) for c in cols])
    else:
        pick = "*"
    q_date = quote_ident(date_col)
    where = ""
    params: list[str] = []
    if str(start).strip() and str(end).strip():
        where = f" WHERE {q_date} BETWEEN ? AND ?"
        params = [str(start), str(end)]
    sql = f"SELECT {pick} FROM {q_table}{where} ORDER BY {q_date}"
    return pd.read_sql_query(sql, conn, params=params)
