from __future__ import annotations

import io
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from .backtest_sqlite import quote_ident


def ensure_model_store_table(conn: sqlite3.Connection, table_name: str = "v30_model_store") -> None:
    q = quote_ident(table_name)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {q} (
            model_key TEXT NOT NULL PRIMARY KEY,
            model_blob BLOB NOT NULL,
            metadata_json TEXT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def upsert_model_bundle(
    conn: sqlite3.Connection,
    model_key: str,
    bundle: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    table_name: str = "v30_model_store",
) -> int:
    ensure_model_store_table(conn, table_name=table_name)
    bio = io.BytesIO()
    joblib.dump(bundle, bio)
    blob = bio.getvalue()
    meta = json.dumps(metadata or {}, ensure_ascii=False)
    now = datetime.now().isoformat(timespec="seconds")
    q = quote_ident(table_name)
    conn.execute(
        f"""
        INSERT INTO {q} (model_key, model_blob, metadata_json, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(model_key) DO UPDATE SET
            model_blob=excluded.model_blob,
            metadata_json=excluded.metadata_json,
            updated_at=excluded.updated_at
        """,
        (str(model_key), sqlite3.Binary(blob), meta, now),
    )
    conn.commit()
    return 1


def load_model_bundle(
    conn: sqlite3.Connection,
    model_key: str,
    table_name: str = "v30_model_store",
) -> dict[str, Any]:
    ensure_model_store_table(conn, table_name=table_name)
    q = quote_ident(table_name)
    row = conn.execute(f"SELECT model_blob FROM {q} WHERE model_key=?", (str(model_key),)).fetchone()
    if row is None or row[0] is None:
        raise FileNotFoundError(f"model key not found in sqlite: {model_key}")
    bio = io.BytesIO(bytes(row[0]))
    obj = joblib.load(bio)
    if not isinstance(obj, dict):
        raise ValueError(f"invalid model bundle in sqlite for key: {model_key}")
    return obj


def ensure_file_store_table(conn: sqlite3.Connection, table_name: str = "v30_artifact_file_store") -> None:
    q = quote_ident(table_name)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {q} (
            file_path TEXT NOT NULL PRIMARY KEY,
            file_name TEXT NOT NULL,
            ext TEXT NULL,
            content_text TEXT NULL,
            content_blob BLOB NULL,
            size_bytes INTEGER NOT NULL,
            mtime TEXT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def upsert_artifact_file(
    conn: sqlite3.Connection,
    file_path: Path,
    root_dir: Path,
    table_name: str = "v30_artifact_file_store",
) -> int:
    ensure_file_store_table(conn, table_name=table_name)
    p = file_path.resolve()
    rel = str(p.relative_to(root_dir.resolve())).replace("\\", "/")
    ext = p.suffix.lower()
    raw = p.read_bytes()
    text_exts = {".json", ".md", ".txt", ".csv", ".yaml", ".yml"}
    content_text = None
    content_blob = None
    if ext in text_exts:
        content_text = raw.decode("utf-8", errors="replace")
    else:
        content_blob = sqlite3.Binary(raw)
    stat = p.stat()
    now = datetime.now().isoformat(timespec="seconds")
    q = quote_ident(table_name)
    conn.execute(
        f"""
        INSERT INTO {q} (
            file_path, file_name, ext, content_text, content_blob, size_bytes, mtime, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_path) DO UPDATE SET
            file_name=excluded.file_name,
            ext=excluded.ext,
            content_text=excluded.content_text,
            content_blob=excluded.content_blob,
            size_bytes=excluded.size_bytes,
            mtime=excluded.mtime,
            updated_at=excluded.updated_at
        """,
        (
            rel,
            p.name,
            ext,
            content_text,
            content_blob,
            int(stat.st_size),
            datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            now,
        ),
    )
    conn.commit()
    return 1


def read_file_store_index(
    conn: sqlite3.Connection, table_name: str = "v30_artifact_file_store"
) -> pd.DataFrame:
    ensure_file_store_table(conn, table_name=table_name)
    q = quote_ident(table_name)
    return pd.read_sql_query(
        f"SELECT file_path, file_name, ext, size_bytes, mtime, updated_at FROM {q} ORDER BY file_path",
        conn,
    )
