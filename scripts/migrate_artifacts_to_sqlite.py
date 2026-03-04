from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.data_layer import connect_backtest_db, read_file_store_index, upsert_artifact_file


def main() -> None:
    p = argparse.ArgumentParser(description="Migrate artifacts files into SQLite store.")
    p.add_argument("--artifacts-dir", default="artifacts")
    p.add_argument("--db-path", default="data/backtest/v30_backtest.sqlite")
    p.add_argument("--table-name", default="v30_artifact_file_store")
    p.add_argument("--glob", default="**/*")
    p.add_argument("--delete-after", action="store_true")
    p.add_argument("--include-binary", action="store_true", help="Include non-text files such as .pkl/.png.")
    args = p.parse_args()

    root = Path(str(args.artifacts_dir)).resolve()
    if not root.exists():
        raise FileNotFoundError(f"artifacts dir not found: {root}")

    text_exts = {".json", ".md", ".txt", ".csv", ".yaml", ".yml"}
    files = [p for p in root.glob(str(args.glob)) if p.is_file()]
    files = sorted(files)
    migrated = 0
    deleted = 0

    with connect_backtest_db(str(args.db_path).strip() or None) as conn:
        for f in files:
            if (not bool(args.include_binary)) and (f.suffix.lower() not in text_exts):
                continue
            migrated += upsert_artifact_file(
                conn,
                file_path=f,
                root_dir=root,
                table_name=str(args.table_name),
            )
            if bool(args.delete_after):
                f.unlink(missing_ok=True)
                deleted += 1
        idx = read_file_store_index(conn, table_name=str(args.table_name))

    print(f"[OK] migrated files: {migrated}")
    print(f"[OK] deleted files: {deleted}")
    print(f"[OK] sqlite rows in `{args.table_name}`: {len(idx)}")
    print(f"[OK] artifacts root: {root}")


if __name__ == "__main__":
    main()

