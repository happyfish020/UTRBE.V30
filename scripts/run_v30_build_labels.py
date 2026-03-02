from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import pandas as pd
import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.shock_engine.labeling import add_shock_label_proxy
from v30.structural_engine.labeling import add_structural_label


def _db_connect():
    host = os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost"))
    port = int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306")))
    user = os.getenv("MYSQL_USER", os.getenv("DB_USER", "us_opr"))
    password = os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "sec@Bobo123"))
    database = os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "us_market"))
    charset = os.getenv("MYSQL_CHARSET", "utf8mb4")
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
    )


def _upsert(df: pd.DataFrame, table_name: str, int_cols: set[str]) -> int:
    if df.empty:
        return 0
    x = df.copy()
    x["date"] = pd.to_datetime(x["date"]).dt.date
    cols = [c for c in x.columns if c != "date"]
    col_defs = ["`date` DATE NOT NULL PRIMARY KEY"]
    for c in cols:
        col_defs.append(f"`{c}` INT NULL" if c in int_cols else f"`{c}` DOUBLE NULL")
    col_list = ", ".join(["`date`"] + [f"`{c}`" for c in cols])
    placeholders = ", ".join(["%s"] * (len(cols) + 1))
    updates = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols])

    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(col_defs)})")
            cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
            existing = {str(r[0]) for r in cur.fetchall()}
            for c in cols:
                if c in existing:
                    continue
                if c in int_cols:
                    cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{c}` INT NULL")
                else:
                    cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{c}` DOUBLE NULL")
            sql = (
                f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {updates}"
            )
            rows = x[["date"] + cols].astype(object).where(pd.notna(x[["date"] + cols]), None).to_numpy().tolist()
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Build V30 structural/shock labels and upsert to DB.")
    p.add_argument("--features-csv", default="output/v30_features_daily.csv")
    p.add_argument("--features-table", default="")
    p.add_argument("--struct-table", default="v30_structural_labels_daily")
    p.add_argument("--shock-table", default="v30_shock_labels_daily")
    p.add_argument("--skip-db-upsert", action="store_true")
    p.add_argument("--skip-csv-output", action="store_true")
    p.add_argument("--output-dir", default="output/v30_labels")

    p.add_argument("--struct-horizon-days", type=int, default=30)
    p.add_argument("--struct-dd-threshold", type=float, default=0.10)
    p.add_argument("--struct-persistence-days", type=int, default=15)

    p.add_argument("--shock-horizon-days", type=int, default=7)
    p.add_argument("--shock-drop-threshold", type=float, default=0.07)
    p.add_argument("--shock-early-share-threshold", type=float, default=0.45)
    p.add_argument("--shock-adaptive-drop", action="store_true")
    p.add_argument("--shock-target-positive-rate", type=float, default=0.10)
    p.add_argument("--shock-min-drop-threshold", type=float, default=0.010)
    p.add_argument("--shock-max-drop-threshold", type=float, default=0.10)
    p.add_argument("--shock-use-stress-override", action="store_true")
    p.add_argument("--shock-stress-gate", type=float, default=0.40)
    args = p.parse_args()

    in_csv = Path(args.features_csv)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if str(args.features_table).strip():
        with _db_connect() as conn:
            df = pd.read_sql(f"SELECT * FROM `{str(args.features_table).strip()}` ORDER BY `date`", conn)
        if df.empty:
            raise ValueError(f"no rows in features table: {args.features_table}")
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
    else:
        if not in_csv.exists():
            raise FileNotFoundError(f"features csv not found: {in_csv}")
        df = pd.read_csv(in_csv, parse_dates=["date"]).sort_values("date").reset_index(drop=True)

    struct_df = add_structural_label(
        df,
        horizon_days=int(args.struct_horizon_days),
        drawdown_threshold=float(args.struct_dd_threshold),
        persistence_days=int(args.struct_persistence_days),
    )
    struct_out = struct_df[["date", "label_structural"]].copy()
    struct_out["label_structural"] = pd.to_numeric(struct_out["label_structural"], errors="coerce")
    struct_out = struct_out[struct_out["label_structural"].notna()].copy()
    struct_out["label_structural"] = struct_out["label_structural"].astype(int)
    if not bool(args.skip_csv_output):
        struct_out.sort_values("date", ascending=False).to_csv(out_dir / "structural_labels.csv", index=False)

    shock_df = add_shock_label_proxy(
        df,
        horizon_days=int(args.shock_horizon_days),
        drop_threshold=float(args.shock_drop_threshold),
        early_share_threshold=float(args.shock_early_share_threshold),
        adaptive_drop_threshold=bool(args.shock_adaptive_drop),
        target_positive_rate=float(args.shock_target_positive_rate),
        min_drop_threshold=float(args.shock_min_drop_threshold),
        max_drop_threshold=float(args.shock_max_drop_threshold),
        use_stress_override=bool(args.shock_use_stress_override),
        stress_gate=float(args.shock_stress_gate),
    )
    shock_cols = ["date", "label_shock", "label_shock_drop_threshold_used"]
    shock_out = shock_df[shock_cols].copy()
    shock_out["label_shock"] = pd.to_numeric(shock_out["label_shock"], errors="coerce")
    shock_out = shock_out[shock_out["label_shock"].notna()].copy()
    shock_out["label_shock"] = shock_out["label_shock"].astype(int)
    if not bool(args.skip_csv_output):
        shock_out.sort_values("date", ascending=False).to_csv(out_dir / "shock_labels.csv", index=False)

    s_rows = 0
    k_rows = 0
    if not bool(args.skip_db_upsert):
        s_rows = _upsert(struct_out, table_name=str(args.struct_table), int_cols={"label_structural"})
        k_rows = _upsert(
            shock_out,
            table_name=str(args.shock_table),
            int_cols={"label_shock"},
        )

    if not bool(args.skip_csv_output):
        print(f"[OK] Wrote: {out_dir / 'structural_labels.csv'}")
        print(f"[OK] Wrote: {out_dir / 'shock_labels.csv'}")
    if not bool(args.skip_db_upsert):
        print(f"[OK] DB upsert rows: {s_rows} -> table `{args.struct_table}`")
        print(f"[OK] DB upsert rows: {k_rows} -> table `{args.shock_table}`")


if __name__ == "__main__":
    main()
