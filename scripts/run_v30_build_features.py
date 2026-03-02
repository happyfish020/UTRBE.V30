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

from v30.feature_engineering.build_features import (
    FeatureBuildConfig,
    attach_breadth,
    build_features,
    load_breadth_data,
    load_market_data,
    write_metadata,
)


def _load_market_data_from_db(table_name: str, start: str = "", end: str = "") -> pd.DataFrame:
    host = os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost"))
    port = int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306")))
    user = os.getenv("MYSQL_USER", os.getenv("DB_USER", "us_opr"))
    password = os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "sec@Bobo123"))
    database = os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "us_market"))
    charset = os.getenv("MYSQL_CHARSET", "utf8mb4")

    cond = ""
    params: list[str] = []
    if str(start).strip() and str(end).strip():
        cond = " WHERE `date` BETWEEN %s AND %s"
        params = [str(start), str(end)]
    sql = (
        f"SELECT `date`, `vol20`, `vol60`, `hurst_100`, `drawdown_252`, `drawdown_126`, `ret5d`, `ret20d`, "
        f"`ibb_ret20d`, `ibb_drawdown_126`, `ibb_rel20d`, `external_event`, `is_trading_day` "
        f"FROM `{table_name}`{cond} ORDER BY `date`"
    )
    with pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
    ) as conn:
        df = pd.read_sql(sql, conn, params=tuple(params))
    if df.empty:
        raise ValueError(f"No rows loaded from table `{table_name}`.")
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    if "is_trading_day" in df.columns:
        df_trading = df[df["is_trading_day"].fillna(1).astype(int) == 1].copy()
        # Guard against stale upstream flags for otherwise valid trading dates.
        if not df_trading.empty:
            df = df_trading
    for c in [
        "vol20",
        "vol60",
        "hurst_100",
        "drawdown_252",
        "drawdown_126",
        "ret5d",
        "ret20d",
        "ibb_ret20d",
        "ibb_drawdown_126",
        "ibb_rel20d",
        "external_event",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


def _upsert_features_to_db(df: pd.DataFrame, table_name: str) -> int:
    if df.empty:
        return 0
    x = df.copy()
    x["date"] = pd.to_datetime(x["date"]).dt.date
    cols = [c for c in x.columns if c != "date"]

    host = os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost"))
    port = int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306")))
    user = os.getenv("MYSQL_USER", os.getenv("DB_USER", "us_opr"))
    password = os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "sec@Bobo123"))
    database = os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "us_market"))
    charset = os.getenv("MYSQL_CHARSET", "utf8mb4")

    col_defs = ["`date` DATE NOT NULL PRIMARY KEY"]
    col_defs.extend([f"`{c}` DOUBLE NULL" for c in cols])
    col_list = ", ".join(["`date`"] + [f"`{c}`" for c in cols])
    placeholders = ", ".join(["%s"] * (len(cols) + 1))
    updates = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols])

    with pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(col_defs)})")
            cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
            existing_cols = {str(r[0]) for r in cur.fetchall()}
            for c in cols:
                if c not in existing_cols:
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
    parser = argparse.ArgumentParser(description="V30 Phase-A feature build runner.")
    parser.add_argument(
        "--input-csv",
        default="../UTRBE/output/dualtrack_daily_pipeline_with_compare_2016/market_features.csv",
        help="Input market feature csv.",
    )
    parser.add_argument(
        "--input-table",
        default="",
        help="Input market feature DB table. If set, takes precedence over --input-csv.",
    )
    parser.add_argument("--input-start", default="")
    parser.add_argument("--input-end", default="")
    parser.add_argument("--output-csv", default="output/v30_features_daily.csv")
    parser.add_argument("--meta-json", default="output/v30_features_build_meta.json")
    parser.add_argument("--table-name", default="v30_features_daily")
    parser.add_argument("--skip-db-upsert", action="store_true")
    parser.add_argument("--skip-csv-output", action="store_true")
    parser.add_argument(
        "--breadth-csv",
        default="../UTRBE/output/full_2006_2026.csv",
        help="External breadth source csv (set empty string to disable).",
    )
    parser.add_argument("--breadth-date-col", default="date")
    parser.add_argument("--breadth-value-col", default="breadth")
    parser.add_argument("--breadth-price-col", default="price")
    parser.add_argument("--breadth-credit-col", default="credit")
    parser.add_argument("--low-vol-cut", type=float, default=0.010)
    parser.add_argument("--high-hurst-cut", type=float, default=0.60)
    args = parser.parse_args()

    input_csv = Path(args.input_csv) if str(args.input_csv).strip() else None
    output_csv = Path(args.output_csv)
    meta_json = Path(args.meta_json)
    breadth_csv = Path(args.breadth_csv) if str(args.breadth_csv).strip() else None

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    meta_json.parent.mkdir(parents=True, exist_ok=True)

    if str(args.input_table).strip():
        raw = _load_market_data_from_db(
            table_name=str(args.input_table).strip(),
            start=str(args.input_start).strip(),
            end=str(args.input_end).strip(),
        )
    else:
        if input_csv is None or not input_csv.exists():
            raise FileNotFoundError(f"Input csv not found: {input_csv}")
        raw = load_market_data(input_csv)
    breadth_df = None
    if breadth_csv is not None:
        if not breadth_csv.exists():
            raise FileNotFoundError(f"Breadth csv not found: {breadth_csv}")
        breadth_df = load_breadth_data(
            breadth_csv,
            date_col=str(args.breadth_date_col),
            value_col=str(args.breadth_value_col),
            price_col=str(args.breadth_price_col),
            credit_col=str(args.breadth_credit_col),
        )
    raw = attach_breadth(raw, breadth_df)
    feat = build_features(
        raw,
        FeatureBuildConfig(low_vol_cut=float(args.low_vol_cut), high_hurst_cut=float(args.high_hurst_cut)),
    )
    if not bool(args.skip_csv_output):
        feat.to_csv(output_csv, index=False)
    upsert_rows = 0
    if not bool(args.skip_db_upsert):
        upsert_rows = _upsert_features_to_db(feat, table_name=str(args.table_name))
    breadth_cov = float(pd.to_numeric(feat.get("breadth_real_stress"), errors="coerce").notna().mean()) if "breadth_real_stress" in feat.columns else 0.0
    write_metadata(
        meta_json,
        input_csv=input_csv or Path("__db_market_features__"),
        output_csv=output_csv,
        rows=len(feat),
        extras={
            "input_table": str(args.input_table).strip(),
            "input_start": str(args.input_start).strip(),
            "input_end": str(args.input_end).strip(),
            "output_table": str(args.table_name),
            "upsert_rows": int(upsert_rows),
            "breadth_csv": str(breadth_csv) if breadth_csv is not None else "",
            "breadth_date_col": str(args.breadth_date_col),
            "breadth_value_col": str(args.breadth_value_col),
            "breadth_price_col": str(args.breadth_price_col),
            "breadth_credit_col": str(args.breadth_credit_col),
            "breadth_coverage": breadth_cov,
        },
    )

    if not bool(args.skip_csv_output):
        print(f"[OK] Wrote: {output_csv}")
    print(f"[OK] Wrote: {meta_json}")
    if not bool(args.skip_db_upsert):
        print(f"[OK] DB upsert rows: {upsert_rows} -> table `{args.table_name}`")


if __name__ == "__main__":
    main()

