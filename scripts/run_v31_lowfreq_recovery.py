from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

import pandas as pd
import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from v30.lowfreq import LowfreqConfig, build_lowfreq_prices, compute_lowfreq_recovery


def _connect():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost")),
        port=int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306"))),
        user=os.getenv("MYSQL_USER", os.getenv("DB_USER", "us_opr")),
        password=os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "sec@Bobo123")),
        database=os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "us_market")),
        charset=os.getenv("MYSQL_CHARSET", "utf8mb4"),
    )


def _read_table(table_name: str, start: str = "", end: str = "") -> pd.DataFrame:
    cond = ""
    params: tuple[str, ...] = ()
    if str(start).strip() and str(end).strip():
        cond = " WHERE `date` BETWEEN %s AND %s"
        params = (str(start), str(end))
    with _connect() as conn:
        df = pd.read_sql(f"SELECT * FROM `{table_name}`{cond} ORDER BY `date`", conn, params=params)
    if df.empty:
        raise ValueError(f"no rows in table: {table_name}")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def _upsert_df(df: pd.DataFrame, table_name: str, key_cols: list[str], int_cols: set[str] | None = None) -> int:
    if df.empty:
        return 0
    int_cols = int_cols or set()
    x = df.copy()
    if "date" in x.columns:
        x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.date
    cols = list(x.columns)
    for k in key_cols:
        if k not in cols:
            raise ValueError(f"missing key column `{k}` for table `{table_name}`")
    with _connect() as conn:
        with conn.cursor() as cur:
            defs: list[str] = []
            for c in cols:
                not_null = " NOT NULL" if c in key_cols else " NULL"
                if c == "date" or c.endswith("_date"):
                    defs.append(f"`{c}` DATE{not_null}")
                elif c in int_cols:
                    defs.append(f"`{c}` INT{not_null}")
                elif pd.api.types.is_numeric_dtype(x[c]):
                    defs.append(f"`{c}` DOUBLE{not_null}")
                else:
                    defs.append(f"`{c}` VARCHAR(128){not_null}")
            pk = ", ".join([f"`{k}`" for k in key_cols])
            cur.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(defs)}, PRIMARY KEY ({pk}))")
            cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
            existing = {str(r[0]) for r in cur.fetchall()}
            for c in cols:
                if c in existing:
                    continue
                if c == "date" or c.endswith("_date"):
                    cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{c}` DATE NULL")
                elif c in int_cols:
                    cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{c}` INT NULL")
                elif pd.api.types.is_numeric_dtype(x[c]):
                    cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{c}` DOUBLE NULL")
                else:
                    cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{c}` VARCHAR(128) NULL")
            col_list = ", ".join([f"`{c}`" for c in cols])
            placeholders = ", ".join(["%s"] * len(cols))
            updates = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols if c not in key_cols])
            sql = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
            rows = x[cols].astype(object).where(pd.notna(x[cols]), None).to_numpy().tolist()
            cur.executemany(sql, rows)
        conn.commit()
    return len(rows)


def _ensure_events_table(table_name: str) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
                "`date` DATE NOT NULL,"
                "`freq` VARCHAR(16) NOT NULL,"
                "`event_idx` INT NOT NULL,"
                "PRIMARY KEY (`date`,`freq`,`event_idx`)"
                ")"
            )
        conn.commit()


def main() -> None:
    p = argparse.ArgumentParser(description="V31 low-frequency recovery layer.")
    p.add_argument("--daily-csv", default="output/v31_backtest_eval_default_prod/v30_backtest_daily.csv")
    p.add_argument("--daily-table", default="")
    p.add_argument("--start-date", default="")
    p.add_argument("--end-date", default="")
    p.add_argument("--date-col", default="date")
    p.add_argument("--price-col", default="")
    p.add_argument("--ret-col", default="spy_ret1d")
    p.add_argument("--flow-csv", default="")
    p.add_argument("--flow-date-col", default="date")
    p.add_argument("--flow-col", default="")
    p.add_argument("--freq", choices=["weekly", "monthly"], default="weekly")
    p.add_argument("--drawdown-threshold", type=float, default=-1.0)
    p.add_argument("--max-horizon", type=int, default=-1)
    p.add_argument("--output-dir", default="output/v31_lowfreq_recovery")
    p.add_argument("--summary-table", default="v31_lowfreq_recovery_summary")
    p.add_argument("--events-table", default="v31_lowfreq_recovery_events")
    p.add_argument("--skip-db-upsert", action="store_true")
    p.add_argument("--write-csv", action="store_true", help="Write events.csv (off by default).")
    args = p.parse_args()

    if str(args.daily_table).strip():
        daily = _read_table(str(args.daily_table).strip(), start=str(args.start_date), end=str(args.end_date))
    else:
        daily_path = Path(args.daily_csv)
        if not daily_path.exists():
            raise FileNotFoundError(f"missing input: {daily_path}")
        daily = pd.read_csv(daily_path, parse_dates=[args.date_col])

    if args.freq == "weekly":
        freq = "W-FRI"
        dd_thr = 0.12 if args.drawdown_threshold < 0 else float(args.drawdown_threshold)
        horizon = 52 if args.max_horizon <= 0 else int(args.max_horizon)
    else:
        freq = "ME"
        dd_thr = 0.18 if args.drawdown_threshold < 0 else float(args.drawdown_threshold)
        horizon = 24 if args.max_horizon <= 0 else int(args.max_horizon)

    lowf = build_lowfreq_prices(
        daily,
        date_col=args.date_col,
        price_col=args.price_col,
        ret_col=args.ret_col,
        freq=freq,
    )

    flow_series = None
    if args.flow_csv and args.flow_col:
        fp = Path(args.flow_csv)
        if fp.exists():
            fx = pd.read_csv(fp, parse_dates=[args.flow_date_col])
            fx[args.flow_date_col] = pd.to_datetime(fx[args.flow_date_col])
            f = fx[[args.flow_date_col, args.flow_col]].dropna().copy()
            f = f.set_index(args.flow_date_col).resample(freq).sum()
            flow_series = pd.to_numeric(f[args.flow_col], errors="coerce")

    cfg = LowfreqConfig(
        freq=freq,
        drawdown_threshold=dd_thr,
        max_horizon=horizon,
    )
    out = compute_lowfreq_recovery(lowf, cfg, flow_series=flow_series)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    end_date = out["window"]["end"]
    trade_date = str(args.end_date).strip() or end_date
    date_tag = str(end_date).replace("-", "")

    (out_dir / "summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    (out_dir / f"summary_{date_tag}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    ev = pd.DataFrame(out.get("events", []))
    if bool(args.write_csv) and (not ev.empty):
        ev.to_csv(out_dir / "events.csv", index=False, encoding="utf-8-sig")

    upsert_summary_rows = 0
    upsert_event_rows = 0
    if not bool(args.skip_db_upsert):
        ls = out.get("latest_state", {})
        s_row = pd.DataFrame(
            [
                {
                    "date": trade_date,
                    "freq": str(args.freq),
                    "window_end_date": end_date,
                    "in_major_drawdown": int(bool(ls.get("in_major_drawdown", False))),
                    "rss": float(ls.get("rss", 0.0) or 0.0),
                    "rsts": float(ls.get("rsts", 0.0) or 0.0),
                    "persistence_adjustment": float(ls.get("persistence_adjustment", 0.0) or 0.0),
                    "gate": str(ls.get("gate", "NA")),
                    "action_hint": str(ls.get("action_hint", "NA")),
                    "short_term_confirmation": str(ls.get("short_term_confirmation", "NA")),
                    "event_count": int(len(ev)),
                }
            ]
        )
        upsert_summary_rows = _upsert_df(
            s_row,
            table_name=str(args.summary_table),
            key_cols=["date", "freq"],
            int_cols={"in_major_drawdown", "event_count"},
        )
        if not ev.empty:
            ev_out = ev.copy().reset_index(drop=True)
            ev_out.insert(0, "date", pd.to_datetime(trade_date))
            ev_out.insert(1, "freq", str(args.freq))
            ev_out.insert(2, "event_idx", ev_out.index.astype(int) + 1)
            int_cols = {"event_idx"}
            if "days" in ev_out.columns:
                int_cols.add("days")
            if "recovery_weeks" in ev_out.columns:
                int_cols.add("recovery_weeks")
            if "recovery_months" in ev_out.columns:
                int_cols.add("recovery_months")
            upsert_event_rows = _upsert_df(
                ev_out,
                table_name=str(args.events_table),
                key_cols=["date", "freq", "event_idx"],
                int_cols=int_cols,
            )
        else:
            _ensure_events_table(str(args.events_table))

    ls = out["latest_state"]
    lines = [
        "# 低频恢复层日报",
        "",
        f"- 频率: {args.freq}",
        f"- 区间: {out['window']['start']} -> {out['window']['end']}",
        f"- 当前大级别回撤中: {ls['in_major_drawdown']}",
        f"- Recovery Speed Score (RSS): {ls['rss']:.4f}",
        f"- Recovery Strength Score (RStS): {ls['rsts']:.4f}",
        f"- Persistence Adjustment: {ls['persistence_adjustment']:.4f}",
        f"- Gate: {ls['gate']}",
        f"- ActionHint: {ls['action_hint']}",
        f"- 短期战术确认: {ls['short_term_confirmation']}",
    ]
    (out_dir / "report.md").write_text("\n".join(lines), encoding="utf-8-sig")
    (out_dir / f"report_{end_date}.md").write_text("\n".join(lines), encoding="utf-8-sig")

    print(f"[OK] Wrote: {out_dir / 'summary.json'}")
    print(f"[OK] Wrote: {out_dir / 'report.md'}")
    if bool(args.write_csv) and (out_dir / "events.csv").exists():
        print(f"[OK] Wrote: {out_dir / 'events.csv'}")
    if not bool(args.skip_db_upsert):
        print(f"[OK] DB upsert rows: {upsert_summary_rows} -> table `{args.summary_table}`")
        print(f"[OK] DB upsert rows: {upsert_event_rows} -> table `{args.events_table}`")


if __name__ == "__main__":
    main()
