from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

import pandas as pd
import pymysql
from pandas.tseries.offsets import BDay


def _rolling_zscore(s: pd.Series, window: int = 30) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce").astype(float)
    mu = s.rolling(window, min_periods=max(10, window // 3)).mean()
    sd = s.rolling(window, min_periods=max(10, window // 3)).std().replace(0.0, pd.NA)
    return ((s - mu) / sd).fillna(0.0)


def _json_safe_value(v):
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if pd.isna(v):
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    return v


def _resolve_db_env(name: str, fallback: str) -> str:
    # Keep compatibility with both MYSQL_* and DB_* style env vars.
    return os.getenv(name, os.getenv(name.replace("MYSQL_", "DB_"), fallback))


def _connect():
    return pymysql.connect(
        host=_resolve_db_env("MYSQL_HOST", "localhost"),
        port=int(_resolve_db_env("MYSQL_PORT", "3306")),
        user=_resolve_db_env("MYSQL_USER", "us_opr"),
        password=_resolve_db_env("MYSQL_PASSWORD", "sec@Bobo123"),
        database=_resolve_db_env("MYSQL_DATABASE", "us_market"),
        charset=_resolve_db_env("MYSQL_CHARSET", "utf8mb4"),
    )


def _query_series(conn, sql: str, params: tuple, out_col: str) -> tuple[pd.Series, dict]:
    try:
        df = pd.read_sql(sql, conn, params=params)
        if df.empty:
            return pd.Series(dtype=float, name=out_col), {"latest": None, "error": "no_rows"}
        dcol = "date"
        vcol = [c for c in df.columns if c != dcol][0]
        d = pd.to_datetime(df[dcol], errors="coerce")
        v = pd.to_numeric(df[vcol], errors="coerce")
        s = pd.Series(v.values, index=d).dropna().sort_index()
        s.name = out_col
        return s, {"latest": None if s.empty else str(s.index.max().date()), "rows": int(len(s))}
    except Exception as e:
        return pd.Series(dtype=float, name=out_col), {"latest": None, "error": f"query_failed:{e}"}


def _enrich_freshness(meta: dict, end: date) -> dict:
    out = dict(meta)
    latest = out.get("latest")
    if latest:
        latest_d = pd.to_datetime(latest).date()
        stale_bdays = max(0, len(pd.bdate_range(start=latest_d, end=end)) - 1)
        out["stale_bdays"] = int(stale_bdays)
        out["is_stale"] = bool(stale_bdays > 0)
    else:
        out["stale_bdays"] = None
        out["is_stale"] = True
    return out


def _load_inputs(start: date, end: date, lookback_bdays: int) -> tuple[pd.DataFrame, dict]:
    idx = pd.bdate_range(start=start, end=end)
    out = pd.DataFrame(index=idx)
    freshness: dict[str, dict] = {}
    query_start = (pd.Timestamp(start) - BDay(max(1, int(lookback_bdays)))).date()
    params = (query_start.isoformat(), end.isoformat())

    with _connect() as conn:
        vix, fr = _query_series(
            conn,
            "SELECT `date`, COALESCE(`close`,`adj_close`) AS `vix` FROM `vix` WHERE `date` BETWEEN %s AND %s ORDER BY `date`",
            params,
            "vix",
        )
        out["vix"] = vix.reindex(out.index, method="ffill").bfill()
        freshness["vix"] = _enrich_freshness(fr, end=end)

        hy, fr = _query_series(
            conn,
            "SELECT `date`, `value` AS `hy_oas` FROM `hy_oas` WHERE `date` BETWEEN %s AND %s ORDER BY `date`",
            params,
            "hy_oas",
        )
        out["hy_oas"] = hy.reindex(out.index, method="ffill").bfill()
        freshness["hy_oas"] = _enrich_freshness(fr, end=end)

        fg, fr = _query_series(
            conn,
            "SELECT `date`, `index_value` AS `fear_greed` FROM `fear_greed` WHERE `date` BETWEEN %s AND %s ORDER BY `date`",
            params,
            "fear_greed",
        )
        out["fear_greed"] = fg.reindex(out.index, method="ffill").bfill()
        freshness["fear_greed"] = _enrich_freshness(fr, end=end)

        aaii, fr = _query_series(
            conn,
            "SELECT `date`, `bull_bear_spread` AS `aaii_spread` FROM `aaii_sentiment_daily` WHERE `date` BETWEEN %s AND %s ORDER BY `date`",
            params,
            "aaii_spread",
        )
        out["aaii_spread"] = aaii.reindex(out.index, method="ffill").bfill()
        freshness["aaii_spread"] = _enrich_freshness(fr, end=end)

        wiki, fr = _query_series(
            conn,
            "SELECT `date`, `views_total` AS `wiki_views_total` FROM `wikipedia_pageviews_risk` WHERE `date` BETWEEN %s AND %s ORDER BY `date`",
            params,
            "wiki_views_total",
        )
        out["wiki_views_total"] = wiki.reindex(out.index, method="ffill").bfill()
        freshness["wiki_views_total"] = _enrich_freshness(fr, end=end)

        gdelt, fr = _query_series(
            conn,
            "SELECT `date`, COALESCE(`gdelt_norm`,`gdelt_count`) AS `gdelt_risk` FROM `gdelt_risk` WHERE `date` BETWEEN %s AND %s ORDER BY `date`",
            params,
            "gdelt_risk",
        )
        out["gdelt_risk"] = gdelt.reindex(out.index, method="ffill").bfill()
        freshness["gdelt_risk"] = _enrich_freshness(fr, end=end)

    return out, freshness


def _compute_signals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["z_vix"] = _rolling_zscore(out["vix"])
    out["z_hy_oas"] = _rolling_zscore(out["hy_oas"])
    out["z_fear_greed_inv"] = -_rolling_zscore(out["fear_greed"])
    out["z_aaii_bear"] = -_rolling_zscore(out["aaii_spread"])
    out["z_wiki_views"] = _rolling_zscore(out["wiki_views_total"])
    out["z_gdelt_risk"] = _rolling_zscore(out["gdelt_risk"])

    out["srs_v2"] = (
        0.30 * out["z_vix"]
        + 0.30 * out["z_hy_oas"]
        + 0.20 * out["z_fear_greed_inv"]
        + 0.20 * out["z_aaii_bear"]
    )
    out["srs_accel_v2"] = out["srs_v2"] - out["srs_v2"].rolling(5, min_periods=2).mean()
    out["sentiment_signal_v2"] = ((out["srs_v2"] > 0.40) & (out["srs_accel_v2"] > 0.20)).astype(int)

    # Extended sentiment score: add wiki + gdelt attention/risk layers.
    out["srs_plus"] = out["srs_v2"] + 0.05 * out["z_wiki_views"] + 0.05 * out["z_gdelt_risk"]
    out["srs_accel_plus"] = out["srs_plus"] - out["srs_plus"].rolling(5, min_periods=2).mean()
    out["sentiment_signal_plus"] = ((out["srs_plus"] > 0.45) & (out["srs_accel_plus"] > 0.20)).astype(int)
    return out


def _upsert_unified_to_db(df: pd.DataFrame, table_name: str) -> int:
    if df.empty:
        return 0
    x = df.copy()
    x["date"] = pd.to_datetime(x["date"]).dt.date
    cols = [c for c in x.columns if c != "date"]

    col_defs = ["`date` DATE NOT NULL PRIMARY KEY"]
    col_defs.extend([f"`{c}` DOUBLE NULL" for c in cols])
    col_list = ", ".join(["`date`"] + [f"`{c}`" for c in cols])
    placeholders = ", ".join(["%s"] * (len(cols) + 1))
    updates = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols])

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(col_defs)})")
            sql = (
                f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {updates}"
            )
            rows = x[["date"] + cols].astype(object).where(pd.notna(x[["date"] + cols]), None).to_numpy().tolist()
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Build unified sentiment layer from DB and upsert back to DB.")
    p.add_argument("--start", default="2016-01-01")
    p.add_argument("--end", default=str(date.today()))
    p.add_argument("--output-dir", default="output/v31_unified_sentiment")
    p.add_argument("--table-name", default="sentiment_daily_unified")
    p.add_argument("--lookback-bdays", type=int, default=40, help="Business-day lookback for sparse sources.")
    p.add_argument("--skip-db-upsert", action="store_true")
    p.add_argument("--write-csv", action="store_true", help="Write sentiment_daily_unified.csv (off by default).")
    args = p.parse_args()

    start = pd.to_datetime(args.start).date()
    end = pd.to_datetime(args.end).date()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw, freshness = _load_inputs(start, end, lookback_bdays=int(args.lookback_bdays))
    feat = _compute_signals(raw).reset_index(names=["date"])
    if bool(args.write_csv):
        feat.to_csv(out_dir / "sentiment_daily_unified.csv", index=False)

    upsert_rows = 0
    if not args.skip_db_upsert:
        upsert_rows = _upsert_unified_to_db(feat, table_name=str(args.table_name))

    latest_raw = feat.iloc[-1].to_dict() if not feat.empty else {}
    latest = {str(k): _json_safe_value(v) for k, v in latest_raw.items()}
    summary = {
        "window": {"start": str(start), "end": str(end)},
        "rows": int(len(feat)),
        "latest_date": None if feat.empty else str(pd.to_datetime(feat["date"]).max().date()),
        "latest_state": latest,
        "source_freshness": freshness,
        "table_name": str(args.table_name),
        "upsert_rows": int(upsert_rows),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "latest_unified_sentiment.json").write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    if bool(args.write_csv):
        print(f"[OK] Wrote: {out_dir / 'sentiment_daily_unified.csv'}")
    print(f"[OK] Wrote: {out_dir / 'summary.json'}")
    print(f"[OK] Wrote: {out_dir / 'latest_unified_sentiment.json'}")
    print(f"[OK] DB upsert rows: {upsert_rows}")


if __name__ == "__main__":
    main()
