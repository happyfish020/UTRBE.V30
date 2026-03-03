from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def _setup_cn_font() -> None:
    plt.rcParams["font.sans-serif"] = [
        "Microsoft YaHei",
        "SimHei",
        "Noto Sans CJK SC",
        "Arial Unicode MS",
        "DejaVu Sans",
    ]
    plt.rcParams["axes.unicode_minus"] = False


def _load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"missing file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_json_optional(path_str: str) -> dict:
    if not path_str:
        return {}
    p = Path(path_str)
    if not p.exists():
        return {}
    txt = p.read_text(encoding="utf-8-sig")
    return json.loads(txt)


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


def _read_table(engine: Engine, table_name: str, start: str = "", end: str = "") -> pd.DataFrame:
    cond = ""
    params: dict[str, str] = {}
    if str(start).strip() and str(end).strip():
        cond = " WHERE `date` BETWEEN :start AND :end"
        params = {"start": str(start), "end": str(end)}
    with engine.connect() as conn:
        df = pd.read_sql_query(text(f"SELECT * FROM {_quote(table_name)}{cond} ORDER BY `date`"), conn, params=params)
    if df.empty:
        raise ValueError(f"no rows in table: {table_name}")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def _list_intraday_files(intraday_dir: Path) -> list[Path]:
    if not intraday_dir.exists():
        return []
    exts = {".csv", ".parquet", ".pq"}
    return sorted([p for p in intraday_dir.rglob("*") if p.is_file() and p.suffix.lower() in exts], key=lambda x: x.stat().st_mtime, reverse=True)


def _detect_col(cols: list[str], preferred: str, candidates: list[str]) -> str | None:
    lower_map = {str(c).lower(): str(c) for c in cols}
    if str(preferred).strip() and str(preferred).lower() in lower_map:
        return lower_map[str(preferred).lower()]
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def _read_intraday_file(path: Path, ts_col: str, price_col: str) -> tuple[pd.DataFrame, float | None]:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_parquet(path)
    if df.empty:
        return pd.DataFrame(columns=["ts", "px"]), None
    cols = [str(c) for c in df.columns]
    use_ts = _detect_col(cols, ts_col, ["datetime", "timestamp", "ts", "time", "dt", "date"])
    use_px = _detect_col(cols, price_col, ["close", "last", "price", "px", "adj_close"])
    if use_ts is None or use_px is None:
        return pd.DataFrame(columns=["ts", "px"]), None

    x = df.copy()
    x["ts"] = pd.to_datetime(x[use_ts], errors="coerce")
    x["px"] = pd.to_numeric(x[use_px], errors="coerce")
    x = x.dropna(subset=["ts", "px"]).sort_values("ts").reset_index(drop=True)
    if x.empty:
        return x, None

    prev_close = None
    prev_close_col = _detect_col(cols, "", ["prev_close", "preclose", "prevclose", "yclose"])
    if prev_close_col is not None:
        s = pd.to_numeric(df[prev_close_col], errors="coerce").dropna()
        if not s.empty:
            prev_close = float(s.iloc[-1])
    return x, prev_close


def _read_prev_close(
    engine: Engine,
    table_name: str,
    trade_date: str,
    date_col: str = "date",
    close_col: str = "close",
) -> float | None:
    q_table = _quote(table_name)
    q_date = _quote(date_col)
    q_close = _quote(close_col)
    sql = (
        f"SELECT {q_close} AS c "
        f"FROM {q_table} "
        f"WHERE {q_date} < :d "
        f"ORDER BY {q_date} DESC "
        "LIMIT 1"
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(text(sql), conn, params={"d": str(trade_date)})
    if df.empty:
        return None
    x = pd.to_numeric(df.iloc[0]["c"], errors="coerce")
    if pd.isna(x):
        return None
    return float(x)


def _intraday_guardrail(
    engine: Engine,
    trade_date: str,
    intraday_dir: str,
    intraday_ts_col: str,
    intraday_price_col: str,
    ref_daily_table: str,
    ref_daily_date_col: str,
    ref_daily_close_col: str,
    warn_drop_pct: float,
    alert_drop_pct: float,
    warn_5m_drop_pct: float,
    alert_5m_drop_pct: float,
) -> dict:
    out = {
        "enabled": bool(str(intraday_dir).strip()),
        "status": "UNAVAILABLE",
        "level": "NA",
        "latest_price": None,
        "open_price": None,
        "prev_close": None,
        "drop_vs_prev_close": None,
        "low_drop_vs_prev_close": None,
        "drop_vs_open": None,
        "drop_5m": None,
        "action_hint": "NONE",
        "reason": "",
    }
    if not out["enabled"]:
        out["reason"] = "intraday dir not configured"
        return out

    base = Path(str(intraday_dir))
    cand_dirs = [base]
    if not base.is_absolute():
        cand_dirs.append((Path.cwd() / base).resolve())
        cand_dirs.append((Path(__file__).resolve().parents[1] / base).resolve())
        cand_dirs.append((Path(__file__).resolve().parents[2] / "UTRBE" / base).resolve())
    files: list[Path] = []
    seen: set[str] = set()
    for d in cand_dirs:
        for f in _list_intraday_files(d):
            k = str(f.resolve())
            if k not in seen:
                files.append(f)
                seen.add(k)
    if not files:
        out["reason"] = f"no intraday files found under: {intraday_dir}"
        return out

    tstr = str(trade_date)
    files = sorted(files, key=lambda p: (0 if tstr in p.name else 1, -p.stat().st_mtime))
    intra = pd.DataFrame(columns=["ts", "px"])
    prev_close = None
    for fp in files:
        try:
            x, pc = _read_intraday_file(fp, ts_col=intraday_ts_col, price_col=intraday_price_col)
        except Exception:
            continue
        if x.empty:
            continue
        xt = x[pd.to_datetime(x["ts"]).dt.date == pd.to_datetime(trade_date).date()].copy()
        if xt.empty:
            continue
        intra = xt.sort_values("ts").reset_index(drop=True)
        prev_close = pc
        # try infer prev_close from same file previous date if missing
        if prev_close is None:
            xp = x[pd.to_datetime(x["ts"]).dt.date < pd.to_datetime(trade_date).date()].copy()
            if not xp.empty:
                prev_close = float(xp.sort_values("ts")["px"].iloc[-1])
        break

    if intra.empty:
        out["reason"] = f"no intraday rows for {trade_date} in files"
        return out
    if prev_close is None or prev_close <= 0:
        try:
            prev_close = _read_prev_close(
                engine=engine,
                table_name=ref_daily_table,
                trade_date=trade_date,
                date_col=ref_daily_date_col,
                close_col=ref_daily_close_col,
            )
        except Exception:
            prev_close = None
    if prev_close is None or prev_close <= 0:
        out["reason"] = "prev close not available (file+db)"
        return out

    latest = float(intra["px"].iloc[-1])
    open_px = float(intra["px"].iloc[0])
    low_px = float(intra["px"].min())
    drop_prev = latest / prev_close - 1.0
    low_drop_prev = low_px / prev_close - 1.0
    drop_open = latest / open_px - 1.0 if open_px > 0 else None

    ts_last = pd.to_datetime(intra["ts"].iloc[-1])
    cutoff = ts_last - pd.Timedelta(minutes=5)
    base = intra.loc[intra["ts"] <= cutoff, "px"]
    px_5m_ago = float(base.iloc[-1]) if not base.empty else None
    drop_5m = (latest / px_5m_ago - 1.0) if (px_5m_ago is not None and px_5m_ago > 0) else None

    level = "NORMAL"
    reasons: list[str] = []
    if low_drop_prev <= -abs(float(alert_drop_pct)):
        level = "ALERT"
        reasons.append(f"day low vs prev close {low_drop_prev:.2%} <= -{abs(float(alert_drop_pct)):.2%}")
    if drop_5m is not None and drop_5m <= -abs(float(alert_5m_drop_pct)):
        level = "ALERT"
        reasons.append(f"5m drop {drop_5m:.2%} <= -{abs(float(alert_5m_drop_pct)):.2%}")
    if level != "ALERT":
        if low_drop_prev <= -abs(float(warn_drop_pct)):
            level = "WARN"
            reasons.append(f"day low vs prev close {low_drop_prev:.2%} <= -{abs(float(warn_drop_pct)):.2%}")
        if drop_5m is not None and drop_5m <= -abs(float(warn_5m_drop_pct)):
            level = "WARN"
            reasons.append(f"5m drop {drop_5m:.2%} <= -{abs(float(warn_5m_drop_pct)):.2%}")

    out.update(
        {
            "status": "OK",
            "level": level,
            "latest_price": latest,
            "open_price": open_px,
            "prev_close": prev_close,
            "drop_vs_prev_close": drop_prev,
            "low_drop_vs_prev_close": low_drop_prev,
            "drop_vs_open": drop_open,
            "drop_5m": drop_5m,
            "action_hint": ("REDUCE_FAST" if level == "ALERT" else ("REDUCE_LIGHT" if level == "WARN" else "NONE")),
            "reason": "; ".join(reasons) if reasons else "normal range",
        }
    )
    return out


def _upsert_df(engine: Engine, df: pd.DataFrame, table_name: str, key_cols: list[str], int_cols: set[str] | None = None) -> int:
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
    q_table = _quote(table_name)
    with engine.begin() as conn:
        defs: list[str] = []
        for c in cols:
            q_col = _quote(c)
            not_null = " NOT NULL" if c in key_cols else " NULL"
            if c == "date" or c.endswith("_date"):
                defs.append(f"{q_col} DATE{not_null}")
            elif c in int_cols:
                defs.append(f"{q_col} INT{not_null}")
            elif pd.api.types.is_numeric_dtype(x[c]):
                defs.append(f"{q_col} DOUBLE{not_null}")
            else:
                defs.append(f"{q_col} VARCHAR(255){not_null}")
        pk = ", ".join([_quote(k) for k in key_cols])
        conn.exec_driver_sql(f"CREATE TABLE IF NOT EXISTS {q_table} ({', '.join(defs)}, PRIMARY KEY ({pk}))")
        existing = {str(r[0]) for r in conn.exec_driver_sql(f"SHOW COLUMNS FROM {q_table}")}
        for c in cols:
            if c in existing:
                continue
            q_col = _quote(c)
            if c == "date" or c.endswith("_date"):
                conn.exec_driver_sql(f"ALTER TABLE {q_table} ADD COLUMN {q_col} DATE NULL")
            elif c in int_cols:
                conn.exec_driver_sql(f"ALTER TABLE {q_table} ADD COLUMN {q_col} INT NULL")
            elif pd.api.types.is_numeric_dtype(x[c]):
                conn.exec_driver_sql(f"ALTER TABLE {q_table} ADD COLUMN {q_col} DOUBLE NULL")
            else:
                conn.exec_driver_sql(f"ALTER TABLE {q_table} ADD COLUMN {q_col} VARCHAR(255) NULL")
        col_list = ", ".join([_quote(c) for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join([f"{_quote(c)}=VALUES({_quote(c)})" for c in cols if c not in key_cols])
        sql = f"INSERT INTO {q_table} ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        rows = [
            tuple(r)
            for r in x[cols].astype(object).where(pd.notna(x[cols]), None).to_numpy().tolist()
        ]
        conn.exec_driver_sql(sql, rows)
    return len(rows)


def _ensure_episodes_table(engine: Engine, table_name: str) -> None:
    q_table = _quote(table_name)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"CREATE TABLE IF NOT EXISTS {q_table} ("
            "`date` DATE NOT NULL,"
            "`episode_idx` INT NOT NULL,"
            "PRIMARY KEY (`date`,`episode_idx`)"
            ")"
        )


def _upsert_summary_archive(engine: Engine, date_str: str, table_name: str, payload: dict) -> int:
    q_table = _quote(table_name)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"CREATE TABLE IF NOT EXISTS {q_table} ("
            "`date` DATE NOT NULL PRIMARY KEY,"
            "`summary_json` LONGTEXT NULL,"
            "`updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
            ")"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {q_table} (`date`,`summary_json`) VALUES (%s,%s) "
            "ON DUPLICATE KEY UPDATE `summary_json`=VALUES(`summary_json`)",
            [(pd.to_datetime(date_str).date(), json.dumps(payload, ensure_ascii=False))],
        )
    return 1


def _upsert_report_archive(
    engine: Engine,
    date_str: str,
    table_name: str,
    report_md: str,
    report_path: str,
) -> int:
    q_table = _quote(table_name)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"CREATE TABLE IF NOT EXISTS {q_table} ("
            "`date` DATE NOT NULL PRIMARY KEY,"
            "`report_md` LONGTEXT NULL,"
            "`report_path` VARCHAR(255) NULL,"
            "`updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
            ")"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {q_table} (`date`,`report_md`,`report_path`) VALUES (%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE `report_md`=VALUES(`report_md`), `report_path`=VALUES(`report_path`)",
            [(pd.to_datetime(date_str).date(), report_md, report_path)],
        )
    return 1


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _load_allocation_with_history(primary_csv: Path) -> pd.DataFrame:
    if not primary_csv.exists():
        raise FileNotFoundError(f"missing file: {primary_csv}")
    cur = pd.read_csv(primary_csv, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    if len(cur) >= 10:
        return cur
    hist_csv = primary_csv.with_name("daily_allocation_history.csv")
    if hist_csv.exists():
        hist = pd.read_csv(hist_csv, parse_dates=["date"]).sort_values("date")
        hist = hist.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        if not hist.empty:
            return hist
    return cur


def _episode_stats(alloc: pd.DataFrame) -> dict:
    x = alloc.sort_values("date").reset_index(drop=True).copy()
    reason = x.get("hard_gate_reason", pd.Series(["NONE"] * len(x))).fillna("NONE").astype(str)
    hard_on = reason.ne("NONE")

    starts = x.index[hard_on & (~hard_on.shift(1, fill_value=False))]
    ends = x.index[(~hard_on) & hard_on.shift(1, fill_value=False)]
    if len(x) > 0 and bool(hard_on.iloc[-1]):
        ends = pd.Index(list(ends) + [len(x) - 1])

    episodes = []
    for s in starts:
        e = next((j for j in ends if j >= s), len(x) - 1)
        seg = x.iloc[s : e + 1].copy()
        major_reason = seg.get("hard_gate_reason", pd.Series(["NONE"])).value_counts().index[0]
        min_alloc = float(pd.to_numeric(seg.get("final_allocation", 0.0), errors="coerce").fillna(0.0).min())
        rec = None
        for k in range(int(e) + 1, len(x)):
            if float(pd.to_numeric(x.loc[k, "final_allocation"], errors="coerce")) >= 0.95:
                rec = int(k - int(e))
                break
        episodes.append(
            {
                "start_date": str(pd.to_datetime(seg["date"].iloc[0]).date()),
                "end_date": str(pd.to_datetime(seg["date"].iloc[-1]).date()),
                "days": int(len(seg)),
                "major_reason": str(major_reason),
                "min_allocation": min_alloc,
                "recovery_days_to_95pct": rec,
            }
        )

    edf = pd.DataFrame(episodes) if episodes else pd.DataFrame(columns=["start_date", "days", "recovery_days_to_95pct", "min_allocation"])
    if edf.empty:
        return {
            "episodes": [],
            "avg_triggers_per_year": 0.0,
            "avg_episode_days": 0.0,
            "avg_recovery_days": 0.0,
            "full_liquidation_days": 0,
            "full_liquidation_episode_ratio": 0.0,
            "annual_trigger_counts": {},
        }

    edf["year"] = pd.to_datetime(edf["start_date"]).dt.year
    annual = {str(int(k)): int(v) for k, v in edf.groupby("year").size().items()}
    avg_triggers = float(edf.groupby("year").size().mean()) if annual else 0.0
    rec = pd.to_numeric(edf["recovery_days_to_95pct"], errors="coerce")
    full_liq_ratio = float((pd.to_numeric(edf["min_allocation"], errors="coerce") <= 0.05).mean())

    return {
        "episodes": episodes,
        "avg_triggers_per_year": avg_triggers,
        "avg_episode_days": float(pd.to_numeric(edf["days"], errors="coerce").mean()),
        "avg_recovery_days": float(rec.mean()) if rec.notna().any() else 0.0,
        "full_liquidation_days": int((pd.to_numeric(x["final_allocation"], errors="coerce") <= 0.05).sum()),
        "full_liquidation_episode_ratio": full_liq_ratio,
        "annual_trigger_counts": annual,
    }


def _cn_action(action: str) -> str:
    m = {
        "FULL_RISK_ON": "维持进攻仓位",
        "LIGHT_REDUCE": "轻度降仓",
        "REDUCE_TO_35_60": "中度降仓",
        "REDUCE_TO_15_35": "明显降仓",
        "DEFENSIVE_0_15": "防守仓位",
    }
    return m.get(str(action).upper(), str(action))


def _action_color(action: str) -> str:
    a = str(action).upper()
    if a in {"FULL_RISK_ON"}:
        return "#2e8b57"
    if a in {"LIGHT_REDUCE", "REDUCE_TO_35_60"}:
        return "#e67e22"
    return "#d62728"


def _cn_reason(reason: str) -> str:
    m = {
        "NONE": "无硬门槛触发",
        "BREADTH_CRISIS": "广度崩塌触发",
        "TREND_CAP": "趋势破位上限触发",
        "CREDIT_HIGH": "信用扩张确认触发",
    }
    return m.get(str(reason).upper(), str(reason))


def _cn_regime(regime: str) -> str:
    m = {
        "LOW": "低风险",
        "MEDIUM": "中风险",
        "MEDIUM_GATED": "中风险（门控）",
        "HIGH": "高风险",
        "CRISIS": "危机",
    }
    return m.get(str(regime).upper(), str(regime))


def _effective_regime_from_row(row: pd.Series) -> str:
    reg = str(row.get("risk_regime", "NA"))
    try:
        gr = int(pd.to_numeric(row.get("guardrail_applied", 0), errors="coerce"))
    except Exception:
        gr = 0
    alloc = float(pd.to_numeric(row.get("final_allocation", 0.0), errors="coerce"))
    if gr == 1 and alloc < 0.95:
        return "MEDIUM_GATED" if alloc >= 0.35 else "HIGH"
    return reg


def _make_120d_plot(
    alloc_sorted: pd.DataFrame,
    bt_daily: pd.DataFrame,
    out_dir: Path,
    trade_date: str,
    today_conclusion: str,
) -> tuple[Path, Path]:
    nav = bt_daily.copy()
    nav["date"] = pd.to_datetime(nav["date"])
    nav = nav.sort_values("date").tail(120).copy()
    if nav.empty:
        raise ValueError("backtest daily dataframe is empty; cannot render strategy chart")

    act = alloc_sorted[["date", "allocation_action", "risk_regime"]].copy()
    act["date"] = pd.to_datetime(act["date"])
    df = nav.merge(act, on="date", how="left")
    if df.empty:
        raise ValueError("merged strategy dataframe is empty; cannot render strategy chart")
    df["strategy_nav"] = pd.to_numeric(df["strategy_nav"], errors="coerce").ffill()
    df["allocation_action"] = df["allocation_action"].fillna("NA").astype(str)
    df["risk_regime"] = df["risk_regime"].fillna("NA").astype(str)

    prev_action = df["allocation_action"].shift(1).fillna(df["allocation_action"])
    signal_mask = df["allocation_action"].astype(str) != prev_action.astype(str)
    sig = df.loc[signal_mask, ["date", "strategy_nav", "allocation_action", "risk_regime"]].copy()
    if len(sig) > 12:
        sig = sig.tail(12).copy()

    fig, ax = plt.subplots(figsize=(16, 9), facecolor="#e6e6e6")
    ax.set_facecolor("#f0f0f0")
    ax.plot(df["date"], df["strategy_nav"], color="#274c77", linewidth=2.4, label="策略净值")
    y_min = float(df["strategy_nav"].min()) * 0.96
    y_max = float(df["strategy_nav"].max()) * 1.05
    ax.set_ylim(y_min, y_max)
    ax.grid(alpha=0.26, linestyle="-", color="#c9c9c9")
    ax.set_ylabel("NAV")
    ax.set_xlabel("日期")
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    ax.set_title(
        f"V31 双轨引擎策略执行图（近120天，截至 {trade_date}）\n信号标签放置于空白区，箭头指向实际执行点",
        fontsize=14,
        fontweight="bold",
        pad=14,
    )

    top_slots = [(0.06, 0.96), (0.30, 0.96), (0.54, 0.96), (0.78, 0.96), (0.18, 0.88), (0.42, 0.88), (0.66, 0.88), (0.90, 0.88)]
    bottom_slots = [(0.06, 0.10), (0.30, 0.10), (0.54, 0.10), (0.78, 0.10), (0.18, 0.18), (0.42, 0.18), (0.66, 0.18), (0.90, 0.18)]
    top_i, bot_i = 0, 0

    for _, row in sig.iterrows():
        x = row["date"]
        y = float(row["strategy_nav"])
        action = str(row["allocation_action"])
        regime = str(row["risk_regime"])
        c = _action_color(action)
        action_cn = _cn_action(action)
        label = f"{action_cn}\n{x.date()} | {regime}"
        y_mid = (y_min + y_max) / 2.0
        if y >= y_mid:
            sx, sy = bottom_slots[bot_i % len(bottom_slots)]
            bot_i += 1
        else:
            sx, sy = top_slots[top_i % len(top_slots)]
            top_i += 1
        ax.annotate(
            label,
            xy=(x, y),
            xycoords="data",
            xytext=(sx, sy),
            textcoords=("axes fraction", "axes fraction"),
            arrowprops={"arrowstyle": "->", "lw": 1.2, "color": c, "alpha": 0.95},
            fontsize=9,
            fontweight="bold",
            bbox={"boxstyle": "round,pad=0.26", "fc": "#f7f7f7", "ec": c, "alpha": 0.96},
            color=c,
            ha="center",
            va="center",
        )
        ax.scatter([x], [y], color=c, s=28, zorder=5, edgecolors="white", linewidths=0.5)

    ax.text(
        0.01,
        0.995,
        f"{df['date'].iloc[0].date()} - {df['date'].iloc[-1].date()}",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        color="#4a4a4a",
        bbox={"boxstyle": "round,pad=0.16", "fc": "#eaeaea", "ec": "#bdbdbd"},
    )
    ax.legend(loc="upper left", frameon=True, framealpha=0.86)
    fig.text(
        0.02,
        0.01,
        f"今日结论：{today_conclusion}",
        ha="left",
        va="bottom",
        fontsize=12,
        fontweight="bold",
        bbox={"boxstyle": "round,pad=0.3", "fc": "#f7f7f7", "ec": "#d0d0d0"},
    )
    fig.tight_layout(rect=[0, 0.05, 1, 1])

    date_tag = trade_date.replace("-", "")
    p1 = out_dir / "strategy_120d.png"
    p2 = out_dir / f"strategy_120d_{date_tag}.png"
    fig.savefig(p1, dpi=140)
    fig.savefig(p2, dpi=140)
    # Backward-compat alias
    fig.savefig(out_dir / "trend_120d.png", dpi=140)
    fig.savefig(out_dir / f"trend_120d_{date_tag}.png", dpi=140)
    plt.close(fig)
    return p1, p2


def _is_intraday_snapshot(trade_date: str, market_tz: str = "America/New_York", close_hhmm: str = "16:00") -> bool:
    try:
        tz = ZoneInfo(market_tz)
    except Exception:
        tz = ZoneInfo("America/New_York")
    now_local = datetime.now(tz)
    trade_dt = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(trade_dt):
        return False
    if trade_dt.date() != now_local.date():
        return False
    if now_local.weekday() >= 5:
        return False
    hh, mm = (close_hhmm.split(":") + ["00"])[:2]
    try:
        close_minutes = int(hh) * 60 + int(mm)
    except Exception:
        close_minutes = 16 * 60
    now_minutes = now_local.hour * 60 + now_local.minute
    return now_minutes < close_minutes


def main() -> None:
    _setup_cn_font()
    engine = _create_engine()
    p = argparse.ArgumentParser(description="V31 每日运维监控报告")
    p.add_argument("--backtest-summary-json", default="output/v31_backtest_eval_default_prod/summary.json")
    p.add_argument("--risk-summary-json", default="output/v31_risk_aggregate_default_prod/summary.json")
    p.add_argument("--allocation-csv", default="output/v31_risk_aggregate_default_prod/daily_allocation.csv")
    p.add_argument("--allocation-table", default="")
    p.add_argument("--backtest-daily-csv", default="output/v31_backtest_eval_default_prod/v30_backtest_daily.csv")
    p.add_argument("--backtest-daily-table", default="")
    p.add_argument("--start-date", default="")
    p.add_argument("--end-date", default="")
    p.add_argument("--reference-summary-json", default="output/v31_backtest_eval_hardgate_gatepass/summary.json")
    p.add_argument("--lowfreq-summary-json", default="")
    p.add_argument("--v2-latest-state-json", default="")
    p.add_argument("--unified-sentiment-json", default="")
    p.add_argument("--unified-sentiment-summary-json", default="")
    p.add_argument("--policy-name", default="")
    p.add_argument("--policy-config-json", default="")
    p.add_argument("--snapshot-table", default="v31_ops_monitor_snapshot")
    p.add_argument("--health-table", default="v31_ops_monitor_health_checks")
    p.add_argument("--episodes-table", default="v31_ops_monitor_episodes")
    p.add_argument("--summary-archive-table", default="v31_ops_monitor_summary_archive")
    p.add_argument("--report-archive-table", default="v31_ops_monitor_report_archive")
    p.add_argument("--artifact-registry-table", default="v31_output_artifact_registry_daily")
    p.add_argument("--market-tz", default="America/New_York")
    p.add_argument("--market-close-hhmm", default="16:00", help="Market close cutoff in HH:MM local market time.")
    p.add_argument("--enable-intraday", action="store_true", help="Enable intraday mode/guards and intraday wording in report.")
    p.add_argument(
        "--intraday-policy",
        default="mark",
        choices=["mark", "skip", "allow"],
        help="When trade_date is today and before market close: mark=generate intraday-labeled report; skip=do not generate report files; allow=generate normal report.",
    )
    p.add_argument("--intraday-dir", default="data/intraday")
    p.add_argument("--intraday-ts-col", default="datetime")
    p.add_argument("--intraday-price-col", default="close")
    p.add_argument("--intraday-ref-daily-table", default="spy")
    p.add_argument("--intraday-ref-date-col", default="date")
    p.add_argument("--intraday-ref-close-col", default="close")
    p.add_argument("--intraday-warn-drop-pct", type=float, default=0.015)
    p.add_argument("--intraday-alert-drop-pct", type=float, default=0.025)
    p.add_argument("--intraday-warn-5m-drop-pct", type=float, default=0.008)
    p.add_argument("--intraday-alert-5m-drop-pct", type=float, default=0.012)
    p.add_argument("--skip-db-upsert", action="store_true")
    p.add_argument("--output-dir", default="output/v31_ops_monitor")
    args = p.parse_args()

    bt = _load_json(Path(args.backtest_summary_json))
    rk = _load_json(Path(args.risk_summary_json))
    rf = _load_json(Path(args.reference_summary_json))
    lf = _load_json_optional(args.lowfreq_summary_json)
    v2 = _load_json_optional(args.v2_latest_state_json)
    us = _load_json_optional(args.unified_sentiment_json)
    us_sum = _load_json_optional(args.unified_sentiment_summary_json)
    alloc_cfg = rk.get("config", {}).get("allocation", {}) if isinstance(rk, dict) else {}
    agg_cfg = rk.get("config", {}).get("aggregation", {}) if isinstance(rk, dict) else {}
    policy_name = str(args.policy_name).strip() or "NA"
    policy_cfg = str(args.policy_config_json).strip() or "NA"
    policy_cfg_json = _load_json_optional(policy_cfg)
    if not alloc_cfg and isinstance(policy_cfg_json, dict):
        alloc_cfg = policy_cfg_json
    if not agg_cfg and isinstance(policy_cfg_json, dict):
        agg_cfg = policy_cfg_json
    def _cfg_pick(k: str, primary: dict, secondary: dict, default):
        if isinstance(primary, dict) and k in primary and primary.get(k) not in (None, ""):
            return primary.get(k)
        if isinstance(secondary, dict) and k in secondary and secondary.get(k) not in (None, ""):
            return secondary.get(k)
        return default
    policy_snapshot = {
        "name": policy_name,
        "config_json": policy_cfg,
        "tactical_gate_mode": str(_cfg_pick("tactical_gate_mode", alloc_cfg, policy_cfg_json, "NA")),
        "early_struct_gate_mode": str(_cfg_pick("early_struct_gate_mode", alloc_cfg, policy_cfg_json, "NA")),
        "early_struct_top20_mult": float(pd.to_numeric(_cfg_pick("early_struct_top20_mult", alloc_cfg, policy_cfg_json, 1.0), errors="coerce")),
        "early_struct_top10_mult": float(pd.to_numeric(_cfg_pick("early_struct_top10_mult", alloc_cfg, policy_cfg_json, 1.0), errors="coerce")),
        "shock_risk_cut": float(pd.to_numeric(_cfg_pick("shock_risk_cut", agg_cfg, policy_cfg_json, 0.0), errors="coerce")),
        "shock_high_cut": float(pd.to_numeric(_cfg_pick("shock_high_cut", agg_cfg, policy_cfg_json, 0.0), errors="coerce")),
    }

    if str(args.allocation_table).strip():
        alloc = _read_table(engine, str(args.allocation_table).strip(), start=str(args.start_date), end=str(args.end_date))
    else:
        alloc_path = Path(args.allocation_csv)
        alloc = _load_allocation_with_history(alloc_path)
    if str(args.backtest_daily_table).strip():
        bt_daily = _read_table(engine, str(args.backtest_daily_table).strip(), start=str(args.start_date), end=str(args.end_date))
    else:
        bt_daily_path = Path(args.backtest_daily_csv)
        if not bt_daily_path.exists():
            raise FileNotFoundError(f"missing file: {bt_daily_path}")
        bt_daily = pd.read_csv(bt_daily_path, parse_dates=["date"])

    cagr_impact = float(bt["comparison"]["cagr_impact"])
    dd_reduction = float(bt["comparison"]["max_drawdown_reduction"])
    pass_dd = bool(dd_reduction >= 0.40)
    pass_cagr = bool(cagr_impact < 0.02)

    ref_cagr = float(rf["comparison"]["cagr_impact"])
    ref_dd = float(rf["comparison"]["max_drawdown_reduction"])
    drift = {
        "cagr_impact_delta_vs_reference": float(cagr_impact - ref_cagr),
        "dd_reduction_delta_vs_reference": float(dd_reduction - ref_dd),
    }

    eps = _episode_stats(alloc)
    hard_counts = rk.get("hard_gate_counts", {})

    health = {
        "gate_dd_reduction_ge_40pct": pass_dd,
        "gate_cagr_impact_lt_2pct": pass_cagr,
        "trigger_density_ok_2_to_8_per_year": bool(2.0 <= eps["avg_triggers_per_year"] <= 8.0),
        "episode_duration_ok_lt_15_days": bool(eps["avg_episode_days"] < 15.0),
        "full_liquidation_forbidden": bool(eps["full_liquidation_days"] == 0),
    }

    alloc_sorted = alloc.sort_values("date").reset_index(drop=True).copy()
    def _col_series(name: str, default: float = 0.0) -> pd.Series:
        if name in alloc_sorted.columns:
            return pd.to_numeric(alloc_sorted[name], errors="coerce")
        return pd.Series([default] * len(alloc_sorted), index=alloc_sorted.index, dtype=float)

    latest_row = alloc_sorted.iloc[-1].to_dict()
    trade_date = str(pd.to_datetime(latest_row["date"]).date())
    date_tag = trade_date.replace("-", "")
    is_intraday = _is_intraday_snapshot(
        trade_date=trade_date,
        market_tz=str(args.market_tz),
        close_hhmm=str(args.market_close_hhmm),
    )
    intraday_enabled = bool(args.enable_intraday)
    intraday_mode = bool(intraday_enabled and is_intraday and str(args.intraday_policy).lower() != "allow")
    intraday_guard = {
        "enabled": False,
        "status": "DISABLED",
        "level": "NA",
        "latest_price": None,
        "open_price": None,
        "prev_close": None,
        "drop_vs_prev_close": None,
        "low_drop_vs_prev_close": None,
        "drop_vs_open": None,
        "drop_5m": None,
        "action_hint": "NONE",
        "reason": "intraday disabled",
    }
    if intraday_enabled:
        intraday_guard = _intraday_guardrail(
            engine=engine,
            trade_date=trade_date,
            intraday_dir=str(args.intraday_dir).strip(),
            intraday_ts_col=str(args.intraday_ts_col),
            intraday_price_col=str(args.intraday_price_col),
            ref_daily_table=str(args.intraday_ref_daily_table),
            ref_daily_date_col=str(args.intraday_ref_date_col),
            ref_daily_close_col=str(args.intraday_ref_close_col),
            warn_drop_pct=float(args.intraday_warn_drop_pct),
            alert_drop_pct=float(args.intraday_alert_drop_pct),
            warn_5m_drop_pct=float(args.intraday_warn_5m_drop_pct),
            alert_5m_drop_pct=float(args.intraday_alert_5m_drop_pct),
        )

    p_struct = float(pd.to_numeric(latest_row.get("p_structural", 0.0), errors="coerce"))
    p_shock = float(pd.to_numeric(latest_row.get("p_shock", 0.0), errors="coerce"))
    early_score = float(pd.to_numeric(latest_row.get("early_score", 0.0), errors="coerce"))
    early_level = int(pd.to_numeric(latest_row.get("early_struct_level", 0), errors="coerce"))
    early_mult = float(pd.to_numeric(latest_row.get("early_struct_multiplier", 1.0), errors="coerce"))
    p_struct_20 = float(_col_series("p_structural").tail(20).mean())
    p_shock_20 = float(_col_series("p_shock").tail(20).mean())
    early_score_20 = float(_col_series("early_score").tail(20).mean())

    out = {
        "window": bt.get("window", {}),
        "policy": policy_snapshot,
        "latest_market_snapshot": {
            "trade_date": trade_date,
            "final_allocation": float(pd.to_numeric(latest_row.get("final_allocation", 0.0), errors="coerce")),
            "allocation_action": str(latest_row.get("allocation_action", "NA")),
            "risk_regime": str(latest_row.get("risk_regime", "NA")),
            "hard_gate_reason": str(latest_row.get("hard_gate_reason", "NONE")),
            "p_structural_today": p_struct,
            "p_shock_today": p_shock,
            "p_structural_20d_avg": p_struct_20,
            "p_shock_20d_avg": p_shock_20,
            "early_score_today": early_score,
            "early_struct_level_today": early_level,
            "early_struct_multiplier_today": early_mult,
            "early_score_20d_avg": early_score_20,
        },
        "metrics": {
            "cagr_impact": cagr_impact,
            "dd_reduction": dd_reduction,
        },
        "reference": {
            "cagr_impact": ref_cagr,
            "dd_reduction": ref_dd,
        },
        "drift_vs_reference": drift,
        "hard_gate_counts": hard_counts,
        "episode_stats": eps,
        "health_checks": health,
        "report_context": {
            "is_intraday_snapshot": bool(is_intraday),
            "intraday_enabled": bool(intraday_enabled),
            "intraday_policy": str(args.intraday_policy),
            "market_tz": str(args.market_tz),
            "market_close_hhmm": str(args.market_close_hhmm),
        },
        "intraday_guardrail": intraday_guard,
    }
    if lf:
        out["lowfreq_recovery"] = lf
    if v2:
        out["v2_signal_state"] = v2
    if us:
        out["unified_sentiment_state"] = us

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    (out_dir / f"summary_{date_tag}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    health_ok = all(bool(v) for v in health.values())
    fail_reasons = []
    if not pass_dd:
        fail_reasons.append(f"目前看不出比基准更抗跌（当前回撤改善 {dd_reduction:.2%}，目标 >=40%）")
    if not pass_cagr:
        fail_reasons.append(f"当前保护成本偏高（收益影响 {cagr_impact:.2%}，目标 <2%）")
    if not health["trigger_density_ok_2_to_8_per_year"]:
        fail_reasons.append(f"风控触发次数偏少/偏多（当前年化 {eps['avg_triggers_per_year']:.2f} 次，建议 2~8 次）")
    if not health["episode_duration_ok_lt_15_days"]:
        fail_reasons.append(f"每次防守持续时间偏长（当前 {eps['avg_episode_days']:.1f} 天，建议 <15 天）")
    if not health["full_liquidation_forbidden"]:
        fail_reasons.append(f"出现过接近清仓的天数（<=5% 仓位）：{eps['full_liquidation_days']} 天")
    status_text = "系统整体正常，可继续按默认配置运行。" if health_ok else ("当前提示：" + "；".join(fail_reasons))
    if (not health_ok) and (intraday_mode or len(alloc_sorted) < 60):
        status_text += "。另外，这次样本还短（盘中/近期窗口），这些统计结论仅供参考，收盘后和更长窗口再看更可靠。"
    if intraday_mode:
        status_text = "[盘中快照] " + status_text
    latest_alloc = float(pd.to_numeric(latest_row.get("final_allocation", 0.0), errors="coerce"))
    latest_action = str(latest_row.get("allocation_action", "NA"))
    latest_regime = str(latest_row.get("risk_regime", "NA"))
    latest_reason = str(latest_row.get("hard_gate_reason", "NONE"))
    guardrail_applied = bool(int(pd.to_numeric(latest_row.get("guardrail_applied", 0), errors="coerce")) == 1)
    data_alerts: list[str] = []
    signal_alerts: list[str] = []

    if us_sum:
        fresh = us_sum.get("source_freshness", {})
        for k in ("vix", "hy_oas", "fear_greed", "aaii_spread"):
            fr = fresh.get(k, {}) if isinstance(fresh, dict) else {}
            err = str(fr.get("error", "")).strip()
            latest = str(fr.get("latest", "")).strip()
            is_stale = bool(fr.get("is_stale", False))
            if err:
                data_alerts.append(f"{k} 数据缺失（{err}）")
            elif is_stale:
                data_alerts.append(f"{k} 非当日更新（latest={latest or 'NA'}）")
    if us:
        for k in ("vix", "hy_oas"):
            if pd.isna(us.get(k, None)):
                data_alerts.append(f"{k} 值为空（null）")

    lf_state = lf.get("latest_state", {}) if lf else {}
    lf_gate = str(lf_state.get("gate", "NA")).upper()
    lf_hint = str(lf_state.get("action_hint", "NA")).upper()
    if lf_gate == "ALERT" and lf_hint == "REDUCE" and latest_action.upper() == "FULL_RISK_ON":
        signal_alerts.append("低频层为 ALERT/REDUCE，但执行层仍 FULL_RISK_ON。")
    v2_warn = str(v2.get("warning_level", "NA")).upper() if v2 else "NA"
    if v2_warn in {"WATCH", "ALERT", "SHORT"} and latest_action.upper() == "FULL_RISK_ON":
        signal_alerts.append(f"V2 预警层为 {v2_warn}，但执行层仍 FULL_RISK_ON。")
    if intraday_enabled and intraday_guard.get("status") == "OK" and str(intraday_guard.get("level", "NA")).upper() in {"WARN", "ALERT"}:
        signal_alerts.append(
            f"盘中快控触发 {intraday_guard.get('level')}（{intraday_guard.get('reason', '')}），建议临时动作={intraday_guard.get('action_hint')}"
        )

    data_alerts = list(dict.fromkeys(data_alerts))
    signal_alerts = list(dict.fromkeys(signal_alerts))

    # Present an execution-consistent status in the report headline.
    effective_regime = latest_regime
    effective_regime_note = ""
    if guardrail_applied and latest_alloc < 0.95:
        if latest_alloc >= 0.35:
            effective_regime = "MEDIUM_GATED"
        else:
            effective_regime = "HIGH"
        effective_regime_note = "（执行护栏）"
    out["alerts"] = {
        "data_quality": data_alerts,
        "signal_conflicts": signal_alerts,
    }
    out["latest_market_snapshot"]["effective_risk_regime"] = effective_regime
    out["latest_market_snapshot"]["effective_risk_regime_note"] = effective_regime_note
    if us_sum:
        out["unified_sentiment_freshness"] = us_sum.get("source_freshness", {})
    (out_dir / "summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    (out_dir / f"summary_{date_tag}.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    if latest_alloc >= 0.95:
        pos_state = "高仓位（进攻）"
    elif latest_alloc >= 0.60:
        pos_state = "中高仓位（偏进攻）"
    elif latest_alloc >= 0.35:
        pos_state = "中性仓位（攻守平衡）"
    elif latest_alloc >= 0.15:
        pos_state = "低仓位（防守）"
    else:
        pos_state = "极低仓位（危机防守）"

    p_struct_lvl = "低" if p_struct < 0.25 else ("中" if p_struct < 0.55 else "高")
    p_shock_lvl = "低" if p_shock < 0.25 else ("中" if p_shock < 0.55 else "高")
    early_lvl_text = {0: "常态", 1: "Top20%预警", 2: "Top10%预警"}.get(early_level, str(early_level))
    comp20 = float(_col_series("composite_risk_score").tail(20).mean())
    comp_lvl = "低" if comp20 < 0.35 else ("中" if comp20 < 0.60 else "高")

    prev_action = str(alloc_sorted.iloc[-2]["allocation_action"]) if len(alloc_sorted) >= 2 else latest_action
    execution_signal = "REBALANCE" if prev_action != latest_action else "NONE"

    last10 = alloc_sorted.tail(10).copy()
    d10 = "->".join(pd.to_datetime(last10["date"]).dt.strftime("%Y-%m-%d").tolist())
    regime10_raw = "->".join(last10.get("risk_regime", pd.Series(["NA"] * len(last10))).astype(str).tolist())
    regime10_exec = "->".join(last10.apply(_effective_regime_from_row, axis=1).astype(str).tolist())
    gate10 = "->".join(last10.get("hard_gate_reason", pd.Series(["NONE"] * len(last10))).astype(str).tolist())
    action10 = "->".join(last10.get("allocation_action", pd.Series(["NA"] * len(last10))).astype(str).tolist())
    alloc10 = "->".join([f"{float(pd.to_numeric(v, errors='coerce')):.2f}" for v in last10.get("final_allocation", pd.Series([0.0] * len(last10))).tolist()])

    tactical_gate_mode = str(policy_snapshot.get("tactical_gate_mode", "NA")).upper()
    early_gate_mode = str(policy_snapshot.get("early_struct_gate_mode", "NA")).upper()
    gate_trend_text = "无法判定（缺少价格数据）"
    if "market_price" in alloc_sorted.columns:
        px_s = pd.to_numeric(alloc_sorted["market_price"], errors="coerce")
        px_now = float(px_s.iloc[-1]) if len(px_s) else float("nan")
        if tactical_gate_mode == "MA100" or early_gate_mode == "MA100":
            ma100 = px_s.rolling(100, min_periods=100).mean()
            ma100_now = float(ma100.iloc[-1]) if len(ma100) else float("nan")
            if pd.notna(px_now) and pd.notna(ma100_now):
                gate_trend_text = "当前价格在 MA100 之上" if px_now >= ma100_now else "当前价格在 MA100 之下"
        elif tactical_gate_mode == "MA200" or early_gate_mode == "MA200":
            ma200 = px_s.rolling(200, min_periods=200).mean()
            ma200_now = float(ma200.iloc[-1]) if len(ma200) else float("nan")
            if pd.notna(px_now) and pd.notna(ma200_now):
                gate_trend_text = "当前价格在 MA200 之上" if px_now >= ma200_now else "当前价格在 MA200 之下"

    overall_safe = (comp20 < 0.35) and (str(effective_regime).upper() in {"LOW", "MEDIUM_GATED"}) and (latest_alloc >= 0.95)
    struct_near_zero = (p_struct <= 0.05 and p_struct_20 <= 0.08)
    shock_near_zero = (p_shock <= 0.08 and p_shock_20 <= 0.10)
    early_top15_mult = float(policy_snapshot.get("early_struct_top20_mult", 0.90))
    early_top5_mult = float(policy_snapshot.get("early_struct_top10_mult", 0.80))
    early_inactive = (early_level == 0 and abs(float(early_mult) - 1.0) < 1e-9)
    hardgate_none = str(latest_reason).upper() == "NONE"
    tactical_level_now = int(pd.to_numeric(latest_row.get("tactical_level", 0), errors="coerce"))
    tactical_mult_now = float(pd.to_numeric(latest_row.get("tactical_multiplier", 1.0), errors="coerce"))
    tactical_gate_on_now = bool(int(pd.to_numeric(latest_row.get("tactical_gate_on", 0), errors="coerce")) == 1)

    data_start = str(pd.to_datetime(alloc_sorted["date"].iloc[0]).date())
    data_end = str(pd.to_datetime(alloc_sorted["date"].iloc[-1]).date())
    lf_lines = []
    if lf:
        ls = lf.get("latest_state", {})
        gate_cn = {"NORMAL": "正常", "CAUTION": "观察", "ALERT": "警报"}.get(str(ls.get("gate", "NA")).upper(), str(ls.get("gate", "NA")))
        hint_cn = {"ADD": "可加仓", "HOLD": "继续持有", "REDUCE": "建议降仓"}.get(str(ls.get("action_hint", "NA")).upper(), str(ls.get("action_hint", "NA")))
        conf_cn = {
            "VETO_SHORT_TERM_WARNING": "否决短期预警（中长期恢复强）",
            "CONFIRM_SHORT_TERM_WARNING": "确认短期预警（中长期恢复弱）",
            "NEUTRAL_CONFIRMATION": "中性确认",
        }.get(str(ls.get("short_term_confirmation", "NA")).upper(), str(ls.get("short_term_confirmation", "NA")))
        lf_lines = [
            "",
            "## 低频恢复层（人话版）",
            f"- 中长期状态：{gate_cn}",
            f"- 中长期动作建议：{hint_cn}",
            f"- 对短期信号的态度：{conf_cn}",
            f"- 恢复速度分：{float(ls.get('rss', 0.0)):.3f}（越高恢复越快）",
            f"- 恢复强度分：{float(ls.get('rsts', 0.0)):.3f}（越高修复越完整）",
            f"- 资金流修正：{float(ls.get('persistence_adjustment', 0.0)):+.3f}",
            "- 解读：这是中长期背景层，用来确认或否决短期预警，不直接替代短期信号。",
        ]

    v2_lines = []
    if v2:
        v2_lines = [
            "",
            "## V2信号状态（兼容视图）",
            f"- 模式：{v2.get('mode', 'NA')} | 活跃轨道：{v2.get('active_track', 'NA')}",
            f"- V2动作：{v2.get('action', 'NA')} | 执行信号：{v2.get('execution_signal', 'NA')}",
            f"- 风险概率（lead5 / lead10）：{float(pd.to_numeric(v2.get('p_lead5', 0.0), errors='coerce')):.3f} / {float(pd.to_numeric(v2.get('p_lead10', 0.0), errors='coerce')):.3f}",
            f"- 预警层：{v2.get('warning_level', 'NA')} | 预警来源：{v2.get('warning_source', 'NA')}",
            "- 用法：这部分保持你熟悉的 v2 视角，和上面的 v31 风控执行视角一起看。",
        ]

    us_lines = []
    if us:
        us_lines = [
            "",
            "## 统一情绪层（Unified Sentiment）",
            f"- srs_v2: {float(pd.to_numeric(us.get('srs_v2', 0.0), errors='coerce')):.3f}",
            f"- srs_accel_v2: {float(pd.to_numeric(us.get('srs_accel_v2', 0.0), errors='coerce')):.3f}",
            f"- sentiment_signal_v2: {int(pd.to_numeric(us.get('sentiment_signal_v2', 0), errors='coerce'))}",
            f"- srs_plus: {float(pd.to_numeric(us.get('srs_plus', 0.0), errors='coerce')):.3f}",
            f"- sentiment_signal_plus: {int(pd.to_numeric(us.get('sentiment_signal_plus', 0), errors='coerce'))}",
            "- 说明：v2-compatible 口径用于与旧系统对齐；plus 口径额外吸收 wiki 情绪热度。",
        ]

    lines = [
        "# 每日风险报告（易读版）" + (" [盘中快照]" if intraday_mode else ""),
        "",
        f"## 今日结论（{trade_date}）",
        f"- 冻结策略：{policy_name}",
        f"- 策略配置文件：{policy_cfg}",
        f"- 策略门控：tactical={policy_snapshot['tactical_gate_mode']} | early={policy_snapshot['early_struct_gate_mode']}",
        f"- Early 乘数：Top15%={policy_snapshot['early_struct_top20_mult']:.2f} | Top5%={policy_snapshot['early_struct_top10_mult']:.2f}",
        f"- 当前状态：{_cn_regime(effective_regime)}{effective_regime_note}",
        f"- 操作建议：{_cn_action(latest_action)}",
        f"- 执行信号：{execution_signal}",
        f"- 仓位状态：{pos_state}",
        f"- 总结：{status_text}",
    ]
    if intraday_mode:
        lines.extend(
            [
                "",
                "## 盘中声明",
                f"- 当前为盘中快照（市场时区：{args.market_tz}，收盘时间：{args.market_close_hhmm}）。",
                "- 数据与信号可能在收盘前继续变化，收盘后版本才作为正式日报。",
            ]
        )
    if intraday_enabled:
        lines.extend(
            [
                "",
                "## 盘中快控补丁（分钟级）",
                f"- 状态：{intraday_guard.get('status')} | 级别：{intraday_guard.get('level')} | 临时动作建议：{intraday_guard.get('action_hint')}",
                f"- 触发依据：{intraday_guard.get('reason')}",
            ]
        )
        if intraday_guard.get("status") == "OK":
            d_prev = intraday_guard.get("drop_vs_prev_close")
            d_low_prev = intraday_guard.get("low_drop_vs_prev_close")
            d_open = intraday_guard.get("drop_vs_open")
            d_5m = intraday_guard.get("drop_5m")
            lines.extend(
                [
                    f"- 最新价/昨收跌幅：{d_prev:.2%}" if isinstance(d_prev, float) else "- 最新价/昨收跌幅：NA",
                    f"- 日内最低/昨收跌幅：{d_low_prev:.2%}" if isinstance(d_low_prev, float) else "- 日内最低/昨收跌幅：NA",
                    f"- 最新价/今开跌幅：{d_open:.2%}" if isinstance(d_open, float) else "- 最新价/今开跌幅：NA",
                    f"- 最近5分钟跌幅：{d_5m:.2%}" if isinstance(d_5m, float) else "- 最近5分钟跌幅：NA",
                ]
            )
    if data_alerts or signal_alerts:
        lines.extend(
            [
                "",
                "## 关键告警（数据新鲜度）",
                *([f"- 数据新鲜度：{x}" for x in data_alerts] if data_alerts else ["- 数据新鲜度：无"]),
                *([f"- 信号冲突：{x}" for x in signal_alerts] if signal_alerts else ["- 信号冲突：无"]),
            ]
        )
    explain_lines = [
        f"- 结构轨当前为{p_struct_lvl}压，主要看市场底层是否在变坏。",
        f"- 冲击轨当前为{p_shock_lvl}压，主要看短期是否有突发下跌风险。",
        f"- Early-Structural 当前为{early_lvl_text}，对应乘数 {early_mult:.2f}，用于顶部失稳期的轻度提前降仓。",
    ]
    if guardrail_applied and latest_alloc < 0.95:
        explain_lines.append("- 执行层已触发护栏：尽管模型原始双轨仍偏低压，出于跨层预警与数据新鲜度约束，仓位按防守口径下调。")

    lines.extend([
        "",
        "## 核心指标",
        f"- 结构轨风险概率（今天 / 近20日）：{p_struct:.3f} / {p_struct_20:.3f}",
        f"- 冲击轨风险概率（今天 / 近20日）：{p_shock:.3f} / {p_shock_20:.3f}",
        f"- Early-Structural（今天 / 近20日）：{early_score:.3f} / {early_score_20:.3f} | 级别={early_lvl_text} | 乘数={early_mult:.2f}",
        f"- 组合风险（近20日均值）：{comp20:.3f}（{comp_lvl}）",
        f"- 今日最终仓位：{latest_alloc:.3f}",
        f"- 回撤压缩（目标>=40%）：{dd_reduction:.6f}",
        f"- 收益影响（目标<2%）：{cagr_impact:.6f}",
        "",
        "## 解释版（更易懂）",
        "### 一、今天整体安全吗？",
        f"- 看三件事：组合风险（近20日）={comp20:.3f}；当前状态={_cn_regime(effective_regime)}；今日最终仓位={latest_alloc:.3f}。",
        ("- 意思是：系统判断当前没有系统性风险，所以保持满仓进攻。"
         if overall_safe else "- 意思是：系统未处于“完全安全”状态，仓位会按规则进入防守或过渡。"),
        ("- 这是模型和门控共同给出的结果，不是主观判断；当前没有明显风险门控触发。"
         if hardgate_none and early_inactive else "- 当前仍有部分门控在生效（硬门槛或 Early/Tactical 层）。"),
        "",
        "### 二、结构轨 & 冲击轨在干嘛？",
        f"- 先说口径：以下判断只基于截至 {trade_date} 的日线数据，不等于盘中实时预警。",
        f"- 结构轨风险概率：今天 {p_struct:.3f}；近20日 {p_struct_20:.3f}。"
        + (" 数值较低，表示在“截至当日收盘”的口径下，暂未看到慢性熊市结构特征。" if struct_near_zero else " 处于可关注区间，需继续观察趋势和中期环境。"),
        ("- 说明：收盘口径下暂未见中期结构恶化信号，不代表盘中不会突发走弱。"
         if struct_near_zero else "- 说明：中期结构风险仍需持续观察。"),
        "- 结构轨升高通常意味着：趋势走坏、中期环境恶化。",
        f"- 冲击轨风险概率：今天 {p_shock:.3f}；近20日 {p_shock_20:.3f}。"
        + (" 数值较低，表示在“截至当日收盘”的口径下，短期冲击压力不高。" if shock_near_zero else " 不算低位，需关注突发波动风险。"),
        ("- 说明：这是收盘口径结论；若盘中突然急跌，日线模型通常要到收盘后才会完整反映。"
         if shock_near_zero else "- 说明：短期冲击压力仍需跟踪。"),
        "- 冲击轨一般会在突发下跌前抬升。",
        "",
        "### 三、Early-Structural 是什么？",
        f"- 今天 {early_score:.3f}；近20日 {early_score_20:.3f}；级别 {early_lvl_text}；乘数 {early_mult:.2f}。",
        "- 意思是：风险分位处在中等区域。",
        ("- 没有进入 Top15%。" if early_level < 1 else "- 已进入 Top15%。"),
        ("- 没有进入 Top5%。" if early_level < 2 else "- 已进入 Top5%。"),
        ("- 当前未进入 Top15% / Top5%，所以没有触发 Early 降仓。"
         if early_inactive else "- 当前已进入 Early 风险分位区间，Early 层正在抑制仓位。"),
        ("- 所以：没触发 Early 降仓。"
         if early_inactive else "- 所以：已触发 Early 降仓。"),
        f"- 规则：进入 Top15% 时仓位乘以 {early_top15_mult:.2f}；进入 Top5% 时仓位乘以 {early_top5_mult:.2f}。",
        "",
        "### 四、为什么仓位是 1.000（或接近）？",
        f"- 当前门控：Tactical={tactical_gate_mode}；Early={early_gate_mode}。",
        f"- 趋势条件：{gate_trend_text}。",
        ("- 在 MA 门控规则下，趋势未破位时通常不减仓。"
         if "MA" in tactical_gate_mode or "MA" in early_gate_mode else "- 当前门控不使用 MA 趋势条件，仓位由风险分层直接决定。"),
        "",
        "### 五、Tactical 是什么？",
        "- Tactical 是短周期风控层，用来在短期波动放大时做临时降仓，避免净值大幅回撤。",
        f"- 今日 Tactical 状态：level={tactical_level_now}，multiplier={tactical_mult_now:.2f}，gate_on={tactical_gate_on_now}。",
        (f"- 当前 Tactical 门控为 {tactical_gate_mode}，且门控未打开，所以 Tactical 对仓位不生效。"
         if (not tactical_gate_on_now) else f"- 当前 Tactical 门控为 {tactical_gate_mode}，门控已打开，Tactical 正在参与仓位调整。"),
        "",
        "## 人话解释（怎么读这些数字）",
        *explain_lines,
        "- 双轨一起看：结构轨决定大方向，冲击轨决定要不要临时踩刹车。",
        f"- 今天的硬门槛触发：{_cn_reason(latest_reason)}。",
        "- 若回撤压缩达标且收益影响不过线，说明风控和收益平衡在可接受区间。",
        "",
        "## 证据链（双轨 + 硬门槛）",
        f"- 风险状态（模型原始）：{latest_regime}",
        f"- 风险状态（执行后）：{effective_regime}{effective_regime_note}",
        f"- 动作：{latest_action}",
        f"- 硬门槛原因：{latest_reason}",
        f"- hard_force_crisis：{bool(latest_row.get('hard_force_crisis', False))}",
        f"- hard_trend_cap_flag：{bool(latest_row.get('hard_trend_cap_flag', False))}",
        f"- hard_credit_high_flag：{bool(latest_row.get('hard_credit_high_flag', False))}",
        "",
        "## 近10日信号轨迹",
        f"- 日期：{d10}",
        f"- 风险状态轨迹（模型原始）：{regime10_raw}",
        f"- 风险状态轨迹（执行后）：{regime10_exec}",
        f"- 硬门槛轨迹：{gate10}",
        f"- 动作轨迹：{action10}",
        f"- 仓位轨迹：{alloc10}",
        "",
        "## 系统体检（人话版）",
        f"- 回撤控制：{'达标' if pass_dd else '未达标'}（当前 {dd_reduction:.2%}，目标 >=40%）",
        f"- 收益代价：{'达标' if pass_cagr else '未达标'}（当前 {cagr_impact:.2%}，目标 <2%）",
        f"- 触发频率：{eps['avg_triggers_per_year']:.2f} 次/年（建议 2~8，{'正常' if health['trigger_density_ok_2_to_8_per_year'] else '偏离'})",
        f"- 触发持续：{eps['avg_episode_days']:.1f} 天（建议 <15，{'正常' if health['episode_duration_ok_lt_15_days'] else '偏长'})",
        f"- 全清仓天数：{eps['full_liquidation_days']} 天（要求 0 天，{'符合' if health['full_liquidation_forbidden'] else '不符合'})",
        f"- 相比参考基线：收益影响变化 {drift['cagr_impact_delta_vs_reference']:+.4f}，回撤压缩变化 {drift['dd_reduction_delta_vs_reference']:+.4f}",
        "",
        "## 数据新鲜度（Data Freshness）",
        f"- 当前报告日期：{trade_date}",
        f"- allocation 数据覆盖区间：{data_start} ~ {data_end}",
        "- 若每天跑一次并更新到最新交易日，这份报告就是当天市场监控。",
        "- 重要：若盘中出现急跌，这份日线报告可能滞后；收盘后重跑才是正式风险结论。",
    ])
    lines.extend(lf_lines)
    lines.extend(v2_lines)
    lines.extend(us_lines)

    report_text = "\n".join(lines)
    named_daily_report = out_dir / f"UTRBEV3_daily_report_{trade_date}.md"
    skip_report_files = bool(is_intraday and str(args.intraday_policy).lower() == "skip")
    if not skip_report_files:
        (out_dir / "report.md").write_text(report_text, encoding="utf-8-sig")
        (out_dir / f"report_{date_tag}.md").write_text(report_text, encoding="utf-8-sig")
        (out_dir / "daily_report.md").write_text(report_text, encoding="utf-8-sig")
        (out_dir / f"daily_report_{trade_date}.md").write_text(report_text, encoding="utf-8-sig")
        named_daily_report.write_text(report_text, encoding="utf-8-sig")
    trend_png, trend_png_d = _make_120d_plot(alloc_sorted, bt_daily, out_dir, trade_date, status_text)

    snapshot_rows = 0
    health_rows = 0
    episode_rows = 0
    archive_rows = 0
    report_archive_rows = 0
    artifact_rows = 0

    risk_summary_path = Path(str(args.risk_summary_json))
    backtest_summary_path = Path(str(args.backtest_summary_json))
    lowfreq_summary_path = Path(str(args.lowfreq_summary_json)) if str(args.lowfreq_summary_json).strip() else Path("")
    us_latest_path = Path(str(args.unified_sentiment_json)) if str(args.unified_sentiment_json).strip() else Path("")
    us_summary_path = Path(str(args.unified_sentiment_summary_json)) if str(args.unified_sentiment_summary_json).strip() else Path("")
    risk_dir = risk_summary_path.parent if str(args.risk_summary_json).strip() else Path("")

    required_artifacts: list[tuple[str, Path, str]] = [
        ("ops_summary_json", out_dir / "summary.json", str(args.summary_archive_table)),
        ("ops_chart_png", trend_png, ""),
        ("ops_chart_png_dated", trend_png_d, ""),
        ("v31_risk_latest_allocation", (risk_dir / "latest_allocation.json") if str(risk_dir) else Path(""), "v31_daily_allocation"),
        ("v31_risk_summary", risk_summary_path, "v31_daily_allocation"),
        ("v31_backtest_summary", backtest_summary_path, "v30_backtest_daily"),
    ]
    if str(us_latest_path):
        required_artifacts.append(("v31_unified_sentiment_latest", us_latest_path, "sentiment_daily_unified"))
    if str(us_summary_path):
        required_artifacts.append(("v31_unified_sentiment_summary", us_summary_path, "sentiment_daily_unified"))
    if str(lowfreq_summary_path):
        required_artifacts.append(("v31_lowfreq_summary", lowfreq_summary_path, "v31_lowfreq_recovery_summary"))
    if not skip_report_files:
        required_artifacts.insert(1, ("ops_report_md", named_daily_report, str(args.report_archive_table)))
    missing_required = [str(pth) for _, pth, _ in required_artifacts if not pth.exists()]
    if missing_required and not bool(args.skip_db_upsert):
        raise FileNotFoundError(f"required daily artifacts missing: {missing_required}")
    if missing_required and bool(args.skip_db_upsert):
        print(f"[WARN] Missing required daily artifacts (skip-db-upsert mode): {missing_required}")

    if not bool(args.skip_db_upsert):
        snapshot = pd.DataFrame(
            [
                {
                    "date": trade_date,
                    "risk_regime": latest_regime,
                    "allocation_action": latest_action,
                    "hard_gate_reason": latest_reason,
                    "final_allocation": latest_alloc,
                    "p_structural_today": p_struct,
                    "p_shock_today": p_shock,
                    "early_score_today": early_score,
                    "early_struct_level_today": early_level,
                    "early_struct_multiplier_today": early_mult,
                    "p_structural_20d_avg": p_struct_20,
                    "p_shock_20d_avg": p_shock_20,
                    "cagr_impact": cagr_impact,
                    "dd_reduction": dd_reduction,
                    "status_text": status_text,
                }
            ]
        )
        snapshot_rows = _upsert_df(
            engine,
            snapshot,
            table_name=str(args.snapshot_table),
            key_cols=["date"],
        )
        health_df = pd.DataFrame(
            [
                {"date": trade_date, "check_name": k, "check_pass": int(bool(v))}
                for k, v in health.items()
            ]
        )
        health_rows = _upsert_df(
            engine,
            health_df,
            table_name=str(args.health_table),
            key_cols=["date", "check_name"],
            int_cols={"check_pass"},
        )
        eps_df = pd.DataFrame(eps.get("episodes", []))
        if not eps_df.empty:
            eps_df = eps_df.reset_index(drop=True)
            eps_df.insert(0, "date", pd.to_datetime(trade_date))
            eps_df.insert(1, "episode_idx", eps_df.index.astype(int) + 1)
            episode_rows = _upsert_df(
                engine,
                eps_df,
                table_name=str(args.episodes_table),
                key_cols=["date", "episode_idx"],
                int_cols={"episode_idx", "days", "recovery_days_to_95pct"},
            )
        else:
            _ensure_episodes_table(engine, str(args.episodes_table))
        archive_rows = _upsert_summary_archive(
            engine=engine,
            date_str=trade_date,
            table_name=str(args.summary_archive_table),
            payload=out,
        )
        if not skip_report_files:
            report_archive_rows = _upsert_report_archive(
                engine=engine,
                date_str=trade_date,
                table_name=str(args.report_archive_table),
                report_md=report_text,
                report_path=str(named_daily_report).replace("\\", "/"),
            )

        artifact_payload: list[dict] = []
        for key, pth, source_table in required_artifacts:
            stat = pth.stat() if pth.exists() else None
            artifact_payload.append(
                {
                    "date": trade_date,
                    "artifact_key": str(key),
                    "artifact_path": str(pth).replace("\\", "/"),
                    "source_table": str(source_table),
                    "exists_flag": int(pth.exists()),
                    "file_size_bytes": int(stat.st_size) if stat is not None else None,
                    "file_mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds") if stat is not None else "",
                    "file_sha256": _sha256_file(pth) if stat is not None else "",
                }
            )
        artifact_df = pd.DataFrame(artifact_payload)
        artifact_rows = _upsert_df(
            engine,
            artifact_df,
            table_name=str(args.artifact_registry_table),
            key_cols=["date", "artifact_key"],
            int_cols={"exists_flag"},
        )

    print(f"[OK] Wrote: {out_dir / 'summary.json'}")
    if skip_report_files:
        print(f"[OK] Skip report markdown generation (intraday policy=skip): trade_date={trade_date}")
    else:
        print(f"[OK] Wrote: {out_dir / 'report.md'}")
        print(f"[OK] Wrote: {out_dir / 'daily_report.md'}")
        print(f"[OK] Wrote: {named_daily_report}")
    print(f"[OK] Wrote: {out_dir / f'summary_{date_tag}.json'}")
    if not skip_report_files:
        print(f"[OK] Wrote: {out_dir / f'report_{date_tag}.md'}")
        print(f"[OK] Wrote: {out_dir / f'daily_report_{trade_date}.md'}")
    print(f"[OK] Wrote: {trend_png}")
    print(f"[OK] Wrote: {trend_png_d}")
    if not bool(args.skip_db_upsert):
        print(f"[OK] DB upsert rows: {snapshot_rows} -> table `{args.snapshot_table}`")
        print(f"[OK] DB upsert rows: {health_rows} -> table `{args.health_table}`")
        print(f"[OK] DB upsert rows: {episode_rows} -> table `{args.episodes_table}`")
        print(f"[OK] DB upsert rows: {archive_rows} -> table `{args.summary_archive_table}`")
        print(f"[OK] DB upsert rows: {report_archive_rows} -> table `{args.report_archive_table}`")
        print(f"[OK] DB upsert rows: {artifact_rows} -> table `{args.artifact_registry_table}`")


if __name__ == "__main__":
    main()
