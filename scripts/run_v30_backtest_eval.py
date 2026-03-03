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

from v30.backtest.portfolio_eval import summarize


def _connect():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost")),
        port=int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306"))),
        user=os.getenv("MYSQL_USER", os.getenv("DB_USER", "us_opr")),
        password=os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "sec@Bobo123")),
        database=os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "us_market")),
        charset=os.getenv("MYSQL_CHARSET", "utf8mb4"),
    )


def _read_table(table_name: str, cols: list[str] | None = None, start: str = "", end: str = "") -> pd.DataFrame:
    pick = "*" if not cols else ", ".join([f"`{c}`" for c in cols])
    cond = ""
    params: tuple[str, ...] = ()
    if str(start).strip() and str(end).strip():
        cond = " WHERE `date` BETWEEN %s AND %s"
        params = (str(start), str(end))
    sql = f"SELECT {pick} FROM `{table_name}`{cond} ORDER BY `date`"
    with _connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        raise ValueError(f"no rows in table: {table_name}")
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def _upsert_df(df: pd.DataFrame, table_name: str) -> int:
    if df.empty:
        return 0
    x = df.copy()
    x["date"] = pd.to_datetime(x["date"]).dt.date
    cols = [c for c in x.columns if c != "date"]
    defs = ["`date` DATE NOT NULL PRIMARY KEY"] + [f"`{c}` DOUBLE NULL" for c in cols]
    col_list = ", ".join(["`date`"] + [f"`{c}`" for c in cols])
    placeholders = ", ".join(["%s"] * (len(cols) + 1))
    updates = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in cols])
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(defs)})")
            cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
            existing = {str(r[0]) for r in cur.fetchall()}
            for c in cols:
                if c not in existing:
                    cur.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{c}` DOUBLE NULL")
            sql = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
            rows = x[["date"] + cols].astype(object).where(pd.notna(x[["date"] + cols]), None).to_numpy().tolist()
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)

def fetch_spy_ret(start: pd.Timestamp, end: pd.Timestamp, spy_ret_csv: str) -> pd.Series:
    p = Path(spy_ret_csv)
    if p.exists():
        x = pd.read_csv(p, parse_dates=['date'])
        if 'spy_ret1d' in x.columns:
            x['date'] = pd.to_datetime(x['date']).dt.normalize()
            s = pd.to_numeric(x.set_index('date')['spy_ret1d'], errors='coerce').fillna(0.0)
            s = s.sort_index()
            s.name = 'spy_ret1d'
            return s[(s.index >= start) & (s.index <= end)]

    # reuse UTRBE data fetcher when return cache is not provided
    utrbe_src = (PROJECT_ROOT.parent / 'UTRBE' / 'src').resolve()
    if str(utrbe_src) not in sys.path:
        sys.path.insert(0, str(utrbe_src))
    from utrbe.data.fetcher import DataFetcher

    # Force UTRBE config root so sources.yaml is resolved from sibling UTRBE project.
    utrbe_root = (PROJECT_ROOT.parent / 'UTRBE').resolve()
    f = DataFetcher(config_dir=utrbe_root)
    s = pd.to_numeric(f.get_price('spy', start.date(), end.date()), errors='coerce')
    s.index = pd.to_datetime(s.index)
    r = s.pct_change().fillna(0.0)
    r.name = 'spy_ret1d'
    return r


def main() -> None:
    parser = argparse.ArgumentParser(description='V30 Phase-E backtest evaluation from allocation output.')
    parser.add_argument('--allocation-csv', default='output/v30_risk_aggregate/daily_allocation.csv')
    parser.add_argument('--allocation-table', default='')
    parser.add_argument('--output-dir', default='output/v30_backtest_eval')
    parser.add_argument('--output-daily-table', default='v30_backtest_daily')
    parser.add_argument('--skip-db-upsert', action='store_true')
    parser.add_argument('--skip-csv-output', action='store_true')
    parser.add_argument('--data-start', default='')
    parser.add_argument('--data-end', default='')
    parser.add_argument('--execution-lag-days', type=int, default=1)
    parser.add_argument('--transaction-cost-bps', type=float, default=2.0)
    parser.add_argument('--spy-ret-csv', default='output/v30_backtest_eval/v30_backtest_daily.csv')
    args = parser.parse_args()

    alloc_csv = Path(args.allocation_csv)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if str(args.allocation_table).strip():
        df = _read_table(
            str(args.allocation_table).strip(),
            start=str(args.data_start),
            end=str(args.data_end),
        )
    else:
        if not alloc_csv.exists():
            raise FileNotFoundError(f'missing allocation csv: {alloc_csv}')
        df = pd.read_csv(alloc_csv, parse_dates=['date'])
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    df = df.sort_values('date').reset_index(drop=True)

    start = pd.Timestamp(df['date'].min())
    end = pd.Timestamp(df['date'].max())
    ret = fetch_spy_ret(start, end, args.spy_ret_csv)

    x = df.merge(ret.rename('spy_ret1d'), left_on='date', right_index=True, how='left')
    x['spy_ret1d'] = pd.to_numeric(x['spy_ret1d'], errors='coerce').fillna(0.0)

    # conservative T+lag application
    lag = max(0, int(args.execution_lag_days))
    x['applied_allocation'] = pd.to_numeric(x['final_allocation'], errors='coerce').shift(lag).fillna(1.0)

    # turnover + cost
    x['turnover'] = x['applied_allocation'].diff().abs().fillna(0.0)
    x['trade_cost'] = x['turnover'] * float(args.transaction_cost_bps) / 10000.0

    x['strategy_ret1d_gross'] = x['applied_allocation'] * x['spy_ret1d']
    x['strategy_ret1d'] = x['strategy_ret1d_gross'] - x['trade_cost']

    x['strategy_nav'] = (1.0 + x['strategy_ret1d']).cumprod()
    x['benchmark_nav'] = (1.0 + x['spy_ret1d']).cumprod()

    s_strat = summarize(x['strategy_nav'], x['strategy_ret1d'])
    s_bench = summarize(x['benchmark_nav'], x['spy_ret1d'])

    cagr_impact = float(s_bench['annualized_return'] - s_strat['annualized_return'])
    dd_reduction = 0.0
    if abs(float(s_bench['max_drawdown'])) > 1e-12:
        dd_reduction = float((abs(float(s_bench['max_drawdown'])) - abs(float(s_strat['max_drawdown']))) / abs(float(s_bench['max_drawdown'])))

    summary = {
        'window': {
            'start': str(start.date()),
            'end': str(end.date()),
            'rows': int(len(x)),
        },
        'execution': {
            'lag_days': int(args.execution_lag_days),
            'transaction_cost_bps': float(args.transaction_cost_bps),
        },
        'strategy': s_strat,
        'benchmark': s_bench,
        'comparison': {
            'cagr_impact': cagr_impact,
            'max_drawdown_reduction': dd_reduction,
            'acceptance_check': {
                'dd_reduction_ge_40pct': bool(dd_reduction >= 0.40),
                'cagr_impact_lt_2pct': bool(cagr_impact < 0.02),
            },
        },
    }

    daily = x[[
        'date','final_allocation','applied_allocation','spy_ret1d','strategy_ret1d_gross','trade_cost','strategy_ret1d','strategy_nav','benchmark_nav'
    ]].sort_values('date', ascending=False).reset_index(drop=True)
    if not bool(args.skip_csv_output):
        daily.to_csv(out_dir / 'v30_backtest_daily.csv', index=False)
    if not bool(args.skip_db_upsert):
        rows = _upsert_df(daily, table_name=str(args.output_daily_table))
        print(f"[OK] DB upsert rows: {rows} -> table `{args.output_daily_table}`")

    if not bool(args.skip_csv_output):
        sum_df = pd.DataFrame([
            {'series': 'strategy', **s_strat},
            {'series': 'benchmark', **s_bench},
        ])
        sum_df.to_csv(out_dir / 'v30_backtest_summary.csv', index=False)

    (out_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    lines = [
        '# V30 Backtest Evaluation Report',
        '',
        f"- window: {start.date()} -> {end.date()} ({len(x)} rows)",
        f"- execution: lag={int(args.execution_lag_days)}d, cost={float(args.transaction_cost_bps):.2f} bps",
        '',
        '## Strategy',
        f"- total_return: {float(s_strat['total_return']):.6f}",
        f"- annualized_return: {float(s_strat['annualized_return']):.6f}",
        f"- max_drawdown: {float(s_strat['max_drawdown']):.6f}",
        f"- ulcer_index: {float(s_strat['ulcer_index']):.6f}",
        f"- recovery_time_days: {int(s_strat['recovery_time_days'])}",
        '',
        '## Benchmark',
        f"- total_return: {float(s_bench['total_return']):.6f}",
        f"- annualized_return: {float(s_bench['annualized_return']):.6f}",
        f"- max_drawdown: {float(s_bench['max_drawdown']):.6f}",
        '',
        '## Comparison',
        f"- cagr_impact: {cagr_impact:.6f}",
        f"- max_drawdown_reduction: {dd_reduction:.6f}",
        f"- gate_dd_reduction_ge_40pct: {summary['comparison']['acceptance_check']['dd_reduction_ge_40pct']}",
        f"- gate_cagr_impact_lt_2pct: {summary['comparison']['acceptance_check']['cagr_impact_lt_2pct']}",
    ]
    (out_dir / 'v30_backtest_report.md').write_text('\n'.join(lines), encoding='utf-8')

    if not bool(args.skip_csv_output):
        print(f"[OK] Wrote: {out_dir / 'v30_backtest_daily.csv'}")
        print(f"[OK] Wrote: {out_dir / 'v30_backtest_summary.csv'}")
    print(f"[OK] Wrote: {out_dir / 'summary.json'}")
    print(f"[OK] Wrote: {out_dir / 'v30_backtest_report.md'}")


if __name__ == '__main__':
    main()
