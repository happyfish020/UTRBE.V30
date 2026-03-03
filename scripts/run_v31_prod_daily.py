from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TextIO
from zoneinfo import ZoneInfo



ROOT = Path(__file__).resolve().parents[1]
UTRBE_ROOT = (ROOT.parent / "UTRBE").resolve()
_ACTIVE_LOG_FH: TextIO | None = None
_RUNTIME_MODULES = (
    "pandas",
    "sqlalchemy",
    "pymysql",
    "joblib",
    "matplotlib",
    "numpy",
    "sklearn",
    "scipy",
    "yaml",
    "pydantic",
    "openpyxl",
)


def _log_line(msg: str) -> None:
    try:
        print(msg)
    except UnicodeEncodeError:
        safe = str(msg).encode("utf-8", errors="replace").decode("utf-8", errors="replace")
        print(safe.encode("ascii", errors="replace").decode("ascii", errors="replace"))
    if _ACTIVE_LOG_FH is not None:
        _ACTIVE_LOG_FH.write(f"{msg}\n")
        _ACTIVE_LOG_FH.flush()


def _run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> None:
    _log_line("[RUN] " + " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.stdout is not None:
        for line in proc.stdout:
            _log_line(line.rstrip("\r\n"))
    ret = proc.wait()
    if ret != 0:
        raise subprocess.CalledProcessError(ret, cmd)


def _pick_utrbe_python() -> str:
    env_override = str(os.getenv("UTRBE_PYTHON", "")).strip()
    if env_override:
        return env_override
    # Prefer the current interpreter (active venv) to keep dependency set consistent.
    if str(sys.executable).strip():
        return sys.executable
    cand = UTRBE_ROOT / ".venv" / "Scripts" / "python.exe"
    if cand.exists():
        return str(cand)
    return sys.executable


def _missing_modules(python_exe: str) -> list[str]:
    code = (
        "import importlib.util as u, json;"
        f"mods={list(_RUNTIME_MODULES)!r};"
        "print(json.dumps([m for m in mods if u.find_spec(m) is None]))"
    )
    proc = subprocess.run(
        [python_exe, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        return list(_RUNTIME_MODULES)
    try:
        return list(json.loads(proc.stdout.strip() or "[]"))
    except Exception:
        return list(_RUNTIME_MODULES)


def _preflight_runtime(python_exe: str) -> None:
    missing = _missing_modules(python_exe)
    if not missing:
        return
    pip_names = {
        "sqlalchemy": "SQLAlchemy",
        "pymysql": "PyMySQL",
        "sklearn": "scikit-learn",
        "yaml": "PyYAML",
    }
    install_list = [pip_names.get(m, m) for m in missing]
    install_cmd = f'"{python_exe}" -m pip install ' + " ".join(install_list)
    raise RuntimeError(
        "Missing runtime dependencies for selected interpreter.\n"
        f"python: {python_exe}\n"
        f"missing modules: {', '.join(missing)}\n"
        f"install command: {install_cmd}"
    )


def _last_trading_day(today: date | None = None) -> date:
    # Default to today's session date; on weekends fallback to previous Friday.
    d = (today or date.today())
    # Simple exchange-calendar fallback: weekend -> previous Friday.
    while d.weekday() >= 5:
        d = d - timedelta(days=1)
    return d


def _prev_business_day(d: date) -> date:
    x = d - timedelta(days=1)
    while x.weekday() >= 5:
        x = x - timedelta(days=1)
    return x


def _auto_default_trade_day(market_tz: str = "America/New_York", close_hhmm: str = "16:00") -> date:
    # No-arg mode behavior:
    # - before market close: previous trading day
    # - at/after market close: current trading day
    try:
        tz = ZoneInfo(market_tz)
    except Exception:
        tz = ZoneInfo("America/New_York")
    now_local = datetime.now(tz)
    hh, mm = (str(close_hhmm).split(":") + ["00"])[:2]
    try:
        close_minutes = int(hh) * 60 + int(mm)
    except Exception:
        close_minutes = 16 * 60

    d = now_local.date()
    if d.weekday() >= 5:
        return _last_trading_day(d)

    now_minutes = now_local.hour * 60 + now_local.minute
    if now_minutes < close_minutes:
        return _prev_business_day(d)
    return d


def main() -> None:
    global _ACTIVE_LOG_FH
    p = argparse.ArgumentParser(description="V31 production daily pipeline (DB -> V30 full chain).")
    p.add_argument("--start", default="2016-01-01", help="YYYY-MM-DD. If no args are provided, start=end=last trading day.")
    p.add_argument("--end", default="auto", help="YYYY-MM-DD or auto(last trading day).")
    p.add_argument("--utrbe-output-dir", default=str((ROOT / "reports" / "utrbe_prod_daily").resolve()))
    p.add_argument("--utrbe-compare-output-dir", default=str((ROOT / "reports" / "utrbe_compare_prod_daily").resolve()))
    p.add_argument("--market-features-table", default="market_features_daily")
    p.add_argument("--skip-utrbe-refresh", action="store_true")
    p.add_argument("--breadth-csv", default="../UTRBE/output/full_2006_2026.csv")
    p.add_argument("--tuning-json", default="config/v31_phase_d_A_ma100_frozen_20260302.json")
    p.add_argument("--policy-name", default="A_ma100_frozen")
    p.add_argument("--retrain-models", action="store_true")
    p.add_argument("--artifact-dir", default="artifacts/v31_train_assets")
    p.add_argument("--struct-train-output-dir", default="")
    p.add_argument("--shock-train-output-dir", default="")
    args = p.parse_args()

    run_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_dir = ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"UTRBEV30_log_{run_stamp}.log"
    with log_path.open("w", encoding="utf-8-sig") as log_fh:
        _ACTIVE_LOG_FH = log_fh
        try:
            _log_line(f"[INFO] Run started: {datetime.now().isoformat(timespec='seconds')}")

            no_args_mode = len(sys.argv) == 1
            last_td = _auto_default_trade_day().isoformat()
            if no_args_mode:
                start = last_td
                end = last_td
                _log_line(
                    f"[INFO] No CLI args detected. Auto day rule (before 16:00 ET -> previous trading day, otherwise today). Using: {last_td}"
                )
            else:
                start = str(args.start)
                end = last_td if str(args.end).lower() == "auto" else str(args.end)
            utrbe_py = _pick_utrbe_python()
            _preflight_runtime(utrbe_py)

            market_features_table = str(args.market_features_table).strip() or "market_features_daily"
            v2_latest_state_json = ""
            artifact_dir = Path(str(args.artifact_dir)).resolve()
            struct_train_dir = (
                Path(str(args.struct_train_output_dir)).resolve()
                if str(args.struct_train_output_dir).strip()
                else (artifact_dir / "v30_structural_train").resolve()
            )
            shock_train_dir = (
                Path(str(args.shock_train_output_dir)).resolve()
                if str(args.shock_train_output_dir).strip()
                else (artifact_dir / "v30_shock_train_step4").resolve()
            )
            features_csv = (artifact_dir / "v30_features_daily.csv").resolve()
            features_meta_json = (artifact_dir / "v30_features_build_meta.json").resolve()
            struct_train_dir.mkdir(parents=True, exist_ok=True)
            shock_train_dir.mkdir(parents=True, exist_ok=True)
            features_csv.parent.mkdir(parents=True, exist_ok=True)

            # Build unified sentiment first (DB -> features -> DB upsert), so v2 can read from DB.
            _run(
                [
                    utrbe_py,
                    "scripts/run_v31_build_unified_sentiment.py",
                    "--start",
                    start,
                    "--end",
                    end,
                    "--output-dir",
                    "reports/v31_unified_sentiment",
                    "--table-name",
                    "sentiment_daily_unified",
                ],
                cwd=ROOT,
            )

            if not args.skip_utrbe_refresh:
                utrbe_env = os.environ.copy()
                py_path_parts = [str(UTRBE_ROOT), str(UTRBE_ROOT / "src")]
                if str(utrbe_env.get("PYTHONPATH", "")).strip():
                    py_path_parts.append(str(utrbe_env["PYTHONPATH"]))
                utrbe_env["PYTHONPATH"] = os.pathsep.join(py_path_parts)
                _run(
                    [
                        utrbe_py,
                        "scripts/run_daily_pipeline.py",
                        "--start",
                        start,
                        "--end",
                        end,
                        "--output-dir",
                        str(args.utrbe_output_dir),
                        "--compare-primary-tracks",
                        "--compare-output-dir",
                        str(args.utrbe_compare_output_dir),
                        "--market-features-table",
                        market_features_table,
                        "--unified-sentiment-table",
                        "sentiment_daily_unified",
                        "--sentiment-profile",
                        "plus",
                        "--plus-force-reduce-on-signal",
                        "--plus-force-reduce-min-prob",
                        "0.12",
                    ],
                    cwd=UTRBE_ROOT,
                    env=utrbe_env,
                )
                v2_latest_state_json = str(Path(args.utrbe_output_dir) / "latest_state.json")
            else:
                cand = Path(args.utrbe_output_dir) / "latest_state.json"
                if cand.exists():
                    v2_latest_state_json = str(cand)

            _run(
        [
            utrbe_py,
            "scripts/run_v30_build_features.py",
            "--input-table",
            market_features_table,
            "--input-start",
            start,
            "--input-end",
            end,
            "--breadth-csv",
            str(args.breadth_csv),
            "--breadth-value-col",
            "breadth",
            "--output-csv",
            str(features_csv),
            "--meta-json",
            str(features_meta_json),
            "--table-name",
            "v30_features_daily",
            "--skip-csv-output",
        ],
                cwd=ROOT,
            )
            _run(
        [
            utrbe_py,
            "scripts/run_v30_build_labels.py",
            "--features-table",
            "v30_features_daily",
            "--struct-table",
            "v30_structural_labels_daily",
            "--shock-table",
            "v30_shock_labels_daily",
            "--shock-adaptive-drop",
            "--shock-use-stress-override",
            "--skip-csv-output",
        ],
                cwd=ROOT,
            )

            struct_model = struct_train_dir / "structural_model.pkl"
            shock_model = shock_train_dir / "shock_model.pkl"
            if args.retrain_models or (not struct_model.exists()) or (not shock_model.exists()):
                _run(
            [
                utrbe_py,
                "scripts/run_v30_structural_train.py",
                "--features-csv",
                str(features_csv),
                "--features-table",
                "v30_features_daily",
                "--output-dir",
                str(struct_train_dir),
                "--calibration",
                "sigmoid",
                "--test-years",
                "2",
            ],
                    cwd=ROOT,
                )
                _run(
            [
                utrbe_py,
                "scripts/run_v30_shock_train.py",
                "--features-csv",
                str(features_csv),
                "--features-table",
                "v30_features_daily",
                "--output-dir",
                str(shock_train_dir),
                "--calibration",
                "sigmoid",
                "--test-years",
                "2",
                "--label-horizon-days",
                "7",
                "--label-early-share-threshold",
                "0.45",
                "--label-adaptive-drop",
                "--label-target-positive-rate",
                "0.10",
                "--label-min-drop-threshold",
                "0.010",
                "--label-max-drop-threshold",
                "0.10",
                "--label-use-stress-override",
                "--label-stress-gate",
                "0.40",
            ],
                    cwd=ROOT,
                )

            _run(
                [
                    utrbe_py,
                    "scripts/run_v30_full_infer.py",
                    "--features-table",
                    "v30_features_daily",
                    "--features-start",
                    start,
                    "--features-end",
                    end,
                    "--struct-model-pkl",
                    str(struct_model),
                    "--shock-model-pkl",
                    str(shock_model),
                    "--struct-output-csv",
                    str((struct_train_dir / "structural_full_predictions.csv").resolve()),
                    "--shock-output-csv",
                    str((shock_train_dir / "shock_full_predictions.csv").resolve()),
                    "--struct-output-table",
                    "v30_structural_full_predictions_daily",
                    "--shock-output-table",
                    "v30_shock_full_predictions_daily",
                    "--skip-csv-output",
                ],
                cwd=ROOT,
            )

            _run(
                [
                    utrbe_py,
                    "scripts/run_v30_risk_aggregate.py",
                    "--features-table",
                    "v30_features_daily",
                    "--struct-pred-table",
                    "v30_structural_full_predictions_daily",
                    "--shock-pred-table",
                    "v30_shock_full_predictions_daily",
                    "--tuning-json",
                    str(args.tuning_json),
                    "--output-dir",
                    "reports/v31_risk_aggregate_default_prod",
                    "--output-table",
                    "v31_daily_allocation",
                    "--data-start",
                    start,
                    "--data-end",
                    end,
                    "--v2-latest-state-json",
                    str(v2_latest_state_json),
                    "--lowfreq-summary-json",
                    "reports/v31_lowfreq_recovery_weekly/summary.json",
                    "--unified-sentiment-summary-json",
                    "reports/v31_unified_sentiment/summary.json",
                    "--skip-csv-output",
                ],
                cwd=ROOT,
            )
            _run(
                [
                    utrbe_py,
                    "scripts/run_v30_backtest_eval.py",
                    "--allocation-table",
                    "v31_daily_allocation",
                    "--output-dir",
                    "reports/v31_backtest_eval_default_prod",
                    "--execution-lag-days",
                    "1",
                    "--transaction-cost-bps",
                    "2",
                    "--output-daily-table",
                    "v30_backtest_daily",
                    "--data-start",
                    start,
                    "--data-end",
                    end,
                    "--skip-csv-output",
                ],
                cwd=ROOT,
            )

            _run(
                [
                    utrbe_py,
                    "scripts/run_v31_lowfreq_recovery.py",
                    "--daily-table",
                    "v30_backtest_daily",
                    "--start-date",
                    start,
                    "--end-date",
                    end,
                    "--freq",
                    "weekly",
                    "--output-dir",
                    "reports/v31_lowfreq_recovery_weekly",
                    "--summary-table",
                    "v31_lowfreq_recovery_summary",
                    "--events-table",
                    "v31_lowfreq_recovery_events",
                ],
                cwd=ROOT,
            )

            _run(
                [
                    utrbe_py,
                    "scripts/run_v31_ops_monitor.py",
                    "--backtest-summary-json",
                    "reports/v31_backtest_eval_default_prod/summary.json",
                    "--backtest-daily-table",
                    "v30_backtest_daily",
                    "--risk-summary-json",
                    "reports/v31_risk_aggregate_default_prod/summary.json",
                    "--allocation-table",
                    "v31_daily_allocation",
                    "--start-date",
                    start,
                    "--end-date",
                    end,
                    "--reference-summary-json",
                    "artifacts/baselines/v31_backtest_eval_hardgate_gatepass/summary.json",
                    "--lowfreq-summary-json",
                    "reports/v31_lowfreq_recovery_weekly/summary.json",
                    "--v2-latest-state-json",
                    str(v2_latest_state_json),
                    "--unified-sentiment-json",
                    "reports/v31_unified_sentiment/latest_unified_sentiment.json",
                    "--unified-sentiment-summary-json",
                    "reports/v31_unified_sentiment/summary.json",
                    "--policy-name",
                    str(args.policy_name),
                    "--policy-config-json",
                    str(args.tuning_json),
                    "--output-dir",
                    "reports/v31_ops_monitor",
                ],
                cwd=ROOT,
            )

            _log_line("[OK] V31 production daily pipeline completed.")
            _log_line("[OK] Outputs:")
            _log_line(f"  - reports/v31_ops_monitor/UTRBEV3_daily_report_{end}.md")
            _log_line("  - reports/v31_ops_monitor/strategy_120d.png")
            _log_line("  - reports/v31_ops_monitor/summary.json")
            _log_line(f"[OK] Log file: {log_path}")
        finally:
            _ACTIVE_LOG_FH = None


if __name__ == "__main__":
    main()
