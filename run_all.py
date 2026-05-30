"""
Run all subindex pipelines in parallel.

Each pipeline runs as an independent process — separate DB connections,
separate API calls, no interference. Total time = max(SI1, SI2, ...).

Usage:
    python run_all.py              # run all
    python run_all.py --only si1   # run SI1 only
    python run_all.py --only si2   # run SI2 only
"""

import argparse
import os as _os_early
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# Watchdog: kill a child pipeline if it produces no output for this many seconds.
# Each subprocess logs progress at every API call, so true silence ≫ a few
# minutes means it's hung (typically on a socket the OS won't unblock).
PIPELINE_IDLE_TIMEOUT_S = int(_os_early.environ.get("WS1_PIPELINE_IDLE_TIMEOUT", "600"))

from dotenv import load_dotenv
load_dotenv()
import os
import psycopg2

PYTHON = sys.executable
ROOT   = Path(__file__).parent

_DB = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   os.environ.get("POSTGRES_DB", "gramercy_workstream1"),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

_VIEWS_SQL = """
-- ORDERING POLICY: confidence_score DESC first, then data_date DESC, then
-- collected_at DESC. The cascade always runs the research agent AFTER the
-- canonical API collector, so without this ordering the agent (typically
-- conf 0.55-0.65) would override canonical sources (IRENA 0.85, EIA 1.0,
-- FAOSTAT 0.75) just because its row is more recent. Confidence-first
-- preserves canonical authority while keeping the agent's row in DB as a
-- cross-check (queryable via si*_raw_metrics directly).

CREATE OR REPLACE VIEW v_si1_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key, metric_label,
    metric_value, unit, data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si1_raw_metrics
ORDER BY country_iso, metric_key, confidence_score DESC NULLS LAST,
         data_date DESC, collected_at DESC;

CREATE OR REPLACE VIEW v_si2_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key, metric_label,
    metric_value, unit, data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si2_raw_metrics
ORDER BY country_iso, metric_key, confidence_score DESC NULLS LAST,
         data_date DESC, collected_at DESC;

CREATE OR REPLACE VIEW v_si4_trade_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key,
    exports_usd, imports_usd, trade_balance_usd,
    data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si4_food_trade_raw
ORDER BY country_iso, metric_key, confidence_score DESC NULLS LAST,
         data_date DESC, collected_at DESC;

CREATE OR REPLACE VIEW v_si4_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key, metric_label,
    metric_value, unit, data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si4_raw_metrics
ORDER BY country_iso, metric_key, confidence_score DESC NULLS LAST,
         data_date DESC, collected_at DESC;
"""

def _apply_views():
    """Ensure all *_latest views pick the row with the newest data_date."""
    try:
        conn = psycopg2.connect(**_DB)
        with conn.cursor() as cur:
            cur.execute(_VIEWS_SQL)
        conn.commit()
        conn.close()
        print("  ✓ Views updated (data_date priority).")
    except Exception as e:
        print(f"  ⚠ Could not update views: {e}")

PIPELINES = {
    "si1": {
        "script":      ROOT / "si1_pipeline.py",
        "label":       "SI1 — Energy Metrics",
        "log":         ROOT / "si1_run.log",
    },
    "si2": {
        "script":      ROOT / "si2_pipeline.py",
        "label":       "SI2 — Water Availability",
        "log":         ROOT / "si2_run.log",
    },
    "si3": {
        "script":      ROOT / "si3_pipeline.py",
        "label":       "SI3 — Critical Mineral Endowment",
        "log":         ROOT / "si3_run.log",
    },
    "si4": {
        "script":      ROOT / "si4_pipeline.py",
        "label":       "SI4 — Food Sub-Index",
        "log":         ROOT / "si4_run.log",
    },
}


import threading
_print_lock = threading.Lock()

def _terminate_group(proc: subprocess.Popen, grace_s: float = 5.0) -> None:
    """Send SIGTERM to the subprocess's whole process group, then SIGKILL after
    `grace_s` if it doesn't exit. Using the group ensures any helper processes
    the pipeline spawned (Claude/Tavily clients with their own workers) die too."""
    if proc.poll() is not None:
        return
    try:
        pgid = _os_early.getpgid(proc.pid)
        _os_early.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    try:
        proc.wait(timeout=grace_s)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        _os_early.killpg(_os_early.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def run_pipeline(name: str, config: dict) -> dict:
    """Run one pipeline as a subprocess. Streams output live with a prefix,
    enforces a per-pipeline idle-timeout watchdog, and reaps the whole process
    group on abort so no zombies outlive the orchestrator."""
    label  = config["label"]
    script = config["script"]
    log    = config["log"]
    prefix = f"[{name.upper()}]"

    with _print_lock:
        print(f"\n{prefix} Starting {label}...")

    t0      = time.perf_counter()
    output  = []
    timed_out = False

    # `-u` = unbuffered child stdout (otherwise child writes hang in libc buffers
    # and the orchestrator sees nothing until the process exits).
    # `start_new_session=True` puts the child in its own process group so we can
    # signal the whole group, including any sub-subprocesses it spawned.
    proc = subprocess.Popen(
        [PYTHON, "-u", str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(ROOT),
        start_new_session=True,
        bufsize=1,  # line-buffered on the parent side
    )

    last_activity = [time.monotonic()]
    watchdog_stop = [False]

    def _watchdog():
        while not watchdog_stop[0] and proc.poll() is None:
            idle = time.monotonic() - last_activity[0]
            if idle > PIPELINE_IDLE_TIMEOUT_S:
                with _print_lock:
                    print(f"  {prefix} ⚠ Watchdog: no output for {int(idle)}s "
                          f"(> {PIPELINE_IDLE_TIMEOUT_S}s) — killing process group.")
                _terminate_group(proc)
                return
            time.sleep(5)

    import threading as _t
    wd = _t.Thread(target=_watchdog, name=f"watchdog-{name}", daemon=True)
    wd.start()

    try:
        with open(log, "w") as lf:
            for line in proc.stdout:
                last_activity[0] = time.monotonic()
                line = line.rstrip()
                output.append(line)
                lf.write(line + "\n")
                lf.flush()
                with _print_lock:
                    print(f"  {prefix} {line}")
    except KeyboardInterrupt:
        with _print_lock:
            print(f"  {prefix} Interrupted — terminating process group.")
        _terminate_group(proc)
        raise
    finally:
        watchdog_stop[0] = True
        proc.wait()
        # Belt-and-suspenders: if the child exited but its group still has
        # stragglers (unlikely but cheap), clean them up.
        _terminate_group(proc, grace_s=1.0)

    # If watchdog killed us, the loop above terminated mid-stream — flag it.
    if proc.returncode != 0 and any("Watchdog" in l for l in output[-5:]):
        timed_out = True

    elapsed = time.perf_counter() - t0

    succeeded = next((l for l in output if "Succeeded" in l), "")
    failed    = next((l for l in output if "Failed"    in l), "")
    cost_line = next((l for l in output if "Estimated cost" in l), "")
    run_id    = next((l for l in output if "run_id" in l.lower() or "Run ID" in l), "")

    return {
        "name":      name,
        "label":     label,
        "ok":        proc.returncode == 0,
        "elapsed":   elapsed,
        "succeeded": succeeded.strip(),
        "failed":    failed.strip() or ("(watchdog killed: idle > "
                                        f"{PIPELINE_IDLE_TIMEOUT_S}s)" if timed_out else ""),
        "cost":      cost_line.strip(),
        "run_id":    run_id.strip(),
        "log":       log,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=list(PIPELINES.keys()),
                        help="Run only one pipeline")
    args = parser.parse_args()

    targets = {args.only: PIPELINES[args.only]} if args.only else PIPELINES

    print(f"\n{'='*60}")
    print(f"  Gramercy Sub-Index Pipeline Runner")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Running: {', '.join(t.upper() for t in targets)}")
    print(f"{'='*60}\n")

    _apply_views()

    wall_start = time.perf_counter()
    results    = []

    try:
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {
                ex.submit(run_pipeline, name, cfg): name
                for name, cfg in targets.items()
            }
            for future in as_completed(futures):
                result = future.result()
                status = "✓" if result["ok"] else "✗"
                print(f"\n  [{status}] {result['label']} done in {result['elapsed']:.0f}s")
                results.append(result)
    except KeyboardInterrupt:
        # Re-raise after letting the per-pipeline finally blocks clean up.
        print("\n  ⚠ Interrupted — children terminated.")
        sys.exit(130)

    wall_elapsed = time.perf_counter() - wall_start

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  All pipelines complete  ({wall_elapsed:.0f}s wall time)")
    print(f"{'='*60}")
    for r in sorted(results, key=lambda x: x["name"]):
        status = "✓" if r["ok"] else "✗ FAILED"
        print(f"\n  {r['label']}  [{status}]  ({r['elapsed']:.0f}s)")
        for line in [r["run_id"], r["succeeded"], r["failed"], r["cost"]]:
            if line:
                print(f"    {line}")
        print(f"    Log: {r['log']}")
    print(f"\n{'='*60}\n")

    if any(not r["ok"] for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
