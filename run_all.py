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
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

PYTHON = str(Path(__file__).parent / ".venv" / "bin" / "python")
ROOT   = Path(__file__).parent

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
        "script":      ROOT / "subindex3-pipeline-main" / "si3_pipeline.py",
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

def run_pipeline(name: str, config: dict) -> dict:
    """Run one pipeline as a subprocess, streaming output live with a prefix."""
    label  = config["label"]
    script = config["script"]
    log    = config["log"]
    prefix = f"[{name.upper()}]"

    with _print_lock:
        print(f"\n{prefix} Starting {label}...")

    t0      = time.perf_counter()
    output  = []

    proc = subprocess.Popen(
        [PYTHON, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(ROOT),
    )

    with open(log, "w") as lf:
        for line in proc.stdout:
            line = line.rstrip()
            output.append(line)
            lf.write(line + "\n")
            with _print_lock:
                print(f"  {prefix} {line}")

    proc.wait()
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
        "failed":    failed.strip(),
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

    wall_start = time.perf_counter()
    results    = []

    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        futures = {
            ex.submit(run_pipeline, name, cfg): name
            for name, cfg in targets.items()
        }
        for future in as_completed(futures):
            result = future.result()
            status = "✓" if result["ok"] else "✗"
            print(f"\n  [{status}] {result['label']} done in {result['elapsed']:.0f}s")
            results.append(result)

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
