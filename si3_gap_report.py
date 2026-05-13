"""
SI3 Gap Report
==============
Queries the `subindex_3` database to display open data gaps, latest values,
and coverage completeness for the critical-mineral sub-index.

Run after at least one pipeline run:
    python si3_gap_report.py
"""

from dotenv import load_dotenv
load_dotenv()

import os, psycopg2
import pandas as pd

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   os.environ.get("POSTGRES_DB", "gramercy_workstream1"),
    "user":     os.environ.get("SI3_POSTGRES_USER",     os.environ.get("POSTGRES_USER", "")),
    "password": os.environ.get("SI3_POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def _print_df(label: str, df: pd.DataFrame):
    print(f"\n── {label} ──")
    if df.empty:
        print("  (no rows)")
    else:
        print(df.to_string(index=False))


def main():
    pd.set_option("display.max_colwidth", 60)
    pd.set_option("display.width", 220)

    conn = get_conn()
    print(f"Connected to {DB_CONFIG['dbname']}")

    # ── Open gaps ─────────────────────────────────────────────────────────────
    df_gaps = pd.read_sql("""
        SELECT c.country_name, mn.mineral_name, md.metric_code,
               g.period_start, g.gap_type, g.severity,
               g.detected_at::date AS detected_at,
               LEFT(g.notes, 120) AS notes
          FROM si3_data_gaps g
          JOIN si3_countries          c  ON c.id  = g.country_id
          JOIN si3_minerals           mn ON mn.id = g.mineral_id
          JOIN si3_metric_definitions md ON md.id = g.metric_id
         WHERE NOT g.is_resolved
         ORDER BY g.severity DESC, c.country_name, mn.mineral_name, md.metric_code
    """, conn)
    _print_df(f"OPEN GAPS ({len(df_gaps)})", df_gaps)

    # ── Latest values per (country, mineral, metric) ─────────────────────────
    df_latest = pd.read_sql("""
        SELECT country_name, mineral_name, metric_code,
               granularity, latest_period, value, unit, flag
          FROM v_si3_latest
         ORDER BY country_name, mineral_name, metric_code
    """, conn)
    _print_df(f"LATEST VALUES ({len(df_latest)})", df_latest)

    # ── Coverage completeness ─────────────────────────────────────────────────
    df_cov = pd.read_sql("""
        SELECT country_name, mineral_name, metric_code,
               granularity, n_filled, has_any_data
          FROM v_si3_completeness
         ORDER BY country_name, mineral_name, metric_code
    """, conn)
    cells = len(df_cov)
    filled = int(df_cov["has_any_data"].sum()) if not df_cov.empty else 0
    print(f"\n── COVERAGE: {filled}/{cells} cells have ≥1 value "
          f"({100*filled/max(cells,1):.1f}%) ──")
    if not df_cov.empty:
        # only show rows missing data for brevity
        missing = df_cov[df_cov["has_any_data"] == 0]
        _print_df(f"MISSING CELLS ({len(missing)})", missing)

    # ── Recent runs ───────────────────────────────────────────────────────────
    df_runs = pd.read_sql("""
        SELECT run_id, pipeline_name, source_name,
               started_at, finished_at, duration, status,
               rows_attempted, rows_succeeded, rows_failed, pct_succeeded
          FROM v_si3_recent_runs
         LIMIT 10
    """, conn)
    _print_df("RECENT RUNS (last 30 days, top 10)", df_runs)

    conn.close()


if __name__ == "__main__":
    main()
