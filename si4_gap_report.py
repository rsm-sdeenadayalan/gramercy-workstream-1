"""
SI4 Gap Report
==============
Queries the `subindex_4` database to display open data gaps, coverage
completeness, and latest collected values for the food sub-index.

Run after at least one pipeline run:
    python si4_gap_report.py
"""

from dotenv import load_dotenv
load_dotenv()

import os, psycopg2, psycopg2.extras
import pandas as pd

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   os.environ.get("POSTGRES_DB", "gramercy_workstream1"),
    "user":     os.environ.get("SI4_POSTGRES_USER",     os.environ.get("POSTGRES_USER", "shankar_1")),
    "password": os.environ.get("SI4_POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")),
}

COUNTRIES = {"US": "United States", "AE": "UAE", "BR": "Brazil",
             "IN": "India", "SG": "Singapore", "PH": "Philippines"}

METRICS = {
    "net_food_trade_balance":         "Net Food Trade Balance",
    "caloric_self_sufficiency_ratio": "Caloric Self-Sufficiency Ratio",
    "share_global_staple_exports":    "Share of Global Exports in Key Staples",
    "arable_land_per_capita":         "Arable Land per Capita",
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
    pd.set_option("display.width", 200)

    conn = get_conn()
    print(f"Connected to {DB_CONFIG['dbname']}")

    # ── Open gaps ─────────────────────────────────────────────────────────────
    df_gaps = pd.read_sql("""
        SELECT country_iso, country_name, metric_key, metric_label, severity,
               attempt_count, collectors_tried,
               LEFT(failure_reason, 150) AS failure_reason,
               first_detected::date AS first_detected,
               last_attempted::date AS last_attempted
        FROM si4_data_gaps
        WHERE status = 'open'
        ORDER BY severity DESC, country_iso, metric_key
    """, conn)
    _print_df(f"OPEN GAPS ({len(df_gaps)})", df_gaps)

    # ── Latest values: trade ─────────────────────────────────────────────────
    df_trade = pd.read_sql("""
        SELECT country_iso, metric_key,
               exports_usd, imports_usd, trade_balance_usd,
               data_date, data_frequency, source_name, confidence_score
        FROM v_si4_trade_latest
        ORDER BY country_iso, metric_key
    """, conn)
    _print_df("LATEST TRADE VALUES", df_trade)

    # ── Latest values: other metrics ─────────────────────────────────────────
    df_other = pd.read_sql("""
        SELECT country_iso, metric_key, metric_value, unit,
               data_date, data_frequency, source_name, confidence_score
        FROM v_si4_latest
        ORDER BY country_iso, metric_key
    """, conn)
    _print_df("LATEST METRIC VALUES", df_other)

    # ── Coverage completeness ─────────────────────────────────────────────────
    df_cov = pd.read_sql("""
        SELECT country_iso, metric_key, total_rows, last_collected, avg_confidence
        FROM v_si4_completeness
    """, conn)
    _print_df("COVERAGE", df_cov)

    # ── Collection runs (last 5) ─────────────────────────────────────────────
    df_runs = pd.read_sql("""
        SELECT run_id, started_at, finished_at,
               total_tasks, succeeded, failed, gaps_opened
        FROM si4_collection_runs
        ORDER BY started_at DESC
        LIMIT 5
    """, conn)
    _print_df("RECENT RUNS", df_runs)

    conn.close()


if __name__ == "__main__":
    main()
