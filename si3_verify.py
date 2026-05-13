"""
SI3 Verify
==========
Sanity-checks si3_pipeline_metrics after a pipeline run:
  - Share metrics must lie in [0, 1]
  - YoY growth must be finite
  - Confidence-score distribution
  - EST-flagged rows summary
  - Freshness: rows older than 400 days flagged

Run:
    python si3_verify.py
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

BOUNDED_METRICS = {"production_share", "reserves_share", "refining_share", "value_add_ratio"}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def _print_df(label: str, df: pd.DataFrame):
    print(f"\n── {label} ──")
    if df.empty:
        print("  (no rows)")
    else:
        print(df.to_string(index=False))


def check_bounds(conn):
    df = pd.read_sql("""
        SELECT country_name, mineral, metric_key, data_date, metric_value, flag
          FROM si3_pipeline_metrics
         WHERE metric_value IS NOT NULL
    """, conn)

    if df.empty:
        print("No metrics stored — run si3_pipeline.py first.")
        return

    anomalies = []
    for _, row in df.iterrows():
        v = float(row["metric_value"])
        code = row["metric_key"]
        if code in BOUNDED_METRICS:
            if not (-1e-9 <= v <= 1.0 + 1e-9):
                anomalies.append({
                    "country": row["country_name"], "mineral": row["mineral"],
                    "metric": code, "date": row["data_date"], "value": v,
                    "issue": "out of [0, 1]",
                })
        if code == "yoy_growth" and abs(v) > 10:
            anomalies.append({
                "country": row["country_name"], "mineral": row["mineral"],
                "metric": code, "date": row["data_date"], "value": v,
                "issue": "|growth| > 1000% (suspicious)",
            })

    if anomalies:
        print(f"\n⚠ VALUE ANOMALIES ({len(anomalies)})")
        print(pd.DataFrame(anomalies).to_string(index=False))
    else:
        print(f"\n✓ All {len(df)} stored values within plausible bounds.")


def check_confidence(conn):
    df = pd.read_sql("""
        SELECT access_method, ROUND(AVG(confidence_score)::numeric, 3) AS avg_conf,
               COUNT(*) AS n
          FROM si3_pipeline_metrics
         GROUP BY access_method
         ORDER BY avg_conf DESC NULLS LAST
    """, conn)
    _print_df("CONFIDENCE SCORE BY ACCESS METHOD", df)

    summary = pd.read_sql("""
        SELECT ROUND(AVG(confidence_score)::numeric, 3) AS avg_conf,
               ROUND(MIN(confidence_score)::numeric, 3) AS min_conf,
               COUNT(*) AS total_rows,
               SUM(CASE WHEN is_imputed THEN 1 ELSE 0 END) AS imputed_rows
          FROM si3_pipeline_metrics
    """, conn)
    _print_df("SUMMARY", summary)


def check_est_flags(conn):
    df = pd.read_sql("""
        SELECT country_name, mineral, metric_key, data_date, metric_value, flag
          FROM si3_pipeline_metrics
         WHERE flag = 'EST'
         ORDER BY country_name, mineral, metric_key
    """, conn)
    if df.empty:
        print("\n✓ No EST-flagged rows.")
    else:
        _print_df(f"EST-FLAGGED ROWS ({len(df)}) — lower-confidence estimates", df)


def check_freshness(conn):
    df = pd.read_sql("""
        SELECT country_name, mineral, metric_key,
               collected_at::date AS collected_on,
               (CURRENT_DATE - collected_at::date) AS days_old
          FROM si3_pipeline_metrics
         WHERE (CURRENT_DATE - collected_at::date) > 400
         ORDER BY days_old DESC
    """, conn)
    if df.empty:
        print("\n✓ All values collected within the last 400 days.")
    else:
        _print_df(f"STALE VALUES (>400 days, {len(df)})", df)


def main():
    pd.set_option("display.max_colwidth", 60)
    pd.set_option("display.width", 220)

    conn = get_conn()
    print(f"Connected to {DB_CONFIG['dbname']}")
    check_bounds(conn)
    check_confidence(conn)
    check_est_flags(conn)
    check_freshness(conn)
    conn.close()
    print("\nVerification complete.")


if __name__ == "__main__":
    main()
