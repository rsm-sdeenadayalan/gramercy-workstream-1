"""
SI3 Verify
==========
Sanity-checks the `subindex_3` database after a pipeline run:
  - Share metrics (production_share, reserves_share, refining_share, value_add_ratio)
    must lie in [0, 1]
  - YoY growth must be a finite number (no inf / nan after store)
  - Confidence-score distribution
  - Rows with EST flag (lower-confidence USGS estimates) summary
  - Freshness: any rows older than 400 days flagged

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
    "dbname":   "subindex_3",
    "user":     os.environ.get("SI3_POSTGRES_USER",     os.environ.get("POSTGRES_USER", "")),
    "password": os.environ.get("SI3_POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")),
}

# Share-style metrics must be in [0, 1]; YoY growth is unbounded but should be finite.
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
        SELECT c.country_name, mn.mineral_name, md.metric_code,
               a.year, a.value, a.flag
          FROM si3_annual_metrics a
          JOIN si3_countries          c  ON c.id  = a.country_id
          JOIN si3_minerals           mn ON mn.id = a.mineral_id
          JOIN si3_metric_definitions md ON md.id = a.metric_id
         WHERE a.value IS NOT NULL
    """, conn)

    if df.empty:
        print("No annual metrics — run si3_pipeline.py first.")
        return

    anomalies = []
    for _, row in df.iterrows():
        v = row["value"]
        code = row["metric_code"]
        if code in BOUNDED_METRICS:
            if not (-1e-9 <= v <= 1.0 + 1e-9):
                anomalies.append({
                    "country": row["country_name"], "mineral": row["mineral_name"],
                    "metric": code, "year": row["year"], "value": v,
                    "issue": "out of [0, 1]",
                })
        # YoY growth: just ensure it's not absurdly extreme (>10x change unlikely)
        if code == "yoy_growth":
            if abs(v) > 10:
                anomalies.append({
                    "country": row["country_name"], "mineral": row["mineral_name"],
                    "metric": code, "year": row["year"], "value": v,
                    "issue": "|growth| > 1000% (suspicious)",
                })

    if anomalies:
        print(f"\n⚠ VALUE ANOMALIES ({len(anomalies)})")
        print(pd.DataFrame(anomalies).to_string(index=False))
    else:
        print(f"\n✓ All {len(df)} stored values within plausible bounds.")


def check_confidence(conn):
    df = pd.read_sql("""
        SELECT (raw_payload->>'access_method')   AS access_method,
               (raw_payload->>'confidence_score')::numeric AS confidence_score,
               COUNT(*) AS n
          FROM si3_raw_metrics
         WHERE ingestion_status = 'transformed'
         GROUP BY access_method, confidence_score
         ORDER BY confidence_score DESC NULLS LAST
    """, conn)
    _print_df("CONFIDENCE SCORE DISTRIBUTION", df)

    summary = pd.read_sql("""
        SELECT ROUND(AVG((raw_payload->>'confidence_score')::numeric), 3) AS avg_conf,
               ROUND(MIN((raw_payload->>'confidence_score')::numeric), 3) AS min_conf,
               COUNT(*) AS total_rows
          FROM si3_raw_metrics
         WHERE ingestion_status = 'transformed'
    """, conn)
    _print_df("SUMMARY", summary)


def check_est_flags(conn):
    df = pd.read_sql("""
        SELECT c.country_name, mn.mineral_name, md.metric_code,
               a.year, a.value, a.flag
          FROM si3_annual_metrics a
          JOIN si3_countries          c  ON c.id  = a.country_id
          JOIN si3_minerals           mn ON mn.id = a.mineral_id
          JOIN si3_metric_definitions md ON md.id = a.metric_id
         WHERE a.flag = 'EST'
         ORDER BY c.country_name, mn.mineral_name, md.metric_code
    """, conn)
    if df.empty:
        print("\n✓ No EST-flagged rows.")
    else:
        _print_df(f"EST-FLAGGED ROWS ({len(df)}) — USGS estimates, lower confidence", df)


def check_freshness(conn):
    df = pd.read_sql("""
        SELECT c.country_name, mn.mineral_name, md.metric_code,
               a.year, a.transformed_at::date AS transformed_on,
               (CURRENT_DATE - a.transformed_at::date) AS days_old
          FROM si3_annual_metrics a
          JOIN si3_countries          c  ON c.id  = a.country_id
          JOIN si3_minerals           mn ON mn.id = a.mineral_id
          JOIN si3_metric_definitions md ON md.id = a.metric_id
         WHERE (CURRENT_DATE - a.transformed_at::date) > 400
         ORDER BY days_old DESC
    """, conn)
    if df.empty:
        print("\n✓ All values transformed within the last 400 days.")
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
