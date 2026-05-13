from dotenv import load_dotenv
load_dotenv()
import os, psycopg2, psycopg2.extras

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   os.environ.get("POSTGRES_DB", "gramercy_workstream1"),
    "user":     os.environ.get("POSTGRES_USER", "shankar_1"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

conn = get_conn()
print("Connected to", DB_CONFIG["dbname"])

import pandas as pd
from IPython.display import display
from datetime import date

BOUNDS = {
    "electricity_price":           (0.03, 0.60),    # USD/kWh
    "renewable_share":             (0.0,  100.0),   # %
    "grid_capacity":               (1.0,  5000.0),  # GW
    "reserve_margin":              (5.0,  60.0),    # %
    "energy_investment":           (0.1,  2000.0),  # USD bn
    "interconnection_queue_depth": (100,  3000000), # MW
}

df = pd.read_sql("SELECT * FROM v_si1_latest", conn)

anomalies = []
for _, row in df.iterrows():
    lo, hi = BOUNDS.get(row["metric_key"], (None, None))
    if lo is not None and not (lo <= row["metric_value"] <= hi):
        anomalies.append({
            "country_iso":  row["country_iso"],
            "metric_key":   row["metric_key"],
            "metric_value": row["metric_value"],
            "unit":         row["unit"],
            "bound_lo":     lo,
            "bound_hi":     hi,
            "source_name":  row["source_name"],
        })

if anomalies:
    print(f"\u26a0 ANOMALIES OUTSIDE EXPECTED BOUNDS ({len(anomalies)} rows):")
    display(pd.DataFrame(anomalies))
else:
    print("\u2713 All values within expected bounds.")

df_fresh = pd.read_sql("""
    SELECT country_iso, metric_key, data_date,
           CURRENT_DATE - data_date AS days_old
    FROM v_si1_latest
    ORDER BY days_old DESC
""", conn)

stale = df_fresh[df_fresh["days_old"] > 90]
print(f"\nDATA FRESHNESS (threshold: 90 days)")
print(f"  Total rows: {len(df_fresh)}")
print(f"  Stale (>90 days): {len(stale)}")
if not stale.empty:
    print("\n\u26a0 STALE DATA:")
    display(stale)
else:
    print("\u2713 All data is fresh.")

df_all = pd.read_sql("""
    SELECT access_method, confidence_score, count(*) AS n
    FROM si1_raw_metrics
    GROUP BY access_method, confidence_score
    ORDER BY confidence_score DESC
""", conn)

print("\nCONFIDENCE SCORE DISTRIBUTION:")
display(df_all)

summary = pd.read_sql("""
    SELECT ROUND(AVG(confidence_score)::numeric, 3)  AS avg_conf,
           ROUND(MIN(confidence_score)::numeric, 3)  AS min_conf,
           COUNT(*)                                   AS total_rows,
           COUNT(DISTINCT (country_iso, metric_key))  AS country_metric_pairs
    FROM si1_raw_metrics
""", conn)
display(summary)

conn.close()
print("Verification complete.")
