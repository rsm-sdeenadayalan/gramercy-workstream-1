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

METRICS = {
    "electricity_price":           {"label": "Average Industrial Electricity Cost",           "unit": "USD/kWh"},
    "renewable_share":             {"label": "Renewable Share of Grid",                       "unit": "%"},
    "grid_capacity":               {"label": "Total Installed Grid Capacity",                  "unit": "GW"},
    "reserve_margin":              {"label": "Grid Reserve Margin",                            "unit": "%"},
    "energy_investment":           {"label": "Planned Energy Infrastructure Investment (5yr)", "unit": "USD bn"},
    "interconnection_queue_depth": {"label": "Grid Interconnection Queue Depth",               "unit": "MW"},
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

conn = get_conn()
print("Connected to", DB_CONFIG["dbname"])

import pandas as pd
from IPython.display import display

df_gaps = pd.read_sql("""
    SELECT
        country_iso,
        country_name,
        metric_key,
        metric_label,
        severity,
        attempt_count,
        collectors_tried,
        LEFT(failure_reason, 150) AS failure_reason,
        first_detected::date     AS first_detected,
        last_attempted::date     AS last_attempted
    FROM si1_data_gaps
    WHERE status = 'open'
    ORDER BY
        CASE severity
            WHEN 'critical' THEN 1
            WHEN 'high'     THEN 2
            WHEN 'medium'   THEN 3
            ELSE 4
        END,
        country_iso, metric_key
""", conn)

print(f"\nOPEN DATA GAPS: {len(df_gaps)}")
if df_gaps.empty:
    print("  ✓ No open gaps — all metrics collected successfully.")
else:
    display(df_gaps)

import pandas as pd
from IPython.display import display

df_comp = pd.read_sql("SELECT * FROM v_si1_completeness", conn)

if df_comp.empty:
    print("No data in si1_raw_metrics yet.")
else:
    pivot = df_comp.pivot_table(
        index="country_iso",
        columns="metric_key",
        values="avg_confidence",
        aggfunc="mean"
    )
    print("\nCOVERAGE — Average confidence score per country/metric")
    print("(— = no data collected yet)\n")
    try:
        styled = pivot.style.format("{:.2f}", na_rep="—").background_gradient(
            cmap="RdYlGn", vmin=0, vmax=1
        )
        display(styled)
    except Exception:
        display(pivot)

import pandas as pd
from IPython.display import display

df_latest = pd.read_sql("""
    SELECT
        country_iso,
        metric_key,
        metric_label,
        ROUND(metric_value::numeric, 4) AS metric_value,
        unit,
        data_date,
        data_frequency,
        source_name,
        ROUND(confidence_score::numeric, 2) AS confidence_score,
        collected_at::date AS collected_on
    FROM v_si1_latest
    ORDER BY country_iso, metric_key
""", conn)

print(f"\nLATEST VALUES: {len(df_latest)} rows\n")
display(df_latest)

# Flag low-confidence rows
low_conf = df_latest[df_latest["confidence_score"] < 0.50]
if not low_conf.empty:
    print(f"\n⚠  LOW CONFIDENCE (<0.50) — {len(low_conf)} row(s):")
    display(low_conf[["country_iso", "metric_key", "confidence_score", "source_name"]])
else:
    print("\n✓ All collected values have confidence ≥ 0.50")

import pandas as pd
from IPython.display import display

df_runs = pd.read_sql("""
    SELECT
        run_id,
        started_at::timestamp(0) AS started_at,
        ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 1) AS elapsed_s,
        total_tasks,
        succeeded,
        failed,
        gaps_opened,
        ROUND(100.0 * succeeded / NULLIF(total_tasks, 0)::numeric, 1) AS success_rate_pct
    FROM si1_collection_runs
    ORDER BY started_at DESC
    LIMIT 10
""", conn)

print("\nRECENT PIPELINE RUNS (last 10):")
display(df_runs)

conn.close()
print("Done.")
