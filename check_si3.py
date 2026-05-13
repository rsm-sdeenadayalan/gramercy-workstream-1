"""Quick diagnostic — show what's actually stored in si3_pipeline_metrics."""
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", 5433)),
    dbname=os.environ.get("POSTGRES_DB", "gramercy_workstream1"),
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)

print("\n=== Row counts in si3_pipeline_metrics by metric_key ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT metric_key, COUNT(*) AS n
          FROM si3_pipeline_metrics
         GROUP BY metric_key
         ORDER BY n DESC
    """)
    rows = cur.fetchall()
    if not rows:
        print("  (no rows in si3_pipeline_metrics — run si3_pipeline.py first)")
    for code, n in rows:
        print(f"  {code:<25} {n}")

print("\n=== Row counts by country × metric ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT country_name, metric_key, mineral, COUNT(*) AS n
          FROM si3_pipeline_metrics
         GROUP BY country_name, metric_key, mineral
         ORDER BY country_name, metric_key, mineral
    """)
    for cty, code, mineral, n in cur.fetchall():
        print(f"  {cty:<15} {code:<25} {mineral:<15} {n}")

print("\n=== Latest values ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT DISTINCT ON (country_iso, metric_key, mineral)
               country_iso, metric_key, mineral,
               metric_value, unit, data_date, confidence_score, is_imputed
          FROM si3_pipeline_metrics
         ORDER BY country_iso, metric_key, mineral, collected_at DESC
    """)
    for row in cur.fetchall():
        iso, mk, min_, val, unit, dt, conf, imp = row
        imputed = " [IMPUTED]" if imp else ""
        print(f"  {iso} {mk:<25} {min_:<15} {val:.4f} {unit} ({dt}){imputed}")

conn.close()
