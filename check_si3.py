"""Quick diagnostic — show what's actually stored in subindex_3."""
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", 5433)),
    dbname="subindex_3",
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)

print("\n=== Row counts in si3_annual_metrics by metric_code ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT md.metric_code, COUNT(*) AS n
          FROM si3_annual_metrics a
          JOIN si3_metric_definitions md ON md.id = a.metric_id
         GROUP BY md.metric_code
         ORDER BY n DESC
    """)
    rows = cur.fetchall()
    if not rows:
        print("  (no rows in si3_annual_metrics)")
    for code, n in rows:
        print(f"  {code:<25} {n}")

print("\n=== Row counts by country × metric ===")
with conn.cursor() as cur:
    cur.execute("""
        SELECT c.country_name, md.metric_code, COUNT(*) AS n
          FROM si3_annual_metrics a
          JOIN si3_countries          c  ON c.id  = a.country_id
          JOIN si3_metric_definitions md ON md.id = a.metric_id
         GROUP BY c.country_name, md.metric_code
         ORDER BY c.country_name, md.metric_code
    """)
    for cty, code, n in cur.fetchall():
        print(f"  {cty:<15} {code:<25} {n}")

print("\n=== Dimension table sanity ===")
with conn.cursor() as cur:
    for tbl in ("si3_countries", "si3_minerals", "si3_metric_definitions"):
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"  {tbl:<30} {cur.fetchone()[0]} rows")

conn.close()
