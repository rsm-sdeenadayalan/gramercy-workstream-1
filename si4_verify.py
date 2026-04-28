"""
SI4 Verify
==========
Sanity-checks the `subindex_4` database after a pipeline run:
  - Trade values within plausible USD bounds
  - Exports/imports positive, balance == exports - imports
  - Data freshness vs frequency-aware thresholds
  - Confidence-score distribution
  - Null trade-balance audit (rows where one side is missing)

Run:
    python si4_verify.py
"""

from dotenv import load_dotenv
load_dotenv()

import os, psycopg2, psycopg2.extras
import pandas as pd

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   "subindex_4",
    "user":     os.environ.get("SI4_POSTGRES_USER",     os.environ.get("POSTGRES_USER", "shankar_1")),
    "password": os.environ.get("SI4_POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")),
}

# Lower bounds: smallest plausible monthly food export/import for any of our 6 countries.
# Upper bounds: largest plausible (US annual agri exports ~200 bn USD).
# Balance is symmetric around zero.
BOUNDS_TRADE = {
    "exports_usd":       (0,                  250_000_000_000),
    "imports_usd":       (0,                  250_000_000_000),
    "trade_balance_usd": (-200_000_000_000,   200_000_000_000),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def _print_df(label: str, df: pd.DataFrame):
    print(f"\n── {label} ──")
    if df.empty:
        print("  (no rows)")
    else:
        print(df.to_string(index=False))


def check_trade_bounds(conn):
    df = pd.read_sql("""
        SELECT country_iso, metric_key,
               exports_usd, imports_usd, trade_balance_usd,
               data_date, source_name, confidence_score
        FROM v_si4_trade_latest
    """, conn)

    if df.empty:
        print("No trade data — run si4_pipeline.py first.")
        return

    anomalies = []
    for _, row in df.iterrows():
        for field, (lo, hi) in BOUNDS_TRADE.items():
            val = row.get(field)
            if val is None or pd.isna(val):
                continue
            if not (lo <= val <= hi):
                anomalies.append({
                    "country_iso": row["country_iso"],
                    "metric_key":  row["metric_key"],
                    "field":       field,
                    "value":       val,
                    "bound_lo":    lo,
                    "bound_hi":    hi,
                    "source_name": row["source_name"],
                })

    if anomalies:
        print(f"\n⚠ TRADE VALUE ANOMALIES OUTSIDE EXPECTED BOUNDS ({len(anomalies)})")
        print(pd.DataFrame(anomalies).to_string(index=False))
    else:
        print("\n✓ All trade values within expected bounds.")

    print("\nSANITY CHECKS:")
    for _, row in df.iterrows():
        iso = row["country_iso"]
        exp, imp, bal = row.get("exports_usd"), row.get("imports_usd"), row.get("trade_balance_usd")
        issues = []
        if exp is not None and exp <= 0:
            issues.append(f"exports_usd={exp:,.0f} (should be >0)")
        if imp is not None and imp <= 0:
            issues.append(f"imports_usd={imp:,.0f} (should be >0)")
        if bal is not None and exp is not None and imp is not None:
            computed = exp - imp
            if abs(computed - bal) > 1.0:
                issues.append(f"balance mismatch: stored={bal:,.0f}, exp-imp={computed:,.0f}")
        if issues:
            print(f"  ⚠ [{iso}]: {'; '.join(issues)}")
        else:
            print(f"  ✓ [{iso}] OK")


def check_freshness(conn):
    df = pd.read_sql("""
        SELECT country_iso, metric_key, data_date, data_frequency,
               CURRENT_DATE - data_date AS days_old, source_name
        FROM v_si4_trade_latest
        ORDER BY days_old DESC
    """, conn)
    if df.empty:
        return

    def _threshold(freq):
        if freq == "annual":    return 400
        if freq == "quarterly": return 120
        return 90  # monthly / irregular

    df["threshold_days"] = df["data_frequency"].apply(_threshold)
    df["stale"]          = df["days_old"] > df["threshold_days"]
    stale = df[df["stale"]]
    print(f"\nFRESHNESS — total: {len(df)}, stale: {len(stale)}")
    if not stale.empty:
        _print_df("STALE DATA", stale[["country_iso","metric_key","data_date","data_frequency","days_old","threshold_days","source_name"]])


def check_confidence(conn):
    df = pd.read_sql("""
        SELECT access_method, confidence_score, COUNT(*) AS n
        FROM si4_food_trade_raw
        GROUP BY access_method, confidence_score
        ORDER BY confidence_score DESC
    """, conn)
    _print_df("CONFIDENCE SCORE DISTRIBUTION (si4_food_trade_raw)", df)

    summary = pd.read_sql("""
        SELECT ROUND(AVG(confidence_score)::numeric, 3) AS avg_conf,
               ROUND(MIN(confidence_score)::numeric, 3) AS min_conf,
               COUNT(*)                                  AS total_rows,
               COUNT(DISTINCT (country_iso, metric_key)) AS country_metric_pairs
        FROM si4_food_trade_raw
    """, conn)
    _print_df("SUMMARY", summary)


def check_null_balance(conn):
    df = pd.read_sql("""
        SELECT country_iso, metric_key, exports_usd, imports_usd,
               data_date, source_name
        FROM si4_food_trade_raw
        WHERE trade_balance_usd IS NULL
        ORDER BY country_iso, data_date DESC
    """, conn)
    print(f"\nNULL TRADE BALANCE AUDIT: {len(df)} row(s)")
    if df.empty:
        print("  ✓ All stored rows have a computable trade balance.")
    else:
        print("  These rows have NULL balance because one or both trade sides are missing.")
        print("  Per design: no interpolation. These rows will not be scored.")
        print(df.to_string(index=False))


def main():
    pd.set_option("display.max_colwidth", 60)
    pd.set_option("display.width", 200)

    conn = get_conn()
    print(f"Connected to {DB_CONFIG['dbname']}")
    check_trade_bounds(conn)
    check_freshness(conn)
    check_confidence(conn)
    check_null_balance(conn)
    conn.close()
    print("\nVerification complete.")


if __name__ == "__main__":
    main()
