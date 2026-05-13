"""
Chessboard Sovereign Index — Scoring Pipeline
=============================================

Reads the latest collected metrics from subindex_1..4 source databases,
applies min-max normalization (0-100) per metric across the 6 target
countries, applies inversions where specified, then composes:

    1. Sub-index scores  (weighted average of normalized metrics)
    2. SI3 per-mineral score → SI3 composite (AI-relevance weights)
    3. Final SDI = 0.35·SI1 + 0.20·SI2 + 0.30·SI3 + 0.15·SI4

All weights and inversions live in the `score_methodology`,
`score_mineral_weights`, and `score_subindex_weights` tables — tweak
those rows in the DB to change the formula, no code edit required.

DB: csi_scores  (same Postgres server as the four sub-index DBs).

Usage:
    python score_pipeline.py
"""

from dotenv import load_dotenv
load_dotenv()

import os
import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras


ROOT = Path(__file__).parent

DB_BASE = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}
GRAMERCY_DB = os.environ.get("POSTGRES_DB", "gramercy_workstream1")

COUNTRIES = ["US", "AE", "BR", "IN", "SG", "PH"]


def _conn(dbname):
    return psycopg2.connect(**{**DB_BASE, "dbname": dbname})


# ─────────────────────────────────────────────────────────────────────────────
# 1. Pull latest values from source DBs
# ─────────────────────────────────────────────────────────────────────────────

def fetch_si1() -> list[dict]:
    """SI1 latest values from subindex_1.v_si1_latest."""
    rows = []
    with _conn(GRAMERCY_DB) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, metric_key, metric_value, unit, data_date, confidence_score
              FROM v_si1_latest
        """)
        for cty, key, val, unit, dt, conf in cur.fetchall():
            rows.append(dict(
                country_iso=cty, sub_index="SI1", metric_key=key,
                mineral=None, raw_value=val, unit=unit, data_date=dt,
                confidence=conf, source_db=GRAMERCY_DB,
            ))
    return rows


def fetch_si2() -> list[dict]:
    rows = []
    with _conn(GRAMERCY_DB) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, metric_key, metric_value, unit, data_date, confidence_score
              FROM v_si2_latest
        """)
        for cty, key, val, unit, dt, conf in cur.fetchall():
            rows.append(dict(
                country_iso=cty, sub_index="SI2", metric_key=key,
                mineral=None, raw_value=val, unit=unit, data_date=dt,
                confidence=conf, source_db=GRAMERCY_DB,
            ))
    return rows


def fetch_si3() -> list[dict]:
    """SI3 latest values per (country, mineral, metric) from si3_pipeline_metrics."""
    rows = []
    with _conn(GRAMERCY_DB) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (country_iso, metric_key, mineral)
                country_iso, metric_key, mineral,
                metric_value, data_date, confidence_score
              FROM si3_pipeline_metrics
             WHERE metric_key IN ('production_share','reserves_share','refining_share')
             ORDER BY country_iso, metric_key, mineral, data_date DESC, collected_at DESC
        """)
        for cty, metric_code, mineral, value, dt, conf in cur.fetchall():
            rows.append(dict(
                country_iso=cty, sub_index="SI3", metric_key=metric_code,
                mineral=mineral, raw_value=value, unit="ratio", data_date=dt,
                confidence=conf, source_db=GRAMERCY_DB,
            ))
    return rows


def fetch_si4() -> list[dict]:
    """SI4 — pulls metric_value rows from v_si4_latest plus trade balance from v_si4_trade_latest."""
    rows = []
    with _conn(GRAMERCY_DB) as conn, conn.cursor() as cur:
        # Non-trade metrics
        cur.execute("""
            SELECT country_iso, metric_key, metric_value, unit, data_date, confidence_score
              FROM v_si4_latest
             WHERE metric_key IN ('caloric_self_sufficiency_ratio',
                                  'share_global_staple_exports',
                                  'arable_land_per_capita')
        """)
        for cty, key, val, unit, dt, conf in cur.fetchall():
            rows.append(dict(
                country_iso=cty, sub_index="SI4", metric_key=key,
                mineral=None, raw_value=val, unit=unit, data_date=dt,
                confidence=conf, source_db=GRAMERCY_DB,
            ))
        # Trade balance from the dedicated trade table
        cur.execute("""
            SELECT country_iso, trade_balance_usd, data_date, confidence_score
              FROM v_si4_trade_latest
             WHERE metric_key = 'net_food_trade_balance'
        """)
        for cty, bal, dt, conf in cur.fetchall():
            rows.append(dict(
                country_iso=cty, sub_index="SI4", metric_key="net_food_trade_balance",
                mineral=None, raw_value=bal, unit="USD", data_date=dt,
                confidence=conf, source_db=GRAMERCY_DB,
            ))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 2. Normalize, weight, store
# ─────────────────────────────────────────────────────────────────────────────

def _minmax(values, invert=False) -> dict:
    """Return {key: 0..100 normalized score}. Handles None, ties, single values."""
    valid = [(k, v) for k, v in values.items() if v is not None]
    if not valid:
        return {k: None for k in values}
    vs = [v for _, v in valid]
    lo, hi = min(vs), max(vs)
    out = {}
    for k, v in values.items():
        if v is None:
            out[k] = None
        elif hi == lo:
            out[k] = 50.0  # all equal → neutral midpoint
        else:
            score = (v - lo) / (hi - lo) * 100
            if invert:
                score = 100 - score
            out[k] = score
    return out


def load_methodology(scores_conn) -> tuple[dict, dict, dict]:
    """Return (metric_weights, mineral_weights, subindex_weights) from config tables."""
    metric_w = {}    # (sub_index, metric_key) → (weight, invert)
    mineral_w = {}
    subindex_w = {}
    with scores_conn.cursor() as cur:
        cur.execute("SELECT sub_index, metric_key, weight, invert FROM score_methodology")
        for si, mk, w, inv in cur.fetchall():
            metric_w[(si, mk)] = (float(w), inv)
        cur.execute("SELECT mineral, weight FROM score_mineral_weights")
        for m, w in cur.fetchall():
            mineral_w[m] = float(w)
        cur.execute("SELECT sub_index, weight FROM score_subindex_weights")
        for si, w in cur.fetchall():
            subindex_w[si] = float(w)
    return metric_w, mineral_w, subindex_w


def compute_and_store(run_uuid: str):
    print(f"\n[SCORE] Run {run_uuid}")
    print(f"[SCORE] Pulling latest values from source DBs…")
    inputs = []
    for fn, label in [(fetch_si1, "SI1"), (fetch_si2, "SI2"),
                      (fetch_si3, "SI3"), (fetch_si4, "SI4")]:
        try:
            r = fn()
            print(f"  ✓ {label}: {len(r)} rows pulled")
            inputs.extend(r)
        except Exception as e:
            print(f"  ✗ {label}: {e}")

    scores_conn = _conn(GRAMERCY_DB)
    try:
        # Wipe prior per-run tables (we always recompute from scratch)
        with scores_conn.cursor() as cur:
            cur.execute("TRUNCATE score_metric_inputs, score_metric_normalized, "
                        "score_mineral, score_subindex, score_sdi")
        scores_conn.commit()

        # Insert raw inputs
        with scores_conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO score_metric_inputs
                    (run_id, country_iso, sub_index, metric_key, mineral,
                     raw_value, unit, data_date, source_db, confidence)
                VALUES (%(run_id)s, %(country_iso)s, %(sub_index)s, %(metric_key)s,
                        %(mineral)s, %(raw_value)s, %(unit)s, %(data_date)s,
                        %(source_db)s, %(confidence)s)
                ON CONFLICT (country_iso, sub_index, metric_key, mineral) DO UPDATE SET
                    raw_value = EXCLUDED.raw_value,
                    pulled_at = NOW()
            """, [{**r, "run_id": run_uuid} for r in inputs])
        scores_conn.commit()

        metric_w, mineral_w, subindex_w = load_methodology(scores_conn)

        # ── 2a. Normalize each metric (or per-mineral metric) across the 6 countries
        print(f"[SCORE] Normalizing metrics (min-max across {len(COUNTRIES)} countries)…")
        normalized_rows = []

        # Group by (sub_index, metric_key, mineral) so each group has 6 country rows
        from collections import defaultdict
        groups = defaultdict(dict)
        for r in inputs:
            key = (r["sub_index"], r["metric_key"], r["mineral"])
            groups[key][r["country_iso"]] = r["raw_value"]

        for (si, mk, mineral), values in groups.items():
            # Ensure every country has a slot (None if missing)
            full = {c: values.get(c) for c in COUNTRIES}
            weight, invert = metric_w.get((si, mk), (0.0, False))
            normed = _minmax(full, invert=invert)
            for cty, n in normed.items():
                if n is None:
                    continue
                normalized_rows.append(dict(
                    run_id=run_uuid, country_iso=cty, sub_index=si,
                    metric_key=mk, mineral=mineral,
                    raw_value=full.get(cty),
                    normalized=round(n, 4),
                    inverted=invert,
                    weight=weight,
                    weighted_score=round(n * weight, 4),
                ))

        with scores_conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO score_metric_normalized
                    (run_id, country_iso, sub_index, metric_key, mineral,
                     raw_value, normalized, inverted, weight, weighted_score)
                VALUES (%(run_id)s, %(country_iso)s, %(sub_index)s, %(metric_key)s,
                        %(mineral)s, %(raw_value)s, %(normalized)s, %(inverted)s,
                        %(weight)s, %(weighted_score)s)
            """, normalized_rows)
        scores_conn.commit()
        print(f"  ✓ {len(normalized_rows)} normalized rows stored")

        # ── 2b. SI3 per-mineral composite, then mineral-weighted SI3 score
        print(f"[SCORE] Composing SI3 per-mineral scores…")
        si3_mineral_rows = []
        # Group SI3 by (country, mineral) → sum weighted_score across the 3 metrics
        si3_per_mineral = defaultdict(float)
        si3_seen = defaultdict(set)
        for r in normalized_rows:
            if r["sub_index"] != "SI3":
                continue
            si3_per_mineral[(r["country_iso"], r["mineral"])] += r["weighted_score"]
            si3_seen[(r["country_iso"], r["mineral"])].add(r["metric_key"])

        for (cty, mineral), score in si3_per_mineral.items():
            mw = mineral_w.get(mineral, 0.0)
            si3_mineral_rows.append(dict(
                run_id=run_uuid, country_iso=cty, mineral=mineral,
                mineral_score=round(score, 4),
                mineral_weight=mw,
                weighted_score=round(score * mw, 4),
            ))

        with scores_conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO score_mineral
                    (run_id, country_iso, mineral, mineral_score, mineral_weight, weighted_score)
                VALUES (%(run_id)s, %(country_iso)s, %(mineral)s,
                        %(mineral_score)s, %(mineral_weight)s, %(weighted_score)s)
            """, si3_mineral_rows)
        scores_conn.commit()

        # ── 2c. Sub-index composite per country
        print(f"[SCORE] Composing sub-index scores…")
        subindex_rows = []
        # Build a (country, sub_index) → list of data_dates lookup
        dates_by_si = defaultdict(list)
        for r in inputs:
            if r["data_date"] is not None:
                dates_by_si[(r["country_iso"], r["sub_index"])].append(r["data_date"])

        def _date_range(key):
            ds = dates_by_si.get(key, [])
            return (min(ds) if ds else None, max(ds) if ds else None)

        # SI1, SI2, SI4 = sum of weighted_score across metrics (already weighted)
        # SI3 = sum of mineral.weighted_score across minerals
        for cty in COUNTRIES:
            for si in ("SI1", "SI2", "SI4"):
                total = sum(r["weighted_score"] for r in normalized_rows
                            if r["sub_index"] == si and r["country_iso"] == cty)
                d_min, d_max = _date_range((cty, si))
                subindex_rows.append(dict(
                    run_id=run_uuid, country_iso=cty, sub_index=si,
                    score=round(total, 4),
                    weight=subindex_w.get(si, 0.0),
                    weighted_score=round(total * subindex_w.get(si, 0.0), 4),
                    data_date_min=d_min, data_date_max=d_max,
                ))
            si3_total = sum(r["weighted_score"] for r in si3_mineral_rows
                            if r["country_iso"] == cty)
            d_min, d_max = _date_range((cty, "SI3"))
            subindex_rows.append(dict(
                run_id=run_uuid, country_iso=cty, sub_index="SI3",
                score=round(si3_total, 4),
                weight=subindex_w.get("SI3", 0.0),
                weighted_score=round(si3_total * subindex_w.get("SI3", 0.0), 4),
                data_date_min=d_min, data_date_max=d_max,
            ))

        with scores_conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO score_subindex
                    (run_id, country_iso, sub_index, score, weight, weighted_score,
                     data_date_min, data_date_max)
                VALUES (%(run_id)s, %(country_iso)s, %(sub_index)s,
                        %(score)s, %(weight)s, %(weighted_score)s,
                        %(data_date_min)s, %(data_date_max)s)
            """, subindex_rows)
        scores_conn.commit()

        # ── 2d. Final SDI per country
        print(f"[SCORE] Composing final SDI scores…")
        per_country = defaultdict(dict)
        for r in subindex_rows:
            per_country[r["country_iso"]][r["sub_index"]] = r
        sdi_rows = []
        for cty in COUNTRIES:
            si_data = per_country.get(cty, {})
            sdi = sum(s.get("weighted_score", 0) for s in si_data.values())
            # Roll up data dates across all sub-indexes for this country
            all_dates = [d for s in si_data.values()
                         for d in (s.get("data_date_min"), s.get("data_date_max")) if d]
            d_min = min(all_dates) if all_dates else None
            d_max = max(all_dates) if all_dates else None
            sdi_rows.append(dict(
                run_id=run_uuid, country_iso=cty,
                si1_energy=  round(si_data.get("SI1", {}).get("score", 0), 4),
                si2_water=   round(si_data.get("SI2", {}).get("score", 0), 4),
                si3_minerals=round(si_data.get("SI3", {}).get("score", 0), 4),
                si4_food=    round(si_data.get("SI4", {}).get("score", 0), 4),
                sdi_score=   round(sdi, 4),
                data_date_min=d_min, data_date_max=d_max,
            ))
        # rank
        sdi_rows.sort(key=lambda r: r["sdi_score"], reverse=True)
        for i, r in enumerate(sdi_rows, 1):
            r["rank"] = i

        with scores_conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO score_sdi
                    (run_id, country_iso, si1_energy, si2_water,
                     si3_minerals, si4_food, sdi_score, rank,
                     data_date_min, data_date_max)
                VALUES (%(run_id)s, %(country_iso)s, %(si1_energy)s, %(si2_water)s,
                        %(si3_minerals)s, %(si4_food)s, %(sdi_score)s, %(rank)s,
                        %(data_date_min)s, %(data_date_max)s)
                ON CONFLICT (country_iso) DO UPDATE SET
                    run_id        = EXCLUDED.run_id,
                    si1_energy    = EXCLUDED.si1_energy,
                    si2_water     = EXCLUDED.si2_water,
                    si3_minerals  = EXCLUDED.si3_minerals,
                    si4_food      = EXCLUDED.si4_food,
                    sdi_score     = EXCLUDED.sdi_score,
                    rank          = EXCLUDED.rank,
                    data_date_min = EXCLUDED.data_date_min,
                    data_date_max = EXCLUDED.data_date_max,
                    computed_at   = NOW()
            """, sdi_rows)
        scores_conn.commit()

        # Print final ranked table
        print(f"\n{'='*78}")
        print(f"  Final SDI — Chessboard Sovereign Index")
        print(f"{'='*78}")
        print(f"  {'Rank':<5}{'Country':<8}{'SI1':>7}{'SI2':>7}{'SI3':>7}{'SI4':>7}"
              f"{'SDI':>8}   {'Data as of (oldest → newest)':<32}")
        for r in sdi_rows:
            d_lo = r["data_date_min"].isoformat() if r["data_date_min"] else "—"
            d_hi = r["data_date_max"].isoformat() if r["data_date_max"] else "—"
            print(f"  {r['rank']:<5}{r['country_iso']:<8}"
                  f"{r['si1_energy']:>7.2f}{r['si2_water']:>7.2f}"
                  f"{r['si3_minerals']:>7.2f}{r['si4_food']:>7.2f}"
                  f"{r['sdi_score']:>8.2f}   {d_lo} → {d_hi}")
        print(f"{'='*78}\n")

        return len(sdi_rows)
    finally:
        scores_conn.close()


def main():
    run_uuid = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Insert run row
    conn = _conn(GRAMERCY_DB)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO score_runs (run_uuid, status) VALUES (%s, 'running')
            RETURNING id
        """, (run_uuid,))
        run_db_id = cur.fetchone()[0]
    conn.commit(); conn.close()

    try:
        n = compute_and_store(run_uuid)
        finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        elapsed = (finished_at - started_at).total_seconds()
        conn = _conn(GRAMERCY_DB)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE score_runs
                   SET finished_at=%s, status='success', countries_scored=%s
                 WHERE id=%s
            """, (finished_at, n, run_db_id))
        conn.commit(); conn.close()
        print(f"Run {run_uuid} complete in {elapsed:.1f}s — {n} countries scored.")
    except Exception as exc:
        conn = _conn(GRAMERCY_DB)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE score_runs SET finished_at=NOW(), status='failed', notes=%s WHERE id=%s
            """, (str(exc)[:500], run_db_id))
        conn.commit(); conn.close()
        raise


if __name__ == "__main__":
    main()
