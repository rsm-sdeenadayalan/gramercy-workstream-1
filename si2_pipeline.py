"""
SI2 Water Availability Pipeline
================================
Matches the cascade + research agent fallback architecture of 02_pipeline.py.

Metrics:
  freshwater_per_capita          — World Bank WDI
  baseline_water_stress          — WRI Aqueduct 4.0 baseline
  projected_water_stress_2050    — WRI Aqueduct 4.0 projected SSP3-7.0
  projected_water_stress_change  — Delta projected - baseline
  regulatory_restrictions_score  — Claude NLP over official regulatory docs

DB: subindex_2  (same PostgreSQL server as SI1, same SSH tunnel)
"""

from dotenv import load_dotenv
load_dotenv()

import os, json, re, time, uuid, psycopg2, psycopg2.extras, requests
from datetime import datetime, date, timezone
from si2_collectors import (
    COUNTRIES, METRICS, CONFIDENCE,
    collect_worldbank_freshwater,
    collect_wri_baseline,
    collect_wri_projected,
    collect_wri_stress_change,
    collect_cckp_stress_change,
    collect_sg_water_stress_known,
    collect_sg_resilience_proxy,
    collect_regulatory_score,
    make_result,
)
from research_agent import run_research_agent, get_token_usage as _agent_tokens

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   os.environ.get("POSTGRES_DB", "gramercy_workstream1"),
    "user":     os.environ.get("POSTGRES_USER", "shankar_1"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TAVILY_API_KEY    = os.environ.get("TAVILY_API_KEY", "")

# Claude Haiku pricing
_INPUT_COST_PER_M  = 0.80
_OUTPUT_COST_PER_M = 4.00
_token_usage = {"input": 0, "output": 0, "calls": 0}

def _track(resp):
    u = resp.get("usage", {})
    _token_usage["input"]  += u.get("input_tokens", 0)
    _token_usage["output"] += u.get("output_tokens", 0)
    _token_usage["calls"]  += 1

def print_token_summary():
    agent = _agent_tokens()
    inp   = _token_usage["input"]  + agent["input"]
    out   = _token_usage["output"] + agent["output"]
    calls = _token_usage["calls"]  + agent["calls"]
    cost  = (inp / 1_000_000 * _INPUT_COST_PER_M) + (out / 1_000_000 * _OUTPUT_COST_PER_M)
    print(f"\n{'─'*50}")
    print(f"Claude usage: {calls} calls | {inp:,} input + {out:,} output tokens")
    print(f"Estimated cost: ${cost:.4f} USD")
    print(f"{'─'*50}")


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS si2_raw_metrics (
    id               SERIAL PRIMARY KEY,
    country_iso      CHAR(2) NOT NULL,
    country_name     VARCHAR(120) NOT NULL,
    metric_key       VARCHAR(120) NOT NULL,
    metric_label     VARCHAR(255) NOT NULL,
    metric_value     DOUBLE PRECISION NOT NULL,
    unit             VARCHAR(80) NOT NULL,
    data_date        DATE NOT NULL,
    data_frequency   VARCHAR(80) NOT NULL,
    source_name      VARCHAR(255) NOT NULL,
    source_url       TEXT,
    access_method    VARCHAR(80) NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL,
    raw_value        TEXT,
    is_manual_override BOOLEAN DEFAULT FALSE,
    override_note    TEXT,
    run_id           UUID,
    collected_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_iso, metric_key, data_date, source_name)
);

CREATE TABLE IF NOT EXISTS si2_collection_runs (
    run_id       UUID PRIMARY KEY,
    started_at   TIMESTAMP DEFAULT NOW(),
    finished_at  TIMESTAMP,
    total_tasks  INTEGER DEFAULT 0,
    succeeded    INTEGER DEFAULT 0,
    failed       INTEGER DEFAULT 0,
    gaps_opened  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS si2_collection_log (
    id             SERIAL PRIMARY KEY,
    run_id         UUID,
    country_iso    CHAR(2),
    metric_key     VARCHAR(120),
    collector_name VARCHAR(120),
    cascade_step   INTEGER,
    status         VARCHAR(20),
    source_url     TEXT,
    error_type     VARCHAR(120),
    error_message  TEXT,
    duration_ms    INTEGER,
    attempted_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS si2_data_gaps (
    id               SERIAL PRIMARY KEY,
    country_iso      CHAR(2),
    country_name     VARCHAR(120),
    metric_key       VARCHAR(120),
    metric_label     VARCHAR(255),
    failure_reason   TEXT,
    collectors_tried TEXT[],
    severity         VARCHAR(20) DEFAULT 'medium',
    first_detected   TIMESTAMP DEFAULT NOW(),
    last_attempted   TIMESTAMP DEFAULT NOW(),
    attempt_count    INTEGER DEFAULT 1,
    status           VARCHAR(20) DEFAULT 'open',
    UNIQUE (country_iso, metric_key)
);

CREATE OR REPLACE VIEW v_si2_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key, metric_label,
    metric_value, unit, data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si2_raw_metrics
ORDER BY country_iso, metric_key, collected_at DESC;
"""

_MIGRATE_SQL = """
-- Drop old kelun tables in dependency order
DROP VIEW  IF EXISTS v_si2_completeness CASCADE;
DROP VIEW  IF EXISTS v_si2_latest      CASCADE;
DROP TABLE IF EXISTS si2_water_metrics   CASCADE;
DROP TABLE IF EXISTS si2_raw_metrics     CASCADE;
DROP TABLE IF EXISTS si2_collection_log  CASCADE;
DROP TABLE IF EXISTS si2_data_gaps       CASCADE;
DROP TABLE IF EXISTS si2_collection_runs CASCADE;
DROP TABLE IF EXISTS si2_pipeline_runs   CASCADE;
DROP TABLE IF EXISTS si2_sources         CASCADE;
DROP TABLE IF EXISTS si2_metric_definitions CASCADE;
DROP TABLE IF EXISTS si2_countries       CASCADE;
"""

def init_db(migrate=False):
    conn = get_conn()
    with conn.cursor() as cur:
        if migrate:
            print("  Dropping old kelun tables...")
            cur.execute(_MIGRATE_SQL)
            print("  Old tables dropped.")
        cur.execute(_SCHEMA_SQL)
    conn.commit()
    conn.close()
    print("SI2 schema created/verified.")


def store_datapoint(conn, dp, run_id):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si2_raw_metrics (
                country_iso, country_name, metric_key, metric_label,
                metric_value, unit, data_date, data_frequency,
                source_name, source_url, access_method,
                confidence_score, raw_value, is_manual_override,
                override_note, run_id
            ) VALUES (
                %(country_iso)s, %(country_name)s, %(metric_key)s, %(metric_label)s,
                %(metric_value)s, %(unit)s, %(data_date)s, %(data_frequency)s,
                %(source_name)s, %(source_url)s, %(access_method)s,
                %(confidence_score)s, %(raw_value)s, %(is_manual_override)s,
                %(override_note)s, %(run_id)s
            )
            ON CONFLICT (country_iso, metric_key, data_date, source_name) DO UPDATE SET
                metric_value     = EXCLUDED.metric_value,
                confidence_score = EXCLUDED.confidence_score,
                raw_value        = EXCLUDED.raw_value,
                run_id           = EXCLUDED.run_id,
                collected_at     = NOW()
        """, {**dp, "run_id": run_id})
    conn.commit()


def log_attempt(conn, run_id, country_iso, metric_key, collector_name,
                step, status, source_url, error_type, error_msg, duration_ms):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si2_collection_log
                (run_id, country_iso, metric_key, collector_name, cascade_step,
                 status, source_url, error_type, error_message, duration_ms)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, country_iso, metric_key, collector_name, step,
              status, source_url, error_type, error_msg, duration_ms))
    conn.commit()


def open_gap(conn, run_id, country_iso, metric_key, failure_reason, collectors_tried):
    country_name = COUNTRIES[country_iso]["name"]
    metric_label = METRICS[metric_key]["label"]
    severity     = METRICS[metric_key]["gap_severity"]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si2_data_gaps
                (country_iso, country_name, metric_key, metric_label,
                 failure_reason, collectors_tried, severity)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (country_iso, metric_key) DO UPDATE SET
                failure_reason   = EXCLUDED.failure_reason,
                collectors_tried = EXCLUDED.collectors_tried,
                last_attempted   = NOW(),
                attempt_count    = si2_data_gaps.attempt_count + 1
        """, (country_iso, country_name, metric_key, metric_label,
              failure_reason, collectors_tried, severity))
    conn.commit()


# Staleness thresholds (days) per access method.
_STALE_THRESHOLDS = {
    "api_annual":    365,
    "api_quarterly": 95,
    "api_monthly":   35,
    "api":           35,
    "file_download": 90,
    "web_scrape":    30,
    "pdf_extract":   180,
    "pdf_regex":     180,
    "imputed":       180,
}

# ── Staleness check ───────────────────────────────────────────────────────────
def _is_stale(conn, country_iso, metric_key):
    """Staleness threshold is per access_method so annual/file sources aren't
    re-fetched on every daily run."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT metric_value, collected_at, access_method
            FROM si2_raw_metrics
            WHERE country_iso=%s AND metric_key=%s
            ORDER BY collected_at DESC LIMIT 1
        """, (country_iso, metric_key))
        row = cur.fetchone()
    if not row:
        return True, None, None
    val, collected_at, method = row
    age_days = (datetime.now(timezone.utc).replace(tzinfo=None) - collected_at).days
    threshold = _STALE_THRESHOLDS.get(method, 1)
    return age_days > threshold, age_days, val


# ── Cascade definition ────────────────────────────────────────────────────────
def _make_cascade():
    """
    Returns dict mapping (country_iso, metric_key) → list of collector steps.
    Each step: {"name": str, "fn": callable, "kwargs": dict}
    """
    cascade = {}

    # freshwater_per_capita — World Bank API for all countries
    for iso in COUNTRIES:
        cascade[(iso, "freshwater_per_capita")] = [
            {"name": "World Bank WDI", "fn": collect_worldbank_freshwater, "kwargs": {}},
        ]

    # baseline_water_stress — WRI Aqueduct primary.
    # SG has no internal watershed; WRI raises ValueError, so we use the known
    # WRI verbal classification (Extremely High = 5.0/5.0) rather than the PUB
    # import-reliability proxy (which measures a different concept).
    for iso in COUNTRIES:
        steps = [{"name": "WRI Aqueduct 4.0", "fn": collect_wri_baseline, "kwargs": {}}]
        if iso == "SG":
            steps.append({"name": "WRI Aqueduct known (Extremely High)",
                          "fn": collect_sg_water_stress_known, "kwargs": {}})
        cascade[(iso, "baseline_water_stress")] = steps

    # projected_water_stress_2050 — WRI primary; SG uses known classification.
    for iso in COUNTRIES:
        steps = [{"name": "WRI Aqueduct 4.0 projected",
                  "fn": collect_wri_projected, "kwargs": {}}]
        if iso == "SG":
            steps.append({"name": "WRI Aqueduct known projected (Extremely High)",
                          "fn": collect_sg_water_stress_known, "kwargs": {}})
        cascade[(iso, "projected_water_stress_2050")] = steps

    # projected_water_stress_change — WRI primary, CCKP runoff-proxy fallback
    for iso in COUNTRIES:
        cascade[(iso, "projected_water_stress_change")] = [
            {"name": "WRI Aqueduct 4.0 delta",    "fn": collect_wri_stress_change,   "kwargs": {}},
            {"name": "CCKP CMIP6 runoff proxy",   "fn": collect_cckp_stress_change,  "kwargs": {}},
        ]

    # regulatory_restrictions_score — dynamic URL discovery + Claude NLP scoring
    # Now works for all 6 countries; research agent is the universal fallback
    # if discovery/scraping fails for any given country.
    for iso in COUNTRIES:
        cascade[(iso, "regulatory_restrictions_score")] = [
            {"name": "Claude NLP regulatory (dynamic discovery)",
             "fn": collect_regulatory_score,
             "kwargs": {"anthropic_api_key": ANTHROPIC_API_KEY}},
        ]

    return cascade


METRIC_CASCADE = _make_cascade()


# ── Research agent fallback ───────────────────────────────────────────────────
# SI2-specific search query templates
_SI2_QUERIES = {
    # Vocabulary patterns validated by "Global Water Data Pipeline Request" report.
    "freshwater_per_capita": [
        "{country} renewable internal freshwater resources per capita World Bank {year}",
        "{country} AQUASTAT FAO water resources per capita cubic meters {year}",
        "{country} ER.H2O.INTR.PC World Bank indicator {year}",
        "{country} annual renewable water resources total population ratio {year}",
    ],
    "baseline_water_stress": [
        "{country} WRI Aqueduct 4.0 country rankings baseline water stress",
        "{country} baseline water stress withdrawal supply ratio score",
        "{country} water stress index industrial extremely-high {year}",
    ],
    "projected_water_stress_2050": [
        "{country} WRI Aqueduct 4.0 future water stress projections 2050 SSP3-7.0",
        "{country} water stress 2050 business-as-usual BAU scenario projection",
        "{country} projected water risk 2050 climate change SSP scenario",
    ],
    "projected_water_stress_change": [
        "{country} CMIP6 runoff anomaly SSP3-7.0 World Bank Climate Knowledge Portal",
        "{country} delta projected baseline water stress change 2050",
        "{country} climateknowledgeportal.worldbank.org CMIP6 projections runoff",
    ],
    "regulatory_restrictions_score": [
        "{country} industrial water withdrawal permit compliance monitoring enforcement {year}",
        "{country} water regulator annual report permits penalties industrial",
        "{country} water use reporting metering requirements large industrial users {year}",
    ],
}


def _try_research_agent(conn, run_id, country_iso, metric_key, step_num, errors, tried):
    """Run the deep research agent as a universal fallback."""
    has_search = bool(TAVILY_API_KEY and TAVILY_API_KEY != "your_tavily_key_here")
    if not has_search:
        return False

    agent_name = "Research Agent"
    tried.append(agent_name)
    t0 = time.perf_counter()
    try:
        # Override query templates with SI2-specific ones
        import research_agent as _ra
        original = _ra.EnergyResearchAgent._PRIMARY_QUERIES
        _ra.EnergyResearchAgent._PRIMARY_QUERIES = {
            **original, **{k: v for k, v in _SI2_QUERIES.items()}
        }

        result = run_research_agent(
            country_iso  = country_iso,
            metric_key   = metric_key,
            country_name = COUNTRIES[country_iso]["name"],
            currency     = "USD",
            metric_label = METRICS[metric_key]["label"],
            metric_unit  = METRICS[metric_key]["unit"],
            fx_rates     = {"USD": 1.0},
        )

        _ra.EnergyResearchAgent._PRIMARY_QUERIES = original

        val       = float(result["value"])
        data_date = date.today().replace(month=1, day=1)
        try:
            data_date = datetime.fromisoformat(result.get("data_date", "")).date()
        except Exception:
            pass

        dp = make_result(
            country_iso, metric_key, val,
            METRICS[metric_key]["unit"], data_date,
            result.get("frequency", "irregular"),
            f"Research Agent — {result.get('source_url', '')[:80]}",
            result.get("source_url", ""),
            "web_scrape", CONFIDENCE["web_scrape"],
            raw_value=result.get("raw_text", ""),
        )
        elapsed = int((time.perf_counter() - t0) * 1000)
        fresh = get_conn()
        try:
            log_attempt(fresh, run_id, country_iso, metric_key, agent_name, step_num,
                        "success", dp.get("source_url"), None, None, elapsed)
            store_datapoint(fresh, dp, run_id)
        finally:
            fresh.close()
        print(f"  \u2713 [{country_iso}] {metric_key} = {val} {dp['unit']} (src={agent_name})")
        return True

    except Exception as exc:
        elapsed = int((time.perf_counter() - t0) * 1000)
        err_msg = str(exc)[:300]
        errors.append(f"[{agent_name}] {type(exc).__name__}: {err_msg}")
        try:
            fresh = get_conn()
            log_attempt(fresh, run_id, country_iso, metric_key, agent_name, step_num,
                        "failed", None, type(exc).__name__, err_msg, elapsed)
            fresh.close()
        except Exception:
            pass
        print(f"  \u2717 [{country_iso}] {metric_key} \u2014 {agent_name}: {err_msg[:80]}")
        return False


# ── Cascade runner ────────────────────────────────────────────────────────────
def _get_last_known_dp(conn, country_iso: str, metric_key: str) -> dict | None:
    """Return the most recent non-NULL stored datapoint for carry-forward imputation."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, country_name, metric_key, metric_label,
                   metric_value, unit, data_date, data_frequency,
                   source_name, source_url, access_method, confidence_score,
                   raw_value, currency_conversion, is_imputed
            FROM si2_raw_metrics
            WHERE country_iso = %s AND metric_key = %s
              AND metric_value IS NOT NULL
            ORDER BY collected_at DESC
            LIMIT 1
        """, (country_iso, metric_key))
        row = cur.fetchone()
    if not row:
        return None
    cols = ["country_iso", "country_name", "metric_key", "metric_label",
            "metric_value", "unit", "data_date", "data_frequency",
            "source_name", "source_url", "access_method", "confidence_score",
            "raw_value", "currency_conversion", "is_imputed"]
    dp = dict(zip(cols, row))
    dp["confidence_score"] = CONFIDENCE["imputed"]
    dp["is_imputed"] = True
    return dp


def run_cascade(conn, run_id, country_iso, metric_key):
    steps  = METRIC_CASCADE.get((country_iso, metric_key), [])
    errors = []
    tried  = []

    # Staleness check
    is_stale, age_days, existing = _is_stale(conn, country_iso, metric_key)
    if not is_stale and existing is not None:
        print(f"  [FRESH] ({country_iso}, {metric_key}) — {age_days}d old, skipping")
        return True
    if age_days is not None:
        print(f"  [STALE {age_days}d] ({country_iso}, {metric_key}) — refreshing...")

    # Run cascade steps
    cascade_succeeded = False
    for step_num, step in enumerate(steps, start=1):
        name   = step["name"]
        fn     = step["fn"]
        kwargs = {**step["kwargs"], "country_iso": country_iso, "metric_key": metric_key}
        tried.append(name)
        t0 = time.perf_counter()
        try:
            dp      = fn(**kwargs)
            elapsed = int((time.perf_counter() - t0) * 1000)
            log_attempt(conn, run_id, country_iso, metric_key, name, step_num,
                        "success", dp.get("source_url"), None, None, elapsed)
            store_datapoint(conn, dp, run_id)
            print(f"  \u2713 [{country_iso}] {metric_key} = {dp['metric_value']:.3f} {dp['unit']} "
                  f"(src={name}, conf={dp['confidence_score']})")
            cascade_succeeded = True
            break
        except Exception as exc:
            elapsed  = int((time.perf_counter() - t0) * 1000)
            err_type = type(exc).__name__
            err_msg  = str(exc)[:300]
            errors.append(f"[{name}] {err_type}: {err_msg}")
            log_attempt(conn, run_id, country_iso, metric_key, name, step_num,
                        "failed", step["kwargs"].get("url", ""),
                        err_type, err_msg, elapsed)
            print(f"  \u2717 [{country_iso}] {metric_key} \u2014 {name}: {err_msg[:80]}")

    # Research agent always runs — finds fresher press/web data even when
    # cascade succeeded. Both results stored; view picks newer data_date.
    print(f"  [AGENT] {'Supplementing cascade with' if cascade_succeeded else 'Trying'} research agent...")
    agent_succeeded = _try_research_agent(conn, run_id, country_iso, metric_key,
                                          len(steps) + 1, errors, tried)
    if cascade_succeeded or agent_succeeded:
        return True

    # Carry forward last known value, then open gap
    fresh = get_conn()
    try:
        carried = _get_last_known_dp(fresh, country_iso, metric_key)
        if carried:
            store_datapoint(fresh, carried, run_id)
            print(f"  [CARRY] ({country_iso}, {metric_key}) = {carried['metric_value']} {carried['unit']} (last known, imputed)")
        open_gap(fresh, run_id, country_iso, metric_key,
                 " | ".join(errors), tried)
    except Exception:
        pass
    finally:
        fresh.close()
    print(f"  \u2717\u2717 GAP: ({country_iso}, {metric_key}) — all {len(tried)} collector(s) failed")
    return False


# ── Pipeline runner ───────────────────────────────────────────────────────────
def run_pipeline():
    run_id     = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    conn       = get_conn()

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO si2_collection_runs (run_id) VALUES (%s)", (run_id,)
        )
    conn.commit()

    combos    = [(c, m) for c in COUNTRIES for m in METRICS]
    total     = len(combos)
    succeeded = failed = 0

    print(f"\nSI2 Run ID: {run_id}")
    print(f"Tasks: {total}  ({len(COUNTRIES)} countries \u00d7 {len(METRICS)} metrics)\n")

    for i, (country_iso, metric_key) in enumerate(combos, start=1):
        print(f"\n[{i}/{total}] {country_iso} / {metric_key}")
        ok = run_cascade(conn, run_id, country_iso, metric_key)
        if ok:
            succeeded += 1
        else:
            failed += 1

    finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
    elapsed     = (finished_at - started_at).total_seconds()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM si2_data_gaps WHERE status='open'")
        gaps_open = cur.fetchone()[0]
        cur.execute("""
            UPDATE si2_collection_runs
            SET finished_at=%s, total_tasks=%s, succeeded=%s, failed=%s, gaps_opened=%s
            WHERE run_id=%s
        """, (finished_at, total, succeeded, failed, gaps_open, run_id))
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"SI2 Pipeline complete in {elapsed:.1f}s")
    print(f"  Succeeded : {succeeded}/{total}")
    print(f"  Failed    : {failed}/{total}")
    print(f"  Open gaps : {gaps_open}")
    print(f"{'='*60}")
    print_token_summary()
    return run_id


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--migrate", action="store_true",
                        help="Drop old kelun tables and recreate clean schema")
    args = parser.parse_args()
    init_db(migrate=args.migrate)
    run_pipeline()
