"""
SI3 Critical Mineral Endowment Pipeline
========================================
Mirrors the cascade + research-agent fallback architecture of si4_pipeline.py.

Metrics (per country × mineral):
  production_share          — country mine prod / world mine prod          (USGS, annual)
  reserves_share            — country reserves / world reserves             (USGS, annual)
  yoy_production_growth     — (prod_t − prod_{t-1}) / prod_{t-1}           (USGS, annual)
  refining_capacity_share   — country processed exports / world processed   (Comtrade, annual)
  value_add_ratio           — processed / (raw + processed) exports         (Comtrade, annual)

Countries (6): US, AE, BR, IN, SG, PH
Minerals  (6): copper, lithium, nickel, cobalt, rare_earths, silicon

DB: subindex_3  (same PostgreSQL server, SSH tunnel port 5433)
"""

from dotenv import load_dotenv
load_dotenv()

import os, sys, psycopg2, psycopg2.extras
from pathlib import Path

# ── SSL trust store (Python 3.14 on macOS often lacks system CA bundle) ──────
# Force requests / urllib3 / comtradeapicall to use certifi's bundle.
try:
    import certifi as _certifi
    os.environ.setdefault("SSL_CERT_FILE",     _certifi.where())
    os.environ.setdefault("REQUESTS_CA_BUNDLE", _certifi.where())
except ImportError:
    pass

# ── DB configuration ──────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   "subindex_3",
    "user":     os.environ.get("SI3_POSTGRES_USER",     os.environ.get("POSTGRES_USER", "")),
    "password": os.environ.get("SI3_POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")),
}

# ── API keys ──────────────────────────────────────────────────────────────────
COMTRADE_KEY      = os.environ.get("COMTRADE_KEY", "").strip() or None
JINA_API_KEY      = os.environ.get("JINA_API_KEY", "")
TAVILY_API_KEY    = os.environ.get("TAVILY_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
BRAVE_API_KEY     = os.environ.get("BRAVE_API_KEY", "")

# ── Research agent import (parent folder) ─────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from research_agent import run_research_agent as _run_deep_research

def get_conn():
    """Connect to subindex_3, auto-creating the database if it doesn't exist."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        # Only auto-create on "database does not exist" — re-raise other errors
        if f'"{DB_CONFIG["dbname"]}" does not exist' not in str(e):
            raise
        print(f"[SI3] Database '{DB_CONFIG['dbname']}' missing — creating it…")
        # Try a list of likely-existing admin DBs to connect to for the CREATE
        last_err = None
        for admin_db in ("postgres", "subindex_1", "subindex_4", "subindex_2",
                         "template1", DB_CONFIG["user"]):
            try:
                admin = psycopg2.connect(**{**DB_CONFIG, "dbname": admin_db})
                admin.autocommit = True
                with admin.cursor() as cur:
                    cur.execute(f'CREATE DATABASE "{DB_CONFIG["dbname"]}"')
                admin.close()
                print(f"[SI3] Database '{DB_CONFIG['dbname']}' created (via {admin_db}).")
                return psycopg2.connect(**DB_CONFIG)
            except psycopg2.OperationalError as inner:
                last_err = inner
                continue
        raise RuntimeError(
            f"Could not connect to any admin database to CREATE {DB_CONFIG['dbname']}. "
            f"Last error: {last_err}"
        )

print(f"[SI3] host={DB_CONFIG['host']}  port={DB_CONFIG['port']}  "
      f"user={DB_CONFIG['user']}  db={DB_CONFIG['dbname']}")

# ── Imports ───────────────────────────────────────────────────────────────────
import re, json, time, uuid, warnings
import numpy as np
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Optional

import requests as _requests
import pandas as pd

warnings.filterwarnings("ignore")

# =============================================================================
# REFERENCE TABLES
# =============================================================================

COUNTRIES = {
    "US": {"name": "United States",   "currency": "USD",
           "usgs_pattern": r"United States|United States of America|U\.S\.",
           "m49": "842", "iso3": "USA"},
    "AE": {"name": "UAE",             "currency": "USD",
           "usgs_pattern": r"United Arab Emirates|UAE",
           "m49": "784", "iso3": "ARE"},
    "BR": {"name": "Brazil",          "currency": "USD",
           "usgs_pattern": r"Brazil",
           "m49": "076", "iso3": "BRA"},
    "IN": {"name": "India",           "currency": "USD",
           "usgs_pattern": r"India",
           "m49": "356", "iso3": "IND"},
    "SG": {"name": "Singapore",       "currency": "USD",
           "usgs_pattern": r"Singapore",
           "m49": "702", "iso3": "SGP"},
    "PH": {"name": "Philippines",     "currency": "USD",
           "usgs_pattern": r"Philippines|Philippine Islands",
           "m49": "608", "iso3": "PHL"},
}

MINERALS = ["copper", "lithium", "nickel", "cobalt", "rare_earths", "silicon"]

# USGS slug used in ScienceBase and PDF URLs
MINERAL_SLUGS = {
    "copper":      "copper",
    "lithium":     "lithium",
    "nickel":      "nickel",
    "cobalt":      "cobalt",
    "rare_earths": "rare-earths",
    "silicon":     "silicon",
}

# Display name for USGS matching
MINERAL_DISPLAY = {
    "copper":      "Copper",
    "lithium":     "Lithium",
    "nickel":      "Nickel",
    "cobalt":      "Cobalt",
    "rare_earths": "Rare Earths",
    "silicon":     "Silicon",
}

# HS codes per mineral (raw ore / processed/refined exports)
HS_CODES = {
    "copper":      {"raw": ["260300"],
                    "processed": ["740311", "740319", "740321", "740322", "740329"]},
    "lithium":     {"raw": ["253090", "261710"],
                    "processed": ["282520", "283691"]},
    "nickel":      {"raw": ["260400"],
                    "processed": ["750110", "750120", "750210", "750220"]},
    "cobalt":      {"raw": ["260500"],
                    "processed": ["810520", "810530"]},
    "rare_earths": {"raw": ["253090", "260190"],
                    "processed": ["284610", "284690"]},
    "silicon":     {"raw": ["250510", "250590", "250610"],
                    "processed": ["280461", "280469"]},
}

# Per-mineral-metric METRICS dict (for make_metric_result label lookup).
# metric_key format: {metric}_{mineral}, e.g. production_share_copper.
# Base metric names match si3_metric_definitions.metric_code in the schema.
_BASE_METRICS = {
    "production_share": {
        "label_tmpl": "Share of global {mineral} mine production",
        "unit": "ratio (0-1)",
        "gap_severity": "high",
        "access_method": "api_annual",
    },
    "reserves_share": {
        "label_tmpl": "Share of global {mineral} reserves",
        "unit": "ratio (0-1)",
        "gap_severity": "high",
        "access_method": "api_annual",
    },
    "yoy_growth": {
        "label_tmpl": "Year-over-year {mineral} production growth rate",
        "unit": "ratio",
        "gap_severity": "medium",
        "access_method": "api_annual",
    },
    "refining_share": {
        "label_tmpl": "Share of global {mineral} refined/processed exports",
        "unit": "ratio (0-1)",
        "gap_severity": "high",
        "access_method": "api_annual",
    },
    "value_add_ratio": {
        "label_tmpl": "Value-add ratio for {mineral} exports (processed share)",
        "unit": "ratio (0-1)",
        "gap_severity": "medium",
        "access_method": "api_annual",
    },
}

# Build flat METRICS dict keyed by {metric}_{mineral}
METRICS = {}
for _metric, _meta in _BASE_METRICS.items():
    for _mineral in MINERALS:
        _key = f"{_metric}_{_mineral}"
        _label = _meta["label_tmpl"].format(mineral=MINERAL_DISPLAY.get(_mineral, _mineral))
        METRICS[_key] = {
            "label":        _label,
            "unit":         _meta["unit"],
            "gap_severity": _meta["gap_severity"],
            "base_metric":  _metric,
            "mineral":      _mineral,
            "access_method": _meta["access_method"],
        }

CONFIDENCE = {
    "api_annual":    0.80,
    "api_est":       0.65,   # USGS 'e' (estimated) flag
    "file_download": 0.75,
    "web_scrape":    0.60,
    "pdf_extract":   0.45,
    "research_agent": 0.55,
}

HEADERS = {"User-Agent": "UCSD-MSBA-Capstone-CSI/1.0 (research; contact: capstone team)",
           "Accept": "application/json, text/csv, */*"}

# =============================================================================
# RESULT BUILDER
# =============================================================================

def make_metric_result(country_iso, metric_key, metric_value, unit, data_date,
                       data_frequency, source_name, source_url, access_method,
                       confidence_score, raw_value=None, mineral=None) -> dict:
    m = METRICS[metric_key]
    return {
        "country_iso":      country_iso,
        "country_name":     COUNTRIES[country_iso]["name"],
        "metric_key":       metric_key,
        "metric_label":     m["label"],
        "metric_value":     metric_value,
        "unit":             unit,
        "data_date":        data_date,
        "data_frequency":   data_frequency,
        "source_name":      source_name,
        "source_url":       source_url,
        "access_method":    access_method,
        "confidence_score": confidence_score,
        "raw_value":        raw_value,
        "mineral":          mineral or m["mineral"],
    }

# =============================================================================
# USGS — SCIENCEBASE CSV + PDF FALLBACK
# =============================================================================

SCIENCEBASE_API = "https://www.sciencebase.gov/catalog"
USGS_PDF_BASE   = "https://pubs.usgs.gov/periodicals/mcs{year}/mcs{year}-{slug}.pdf"

_SESSION = _requests.Session()
_SESSION.headers.update(HEADERS)

# MCS edition auto-detection:
# USGS publishes MCS{YEAR} in late Jan/early Feb covering {YEAR-1} data.
# If it's January, the new edition may not be out yet — fall back to prior year.
_TODAY = datetime.utcnow()
_MCS_YEAR_DEFAULT = _TODAY.year if _TODAY.month >= 2 else _TODAY.year - 1

# Module-level ScienceBase cache (initialized once per process)
_SB_PARENT_ID    = None
_SB_PARENT_YEAR  = None
_SB_CHILDREN     = None   # pd.DataFrame: id, title


def _http_get(url, timeout=15, retries=2, **kw):
    """GET with retry + back-off. Tighter default timeout (15s) so an unresponsive
    ScienceBase doesn't hang the whole pipeline — research agent picks up the slack."""
    for attempt in range(retries + 1):
        try:
            r = _SESSION.get(url, timeout=timeout, **kw)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 503):
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
        except _requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed after {retries + 1} attempts: {url}")


def _find_mcs_parent_item(year: int) -> Optional[str]:
    """Discover the ScienceBase parent item ID for the MCS {year} Data Release.
    Returns None if not yet published."""
    url = f"{SCIENCEBASE_API}/items"
    params = {
        "q": f"Mineral Commodity Summaries {year} Data Release",
        "format": "json", "max": 25, "fields": "id,title",
    }
    r = _http_get(url, params=params)
    items = r.json().get("items", [])
    title_pat = re.compile(
        rf"Mineral Commodity Summaries {year}.*Data Release", re.IGNORECASE
    )
    for it in items:
        title = it.get("title", "")
        # Prefer top-level parent (no " - " separating the commodity name)
        if title_pat.search(title) and " - " not in title:
            return it["id"]
    return None


def resolve_mcs_year_and_parent(preferred_year: int) -> tuple:
    """Find the most recent MCS edition that has data for all 6 SI3 minerals.

    Falls back up to 3 years if the preferred edition is missing or partial
    (some early-year editions only ship a subset of commodities).
    """
    min_required_children = len(MINERALS)   # need data for all 6
    for offset in range(3):
        year = preferred_year - offset
        pid = _find_mcs_parent_item(year)
        if not pid:
            continue
        # Probe: does this edition actually have all our target minerals?
        url = f"{SCIENCEBASE_API}/items"
        try:
            r = _http_get(url, params={
                "parentId": pid, "format": "json", "max": 200, "fields": "id,title"
            })
            children = r.json().get("items", [])
            n = len(children)
        except Exception:
            n = 0
        if n >= min_required_children:
            if offset > 0:
                print(f"  [USGS] MCS {preferred_year} parent had {n} children — "
                      f"falling back to MCS {year} ({n} children)")
            return year, pid
        else:
            print(f"  [USGS] MCS {year} only has {n} children "
                  f"(need {min_required_children}); trying older edition…")
    raise RuntimeError(
        f"Could not find any MCS Data Release on ScienceBase for "
        f"years {preferred_year-2}..{preferred_year}."
    )


def _ensure_sb_initialized():
    global _SB_PARENT_ID, _SB_PARENT_YEAR, _SB_CHILDREN
    if _SB_CHILDREN is not None:
        return
    _SB_PARENT_YEAR, _SB_PARENT_ID = resolve_mcs_year_and_parent(_MCS_YEAR_DEFAULT)
    print(f"  [USGS] ScienceBase parent: {_SB_PARENT_ID}  (MCS {_SB_PARENT_YEAR})")
    url = f"{SCIENCEBASE_API}/items"
    params = {"parentId": _SB_PARENT_ID, "format": "json", "max": 200,
              "fields": "id,title"}
    r = _http_get(url, params=params)
    items = r.json().get("items", [])
    _SB_CHILDREN = pd.DataFrame([{"id": i["id"], "title": i["title"]} for i in items])
    print(f"  [USGS] Found {len(_SB_CHILDREN)} child items")


def _get_sb_child_for_mineral(mineral: str) -> Optional[str]:
    """Return the ScienceBase child item ID for a mineral (e.g. 'Copper')."""
    try:
        _ensure_sb_initialized()
    except Exception as e:
        print(f"  [USGS] ScienceBase init failed: {e}")
        return None
    display = MINERAL_DISPLAY[mineral]
    hits = _SB_CHILDREN[_SB_CHILDREN["title"].str.contains(
        rf"- {re.escape(display.upper())} Data Release", case=False, regex=True
    )]
    if len(hits) == 0:
        return None
    return hits.iloc[0]["id"]


# ── Number cleaning ───────────────────────────────────────────────────────────

_NUM_CLEAN_RE    = re.compile(r"[,\s]")
_WITHHELD_TOKENS = {"W", "w", "—", "-", "NA", "n/a", "(W)"}

def clean_usgs_number(raw) -> tuple:
    """Convert a raw USGS table cell to (value, flag).

    flag: None = clean, 'W' = withheld, 'EST' = estimated.
    """
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return (np.nan, None)
    s = str(raw).strip()
    if not s:
        return (np.nan, None)
    if s in _WITHHELD_TOKENS or s.upper().startswith("W"):
        return (np.nan, "W")
    flag = None
    if s.endswith("e") or "ᵉ" in s:
        flag = "EST"
        s = s.rstrip("e").replace("ᵉ", "")
    # Strip footnote superscripts
    s = re.sub(r"[²³⁴⁵⁶⁷⁸⁹¹⁰ᵃᵇᶜᵈʳ]", "", s)
    s = _NUM_CLEAN_RE.sub("", s).strip("()")
    if not s:
        return (np.nan, flag)
    try:
        return (float(s), flag)
    except ValueError:
        return (np.nan, flag)


# ── CSV parsing ───────────────────────────────────────────────────────────────

def _parse_sciencebase_world_csv(df: pd.DataFrame, mineral: str) -> pd.DataFrame:
    """Normalize a ScienceBase 'world production and reserves' CSV to long format.

    Returns columns: country, year, metric, value, flag, mineral
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    country_col = next(
        (c for c in df.columns if c.lower() in ("country", "nation")), df.columns[0]
    )
    prod_cols = [c for c in df.columns if re.search(r"production.*20\d{2}|^20\d{2}$", c, re.I)]
    res_cols  = [c for c in df.columns if re.search(r"reserves?", c, re.I)]

    records = []
    for _, row in df.iterrows():
        country = str(row[country_col]).strip()
        if not country:
            continue
        if country.lower() in ("world total", "total", "world"):
            country = "WORLD_TOTAL"

        for c in prod_cols:
            yr_m = re.search(r"(20\d{2})", c)
            if not yr_m:
                continue
            val, flag = clean_usgs_number(row[c])
            records.append({
                "mineral": mineral, "country": country,
                "year": int(yr_m.group(1)), "metric": "production",
                "value": val, "flag": flag,
            })

        if res_cols:
            # Infer reserves year from production column years; else MCS_YEAR-1
            inferred = [int(re.search(r"(20\d{2})", pc).group(1))
                        for pc in prod_cols if re.search(r"(20\d{2})", pc)]
            fallback = (_SB_PARENT_YEAR - 1) if _SB_PARENT_YEAR else _TODAY.year - 1
            reserves_year = max(inferred, default=fallback)
            for c in res_cols:
                val, flag = clean_usgs_number(row[c])
                records.append({
                    "mineral": mineral, "country": country,
                    "year": reserves_year, "metric": "reserves",
                    "value": val, "flag": flag,
                })

    return pd.DataFrame(records)


def fetch_usgs_mineral_csv(mineral: str) -> pd.DataFrame:
    """Fetch USGS MCS data for one mineral.

    Tries ScienceBase CSV first; falls back to PDF text extraction.
    Returns a long-format DataFrame: country, year, metric, value, flag, mineral.
    """
    slug = MINERAL_SLUGS[mineral]

    # Attempt 1: ScienceBase CSV
    sb_id = _get_sb_child_for_mineral(mineral)
    if sb_id:
        try:
            url = f"{SCIENCEBASE_API}/item/{sb_id}"
            r   = _http_get(url, params={"format": "json"})
            item_data = r.json()
            for f in item_data.get("files", []):
                name = f.get("name", "")
                if not name.lower().endswith(".csv"):
                    continue
                if not re.search(r"world|production.*reserve|reserve", name, re.I):
                    continue
                file_url = f.get("url") or f.get("downloadUri")
                if not file_url:
                    continue
                csv_r = _http_get(file_url)
                raw_df = pd.read_csv(pd.io.common.BytesIO(csv_r.content))
                parsed = _parse_sciencebase_world_csv(raw_df, mineral)
                parsed["source"] = "sciencebase_csv"
                return parsed
        except Exception as e:
            print(f"  [USGS] {mineral}: ScienceBase CSV failed ({e}); trying PDF...")

    # Attempt 2: PDF fallback
    pdf_year = _SB_PARENT_YEAR if _SB_PARENT_YEAR else _MCS_YEAR_DEFAULT
    pdf_url  = USGS_PDF_BASE.format(year=pdf_year, slug=slug)
    try:
        r = _http_get(pdf_url)
        try:
            from pypdf import PdfReader
            from io import BytesIO
            reader = PdfReader(BytesIO(r.content))
            _text  = "\n".join(p.extract_text() or "" for p in reader.pages)
        except ImportError:
            import pdfplumber
            from io import BytesIO
            with pdfplumber.open(BytesIO(r.content)) as pdf:
                _text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        print(f"  [USGS] {mineral}: PDF fetched ({pdf_year}); "
              f"structured parsing limited — returning empty.")
        return pd.DataFrame(columns=["mineral", "country", "year", "metric",
                                     "value", "flag", "source"])
    except Exception as e:
        print(f"  [USGS] {mineral}: both ScienceBase and PDF failed: {e}")
        return pd.DataFrame(columns=["mineral", "country", "year", "metric",
                                     "value", "flag", "source"])


# ── USGS metric computation ───────────────────────────────────────────────────

def _filter_target_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize USGS country names to our internal ISO codes."""
    if df.empty:
        return df
    out = []
    for iso, meta in COUNTRIES.items():
        mask = df["country"].str.match(meta["usgs_pattern"], case=False, na=False)
        sub  = df[mask].copy()
        sub["country_iso"] = iso
        out.append(sub)
    world = df[df["country"] == "WORLD_TOTAL"].copy()
    world["country_iso"] = "WORLD_TOTAL"
    out.append(world)
    return pd.concat(out, ignore_index=True) if out else df


_USGS_METRICS_COLS = ["country_iso", "mineral", "metric_name", "value", "year", "flag"]

def compute_usgs_metrics(usgs_long: pd.DataFrame) -> pd.DataFrame:
    """Derive production_share, reserves_share, yoy_growth from raw USGS data.

    Returns rows: country_iso, mineral, metric_name, value, year, flag
    """
    if usgs_long.empty:
        return pd.DataFrame(columns=_USGS_METRICS_COLS)

    df = _filter_target_countries(usgs_long)
    latest_year = int(df["year"].max())
    prior_year  = latest_year - 1

    records = []

    for mineral in MINERALS:
        mdf = df[df["mineral"] == mineral]
        if mdf.empty:
            continue

        prod_latest = mdf[(mdf["metric"] == "production") & (mdf["year"] == latest_year)]
        world_prod  = prod_latest[prod_latest["country_iso"] == "WORLD_TOTAL"]
        if world_prod.empty or pd.isna(world_prod.iloc[0]["value"]):
            world_prod_val = None
        else:
            world_prod_val = float(world_prod.iloc[0]["value"])

        prod_prior  = mdf[(mdf["metric"] == "production") & (mdf["year"] == prior_year)]
        reserves    = mdf[mdf["metric"] == "reserves"]
        world_res   = reserves[reserves["country_iso"] == "WORLD_TOTAL"]
        if world_res.empty or pd.isna(world_res.iloc[0]["value"]):
            world_res_val = None
        else:
            world_res_val = float(world_res.iloc[0]["value"])

        for iso in COUNTRIES:
            # Production share
            cty_prod = prod_latest[prod_latest["country_iso"] == iso]
            if not cty_prod.empty:
                val, flag = float(cty_prod.iloc[0]["value"]), cty_prod.iloc[0].get("flag")
                if not pd.isna(val) and world_prod_val and world_prod_val > 0:
                    records.append({
                        "country_iso": iso, "mineral": mineral,
                        "metric_name": "production_share",
                        "value": val / world_prod_val,
                        "year": latest_year, "flag": flag,
                    })

            # Reserves share
            cty_res = reserves[reserves["country_iso"] == iso]
            if not cty_res.empty:
                val, flag = float(cty_res.iloc[0]["value"]), cty_res.iloc[0].get("flag")
                if not pd.isna(val) and world_res_val and world_res_val > 0:
                    records.append({
                        "country_iso": iso, "mineral": mineral,
                        "metric_name": "reserves_share",
                        "value": val / world_res_val,
                        "year": latest_year, "flag": flag,
                    })

            # YoY production growth
            cty_curr  = prod_latest[prod_latest["country_iso"] == iso]
            cty_prior = prod_prior[prod_prior["country_iso"] == iso]
            if not cty_curr.empty and not cty_prior.empty:
                v_curr  = float(cty_curr.iloc[0]["value"])
                v_prior = float(cty_prior.iloc[0]["value"])
                flag    = cty_curr.iloc[0].get("flag")
                if not pd.isna(v_curr) and not pd.isna(v_prior) and v_prior != 0:
                    records.append({
                        "country_iso": iso, "mineral": mineral,
                        "metric_name": "yoy_growth",
                        "value": (v_curr - v_prior) / v_prior,
                        "year": latest_year, "flag": flag,
                    })

    return pd.DataFrame(records)


# ── Module-level USGS cache (fetched once per run) ────────────────────────────
_USGS_METRICS_CACHE: Optional[pd.DataFrame] = None

def _get_usgs_metrics() -> pd.DataFrame:
    """Fetch and cache USGS metrics for all minerals (called once per pipeline run)."""
    global _USGS_METRICS_CACHE
    if _USGS_METRICS_CACHE is not None:
        return _USGS_METRICS_CACHE

    print("[USGS] Fetching MCS data for all minerals...")
    all_dfs = []
    for mineral in MINERALS:
        df = fetch_usgs_mineral_csv(mineral)
        if not df.empty:
            all_dfs.append(df)

    usgs_long = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    _USGS_METRICS_CACHE = compute_usgs_metrics(usgs_long)
    print(f"[USGS] Metrics computed: {len(_USGS_METRICS_CACHE)} rows")
    return _USGS_METRICS_CACHE


# =============================================================================
# COMTRADE — PROCESSED & RAW EXPORT FLOWS
# =============================================================================

def _monthly_periods(start_yyyy: int, start_mm: int,
                     end_yyyy: int,   end_mm: int) -> list:
    out = []
    y, m = start_yyyy, start_mm
    while (y, m) <= (end_yyyy, end_mm):
        out.append(f"{y:04d}{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out

def _chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# Comtrade typically lags 2 months
_LAG_MONTHS = 2
def _back_month(today: datetime, n: int) -> tuple:
    y, m = today.year, today.month - n
    while m <= 0:
        m += 12; y -= 1
    return y, m

_PERIOD_END_YEAR, _PERIOD_END_MONTH = _back_month(_TODAY, _LAG_MONTHS)
_ALL_PERIODS = _monthly_periods(2020, 1, _PERIOD_END_YEAR, _PERIOD_END_MONTH)


def _comtrade_get(reporter_m49: str, hs_codes: list, periods: list,
                  flow: str = "X") -> pd.DataFrame:
    """Single Comtrade API call. Switches preview/premium based on COMTRADE_KEY."""
    try:
        import comtradeapicall as cta
    except ImportError:
        raise RuntimeError("comtradeapicall not installed. Run: pip install comtradeapicall")

    common = dict(
        typeCode="C", freqCode="M", clCode="HS",
        period=",".join(periods),
        reporterCode=reporter_m49,
        cmdCode=",".join(hs_codes),
        flowCode=flow,
        partnerCode="0",
        partner2Code=None, customsCode=None, motCode=None,
        format_output="JSON", breakdownMode="classic", includeDesc=True,
    )
    if COMTRADE_KEY:
        df = cta.getFinalData(COMTRADE_KEY, maxRecords=250000,
                              aggregateBy=None, countOnly=None, **common)
    else:
        df = cta.previewFinalData(maxRecords=500,
                                  aggregateBy=None, countOnly=None, **common)

    if df is None or len(df) == 0:
        return pd.DataFrame()
    return df


def fetch_comtrade_exports(reporter_m49: str, hs_codes: list,
                           all_periods: list) -> pd.DataFrame:
    """Fetch all months of export data for one reporter + HS code set."""
    results = []
    for batch in _chunked(all_periods, 12):
        try:
            df = _comtrade_get(reporter_m49, hs_codes, batch, flow="X")
            if not df.empty:
                results.append(df)
        except Exception as e:
            print(f"    [Comtrade] {reporter_m49}/{hs_codes[0]}…/{batch[0]}-{batch[-1]}: {e}")
        time.sleep(0.6 if not COMTRADE_KEY else 0.2)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def fetch_world_processed_exports_latest(year: int) -> dict:
    """Fetch world-aggregate processed exports (all reporters) for each mineral in `year`.

    Returns dict: mineral -> total USD
    """
    try:
        import comtradeapicall as cta
    except ImportError:
        raise RuntimeError("comtradeapicall not installed.")

    periods = _monthly_periods(year, 1, year, 12)
    world_totals = {}

    for mineral in MINERALS:
        hs_codes = HS_CODES[mineral]["processed"]
        total_usd = 0.0
        for batch in _chunked(periods, 12):
            common = dict(
                typeCode="C", freqCode="M", clCode="HS",
                period=",".join(batch),
                reporterCode="all",
                cmdCode=",".join(hs_codes),
                flowCode="X",
                partnerCode="0",
                partner2Code=None, customsCode=None, motCode=None,
                format_output="JSON", breakdownMode="classic", includeDesc=False,
            )
            try:
                if COMTRADE_KEY:
                    df = cta.getFinalData(COMTRADE_KEY, maxRecords=250000,
                                          aggregateBy=None, countOnly=None, **common)
                else:
                    df = cta.previewFinalData(maxRecords=500,
                                              aggregateBy=None, countOnly=None, **common)
                if df is not None and len(df) > 0:
                    total_usd += float(df["primaryValue"].sum())
            except Exception as e:
                print(f"    [Comtrade] world/{mineral}/{batch[0]}-{batch[-1]}: {e}")
            time.sleep(0.6 if not COMTRADE_KEY else 0.2)

        world_totals[mineral] = total_usd if total_usd > 0 else None

    return world_totals


# ── Module-level Comtrade cache ───────────────────────────────────────────────
_COMTRADE_ANNUAL_CACHE: Optional[pd.DataFrame] = None   # country, mineral, stage, year, usd
_WORLD_PROCESSED_CACHE: Optional[dict] = None           # mineral -> usd


def _get_comtrade_data() -> tuple:
    """Fetch and cache all Comtrade flows (called once per pipeline run)."""
    global _COMTRADE_ANNUAL_CACHE, _WORLD_PROCESSED_CACHE
    if _COMTRADE_ANNUAL_CACHE is not None:
        return _COMTRADE_ANNUAL_CACHE, _WORLD_PROCESSED_CACHE

    print(f"[Comtrade] Fetching monthly flows "
          f"({_ALL_PERIODS[0]} → {_ALL_PERIODS[-1]}) for all countries/minerals…")
    records = []
    total   = len(COUNTRIES) * len(MINERALS) * 2
    done    = 0
    for iso, meta in COUNTRIES.items():
        for mineral in MINERALS:
            for stage in ("processed", "raw"):
                done += 1
                hs   = HS_CODES[mineral][stage]
                print(f"  [{done}/{total}] {iso}/{mineral}/{stage}", end="  ", flush=True)
                df = fetch_comtrade_exports(meta["m49"], hs, _ALL_PERIODS)
                if not df.empty:
                    df["country_iso"] = iso
                    df["mineral"]     = mineral
                    df["stage"]       = stage
                    records.append(df)
                    print(f"({len(df)} rows)")
                else:
                    print("(no data)")

    if records:
        comtrade_long = pd.concat(records, ignore_index=True)
        comtrade_long["year"] = comtrade_long["period"].astype(str).str[:4].astype(int)
        _COMTRADE_ANNUAL_CACHE = (
            comtrade_long.groupby(["country_iso", "mineral", "stage", "year"],
                                  as_index=False)
            .agg(annual_value_usd=("primaryValue", "sum"))
        )
    else:
        _COMTRADE_ANNUAL_CACHE = pd.DataFrame(
            columns=["country_iso", "mineral", "stage", "year", "annual_value_usd"]
        )

    # World processed exports for the latest year (denominator for refining share)
    if not _COMTRADE_ANNUAL_CACHE.empty:
        latest_year = int(_COMTRADE_ANNUAL_CACHE["year"].max())
        print(f"[Comtrade] Fetching world processed totals for {latest_year}…")
        _WORLD_PROCESSED_CACHE = fetch_world_processed_exports_latest(latest_year)
    else:
        _WORLD_PROCESSED_CACHE = {}

    print(f"[Comtrade] Annual rows cached: {len(_COMTRADE_ANNUAL_CACHE)}")
    return _COMTRADE_ANNUAL_CACHE, _WORLD_PROCESSED_CACHE


def compute_refining_share(country_iso: str, mineral: str) -> Optional[float]:
    """Return country's share of world processed exports for the latest year."""
    annual, world = _get_comtrade_data()
    if annual.empty:
        return None
    latest = int(annual["year"].max())
    mask   = ((annual["country_iso"] == country_iso) &
              (annual["mineral"] == mineral) &
              (annual["stage"] == "processed") &
              (annual["year"] == latest))
    cty_val = annual[mask]["annual_value_usd"].sum()
    if cty_val == 0:
        return None
    world_val = (world or {}).get(mineral)
    if not world_val or world_val == 0:
        return None
    return float(cty_val / world_val)


def compute_value_add_ratio(country_iso: str, mineral: str) -> Optional[float]:
    """Return processed / (raw + processed) for the latest year."""
    annual, _ = _get_comtrade_data()
    if annual.empty:
        return None
    latest = int(annual["year"].max())
    base   = annual[(annual["country_iso"] == country_iso) &
                    (annual["mineral"] == mineral) &
                    (annual["year"] == latest)]
    proc_row = base[base["stage"] == "processed"]["annual_value_usd"].sum()
    raw_row  = base[base["stage"] == "raw"]["annual_value_usd"].sum()
    denom = proc_row + raw_row
    if denom == 0:
        return None
    return float(proc_row / denom)


def _comtrade_data_date() -> str:
    """Return the data_date string for Comtrade metrics (YYYY-12-31 of latest year)."""
    annual, _ = _get_comtrade_data()
    if annual.empty:
        return date.today().isoformat()
    latest = int(annual["year"].max())
    return f"{latest}-12-31"


def _usgs_data_date() -> str:
    """Return the data_date string for USGS metrics (YYYY-12-31 of latest year)."""
    df = _get_usgs_metrics()
    if df.empty or "year" not in df.columns:
        return date.today().isoformat()
    latest = int(df["year"].max())
    return f"{latest}-12-31"


# =============================================================================
# COLLECTOR FUNCTIONS  (called by cascade; each returns a make_metric_result dict)
# =============================================================================

def _usgs_source_url() -> str:
    year = _SB_PARENT_YEAR or _MCS_YEAR_DEFAULT
    return f"https://www.sciencebase.gov/catalog/items?parentId={_SB_PARENT_ID or ''}&q=MCS{year}"


def collect_usgs_production_share(country_iso: str, metric_key: str, **_) -> dict:
    mineral = METRICS[metric_key]["mineral"]
    df = _get_usgs_metrics()
    row = df[(df["country_iso"] == country_iso) &
             (df["mineral"] == mineral) &
             (df["metric_name"] == "production_share")]
    if row.empty:
        raise ValueError(f"No USGS production_share for {country_iso}/{mineral}")
    val  = float(row.iloc[0]["value"])
    flag = row.iloc[0].get("flag")
    if flag == "W" or np.isnan(val):
        raise ValueError(f"Withheld value (W) for {country_iso}/{mineral} production_share")
    conf = CONFIDENCE["api_est"] if flag == "EST" else CONFIDENCE["api_annual"]
    return make_metric_result(
        country_iso, metric_key, val, METRICS[metric_key]["unit"],
        _usgs_data_date(), "annual",
        f"USGS Mineral Commodity Summaries {_SB_PARENT_YEAR or _MCS_YEAR_DEFAULT}",
        _usgs_source_url(), "api_annual", conf,
        raw_value=str(val), mineral=mineral,
    )


def collect_usgs_reserves_share(country_iso: str, metric_key: str, **_) -> dict:
    mineral = METRICS[metric_key]["mineral"]
    df = _get_usgs_metrics()
    row = df[(df["country_iso"] == country_iso) &
             (df["mineral"] == mineral) &
             (df["metric_name"] == "reserves_share")]
    if row.empty:
        raise ValueError(f"No USGS reserves_share for {country_iso}/{mineral}")
    val  = float(row.iloc[0]["value"])
    flag = row.iloc[0].get("flag")
    if flag == "W" or np.isnan(val):
        raise ValueError(f"Withheld value for {country_iso}/{mineral} reserves_share")
    conf = CONFIDENCE["api_est"] if flag == "EST" else CONFIDENCE["api_annual"]
    return make_metric_result(
        country_iso, metric_key, val, METRICS[metric_key]["unit"],
        _usgs_data_date(), "annual",
        f"USGS Mineral Commodity Summaries {_SB_PARENT_YEAR or _MCS_YEAR_DEFAULT}",
        _usgs_source_url(), "api_annual", conf,
        raw_value=str(val), mineral=mineral,
    )


def collect_usgs_yoy_growth(country_iso: str, metric_key: str, **_) -> dict:
    mineral = METRICS[metric_key]["mineral"]
    df = _get_usgs_metrics()
    row = df[(df["country_iso"] == country_iso) &
             (df["mineral"] == mineral) &
             (df["metric_name"] == "yoy_growth")]
    if row.empty:
        raise ValueError(f"No USGS yoy_growth for {country_iso}/{mineral}")
    val  = float(row.iloc[0]["value"])
    flag = row.iloc[0].get("flag")
    if flag == "W" or np.isnan(val):
        raise ValueError(f"Withheld value for {country_iso}/{mineral} yoy_growth")
    conf = CONFIDENCE["api_est"] if flag == "EST" else CONFIDENCE["api_annual"]
    return make_metric_result(
        country_iso, metric_key, val, METRICS[metric_key]["unit"],
        _usgs_data_date(), "annual",
        f"USGS Mineral Commodity Summaries {_SB_PARENT_YEAR or _MCS_YEAR_DEFAULT}",
        _usgs_source_url(), "api_annual", conf,
        raw_value=str(val), mineral=mineral,
    )


def collect_comtrade_refining_share(country_iso: str, metric_key: str, **_) -> dict:
    mineral = METRICS[metric_key]["mineral"]
    val = compute_refining_share(country_iso, mineral)
    if val is None:
        raise ValueError(f"No Comtrade refining_share for {country_iso}/{mineral}")
    return make_metric_result(
        country_iso, metric_key, val, METRICS[metric_key]["unit"],
        _comtrade_data_date(), "annual",
        "UN Comtrade (processed exports, HS6)",
        "https://comtradeplus.un.org", "api_annual", CONFIDENCE["api_annual"],
        raw_value=str(val), mineral=mineral,
    )


def collect_comtrade_value_add(country_iso: str, metric_key: str, **_) -> dict:
    mineral = METRICS[metric_key]["mineral"]
    val = compute_value_add_ratio(country_iso, mineral)
    if val is None:
        raise ValueError(f"No Comtrade value_add_ratio for {country_iso}/{mineral}")
    return make_metric_result(
        country_iso, metric_key, val, METRICS[metric_key]["unit"],
        _comtrade_data_date(), "annual",
        "UN Comtrade (raw + processed exports, HS6)",
        "https://comtradeplus.un.org", "api_annual", CONFIDENCE["api_annual"],
        raw_value=str(val), mineral=mineral,
    )


# =============================================================================
# METRIC CASCADE
# =============================================================================

METRIC_CASCADE: dict = {}

for _iso in COUNTRIES:
    for _mineral in MINERALS:
        # USGS-based metrics
        for _metric, _collector in [
            ("production_share", collect_usgs_production_share),
            ("reserves_share",   collect_usgs_reserves_share),
            ("yoy_growth",       collect_usgs_yoy_growth),
        ]:
            _key = f"{_metric}_{_mineral}"
            METRIC_CASCADE[(_iso, _key)] = [
                {"name": f"USGS MCS ScienceBase CSV ({_mineral})",
                 "fn":   _collector,
                 "kwargs": {}},
            ]

        # Comtrade-based metrics
        for _metric, _collector in [
            ("refining_share",  collect_comtrade_refining_share),
            ("value_add_ratio", collect_comtrade_value_add),
        ]:
            _key = f"{_metric}_{_mineral}"
            METRIC_CASCADE[(_iso, _key)] = [
                {"name": f"Comtrade API (HS6, {_mineral})",
                 "fn":   _collector,
                 "kwargs": {}},
            ]

print(f"METRIC_CASCADE: {len(METRIC_CASCADE)} entries "
      f"({len(COUNTRIES)} countries × {len(MINERALS)} minerals × 5 metrics)")

# =============================================================================
# DB HELPERS — FK-based schema (si3_countries / si3_minerals / si3_metric_definitions)
# =============================================================================

_STALE_THRESHOLDS = {
    "api_annual":    400,
    "file_download": 90,
    "web_scrape":    90,
    "pdf_extract":   180,
    "imputed":       180,
}

# ── Schema bootstrap ──────────────────────────────────────────────────────────
_SCHEMA_SQL_PATH = os.path.join(os.path.dirname(__file__), "api_pipeline.sql")

def _ensure_schema(conn):
    """If si3_countries doesn't exist, apply api_pipeline.sql to bootstrap the schema."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                 WHERE table_schema='public' AND table_name='si3_countries'
            )
        """)
        exists = cur.fetchone()[0]
    if exists:
        return
    if not os.path.exists(_SCHEMA_SQL_PATH):
        raise RuntimeError(
            f"SI3 schema not found at {_SCHEMA_SQL_PATH}. "
            "Apply manually: psql -d subindex_3 -f api_pipeline.sql"
        )
    print(f"[SI3] Bootstrapping schema from {os.path.basename(_SCHEMA_SQL_PATH)}…")
    with open(_SCHEMA_SQL_PATH, "r") as fh:
        sql = fh.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("[SI3] Schema applied — si3_* tables ready.")


# Dimension ID cache (loaded once per process)
_dim_cache = {"country": {}, "mineral": {}, "metric": {}}

def _load_dim_cache(conn):
    if _dim_cache["country"]:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT id, iso3 FROM si3_countries")
        _dim_cache["country"] = {iso3: cid for cid, iso3 in cur.fetchall()}
        cur.execute("SELECT id, usgs_slug FROM si3_minerals")
        # Pipeline uses underscore slugs ('rare_earths'); schema uses dashes ('rare-earths')
        for mid, slug in [(r[0], r[1]) for r in cur.fetchall()] if False else []:
            pass
        cur.execute("SELECT id, usgs_slug FROM si3_minerals")
        _dim_cache["mineral"] = {slug: mid for mid, slug in cur.fetchall()}
        cur.execute("SELECT id, metric_code FROM si3_metric_definitions")
        _dim_cache["metric"] = {code: mid for mid, code in cur.fetchall()}


def _country_id(conn, country_iso):
    _load_dim_cache(conn)
    iso3 = COUNTRIES[country_iso]["iso3"]
    return _dim_cache["country"].get(iso3)

def _mineral_id(conn, mineral_underscore):
    """mineral_underscore is the pipeline's slug (e.g. 'rare_earths').
    Schema stores dash form ('rare-earths') in si3_minerals.usgs_slug."""
    _load_dim_cache(conn)
    schema_slug = MINERAL_SLUGS[mineral_underscore]
    return _dim_cache["mineral"].get(schema_slug)

def _metric_id(conn, base_metric):
    _load_dim_cache(conn)
    return _dim_cache["metric"].get(base_metric)


def _split_metric_key(metric_key):
    """Return (base_metric, mineral_underscore) from a composite metric_key.
    e.g. 'production_share_copper' -> ('production_share', 'copper')."""
    info = METRICS[metric_key]
    return info["base_metric"], info["mineral"]


def log_attempt(conn, run_id_int, country_iso, metric_key, collector_name,
                step, status, source_url, error_type, error_msg, duration_ms):
    """Insert into si3_collection_log (FK-based)."""
    base_metric, mineral_us = _split_metric_key(metric_key)
    period = date(int(_period_year_for(metric_key)), 1, 1)
    # Map cascade-style status to schema status enum
    status_map = {"success": "success", "failed": "parse_error"}
    sch_status = status_map.get(status, "parse_error")
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si3_collection_log
                (run_id, country_id, mineral_id, metric_id,
                 period_start, status, error_message, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (run_id_int,
              _country_id(conn, country_iso),
              _mineral_id(conn, mineral_us),
              _metric_id(conn, base_metric),
              period, sch_status,
              (f"[{collector_name}] {error_type}: {error_msg}"
               if error_msg else (collector_name if status != "success" else None)),
              duration_ms))
    conn.commit()


def _period_year_for(metric_key):
    """Best-effort latest data year from in-memory caches; falls back to today's year-1."""
    base, _ = _split_metric_key(metric_key)
    try:
        if base in ("production_share", "reserves_share", "yoy_growth"):
            df = _get_usgs_metrics()
            if not df.empty:
                return int(df["year"].max())
        else:
            annual, _ = _get_comtrade_data()
            if not annual.empty:
                return int(annual["year"].max())
    except Exception:
        pass
    return _TODAY.year - 1


def store_metric_datapoint(conn, dp: dict, run_id_int: int):
    """Insert verbatim row into si3_raw_metrics + upsert into si3_annual_metrics."""
    country_iso = dp["country_iso"]
    metric_key  = dp["metric_key"]
    base_metric, mineral_us = _split_metric_key(metric_key)
    cid = _country_id(conn, country_iso)
    mid = _mineral_id(conn, mineral_us)
    metric_id = _metric_id(conn, base_metric)

    period_start = date.fromisoformat(dp["data_date"][:10])
    year = period_start.year

    flag = None
    raw_str = dp.get("raw_value") or ""
    if "EST" in raw_str.upper():
        flag = "EST"

    payload = {
        "metric_key":      metric_key,
        "metric_label":    dp.get("metric_label"),
        "metric_value":    dp["metric_value"],
        "unit":            dp["unit"],
        "data_date":       dp["data_date"],
        "data_frequency":  dp["data_frequency"],
        "source_name":     dp["source_name"],
        "source_url":      dp["source_url"],
        "access_method":   dp["access_method"],
        "confidence_score": dp["confidence_score"],
        "raw_value":       dp.get("raw_value"),
    }

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si3_raw_metrics
                (run_id, country_id, mineral_id, metric_id,
                 period_start, granularity, raw_value, raw_unit, raw_flag,
                 raw_payload, ingestion_status, transformed_at)
            VALUES (%s, %s, %s, %s, %s, 'annual', %s, %s, %s,
                    %s::jsonb, 'transformed', NOW())
            RETURNING id
        """, (run_id_int, cid, mid, metric_id, period_start,
              dp["metric_value"], dp["unit"], flag,
              psycopg2.extras.Json(payload)))
        raw_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO si3_annual_metrics
                (country_id, mineral_id, metric_id, year, value, unit, flag, raw_metric_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (country_id, mineral_id, metric_id, year) DO UPDATE SET
                value          = EXCLUDED.value,
                unit           = EXCLUDED.unit,
                flag           = EXCLUDED.flag,
                raw_metric_id  = EXCLUDED.raw_metric_id,
                transformed_at = NOW()
        """, (cid, mid, metric_id, year, dp["metric_value"], dp["unit"], flag, raw_id))
    conn.commit()


def open_gap(conn, run_id_int, country_iso, metric_key,
             failure_reason, collectors_tried, severity):
    base_metric, mineral_us = _split_metric_key(metric_key)
    cid = _country_id(conn, country_iso)
    mid = _mineral_id(conn, mineral_us)
    metric_id = _metric_id(conn, base_metric)
    period = date(int(_period_year_for(metric_key)), 1, 1)
    notes  = (f"Tried: {', '.join(collectors_tried)} | "
              f"Errors: {failure_reason}")[:1000]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si3_data_gaps
                (country_id, mineral_id, metric_id, period_start,
                 gap_type, severity, detected_in_run, notes)
            VALUES (%s, %s, %s, %s, 'missing', %s, %s, %s)
            ON CONFLICT (country_id, mineral_id, metric_id, period_start, gap_type)
            DO UPDATE SET
                severity        = EXCLUDED.severity,
                detected_in_run = EXCLUDED.detected_in_run,
                notes           = EXCLUDED.notes,
                detected_at     = NOW(),
                is_resolved     = FALSE
        """, (cid, mid, metric_id, period, severity, run_id_int, notes))
    conn.commit()


def _data_is_stale(conn, country_iso: str, metric_key: str) -> tuple:
    """Return (is_stale, age_days, existing_value) from si3_annual_metrics."""
    base_metric, mineral_us = _split_metric_key(metric_key)
    cid = _country_id(conn, country_iso)
    mid = _mineral_id(conn, mineral_us)
    metric_id = _metric_id(conn, base_metric)
    if not (cid and mid and metric_id):
        return True, None, None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.value, a.transformed_at,
                   r.raw_payload->>'access_method' AS access_method
              FROM si3_annual_metrics a
              LEFT JOIN si3_raw_metrics r ON r.id = a.raw_metric_id
             WHERE a.country_id=%s AND a.mineral_id=%s AND a.metric_id=%s
             ORDER BY a.year DESC LIMIT 1
        """, (cid, mid, metric_id))
        row = cur.fetchone()
    if not row:
        return True, None, None
    value, transformed_at, access_method = row
    if transformed_at.tzinfo is not None:
        transformed_at = transformed_at.replace(tzinfo=None)
    age_days  = (datetime.now(timezone.utc).replace(tzinfo=None) - transformed_at).days
    threshold = _STALE_THRESHOLDS.get(access_method or "api_annual", 400)
    return age_days > threshold, age_days, value


def _fresh_conn():
    return psycopg2.connect(**DB_CONFIG)


# =============================================================================
# RESEARCH AGENT FALLBACK
# =============================================================================

def collect_research_si3(country_iso: str, metric_key: str,
                          confidence: float = CONFIDENCE["research_agent"]) -> dict:
    """Deep-research collector for SI3 metrics."""
    m = METRICS[metric_key]
    result = _run_deep_research(
        country_iso  = country_iso,
        metric_key   = metric_key,
        country_name = COUNTRIES[country_iso]["name"],
        currency     = COUNTRIES[country_iso].get("currency", "USD"),
        metric_label = m["label"],
        metric_unit  = m["unit"],
        fx_rates     = {},
        trusted_urls = None,
    )
    val = result.get("value")
    if val is None:
        raise ValueError(f"Research agent returned null for {metric_key}/{country_iso}")
    return make_metric_result(
        country_iso, metric_key, float(val), m["unit"],
        result.get("data_date") or date.today().isoformat(),
        result.get("frequency", "annual"),
        f"Deep research agent — {country_iso}/{metric_key}",
        result.get("source_url", ""),
        "web_scrape", confidence,
        raw_value=result.get("raw_text", ""),
        mineral=m["mineral"],
    )


def _try_research_agent(conn, run_id, country_iso, metric_key,
                        step_num, errors, tried) -> bool:
    """Run the research agent as universal last resort. Returns True on success."""
    has_search = (
        (TAVILY_API_KEY and TAVILY_API_KEY != "your_tavily_key_here")
        or JINA_API_KEY or BRAVE_API_KEY
    )
    if not has_search:
        return False
    agent_name = "Research Agent (Tavily/Brave + Claude)"
    tried.append(agent_name)
    t0 = time.perf_counter()
    try:
        dp      = collect_research_si3(country_iso, metric_key)
        elapsed = int((time.perf_counter() - t0) * 1000)
        fresh   = _fresh_conn()
        try:
            log_attempt(fresh, run_id, country_iso, metric_key, agent_name,
                        step_num, "success", dp.get("source_url"),
                        None, None, elapsed)
            store_metric_datapoint(fresh, dp, run_id)
            print(f"  ✓ [{country_iso}] {metric_key} = {dp['metric_value']:.4f} "
                  f"{dp.get('unit','')} | src={agent_name} conf={dp['confidence_score']:.2f}")
        finally:
            fresh.close()
        return True
    except Exception as exc:
        elapsed = int((time.perf_counter() - t0) * 1000)
        err_msg = str(exc)[:500]
        errors.append(f"[{agent_name}] {type(exc).__name__}: {err_msg}")
        try:
            fresh = _fresh_conn()
            log_attempt(fresh, run_id, country_iso, metric_key, agent_name,
                        step_num, "failed", None,
                        type(exc).__name__, err_msg, elapsed)
            fresh.close()
        except Exception:
            pass
        print(f"  ✗ [{country_iso}] {metric_key} — {agent_name}: {err_msg[:80]}")
        return False


# =============================================================================
# CASCADE RUNNER
# =============================================================================

def run_cascade(conn, run_id: str, country_iso: str, metric_key: str) -> bool:
    """
    Try each cascade step for (country_iso, metric_key).
    Research agent is the universal last resort.
    Returns True on first success, False if everything fails.
    """
    steps  = METRIC_CASCADE.get((country_iso, metric_key), [])
    errors, tried = [], []

    # Staleness short-circuit
    is_stale, age_days, existing_val = _data_is_stale(conn, country_iso, metric_key)
    if not is_stale and existing_val is not None:
        print(f"  [FRESH] ({country_iso}, {metric_key}) — {age_days}d old, skipping")
        return True
    if age_days is not None:
        print(f"  [STALE {age_days}d] ({country_iso}, {metric_key}) — refreshing...")

    # Run cascade collectors
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
            store_metric_datapoint(conn, dp, run_id)
            print(
                f"  ✓ [{country_iso}] {metric_key} | "
                f"value={dp.get('metric_value'):.4f} {dp.get('unit','')} "
                f"date={dp.get('data_date')} | "
                f"src={name} conf={dp['confidence_score']:.2f}"
            )
            return True
        except Exception as exc:
            elapsed  = int((time.perf_counter() - t0) * 1000)
            err_type = type(exc).__name__
            err_msg  = str(exc)[:500]
            errors.append(f"[{name}] {err_type}: {err_msg}")
            log_attempt(conn, run_id, country_iso, metric_key, name, step_num,
                        "failed", None, err_type, err_msg, elapsed)
            print(f"  ✗ [{country_iso}] {metric_key} — {name}: {err_type}: {err_msg[:80]}")

    # Universal research-agent fallback
    if steps:
        print(f"  [AGENT] All {len(steps)} collector(s) failed — trying research agent...")
    else:
        print(f"  [AGENT] No cascade defined — trying research agent directly...")
    if _try_research_agent(conn, run_id, country_iso, metric_key,
                           len(steps) + 1, errors, tried):
        return True

    # Everything failed → open gap
    try:
        fresh = _fresh_conn()
        open_gap(fresh, run_id, country_iso, metric_key,
                 " | ".join(errors), tried,
                 METRICS[metric_key]["gap_severity"])
        fresh.close()
    except Exception:
        pass
    print(f"  ✗✗ GAP: ({country_iso}, {metric_key}) — all {len(tried)} collector(s) failed")
    return False


# =============================================================================
# PIPELINE ENTRY POINT
# =============================================================================

def run_pipeline(countries=None, metrics=None):
    """
    Run the full SI3 collection pipeline.

    Args:
        countries: list of ISO-2 codes, or None for all 6.
        metrics:   list of metric_keys (e.g. 'production_share_copper'),
                   or None for all 30 (6 minerals × 5 metrics).

    Returns:
        run_id (str)
    """
    target_countries = countries or list(COUNTRIES.keys())
    target_metrics   = metrics   or list(METRICS.keys())
    started_at       = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_conn()
    _ensure_schema(conn)
    _load_dim_cache(conn)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si3_collection_runs (pipeline_name, triggered_by, status)
            VALUES ('si3_pipeline', 'manual', 'running')
            RETURNING id, run_uuid
        """)
        run_id_int, run_uuid = cur.fetchone()
    conn.commit()
    run_id = str(run_uuid)

    combos    = [(c, m) for c in target_countries for m in target_metrics]
    total     = len(combos)
    succeeded = 0
    failed    = 0

    print(f"\n{'='*65}")
    print(f"SI3 Pipeline  Run ID: {run_id}")
    print(f"Countries: {target_countries}")
    print(f"Tasks: {total}  "
          f"({len(target_countries)} countries × {len(target_metrics)} metrics)")
    print(f"{'='*65}\n")

    # Pre-warm caches once before the cascade loop:
    # This triggers one USGS fetch + all Comtrade fetches up front, so individual
    # collectors hit the in-memory cache rather than making repeated API calls.
    print("[SI3] Pre-warming data caches (USGS + Comtrade)…")
    try:
        _get_usgs_metrics()
    except Exception as e:
        print(f"  [WARN] USGS pre-warm failed: {e}")
    try:
        _get_comtrade_data()
    except Exception as e:
        print(f"  [WARN] Comtrade pre-warm failed: {e}")
    print("[SI3] Caches ready. Starting cascade…\n")

    for idx, (country_iso, metric_key) in enumerate(combos, start=1):
        print(f"\n[{idx}/{total}] {country_iso} / {metric_key}")
        ok = run_cascade(conn, run_id_int, country_iso, metric_key)
        if ok:
            succeeded += 1
        else:
            failed += 1

    finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
    elapsed     = (finished_at - started_at).total_seconds()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM si3_data_gaps WHERE NOT is_resolved")
        gaps_open = cur.fetchone()[0]
        status = "success" if failed == 0 else ("partial" if succeeded > 0 else "failed")
        cur.execute("""
            UPDATE si3_collection_runs
               SET finished_at=%s, status=%s,
                   rows_attempted=%s, rows_succeeded=%s, rows_failed=%s,
                   notes=%s
             WHERE id=%s
        """, (finished_at, status, total, succeeded, failed,
              f"gaps_opened={gaps_open}", run_id_int))
    conn.commit()
    conn.close()

    print(f"\n{'='*65}")
    print(f"SI3 Run complete in {elapsed:.1f}s")
    print(f"  Run ID:    {run_id}")
    print(f"  Succeeded: {succeeded}/{total}")
    print(f"  Failed:    {failed}/{total}")
    print(f"  Open gaps: {gaps_open}")
    print(f"{'='*65}")
    return run_id


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="SI3 Critical Mineral Endowment Pipeline"
    )
    parser.add_argument(
        "--countries", nargs="+",
        choices=list(COUNTRIES.keys()),
        help="Restrict run to specific countries (default: all 6)",
    )
    parser.add_argument(
        "--minerals", nargs="+",
        choices=MINERALS,
        help="Restrict run to specific minerals (default: all 6)",
    )
    parser.add_argument(
        "--metrics", nargs="+",
        choices=list(_BASE_METRICS.keys()),
        help="Restrict run to specific base metric types (default: all 5)",
    )
    args = parser.parse_args()

    # Build target metric_key list from optional mineral/metric filters
    target_metrics = None
    if args.minerals or args.metrics:
        filtered_minerals = args.minerals or MINERALS
        filtered_metrics  = args.metrics or list(_BASE_METRICS.keys())
        target_metrics = [
            f"{m}_{mn}"
            for m in filtered_metrics
            for mn in filtered_minerals
        ]

    run_pipeline(
        countries=args.countries,
        metrics=target_metrics,
    )
