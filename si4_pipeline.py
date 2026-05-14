"""
SI4 Food Sub-Index Pipeline
============================
Matches the cascade + research agent fallback architecture of 02_pipeline.py / si2_pipeline.py.

Metrics:
  net_food_trade_balance         — USDA FATUS / Comtrade / World Bank
  caloric_self_sufficiency_ratio — FAOSTAT Food Balance Sheets
  share_global_staple_exports    — Comtrade HS4 basket / FAOSTAT TCL
  arable_land_per_capita         — FAOSTAT Land Use ÷ World Bank population

DB: subindex_4  (same PostgreSQL server as SI1/SI2, same SSH tunnel on port 5433)
"""

from dotenv import load_dotenv
load_dotenv()

import os, psycopg2, psycopg2.extras
from pathlib import Path

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   os.environ.get("POSTGRES_DB", "gramercy_workstream1"),
    "user":     os.environ.get("SI4_POSTGRES_USER",     os.environ.get("POSTGRES_USER", "shankar_1")),
    "password": os.environ.get("SI4_POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")),
}

TAVILY_API_KEY    = os.environ.get("TAVILY_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

from research_agent import run_research_agent as _run_deep_research, get_token_usage as _agent_token_usage

COUNTRIES = {
    "US": {"name": "United States",   "currency": "USD"},
    "AE": {"name": "UAE",             "currency": "USD"},
    "BR": {"name": "Brazil",          "currency": "USD"},
    "IN": {"name": "India",           "currency": "USD"},
    "SG": {"name": "Singapore",       "currency": "USD"},
    "PH": {"name": "Philippines",     "currency": "USD"},
}

METRICS = {
    "net_food_trade_balance":         {"label": "Net Food Trade Balance",                 "unit": "USD",       "gap_severity": "high"},
    "caloric_self_sufficiency_ratio": {"label": "Caloric Self-Sufficiency Ratio",         "unit": "ratio",     "gap_severity": "high"},
    "share_global_staple_exports":    {"label": "Share of Global Exports in Key Staples", "unit": "%",         "gap_severity": "medium"},
    "arable_land_per_capita":         {"label": "Arable Land per Capita",                 "unit": "ha/person", "gap_severity": "medium"},
}

CONFIDENCE = {
    "api_monthly":   1.00,
    "api_quarterly": 0.92,
    "api_annual":    0.88,
    "file_download": 0.75,
    "web_scrape":    0.60,
    "pdf_regex":     0.45,
    "gemini":        0.40,
    "imputed":       0.30,
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CSI-WS1-Pipeline/1.0; Research)"}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

print(f"[SI4] host={DB_CONFIG['host']}  port={DB_CONFIG['port']}  user={DB_CONFIG['user']}  db={DB_CONFIG['dbname']}")

# In[2]:


# ── ALL COLLECTOR FUNCTIONS ────────────────────────────────────────────────────
import requests as _requests, io, re, json, time, zipfile as _zipfile
from collections import defaultdict as _defaultdict
from datetime import date, datetime
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd

_CACHE_DIR = Path(__file__).parent / "fao_cache"
_CACHE_DIR.mkdir(exist_ok=True)

_FAO_AREA = {
    "US": "United States of America", "BR": "Brazil", "IN": "India",
    "SG": "Singapore", "PH": "Philippines", "AE": "United Arab Emirates",
}
_WB_ISO3 = {"US": "USA", "BR": "BRA", "IN": "IND", "SG": "SGP", "PH": "PHL", "AE": "ARE"}
_COMTRADE_REPORTERS = {"US": 842, "BR": 76, "IN": 699, "SG": 702, "PH": 608, "AE": 784}

_fbs_cache = None
_tcl_cache = None
_lu_cache  = None

_COMTRADE_ANNUAL_BASE  = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"
_COMTRADE_MONTHLY_BASE = "https://comtradeapi.un.org/public/v1/preview/C/M/HS"
HS_FOOD_CHAPTERS = ",".join(str(i).zfill(2) for i in range(1, 25))
WB_BASE = "https://api.worldbank.org/v2/country"

_HS4_CODES_STR = "1001,1003,1005,1006,1201,1205,1206,1507,1511,1701"

_TCL_STAPLES_ALIASES = {
    "maize":          ["Maize (corn)", "Maize"],
    "wheat":          ["Wheat"],
    "soybeans":       ["Soya beans", "Soybeans"],
    "palm_oil":       ["Oil of palm", "Palm oil"],
    "rice":           ["Rice", "Rice, paddy"],
    "sugar":          ["Sugar Raw Centrifugal", "Raw cane or beet sugar (centrifugal only)",
                       "Sugar, raw centrifugal", "Sugar raw centrifugal"],
    "barley":         ["Barley"],
    "soybean_oil":    ["Oil of soya beans", "Soybean oil"],
    "rapeseed":       ["Rape or colza seed", "Rapeseed", "Rapeseed or canola seed"],
    "sunflower_seed": ["Sunflower seed", "Sunflower seeds"],
}

_FBS_URL = "https://bulks-faostat.fao.org/production/FoodBalanceSheets_E_All_Data_(Normalized).zip"
_TCL_URL = "https://bulks-faostat.fao.org/production/Trade_CropsLivestock_E_All_Data_(Normalized).zip"
_LU_URL  = "https://bulks-faostat.fao.org/production/Inputs_LandUse_E_All_Data_(Normalized).zip"

_FAO_DL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept": "application/zip,application/octet-stream,*/*",
    "Referer": "https://www.fao.org/faostat/en/",
}

# ── Result builders ───────────────────────────────────────────────────────────

def make_trade_result(country_iso, metric_key, exports_usd, imports_usd,
                      trade_balance_usd, data_date, data_frequency,
                      source_name, source_url, access_method, confidence_score) -> dict:
    if trade_balance_usd is None and exports_usd is not None and imports_usd is not None:
        trade_balance_usd = exports_usd - imports_usd
    return {
        "country_iso": country_iso, "country_name": COUNTRIES[country_iso]["name"],
        "metric_key": metric_key, "exports_usd": exports_usd, "imports_usd": imports_usd,
        "trade_balance_usd": trade_balance_usd, "data_date": data_date,
        "data_frequency": data_frequency, "source_name": source_name,
        "source_url": source_url, "access_method": access_method,
        "confidence_score": confidence_score,
    }

def make_metric_result(country_iso, metric_key, metric_value, unit, data_date,
                       data_frequency, source_name, source_url, access_method,
                       confidence_score, raw_value=None) -> dict:
    return {
        "country_iso": country_iso, "country_name": COUNTRIES[country_iso]["name"],
        "metric_key": metric_key, "metric_label": METRICS[metric_key]["label"],
        "metric_value": metric_value, "unit": unit, "data_date": data_date,
        "data_frequency": data_frequency, "source_name": source_name,
        "source_url": source_url, "access_method": access_method,
        "confidence_score": confidence_score, "raw_value": raw_value,
    }

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def fetch_html(url, timeout=30):
    for attempt in range(1, 4):
        try:
            r = _requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status(); return r.text
        except Exception:
            if attempt == 3: raise
            time.sleep(2 * attempt)

def download_file(url, timeout=120):
    for attempt in range(1, 4):
        try:
            r = _requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status(); return r.content
        except Exception:
            if attempt == 3: raise
            time.sleep(3 * attempt)

def _fao_bulk_csv(url, cache_csv, csv_candidates):
    if cache_csv.exists():
        return cache_csv
    print(f"  Downloading {url.split('/')[-1]} …")
    r = _requests.get(url, timeout=600, headers=_FAO_DL_HEADERS)
    r.raise_for_status()
    with _zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = zf.namelist()
        csv_name = next((n for n in csv_candidates if n in names), None) or \
                   next((n for n in names if n.lower().endswith(".csv")), None)
        if not csv_name: raise ValueError(f"No CSV in ZIP: {names}")
        zf.extract(csv_name, path=_CACHE_DIR)
    (_CACHE_DIR / csv_name).rename(cache_csv)
    print(f"  Cached → {cache_csv} ({cache_csv.stat().st_size/1e6:.0f} MB)")
    return cache_csv

# ── FAO loaders ───────────────────────────────────────────────────────────────

def _load_fbs():
    global _fbs_cache
    if _fbs_cache is not None: return _fbs_cache
    csv = _fao_bulk_csv(_FBS_URL, _CACHE_DIR/"FBS_normalized.csv",
                        ["FoodBalanceSheets_E_All_Data_(Normalized).csv"])
    keep_elems = {"Production","Domestic supply quantity","Food supply (kcal/capita/day)"}
    df = pd.read_csv(csv, encoding="latin-1",
                     usecols=["Area","Item","Element","Year","Value"],
                     dtype={"Area":"string","Item":"string","Element":"string","Year":"int32"})
    elem_map = {e: t for e in df["Element"].dropna().unique()
                for t in keep_elems if e.strip().lower()==t.strip().lower() and e!=t}
    if elem_map: df["Element"] = df["Element"].replace(elem_map)
    df = df[df["Area"].isin(set(_FAO_AREA.values())) & df["Element"].isin(keep_elems)].copy()
    _fbs_cache = df
    print(f"  FBS loaded: {len(df):,} rows for {df['Area'].nunique()} countries")
    return df

def _load_tcl():
    global _tcl_cache
    if _tcl_cache is not None: return _tcl_cache
    csv = _fao_bulk_csv(_TCL_URL, _CACHE_DIR/"TCL_normalized.csv",
                        ["Trade_CropsLivestock_E_All_Data_(Normalized).csv",
                         "Trade_Crops_Livestock_E_All_Data_(Normalized).csv"])
    all_areas = set(_FAO_AREA.values()) | {"World","World + (Total)","World, FAO"}
    df = pd.read_csv(csv, encoding="latin-1",
                     usecols=["Area","Item","Element","Year","Value"],
                     dtype={"Area":"string","Item":"string","Element":"string","Year":"int32"})
    df = df[df["Area"].isin(all_areas)].copy()
    for e in df["Element"].dropna().unique():
        if e.strip().lower()=="export value" and e!="Export Value":
            df["Element"] = df["Element"].replace({e:"Export Value"})
    df = df[df["Element"]=="Export Value"].copy()
    _tcl_cache = df
    print(f"  TCL loaded: {len(df):,} rows")
    return df

def _load_landuse():
    global _lu_cache
    if _lu_cache is not None: return _lu_cache
    csv = _fao_bulk_csv(_LU_URL, _CACHE_DIR/"LandUse_normalized.csv",
                        ["Inputs_LandUse_E_All_Data_(Normalized).csv"])
    df = pd.read_csv(csv, encoding="latin-1",
                     usecols=["Area","Item","Element","Year","Value"],
                     dtype={"Area":"string","Item":"string","Element":"string","Year":"int32"})
    item_match = next((i for i in df["Item"].dropna().unique() if i.strip().lower()=="arable land"), None)
    elem_match = next((e for e in df["Element"].dropna().unique() if e.strip().lower()=="area"), None)
    if not item_match or not elem_match: raise ValueError("FAO Land Use: Arable land/Area not found")
    df = df[df["Area"].isin(set(_FAO_AREA.values())) &
            (df["Item"]==item_match) & (df["Element"]==elem_match)].copy()
    _lu_cache = df
    print(f"  Land Use loaded: {len(df):,} rows for {df['Area'].nunique()} countries")
    return df

# ── World Bank helper ─────────────────────────────────────────────────────────

def _wb_indicator(country_code, indicator, mrv=5):
    r = _requests.get(f"{WB_BASE}/{country_code}/indicator/{indicator}?format=json&mrv={mrv}",
                      headers=HEADERS, timeout=45)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list) and len(data) > 1:
        for rec in data[1]:
            if rec.get("value") is not None:
                return float(rec["value"]), int(rec["date"])
    raise ValueError(f"World Bank: no data for {country_code}/{indicator}")

# ── Comtrade helpers ──────────────────────────────────────────────────────────

def _recent_ym(n=6):
    today = date.today(); y, m = today.year, today.month; out = []
    for _ in range(n):
        out.append(f"{y:04d}{m:02d}"); m -= 1
        if m == 0: m = 12; y -= 1
    return out

def _comtrade_get(url):
    for attempt in range(1, 4):
        try:
            r = _requests.get(url, headers=HEADERS, timeout=45)
            r.raise_for_status(); return r.json().get("data", [])
        except Exception:
            if attempt == 3: raise
            time.sleep(2 * attempt)

def _comtrade_monthly_food_trade(reporter_code):
    """
    Query a single month at a time (newest first) per flow. Comtrade returns
    HS-4 sub-rows even when cmdCode is HS-2, so 6 months × 24 chapters silently
    hits the 500-record cap and truncates the most recent month. One month per
    call ≈ 120 records per flow — well under the cap. Stops at the first month
    where both exports and imports are reported.
    """
    for period in _recent_ym(6):
        totals = {"X": 0.0, "M": 0.0}
        for fc_query in ("X", "M"):
            url = (f"{_COMTRADE_MONTHLY_BASE}?reporterCode={reporter_code}"
                   f"&period={period}&cmdCode={HS_FOOD_CHAPTERS}"
                   f"&flowCode={fc_query}&partnerCode=0&maxRecords=500")
            try:
                rows = _comtrade_get(url)
            except Exception:
                rows = []
            for row in rows:
                fc  = row.get("flowCode", "")
                usd = float(row.get("primaryValue") or 0)
                if fc in ("X", "DX", "x"): totals["X"] += usd
                elif fc in ("M", "m"):     totals["M"] += usd
        if totals["X"] > 0 and totals["M"] > 0:
            return totals["X"], totals["M"], datetime.strptime(period, "%Y%m").date()
    raise ValueError(f"Comtrade monthly food: no month with both X and M for reporter={reporter_code}")

def _comtrade_annual_food_trade(reporter_code):
    """Annual variant — same split-by-flow safeguard as the monthly call."""
    today = date.today()
    periods = ",".join(str(today.year - i) for i in range(1, 4))
    by_y = _defaultdict(lambda: {"X": 0.0, "M": 0.0})
    for fc_query in ("X", "M"):
        url = (f"{_COMTRADE_ANNUAL_BASE}?reporterCode={reporter_code}"
               f"&period={periods}&cmdCode={HS_FOOD_CHAPTERS}"
               f"&flowCode={fc_query}&partnerCode=0&maxRecords=500")
        try:
            rows = _comtrade_get(url)
        except Exception:
            rows = []
        for row in rows:
            p = str(row.get("period", "")); fc = row.get("flowCode", "")
            usd = float(row.get("primaryValue") or 0)
            if fc in ("X", "DX", "x"): by_y[p]["X"] += usd
            elif fc in ("M", "m"):     by_y[p]["M"] += usd
    if not by_y:
        raise ValueError(f"Comtrade annual food: no data for reporter={reporter_code}")
    for yr in sorted(by_y, reverse=True):
        if by_y[yr]["X"] > 0 and by_y[yr]["M"] > 0:
            return by_y[yr]["X"], by_y[yr]["M"], date(int(yr), 1, 1)
    raise ValueError(f"Comtrade annual food: all years zero for reporter={reporter_code}")

def _comtrade_monthly_basket(reporter_code):
    periods = _recent_ym(6)
    flow = "DX" if reporter_code==702 else "X"
    url = (f"{_COMTRADE_MONTHLY_BASE}?reporterCode={reporter_code}"
           f"&period={','.join(periods)}&cmdCode={_HS4_CODES_STR}"
           f"&flowCode={flow}&partnerCode=0&maxRecords=500")
    rows = _comtrade_get(url)
    if not rows and reporter_code==702:  # SG DX fallback to X
        rows = _comtrade_get(url.replace("flowCode=DX","flowCode=X"))
    if not rows: raise ValueError(f"Comtrade monthly basket: no data for reporter={reporter_code}")
    by_p = _defaultdict(float)
    for row in rows:
        by_p[str(row.get("period",""))] += float(row.get("primaryValue") or 0)
    for p in sorted(by_p, reverse=True):
        if by_p[p]>0: return by_p[p], datetime.strptime(p,"%Y%m").date()
    raise ValueError(f"Comtrade monthly basket: all periods zero for reporter={reporter_code}")

def _comtrade_annual_basket(reporter_code):
    today = date.today()
    periods = ",".join(str(today.year-i) for i in range(1,4))
    url = (f"{_COMTRADE_ANNUAL_BASE}?reporterCode={reporter_code}"
           f"&period={periods}&cmdCode={_HS4_CODES_STR}"
           f"&flowCode=X&partnerCode=0&maxRecords=500")
    rows = _comtrade_get(url)
    if not rows: raise ValueError(f"Comtrade annual basket: no data for reporter={reporter_code}")
    by_y = _defaultdict(float)
    for row in rows:
        by_y[str(row.get("period",""))] += float(row.get("primaryValue") or 0)
    for yr in sorted(by_y, reverse=True):
        if by_y[yr]>0: return by_y[yr]/12, date(int(yr),1,1)  # annual ÷ 12 → monthly-equivalent
    raise ValueError(f"Comtrade annual basket: all years zero for reporter={reporter_code}")

def _tcl_country_basket(country_iso):
    fao_area = _FAO_AREA[country_iso]
    df = _load_tcl()
    present_lower = {i.strip().lower():i for i in df["Item"].dropna().unique()}
    basket_items = set()
    for aliases in _TCL_STAPLES_ALIASES.values():
        for alias in aliases:
            m = present_lower.get(alias.strip().lower())
            if m: basket_items.add(m); break
    cdf = df[(df["Area"]==fao_area) & df["Item"].isin(basket_items)]
    if cdf.empty: raise ValueError(f"TCL: no basket data for {fao_area}")
    latest_yr = int(cdf["Year"].max())
    return float(cdf[cdf["Year"]==latest_yr]["Value"].sum())*1000/12, date(latest_yr,1,1)

# ── Trade balance collectors ──────────────────────────────────────────────────

FATUS_PAGE_URL = ("https://www.ers.usda.gov/data-products/"
                  "foreign-agricultural-trade-of-the-united-states-fatus/"
                  "us-agricultural-trade-data-update/")

def collect_us_food_trade(country_iso, metric_key, confidence=CONFIDENCE["file_download"]):
    if country_iso != "US": raise ValueError("US only")
    soup = BeautifulSoup(fetch_html(FATUS_PAGE_URL), "html.parser")
    xlsx_url = next((("https://www.ers.usda.gov"+a["href"]) if a["href"].startswith("/") else a["href"]
                     for a in soup.find_all("a", href=True) if ".xlsx" in a["href"].lower()), None)
    if not xlsx_url: raise ValueError("FATUS: no .xlsx link found")
    df = pd.read_excel(io.BytesIO(download_file(xlsx_url)), sheet_name=0, header=None)
    cal_row = next((i for i,row in df.iterrows()
                    if any("calendar" in str(v).lower() for v in row if str(v)!="nan")), None)
    section = df.iloc[cal_row:].reset_index(drop=True) if cal_row is not None else df
    exp_row = imp_row = bal_row = None
    for i,row in section.iterrows():
        lbl = str(row.iloc[0]).lower()
        if "agricultural export" in lbl:   exp_row = int(i)
        elif "agricultural import" in lbl: imp_row = int(i)
        elif "balance" in lbl:             bal_row = int(i)
    if exp_row is None or imp_row is None: raise ValueError("FATUS: cannot find export/import rows")
    exp_series = pd.to_numeric(section.iloc[exp_row], errors="coerce")
    last_col = int(exp_series.last_valid_index())
    try:
        data_date = datetime.strptime(
            f"{section.iloc[0,last_col]} {int(float(section.iloc[1,last_col]))}", "%B %Y").date()
    except Exception:
        data_date = date.today().replace(day=1)
    exports_usd = float(exp_series[last_col]) * 1e9
    imports_usd = float(pd.to_numeric(section.iloc[imp_row], errors="coerce")[last_col]) * 1e9
    bal_usd = None
    if bal_row is not None:
        bv = pd.to_numeric(section.iloc[bal_row], errors="coerce")[last_col]
        if str(bv) != "nan": bal_usd = float(bv) * 1e9
    return make_trade_result("US", metric_key, exports_usd, imports_usd, bal_usd,
        data_date, "monthly", "USDA ERS FATUS", xlsx_url, "file_download", confidence)

# Generic Comtrade trade balance wrappers — used directly in cascade

def collect_comtrade_monthly_trade(country_iso, metric_key, confidence=CONFIDENCE["api_monthly"]):
    exp, imp, dt = _comtrade_monthly_food_trade(_COMTRADE_REPORTERS[country_iso])
    return make_trade_result(country_iso, metric_key, exp, imp, None, dt, "monthly",
        "UN Comtrade HS 01-24 (monthly)", _COMTRADE_MONTHLY_BASE, "api_monthly", confidence)

def collect_comtrade_annual_trade(country_iso, metric_key, confidence=CONFIDENCE["api_annual"]):
    exp, imp, dt = _comtrade_annual_food_trade(_COMTRADE_REPORTERS[country_iso])
    return make_trade_result(country_iso, metric_key, exp, imp, None, dt, "annual",
        "UN Comtrade HS 01-24 (annual)", _COMTRADE_ANNUAL_BASE, "api_annual", confidence)

def collect_wb_trade(country_iso, metric_key, confidence=CONFIDENCE["api_annual"]):
    wb3 = _WB_ISO3[country_iso]
    total_exp, yr_exp = _wb_indicator(wb3, "TX.VAL.MRCH.CD.WT")
    total_imp, yr_imp = _wb_indicator(wb3, "TM.VAL.MRCH.CD.WT")
    food_exp_pct, _   = _wb_indicator(wb3, "TX.VAL.FOOD.ZS.UN")
    food_imp_pct, _   = _wb_indicator(wb3, "TM.VAL.FOOD.ZS.UN")
    return make_trade_result(country_iso, metric_key,
        total_exp*food_exp_pct/100, total_imp*food_imp_pct/100, None,
        date(min(yr_exp,yr_imp),1,1), "annual",
        "World Bank TX/TM.VAL.MRCH × TX/TM.VAL.FOOD.ZS", WB_BASE, "api_annual", confidence)

# Per-country wrappers for cascade (BR/PH/IN are single-step; SG/AE use multi-step cascade)
def collect_brazil_food_trade(country_iso, metric_key, **kw):
    return collect_comtrade_monthly_trade("BR", metric_key)
def collect_india_food_trade(country_iso, metric_key, **kw):
    return collect_comtrade_monthly_trade("IN", metric_key)
def collect_singapore_food_trade(country_iso, metric_key, **kw):
    return collect_comtrade_monthly_trade("SG", metric_key)
def collect_philippines_food_trade(country_iso, metric_key, **kw):
    return collect_comtrade_monthly_trade("PH", metric_key)
def collect_uae_food_trade(country_iso, metric_key, **kw):
    return collect_comtrade_monthly_trade("AE", metric_key)

# ── CSR ───────────────────────────────────────────────────────────────────────

_FBS_ITEM_GROUPS = {
    "Cereals - Excluding Beer","Starchy Roots","Sugar Crops","Sugar & Sweeteners",
    "Pulses","Treenuts","Oilcrops","Vegetable Oils","Vegetables",
    "Fruits - Excluding Wine","Stimulants","Spices","Alcoholic Beverages",
    "Animal fats","Meat","Offals","Milk - Excluding Butter","Eggs",
    "Fish, Seafood","Aquatic Products, Other","Miscellaneous",
}

def collect_csr(country_iso, metric_key="caloric_self_sufficiency_ratio",
                confidence=CONFIDENCE["file_download"]):
    fao_area = _FAO_AREA[country_iso]
    df = _load_fbs()
    cdf = df[df["Area"]==fao_area].copy()
    if country_iso == "SG":
        gt = cdf[(cdf["Item"]=="Grand Total")&(cdf["Element"]=="Food supply (kcal/capita/day)")]
        if gt.empty or gt["Value"].dropna().empty:
            try:
                fpi, yr = _wb_indicator("SG", "AG.PRD.FOOD.XD")
                return make_metric_result("SG", metric_key, round((fpi/100)*0.30,4), "ratio",
                    date(yr,1,1), "annual", "World Bank AG.PRD.FOOD.XD (FPI proxy)",
                    "https://data.worldbank.org/indicator/AG.PRD.FOOD.XD",
                    "api_annual", CONFIDENCE["imputed"], raw_value=f"wb_fpi={fpi:.2f}")
            except Exception as wb_exc:
                raise ValueError(
                    f"SG caloric_self_sufficiency_ratio: FAO FBS empty and "
                    f"World Bank FPI fallback failed ({wb_exc}). "
                    "No reliable value available — opening gap."
                ) from wb_exc
    leaf_df = cdf[(cdf["Item"]!="Grand Total")&(~cdf["Item"].isin(_FBS_ITEM_GROUPS))].copy()
    pivot = (leaf_df.pivot_table(index=["Year","Item"], columns="Element",
                                  values="Value", aggfunc="first").reset_index())
    for col in ["Production","Domestic supply quantity","Food supply (kcal/capita/day)"]:
        if col not in pivot.columns: pivot[col] = float("nan")
    mask = (pivot["Domestic supply quantity"].fillna(0)>0) & pivot["Food supply (kcal/capita/day)"].notna()
    pivot.loc[mask,"_prod_kcal"] = (
        pivot.loc[mask,"Food supply (kcal/capita/day)"]
        * pivot.loc[mask,"Production"].fillna(0)
        / pivot.loc[mask,"Domestic supply quantity"])
    prod_kcal_by_yr = pivot.groupby("Year")["_prod_kcal"].sum(min_count=1)
    gt = cdf[(cdf["Item"]=="Grand Total")&(cdf["Element"]=="Food supply (kcal/capita/day)")]
    supply_kcal_by_yr = gt.set_index("Year")["Value"].dropna()
    combined = pd.DataFrame({"prod_kcal":prod_kcal_by_yr,"supply_kcal":supply_kcal_by_yr}).dropna()
    if combined.empty: raise ValueError(f"CSR: no data for {COUNTRIES[country_iso]['name']}")
    combined["csr_kcal"] = combined["prod_kcal"]/combined["supply_kcal"]
    combined = combined.sort_index()
    latest_yr = int(combined.index[-1]); latest_row = combined.iloc[-1]
    return make_metric_result(country_iso, metric_key,
        round(float(latest_row["csr_kcal"]),6), "ratio",
        date(latest_yr,1,1), "annual", "FAOSTAT Food Balance Sheets (FBS)", _FBS_URL,
        "file_download", confidence,
        raw_value=f"csr_kcal={latest_row['csr_kcal']:.6f}")

# ── Export share — shared denominator helper ──────────────────────────────────

def _export_share_denominator(data_date):
    """Returns (world_monthly_usd, w_year) from FAOSTAT TCL Export Value (1000 USD)."""
    tcl_df = _load_tcl()
    world_name = next((w for w in ["World","World + (Total)","World, FAO"]
                       if w in tcl_df["Area"].values), None)
    if world_name is None: raise ValueError("TCL: no World row")
    present_lower = {i.strip().lower():i for i in tcl_df["Item"].dropna().unique()}
    basket_items = set()
    for aliases in _TCL_STAPLES_ALIASES.values():
        for alias in aliases:
            m = present_lower.get(alias.strip().lower())
            if m: basket_items.add(m); break
    w_df = tcl_df[(tcl_df["Area"]==world_name) & tcl_df["Item"].isin(basket_items)]
    if w_df.empty: raise ValueError("TCL: no world basket data")
    w_year = int(w_df[w_df["Year"]<=data_date.year]["Year"].max())
    world_monthly_usd = float(w_df[w_df["Year"]==w_year]["Value"].sum()) * 1000 / 12
    if world_monthly_usd <= 0: raise ValueError("TCL: world basket is zero")
    return world_monthly_usd, w_year

def _make_export_share(country_iso, metric_key, val_usd, data_date, freq, conf):
    world_monthly_usd, w_year = _export_share_denominator(data_date)
    share = val_usd / world_monthly_usd
    return make_metric_result(country_iso, metric_key,
        round(share, 8), "ratio", data_date, freq,
        "UN Comtrade primaryValue USD (numerator) + FAOSTAT TCL Export Value ÷ 12 (denominator)",
        _COMTRADE_MONTHLY_BASE, conf, conf,
        raw_value=f"val_usd={val_usd:.0f}, world_monthly_usd={world_monthly_usd:.0f}, w_year={w_year}")

# Three separate export-share collectors — used as explicit cascade steps

def collect_export_share_monthly(country_iso, metric_key="share_global_staple_exports",
                                  confidence=CONFIDENCE["api_monthly"], **kw):
    """Primary: Comtrade monthly HS4 basket (USD value)."""
    val_usd, dt = _comtrade_monthly_basket(_COMTRADE_REPORTERS[country_iso])
    return _make_export_share(country_iso, metric_key, val_usd, dt, "monthly", confidence)

def collect_export_share_annual_comtrade(country_iso, metric_key="share_global_staple_exports",
                                          confidence=CONFIDENCE["api_annual"], **kw):
    """Fallback: Comtrade annual basket ÷ 12 (USD value)."""
    val_usd, dt = _comtrade_annual_basket(_COMTRADE_REPORTERS[country_iso])
    return _make_export_share(country_iso, metric_key, val_usd, dt, "annual", confidence)

def collect_export_share_fao_tcl(country_iso, metric_key="share_global_staple_exports",
                                   confidence=CONFIDENCE["file_download"], **kw):
    """Final fallback: FAOSTAT TCL country Export Value ÷ 12 (USD)."""
    val_usd, dt = _tcl_country_basket(country_iso)
    return _make_export_share(country_iso, metric_key, val_usd, dt, "annual", confidence)

def collect_export_share_sg_reexport(country_iso, metric_key="share_global_staple_exports",
                                      confidence=CONFIDENCE["api_monthly"], **kw):
    """
    SG-specific wrapper: Singapore is a global food re-export hub, so Comtrade
    data reflects trade flows through the port, not domestic agricultural output.
    The share value is correct as a trade-flow metric, but confidence is reduced
    to reflect that it does NOT indicate domestic production capacity.
    """
    val_usd, dt = _comtrade_monthly_basket(_COMTRADE_REPORTERS["SG"])
    world_monthly_usd, w_year = _export_share_denominator(dt)
    share = val_usd / world_monthly_usd
    reexport_conf = min(confidence, CONFIDENCE["web_scrape"])  # cap at 0.60
    return make_metric_result(
        "SG", metric_key,
        round(share, 8), "ratio", dt, "monthly",
        "UN Comtrade primaryValue USD [SG: re-export hub — trade flow, not production]",
        _COMTRADE_MONTHLY_BASE, reexport_conf, reexport_conf,
        raw_value=f"val_usd={val_usd:.0f}, world_monthly_usd={world_monthly_usd:.0f}, "
                  f"w_year={w_year}, note=reexport_hub",
    )


# ── Arable land per capita ────────────────────────────────────────────────────

def collect_arable_per_capita(country_iso, metric_key="arable_land_per_capita",
                               confidence=CONFIDENCE["file_download"], **kw):
    fao_area = _FAO_AREA[country_iso]; wb_iso3 = _WB_ISO3[country_iso]
    df = _load_landuse()
    cdf = df[df["Area"]==fao_area].dropna(subset=["Value"]).sort_values("Year",ascending=False)
    if cdf.empty: raise ValueError(f"Arable: no FAO data for {COUNTRIES[country_iso]['name']}")
    yr = int(cdf.iloc[0]["Year"]); arable_ha = float(cdf.iloc[0]["Value"]) * 1000.0
    pop = None
    for attempt in range(1,4):
        try:
            r = _requests.get(
                f"https://api.worldbank.org/v2/country/{wb_iso3}/indicator/SP.POP.TOTL"
                f"?format=json&date={yr-2}:{yr}&per_page=20",
                headers=HEADERS, timeout=60)
            r.raise_for_status()
            data = r.json()
            if isinstance(data,list) and len(data)>1:
                for rec in data[1]:
                    if rec.get("value") is not None: pop=int(rec["value"]); break
            if pop is not None: break
        except Exception:
            if attempt==3: raise
            time.sleep(3*attempt)
    if pop is None: raise ValueError(f"Arable: no WB population for {wb_iso3}")
    return make_metric_result(country_iso, metric_key,
        round(arable_ha/pop,8), "ha/person", date(yr,1,1), "annual",
        "FAOSTAT Land Use + World Bank SP.POP.TOTL", _LU_URL,
        "file_download", confidence, raw_value=f"arable_ha={arable_ha:.0f}, pop={pop}")

print("All collector functions defined.")
print("  Trade:        US=FATUS | BR/IN/PH=Comtrade monthly | SG/AE=monthly→annual→WB (cascade steps)")
print("  Export share: monthly basket → annual basket → FAO TCL (cascade steps)")
print("  CSR:          FAOSTAT FBS (annual)")
print("  Arable:       FAOSTAT LandUse + WB pop (60s timeout)")
# ── Comtrade quarterly helpers ────────────────────────────────────────────────

_COMTRADE_QUARTERLY_BASE = "https://comtradeapi.un.org/public/v1/preview/C/Q/HS"

def _recent_quarters(n=4):
    today = date.today()
    q = (today.month - 1) // 3 + 1
    y = today.year
    out = []
    for _ in range(n):
        out.append(f"{y}Q{q}")
        q -= 1
        if q == 0: q = 4; y -= 1
    return out

def _quarter_to_date(p):
    """Convert '2024Q2' → date(2024, 4, 1) (first month of quarter)."""
    yr, qn = int(p[:4]), int(p[5])
    return date(yr, (qn - 1) * 3 + 1, 1)

def _comtrade_quarterly_food_trade(reporter_code):
    """Quarterly variant — split-by-flow safeguard, both sides required."""
    periods = ",".join(_recent_quarters(4))
    by_p = _defaultdict(lambda: {"X": 0.0, "M": 0.0})
    for fc_query in ("X", "M"):
        url = (f"{_COMTRADE_QUARTERLY_BASE}?reporterCode={reporter_code}"
               f"&period={periods}&cmdCode={HS_FOOD_CHAPTERS}"
               f"&flowCode={fc_query}&partnerCode=0&maxRecords=500")
        try:
            rows = _comtrade_get(url)
        except Exception:
            rows = []
        for row in rows:
            p = str(row.get("period", "")); fc = row.get("flowCode", "")
            usd = float(row.get("primaryValue") or 0)
            if fc in ("X", "DX", "x"): by_p[p]["X"] += usd
            elif fc in ("M", "m"):     by_p[p]["M"] += usd
    if not by_p:
        raise ValueError(f"Comtrade quarterly food: no data for reporter={reporter_code}")
    for p in sorted(by_p, reverse=True):
        if by_p[p]["X"] > 0 and by_p[p]["M"] > 0:
            return by_p[p]["X"], by_p[p]["M"], _quarter_to_date(p)
    raise ValueError(f"Comtrade quarterly food: no period with both X and M for reporter={reporter_code}")

def _comtrade_quarterly_basket(reporter_code):
    periods = ",".join(_recent_quarters(4))
    url = (f"{_COMTRADE_QUARTERLY_BASE}?reporterCode={reporter_code}"
           f"&period={periods}&cmdCode={_HS4_CODES_STR}"
           f"&flowCode=X&partnerCode=0&maxRecords=500")
    rows = _comtrade_get(url)
    if not rows:
        raise ValueError(f"Comtrade quarterly basket: no data for reporter={reporter_code}")
    by_p = _defaultdict(float)
    for row in rows:
        by_p[str(row.get("period", ""))] += float(row.get("primaryValue") or 0)
    for p in sorted(by_p, reverse=True):
        if by_p[p] > 0:
            return by_p[p] / 3, _quarter_to_date(p)  # quarterly ÷ 3 = monthly equivalent
    raise ValueError(f"Comtrade quarterly basket: all periods zero for reporter={reporter_code}")

def collect_comtrade_quarterly_trade(country_iso, metric_key,
                                      confidence=CONFIDENCE["api_quarterly"], **kw):
    exp, imp, dt = _comtrade_quarterly_food_trade(_COMTRADE_REPORTERS[country_iso])
    return make_trade_result(country_iso, metric_key, exp, imp, None, dt, "quarterly",
        "UN Comtrade HS 01-24 (quarterly)", _COMTRADE_QUARTERLY_BASE,
        "api_quarterly", confidence)

def collect_export_share_quarterly(country_iso, metric_key="share_global_staple_exports",
                                    confidence=CONFIDENCE["api_quarterly"], **kw):
    val_usd, dt = _comtrade_quarterly_basket(_COMTRADE_REPORTERS[country_iso])
    return _make_export_share(country_iso, metric_key, val_usd, dt, "quarterly", confidence)

print("Quarterly snapshot collectors defined: collect_comtrade_quarterly_trade, collect_export_share_quarterly")


# In[3]:


import uuid, time as _time
from datetime import datetime

def log_attempt(conn, run_id, country_iso, metric_key, collector_name,
                step, status, source_url, error_type, error_msg, duration_ms):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si4_collection_log
                (run_id, country_iso, metric_key, collector_name, cascade_step,
                 status, source_url, error_type, error_message, duration_ms)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, country_iso, metric_key, collector_name, step,
              status, source_url, error_type, error_msg, duration_ms))
    conn.commit()


def store_trade_datapoint(conn, dp: dict, run_id: str):
    """Upsert a trade result into si4_food_trade_raw."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si4_food_trade_raw (
                country_iso, country_name, metric_key,
                exports_usd, imports_usd, trade_balance_usd,
                data_date, data_frequency, source_name, source_url,
                access_method, confidence_score, run_id
            ) VALUES (
                %(country_iso)s, %(country_name)s, %(metric_key)s,
                %(exports_usd)s, %(imports_usd)s, %(trade_balance_usd)s,
                %(data_date)s, %(data_frequency)s, %(source_name)s, %(source_url)s,
                %(access_method)s, %(confidence_score)s, %(run_id)s
            )
            ON CONFLICT (country_iso, metric_key, data_date, source_name) DO UPDATE SET
                exports_usd       = EXCLUDED.exports_usd,
                imports_usd       = EXCLUDED.imports_usd,
                trade_balance_usd = EXCLUDED.trade_balance_usd,
                confidence_score  = EXCLUDED.confidence_score,
                run_id            = EXCLUDED.run_id,
                collected_at      = NOW()
        """, {**dp, "run_id": run_id})
    conn.commit()


def store_metric_datapoint(conn, dp: dict, run_id: str):
    """Upsert a metric result into si4_raw_metrics."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si4_raw_metrics (
                country_iso, country_name, metric_key, metric_label,
                metric_value, unit, data_date, data_frequency,
                source_name, source_url, access_method,
                confidence_score, raw_value, is_imputed, run_id
            ) VALUES (
                %(country_iso)s, %(country_name)s, %(metric_key)s, %(metric_label)s,
                %(metric_value)s, %(unit)s, %(data_date)s, %(data_frequency)s,
                %(source_name)s, %(source_url)s, %(access_method)s,
                %(confidence_score)s, %(raw_value)s, %(is_imputed)s, %(run_id)s
            )
            ON CONFLICT (country_iso, metric_key, data_date, source_name) DO UPDATE SET
                metric_value     = EXCLUDED.metric_value,
                confidence_score = EXCLUDED.confidence_score,
                raw_value        = EXCLUDED.raw_value,
                is_imputed       = EXCLUDED.is_imputed,
                run_id           = EXCLUDED.run_id,
                collected_at     = NOW()
        """, {**dp, "is_imputed": dp.get("access_method") == "imputed", "run_id": run_id})
    conn.commit()


def open_gap(conn, run_id, country_iso, metric_key, failure_reason, collectors_tried, severity):
    country_name = COUNTRIES[country_iso]["name"]
    metric_label = METRICS[metric_key]["label"]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si4_data_gaps
                (country_iso, country_name, metric_key, metric_label,
                 failure_reason, collectors_tried, severity)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (country_iso, metric_key) DO UPDATE SET
                failure_reason   = EXCLUDED.failure_reason,
                collectors_tried = EXCLUDED.collectors_tried,
                last_attempted   = NOW(),
                attempt_count    = si4_data_gaps.attempt_count + 1,
                status           = 'open'
        """, (country_iso, country_name, metric_key, metric_label,
              failure_reason, collectors_tried, severity))
    conn.commit()


print("DB helpers defined: log_attempt | store_trade_datapoint | store_metric_datapoint | open_gap")

# In[4]:


METRIC_CASCADE = {

    # ── net_food_trade_balance ─────────────────────────────────────────────────
    ("US", "net_food_trade_balance"): [
        {"name": "USDA ERS FATUS (monthly)",      "fn": collect_us_food_trade,               "kwargs": {}},
    ],
    ("BR", "net_food_trade_balance"): [
        {"name": "Comtrade monthly",              "fn": collect_brazil_food_trade,            "kwargs": {}},
    ],
    ("IN", "net_food_trade_balance"): [
        {"name": "Comtrade monthly",              "fn": collect_india_food_trade,             "kwargs": {}},
        {"name": "Comtrade quarterly",            "fn": collect_comtrade_quarterly_trade,     "kwargs": {}},
        {"name": "World Bank annual",             "fn": collect_wb_trade,                     "kwargs": {}},
    ],
    ("PH", "net_food_trade_balance"): [
        {"name": "Comtrade monthly",              "fn": collect_philippines_food_trade,       "kwargs": {}},
    ],
    ("SG", "net_food_trade_balance"): [
        {"name": "Comtrade monthly",              "fn": collect_comtrade_monthly_trade,       "kwargs": {}},
        {"name": "Comtrade quarterly",            "fn": collect_comtrade_quarterly_trade,     "kwargs": {}},
        {"name": "Comtrade annual",               "fn": collect_comtrade_annual_trade,        "kwargs": {}},
        {"name": "World Bank annual",             "fn": collect_wb_trade,                     "kwargs": {}},
    ],
    ("AE", "net_food_trade_balance"): [
        {"name": "Comtrade monthly",              "fn": collect_comtrade_monthly_trade,       "kwargs": {}},
        {"name": "Comtrade quarterly",            "fn": collect_comtrade_quarterly_trade,     "kwargs": {}},
        {"name": "Comtrade annual",               "fn": collect_comtrade_annual_trade,        "kwargs": {}},
        {"name": "World Bank annual",             "fn": collect_wb_trade,                     "kwargs": {}},
    ],

    # ── caloric_self_sufficiency_ratio (annual only — FBS is annual) ──────────
    ("US", "caloric_self_sufficiency_ratio"): [{"name": "FAOSTAT FBS", "fn": collect_csr, "kwargs": {}}],
    ("BR", "caloric_self_sufficiency_ratio"): [{"name": "FAOSTAT FBS", "fn": collect_csr, "kwargs": {}}],
    ("IN", "caloric_self_sufficiency_ratio"): [{"name": "FAOSTAT FBS", "fn": collect_csr, "kwargs": {}}],
    ("SG", "caloric_self_sufficiency_ratio"): [{"name": "FAOSTAT FBS", "fn": collect_csr, "kwargs": {}}],
    ("PH", "caloric_self_sufficiency_ratio"): [{"name": "FAOSTAT FBS", "fn": collect_csr, "kwargs": {}}],
    ("AE", "caloric_self_sufficiency_ratio"): [{"name": "FAOSTAT FBS", "fn": collect_csr, "kwargs": {}}],

    # ── share_global_staple_exports ───────────────────────────────────────────
    ("US", "share_global_staple_exports"): [
        {"name": "Comtrade monthly basket",       "fn": collect_export_share_monthly,         "kwargs": {}},
    ],
    ("BR", "share_global_staple_exports"): [
        {"name": "Comtrade monthly basket",       "fn": collect_export_share_monthly,         "kwargs": {}},
    ],
    ("PH", "share_global_staple_exports"): [
        {"name": "Comtrade monthly basket",       "fn": collect_export_share_monthly,         "kwargs": {}},
    ],
    ("IN", "share_global_staple_exports"): [
        {"name": "Comtrade monthly basket",       "fn": collect_export_share_monthly,         "kwargs": {}},
        {"name": "Comtrade quarterly basket",     "fn": collect_export_share_quarterly,       "kwargs": {}},
        {"name": "Comtrade annual basket",        "fn": collect_export_share_annual_comtrade, "kwargs": {}},
        {"name": "FAO TCL country basket",        "fn": collect_export_share_fao_tcl,         "kwargs": {}},
    ],
    ("SG", "share_global_staple_exports"): [
        {"name": "Comtrade monthly basket (re-export flagged)", "fn": collect_export_share_sg_reexport, "kwargs": {}},
        {"name": "Comtrade quarterly basket",     "fn": collect_export_share_quarterly,       "kwargs": {}},
        {"name": "Comtrade annual basket",        "fn": collect_export_share_annual_comtrade, "kwargs": {}},
        {"name": "FAO TCL country basket",        "fn": collect_export_share_fao_tcl,         "kwargs": {}},
    ],
    ("AE", "share_global_staple_exports"): [
        {"name": "Comtrade monthly basket",       "fn": collect_export_share_monthly,         "kwargs": {}},
        {"name": "Comtrade quarterly basket",     "fn": collect_export_share_quarterly,       "kwargs": {}},
        {"name": "Comtrade annual basket",        "fn": collect_export_share_annual_comtrade, "kwargs": {}},
        {"name": "FAO TCL country basket",        "fn": collect_export_share_fao_tcl,         "kwargs": {}},
    ],

    # ── arable_land_per_capita (annual only — land surveys are annual) ────────
    ("US", "arable_land_per_capita"): [{"name": "FAOSTAT LandUse + WB pop", "fn": collect_arable_per_capita, "kwargs": {}}],
    ("BR", "arable_land_per_capita"): [{"name": "FAOSTAT LandUse + WB pop", "fn": collect_arable_per_capita, "kwargs": {}}],
    ("IN", "arable_land_per_capita"): [{"name": "FAOSTAT LandUse + WB pop", "fn": collect_arable_per_capita, "kwargs": {}}],
    ("SG", "arable_land_per_capita"): [{"name": "FAOSTAT LandUse + WB pop", "fn": collect_arable_per_capita, "kwargs": {}}],
    ("PH", "arable_land_per_capita"): [{"name": "FAOSTAT LandUse + WB pop", "fn": collect_arable_per_capita, "kwargs": {}}],
    ("AE", "arable_land_per_capita"): [{"name": "FAOSTAT LandUse + WB pop", "fn": collect_arable_per_capita, "kwargs": {}}],
}

TRADE_METRIC_KEYS = {"net_food_trade_balance"}

total = len(METRIC_CASCADE)
steps = sum(len(v) for v in METRIC_CASCADE.values())
print(f"METRIC_CASCADE: {total} entries, {steps} total steps (incl. fallbacks)")
print("Cascade order: monthly → quarterly → annual (where applicable)")

# In[5]:


import time as _time
from datetime import timezone

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

def _data_is_stale(conn, country_iso: str, metric_key: str) -> tuple:
    """Return (is_stale, age_days, existing_value) by inspecting the right SI4 table.
    Staleness threshold is per access_method so annual/file sources are not
    re-fetched on every daily run."""
    is_trade = metric_key in TRADE_METRIC_KEYS
    sql = (
        "SELECT trade_balance_usd, collected_at, access_method "
        "FROM si4_food_trade_raw WHERE country_iso=%s AND metric_key=%s "
        "ORDER BY collected_at DESC LIMIT 1"
    ) if is_trade else (
        "SELECT metric_value, collected_at, access_method "
        "FROM si4_raw_metrics WHERE country_iso=%s AND metric_key=%s "
        "ORDER BY collected_at DESC LIMIT 1"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (country_iso, metric_key))
        row = cur.fetchone()
    if not row:
        return True, None, None
    value, collected_at, access_method = row
    age_days = (datetime.now(timezone.utc).replace(tzinfo=None) - collected_at).days
    threshold = _STALE_THRESHOLDS.get(access_method, 1)
    return age_days > threshold, age_days, value


def _fresh_conn():
    """Always return a fresh DB connection \u2014 agent runs can take minutes."""
    return psycopg2.connect(**DB_CONFIG)


# \u2500\u2500 Research-agent fallback (universal \u2014 fires for ALL combos) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

def collect_research_si4(country_iso: str, metric_key: str,
                         confidence: float = CONFIDENCE["web_scrape"]) -> dict:
    """
    Deep-research collector for SI4. Delegates to research_agent.run_research_agent
    and returns a datapoint shaped for the right SI4 storage table.

    For trade metrics, exports/imports are unknown (single-value reports) so they
    are stored as NULL with the balance carrying the value.
    """
    result = _run_deep_research(
        country_iso  = country_iso,
        metric_key   = metric_key,
        country_name = COUNTRIES[country_iso]["name"],
        currency     = COUNTRIES[country_iso].get("currency", "USD"),
        metric_label = METRICS[metric_key]["label"],
        metric_unit  = METRICS[metric_key]["unit"],
        fx_rates     = {},
        trusted_urls = None,
    )
    val = result.get("value")
    if val is None:
        raise ValueError(f"Research agent returned null value for {metric_key}/{country_iso}")
    val = float(val)
    src_url   = result.get("source_url", "")
    src_name  = f"Deep research agent \u2014 {country_iso}/{metric_key}"
    data_date = result.get("data_date") or date.today().isoformat()
    frequency = result.get("frequency", "irregular")

    if metric_key in TRADE_METRIC_KEYS:
        return make_trade_result(
            country_iso, metric_key,
            exports_usd=None, imports_usd=None, trade_balance_usd=val,
            data_date=data_date, data_frequency=frequency,
            source_name=src_name, source_url=src_url,
            access_method="web_scrape", confidence_score=confidence,
        )
    return make_metric_result(
        country_iso, metric_key, val, METRICS[metric_key]["unit"],
        data_date, frequency, src_name, src_url,
        "web_scrape", confidence,
        raw_value=result.get("raw_text", ""),
    )


def _try_research_agent(conn, run_id, country_iso, metric_key,
                        step_num, errors, tried) -> bool:
    """Run the research agent as a fallback and store result if successful."""
    has_search = TAVILY_API_KEY and TAVILY_API_KEY != "your_tavily_key_here"
    if not has_search:
        return False
    agent_name = "Research Agent (Tavily + Claude)"
    tried.append(agent_name)
    t0 = _time.perf_counter()
    try:
        dp = collect_research_si4(country_iso, metric_key,
                                  confidence=CONFIDENCE["web_scrape"])
        elapsed = int((_time.perf_counter() - t0) * 1000)
        fresh = _fresh_conn()
        try:
            log_attempt(fresh, run_id, country_iso, metric_key, agent_name, step_num,
                        "success", dp.get("source_url"), None, None, elapsed)
            if metric_key in TRADE_METRIC_KEYS:
                store_trade_datapoint(fresh, dp, run_id)
                bal = dp.get("trade_balance_usd")
                print(f"  \u2713 [{country_iso}] {metric_key} | bal={bal:,.0f} (USD) | src={agent_name} conf={dp['confidence_score']}")
            else:
                store_metric_datapoint(fresh, dp, run_id)
                print(f"  \u2713 [{country_iso}] {metric_key} = {dp['metric_value']} {dp.get('unit','')} (src={agent_name}, conf={dp['confidence_score']})")
        finally:
            fresh.close()
        return True
    except Exception as exc:
        elapsed = int((_time.perf_counter() - t0) * 1000)
        err_msg = str(exc)[:500]
        errors.append(f"[{agent_name}] {type(exc).__name__}: {err_msg}")
        try:
            fresh = _fresh_conn()
            log_attempt(fresh, run_id, country_iso, metric_key, agent_name, step_num,
                        "failed", None, type(exc).__name__, err_msg, elapsed)
            fresh.close()
        except Exception:
            pass
        print(f"  \u2717 [{country_iso}] {metric_key} \u2014 {agent_name}: {err_msg[:80]}")
        return False


def _get_last_known_dp(conn, country_iso: str, metric_key: str) -> dict | None:
    """Return the most recent non-NULL stored datapoint for carry-forward imputation.
    Only applies to non-trade metrics (si4_raw_metrics table)."""
    if metric_key in TRADE_METRIC_KEYS:
        return None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, country_name, metric_key, metric_label,
                   metric_value, unit, data_date, data_frequency,
                   source_name, source_url, access_method, confidence_score,
                   raw_value, is_imputed
            FROM si4_raw_metrics
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
            "raw_value", "is_imputed"]
    dp = dict(zip(cols, row))
    dp["confidence_score"] = CONFIDENCE["imputed"]
    dp["is_imputed"] = True
    return dp


def run_cascade(conn, run_id: str, country_iso: str, metric_key: str) -> bool:
    """
    Try each cascade step for (country, metric). Research agent is the universal
    last resort \u2014 fires whether the cascade is defined or not, and whether data
    is missing or stale. Returns True on first success, False if everything fails.
    """
    steps    = METRIC_CASCADE.get((country_iso, metric_key), [])
    is_trade = metric_key in TRADE_METRIC_KEYS
    errors, tried = [], []

    # \u2500\u2500 Staleness short-circuit \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    is_stale, age_days, existing_val = _data_is_stale(conn, country_iso, metric_key)
    if not is_stale and existing_val is not None:
        print(f"  [FRESH] ({country_iso}, {metric_key}) \u2014 {age_days}d old, skipping")
        return True
    if age_days is not None:
        print(f"  [STALE {age_days}d] ({country_iso}, {metric_key}) \u2014 refreshing...")

    # \u2500\u2500 Run cascade collectors (if any defined) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    cascade_succeeded = False
    for step_num, step in enumerate(steps, start=1):
        name   = step["name"]
        fn     = step["fn"]
        kwargs = {**step["kwargs"], "country_iso": country_iso, "metric_key": metric_key}
        tried.append(name)
        t0 = _time.perf_counter()
        try:
            dp      = fn(**kwargs)
            elapsed = int((_time.perf_counter() - t0) * 1000)
            log_attempt(conn, run_id, country_iso, metric_key, name, step_num,
                        "success", dp.get("source_url"), None, None, elapsed)
            if is_trade:
                store_trade_datapoint(conn, dp, run_id)
                bal = dp.get("trade_balance_usd")
                print(
                    f"  \u2713 [{country_iso}] {metric_key} | "
                    f"exp={dp.get('exports_usd') or 0:,.0f} "
                    f"imp={dp.get('imports_usd') or 0:,.0f} "
                    f"bal={bal or 0:,.0f} | "
                    f"src={name} conf={dp['confidence_score']} freq={dp['data_frequency']}"
                )
            else:
                store_metric_datapoint(conn, dp, run_id)
                print(
                    f"  \u2713 [{country_iso}] {metric_key} | "
                    f"value={dp.get('metric_value')} {dp.get('unit','')} "
                    f"date={dp.get('data_date')} | "
                    f"src={name} conf={dp['confidence_score']} method={dp.get('access_method')}"
                )
            cascade_succeeded = True
            break
        except Exception as exc:
            elapsed  = int((_time.perf_counter() - t0) * 1000)
            err_type = type(exc).__name__
            err_msg  = str(exc)[:500]
            errors.append(f"[{name}] {err_type}: {err_msg}")
            log_attempt(conn, run_id, country_iso, metric_key, name, step_num,
                        "failed",
                        step["kwargs"].get("url") or step["kwargs"].get("pdf_url"),
                        err_type, err_msg, elapsed)
            print(f"  \u2717 [{country_iso}] {metric_key} \u2014 {name}: {err_type}: {err_msg[:80]}")

    # Research agent always runs — finds fresher press/web data even when
    # cascade succeeded. Both results stored; view picks newer data_date.
    print(f"  [AGENT] {'Supplementing cascade with' if cascade_succeeded else 'Trying'} research agent...")
    agent_succeeded = _try_research_agent(conn, run_id, country_iso, metric_key,
                                          len(steps) + 1, errors, tried)
    if cascade_succeeded or agent_succeeded:
        return True
    # \u2500\u2500 Everything failed \u2192 carry forward last known value, then open gap \u2500
    fresh = _fresh_conn()
    try:
        carried = _get_last_known_dp(fresh, country_iso, metric_key)
        if carried:
            store_metric_datapoint(fresh, carried, run_id)
            print(f"  [CARRY] ({country_iso}, {metric_key}) = {carried['metric_value']} {carried['unit']} (last known, imputed)")
        open_gap(fresh, run_id, country_iso, metric_key,
                 " | ".join(errors), tried,
                 METRICS[metric_key]["gap_severity"])
    except Exception:
        pass
    finally:
        fresh.close()
    print(f"  \u2717\u2717 GAP: ({country_iso}, {metric_key}) \u2014 all {len(tried)} collector(s) failed")
    return False


print("run_cascade defined (with universal research-agent fallback).")

# In[6]:


import uuid
from datetime import datetime

def run_pipeline(countries=None, metrics=None):
    """
    Run the full collection pipeline.

    Args:
        countries: list of ISO-2 codes, or None for all.
        metrics:   list of metric keys, or None for all.

    Returns:
        run_id (str)
    """
    target_countries = countries or list(COUNTRIES.keys())
    target_metrics   = metrics   or list(METRICS.keys())
    run_id           = str(uuid.uuid4())
    started_at       = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO si4_collection_runs (run_id) VALUES (%s)", (run_id,)
        )
    conn.commit()

    total = succeeded = failed = 0
    combos = [(c, m) for c in target_countries for m in target_metrics]

    print(f"Run ID: {run_id}")
    print(f"Starting {len(combos)} tasks ({len(target_countries)} countries × {len(target_metrics)} metrics)\n")

    for country_iso, metric_key in combos:
        total += 1
        print(f"\n[{total}/{len(combos)}] {country_iso} / {metric_key}")
        ok = run_cascade(conn, run_id, country_iso, metric_key)
        if ok:
            succeeded += 1
        else:
            failed += 1

    finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
    elapsed     = (finished_at - started_at).total_seconds()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM si4_data_gaps WHERE status='open'")
        gaps_open = cur.fetchone()[0]
        cur.execute("""
            UPDATE si4_collection_runs
            SET finished_at=%s, total_tasks=%s, succeeded=%s, failed=%s, gaps_opened=%s
            WHERE run_id=%s
        """, (finished_at, total, succeeded, failed, gaps_open, run_id))
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"Run complete in {elapsed:.1f}s")
    print(f"  Succeeded: {succeeded}/{total}")
    print(f"  Failed:    {failed}/{total}")
    print(f"  Open gaps: {gaps_open}")
    print(f"{'='*60}")
    return run_id


print("run_pipeline defined.")

# In[ ]:


# ── MONTHLY HISTORICAL COLLECTORS ─────────────────────────────────────────────
# New functions that fetch monthly data (2020→present) for each country/metric.

# ── Tavily helper ─────────────────────────────────────────────────────────────

def _tavily_search(query, max_results=5):
    """Search via Tavily API; returns list of {url, title, content} dicts."""
    if not TAVILY_API_KEY:
        raise ValueError("TAVILY_API_KEY not set")
    r = _requests.post(
        "https://api.tavily.com/search",
        json={"api_key": TAVILY_API_KEY, "query": query,
              "max_results": max_results, "search_depth": "basic"},
        timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

def _parse_usd_billion(text, keyword):
    """Extract a USD-billion figure that follows `keyword` in text."""
    pattern = re.compile(
        rf"{keyword}[^.\n]{{0,80}}?(?:USD|US\$|\$)?\s*(\d+(?:[,\.]\d+)*)\s*(?:billion|bn)",
        re.IGNORECASE)
    m = pattern.search(text)
    if m:
        return float(m.group(1).replace(",", "")) * 1e9
    return None

# ── USDA FATUS — full monthly history ────────────────────────────────────────

def _fatus_all_months(country_iso, start_year=2020):
    """Parse EVERY monthly column from the USDA FATUS Excel (US only).
    Returns one trade-result dict per month from start_year to latest release."""
    soup = BeautifulSoup(fetch_html(FATUS_PAGE_URL), "html.parser")
    xlsx_url = next(
        (("https://www.ers.usda.gov" + a["href"]) if a["href"].startswith("/") else a["href"]
         for a in soup.find_all("a", href=True) if ".xlsx" in a["href"].lower()), None)
    if not xlsx_url:
        raise ValueError("FATUS: no .xlsx link found")
    df = pd.read_excel(io.BytesIO(download_file(xlsx_url)), sheet_name=0, header=None)
    cal_row = next((i for i, row in df.iterrows()
                    if any("calendar" in str(v).lower() for v in row if str(v) != "nan")), None)
    section = df.iloc[cal_row:].reset_index(drop=True) if cal_row is not None else df
    exp_row = imp_row = None
    for i, row in section.iterrows():
        lbl = str(row.iloc[0]).lower()
        if "agricultural export" in lbl:   exp_row = i
        elif "agricultural import" in lbl: imp_row = i
    if exp_row is None or imp_row is None:
        raise ValueError("FATUS: cannot find export/import rows")
    exp_s = pd.to_numeric(section.iloc[exp_row], errors="coerce")
    imp_s = pd.to_numeric(section.iloc[imp_row], errors="coerce")
    results = []
    for col_idx in range(len(section.columns)):
        try:
            month_str = str(section.iloc[0, col_idx]).strip()
            year_float = float(section.iloc[1, col_idx])
            if month_str in ("nan", "") or pd.isna(year_float): continue
            data_date = datetime.strptime(f"{month_str} {int(year_float)}", "%B %Y").date()
        except Exception:
            continue
        if data_date.year < start_year: continue
        exp_val = exp_s.iloc[col_idx]
        imp_val = imp_s.iloc[col_idx]
        if pd.isna(exp_val) or pd.isna(imp_val): continue
        results.append(make_trade_result(
            "US", "net_food_trade_balance",
            float(exp_val) * 1e9, float(imp_val) * 1e9, None,
            data_date, "monthly", "USDA ERS FATUS (monthly)", xlsx_url,
            "file_download", CONFIDENCE["file_download"]))
    return sorted(results, key=lambda r: r["data_date"])

# ── Comtrade monthly history helpers ─────────────────────────────────────────

def _comtrade_month_chunks(reporter_code, cmd_code, flow_code, start_year=2020,
                           chunk_months=6):
    """
    Fetch Comtrade monthly data in `chunk_months`-month chunks. Comtrade returns
    HS-4 sub-rows even when cmdCode is HS-2, so a 6-month query of 24 chapters
    can exceed the 500-record cap and silently truncate the most recent month.
    Trade collectors should pass `chunk_months=2`; smaller HS4-basket queries
    (export share) can keep the default 6.
    """
    today = date.today()
    all_rows = []
    for year in range(start_year, today.year + 1):
        end_month = today.month if year == today.year else 12
        m = 1
        while m <= end_month:
            chunk_end = min(m + chunk_months - 1, end_month)
            periods = ",".join(f"{year}{i:02d}" for i in range(m, chunk_end + 1))
            url = (f"{_COMTRADE_MONTHLY_BASE}?reporterCode={reporter_code}"
                   f"&period={periods}&cmdCode={cmd_code}"
                   f"&flowCode={flow_code}&partnerCode=0&maxRecords=500")
            try:
                rows = _comtrade_get(url)
                all_rows.extend(rows)
            except Exception:
                pass
            time.sleep(1.2)  # ~50 req/min public API limit
            m = chunk_end + 1
    return all_rows

def _hist_trade_monthly(country_iso, start_year=2020):
    """
    Monthly food trade balance history via Comtrade C/M/HS.

    Exports (X) and imports (M) are fetched in SEPARATE chunked calls so
    neither side is silently truncated by the 500-record cap. Only periods
    where both sides are reported (>0) are emitted — a partial month with
    one side missing has a meaningless balance.
    """
    reporter = _COMTRADE_REPORTERS[country_iso]
    rows_x = _comtrade_month_chunks(reporter, HS_FOOD_CHAPTERS, "X", start_year, chunk_months=2)
    rows_m = _comtrade_month_chunks(reporter, HS_FOOD_CHAPTERS, "M", start_year, chunk_months=2)
    if not rows_x and not rows_m:
        raise ValueError(f"Comtrade monthly history: no data for reporter={reporter}")
    by_p = _defaultdict(lambda: {"X": 0.0, "M": 0.0})
    for row in rows_x + rows_m:
        p = str(row.get("period", "")); fc = row.get("flowCode", "")
        usd = float(row.get("primaryValue") or 0)
        if fc in ("X", "DX", "x"): by_p[p]["X"] += usd
        elif fc in ("M", "m"):     by_p[p]["M"] += usd
    out = []
    for p in sorted(by_p):
        x, m = by_p[p]["X"], by_p[p]["M"]
        if x > 0 and m > 0:
            out.append(make_trade_result(
                country_iso, "net_food_trade_balance", x, m, None,
                datetime.strptime(p, "%Y%m").date(), "monthly",
                "UN Comtrade HS 01-24 (monthly)", _COMTRADE_MONTHLY_BASE,
                "api_monthly", CONFIDENCE["api_monthly"]))
    return out

def _hist_export_share_monthly(country_iso, start_year=2020):
    """Monthly export share history 2020→present via Comtrade basket (10 HS4 codes).
    One API call per year — basket is small (max 120 rows/year)."""
    reporter = _COMTRADE_REPORTERS[country_iso]
    flow = "DX" if reporter == 702 else "X"
    today = date.today()
    all_rows = []
    for year in range(start_year, today.year + 1):
        end_month = today.month if year == today.year else 12
        periods = ",".join(f"{year}{m:02d}" for m in range(1, end_month + 1))
        url = (f"{_COMTRADE_MONTHLY_BASE}?reporterCode={reporter}"
               f"&period={periods}&cmdCode={_HS4_CODES_STR}"
               f"&flowCode={flow}&partnerCode=0&maxRecords=500")
        try:
            rows = _comtrade_get(url)
            if not rows and reporter == 702:
                rows = _comtrade_get(url.replace(f"flowCode={flow}", "flowCode=X"))
            all_rows.extend(rows)
        except Exception:
            pass
        time.sleep(1.2)
    if not all_rows:
        raise ValueError(f"Comtrade monthly basket history: no data for reporter={reporter}")
    by_p = _defaultdict(float)
    for row in all_rows:
        by_p[str(row.get("period", ""))] += float(row.get("primaryValue") or 0)
    out = []
    for p in sorted(by_p):
        val_usd = by_p[p]
        if val_usd <= 0: continue
        dt = datetime.strptime(p, "%Y%m").date()
        try:
            world_monthly_usd, w_year = _export_share_denominator(dt)
        except Exception:
            continue
        share = val_usd / world_monthly_usd
        out.append(make_metric_result(
            country_iso, "share_global_staple_exports",
            round(share, 8), "ratio", dt, "monthly",
            "UN Comtrade monthly basket USD + FAOSTAT TCL Export Value / 12",
            _COMTRADE_MONTHLY_BASE, "api_monthly", CONFIDENCE["api_monthly"],
            raw_value=f"val_usd={val_usd:.0f}, world_monthly_usd={world_monthly_usd:.0f}, w_year={w_year}"))
    return out

# ── India monthly trade — Tavily scrape ──────────────────────────────────────

def _scrape_india_trade_monthly(country_iso, start_year=2020):
    """Tier-2 for IN: scrape Ministry of Commerce press releases via Tavily.
    Scales total merchandise trade by WB food-trade % to estimate food trade.
    Queries last 2 years only to limit API calls. Confidence = 0.60."""
    if not TAVILY_API_KEY:
        raise ValueError("TAVILY_API_KEY not set")
    today = date.today()
    try:
        food_exp_pct, _ = _wb_indicator("IND", "TX.VAL.FOOD.ZS.UN")
        food_imp_pct, _ = _wb_indicator("IND", "TM.VAL.FOOD.ZS.UN")
    except Exception:
        food_exp_pct = food_imp_pct = 9.0  # India ~9% food share of merch trade
    results = []
    for year in range(max(start_year, today.year - 2), today.year + 1):
        end_month = today.month - 1 if year == today.year else 12
        for month in range(1, end_month + 1):
            month_name = date(year, month, 1).strftime("%B")
            query = (f"India foreign trade press note {month_name} {year} "
                     f"exports imports billion commerce ministry")
            try:
                hits = _tavily_search(query, max_results=3)
                for hit in hits:
                    url = hit.get("url", "")
                    if not url or not any(s in url for s in
                            ["commerce.gov.in", "pib.gov.in", "ibef.org", "rbi.org.in"]):
                        continue
                    try:
                        html = fetch_html(url)
                        text = BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)
                    except Exception:
                        continue
                    exp_total = _parse_usd_billion(text, "export")
                    imp_total = _parse_usd_billion(text, "import")
                    if exp_total and imp_total and exp_total > 5e9 and imp_total > 5e9:
                        results.append(make_trade_result(
                            "IN", "net_food_trade_balance",
                            exp_total * food_exp_pct / 100,
                            imp_total * food_imp_pct / 100, None,
                            date(year, month, 1), "monthly",
                            "Web scrape MoCI/PIB x WB food% (Tavily)",
                            url, "web_scrape", CONFIDENCE["web_scrape"]))
                        break
                time.sleep(0.5)
            except Exception:
                continue
    if not results:
        raise ValueError("India scrape: no press-release data extracted")
    return results

# ── UAE monthly trade — Tavily scrape ────────────────────────────────────────

def _scrape_uae_trade_monthly(country_iso, start_year=2020):
    """Tier-2 for AE: scrape UAE statistics portals via Tavily.
    Returns year-level estimates scaled by WB food %. Confidence = 0.60."""
    if not TAVILY_API_KEY:
        raise ValueError("TAVILY_API_KEY not set")
    today = date.today()
    try:
        food_exp_pct, _ = _wb_indicator("ARE", "TX.VAL.FOOD.ZS.UN")
        food_imp_pct, _ = _wb_indicator("ARE", "TM.VAL.FOOD.ZS.UN")
    except Exception:
        food_exp_pct = food_imp_pct = 6.5  # UAE ~6.5% food share
    results = []
    for year in range(max(start_year, today.year - 2), today.year + 1):
        query = (f"UAE United Arab Emirates food imports exports {year} "
                 f"annual trade statistics billion FCSC federal")
        try:
            hits = _tavily_search(query, max_results=3)
            for hit in hits:
                url = hit.get("url", "")
                if not url: continue
                try:
                    html = fetch_html(url)
                    text = BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)
                except Exception:
                    continue
                exp_total = _parse_usd_billion(text, "export")
                imp_total = _parse_usd_billion(text, "import")
                if exp_total and imp_total and exp_total > 1e9:
                    results.append(make_trade_result(
                        "AE", "net_food_trade_balance",
                        exp_total * food_exp_pct / 100,
                        imp_total * food_imp_pct / 100, None,
                        date(year, 1, 1), "annual",
                        "Web scrape UAE stats x WB food% (Tavily)",
                        url, "web_scrape", CONFIDENCE["web_scrape"]))
                    break
            time.sleep(0.5)
        except Exception:
            continue
    if not results:
        raise ValueError("UAE scrape: no data extracted")
    return results

print("Monthly historical collectors defined.")
print("  _fatus_all_months           US trade  — full monthly history via FATUS Excel")
print("  _hist_trade_monthly         BR/PH     — Comtrade monthly, year-by-year loop")
print("  _hist_export_share_monthly  US/BR/PH  — Comtrade basket monthly")
print("  _scrape_india_trade_monthly IN trade  — Tavily tier-2 (last 2 yrs)")
print("  _scrape_uae_trade_monthly   AE trade  — Tavily tier-2 (last 2 yrs)")
# ── Comtrade quarterly historical helpers ─────────────────────────────────────

def _all_quarter_periods(start_year=2020):
    """Generate all quarter period strings from start_year to present."""
    today = date.today()
    current_q = (today.month - 1) // 3 + 1
    out = []
    for year in range(start_year, today.year + 1):
        end_q = current_q if year == today.year else 4
        for q in range(1, end_q + 1):
            out.append(f"{year}Q{q}")
    return out

def _hist_trade_quarterly(country_iso, start_year=2020):
    """
    Quarterly food trade history via Comtrade C/Q/HS, in 8-quarter chunks.
    Exports and imports are fetched as separate flow queries to avoid the
    500-record truncation that drops one side at large response sizes.
    Only periods with both sides reported are emitted.
    """
    reporter = _COMTRADE_REPORTERS[country_iso]
    periods_all = _all_quarter_periods(start_year)
    all_rows = []
    for i in range(0, len(periods_all), 8):
        chunk = periods_all[i:i+8]
        for fc_query in ("X", "M"):
            url = (f"{_COMTRADE_QUARTERLY_BASE}?reporterCode={reporter}"
                   f"&period={','.join(chunk)}&cmdCode={HS_FOOD_CHAPTERS}"
                   f"&flowCode={fc_query}&partnerCode=0&maxRecords=500")
            try:
                rows = _comtrade_get(url)
                all_rows.extend(rows)
            except Exception:
                pass
            time.sleep(1.2)
    if not all_rows:
        raise ValueError(f"Comtrade quarterly history: no data for reporter={reporter}")
    by_p = _defaultdict(lambda: {"X": 0.0, "M": 0.0})
    for row in all_rows:
        p = str(row.get("period", "")); fc = row.get("flowCode", "")
        usd = float(row.get("primaryValue") or 0)
        if fc in ("X", "DX", "x"): by_p[p]["X"] += usd
        elif fc in ("M", "m"):     by_p[p]["M"] += usd
    out = []
    for p in sorted(by_p):
        x, m = by_p[p]["X"], by_p[p]["M"]
        if x > 0 and m > 0:
            out.append(make_trade_result(
                country_iso, "net_food_trade_balance", x, m, None,
                _quarter_to_date(p), "quarterly",
                "UN Comtrade HS 01-24 (quarterly)", _COMTRADE_QUARTERLY_BASE,
                "api_quarterly", CONFIDENCE["api_quarterly"]))
    return out

def _hist_export_share_quarterly(country_iso, start_year=2020):
    """Quarterly export share history 2020→present via Comtrade basket."""
    reporter = _COMTRADE_REPORTERS[country_iso]
    periods_all = _all_quarter_periods(start_year)
    all_rows = []
    for i in range(0, len(periods_all), 8):
        chunk = periods_all[i:i+8]
        url = (f"{_COMTRADE_QUARTERLY_BASE}?reporterCode={reporter}"
               f"&period={','.join(chunk)}&cmdCode={_HS4_CODES_STR}"
               f"&flowCode=X&partnerCode=0&maxRecords=500")
        try:
            rows = _comtrade_get(url)
            all_rows.extend(rows)
        except Exception:
            pass
        time.sleep(1.2)
    if not all_rows:
        raise ValueError(f"Comtrade quarterly basket history: no data for reporter={reporter}")
    by_p = _defaultdict(float)
    for row in all_rows:
        by_p[str(row.get("period", ""))] += float(row.get("primaryValue") or 0)
    out = []
    for p in sorted(by_p):
        val_usd = by_p[p]
        if val_usd <= 0: continue
        dt = _quarter_to_date(p)
        try:
            world_monthly_usd, w_year = _export_share_denominator(dt)
        except Exception:
            continue
        share = (val_usd / 3) / world_monthly_usd  # quarterly ÷ 3 = monthly equiv
        out.append(make_metric_result(
            country_iso, "share_global_staple_exports",
            round(share, 8), "ratio", dt, "quarterly",
            "UN Comtrade quarterly basket USD + FAOSTAT TCL Export Value / 12",
            _COMTRADE_QUARTERLY_BASE, "api_quarterly", CONFIDENCE["api_quarterly"],
            raw_value=f"val_usd_q={val_usd:.0f}, monthly_equiv={val_usd/3:.0f}, world_monthly_usd={world_monthly_usd:.0f}, w_year={w_year}"))
    return out

print("Quarterly historical collectors defined: _hist_trade_quarterly, _hist_export_share_quarterly")


# In[ ]:


# ── ANNUAL HISTORICAL COLLECTORS (fallback when monthly unavailable) ──────────
import uuid as _uuid_mod
from datetime import datetime as _dt_mod

def _hist_trade(country_iso, start_year=2020):
    reporter = _COMTRADE_REPORTERS[country_iso]
    today = date.today()
    years = ",".join(str(y) for y in range(start_year, today.year + 1))
    url = (f"{_COMTRADE_ANNUAL_BASE}?reporterCode={reporter}"
           f"&period={years}&cmdCode={HS_FOOD_CHAPTERS}"
           f"&flowCode=X,M&partnerCode=0&maxRecords=2000")
    rows = _comtrade_get(url)
    by_y = _defaultdict(lambda: {"X": 0.0, "M": 0.0})
    for row in rows:
        p = str(row.get("period", "")); fc = row.get("flowCode", "")
        usd = float(row.get("primaryValue") or 0)
        if fc in ("X","DX","x"): by_y[p]["X"] += usd
        elif fc in ("M","m"):    by_y[p]["M"] += usd
    out = []
    for yr in sorted(by_y):
        x, m = by_y[yr]["X"], by_y[yr]["M"]
        if x > 0 or m > 0:
            out.append(make_trade_result(
                country_iso, "net_food_trade_balance", x, m, None,
                date(int(yr), 1, 1), "annual",
                "UN Comtrade HS 01-24 (annual)", _COMTRADE_ANNUAL_BASE,
                "api_annual", CONFIDENCE["api_annual"]))
    return out


def _hist_csr(country_iso, start_year=2020):
    fao_area = _FAO_AREA[country_iso]
    df = _load_fbs()
    cdf = df[df["Area"] == fao_area].copy()
    if country_iso == "SG":
        return []
    leaf_df = cdf[(cdf["Item"] != "Grand Total") & (~cdf["Item"].isin(_FBS_ITEM_GROUPS))].copy()
    pivot = (leaf_df.pivot_table(index=["Year","Item"], columns="Element",
                                  values="Value", aggfunc="first").reset_index())
    for col in ["Production","Domestic supply quantity","Food supply (kcal/capita/day)"]:
        if col not in pivot.columns: pivot[col] = float("nan")
    mask = (pivot["Domestic supply quantity"].fillna(0) > 0) & \
           pivot["Food supply (kcal/capita/day)"].notna()
    pivot.loc[mask, "_prod_kcal"] = (
        pivot.loc[mask, "Food supply (kcal/capita/day)"]
        * pivot.loc[mask, "Production"].fillna(0)
        / pivot.loc[mask, "Domestic supply quantity"])
    prod_kcal_by_yr = pivot.groupby("Year")["_prod_kcal"].sum(min_count=1)
    gt = cdf[(cdf["Item"]=="Grand Total") & (cdf["Element"]=="Food supply (kcal/capita/day)")]
    supply_kcal_by_yr = gt.set_index("Year")["Value"].dropna()
    combined = pd.DataFrame({"prod_kcal": prod_kcal_by_yr,
                              "supply_kcal": supply_kcal_by_yr}).dropna()
    combined["csr"] = combined["prod_kcal"] / combined["supply_kcal"]
    out = []
    for yr, row in combined[combined.index >= start_year].iterrows():
        out.append(make_metric_result(
            country_iso, "caloric_self_sufficiency_ratio",
            round(float(row["csr"]), 6), "ratio",
            date(int(yr), 1, 1), "annual",
            "FAOSTAT Food Balance Sheets (FBS)", _FBS_URL,
            "file_download", CONFIDENCE["file_download"],
            raw_value=f"csr_kcal={row['csr']:.6f}"))
    return out


def _hist_export_share(country_iso, start_year=2020):
    reporter = _COMTRADE_REPORTERS[country_iso]
    today = date.today()
    years_str = ",".join(str(y) for y in range(start_year, today.year + 1))
    url = (f"{_COMTRADE_ANNUAL_BASE}?reporterCode={reporter}"
           f"&period={years_str}&cmdCode={_HS4_CODES_STR}"
           f"&flowCode=X&partnerCode=0&maxRecords=2000")
    rows = _comtrade_get(url)
    by_y = _defaultdict(float)
    for row in rows:
        by_y[str(row.get("period",""))] += float(row.get("primaryValue") or 0)
    comtrade_ok = any(v > 0 for v in by_y.values())
    if not comtrade_ok:
        tcl_df = _load_tcl()
        fao_area = _FAO_AREA[country_iso]
        present_lower = {i.strip().lower(): i for i in tcl_df["Item"].dropna().unique()}
        basket_items = set()
        for aliases in _TCL_STAPLES_ALIASES.values():
            for alias in aliases:
                m = present_lower.get(alias.strip().lower())
                if m: basket_items.add(m); break
        cdf = tcl_df[(tcl_df["Area"]==fao_area) & tcl_df["Item"].isin(basket_items)
                     & (tcl_df["Year"]>=start_year)]
        for yr, grp in cdf.groupby("Year"):
            by_y[str(int(yr))] = float(grp["Value"].sum()) * 1000  # 1000 USD → USD
        conf_method = "file_download"; source_name = "FAOSTAT TCL Export Value (annual)"
        source_url = _TCL_URL; conf_val = CONFIDENCE["file_download"]
    else:
        conf_method = "api_annual"
        source_name = "UN Comtrade annual basket USD + FAOSTAT TCL Export Value"
        source_url = _COMTRADE_ANNUAL_BASE; conf_val = CONFIDENCE["api_annual"]
    out = []
    for yr in sorted(by_y):
        annual_usd = by_y[yr]
        if annual_usd <= 0 or int(yr) < start_year: continue
        dt = date(int(yr), 1, 1)
        try:
            world_monthly_usd, w_year = _export_share_denominator(dt)
        except Exception:
            continue
        share = (annual_usd / 12) / world_monthly_usd
        out.append(make_metric_result(
            country_iso, "share_global_staple_exports",
            round(share, 8), "ratio", dt, "annual",
            source_name, source_url, conf_method, conf_val,
            raw_value=f"annual_usd={annual_usd:.0f}, world_monthly_usd={world_monthly_usd:.0f}, w_year={w_year}"))
    return out


def _hist_arable(country_iso, start_year=2020):
    fao_area = _FAO_AREA[country_iso]; wb_iso3 = _WB_ISO3[country_iso]
    df = _load_landuse()
    cdf = df[(df["Area"]==fao_area) & (df["Year"]>=start_year)].dropna(subset=["Value"])
    if cdf.empty: return []
    today = date.today()
    for attempt in range(1, 4):
        try:
            r = _requests.get(
                f"https://api.worldbank.org/v2/country/{wb_iso3}/indicator/SP.POP.TOTL"
                f"?format=json&date={start_year}:{today.year}&per_page=30",
                headers=HEADERS, timeout=60)
            r.raise_for_status(); break
        except Exception:
            if attempt == 3: raise
            time.sleep(3 * attempt)
    pop_by_yr = {}
    data = r.json()
    if isinstance(data, list) and len(data) > 1:
        for rec in data[1]:
            if rec.get("value") is not None:
                pop_by_yr[int(rec["date"])] = int(rec["value"])
    out = []
    for _, land_row in cdf.sort_values("Year").iterrows():
        yr = int(land_row["Year"])
        arable_ha = float(land_row["Value"]) * 1000.0
        pop = pop_by_yr.get(yr) or pop_by_yr.get(yr-1) or pop_by_yr.get(yr+1)
        if not pop: continue
        out.append(make_metric_result(
            country_iso, "arable_land_per_capita",
            round(arable_ha/pop, 8), "ha/person",
            date(yr, 1, 1), "annual",
            "FAOSTAT Land Use + World Bank SP.POP.TOTL", _LU_URL,
            "file_download", CONFIDENCE["file_download"],
            raw_value=f"arable_ha={arable_ha:.0f}, pop={pop}"))
    return out


# ── HIST_COLLECTORS dispatch table ────────────────────────────────────────────
# Each key (metric_key, country_iso) maps to an ordered list of functions.
# The first function that returns a non-empty list wins; subsequent are skipped.
# All functions share the signature: fn(country_iso, start_year) -> list[dict]
#
# Frequency achieved per country x metric:
#   Trade balance:    US=monthly(FATUS), BR/PH=monthly(Comtrade),
#                     IN=monthly(scrape,tier2)→annual, SG=annual, AE=monthly(scrape,tier2)→annual
#   Export share:     US/BR/PH=monthly(Comtrade), IN/SG/AE=annual
#   CSR:              all=annual (FBS is annual by design)
#   Arable land:      all=annual (land surveys are annual)

HIST_COLLECTORS = {
    # ── net_food_trade_balance — monthly → quarterly → annual ─────────────────
    ("net_food_trade_balance", "US"): [_fatus_all_months],
    ("net_food_trade_balance", "BR"): [_hist_trade_monthly, _hist_trade_quarterly, _hist_trade],
    ("net_food_trade_balance", "PH"): [_hist_trade_monthly, _hist_trade_quarterly, _hist_trade],
    ("net_food_trade_balance", "IN"): [_scrape_india_trade_monthly, _hist_trade_quarterly, _hist_trade],
    ("net_food_trade_balance", "SG"): [_hist_trade_quarterly, _hist_trade],
    ("net_food_trade_balance", "AE"): [_scrape_uae_trade_monthly, _hist_trade_quarterly, _hist_trade],
    # ── caloric_self_sufficiency_ratio (annual only — FBS is annual) ──────────
    **{("caloric_self_sufficiency_ratio", c): [_hist_csr] for c in COUNTRIES},
    # ── share_global_staple_exports — monthly → quarterly → annual ────────────
    ("share_global_staple_exports", "US"): [_hist_export_share_monthly, _hist_export_share_quarterly, _hist_export_share],
    ("share_global_staple_exports", "BR"): [_hist_export_share_monthly, _hist_export_share_quarterly, _hist_export_share],
    ("share_global_staple_exports", "PH"): [_hist_export_share_monthly, _hist_export_share_quarterly, _hist_export_share],
    ("share_global_staple_exports", "IN"): [_hist_export_share_quarterly, _hist_export_share],
    ("share_global_staple_exports", "SG"): [_hist_export_share_quarterly, _hist_export_share],
    ("share_global_staple_exports", "AE"): [_hist_export_share_quarterly, _hist_export_share],
    # ── arable_land_per_capita (annual only — land surveys are annual) ────────
    **{("arable_land_per_capita", c): [_hist_arable] for c in COUNTRIES},
}

HIST_TRADE_METRICS = {"net_food_trade_balance"}


def run_pipeline_historical(start_year=2020):
    """Run historical collection for all 6 countries x 4 metrics.
    Uses HIST_COLLECTORS dispatch: monthly functions tried first, annual as fallback.
    Safe to rerun — all writes use ON CONFLICT DO UPDATE."""
    run_id  = str(_uuid_mod.uuid4())
    started = _dt_mod.now(timezone.utc).replace(tzinfo=None)
    conn    = get_conn()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO si4_collection_runs (run_id) VALUES (%s)", (run_id,))
    conn.commit()

    total = succeeded = failed = 0
    print(f"Historical run ID: {run_id}  (start_year={start_year})\n")

    for country_iso in COUNTRIES:
        print(f"── {country_iso} ──")
        for metric_key in METRICS:
            total += 1
            is_trade = metric_key in HIST_TRADE_METRICS
            fns = HIST_COLLECTORS.get((metric_key, country_iso), [])

            results = None
            used_fn = None
            errors  = []

            for fn in fns:
                try:
                    r = fn(country_iso, start_year)
                    if r:
                        results = r
                        used_fn = fn.__name__
                        break
                    errors.append(f"{fn.__name__}: returned empty")
                except Exception as exc:
                    errors.append(f"{fn.__name__}: {type(exc).__name__}: {str(exc)[:80]}")

            if not results:
                msg = " | ".join(errors) if errors else "no collectors defined"
                print(f"  ~ [{country_iso}] {metric_key}: no data ({msg})")
                failed += 1
                continue

            stored = 0
            for dp in results:
                try:
                    if is_trade: store_trade_datapoint(conn, dp, run_id)
                    else:        store_metric_datapoint(conn, dp, run_id)
                    stored += 1
                except Exception as exc:
                    print(f"    ! store error: {exc}")

            freq = results[0].get("data_frequency", "?")
            print(f"  ✓ [{country_iso}] {metric_key}: {stored} row(s) "
                  f"({results[0]['data_date']} -> {results[-1]['data_date']}) "
                  f"[{freq}] via {used_fn}")
            succeeded += 1

    finished = _dt_mod.now(timezone.utc).replace(tzinfo=None)
    elapsed  = (finished - started).total_seconds()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM si4_data_gaps WHERE status='open'")
        gaps = cur.fetchone()[0]
        cur.execute("""UPDATE si4_collection_runs
                       SET finished_at=%s, total_tasks=%s, succeeded=%s, failed=%s, gaps_opened=%s
                       WHERE run_id=%s""",
                    (finished, total, succeeded, failed, gaps, run_id))
    conn.commit(); conn.close()
    print(f"\n{'='*60}\nHistorical run complete in {elapsed:.1f}s")
    print(f"  Succeeded: {succeeded}/{total}  Failed: {failed}/{total}")
    print(f"{'='*60}")
    return run_id

print("run_pipeline_historical defined (monthly-upgraded).")
print("HIST_COLLECTORS:", len(HIST_COLLECTORS), "entries")


# ── Entry point ────────────────────────────────────────────────────────────────
# Standard run: snapshot of latest values for all 24 (country × metric) combos.
# Historical run is opt-in via `--historical` (slower, fills 2020→present).
if __name__ == "__main__":
    import sys
    if "--historical" in sys.argv:
        start_year = 2020
        for arg in sys.argv:
            if arg.startswith("--start-year="):
                start_year = int(arg.split("=", 1)[1])
        run_id = run_pipeline_historical(start_year=start_year)
        print(f"\nHistorical run_id = {run_id}")
    else:
        run_id = run_pipeline()
        print(f"\nrun_id = {run_id}")
