from dotenv import load_dotenv
load_dotenv()
import os, psycopg2, psycopg2.extras
from research_agent import run_research_agent as _run_deep_research, get_token_usage as _agent_token_usage

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   os.environ.get("POSTGRES_DB", "gramercy_workstream1"),
    "user":     os.environ.get("POSTGRES_USER", "shankar_1"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

EIA_API_KEY    = os.environ["EIA_API_KEY"]
# Note: the helpers below named `gemini_*` actually call the Anthropic Claude
# API (see lines ~150 and ~1615). Gemini is not used; the names are vestigial
# from a partial rename. The Anthropic key + model are defined further down.

COUNTRIES = {
    "US": {"name": "United States",   "currency": "USD"},
    "AE": {"name": "UAE",             "currency": "AED"},
    "BR": {"name": "Brazil",          "currency": "BRL"},
    "IN": {"name": "India",           "currency": "INR"},
    "SG": {"name": "Singapore",       "currency": "SGD"},
    "PH": {"name": "Philippines",     "currency": "PHP"},
}

METRICS = {
    "electricity_price":           {"label": "Average Industrial Electricity Cost",           "unit": "USD/kWh", "gap_severity": "high"},
    "renewable_share":             {"label": "Renewable Share of Grid",                       "unit": "%",       "gap_severity": "high"},
    "grid_capacity":               {"label": "Total Installed Grid Capacity",                  "unit": "GW",      "gap_severity": "medium"},
    "reserve_margin":              {"label": "Grid Reserve Margin",                            "unit": "%",       "gap_severity": "medium"},
    "energy_investment":           {"label": "Planned Energy Infrastructure Investment (5yr)", "unit": "USD bn",  "gap_severity": "low"},
    "interconnection_queue_depth": {"label": "Grid Interconnection Queue Depth",               "unit": "MW",      "gap_severity": "low"},
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CSI-WS1-Pipeline/1.0; Research; contact: research@csi-project.org)"
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

print("Config loaded.")

import requests as _requests
from datetime import date as _date

# FX cache keyed by (currency, YYYY-MM-DD or 'latest') so we never hit
# Frankfurter twice for the same (currency, date) within a single run.
_FX_RATE_CACHE: dict = {}

def fetch_fx_rate_on_date(currency: str, when) -> float:
    """USD value of 1 unit of `currency` as of `when` (a date or YYYY-MM-DD).

    Uses Frankfurter's historical endpoint. If the requested date is
    pre-1999 (Frankfurter's ECB-backed history start) or in the future,
    falls back to /latest. AED is hardcoded at 0.2723 (it's been pegged
    to USD at 3.6725 AED since 1997).
    """
    if currency == "USD":
        return 1.0
    if currency == "AED":
        return 0.2723
    if isinstance(when, _date):
        when_str = when.isoformat()
    else:
        when_str = str(when)[:10]
    key = (currency, when_str)
    if key in _FX_RATE_CACHE:
        return _FX_RATE_CACHE[key]
    # Pre-1999 or non-parseable → use latest
    try:
        y = int(when_str[:4])
    except (ValueError, TypeError):
        y = 0
    endpoint = ("latest" if (y < 1999 or y > _date.today().year + 1)
                else when_str)
    r = _requests.get(
        f"https://api.frankfurter.app/{endpoint}?from={currency}&to=USD",
        timeout=10,
    )
    r.raise_for_status()
    rate = float(r.json()["rates"]["USD"])
    _FX_RATE_CACHE[key] = rate
    return rate

def fetch_fx_rate(currency: str) -> float:
    """Latest USD rate. Kept for legacy callsites; prefer
    fetch_fx_rate_on_date(currency, data_date) for per-observation accuracy
    so a value dated 2021 uses the 2021 FX rate, not today's."""
    return fetch_fx_rate_on_date(currency, "latest")

# Tests
assert fetch_fx_rate("USD") == 1.0
assert fetch_fx_rate("AED") == 0.2723
brl_rate = fetch_fx_rate("BRL")
assert 0.05 < brl_rate < 0.50, f"BRL rate {brl_rate} looks wrong"
# Historical: BRL/USD at year-end 2020 should be ~0.19
brl_2020 = fetch_fx_rate_on_date("BRL", _date(2020, 12, 30))
assert 0.15 < brl_2020 < 0.25, f"BRL 2020 rate {brl_2020} looks wrong"
print(f"fetch_fx_rate OK — BRL/USD latest={brl_rate:.5f} 2020-12-30={brl_2020:.5f}")

import json, re, requests as _requests
from datetime import date, datetime, timezone

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = "claude-haiku-4-5-20251001"

# Token tracking — Haiku 4.5 pricing per million tokens
_HAIKU_INPUT_COST_PER_M  = 0.80   # USD
_HAIKU_OUTPUT_COST_PER_M = 4.00   # USD
_token_usage = {"input": 0, "output": 0, "calls": 0}

def _track_tokens(response_json: dict):
    usage = response_json.get("usage", {})
    _token_usage["input"]  += usage.get("input_tokens", 0)
    _token_usage["output"] += usage.get("output_tokens", 0)
    _token_usage["calls"]  += 1

def print_token_summary():
    # Combine pipeline tokens + agent tokens
    agent = _agent_token_usage()
    inp   = _token_usage["input"]  + agent["input"]
    out   = _token_usage["output"] + agent["output"]
    calls = _token_usage["calls"]  + agent["calls"]
    cost  = (inp / 1_000_000 * _HAIKU_INPUT_COST_PER_M) + (out / 1_000_000 * _HAIKU_OUTPUT_COST_PER_M)
    print(f"\n{'─'*50}")
    print(f"Claude usage: {calls} calls | {inp:,} input + {out:,} output tokens")
    print(f"Estimated cost: ${cost:.4f} USD")
    print(f"{'─'*50}")

def _infer_date_from_source(source_url: str, extracted_dstr: str) -> date:
    """
    Determine the correct data date:
    1. Use Claude's extracted date if it looks valid (not in the future, not suspiciously recent)
    2. Otherwise infer year from the source URL/filename
    3. Never return a future date — cap at today
    """
    today = date.today()

    # Try Claude's extracted date first
    if extracted_dstr:
        try:
            d = datetime.fromisoformat(extracted_dstr).date()
            if d <= today:  # reject future dates
                return d
        except Exception:
            pass

    # Extract year from URL (e.g. "NERC_SRA_2024.pdf" → 2024, "/2023/" → 2023)
    year_matches = re.findall(r"20\d{2}", source_url)
    if year_matches:
        url_year = int(year_matches[-1])  # use last year found in URL
        if 2015 <= url_year <= today.year:
            return date(url_year, 1, 1)

    # Last resort: use previous year (data is almost never from the current year)
    return date(today.year - 1, 1, 1)


def gemini_extract(text: str, metric_key: str, country_iso: str, source_url: str) -> dict:
    """Extract metric value from text using Claude API."""
    metric_label = METRICS[metric_key]["label"]
    metric_unit  = METRICS[metric_key]["unit"]

    prompt = (
        f"From the following content (source: {source_url}), extract the most recent value for:\n\n"
        f"Metric: {metric_label}\nUnit: {metric_unit}\nCountry ISO: {country_iso}\n\n"
        f"Respond ONLY with a valid JSON object — no prose, no markdown fences:\n"
        '{{"value": <float or null>, "raw_text": "<exact text found>", '
        '"data_date": "<YYYY-MM-DD or null>", "frequency": "<monthly|quarterly|annual|irregular>"}}\n\n'
        f"Content:\n---\n{text[:4000]}\n---"
    )

    r = _requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json={
            "model":      CLAUDE_MODEL,
            "max_tokens": 512,
            "messages":   [{"role": "user", "content": prompt}],
        },
        timeout=45,
    )
    r.raise_for_status()
    resp = r.json()
    _track_tokens(resp)
    raw_out = resp["content"][0]["text"].strip()
    raw_out = re.sub(r"^```(?:json)?\s*", "", raw_out, flags=re.MULTILINE)
    raw_out = re.sub(r"^```\s*$",         "", raw_out, flags=re.MULTILINE).strip()

    parsed = json.loads(raw_out)
    val = parsed.get("value")
    if val is None:
        raise ValueError(f"Claude returned null for {metric_key} at {source_url}")

    dstr = parsed.get("data_date")
    try:
        data_date = datetime.fromisoformat(dstr).date() if dstr else date.today().replace(day=1)
    except Exception:
        data_date = date.today().replace(day=1)

    return {
        "value":     float(val),
        "raw_text":  parsed.get("raw_text", ""),
        "data_date": data_date,
        "frequency": parsed.get("frequency", "irregular"),
    }

print("gemini_extract defined.")

import time
import requests as _requests
from bs4 import BeautifulSoup

def fetch_html_requests(url: str, timeout: int = 30) -> str:
    """Static HTML fetch with up to 3 retries and exponential backoff."""
    for attempt in range(1, 4):
        try:
            r = _requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as exc:
            if attempt == 3:
                raise
            time.sleep(2 * attempt)

def fetch_html_playwright(url: str) -> str:
    """Headless Chromium fetch for JS-rendered pages."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        page.set_extra_http_headers({"User-Agent": HEADERS["User-Agent"]})
        page.goto(url, wait_until="networkidle", timeout=30000)
        html = page.content()
        browser.close()
    return html

_PLAYWRIGHT_DOMAINS = {
    "ema.gov.sg", "data.gov.sg", "greenplan.gov.sg",
    "doe.gov.ph", "ngcp.ph", "moei.gov.ae", "dewa.gov.ae",
    "aneel.gov.br", "epe.gov.br", "ons.org.br",
    "mnre.gov.in", "powermin.gov.in", "npp.gov.in",
}

def fetch_html(url: str) -> str:
    """
    Fetch page HTML. Uses Playwright immediately for known JS-heavy domains.
    Falls back to Playwright for any domain where requests returns < 500 chars of text.
    """
    domain = url.split("/")[2].lstrip("www.")
    use_playwright = any(d in domain for d in _PLAYWRIGHT_DOMAINS)

    if not use_playwright:
        try:
            html = fetch_html_requests(url)
            text = BeautifulSoup(html, "html.parser").get_text(strip=True)
            if len(text) > 500:
                return html
        except Exception:
            pass

    # Use Playwright
        pass
    return fetch_html_playwright(url)

def download_pdf(url: str) -> bytes:
    """Download PDF bytes; if landing page, follow first .pdf href. Max 50 MB."""
    r = _requests.get(url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "pdf" not in ct and not url.lower().endswith(".pdf"):
        soup  = BeautifulSoup(r.text, "html.parser")
        hrefs = [a["href"] for a in soup.find_all("a", href=True)
                 if a["href"].lower().endswith(".pdf")]
        if not hrefs:
            raise ValueError(f"No PDF link found at {url}")
        pdf_url = hrefs[0] if hrefs[0].startswith("http") else url.rsplit("/", 1)[0] + "/" + hrefs[0]
        r = _requests.get(pdf_url, headers=HEADERS, timeout=120)
        r.raise_for_status()
    if len(r.content) > 50 * 1024 * 1024:
        raise ValueError(f"PDF too large (>50MB) at {url}")
    return r.content

print("HTML/PDF helpers defined.")

def make_result(country_iso, metric_key, value, unit, data_date, data_frequency,
                source_name, source_url, access_method, confidence_score,
                raw_value=None, currency_conversion=None, is_imputed=False):
    """Returns the standard collector result dict."""
    return {
        "country_iso":        country_iso,
        "country_name":       COUNTRIES[country_iso]["name"],
        "metric_key":         metric_key,
        "metric_label":       METRICS[metric_key]["label"],
        "metric_value":       round(float(value), 6),
        "unit":               unit,
        "data_date":          data_date,
        "data_frequency":     data_frequency,
        "source_name":        source_name,
        "source_url":         source_url,
        "access_method":      access_method,
        "confidence_score":   confidence_score,
        "raw_value":          str(raw_value) if raw_value is not None else None,
        "currency_conversion": str(currency_conversion) if currency_conversion else None,
        "is_imputed":         is_imputed,
    }

print("make_result defined.")

import requests as _requests
from datetime import datetime, date
from collections import defaultdict

def _parse_eia_period(period_str: str) -> date:
    if len(period_str) == 7:
        return datetime.strptime(period_str + "-01", "%Y-%m-%d").date()
    if len(period_str) == 10:
        return datetime.strptime(period_str, "%Y-%m-%d").date()
    return date.today().replace(day=1)

def collect_eia(country_iso, metric_key, endpoint, params, value_field,
                value_scale=1.0, compute=None, aggregate=None,
                source_url=None, confidence=1.00, **_):
    """EIA Open Data API collector. Raises on any failure."""
    full_params = {**params, "api_key": EIA_API_KEY}
    base = "https://api.eia.gov"
    url  = base + endpoint
    src  = source_url or url

    r = _requests.get(url, params=full_params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    data = r.json()

    if "response" not in data or "data" not in data["response"]:
        raise ValueError(f"EIA: unexpected structure — {list(data.keys())}")

    rows = data["response"]["data"]
    if not rows:
        raise ValueError("EIA: no rows returned")

    if compute == "renewable_share_pct":
        renewable_fuels = {"SUN", "WND", "WAT", "GEO", "WAS", "BIO"}
        period_gen = defaultdict(lambda: {"renew": 0.0, "total": 0.0})
        for row in rows:
            period = row.get("period", "")
            gen    = float(row.get(value_field) or 0)
            fuel   = row.get("fueltypeid", "")
            period_gen[period]["total"] += gen
            if fuel in renewable_fuels:
                period_gen[period]["renew"] += gen
        latest = sorted(period_gen.keys())[-1]
        pg = period_gen[latest]
        if pg["total"] == 0:
            raise ValueError("EIA: total generation is zero")
        value     = round(pg["renew"] / pg["total"] * 100, 2)
        data_date = _parse_eia_period(latest)
        frequency = "monthly"

    elif aggregate == "sum":
        period_totals = defaultdict(float)
        for row in rows:
            period_totals[row.get("period", "")] += float(row.get(value_field) or 0)
        latest    = sorted(period_totals.keys())[-1]
        value     = period_totals[latest] * value_scale
        data_date = _parse_eia_period(latest)
        frequency = "monthly"

    else:
        row  = rows[0]
        raw  = row.get(value_field)
        if raw is None:
            raise ValueError(f"EIA: field '{value_field}' is null")
        value     = float(raw) * value_scale
        data_date = _parse_eia_period(row["period"])
        frequency = params.get("frequency", "monthly")

    return make_result(country_iso, metric_key, value,
                       METRICS[metric_key]["unit"], data_date, frequency,
                       "EIA Open Data API", src, "api", confidence,
                       raw_value=rows[0].get(value_field))

try:
    result = collect_eia(
        country_iso="US", metric_key="electricity_price",
        endpoint="/v2/electricity/retail-sales/data/",
        params={"frequency": "monthly", "data[0]": "price", "facets[sectorid][]": "IND",
                "sort[0][column]": "period", "sort[0][direction]": "desc", "length": "1"},
        value_field="price", value_scale=0.01,
        source_url="https://api.eia.gov/v2/electricity/retail-sales/data/",
        confidence=CONFIDENCE["api_monthly"],
    )
    print(f"collect_eia OK — {result['metric_value']:.4f} {result['unit']} ({result['data_date']})")
except Exception as e:
    print(f"collect_eia: {e}")

import requests as _requests
from datetime import date

def _wb_api_fetch(iso, indicator, mrv=5, retries=3):
    """GET a World Bank WDI series with retry. WB returns transient 400/timeout
    under load — a quiet retry chain hides the flakes."""
    import time as _time
    url    = f"https://api.worldbank.org/v2/country/{iso}/indicator/{indicator}"
    params = {"format": "json", "mrv": str(mrv), "per_page": str(mrv)}
    last_err = None
    for attempt in range(retries):
        try:
            r = _requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list) or len(data) < 2:
                raise ValueError("WorldBank: unexpected response structure")
            if isinstance(data[0], dict) and data[0].get("message"):
                raise ValueError(f"WorldBank API message: {data[0]['message']}")
            return data
        except (_requests.RequestException, ValueError) as e:
            last_err = e
            _time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"WorldBank {indicator}/{iso}: {last_err}")


def collect_worldbank(country_iso, metric_key, indicator, confidence=0.88, **_):
    iso2    = country_iso
    src_url = f"https://data.worldbank.org/indicator/{indicator}?locations={iso2}"

    data = _wb_api_fetch(iso2, indicator, mrv=5)
    obs  = [o for o in data[1] if o.get("value") is not None]
    if not obs:
        raise ValueError(f"WorldBank: no non-null data for {indicator}/{iso2}")

    o         = max(obs, key=lambda x: int(x["date"]))
    value     = float(o["value"])
    data_date = date(int(o["date"]), 1, 1)

    return make_result(country_iso, metric_key, value,
                       METRICS[metric_key]["unit"], data_date, "annual",
                       f"World Bank — {indicator}", src_url, "api", confidence,
                       raw_value=o["value"])

try:
    result = collect_worldbank("US", "renewable_share", "EG.ELC.RNEW.ZS")
    print(f"collect_worldbank OK — {result['metric_value']:.2f}% ({result['data_date']})")
except Exception as e:
    print(f"collect_worldbank: {e}")

import requests as _requests, io
from datetime import date
import pandas as pd

_IRENA_PXWEB_BASE = "https://pxweb.irena.org/api/v1/en/IRENASTAT/Power%20Capacity%20and%20Generation"
# Country display names IRENA uses (for matching the country dimension by text
# when ISO3 lookup isn't possible). Some tables truncate labels (e.g. "United
# Arab Em"), so prefer ISO3 lookup below.
_IRENA_COUNTRY_MAP = {
    "AE": "United Arab Emirates",
    "US": "United States of America",
    "BR": "Brazil",
    "IN": "India",
    "SG": "Singapore",
    "PH": "Philippines",
}
# ISO3 codes IRENA uses as country dimension values. Robust to label changes
# (e.g. "United States of America" → "USA") and label truncation.
_IRENA_ISO3 = {"US": "USA", "AE": "ARE", "BR": "BRA",
               "IN": "IND", "SG": "SGP", "PH": "PHL"}
# IRENA's pre-computed renewable-share table. Two indicators are published:
#   "capacity"   — RE share of electricity *capacity*   (installed nameplate)
#   "generation" — RE share of electricity *generation* (energy actually delivered)
# We default to capacity because it's what IRENA's public-facing tables surface
# (e.g. https://pxweb.irena.org → RE-SHARE) and what's commonly cited. Flip to
# "generation" via SI1_IRENA_INDICATOR=generation for a more economically
# meaningful number (low capacity-factor renewables overstate capacity share).
_IRENA_RE_SHARE_TABLE     = "RE-SHARE_2026_H1_v-PX 1.px"
_IRENA_RE_SHARE_INDICATOR = os.environ.get("SI1_IRENA_INDICATOR", "capacity")


def _irena_pxweb_json(method: str, url: str, **kw):
    """GET/POST against IRENA PxWeb with two retries; raise if all attempts
    return non-JSON or no `variables`/`value` payload (their server occasionally
    returns an HTML error page under load)."""
    last_err = None
    for attempt in range(3):
        try:
            r = (_requests.post(url, timeout=30, headers=HEADERS, **kw)
                 if method == "POST"
                 else _requests.get(url, timeout=30, headers=HEADERS, **kw))
            r.raise_for_status()
            data = r.json()
            if "variables" in data or "value" in data or "dimension" in data:
                return data
            last_err = f"unexpected payload keys: {list(data.keys())[:5]}"
        except (_requests.RequestException, ValueError) as e:
            last_err = repr(e)
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"IRENA PxWeb {method} {url}: {last_err}")


def _irena_re_share(country_iso: str, confidence: float = 0.85, **_):
    """Fetch IRENA's pre-computed renewable share of electricity (%) for a country.

    Uses the authoritative RE-SHARE table — no manual division required, no
    ambiguous tech-row matching. Returns the most recent non-null year.
    """
    country_name = _IRENA_COUNTRY_MAP.get(country_iso, country_iso)
    url = f"{_IRENA_PXWEB_BASE}/{_requests.utils.quote(_IRENA_RE_SHARE_TABLE)}"

    meta = _irena_pxweb_json("GET", url)
    country_var = next(v for v in meta["variables"]
                       if "country" in v["code"].lower() or "region" in v["code"].lower())
    ind_var     = next(v for v in meta["variables"] if v["code"].lower().startswith("ind"))
    year_var    = next(v for v in meta["variables"] if "year" in v["code"].lower())

    # Prefer ISO3 — works even when IRENA truncates display labels.
    iso3 = _IRENA_ISO3.get(country_iso)
    if iso3 and iso3 in country_var["values"]:
        country_code = iso3
    else:
        country_idx = next(
            (i for i, t in enumerate(country_var["valueTexts"])
             if country_name.lower() in t.lower()),
            None
        )
        if country_idx is None:
            raise ValueError(f"IRENA RE-SHARE: '{country_iso}/{country_name}' not in country list")
        country_code = country_var["values"][country_idx]

    # Pick generation vs capacity indicator.
    needle = "generation" if _IRENA_RE_SHARE_INDICATOR == "generation" else "capacity"
    ind_idx = next(
        (i for i, t in enumerate(ind_var["valueTexts"]) if needle in t.lower()),
        None
    )
    if ind_idx is None:
        raise ValueError(f"IRENA RE-SHARE: indicator '{needle}' not found")
    ind_code = ind_var["values"][ind_idx]

    query = {
        "query": [
            {"code": country_var["code"], "selection": {"filter": "item", "values": [country_code]}},
            {"code": ind_var["code"],     "selection": {"filter": "item", "values": [ind_code]}},
        ],
        "response": {"format": "json-stat2"},
    }
    data = _irena_pxweb_json("POST", url, json=query)
    years = list(data["dimension"][year_var["code"]]["category"]["label"].values())
    vals  = data.get("value", [])
    paired = [(int(y), v) for y, v in zip(years, vals) if v is not None]
    if not paired:
        raise ValueError(f"IRENA RE-SHARE: no values for {country_name}")
    year, share = max(paired, key=lambda x: x[0])
    return make_result(country_iso, "renewable_share", round(float(share), 2), "%",
                       date(year, 1, 1), "annual",
                       f"IRENA Renewable Energy Share ({_IRENA_RE_SHARE_INDICATOR})",
                       "https://pxweb.irena.org",
                       "api", confidence,
                       raw_value=f"indicator={_IRENA_RE_SHARE_INDICATOR}")

def _irena_find_table():
    """Dynamically find the current capacity table from IRENA PxWeb metadata."""
    r = _requests.get(_IRENA_PXWEB_BASE + "/", headers=HEADERS, timeout=30)
    r.raise_for_status()
    tables = r.json()
    # PxWeb may return list of dicts {"id":...} or list of strings
    def _tid(t):
        return t.get("id", "") if isinstance(t, dict) else str(t)
    for t in tables:
        tid = _tid(t)
        if "ELECCAP" in tid.upper() or "CAP" in tid.upper():
            return f"{_IRENA_PXWEB_BASE}/{tid}"
    return f"{_IRENA_PXWEB_BASE}/{_tid(tables[0])}"

def _irena_load_meta(table_url):
    """Fetch and parse IRENA PxWeb table metadata into vars_meta dict."""
    meta = _requests.get(table_url, headers=HEADERS, timeout=30).json()
    vars_meta = {}
    for v in meta.get("variables", []):
        if not isinstance(v, dict):
            continue
        code  = v.get("code", "")
        vals  = v.get("values", [])
        texts = v.get("valueTexts", vals)
        vars_meta[code] = {"values": vals, "texts": texts}
    return vars_meta

def _irena_query_mw(table_url, country_key, country_code, vars_meta, tech_match):
    """POST a single IRENA query filtered to one technology keyword; return (year, mw)."""
    query = {
        "query": [{"code": country_key, "selection": {"filter": "item", "values": [country_code]}}],
        "response": {"format": "json-stat2"},
    }
    if "Technology" in vars_meta and tech_match:
        tech_texts = vars_meta["Technology"]["texts"]
        tech_vals  = vars_meta["Technology"]["values"]
        tech_idx   = next(
            (i for i, t in enumerate(tech_texts)
             if all(kw in t.lower() for kw in tech_match)),
            None
        )
        if tech_idx is not None:
            query["query"].append({"code": "Technology",
                                   "selection": {"filter": "item", "values": [tech_vals[tech_idx]]}})
    r = _requests.post(table_url, json=query, headers=HEADERS, timeout=60)
    r.raise_for_status()
    data   = r.json()
    values = list(data.get("value", []))
    dims   = data.get("dimension", {})
    year_dim = next((k for k in dims if "year" in k.lower() or "time" in k.lower()), None)
    years  = list(dims[year_dim]["category"]["label"].values()) if year_dim else []
    paired = [(int(y), v) for y, v in zip(years, values) if v is not None and v > 0]
    if not paired:
        return None
    return max(paired, key=lambda x: x[0])

def collect_irena(country_iso, metric_key, confidence=0.75, **_):
    # renewable_share goes through IRENA's pre-computed RE-SHARE table — the
    # old manual capacity-division logic produced 100% for the Philippines
    # because the technology-row matcher couldn't disambiguate "Total" from
    # "Total renewable". RE-SHARE is the authoritative number IRENA itself
    # publishes for press releases.
    if metric_key == "renewable_share":
        return _irena_re_share(country_iso, confidence=max(confidence, 0.85))

    country_name = _IRENA_COUNTRY_MAP.get(country_iso, country_iso)
    table_url    = _irena_find_table()
    vars_meta    = _irena_load_meta(table_url)

    country_key  = "Country/area" if "Country/area" in vars_meta else "Country"
    if country_key not in vars_meta:
        raise ValueError("IRENA PxWeb: no Country variable in metadata")

    country_vals  = vars_meta[country_key]["values"]
    country_texts = vars_meta[country_key]["texts"]
    # Prefer ISO3 lookup — robust against truncated labels like "United Arab Em".
    iso3 = _IRENA_ISO3.get(country_iso)
    country_code = iso3 if iso3 in country_vals else None
    if country_code is None:
        idx = next((i for i, t in enumerate(country_texts)
                    if country_name.lower() in t.lower()), None)
        if idx is None:
            raise ValueError(f"IRENA PxWeb: '{country_iso}/{country_name}' not in country list")
        country_code = country_vals[idx]

    # grid_capacity: IRENA ELECCAP has no "Total" row — only "Total renewable
    # energy" and "Total non-renewable energy". The earlier code's regex picked
    # whichever matched first, returning either the renewable-only total
    # (Singapore 1.7 GW) or the non-renewable-only total (US 336 GW). The
    # correct grand total is the sum of the two.
    if "Technology" in vars_meta:
        tech_texts = vars_meta["Technology"]["texts"]
        tech_vals  = vars_meta["Technology"]["values"]
        renew_idx = next(
            (i for i, t in enumerate(tech_texts)
             if "total renewable" in t.lower()), None)
        nonrenew_idx = next(
            (i for i, t in enumerate(tech_texts)
             if "total non-renewable" in t.lower() or "total nonrenewable" in t.lower()), None)
        if renew_idx is None or nonrenew_idx is None:
            raise ValueError("IRENA ELECCAP: missing renewable/non-renewable totals")
        renew_res    = _irena_query_mw(table_url, country_key, country_code, vars_meta,
                                       tech_match=["total", "renewable"])
        nonrenew_res = _irena_query_mw(table_url, country_key, country_code, vars_meta,
                                       tech_match=["total", "non-renewable"])
        if nonrenew_res is None:
            nonrenew_res = _irena_query_mw(table_url, country_key, country_code, vars_meta,
                                           tech_match=["total", "nonrenewable"])
        if renew_res is None and nonrenew_res is None:
            raise ValueError(f"IRENA ELECCAP: no capacity data for {country_name}")
        renew_year, renew_mw = renew_res if renew_res else (0, 0.0)
        nr_year,    nr_mw    = nonrenew_res if nonrenew_res else (0, 0.0)
        total_mw = float(renew_mw) + float(nr_mw)
        year     = max(renew_year, nr_year)
        value    = round(total_mw / 1000, 3)
        return make_result(country_iso, metric_key, value, "GW",
                           date(year, 1, 1), "annual",
                           "IRENA Power Capacity Statistics", "https://pxweb.irena.org",
                           "api", confidence,
                           raw_value=f"renew={renew_mw:.0f}MW + nonrenew={nr_mw:.0f}MW")

    # Fallback: no Technology dimension — sum whatever the table returns
    query = {
        "query": [{"code": country_key, "selection": {"filter": "item", "values": [country_code]}}],
        "response": {"format": "json-stat2"},
    }
    r = _requests.post(table_url, json=query, headers=HEADERS, timeout=60)
    r.raise_for_status()
    data   = r.json()
    values = list(data.get("value", []))
    dims   = data.get("dimension", {})
    year_dim = next((k for k in dims if "year" in k.lower() or "time" in k.lower()), None)
    years  = list(dims[year_dim]["category"]["label"].values()) if year_dim else []
    paired = [(int(y), v) for y, v in zip(years, values) if v is not None and v > 0]
    if not paired:
        raise ValueError(f"IRENA PxWeb: no total capacity data for {country_name}")
    year, raw_mw = max(paired, key=lambda x: x[0])
    value = round(raw_mw / 1000, 3)
    return make_result(country_iso, metric_key, value, "GW",
                       date(year, 1, 1), "annual",
                       "IRENA Power Capacity Statistics", "https://pxweb.irena.org",
                       "api", confidence, raw_value=raw_mw)

def _pick_best(r1, r2):
    """Return the result with the more recent data_date; on tie, higher confidence."""
    if r1 is None:
        return r2
    if r2 is None:
        return r1
    if r1["data_date"] > r2["data_date"]:
        return r1
    if r2["data_date"] > r1["data_date"]:
        return r2
    return r1 if r1["confidence_score"] >= r2["confidence_score"] else r2

def collect_irena_or_worldbank(country_iso, metric_key, wb_indicator=None, confidence=0.75, **_):
    """Run IRENA PxWeb and World Bank independently, return the more recent/reliable result."""
    r_irena, r_wb = None, None
    try:
        r_irena = collect_irena(country_iso, metric_key, confidence=confidence)
    except Exception:
        pass
    if wb_indicator:
        try:
            r_wb = collect_worldbank(country_iso, metric_key,
                                     indicator=wb_indicator,
                                     confidence=CONFIDENCE["api_annual"])
        except Exception:
            pass
    result = _pick_best(r_irena, r_wb)
    if result is None:
        raise ValueError(f"IRENA and World Bank both failed for {country_iso}/{metric_key}")
    return result

# Smoke test
try:
    result = collect_irena("AE", "grid_capacity")
    print(f"collect_irena OK — {result['metric_value']} {result['unit']}")
except Exception as e:
    print(f"collect_irena: {e}  (update IRENA_CAPACITY_URL if file moved)")

import requests as _requests
from datetime import date, datetime

ANEEL_BASE = "https://dadosabertos.aneel.gov.br"

def _aneel_get_resource_id(dataset_slug: str) -> str:
    url = f"{ANEEL_BASE}/api/3/action/package_show?id={dataset_slug}"
    r   = _requests.get(url, timeout=30)
    r.raise_for_status()
    resources = r.json()["result"]["resources"]
    for res in resources:
        if res.get("datastore_active"):
            return res["id"]
    return resources[0]["id"]

def collect_aneel_ckan(country_iso, metric_key, dataset, value_field,
                        filters=None, value_scale=1.0, aggregate=None,
                        compute=None, confidence=0.92, **_):
    if country_iso != "BR":
        raise ValueError(f"ANEEL only applies to Brazil, got {country_iso}")

    resource_id = _aneel_get_resource_id(dataset)
    url    = f"{ANEEL_BASE}/api/3/action/datastore_search"
    params = {"resource_id": resource_id, "limit": 1000}
    if filters:
        for k, v in filters.items():
            params[f"filters[{k}]"] = v

    r = _requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    records = r.json()["result"]["records"]
    if not records:
        raise ValueError(f"ANEEL: no records for dataset={dataset}")

    # Parse date from first record
    date_field = next((k for k in records[0] if "dat" in k.lower() or "per" in k.lower()), None)
    data_date  = date.today().replace(day=1)
    if date_field:
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m"):
            try:
                data_date = datetime.strptime(str(records[0][date_field])[:10], fmt).date()
                break
            except Exception:
                continue

    fx_rate = fetch_fx_rate("BRL")

    if aggregate == "sum" or compute == "grid_capacity_gw":
        total_kw = sum(float(rec.get(value_field, 0) or 0) for rec in records)
        value_gw = total_kw * value_scale
        return make_result(country_iso, metric_key, value_gw, "GW",
                           data_date, "monthly", "ANEEL Open Data — CKAN",
                           f"{ANEEL_BASE}/dataset/{dataset}", "api", confidence,
                           raw_value=total_kw)

    if compute == "renewable_share_pct":
        fuel_col  = next((k for k in records[0]
                          if "fonte" in k.lower() or "combusti" in k.lower()), None)
        renew_kws = {"solar", "eólica", "eolica", "hidro", "biomassa", "pcg", "cgr"}
        total_kw  = sum(float(rec.get(value_field, 0) or 0) for rec in records)
        renew_kw  = sum(float(rec.get(value_field, 0) or 0) for rec in records
                        if fuel_col and any(k in str(rec.get(fuel_col, "")).lower() for k in renew_kws))
        if total_kw == 0:
            raise ValueError("ANEEL: total capacity is zero")
        value = round(renew_kw / total_kw * 100, 2)
        return make_result(country_iso, metric_key, value, "%",
                           data_date, "monthly", "ANEEL Open Data — CKAN",
                           f"{ANEEL_BASE}/dataset/{dataset}", "api", confidence)

    # Default: electricity price — BRL/MWh → USD/kWh
    raw_brl_mwh = float(records[0].get(value_field, 0) or 0)
    usd_kwh     = raw_brl_mwh / 1000 * fx_rate
    return make_result(country_iso, metric_key, usd_kwh, "USD/kWh",
                       data_date, "monthly", "ANEEL Open Data — CKAN",
                       f"{ANEEL_BASE}/dataset/{dataset}", "api", confidence,
                       raw_value=raw_brl_mwh,
                       currency_conversion=f"BRL/USD @ {fx_rate:.4f}")

# Smoke test
try:
    result = collect_aneel_ckan("BR", "electricity_price",
                                 "tarifas-distribuidoras-energia-eletrica",
                                 "VlrTarifaAplicadaDistribuicao",
                                 filters={"DscClasseConsumidor": "Industrial"})
    print(f"collect_aneel_ckan OK — {result['metric_value']:.4f} {result['unit']}")
except Exception as e:
    print(f"collect_aneel_ckan: {e}")

import requests as _requests
from datetime import date, datetime

def collect_data_gov_sg(country_iso, metric_key, resource_id, value_field,
                         date_field=None, currency="SGD", confidence=0.92, **_):
    """data.gov.sg v1 API: initiate-download → poll-download → CSV parse."""
    import csv, io as _io, time as _time
    api_key = os.environ.get("DATAGOV_SG_API_KEY", "")
    sg_headers = {**HEADERS, **({"X-API-KEY": api_key} if api_key else {})}

    initiate_url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{resource_id}/initiate-download"
    poll_url     = f"https://api-open.data.gov.sg/v1/public/api/datasets/{resource_id}/poll-download"

    _requests.get(initiate_url, headers=sg_headers, timeout=30).raise_for_status()

    download_url = None
    for _ in range(6):
        rp = _requests.get(poll_url, headers=sg_headers, timeout=30)
        rp.raise_for_status()
        dl = rp.json().get("data") or rp.json()
        download_url = dl.get("url") or dl.get("downloadUrl")
        if download_url:
            break
        _time.sleep(5)

    if not download_url:
        raise ValueError(f"data.gov.sg: poll-download did not return a URL for {resource_id}")

    rc = _requests.get(download_url, headers=HEADERS, timeout=60)
    rc.raise_for_status()
    records = list(csv.DictReader(_io.StringIO(rc.content.decode("utf-8-sig"))))
    if not records:
        raise ValueError(f"data.gov.sg: CSV is empty for dataset {resource_id}")

    cols = list(records[0].keys())
    label_col = cols[0]  # usually "DataSeries" or similar

    # Wide format: find the row matching value_field keyword, then pick latest date column
    import re as _re
    target_row = next(
        (r for r in records if value_field.lower() in str(r.get(label_col, "")).lower()),
        None
    )
    if target_row is None:
        # Narrow format fallback
        if value_field in cols:
            target_row = sorted(records, key=lambda r: r.get(date_field or cols[1], ""), reverse=True)[0]
        else:
            raise ValueError(f"data.gov.sg: '{value_field}' not found in rows or columns {cols[:10]}")

    # Date columns look like "2026Jun", "2026May", "2025Q4" etc — pick the latest non-empty
    date_cols = [c for c in cols[1:] if _re.match(r"\d{4}", c)]
    date_cols_sorted = sorted(date_cols, reverse=True)
    raw_val = None
    best_date_str = None
    for dc in date_cols_sorted:
        v = target_row.get(dc, "").strip()
        if v and v not in ("na", "-", ""):
            try:
                raw_val = float(v)
                best_date_str = dc
                break
            except ValueError:
                continue

    if raw_val is None:
        raise ValueError(f"data.gov.sg: no numeric value found in row for '{value_field}'")

    fx_rate = fetch_fx_rate(currency)
    usd_kwh = raw_val / 100 * fx_rate

    # Parse date from column name e.g. "2026Jun" → 2026-06-01, "2025Q4" → 2025-10-01
    data_date = date.today().replace(day=1)
    if best_date_str:
        try:
            m = _re.match(r"(\d{4})([A-Za-z]+)", best_date_str)
            if m:
                yr, mon = int(m.group(1)), m.group(2)
                if "Q" in mon.upper():
                    q = int(mon.strip("Qq"))
                    data_date = date(yr, (q - 1) * 3 + 1, 1)
                else:
                    data_date = datetime.strptime(f"{yr}{mon}", "%Y%b").date().replace(day=1)
        except Exception:
            pass

    freq = "monthly"
    return make_result(country_iso, metric_key, usd_kwh, "USD/kWh",
                       data_date, freq, "data.gov.sg",
                       f"https://data.gov.sg/datasets/{resource_id}",
                       "api", confidence, raw_value=raw_val,
                       currency_conversion=f"SGD cents/kWh → USD/kWh @ {fx_rate:.4f}")

# Smoke test
try:
    result = collect_data_gov_sg("SG", "electricity_price",
                                  "d_61eac3cdb086814af485dcc682b75ae9",
                                  value_field="tariff", date_field="quarter")
    print(f"collect_data_gov_sg OK — {result['metric_value']:.4f} {result['unit']}")
except Exception as e:
    print(f"collect_data_gov_sg: {e}")

import requests as _requests, io
from datetime import date
import pandas as pd

_ONS_PORTAL = "https://dados.ons.org.br"
_ONS_DATASET_SLUGS = {
    "grid_capacity":   ["capacidade instalada geracao", "capacidade instalada"],
    "renewable_share": ["geracao usinas fontes", "geracao energia"],
    "reserve_margin":  ["margem de reserva", "adequabilidade energetica", "balanco energetico reserva", "margem reserva"],
}

def _ons_get_latest_csv(search_terms):
    """Search ONS CKAN portal for a dataset and return the latest CSV URL. Tries each search term in order."""
    if isinstance(search_terms, str):
        search_terms = [search_terms]
    for term in search_terms:
        try:
            r = _requests.get(
                f"{_ONS_PORTAL}/api/3/action/package_search",
                params={"q": term, "rows": 5},
                headers=HEADERS, timeout=30
            )
            r.raise_for_status()
            results = r.json()["result"]["results"]
            for pkg in results:
                csvs = [res for res in pkg.get("resources", []) if res.get("format", "").upper() == "CSV"]
                if csvs:
                    latest = sorted(csvs, key=lambda x: x.get("last_modified", ""), reverse=True)[0]
                    return latest["url"]
        except Exception:
            continue
    raise ValueError(f"ONS: no CSV resources found for any search term: {search_terms}")

def collect_ons_s3(country_iso, metric_key, confidence=0.75, **_):
    if country_iso != "BR":
        raise ValueError(f"ONS only applies to Brazil, got {country_iso}")

    slug = _ONS_DATASET_SLUGS.get(metric_key)
    if not slug:
        raise ValueError(f"ONS: no dataset configured for metric_key={metric_key}")

    url = _ons_get_latest_csv(slug)
    r = _requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.content.decode("latin-1")), sep=";", decimal=",", thousands=".")
    df.columns = [c.strip().upper() for c in df.columns]

    if df is None:
        raise ValueError(f"ONS: empty dataframe for {metric_key}")

    data_date = date(date.today().year, 1, 1)

    if metric_key == "grid_capacity":
        mw_col = next((c for c in df.columns if "POTENCIA" in c or "CAPAC" in c), None)
        if mw_col is None:
            raise ValueError(f"ONS: no capacity column in {list(df.columns)[:8]}")
        # Filter to the most recent period to avoid summing across time
        date_col = next((c for c in df.columns if "DAT" in c or "ANO" in c or "MES" in c or "PERIOD" in c), None)
        if date_col:
            df = df[df[date_col] == df[date_col].max()]
        df[mw_col] = pd.to_numeric(df[mw_col].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        total_mw = df[mw_col].sum()
        # ONS reports in MW; divide by 1000 to get GW
        total_gw = round(total_mw / 1_000, 3)
        return make_result("BR", metric_key, total_gw, "GW",
                           data_date, "monthly", "ONS Open Data (S3)",
                           url, "file_download", confidence, raw_value=total_mw)

    if metric_key == "renewable_share":
        gen_col  = next((c for c in df.columns if "GERACAO" in c or "GWH" in c), None)
        fuel_col = next((c for c in df.columns if "FONTE" in c or "TIPO" in c), None)
        if gen_col is None:
            raise ValueError(f"ONS: no generation column in {list(df.columns)[:8]}")
        if fuel_col is None:
            raise ValueError(f"ONS: no fuel/source column found in {list(df.columns)[:8]}")
        # Filter to latest period to avoid summing across time
        date_col = next((c for c in df.columns if "DAT" in c or "ANO" in c or "MES" in c or "PERIOD" in c), None)
        if date_col:
            df = df[df[date_col] == df[date_col].max()]
        renew_kws = {"solar", "eolica", "eólica", "hidro", "biomassa"}
        df[gen_col] = pd.to_numeric(
            df[gen_col].astype(str).str.replace(",", "."), errors="coerce"
        ).fillna(0)
        total = df[gen_col].sum()
        renew = df[df[fuel_col].str.lower().apply(
            lambda x: any(k in x for k in renew_kws))][gen_col].sum()
        if total <= 0:
            raise ValueError("ONS: total generation is zero — no usable data")
        value = round(renew / total * 100, 2)
        return make_result("BR", metric_key, value, "%",
                           data_date, "monthly", "ONS Open Data (S3)",
                           url, "file_download", confidence)

    if metric_key == "reserve_margin":
        pct_col = next((c for c in df.columns if "MARG" in c or "%" in c), None)
        if pct_col is None:
            raise ValueError(f"ONS: no reserve margin column in {list(df.columns)[:8]}")
        val = pd.to_numeric(
            df[pct_col].astype(str).str.replace(",", "."), errors="coerce"
        ).dropna().iloc[-1]
        return make_result("BR", metric_key, float(val), "%",
                           data_date, "monthly", "ONS Open Data (S3)",
                           url, "file_download", confidence, raw_value=val)

    raise ValueError(f"ONS: unhandled metric_key={metric_key}")

# Smoke test
try:
    result = collect_ons_s3("BR", "grid_capacity")
    print(f"collect_ons_s3 OK — {result['metric_value']:.1f} {result['unit']}")
except Exception as e:
    print(f"collect_ons_s3: {e}")

import requests as _requests
import io
from datetime import date

CEA_BASE = "https://cea.nic.in"

CEA_ENDPOINTS = {
    "renewable_share":  "/api/installed_capacity",
    "grid_capacity":    "/api/installed_capacity",
    "reserve_margin":   "/api/energy_statistics",
}

def collect_cea(country_iso, metric_key, data_type=None, confidence=0.88, **_):
    if country_iso != "IN":
        raise ValueError(f"CEA only applies to India, got {country_iso}")

    endpoint = CEA_ENDPOINTS.get(metric_key)
    if not endpoint:
        raise ValueError(f"CEA: no endpoint for metric_key={metric_key}")

    url = f"{CEA_BASE}{endpoint}"
    r   = _requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    try:
        data = r.json()
    except Exception:
        import pandas as pd
        df   = pd.read_csv(io.StringIO(r.text))
        data = df.to_dict(orient="records")

    if not data:
        raise ValueError(f"CEA: empty response for {endpoint}")

    rec = data[-1] if isinstance(data, list) else data
    data_date = date.today().replace(day=1)

    if metric_key == "grid_capacity":
        raw   = float(rec.get("total_capacity_mw", rec.get("total", 0)))
        value = raw / 1000
        return make_result("IN", metric_key, value, "GW",
                           data_date, "monthly",
                           "Central Electricity Authority India",
                           url, "api", confidence, raw_value=raw)

    if metric_key == "renewable_share":
        total = float(rec.get("total_capacity_mw", 1))
        renew = float(rec.get("renewable_mw", rec.get("re_capacity_mw", 0)))
        if not total:
            raise ValueError("CEA: total capacity is zero — no usable data")
        value = round(renew / total * 100, 2)
        return make_result("IN", metric_key, value, "%",
                           data_date, "monthly",
                           "Central Electricity Authority India",
                           url, "api", confidence)

    if metric_key == "reserve_margin":
        value = float(rec.get("reserve_margin_pct", rec.get("margin")) or 0)
        if value == 0:
            raise ValueError("CEA: reserve_margin field missing or zero")
        return make_result("IN", metric_key, value, "%",
                           data_date, "annual",
                           "Central Electricity Authority India",
                           url, "api", confidence)

    raise ValueError(f"CEA: unhandled metric_key={metric_key}")

# Smoke test
try:
    result = collect_cea("IN", "grid_capacity")
    print(f"collect_cea OK — {result['metric_value']:.1f} {result['unit']}")
except Exception as e:
    print(f"collect_cea: {e}  (expected if API schema differs)")

import requests as _requests
from datetime import date

NPP_BASE = "https://npp.gov.in"

NPP_ENDPOINTS = {
    "renewable_share":   "/api/v1/renewable_capacity",
    "grid_capacity":     "/api/v1/total_capacity",
    "reserve_margin":    "/api/v1/grid_parameters",
    "electricity_price": "/api/v1/tariff",
}

def collect_npp_india(country_iso, metric_key, confidence=0.88, **_):
    if country_iso != "IN":
        raise ValueError(f"NPP only applies to India, got {country_iso}")

    endpoint = NPP_ENDPOINTS.get(metric_key)
    if not endpoint:
        raise ValueError(f"NPP: no endpoint for metric_key={metric_key}")

    url = NPP_BASE + endpoint
    r   = _requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    try:
        data = r.json()
    except Exception:
        raise ValueError(f"NPP: non-JSON response for {endpoint}")

    if not data:
        raise ValueError("NPP: empty response")

    rec       = data[0] if isinstance(data, list) else data
    data_date = date.today().replace(day=1)

    if metric_key == "grid_capacity":
        raw   = float(rec.get("total_capacity_mw", rec.get("capacity", 0)))
        value = raw / 1000
        return make_result("IN", metric_key, value, "GW",
                           data_date, "monthly",
                           "NPP — National Power Portal",
                           url, "api", confidence, raw_value=raw)

    if metric_key == "renewable_share":
        total = float(rec.get("total_mw", 1))
        renew = float(rec.get("renewable_mw", 0))
        if not total:
            raise ValueError("NPP: total capacity is zero — no usable data")
        value = round(renew / total * 100, 2)
        return make_result("IN", metric_key, value, "%",
                           data_date, "monthly",
                           "NPP — National Power Portal",
                           url, "api", confidence)

    raise ValueError(f"NPP: unhandled metric_key={metric_key}")

# Smoke test
try:
    result = collect_npp_india("IN", "grid_capacity")
    print(f"collect_npp_india OK — {result['metric_value']:.1f} {result['unit']}")
except Exception as e:
    print(f"collect_npp_india: {e}  (expected if API schema differs)")

import requests as _requests, io
from datetime import date
import pandas as pd

def _lbnl_find_url():
    """Find the current LBNL Queued Up Excel. Tries known direct URLs then falls back to scraping."""
    import datetime as _dt
    from bs4 import BeautifulSoup
    browser_headers = {**HEADERS, "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

    current_year = _dt.date.today().year
    # Try direct CDN URL patterns for current and prior year editions
    direct_patterns = []
    for yr in [current_year, current_year - 1]:
        for suffix in ["", "_v1", "_v2", f"_{yr}_data_file", f"_{yr}"]:
            direct_patterns.append(
                f"https://emp.lbl.gov/sites/default/files/queued_up_{yr}_data_file{suffix}.xlsx"
            )
    for url in direct_patterns:
        try:
            r = _requests.head(url, headers=browser_headers, timeout=15, allow_redirects=True)
            if r.status_code == 200:
                return url
        except Exception:
            continue

    # Fall back to scraping the publications/hub pages
    for page_url in [
        f"https://emp.lbl.gov/publications/queued-{current_year}-edition-characteristics",
        f"https://emp.lbl.gov/publications/queued-{current_year - 1}-edition-characteristics",
        "https://emp.lbl.gov/queues",
    ]:
        try:
            r = _requests.get(page_url, headers=browser_headers, timeout=30)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "queued_up" in href.lower() and href.endswith(".xlsx"):
                    return href if href.startswith("http") else f"https://emp.lbl.gov{href}"
        except Exception:
            continue
    raise ValueError("LBNL: could not find .xlsx link — check emp.lbl.gov/queues manually")

def collect_lbnl_excel(country_iso, metric_key, confidence=0.75, **_):
    if country_iso != "US":
        raise ValueError("LBNL Queued Up only applies to US")

    lbnl_url = _lbnl_find_url()
    r = _requests.get(lbnl_url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content))
    df.columns = [str(c).lower().strip() for c in df.columns]

    mw_col = next((c for c in df.columns
                   if "capac" in c or " mw" in c or c == "mw"), None)
    if mw_col is None:
        raise ValueError(f"LBNL: no MW column. Columns: {list(df.columns)[:10]}")

    status_col = next((c for c in df.columns if "status" in c or "queue" in c), None)
    if status_col:
        active = df[df[status_col].astype(str).str.contains(
            "active|queue|pending|ia pending", case=False, na=False)]
    else:
        active = df

    total_mw = pd.to_numeric(active[mw_col], errors="coerce").sum()
    if pd.isna(total_mw) or total_mw == 0:
        raise ValueError("LBNL: summed MW is 0 or NaN")

    return make_result("US", metric_key, float(total_mw), "MW",
                       date.today().replace(day=1), "annual",
                       "LBNL Queued Up Report", lbnl_url,
                       "file_download", confidence, raw_value=total_mw)

# Smoke test
try:
    result = collect_lbnl_excel("US", "interconnection_queue_depth")
    print(f"collect_lbnl_excel OK — {result['metric_value']:,.0f} {result['unit']}")
except Exception as e:
    print(f"collect_lbnl_excel: {e}  (update LBNL_URL if file moved)")

import re
from datetime import date
from bs4 import BeautifulSoup

GPP_COUNTRY_URLS = {
    "US": "https://www.globalpetrolprices.com/USA/electricity_prices/",
    "AE": "https://www.globalpetrolprices.com/United-Arab-Emirates/electricity_prices/",
    "BR": "https://www.globalpetrolprices.com/Brazil/electricity_prices/",
    "IN": "https://www.globalpetrolprices.com/India/electricity_prices/",
    "SG": "https://www.globalpetrolprices.com/Singapore/electricity_prices/",
    "PH": "https://www.globalpetrolprices.com/Philippines/electricity_prices/",
}

def collect_globalpetrolprices(country_iso, metric_key, confidence=CONFIDENCE["web_scrape"], **_):
    """Scrape GlobalPetrolPrices.com for USD/kWh industrial electricity price."""
    url  = GPP_COUNTRY_URLS.get(country_iso)
    if not url:
        raise ValueError(f"GPP: no URL configured for {country_iso}")

    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()

    # USD/kWh values are in the range 0.03–0.60
    matches   = re.findall(r"\b(0\.\d{3,4})\b", text)
    plausible = [float(m) for m in matches if 0.03 <= float(m) <= 0.60]

    if plausible:
        value = plausible[0]
        return make_result(country_iso, metric_key, value, "USD/kWh",
                           date.today().replace(day=1), "irregular",
                           "GlobalPetrolPrices.com", url,
                           "web_scrape", confidence, raw_value=value)

    # Fallback: Gemini extraction from page text
    result = gemini_extract(text, metric_key, country_iso, url)
    if not (0.03 <= result["value"] <= 0.60):
        raise ValueError(
            f"GPP Gemini: value {result['value']} outside plausible range for electricity price"
        )
    return make_result(country_iso, metric_key, result["value"], "USD/kWh",
                       result["data_date"], result["frequency"],
                       "GlobalPetrolPrices.com (Gemini)", url,
                       "web_scrape", CONFIDENCE["gemini"],
                       raw_value=result["raw_text"])

# Smoke test
try:
    result = collect_globalpetrolprices("US", "electricity_price")
    print(f"collect_globalpetrolprices OK — {result['metric_value']:.4f} {result['unit']}")
except Exception as e:
    print(f"collect_globalpetrolprices: {e}")

import pdfplumber, io, re
from datetime import date

METRIC_PATTERNS = {
    "reserve_margin_pct": [
        (r"reserve\s+margin[^\d]*(\d{1,3}(?:\.\d{1,2})?)\s*%",     1, 1.0),
        (r"(\d{1,3}(?:\.\d{1,2})?)\s*%\s*reserve\s+margin",         1, 1.0),
        # Portuguese: "Margem de Reserva Eficiente ... 15,3%" (comma as decimal separator)
        (r"margem\s+de\s+reserva[^\d]*(\d{1,3}(?:[,\.]\d{1,2})?)\s*%", 1, 1.0),
    ],
    "installed_capacity_gw": [
        (r"total\s+installed\s+capacity[^\d]*(\d+(?:,\d{3})*(?:\.\d+)?)\s*GW", 1, 1.0),
        (r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*GW\s*(?:total|installed)",              1, 1.0),
        (r"total\s+installed\s+capacity[^\d]*(\d+(?:,\d{3})*)\s*MW",            1, 0.001),
    ],
    "energy_investment_usd_bn": [
        (r"(?:USD|US\$|\$)\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|bn)",  1, 1.0),
        (r"(?:Rs\.?|INR)\s*(\d+(?:,\d{3})*)\s*crore",                        1, 0.0012),
    ],
    "capacity_under_implementation_mw": [
        (r"(?:pipeline|under\s+implementation)[^\d]*(\d+(?:,\d{3})*)\s*MW", 1, 1.0),
    ],
    "industrial_electricity_tariff": [
        (r"(?:HT|LT)\s+industrial[^\d]*(\d+\.\d{2,3})\s*(?:rs|rupees|inr)\/kwh", 1, 1.0),
        (r"average\s+(?:cost|tariff)[^\d]*(\d{3,4}(?:\.\d+)?)\s*paise",           1, 0.01),
    ],
}

def _find_year_in_text(text: str) -> date:
    years = re.findall(r"\b(20[12]\d)\b", text)
    if years:
        return date(max(int(y) for y in years), 1, 1)
    return date.today().replace(day=1)

def collect_pdf_gemini(country_iso, metric_key, pdf_url, metric_slug,
                        confidence=CONFIDENCE["pdf_regex"], use_gemini=True, **_):
    """Download PDF, try pdfplumber regex, then Gemini extraction."""
    pdf_bytes = download_pdf(pdf_url)

    # Step 1: pdfplumber regex
    patterns  = METRIC_PATTERNS.get(metric_slug, [])
    full_text = ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:30]:
                for table in (page.extract_tables() or []):
                    full_text += " | ".join(str(c or "") for row in table for c in row) + "\n"
                full_text += (page.extract_text() or "") + "\n"
    except Exception as exc:
        full_text = ""

    for pattern, group, scale in patterns:
        m = re.search(pattern, full_text, re.IGNORECASE | re.MULTILINE)
        if m:
            raw = m.group(0)
            val = float(m.group(group).replace(",", "")) * scale
            return make_result(country_iso, metric_key, val,
                               METRICS[metric_key]["unit"],
                               _find_year_in_text(full_text), "annual",
                               f"PDF — {pdf_url.split('/')[-1][:60]}", pdf_url,
                               "pdf_extract", confidence, raw_value=raw)

    # Step 2: Smart Claude extraction on scored chunks
    if use_gemini:
        # Score PDF text chunks by keyword relevance before sending to Claude
        metric_label = METRICS[metric_key]["label"]
        keywords = set(metric_label.lower().split() + metric_key.lower().split("_"))
        lines = [l.strip() for l in full_text.split("\n") if l.strip()]
        scored = sorted(
            [(sum(1 for kw in keywords if kw in l.lower()) + bool(re.search(r"\d+\.?\d*", l)), l)
             for l in lines],
            reverse=True
        )
        relevant = "\n".join(l for _, l in scored[:80])[:3500]
        result = _smart_extract(relevant or full_text[:3500], metric_key, country_iso, pdf_url)
        return make_result(country_iso, metric_key, result["value"],
                           METRICS[metric_key]["unit"],
                           result["data_date"], result["frequency"],
                           f"PDF (Claude) — {pdf_url.split('/')[-1][:60]}", pdf_url,
                           "pdf_extract", CONFIDENCE["gemini"],
                           raw_value=result["raw_text"])

    raise ValueError(f"collect_pdf_gemini: all extraction methods failed for {pdf_url}")

# Smoke test
try:
    result = collect_pdf_gemini(
        "US", "reserve_margin",
        "https://www.nerc.com/pa/RAPA/ra/Reliability%20Assessments%20DL/NERC_SRA_2024.pdf",
        "reserve_margin_pct",
        confidence=CONFIDENCE["pdf_regex"],
    )
    print(f"collect_pdf_gemini OK — {result['metric_value']:.1f} {result['unit']}")
except Exception as e:
    print(f"collect_pdf_gemini: {e}")

from bs4 import BeautifulSoup
import trafilatura

BRAVE_API_KEY = os.environ.get("BRAVE_API_KEY", "")
JINA_API_KEY   = os.environ.get("JINA_API_KEY", "")
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")

# ── Trusted source domains per country for targeted search ───────────────────
# Tiered: government/regulator first, then utilities, then specialised think-tanks/
# consultancies that publish regulator-grade analysis (ICSC, PIDS, ENGIE R&D, etc.)
_TRUSTED_SOURCES = {
    "US": ["eia.gov", "iea.org", "energy.gov", "ferc.gov", "lbl.gov", "nerc.com"],
    "AE": ["iea.org", "moei.gov.ae", "dewa.gov.ae", "doe.gov.ae", "u.ae",
           "irena.org", "feeds.dfm.ae", "gccia.com.sa", "ewec.ae", "taqa.com"],
    "BR": ["epe.gov.br", "iea.org", "aneel.gov.br", "ons.org.br", "mme.gov.br",
           "gov.br", "engie.com.br", "absolar.org.br"],
    "IN": ["iea.org", "cea.nic.in", "mnre.gov.in", "powermin.gov.in",
           "niti.gov.in", "beeindia.gov.in"],
    "SG": ["ema.gov.sg", "mti.gov.sg", "iea.org", "greenplan.gov.sg",
           "edb.gov.sg", "data.gov.sg"],
    "PH": ["doe.gov.ph", "iea.org", "transco.com.ph", "ngcp.ph", "erc.gov.ph",
           "icsc.ngo", "pids.gov.ph", "meralco.com.ph"],
}

# ── Search query templates per metric ────────────────────────────────────────
# Base templates are generic across all countries; per-country overrides below
# add domain-specific vocabulary (e.g. "Margem de Reserva" for BR, "Power
# Requirements Met" for PH) that significantly improves retrieval precision.
_QUERY_TEMPLATES = {
    "electricity_price":           "{country} industrial electricity tariff price USD kWh {year}",
    "renewable_share":             "{country} renewable energy percentage share grid generation {year}",
    "grid_capacity":               "{country} total installed electricity generation capacity GW MW {year}",
    "reserve_margin":              "{country} electricity grid reserve margin adequacy percentage {year}",
    "energy_investment":           "{country} energy infrastructure investment plan billion USD next 5 years {year}",
    "interconnection_queue_depth": "{country} grid interconnection queue MW projects awaiting connection {year}",
}

# Country+metric-specific query enrichments: keyword phrases that local authorities
# actually use. These are appended to the base template to bias retrieval toward
# the most authoritative documents (regulator reports, utility integrated reports).
_QUERY_ENRICHMENTS = {
    ("AE", "reserve_margin"):              "DEWA integrated report installed capacity peak demand OR Abu Dhabi DoE annual technical report",
    ("AE", "energy_investment"):           "DEWA capital expenditure AED billion OR UAE Energy Strategy 2050 renewable investment",
    ("AE", "interconnection_queue_depth"): "Mohammed bin Rashid Al Maktoum Solar Park phase under construction OR GCCIA interconnection expansion",
    ("BR", "reserve_margin"):              "margem de reserva eficiente ONS EPE OR capacity reserve auction",
    ("BR", "interconnection_queue_depth"): "fila de acesso ANEEL transmissão OR Resolução 1069/2023 outorgas",
    ("PH", "reserve_margin"):              "Luzon operating margin ICSC power outlook OR available capacity over peak demand DOE",
    ("PH", "interconnection_queue_depth"): "DOE RE50 high demand scenario MW pipeline OR competitive renewable energy zones",
    ("SG", "interconnection_queue_depth"): "EMA conditional approval low-carbon electricity import GW OR LTMS-PIP import",
    ("SG", "reserve_margin"):              "EMA Required Reserve Margin RRM centralised process generation capacity",
}


def brave_search(query: str, count: int = 5) -> list:
    """Query Brave Search API and return list of {url, title, description}."""
    if not BRAVE_API_KEY or BRAVE_API_KEY == "your_brave_api_key_here":
        raise ValueError("BRAVE_API_KEY not set in .env")
    r = _requests.get(
        "https://api.search.brave.com/res/v1/web/search",
        headers={
            "X-Subscription-Token": BRAVE_API_KEY,
            "Accept":               "application/json",
        },
        params={"q": query, "count": count, "search_lang": "en", "safesearch": "off"},
        timeout=15,
    )
    r.raise_for_status()
    raw = r.json().get("web", {}).get("results", [])
    return [{"url": res["url"], "title": res.get("title", ""), "description": res.get("description", "")} for res in raw]


def _build_search_query(country_iso: str, metric_key: str, extra: str = "") -> str:
    """Build a targeted search query with trusted source hints and metric-specific enrichments."""
    country_name = COUNTRIES[country_iso]["name"]
    year         = date.today().year
    template     = _QUERY_TEMPLATES.get(metric_key, "{country} {metric} {year}")
    base_query   = template.format(country=country_name, metric=metric_key.replace("_", " "), year=year)
    enrichment   = _QUERY_ENRICHMENTS.get((country_iso, metric_key), "")
    site_filter  = " OR ".join(f"site:{s}" for s in _TRUSTED_SOURCES.get(country_iso, []))
    parts        = [base_query]
    if enrichment:
        parts.append(enrichment)
    parts.append(f"({site_filter})")
    if extra:
        parts.append(extra)
    return " ".join(parts)


def _fetch_clean_text(url: str) -> str:
    """
    Fetch a URL and return clean article text using Trafilatura.
    Falls back to BeautifulSoup keyword-scored extraction if Trafilatura returns nothing.
    """
    try:
        # Try Trafilatura's own downloader first (handles most sites cleanly)
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(
                downloaded,
                include_tables=True,
                include_links=False,
                deduplicate=True,
                favor_recall=True,
            )
            if text and len(text.strip()) > 150:
                return text.strip()
    except Exception:
        pass

    # Fallback: fetch with our Playwright-aware fetcher, then Trafilatura parse
    try:
        html = fetch_html(url)
        text = trafilatura.extract(html, include_tables=True, favor_recall=True)
        if text and len(text.strip()) > 150:
            return text.strip()
    except Exception:
        pass

    # Last resort: BeautifulSoup plain text
    try:
        html = fetch_html(url)
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)
    except Exception:
        return ""


_AGENT_TOOLS = [
    {
        "name": "search_web",
        "description": (
            "Search the web using Brave Search. Use targeted queries with site: filters "
            "for trusted sources. Returns a list of results with URL, title, and snippet."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query. Include site: filters for trusted domains."}
            },
            "required": ["query"],
        },
    },
    {
        "name": "fetch_page",
        "description": (
            "Fetch the full text content of a URL using Trafilatura (clean article extraction). "
            "Use this when a search snippet contains the right source but not enough data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "Full URL to fetch and read."}
            },
            "required": ["url"],
        },
    },
    {
        "name": "return_result",
        "description": (
            "Submit your final answer. Call this ONLY when you are confident you have found "
            "the correct, most recent value. Always convert to the requested output unit."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "value":           {"type": "number",  "description": "Numeric value in the output unit."},
                "data_date":       {"type": "string",  "description": "Date of the data in YYYY-MM-DD format."},
                "frequency":       {"type": "string",  "description": "monthly | quarterly | annual | irregular"},
                "source_url":      {"type": "string",  "description": "URL where the value was found."},
                "raw_text":        {"type": "string",  "description": "Exact text snippet containing the number."},
                "conversion_note": {"type": "string",  "description": "Conversion applied, e.g. '500 SGD/MWh ÷ 1000 × 0.74 = 0.37 USD/kWh'. Empty if none."},
            },
            "required": ["value", "data_date", "source_url", "raw_text"],
        },
    },
]


def collect_research(country_iso, metric_key, confidence=CONFIDENCE["web_scrape"],
                     extra_query="", trusted_urls=None, **_):
    """
    Deep research collector — delegates to research_agent.py which implements
    the Generate → Search → Reflect → Loop → Synthesize pattern.
    """
    result = _run_deep_research(
        country_iso  = country_iso,
        metric_key   = metric_key,
        country_name = COUNTRIES[country_iso]["name"],
        currency     = COUNTRIES[country_iso]["currency"],
        metric_label = METRICS[metric_key]["label"],
        metric_unit  = METRICS[metric_key]["unit"],
        fx_rates     = _get_fx_rates(),
        trusted_urls = trusted_urls,
    )

    val = result.get("value")
    if val is None:
        raise ValueError(f"Research agent returned null value for {metric_key} in {country_iso}")
    val = float(val)
    data_date = _infer_date_from_source(result.get("source_url", ""), result.get("data_date"))

    return make_result(
        country_iso, metric_key, val,
        METRICS[metric_key]["unit"],
        data_date,
        result.get("frequency", "irregular"),
        f"Deep research agent — {country_iso}/{metric_key}",
        result.get("source_url", ""),
        "web_scrape", confidence,
        raw_value=result.get("raw_text", ""),
        currency_conversion=result.get("conversion_note", ""),
    )


def _extract_relevant_text(html: str, metric_key: str) -> str:
    """
    Smart extraction: pull tables + scored text chunks most relevant to the metric.
    Returns up to 3500 chars of high-signal content.
    """
    metric_label = METRICS[metric_key]["label"]
    keywords = set(metric_label.lower().split() + metric_key.lower().split("_"))
    # Add domain-specific keywords per metric
    extra = {
        "electricity_price":           {"tariff", "rate", "kwh", "cents", "price", "cost"},
        "renewable_share":             {"renewable", "solar", "wind", "hydro", "clean", "green", "%"},
        "grid_capacity":               {"capacity", "installed", "gw", "mw", "generation"},
        "reserve_margin":              {"reserve", "margin", "surplus", "adequacy", "%"},
        "energy_investment":           {"investment", "billion", "capex", "spending", "usd", "plan"},
        "interconnection_queue_depth": {"queue", "interconnection", "mw", "pending", "backlog"},
    }
    keywords |= extra.get(metric_key, set())

    soup = BeautifulSoup(html, "html.parser")
    # Remove boilerplate tags
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "menu"]):
        tag.decompose()

    chunks = []

    # 1. Extract tables — convert to compact text
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if cells:
                rows.append(" | ".join(cells))
        table_text = "\n".join(rows)
        if table_text.strip():
            score = sum(1 for kw in keywords if kw in table_text.lower())
            chunks.append((score + 2, table_text))  # tables get a +2 bonus

    # 2. Score paragraphs and headings
    import re as _re
    for tag in soup.find_all(["p", "li", "h1", "h2", "h3", "h4", "td", "span"]):
        t = tag.get_text(strip=True)
        if len(t) < 20:
            continue
        score = sum(1 for kw in keywords if kw in t.lower())
        has_number = bool(_re.search(r"\d+\.?\d*", t))
        if score > 0 and has_number:
            chunks.append((score, t))

    # Sort by score descending, deduplicate, take top content up to 3500 chars
    seen, selected = set(), []
    for _, text in sorted(chunks, key=lambda x: -x[0]):
        if text not in seen:
            seen.add(text)
            selected.append(text)

    combined = "\n\n".join(selected)
    if not combined.strip():
        # Fallback: plain text of whole page
        combined = soup.get_text(separator="\n", strip=True)

    return combined[:3500]


_FX_CACHE: dict = {}

def _get_fx_rates() -> dict:
    """Return cached live exchange rates for all pipeline currencies → USD."""
    global _FX_CACHE
    if _FX_CACHE:
        return _FX_CACHE
    for currency in ["SGD", "BRL", "INR", "PHP", "AED"]:
        try:
            _FX_CACHE[currency] = fetch_fx_rate(currency)
        except Exception:
            pass
    _FX_CACHE["USD"] = 1.0
    return _FX_CACHE


def _smart_extract(text: str, metric_key: str, country_iso: str, source_url: str) -> dict:
    """
    Claude extraction with full conversion context.
    Passes live FX rates + unit hints so Claude can convert any currency/unit automatically.
    """
    metric_label = METRICS[metric_key]["label"]
    metric_unit  = METRICS[metric_key]["unit"]
    country_name = COUNTRIES[country_iso]["name"]
    currency     = COUNTRIES[country_iso]["currency"]
    fx_rates     = _get_fx_rates()

    unit_hints = {
        "electricity_price":           f"typically 0.05–0.50 USD/kWh; may appear as {currency}/MWh, {currency} cents/kWh, or local tariff",
        "renewable_share":             "a percentage 0–100%; may be labeled as % of total generation, capacity mix, or clean energy share",
        "grid_capacity":               f"total installed generation capacity in GW; may appear as MW — divide by 1000; typically 1–2000 GW",
        "reserve_margin":              "a percentage, typically 10–40%; may be labeled as reserve margin, capacity surplus, or adequacy ratio",
        "energy_investment":           f"planned capital investment in USD billions over 5 years; may appear in {currency} — convert using FX rate",
        "interconnection_queue_depth": "MW or GW of projects awaiting grid connection; may be labeled as connection queue, pipeline, or backlog",
    }

    fx_context = ", ".join(f"1 {k} = {v:.4f} USD" for k, v in fx_rates.items())

    prompt = (
        f"You are an expert energy data analyst extracting statistics from web content.\n\n"
        f"Country: {country_name} ({country_iso})\n"
        f"Local currency: {currency}\n"
        f"Live FX rates: {fx_context}\n"
        f"Metric: {metric_label}\n"
        f"Output unit: {metric_unit}\n"
        f"Guidance: {unit_hints.get(metric_key, '')}\n\n"
        f"Source: {source_url}\n\n"
        f"Instructions:\n"
        f"1. Scan ALL content — tables, paragraphs, statistics boxes, footnotes, article text\n"
        f"2. Accept data from any credible source: government reports, IEA, World Bank, reputable news\n"
        f"3. Pick the MOST RECENT value available\n"
        f"4. If value is in local currency, convert to USD using the FX rates above\n"
        f"5. If value is in MW and unit is GW, divide by 1000\n"
        f"6. If value is in MWh/kWh tariff, convert to USD/kWh\n"
        f"7. Only set value to null if the metric is genuinely absent from the content\n\n"
        f"Respond ONLY with this JSON (no markdown, no prose):\n"
        '{{"value": <float or null>, "raw_text": "<exact snippet>", '
        '"data_date": "<YYYY-MM-DD or null>", "frequency": "<monthly|quarterly|annual|irregular>", '
        '"conversion_note": "<what conversion was applied, or none>"}}\n\n'
        f"Content:\n---\n{text}\n---"
    )

    r = _requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json={
            "model":      CLAUDE_MODEL,
            "max_tokens": 600,
            "messages":   [{"role": "user", "content": prompt}],
        },
        timeout=45,
    )
    r.raise_for_status()
    resp = r.json()
    _track_tokens(resp)
    raw_out = resp["content"][0]["text"].strip()
    raw_out = re.sub(r"^```(?:json)?\s*", "", raw_out, flags=re.MULTILINE)
    raw_out = re.sub(r"^```\s*$",         "", raw_out, flags=re.MULTILINE).strip()

    parsed = json.loads(raw_out)
    val = parsed.get("value")
    if val is None:
        raise ValueError(f"Claude returned null for {metric_key} at {source_url}")

    dstr = parsed.get("data_date")
    try:
        data_date = datetime.fromisoformat(dstr).date() if dstr else date.today().replace(day=1)
    except Exception:
        data_date = date.today().replace(day=1)

    return {
        "value":           float(val),
        "raw_text":        parsed.get("raw_text", ""),
        "data_date":       data_date,
        "frequency":       parsed.get("frequency", "irregular"),
        "conversion_note": parsed.get("conversion_note", ""),
    }


def collect_scrape_gemini(country_iso, metric_key, url, confidence=CONFIDENCE["gemini"], **_):
    """Smart scrape: extract relevant text, score by keyword, send best chunks to Claude."""
    html = fetch_html(url)
    text = _extract_relevant_text(html, metric_key)
    result = _smart_extract(text, metric_key, country_iso, url)
    return make_result(country_iso, metric_key, result["value"],
                       METRICS[metric_key]["unit"],
                       result["data_date"], result["frequency"],
                       f"Claude scrape — {url.split('/')[2]}", url,
                       "web_scrape", confidence,
                       raw_value=result["raw_text"],
                       currency_conversion=result.get("conversion_note", ""))


def collect_multi_source(country_iso, metric_key, urls, confidence=CONFIDENCE["web_scrape"], **_):
    """
    Try multiple URLs in order, return the first successful extraction.
    Ideal for metrics where data appears in various reports/articles.
    """
    last_exc = None
    for url in urls:
        try:
            html = fetch_html(url)
            text = _extract_relevant_text(html, metric_key)
            result = _smart_extract(text, metric_key, country_iso, url)
            return make_result(country_iso, metric_key, result["value"],
                               METRICS[metric_key]["unit"],
                               result["data_date"], result["frequency"],
                               f"Claude scrape — {url.split('/')[2]}", url,
                               "web_scrape", confidence,
                               raw_value=result["raw_text"],
                               currency_conversion=result.get("conversion_note", ""))
        except Exception as exc:
            last_exc = exc
            continue
    raise ValueError(f"collect_multi_source: all {len(urls)} URLs failed. Last: {last_exc}")

print("collect_research defined.")

import time

def log_attempt(conn, run_id, country_iso, metric_key, collector_name,
                step, status, source_url, error_type, error_msg, duration_ms):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si1_collection_log
                (run_id, country_iso, metric_key, collector_name, cascade_step,
                 status, source_url, error_type, error_message, duration_ms)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, country_iso, metric_key, collector_name, step,
              status, source_url, error_type, error_msg, duration_ms))
    conn.commit()

def store_datapoint(conn, dp: dict, run_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si1_raw_metrics (
                country_iso, country_name, metric_key, metric_label,
                metric_value, unit, data_date, data_frequency,
                source_name, source_url, access_method,
                confidence_score, raw_value, currency_conversion,
                is_imputed, run_id
            ) VALUES (
                %(country_iso)s, %(country_name)s, %(metric_key)s, %(metric_label)s,
                %(metric_value)s, %(unit)s, %(data_date)s, %(data_frequency)s,
                %(source_name)s, %(source_url)s, %(access_method)s,
                %(confidence_score)s, %(raw_value)s, %(currency_conversion)s,
                %(is_imputed)s, %(run_id)s
            )
            ON CONFLICT (country_iso, metric_key, data_date, source_name)
            DO UPDATE SET
                metric_value        = EXCLUDED.metric_value,
                confidence_score    = EXCLUDED.confidence_score,
                raw_value           = EXCLUDED.raw_value,
                currency_conversion = EXCLUDED.currency_conversion,
                run_id              = EXCLUDED.run_id,
                collected_at        = NOW()
        """, {**dp, "run_id": run_id})
    conn.commit()

def open_gap(conn, run_id, country_iso, metric_key, failure_reason, collectors_tried, severity):
    country_name = COUNTRIES[country_iso]["name"]
    metric_label = METRICS[metric_key]["label"]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO si1_data_gaps
                (country_iso, country_name, metric_key, metric_label,
                 failure_reason, collectors_tried, severity)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (country_iso, metric_key) DO UPDATE SET
                failure_reason   = EXCLUDED.failure_reason,
                collectors_tried = EXCLUDED.collectors_tried,
                last_attempted   = NOW(),
                attempt_count    = si1_data_gaps.attempt_count + 1,
                status           = 'open'
        """, (country_iso, country_name, metric_key, metric_label,
              failure_reason, collectors_tried, severity))
    conn.commit()

print("DB helpers defined.")

# ── Singapore-specific collectors ────────────────────────────────────────────

def _ema_find_excel_playwright(page_url: str) -> str:
    """
    Use Playwright to fully render the EMA page (JS-heavy) and find the Excel download link.
    Falls back to checking for direct data table on the page.
    """
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({"User-Agent": HEADERS["User-Agent"]})
        page.goto(page_url, wait_until="networkidle", timeout=30000)
        # Wait for any dynamic content
        try:
            page.wait_for_selector("a[href$='.xlsx'], a[href$='.xls']", timeout=5000)
        except Exception:
            pass
        html = page.content()
        # Also collect all hrefs via JS evaluation
        hrefs = page.evaluate("() => Array.from(document.querySelectorAll('a')).map(a => a.href)")
        browser.close()

    # Check evaluated hrefs first (catches JS-generated links)
    for href in hrefs:
        if href.endswith((".xlsx", ".xls")):
            return href

    # Fallback: parse rendered HTML
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith((".xlsx", ".xls")):
            return href if href.startswith("http") else f"https://www.ema.gov.sg{href}"

    raise ValueError(f"EMA: no Excel link found on {page_url} (even after JS render)")


def _parse_ema_excel(content: bytes) -> tuple:
    """
    Parse an EMA statistics Excel file.
    Returns (latest_value, data_date) where value is the last non-null numeric cell
    in the last data row, and date is parsed from adjacent cells.
    """
    # Try all sheets, pick the one with the most numeric data
    xl = pd.ExcelFile(io.BytesIO(content))
    best_df = None
    best_count = 0
    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet, header=None)
            num_df = df.apply(pd.to_numeric, errors="coerce")
            count = num_df.count().sum()
            if count > best_count:
                best_count = count
                best_df = df
        except Exception:
            continue

    if best_df is None:
        raise ValueError("EMA Excel: no parseable sheet found")

    df = best_df.dropna(how="all").reset_index(drop=True)
    num_df = df.apply(pd.to_numeric, errors="coerce")

    # Last row with at least one numeric value
    last_numeric_idx = num_df.dropna(how="all").index[-1]
    last_row = num_df.iloc[last_numeric_idx]
    value = float(last_row.dropna().iloc[-1])

    # Extract date: scan the same row and nearby rows for year/month patterns
    data_date = date.today().replace(day=1)
    for row_idx in [last_numeric_idx, last_numeric_idx - 1]:
        if row_idx < 0:
            continue
        for cell in df.iloc[row_idx]:
            s = str(cell).strip()
            # Match "Jan 2026", "2026-01", "2026", "January 2026" etc.
            m = re.search(r"(\d{4})[^\d]*(\d{1,2})?|(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[\s\-]*(\d{4})", s, re.IGNORECASE)
            if m:
                try:
                    data_date = datetime.strptime(s[:10], "%Y-%m-%d").date()
                except Exception:
                    try:
                        year = int(m.group(1) or m.group(4))
                        data_date = date(year, int(m.group(2) or 1), 1)
                    except Exception:
                        pass
                break

    return value, data_date


def collect_ema_grid_capacity(country_iso, metric_key, confidence=CONFIDENCE["file_download"], **_):
    """SG total installed generating capacity (GW) from EMA statistics Excel."""
    cap_url = "https://www.ema.gov.sg/resources/statistics/installed-generating-capacity"
    cap_xl_url = _ema_find_excel_playwright(cap_url)
    r = _requests.get(cap_xl_url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    cap_mw, cap_date = _parse_ema_excel(r.content)
    cap_gw = round(cap_mw / 1000, 4)
    return make_result("SG", metric_key, cap_gw, "GW",
                       cap_date, "monthly",
                       "EMA Installed Generating Capacity", cap_url,
                       "file_download", confidence,
                       raw_value=f"{cap_mw:.0f} MW")


def collect_worldbank_energy_investment(country_iso, metric_key,
                                        confidence=CONFIDENCE["api_annual"], **_):
    """
    World Bank Private Participation in Infrastructure — Energy commitments (USD bn).
    Indicator: IE.PPI.ENGY.CD (current USD).
    Best coverage for BR, IN, PH. US/SG/AE often have no entries — falls through
    to the research agent in those cases.

    KNOWN METHODOLOGY LIMITATION: WB PPI captures *private* infrastructure
    commitments only, missing public/utility/sovereign spending. For high-income
    countries with mostly public energy investment (US, SG, AE), the dataset is
    empty and the cascade falls through to the research agent, which can return
    wildly different definitions (BloombergNEF energy-transition totals, 5-year
    plans, sovereign-fund commitments). The result: energy_investment values
    are NOT directly comparable across the 6 countries as currently sourced.
    For production methodology, replace with IEA World Energy Investment or
    similar single-source per-country data.
    """
    data = _wb_api_fetch(country_iso, "IE.PPI.ENGY.CD", mrv=5)
    obs = [o for o in data[1] if o.get("value") is not None]
    if not obs:
        raise ValueError(f"WorldBank PPI: no energy investment data for {country_iso}")
    o = max(obs, key=lambda x: int(x["date"]))
    value_bn = round(float(o["value"]) / 1e9, 4)
    data_date = date(int(o["date"]), 1, 1)
    return make_result(
        country_iso, metric_key, value_bn, "USD bn",
        data_date, "annual",
        "World Bank PPI — Energy (IE.PPI.ENGY.CD)",
        f"https://data.worldbank.org/indicator/IE.PPI.ENGY.CD?locations={country_iso}",
        "api_annual", confidence,
        raw_value=f"{float(o['value']):,.0f} USD",
    )


def collect_irena_reserve_margin_proxy(country_iso, metric_key,
                                        confidence=CONFIDENCE["api_annual"], **_):
    """
    Proxy reserve margin for countries without a direct reserve-margin API.

    Method:
      1. IRENA PxWeb  → total installed capacity (MW)
      2. World Bank   → electricity consumption kWh/capita × population
                        ÷ 8 760 000 = average demand (MW)
      3. Peak demand  ≈ average demand × 1.6  (typical peak-to-average factor)
      4. Reserve margin = (capacity - peak) / peak × 100

    Confidence is deliberately lower than a direct source (0.65 default).
    """
    cap_result = collect_irena(country_iso, "grid_capacity", confidence=0.70)
    cap_mw = cap_result["metric_value"] * 1000

    def _wb(indicator):
        d = _wb_api_fetch(country_iso, indicator, mrv=5)
        obs = [o for o in (d[1] if len(d) > 1 else []) if o.get("value")]
        if not obs:
            raise ValueError(f"WB {indicator}: no data for {country_iso}")
        return float(max(obs, key=lambda x: int(x["date"]))["value"])

    cons_kwh_pc = _wb("EG.USE.ELEC.KH.PC")
    population  = _wb("SP.POP.TOTL")

    avg_mw       = (cons_kwh_pc * population) / 8_760_000
    peak_mw      = avg_mw * 1.6
    reserve_pct  = round((cap_mw - peak_mw) / peak_mw * 100, 2)

    return make_result(
        country_iso, metric_key, reserve_pct, "%",
        cap_result["data_date"], "annual",
        "IRENA capacity + WB consumption proxy",
        "https://pxweb.irena.org",
        "api_annual", confidence,
        raw_value=f"cap={cap_mw:.0f}MW peak_est={peak_mw:.0f}MW",
        currency_conversion="peak = WB kWh/capita × pop ÷ 8_760_000 × 1.6 peak_factor",
    )


def collect_ema_usep(country_iso, metric_key, confidence=CONFIDENCE["api_monthly"], **_):
    """
    EMA Uniform Singapore Energy Price (USEP) → USD/kWh.
    Uses Playwright to render the JS page, finds and downloads the Excel file,
    extracts the most recent monthly SGD/MWh value and converts to USD/kWh.
    """
    page_url = "https://www.ema.gov.sg/resources/statistics/average-monthly-uniform-singapore-energy-price"
    xl_url = _ema_find_excel_playwright(page_url)
    r = _requests.get(xl_url, headers=HEADERS, timeout=60)
    r.raise_for_status()

    sgd_mwh, data_date = _parse_ema_excel(r.content)
    fx = fetch_fx_rate("SGD")
    usd_kwh = round(sgd_mwh / 1000 * fx, 6)

    return make_result("SG", metric_key, usd_kwh, "USD/kWh",
                       data_date, "monthly",
                       "EMA USEP", xl_url, "file_download", confidence,
                       raw_value=sgd_mwh,
                       currency_conversion=f"{sgd_mwh:.2f} SGD/MWh ÷ 1000 × {fx:.4f} SGD/USD = {usd_kwh:.6f} USD/kWh")


def collect_ema_reserve_margin(country_iso, metric_key, confidence=CONFIDENCE["file_download"], **_):
    """
    Calculate SG reserve margin using:
      - Total installed capacity: EMA installed generating capacity Excel
      - Monthly peak demand: EMA monthly peak system demand Excel
    Formula: (capacity_MW - peak_demand_MW) / peak_demand_MW * 100
    """
    cap_url  = "https://www.ema.gov.sg/resources/statistics/installed-generating-capacity"
    peak_url = "https://www.ema.gov.sg/resources/statistics/monthly-peak-system-demand"

    # Download capacity Excel
    cap_xl_url = _ema_find_excel_playwright(cap_url)
    r = _requests.get(cap_xl_url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    cap_mw, cap_date = _parse_ema_excel(r.content)

    # Download peak demand Excel
    peak_xl_url = _ema_find_excel_playwright(peak_url)
    r = _requests.get(peak_xl_url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    peak_mw, peak_date = _parse_ema_excel(r.content)

    reserve_pct = round((cap_mw - peak_mw) / peak_mw * 100, 2)
    data_date = max(cap_date, peak_date)

    return make_result("SG", metric_key, reserve_pct, "%",
                       data_date, "monthly",
                       "EMA capacity + peak demand", peak_url,
                       "file_download", confidence,
                       raw_value=f"cap={cap_mw:.0f}MW peak={peak_mw:.0f}MW")

print("Singapore collectors defined.")

METRIC_CASCADE = {

    # ── electricity_price ────────────────────────────────────────────────────
    ("US", "electricity_price"): [
        {"name": "EIA",                "fn": collect_eia,
         "kwargs": {"endpoint": "/v2/electricity/retail-sales/data/",
                    "params": {"frequency":"monthly","data[0]":"price","facets[sectorid][]":"IND",
                               "sort[0][column]":"period","sort[0][direction]":"desc","length":"1"},
                    "value_field": "price", "value_scale": 0.01,
                    "source_url": "https://api.eia.gov/v2/electricity/retail-sales/data/",
                    "confidence": CONFIDENCE["api_monthly"]}},
        {"name": "GlobalPetrolPrices", "fn": collect_globalpetrolprices,
         "kwargs": {"confidence": CONFIDENCE["web_scrape"]}},
    ],
    ("AE", "electricity_price"): [
        {"name": "GlobalPetrolPrices", "fn": collect_globalpetrolprices,
         "kwargs": {"confidence": CONFIDENCE["web_scrape"]}},
    ],
    ("BR", "electricity_price"): [
        {"name": "ANEEL CKAN",         "fn": collect_aneel_ckan,
         "kwargs": {"dataset": "tarifas-distribuidoras-energia-eletrica",
                    "value_field": "VlrTarifaAplicadaDistribuicao",
                    "filters": {"DscClasseConsumidor": "Industrial"},
                    "confidence": CONFIDENCE["api_monthly"]}},
        {"name": "GlobalPetrolPrices", "fn": collect_globalpetrolprices,
         "kwargs": {"confidence": CONFIDENCE["web_scrape"]}},
    ],
    ("IN", "electricity_price"): [
        {"name": "NPP India",          "fn": collect_npp_india,
         "kwargs": {"confidence": CONFIDENCE["api_monthly"]}},
        {"name": "CEA",                "fn": collect_cea,
         "kwargs": {"confidence": CONFIDENCE["api_annual"]}},
        {"name": "BEE PDF Gemini",     "fn": collect_pdf_gemini,
         "kwargs": {"pdf_url": "https://beeindia.gov.in/sites/default/files/Annual_Report_2023-24.pdf",
                    "metric_slug": "industrial_electricity_tariff",
                    "confidence": CONFIDENCE["pdf_regex"]}},
        {"name": "GlobalPetrolPrices", "fn": collect_globalpetrolprices,
         "kwargs": {"confidence": CONFIDENCE["web_scrape"]}},
    ],
    ("SG", "electricity_price"): [
        {"name": "EMA USEP",           "fn": collect_ema_usep,
         "kwargs": {"confidence": CONFIDENCE["api_monthly"]}},
        {"name": "data.gov.sg",        "fn": collect_data_gov_sg,
         "kwargs": {"resource_id": "d_61eac3cdb086814af485dcc682b75ae9",
                    "value_field": "tariff", "date_field": "quarter",
                    "confidence": CONFIDENCE["api_quarterly"]}},
        {"name": "GlobalPetrolPrices", "fn": collect_globalpetrolprices,
         "kwargs": {"confidence": CONFIDENCE["web_scrape"]}},
    ],
    ("PH", "electricity_price"): [
        {"name": "GlobalPetrolPrices", "fn": collect_globalpetrolprices,
         "kwargs": {"confidence": CONFIDENCE["web_scrape"]}},
    ],

    # ── renewable_share ──────────────────────────────────────────────────────
    ("US", "renewable_share"): [
        {"name": "EIA renewable share","fn": collect_eia,
         "kwargs": {"endpoint": "/v2/electricity/electric-power-operational-data/data/",
                    "params": {"frequency":"monthly","data[0]":"generation",
                               "facets[location][]":"US",
                               "sort[0][column]":"period","sort[0][direction]":"desc","length":"100"},
                    "value_field": "generation", "compute": "renewable_share_pct",
                    "source_url": "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/",
                    "confidence": CONFIDENCE["api_monthly"]}},
        {"name": "World Bank",         "fn": collect_worldbank,
         "kwargs": {"indicator": "EG.ELC.RNEW.ZS", "confidence": CONFIDENCE["api_annual"]}},
    ],
    ("AE", "renewable_share"): [
        {"name": "IRENA or World Bank", "fn": collect_irena_or_worldbank,
         "kwargs": {"wb_indicator": "EG.ELC.RNEW.ZS", "confidence": CONFIDENCE["api_annual"]}},
    ],
    ("BR", "renewable_share"): [
        {"name": "ONS S3",             "fn": collect_ons_s3,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
        {"name": "ANEEL CKAN capacity","fn": collect_aneel_ckan,
         "kwargs": {"dataset": "empreendimentos-de-geracao-de-energia-eletrica",
                    "value_field": "MdaPotenciaFiscalizadaKw",
                    "compute": "renewable_share_pct",
                    "confidence": CONFIDENCE["api_monthly"]}},
        {"name": "World Bank",         "fn": collect_worldbank,
         "kwargs": {"indicator": "EG.ELC.RNEW.ZS", "confidence": CONFIDENCE["api_annual"]}},
    ],
    ("IN", "renewable_share"): [
        {"name": "NPP India",          "fn": collect_npp_india,
         "kwargs": {"confidence": CONFIDENCE["api_monthly"]}},
        {"name": "CEA",                "fn": collect_cea,
         "kwargs": {"confidence": CONFIDENCE["api_monthly"]}},
        {"name": "IRENA PxWeb",        "fn": collect_irena,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
        {"name": "World Bank",         "fn": collect_worldbank,
         "kwargs": {"indicator": "EG.ELC.RNEW.ZS", "confidence": CONFIDENCE["api_annual"]}},
    ],
    ("SG", "renewable_share"): [
        {"name": "data.gov.sg fuel mix","fn": collect_data_gov_sg,
         "kwargs": {"resource_id": "d_ae4afbaf5bc96bde19d8ce85810ab9f4",
                    "value_field": "renewable_pct", "date_field": "month",
                    "confidence": CONFIDENCE["api_monthly"]}},
        {"name": "World Bank",         "fn": collect_worldbank,
         "kwargs": {"indicator": "EG.ELC.RNEW.ZS", "confidence": CONFIDENCE["api_annual"]}},
    ],
    ("PH", "renewable_share"): [
        {"name": "IRENA PxWeb",        "fn": collect_irena,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
        {"name": "World Bank",         "fn": collect_worldbank,
         "kwargs": {"indicator": "EG.ELC.RNEW.ZS", "confidence": CONFIDENCE["api_annual"]}},
    ],

    # ── grid_capacity ────────────────────────────────────────────────────────
    ("US", "grid_capacity"): [
        {"name": "EIA capacity",       "fn": collect_eia,
         "kwargs": {"endpoint": "/v2/electricity/operating-generator-capacity/data/",
                    "params": {"frequency":"monthly","data[0]":"nameplate-capacity-mw",
                               "sort[0][column]":"period","sort[0][direction]":"desc","length":"15000"},
                    "value_field": "nameplate-capacity-mw", "value_scale": 0.001,
                    "aggregate": "sum",
                    "source_url": "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/",
                    "confidence": CONFIDENCE["api_monthly"]}},
    ],
    ("AE", "grid_capacity"): [
        {"name": "DEWA PDF Gemini",    "fn": collect_pdf_gemini,
         "kwargs": {"pdf_url": "https://www.dewa.gov.ae/en/about-us/strategy-excellence/annual-statistics",
                    "metric_slug": "installed_capacity_gw",
                    "confidence": CONFIDENCE["pdf_regex"]}},
        {"name": "IRENA or World Bank", "fn": collect_irena_or_worldbank,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
    ],
    ("BR", "grid_capacity"): [
        {"name": "ONS S3",             "fn": collect_ons_s3,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
        {"name": "ANEEL CKAN sum",     "fn": collect_aneel_ckan,
         "kwargs": {"dataset": "empreendimentos-de-geracao-de-energia-eletrica",
                    "value_field": "MdaPotenciaFiscalizadaKw", "value_scale": 1e-6,
                    "aggregate": "sum",
                    "confidence": CONFIDENCE["api_monthly"]}},
    ],
    ("IN", "grid_capacity"): [
        {"name": "NPP India",          "fn": collect_npp_india,
         "kwargs": {"confidence": CONFIDENCE["api_monthly"]}},
        {"name": "CEA",                "fn": collect_cea,
         "kwargs": {"confidence": CONFIDENCE["api_monthly"]}},
    ],
    ("SG", "grid_capacity"): [
        {"name": "EMA Installed Capacity", "fn": collect_ema_grid_capacity,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
        {"name": "IRENA PxWeb",            "fn": collect_irena,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
    ],
    ("PH", "grid_capacity"): [
        {"name": "IRENA PxWeb",            "fn": collect_irena,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
    ],

    # ── reserve_margin ───────────────────────────────────────────────────────
    ("US", "reserve_margin"): [
        {"name": "NERC PDF Gemini",    "fn": collect_pdf_gemini,
         "kwargs": {"pdf_url": "https://www.nerc.com/pa/RAPA/ra/Reliability%20Assessments%20DL/NERC_SRA_2024.pdf",
                    "metric_slug": "reserve_margin_pct",
                    "confidence": CONFIDENCE["pdf_regex"]}},
    ],
    ("AE", "reserve_margin"): [
        {"name": "IRENA+WB proxy",     "fn": collect_irena_reserve_margin_proxy,
         "kwargs": {"confidence": 0.55}},
    ],
    ("BR", "reserve_margin"): [
        {"name": "ONS S3",             "fn": collect_ons_s3,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
    ],
    ("IN", "reserve_margin"): [
        {"name": "CEA",                "fn": collect_cea,
         "kwargs": {"confidence": CONFIDENCE["api_annual"]}},
        {"name": "CEA PDF Gemini",     "fn": collect_pdf_gemini,
         "kwargs": {"pdf_url": "https://cea.nic.in/wp-content/uploads/annual_report/2023/Annual_Report_2022_23.pdf",
                    "metric_slug": "reserve_margin_pct",
                    "confidence": CONFIDENCE["pdf_regex"]}},
    ],
    ("SG", "reserve_margin"): [
        {"name": "EMA capacity+demand","fn": collect_ema_reserve_margin,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
    ],
    ("PH", "reserve_margin"): [
        {"name": "IRENA+WB proxy",     "fn": collect_irena_reserve_margin_proxy,
         "kwargs": {"confidence": 0.55}},
    ],

    # ── energy_investment ────────────────────────────────────────────────────
    ("US", "energy_investment"): [
        {"name": "World Bank PPI",     "fn": collect_worldbank_energy_investment,
         "kwargs": {"confidence": CONFIDENCE["api_annual"]}},
    ],
    ("AE", "energy_investment"): [
        {"name": "World Bank PPI",     "fn": collect_worldbank_energy_investment,
         "kwargs": {"confidence": CONFIDENCE["api_annual"]}},
    ],
    ("BR", "energy_investment"): [
        {"name": "World Bank PPI",     "fn": collect_worldbank_energy_investment,
         "kwargs": {"confidence": CONFIDENCE["api_annual"]}},
    ],
    ("IN", "energy_investment"): [
        {"name": "World Bank PPI",     "fn": collect_worldbank_energy_investment,
         "kwargs": {"confidence": CONFIDENCE["api_annual"]}},
    ],
    ("SG", "energy_investment"): [
        {"name": "World Bank PPI",     "fn": collect_worldbank_energy_investment,
         "kwargs": {"confidence": CONFIDENCE["api_annual"]}},
    ],
    ("PH", "energy_investment"): [
        {"name": "World Bank PPI",     "fn": collect_worldbank_energy_investment,
         "kwargs": {"confidence": CONFIDENCE["api_annual"]}},
    ],

    # ── interconnection_queue_depth ──────────────────────────────────────────
    ("US", "interconnection_queue_depth"): [
        {"name": "LBNL Queued Up",     "fn": collect_lbnl_excel,
         "kwargs": {"confidence": CONFIDENCE["file_download"]}},
    ],
    ("AE", "interconnection_queue_depth"): [],  # → research agent (MBR Solar phases + GCCIA interconnect)
    ("BR", "interconnection_queue_depth"): [],  # → research agent (ANEEL Resolução 1069/2023 queue)
    ("IN", "interconnection_queue_depth"): [],  # → research agent (CEA transmission project queue)
    ("SG", "interconnection_queue_depth"): [],  # → research agent (EMA low-carbon import conditional approvals)
    ("PH", "interconnection_queue_depth"): [],  # → research agent (DOE RE50 pipeline + CREZ zones)
}

print(f"METRIC_CASCADE: {len(METRIC_CASCADE)} entries")
assert len(METRIC_CASCADE) == 36, f"Expected 36, got {len(METRIC_CASCADE)}"
print("All 36 country/metric combinations defined.")

import time

# Staleness thresholds (days) per access method — avoids re-fetching annual
# sources (USGS, FAO, EIA annual) on every daily run.
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
    """
    Check if existing DB data for this combo is stale or missing.
    Returns (is_stale: bool, age_days: int, existing_value).

    Staleness threshold is per access_method so annual API data isn't
    re-fetched daily. Same-day re-runs (age_days == 0) are never stale.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT metric_value, unit, collected_at, access_method, data_date
            FROM si1_raw_metrics
            WHERE country_iso = %s AND metric_key = %s
            ORDER BY collected_at DESC
            LIMIT 1
        """, (country_iso, metric_key))
        row = cur.fetchone()

    if not row:
        return True, None, None

    value, unit, collected_at, access_method, data_date = row
    age_days = (datetime.now(timezone.utc).replace(tzinfo=None) - collected_at).days
    threshold = _STALE_THRESHOLDS.get(access_method, 1)
    return age_days > threshold, age_days, value


def _fresh_conn():
    """Always return a fresh DB connection to avoid stale connections after long agent runs."""
    return psycopg2.connect(**DB_CONFIG)


def _try_research_agent(conn, run_id, country_iso, metric_key,
                        step_num, reason, errors, tried) -> bool:
    """Run the research agent as a fallback and store result if successful."""
    has_search = (TAVILY_API_KEY and TAVILY_API_KEY != "your_tavily_key_here") or JINA_API_KEY or BRAVE_API_KEY
    if not has_search:
        return False
    agent_name = "Research Agent (Brave + Claude)"
    tried.append(agent_name)
    t0 = time.perf_counter()
    try:
        dp      = collect_research(country_iso, metric_key,
                                   confidence=CONFIDENCE["web_scrape"])
        elapsed = int((time.perf_counter() - t0) * 1000)
        # Use a fresh connection after the potentially long agent run
        fresh = _fresh_conn()
        try:
            log_attempt(fresh, run_id, country_iso, metric_key, agent_name, step_num,
                        "success", dp.get("source_url"), None, None, elapsed)
            store_datapoint(fresh, dp, run_id)
        finally:
            fresh.close()
        print(f"  \u2713 [{country_iso}] {metric_key} = {dp['metric_value']} {dp['unit']} "
              f"(src={agent_name}, conf={dp['confidence_score']})")
        return True
    except Exception as exc:
        elapsed = int((time.perf_counter() - t0) * 1000)
        err_msg = str(exc)[:500]
        errors.append(f"[{agent_name}] {type(exc).__name__}: {err_msg}")
        try:
            fresh = _fresh_conn()
            log_attempt(fresh, run_id, country_iso, metric_key, agent_name, step_num,
                        "failed", None, type(exc).__name__, err_msg, elapsed)
            fresh.close()
        except Exception:
            pass  # DB logging failure shouldn't crash the pipeline
        print(f"  \u2717 [{country_iso}] {metric_key} \u2014 {agent_name}: {err_msg[:80]}")
        return False


def _get_last_known_dp(conn, country_iso: str, metric_key: str) -> dict | None:
    """Return the most recent non-NULL stored datapoint for carry-forward imputation."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, country_name, metric_key, metric_label,
                   metric_value, unit, data_date, data_frequency,
                   source_name, source_url, access_method, confidence_score,
                   raw_value, currency_conversion, is_imputed
            FROM si1_raw_metrics
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


def run_cascade(conn, run_id, country_iso, metric_key) -> bool:
    """
    Try each step in the cascade for this (country, metric) combo.
    The research agent is the universal last resort for ALL combos — whether
    the cascade is defined or not, and whether data is missing or stale.
    Returns True on first success, False if everything fails (gap opened).
    """
    steps = METRIC_CASCADE.get((country_iso, metric_key), [])
    errors = []
    tried  = []

    # ── Staleness check ───────────────────────────────────────────────────────
    is_stale, age_days, existing_val = _data_is_stale(conn, country_iso, metric_key)
    if not is_stale and existing_val is not None:
        print(f"  [FRESH] ({country_iso}, {metric_key}) — data is {age_days}d old, skipping")
        return True
    if age_days is not None:
        print(f"  [STALE {age_days}d] ({country_iso}, {metric_key}) — refreshing...")

    # ── Run cascade collectors (if any defined) ───────────────────────────────
    cascade_succeeded = False
    for step_num, step in enumerate(steps, start=1):
        name   = step["name"]
        fn     = step["fn"]
        kwargs = {**step["kwargs"], "country_iso": country_iso, "metric_key": metric_key}
        tried.append(name)
        t0     = time.perf_counter()
        try:
            dp      = fn(**kwargs)
            elapsed = int((time.perf_counter() - t0) * 1000)
            log_attempt(conn, run_id, country_iso, metric_key, name, step_num,
                        "success", dp.get("source_url"), None, None, elapsed)
            store_datapoint(conn, dp, run_id)
            print(f"  \u2713 [{country_iso}] {metric_key} = {dp['metric_value']} {dp['unit']} "
                  f"(src={name}, conf={dp['confidence_score']}, freq={dp['data_frequency']})")
            cascade_succeeded = True
            break
        except Exception as exc:
            elapsed  = int((time.perf_counter() - t0) * 1000)
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
                                          len(steps) + 1, "cascade_exhausted", errors, tried)
    if cascade_succeeded or agent_succeeded:
        return True

    # ── Everything failed → carry forward last known value, then open gap ────
    fresh = _fresh_conn()
    try:
        carried = _get_last_known_dp(fresh, country_iso, metric_key)
        if carried:
            store_datapoint(fresh, carried, run_id)
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

print("run_cascade defined.")

import uuid
from datetime import datetime

def run_pipeline(countries=None, metrics=None):
    """
    Run the full cascade for all (country, metric) combinations.
    Creates a run record, loops all tasks, logs results, closes run.
    Returns the run_id UUID string.
    """
    target_countries = countries or list(COUNTRIES.keys())
    target_metrics   = metrics   or list(METRICS.keys())
    run_id           = str(uuid.uuid4())
    started_at       = datetime.now(timezone.utc).replace(tzinfo=None)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO si1_collection_runs (run_id) VALUES (%s)", (run_id,)
        )
    conn.commit()

    combos    = [(c, m) for c in target_countries for m in target_metrics]
    total     = len(combos)
    succeeded = 0
    failed    = 0

    print(f"Run ID: {run_id}")
    print(f"Tasks:  {total}  ({len(target_countries)} countries \u00d7 {len(target_metrics)} metrics)\n")

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
        cur.execute("SELECT COUNT(*) FROM si1_data_gaps WHERE status='open'")
        gaps_open = cur.fetchone()[0]
        cur.execute("""
            UPDATE si1_collection_runs
            SET finished_at=%s, total_tasks=%s, succeeded=%s, failed=%s, gaps_opened=%s
            WHERE run_id=%s
        """, (finished_at, total, succeeded, failed, gaps_open, run_id))
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"Pipeline complete in {elapsed:.1f}s")
    print(f"  Succeeded : {succeeded}/{total}")
    print(f"  Failed    : {failed}/{total}")
    print(f"  Open gaps : {gaps_open}")
    print(f"{'='*60}")
    return run_id

print("run_pipeline defined.")

if __name__ == "__main__":
    run_id = run_pipeline()
    print(f"\nrun_id = {run_id}")
    print_token_summary()
