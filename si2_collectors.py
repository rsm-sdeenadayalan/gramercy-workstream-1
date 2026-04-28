"""
SI2 Water Availability Collectors
===================================
Live API collectors for Sub-Index 2 metrics, matching the cascade pattern of 02_pipeline.py.

Metrics:
  freshwater_per_capita          — World Bank WDI (ER.H2O.INTR.PC)
  baseline_water_stress          — WRI Aqueduct 4.0 (baseline sheet)
  projected_water_stress_2050    — WRI Aqueduct 4.0 (future sheet, SSP3-7.0)
  projected_water_stress_change  — Delta: projected - baseline
  regulatory_restrictions_score  — Claude NLP scoring over official regulatory docs

Countries: US, AE, BR, IN, SG, PH  (ISO2, matching the main pipeline)
"""

from __future__ import annotations

import io
import re
import zipfile
from datetime import date, datetime
from functools import lru_cache

import requests

# ── Shared config (loaded from main env by si2_pipeline.py) ──────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CSI-SI2-Pipeline/1.0; Research)"
}

COUNTRIES = {
    "US": {"name": "United States",        "iso3": "USA"},
    "AE": {"name": "United Arab Emirates", "iso3": "ARE"},
    "BR": {"name": "Brazil",               "iso3": "BRA"},
    "IN": {"name": "India",                "iso3": "IND"},
    "SG": {"name": "Singapore",            "iso3": "SGP"},
    "PH": {"name": "Philippines",          "iso3": "PHL"},
}

METRICS = {
    "freshwater_per_capita": {
        "label": "National Renewable Freshwater Resources per Capita",
        "unit":  "m3/year per capita",
        "gap_severity": "high",
    },
    "baseline_water_stress": {
        "label": "Baseline Water Stress (withdrawal-to-supply ratio)",
        "unit":  "0-5 score",
        "gap_severity": "high",
    },
    "projected_water_stress_2050": {
        "label": "Projected Water Stress in 2050 (SSP3-7.0)",
        "unit":  "0-5 score",
        "gap_severity": "medium",
    },
    "projected_water_stress_change": {
        "label": "Projected Change in Water Stress to 2050",
        "unit":  "delta score",
        "gap_severity": "medium",
    },
    "regulatory_restrictions_score": {
        "label": "Regulatory Restrictions on Industrial Water Use",
        "unit":  "1-5 score",
        "gap_severity": "low",
    },
}

CONFIDENCE = {
    "api":          1.00,
    "file":         0.88,
    "claude_nlp":   0.75,
    "manual":       0.70,
    "web_scrape":   0.60,
    "imputed":      0.30,
}

# ── Result builder ────────────────────────────────────────────────────────────
def make_result(country_iso, metric_key, value, unit, data_date,
                frequency, source_name, source_url, access_method,
                confidence, raw_value=None, is_override=False, override_note=""):
    return {
        "country_iso":       country_iso,
        "country_name":      COUNTRIES[country_iso]["name"],
        "metric_key":        metric_key,
        "metric_label":      METRICS[metric_key]["label"],
        "metric_value":      float(value),
        "unit":              unit,
        "data_date":         data_date,
        "data_frequency":    frequency,
        "source_name":       source_name,
        "source_url":        source_url,
        "access_method":     access_method,
        "confidence_score":  confidence,
        "raw_value":         str(raw_value) if raw_value is not None else None,
        "is_manual_override": is_override,
        "override_note":     override_note,
    }


# ── Metric 1: Freshwater per capita — World Bank WDI ─────────────────────────
def collect_worldbank_freshwater(country_iso, **_):
    """
    Fetch renewable freshwater per capita (m³/year) from World Bank API.
    Indicator: ER.H2O.INTR.PC  (FAO AQUASTAT)
    Returns the most recent non-null year.
    """
    iso2 = country_iso
    url  = (
        f"https://api.worldbank.org/v2/country/{iso2}"
        f"/indicator/ER.H2O.INTR.PC"
        f"?format=json&per_page=100&mrv=10"
    )
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    raw = r.json()
    if len(raw) < 2 or not raw[1]:
        raise ValueError(f"World Bank returned empty data for {country_iso}")

    # Find most recent non-null value
    records = [x for x in raw[1] if x.get("value") is not None]
    if not records:
        raise ValueError(f"No freshwater data for {country_iso}")
    latest = max(records, key=lambda x: int(x["date"]))
    value  = float(latest["value"])
    yr     = int(latest["date"])

    return make_result(
        country_iso, "freshwater_per_capita", value,
        "m3/year per capita", date(yr, 1, 1), "annual",
        "World Bank WDI (FAO AQUASTAT)",
        "https://data.worldbank.org/indicator/ER.H2O.INTR.PC",
        "api", CONFIDENCE["api"], raw_value=value,
    )


# ── WRI Aqueduct shared downloader (auto-detects latest version) ──────────────
_AQUEDUCT_ZIP_FALLBACK = "https://files.wri.org/aqueduct/aqueduct-4-0-country-rankings.zip"
_AQUEDUCT_VERSION_PAGE = "https://www.wri.org/data/aqueduct-global-maps-40-data"

def _find_aqueduct_zip_url():
    """Check WRI page for a newer Aqueduct ZIP; fall back to known 4.0 URL."""
    import re as _re
    try:
        r = requests.get(_AQUEDUCT_VERSION_PAGE, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            matches = _re.findall(
                r'https://files\.wri\.org/aqueduct/[^\s"\'<>]+country-rankings[^\s"\'<>]*\.zip',
                r.text
            )
            if matches:
                latest = sorted(matches, reverse=True)[0]
                if latest != _AQUEDUCT_ZIP_FALLBACK:
                    print(f"  [WRI] Newer Aqueduct ZIP found: {latest}")
                return latest
    except Exception:
        pass
    return _AQUEDUCT_ZIP_FALLBACK

@lru_cache(maxsize=1)
def _load_aqueduct():
    """Download latest WRI Aqueduct ZIP and cache all sheets. Called once per run."""
    zip_url = _find_aqueduct_zip_url()
    print(f"  [WRI] Downloading Aqueduct workbook from {zip_url} ...")
    r = requests.get(zip_url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    import pandas as pd
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        xlsx = next((n for n in zf.namelist() if n.lower().endswith(".xlsx")), None)
        if not xlsx:
            raise ValueError("WRI ZIP contains no .xlsx file")
        with zf.open(xlsx) as f:
            wb = pd.read_excel(f, sheet_name=None)
    print(f"  [WRI] Loaded sheets: {list(wb.keys())}")
    return wb


def _aqueduct_score(sheet_name, country_iso, scenario=None, year=None):
    """Extract a score from WRI Aqueduct for one country."""
    import pandas as pd
    import numpy as np

    wb       = _load_aqueduct()
    iso3     = COUNTRIES[country_iso]["iso3"]
    df       = wb[sheet_name].copy()

    # Find ISO column
    iso_col = next(
        (c for c in ["gid_0", "iso_a3", "iso3", "ISO3", "country_iso3", "iso_a3_eh"]
         if c in df.columns), None
    )
    if not iso_col:
        raise ValueError(f"WRI: no ISO column in sheet '{sheet_name}'. Cols: {list(df.columns)}")

    df = df[df[iso_col] == iso3].copy()
    if df.empty:
        raise ValueError(f"WRI: no rows for {iso3} in sheet '{sheet_name}'")

    # Filter by scenario/year if provided
    if "indicator_name" in df.columns:
        df = df[df["indicator_name"] == "bws"]
    if "weight" in df.columns:
        df = df[df["weight"] == "Tot"]
    if scenario and "scenario" in df.columns:
        df = df[df["scenario"] == scenario]
    if year and "year" in df.columns:
        df = df[df["year"] == year]

    score_col = next((c for c in ["score", "bws_score"] if c in df.columns), None)
    if not score_col:
        raise ValueError(f"WRI: no score column. Cols: {list(df.columns)}")

    val = pd.to_numeric(df[score_col], errors="coerce").replace(-9999, np.nan).dropna()
    if val.empty:
        raise ValueError(f"WRI: no valid score for {iso3}")
    return float(val.iloc[0])


# ── Metric 2: Baseline water stress ──────────────────────────────────────────
def collect_wri_baseline(country_iso, **_):
    """WRI Aqueduct 4.0 baseline water stress score (0-5).
    Countries without a watershed entry (e.g. SG) raise ValueError — the pipeline
    then falls through to the research agent which discovers a current source."""
    score = _aqueduct_score("country_baseline", country_iso)
    return make_result(
        country_iso, "baseline_water_stress", score, "0-5 score",
        date(2023, 1, 1), "structural",
        "WRI Aqueduct 4.0", _AQUEDUCT_ZIP_FALLBACK,
        "file", CONFIDENCE["file"], raw_value=score,
    )


# ── Metric 3: Projected water stress 2050 ────────────────────────────────────
def collect_wri_projected(country_iso, **_):
    """WRI Aqueduct 4.0 projected 2050 water stress (SSP3-7.0 / business-as-usual).
    Countries without a watershed entry raise ValueError → fallthrough to agent."""
    score = _aqueduct_score("country_future", country_iso, scenario="bau", year=2050)
    return make_result(
        country_iso, "projected_water_stress_2050", score, "0-5 score",
        date(2050, 1, 1), "scenario",
        "WRI Aqueduct 4.0 (SSP3-7.0)", _AQUEDUCT_ZIP_FALLBACK,
        "file", CONFIDENCE["file"], raw_value=score,
    )


# ── Metric 3b: Projected change in water stress ───────────────────────────────
def collect_wri_stress_change(country_iso, **_):
    """Delta: projected 2050 (SSP3-7.0) minus baseline.
    Countries without a watershed entry raise ValueError → fallthrough to CCKP proxy, then agent."""
    baseline  = _aqueduct_score("country_baseline", country_iso)
    projected = _aqueduct_score("country_future", country_iso, scenario="bau", year=2050)
    delta     = round(projected - baseline, 4)
    return make_result(
        country_iso, "projected_water_stress_change", delta, "delta score",
        date(2050, 1, 1), "scenario",
        "WRI Aqueduct 4.0 (projected - baseline)", _AQUEDUCT_ZIP_FALLBACK,
        "file", CONFIDENCE["file"], raw_value=f"proj={projected} base={baseline}",
    )


# ── Metric 3c: CCKP fallback for projected stress change ─────────────────────
# World Bank Climate Knowledge Portal — CMIP6 ensemble, SSP3-7.0
# Used only when WRI Aqueduct ZIP is unavailable.
_CCKP_ISO3 = {c: d["iso3"] for c, d in COUNTRIES.items()}
_CCKP_BASE = "https://cckpapi.worldbank.org/cckp/v1"

def collect_cckp_stress_change(country_iso, **_):
    """
    Fallback: estimate projected water stress change from World Bank CCKP CMIP6.
    Uses runoff (ro) anomaly under SSP3-7.0 2040-2059 vs 1995-2014 baseline.
    A negative runoff anomaly → positive water stress increase (inverted sign).
    Confidence is lower than WRI because CCKP uses runoff proxy, not withdrawal ratio.
    """
    iso3 = _CCKP_ISO3[country_iso]

    def _fetch(period, scenario="historical"):
        var = "ro"  # runoff anomaly
        url = f"{_CCKP_BASE}/cmip6-x0.5_timeseries_{scenario}_{period}_{iso3}_mean"
        r = requests.get(url, params={"_format": "json"}, headers=HEADERS, timeout=30)
        if r.status_code == 404:
            raise ValueError(f"CCKP: no runoff data for {iso3}/{period}/{scenario}")
        r.raise_for_status()
        data = r.json()
        # Response is a list of {year, value} or a nested dict — handle both
        if isinstance(data, list):
            vals = [float(d["value"]) for d in data if d.get("value") is not None]
        elif isinstance(data, dict):
            vals = [float(v) for v in data.get(var, {}).values() if v is not None]
        else:
            raise ValueError(f"CCKP: unexpected response format")
        if not vals:
            raise ValueError(f"CCKP: empty runoff data for {iso3}")
        return sum(vals) / len(vals)

    baseline_ro  = _fetch("1995-2014")
    projected_ro = _fetch("2040-2059", scenario="ssp370")

    # Runoff decrease → stress increases; scale to approximate 0-5 WRI delta
    ro_change_pct = (projected_ro - baseline_ro) / max(abs(baseline_ro), 1e-6) * 100
    # Heuristic: −10% runoff ≈ +0.3 stress delta (calibrated against WRI for 6 countries)
    delta = round(-ro_change_pct / 33.3, 4)
    delta = max(-2.0, min(2.0, delta))  # clamp to plausible range

    return make_result(
        country_iso, "projected_water_stress_change", delta, "delta score",
        date(2050, 1, 1), "scenario",
        "World Bank CCKP CMIP6 SSP3-7.0 (runoff proxy)",
        f"https://climateknowledgeportal.worldbank.org/country/{iso3.lower()}",
        "api", 0.65, raw_value=f"ro_baseline={baseline_ro:.2f} ro_proj={projected_ro:.2f}",
    )


# ── Singapore-specific proxy: PUB Resilience Margin ──────────────────────────
# The standard withdrawal/supply stress metric fails for city-states with no
# internal watershed — WRI caps SG at 5.0 which misrepresents its actual risk
# profile. The investment-relevant metric is how much of demand can be met by
# weather-resilient sources (NEWater + Desalination) regardless of rainfall.
#
# Proxy formula (defensible logic, all inputs from PUB Annual Sustainability
# Report, discovered dynamically each run):
#     resilience_margin = (NEWater_capacity + Desal_capacity) / Total_demand
# Mapped to a 0-5 stress score so it's comparable to WRI scores for other
# countries. Linear inversion: margin 1.0 → 1.5, margin 0.0 → 5.0.

def collect_sg_resilience_proxy(country_iso, metric_key, **_):
    """
    SG-only. Discovers the latest PUB Annual Sustainability Report via search,
    extracts NEWater/Desalination capacity and total demand with Claude, and
    computes a resilience-adjusted water-stress score.
    """
    if country_iso != "SG":
        raise ValueError("PUB resilience proxy applies only to Singapore")
    if metric_key not in ("baseline_water_stress", "projected_water_stress_2050"):
        raise ValueError(f"PUB resilience proxy does not cover {metric_key}")

    import os, json
    from research_agent import web_search
    import pdfplumber

    # Phase 1: discover the latest PUB Annual/Sustainability Report (dynamic)
    query   = "PUB Annual Sustainability Report NEWater desalination capacity demand site:pub.gov.sg"
    results = web_search(query, count=5)
    pdf_hits = [r for r in results
                if "pub.gov.sg" in r.get("url", "")
                and r.get("url", "").lower().endswith(".pdf")]
    if not pdf_hits:
        # Accept any pub.gov.sg hit — the page may link to the PDF
        pdf_hits = [r for r in results if "pub.gov.sg" in r.get("url", "")]
    if not pdf_hits:
        raise ValueError("PUB Annual/Sustainability Report not found via search")

    # Phase 2: download PDF (download_pdf-style fallback: follow first .pdf link)
    src_url  = pdf_hits[0]["url"]
    r        = requests.get(src_url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "pdf" not in ct and not src_url.lower().endswith(".pdf"):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        hrefs = [a["href"] for a in soup.find_all("a", href=True)
                 if a["href"].lower().endswith(".pdf") and "ASR" in a["href"].upper()
                 or a["href"].lower().endswith(".pdf") and "SUSTAIN" in a["href"].upper()]
        if not hrefs:
            raise ValueError("No PUB ASR PDF link found on landing page")
        pdf_url = hrefs[0] if hrefs[0].startswith("http") else "https://www.pub.gov.sg" + hrefs[0]
        r = requests.get(pdf_url, headers=HEADERS, timeout=60); r.raise_for_status()
        src_url = pdf_url

    # Phase 3: extract text (first 50 pages — operational sections usually early)
    with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        full_text = "\n".join(p.extract_text() or "" for p in pdf.pages[:50])
    if len(full_text.strip()) < 500:
        raise ValueError("PUB ASR text extraction returned insufficient content")

    # Phase 4: ask Claude to extract the three capacity/demand figures
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not anthropic_key:
        raise ValueError("ANTHROPIC_API_KEY not set")
    year_context = "future-target (2060-2065)" if metric_key.startswith("projected") \
                   else "current-year operational"
    prompt = f"""Extract these three figures from Singapore's PUB Annual/Sustainability Report.
Prefer {year_context} values for the capacity figures.

Return ONLY JSON:
{{
  "newater_mgd": <NEWater production capacity, million gallons per day (float)>,
  "desal_mgd":   <Desalination production capacity, million gallons per day (float)>,
  "demand_mgd":  <Total national water demand, million gallons per day (float)>,
  "data_year":   <int: year these figures describe>,
  "evidence":    "<one sentence quoting the strongest supporting text>"
}}

If a figure is expressed as "% of demand", convert it (e.g. "NEWater meets 40%% of demand" with demand=440 → newater_mgd=176).

Source text:
---
{full_text[:15000]}
---"""

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": anthropic_key, "anthropic-version": "2023-06-01",
                 "content-type": "application/json"},
        json={"model": "claude-haiku-4-5-20251001", "max_tokens": 600,
              "messages": [{"role": "user", "content": prompt}]},
        timeout=90,
    )
    resp.raise_for_status()
    raw = resp.json()["content"][0]["text"].strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE).strip()
    raw = re.sub(r"```\s*$", "", raw).strip()
    extracted = __import__("json").loads(raw[raw.find("{"):raw.rfind("}") + 1])

    newater = float(extracted["newater_mgd"])
    desal   = float(extracted["desal_mgd"])
    demand  = float(extracted["demand_mgd"])
    if demand <= 0:
        raise ValueError("PUB ASR extraction returned invalid demand")

    resilience_margin = (newater + desal) / demand

    # Linear inversion to 0-5 WRI-compatible stress scale:
    #   margin ≥ 1.0 → 1.5   (weather-resilient > demand)
    #   margin  0.7 → 2.55  (current SG profile: 40% NEWater + 30% Desal)
    #   margin  0.0 → 5.0
    score = max(0.5, min(5.0, 5.0 - resilience_margin * 3.5))

    data_year = int(extracted.get("data_year", date.today().year))
    data_dt   = date(data_year, 1, 1) if metric_key == "baseline_water_stress" \
                else date(2050, 1, 1)

    return make_result(
        "SG", metric_key, score, "0-5 score",
        data_dt, "annual" if metric_key == "baseline_water_stress" else "scenario",
        "PUB Annual Sustainability Report (Resilience Margin proxy)",
        src_url, "file", CONFIDENCE["file"],
        raw_value=(f"NEWater={newater}mgd + Desal={desal}mgd / Demand={demand}mgd "
                   f"= margin {resilience_margin:.3f} → score {score:.2f}"),
    )


# ── Metric 4: Regulatory restrictions — Claude NLP scoring ───────────────────
# ── Regulator-domain hints per country (stable reference data, not document URLs) ──
# Used to *restrict* dynamic discovery to authoritative regulator websites.
# The scorer searches each country's trusted domains and discovers the current
# regulatory landing pages at runtime rather than binding to specific URLs.
# Domains identified from "Global Water Data Pipeline Request" research report.
_REGULATOR_DOMAINS = {
    "US": ["epa.gov", "echo.epa.gov", "govinfo.gov", "usgs.gov", "federalregister.gov"],
    "AE": ["moccae.gov.ae", "ead.gov.ae", "moei.gov.ae", "u.ae", "dewa.gov.ae", "ewec.ae"],
    "BR": ["ana.gov.br", "gov.br", "mma.gov.br", "snirh.gov.br",
           "progestao.ana.gov.br", "cetesb.sp.gov.br", "igam.mg.gov.br"],
    "IN": ["jalshakti-dowr.gov.in", "cgwa-noc.gov.in", "cpcb.nic.in", "cgwb.gov.in",
           "pib.gov.in", "mowr.gov.in"],
    "SG": ["pub.gov.sg", "nea.gov.sg", "sso.agc.gov.sg", "data.gov.sg", "mewr.gov.sg"],
    "PH": ["emb.gov.ph", "nwrb.gov.ph", "denr.gov.ph", "eia.emb.gov.ph",
           "pids.gov.ph", "senate.gov.ph", "dpwh.gov.ph"],
}

# Country-specific search query phrasings that retrieve the most relevant
# regulatory pages. Native-language terms where authorities publish in
# non-English. Not hardcoded URLs — just vocabulary validated by the research
# report to retrieve current authoritative documents.
_REGULATORY_QUERIES = {
    "US": [
        "industrial water withdrawal permit NPDES pretreatment standards",
        "EPA ECHO NPDES facility search enforcement compliance",
        "Clean Water Act industrial user requirements DMR",
    ],
    "AE": [
        "industrial water use regulations compliance water resources law",
        "UAE Water Security Strategy 2036 industrial permits",
        # Arabic: "Water Resources Status Report" and "Water Well Permits"
        "تقرير حالة الموارد المائية الإمارات",
        "تصاريح حفر آبار المياه الإمارات",
    ],
    "BR": [
        "outorga de direito de uso de recursos hídricos industrial",
        "Conjuntura dos Recursos Hídricos no Brasil ANA",
        "SNIRH CNARH cadastro usuários recursos hídricos",
        "certificação de metas Progestão ANA",
        "fiscalização captação de água ANA indústria",
    ],
    "IN": [
        "CGWA NOC industrial ground water extraction compliance",
        "Dynamic Ground Water Resources Assessment India",
        "Bhuneer portal industrial water NOC guidelines",
        "Central Pollution Control Board industrial effluent standards",
    ],
    "SG": [
        "PUB Annual Sustainability Report water efficiency industrial",
        "Water Efficiency Management Plan WEMP compliance",
        "Public Utilities Act Sewerage Drainage industrial",
    ],
    "PH": [
        "NWRB water permit application industrial withdrawal",
        "DENR-EMB Environmental Compliance Certificate ECC industrial",
        "Clean Water Act RA 9275 industrial discharge compliance",
        "Water Code Philippines PD 1067 industrial rights",
    ],
}

_REGULATORY_PROMPT = """You are an expert in water law and industrial water regulation.

Country: {country_name} ({country_iso})
Primary regulator: {regulator}

Below is official regulatory text collected from government sources. Score this country on THREE dimensions (each 1-5):

**Dimension 1 — Regulatory Framework (1-5)**
Does the country have a formal permitting/licensing system for industrial water withdrawals?
1=No formal framework; 5=Comprehensive mandatory permit system with clear standards

**Dimension 2 — Monitoring & Reporting (1-5)**
Are industries required to monitor and report water use to regulators?
1=No requirements; 5=Real-time metering + mandatory regular reporting with verification

**Dimension 3 — Enforcement & Restrictions (1-5)**
Are there enforceable restrictions, penalties, or compliance mechanisms for industrial water use?
1=No enforcement; 5=Strong penalties + active enforcement + suspension powers

**Overall Score** = weighted average: Framework×0.4 + Monitoring×0.3 + Enforcement×0.3

Source content:
---
{content}
---

Respond ONLY with valid JSON:
{{"dimension_framework": <1-5 float>, "dimension_monitoring": <1-5 float>, "dimension_enforcement": <1-5 float>, "overall_score": <1-5 float>, "confidence": "<high|medium|low>", "key_evidence": "<one sentence quoting the strongest evidence found>", "data_date": "<YYYY-MM-DD of most recent source>"}}"""

_REGULATORS = {
    "US": "US EPA / delegated state permitting authorities",
    "AE": "MOCCAE / Environment Agency Abu Dhabi",
    "BR": "Agência Nacional de Águas (ANA)",
    "IN": "Central Ground Water Authority / Ministry of Jal Shakti",
    "SG": "PUB — Singapore's National Water Agency",
    "PH": "National Water Resources Board / EMB",
}


def collect_regulatory_score(country_iso, anthropic_api_key, **_):
    """
    Score regulatory restrictions on industrial water use using Claude.
    URLs are discovered dynamically at runtime by searching the country's
    trusted regulator domains — no hardcoded document URLs.
    """
    import trafilatura
    from research_agent import web_search

    domains       = _REGULATOR_DOMAINS.get(country_iso, [])
    query_phrases = _REGULATORY_QUERIES.get(country_iso, ["industrial water regulation permit"])
    country_name  = COUNTRIES[country_iso]["name"]
    if not domains:
        raise ValueError(f"No regulator-domain hints configured for {country_iso}")

    # ── Phase 1: discover current regulator URLs via search ─────────────────
    discovered = []
    seen       = set()
    for phrase in query_phrases:
        site_filter = " OR ".join(f"site:{d}" for d in domains)
        q           = f"{country_name} {phrase} ({site_filter})"
        try:
            for r in web_search(q, count=3):
                url = r.get("url", "")
                if not url or url in seen:
                    continue
                # Keep only hits on the trusted regulator domains
                if not any(d in url for d in domains):
                    continue
                seen.add(url)
                discovered.append({"url": url, "content": r.get("content", "")})
        except Exception as e:
            print(f"    [SI2 regulatory discovery] {country_iso}/{phrase[:40]}: {e}")

    if not discovered:
        raise ValueError(f"No regulator URLs discovered for {country_iso}")

    # ── Phase 2: fetch+extract text from each discovered URL ────────────────
    combined_text = ""
    fetched_urls  = []
    for hit in discovered[:5]:  # cap to top 5 to control cost/latency
        url     = hit["url"]
        prefetch = hit.get("content", "")
        try:
            if prefetch and len(prefetch.strip()) > 200:
                # Search API already returned extracted content — use it directly
                text = prefetch
            elif url.lower().endswith(".pdf"):
                import pdfplumber
                r = requests.get(url, headers=HEADERS, timeout=45)
                r.raise_for_status()
                with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                    text = "\n".join(p.extract_text() or "" for p in pdf.pages[:10])
            else:
                downloaded = trafilatura.fetch_url(url)
                text = trafilatura.extract(downloaded, include_tables=True, favor_recall=True) or ""

            if len(text.strip()) > 100:
                combined_text += f"\n\n=== Source: {url} ===\n{text[:3000]}"
                fetched_urls.append(url)
        except Exception as e:
            print(f"    [SI2 regulatory fetch] {country_iso}/{url[:60]}: {e}")

    if not combined_text:
        raise ValueError(f"Could not fetch regulatory text for {country_iso}")

    prompt = _REGULATORY_PROMPT.format(
        country_name=COUNTRIES[country_iso]["name"],
        country_iso=country_iso,
        regulator=_REGULATORS.get(country_iso, ""),
        content=combined_text[:6000],
    )

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         anthropic_api_key,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json={
            "model":      "claude-haiku-4-5-20251001",
            "max_tokens": 512,
            "messages":   [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    r.raise_for_status()

    raw = r.json()["content"][0]["text"].strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"```\s*$", "", raw).strip()

    # Extract JSON robustly
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    result = __import__("json").loads(raw[start:end])

    score     = float(result["overall_score"])
    conf_map  = {"high": CONFIDENCE["claude_nlp"], "medium": 0.65, "low": 0.55}
    conf      = conf_map.get(result.get("confidence", "medium"), 0.65)

    try:
        data_date = datetime.fromisoformat(result.get("data_date", "")).date()
        if data_date > date.today():
            data_date = date.today()
    except Exception:
        data_date = date.today().replace(month=1, day=1)

    return make_result(
        country_iso, "regulatory_restrictions_score", score, "1-5 score",
        data_date, "qualitative",
        f"Claude NLP — {_REGULATORS.get(country_iso, 'regulatory sources')}",
        fetched_urls[0] if fetched_urls else discovered[0]["url"],
        "claude_nlp", conf,
        raw_value=result.get("key_evidence", ""),
    )


print("SI2 collectors loaded.")
