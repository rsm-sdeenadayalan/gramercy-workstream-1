"""
Deep Research Agent for Energy Metrics
=======================================
Implements a Gemini Deep Research-equivalent loop:
  1. Generate multiple targeted search queries
  2. Search + fetch pages in parallel
  3. Reflect — is the data sufficient? What's missing?
  4. Loop with follow-up queries until confident
  5. Synthesize — extract value, convert units, cite source

Called from 02_pipeline.py via collect_research().
"""

from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Optional

import requests
import trafilatura
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TAVILY_API_KEY    = os.environ.get("TAVILY_API_KEY", "")
CLAUDE_MODEL      = os.environ.get("CLAUDE_RESEARCH_MODEL", "claude-haiku-4-5-20251001")

# Mineral-authority domains added to every country's trusted list (global IGOs +
# national geological surveys). Sourced from SI3 audit — these publish per-country
# mineral production / reserves / refining data that USGS MCS may not surface.
_MINERAL_GLOBAL_DOMAINS = [
    "usgs.gov", "pubs.usgs.gov",                  # USGS Mineral Commodity Summaries
    "bgs.ac.uk", "www2.bgs.ac.uk",                # British Geological Survey
    "bgr.bund.de",                                 # German Federal Inst. Geosciences
    "world-mining-data.info",                      # Austrian Federal Ministry
    "icsg.org",                                    # Int'l Copper Study Group
    "insg.org",                                    # Int'l Nickel Study Group
    "cobaltinstitute.org",                         # Cobalt Institute
    "minerals4eu.eu",                              # EU mineral resources network
    "ga.gov.au",                                   # Geoscience Australia
    "data.worldbank.org",                          # World Bank Mineral Rents indicator
]

# Trusted source domains per country
# Combined trusted domains covering BOTH sub-indexes (energy + water). Patterns
# identified from two research reports: energy sources + water data pipeline.
TRUSTED_SOURCES = {
    "US": ["eia.gov", "energy.gov", "iea.org", "ferc.gov", "lbl.gov", "nerc.com",
           # Water
           "epa.gov", "echo.epa.gov", "usgs.gov", "govinfo.gov",
           # Food
           "usda.gov", "fas.usda.gov", "ers.usda.gov", "nass.usda.gov",
           "census.gov", "bea.gov", "bls.gov",
           # International
           "worldbank.org", "fao.org", "faostat.fao.org", "comtrade.un.org",
           "comtradeplus.un.org", "wto.org", "wri.org",
           "climateknowledgeportal.worldbank.org",
           "reuters.com", "bloomberg.com", "ft.com"],
    "AE": ["moei.gov.ae", "dewa.gov.ae", "doe.gov.ae", "u.ae", "feeds.dfm.ae",
           "gccia.com.sa", "ewec.ae", "taqa.com", "iea.org", "irena.org",
           # Water
           "moccae.gov.ae", "ead.gov.ae",
           # Food
           "fcsc.gov.ae", "moec.gov.ae", "moiat.gov.ae", "adafsa.gov.ae",
           "opendata.fcsc.gov.ae",
           # International
           "worldbank.org", "fao.org", "faostat.fao.org", "comtrade.un.org",
           "comtradeplus.un.org", "wto.org", "wri.org",
           "climateknowledgeportal.worldbank.org",
           "reuters.com", "bloomberg.com", "thenationalnews.com"],
    "BR": ["epe.gov.br", "aneel.gov.br", "ons.org.br", "mme.gov.br", "gov.br",
           "engie.com.br", "absolar.org.br", "iea.org",
           # Water
           "ana.gov.br", "snirh.gov.br", "progestao.ana.gov.br",
           "cetesb.sp.gov.br", "igam.mg.gov.br", "mma.gov.br",
           # Food
           "agricultura.gov.br", "conab.gov.br", "ibge.gov.br", "embrapa.br",
           "mdic.gov.br", "siscomex.gov.br",
           # International
           "worldbank.org", "fao.org", "faostat.fao.org", "comtrade.un.org",
           "comtradeplus.un.org", "wto.org", "wri.org",
           "climateknowledgeportal.worldbank.org",
           "reuters.com", "bloomberg.com"],
    "IN": ["cea.nic.in", "mnre.gov.in", "powermin.gov.in", "iea.org", "niti.gov.in",
           "beeindia.gov.in",
           # Water
           "jalshakti-dowr.gov.in", "cgwa-noc.gov.in", "cgwb.gov.in",
           "cpcb.nic.in", "pib.gov.in", "mowr.gov.in",
           # Food
           "agricoop.nic.in", "commerce.gov.in", "dgft.gov.in", "icar.org.in",
           "mospi.gov.in", "tradestat.commerce.gov.in", "ftddp.dgciskol.gov.in",
           # International
           "worldbank.org", "fao.org", "faostat.fao.org", "comtrade.un.org",
           "comtradeplus.un.org", "wto.org", "wri.org",
           "climateknowledgeportal.worldbank.org",
           "reuters.com", "bloomberg.com"],
    "SG": ["ema.gov.sg", "mti.gov.sg", "greenplan.gov.sg", "iea.org", "edb.gov.sg",
           "data.gov.sg",
           # Water
           "pub.gov.sg", "nea.gov.sg", "sso.agc.gov.sg", "mewr.gov.sg",
           # Food
           "sfa.gov.sg", "singstat.gov.sg", "tablebuilder.singstat.gov.sg",
           "mof.gov.sg", "customs.gov.sg",
           # International
           "worldbank.org", "fao.org", "faostat.fao.org", "comtrade.un.org",
           "comtradeplus.un.org", "wto.org", "wri.org",
           "climateknowledgeportal.worldbank.org",
           "reuters.com", "bloomberg.com", "straitstimes.com"],
    "PH": ["doe.gov.ph", "ngcp.ph", "transco.com.ph", "erc.gov.ph", "icsc.ngo",
           "pids.gov.ph", "meralco.com.ph", "iea.org",
           # Water
           "nwrb.gov.ph", "emb.gov.ph", "denr.gov.ph", "eia.emb.gov.ph",
           "dpwh.gov.ph", "senate.gov.ph",
           # Food
           "da.gov.ph", "psa.gov.ph", "bas.psa.gov.ph", "fida.da.gov.ph",
           "philfida.da.gov.ph", "neda.gov.ph",
           # International
           "worldbank.org", "fao.org", "faostat.fao.org", "comtrade.un.org",
           "comtradeplus.un.org", "wto.org", "wri.org",
           "climateknowledgeportal.worldbank.org",
           "reuters.com", "bloomberg.com", "pna.gov.ph"],
}

# Country-specific mineral / mining authority domains
_COUNTRY_MINERAL_DOMAINS = {
    "US": ["usgs.gov", "doi.gov", "energy.gov"],
    "AE": ["moiat.gov.ae", "u.ae"],
    "BR": ["gov.br/anm", "anm.gov.br", "ibram.org.br", "cprm.gov.br"],
    "IN": ["ibm.gov.in", "mines.gov.in", "geologicalsurvey.gov.in"],
    "SG": [],  # Singapore has no domestic mining
    "PH": ["mgb.gov.ph", "denr.gov.ph"],
}

# Merge mineral-authority domains into every country's trusted list (additive,
# de-duped). This biases the research agent's site-restricted searches toward
# government/IGO mineral sources for SI3 metrics without changing other indexes.
for _iso in TRUSTED_SOURCES:
    _existing = set(TRUSTED_SOURCES[_iso])
    for _d in _MINERAL_GLOBAL_DOMAINS + _COUNTRY_MINERAL_DOMAINS.get(_iso, []):
        if _d not in _existing:
            TRUSTED_SOURCES[_iso].append(_d)
            _existing.add(_d)

# Native-language search support: only for countries where authoritative sources
# publish primarily in a non-English language. English-speaking or English-
# publishing-first countries (US/IN/SG/PH) are excluded.
NATIVE_LANGUAGES = {
    "BR": "pt",    # Portuguese — ONS, EPE, ANEEL, ANA, ENGIE publish in Portuguese
    "AE": "ar",    # Arabic — supplementary; DEWA/DoE/MOCCAE have English primaries
                   # but federal-regulator strategic docs are often Arabic-only
    "IN": "hi",    # Hindi — supplementary only; CGWA/PIB publish primarily in English
                   # but PIB and state-level notices also in Hindi
}

# Native-language query templates — authoritative local vocabulary that retrieves
# documents English searches miss. Keyed by (country_iso, metric_key).
NATIVE_QUERY_TEMPLATES = {
    # ═══ ENERGY (SI1) ═════════════════════════════════════════════════════════
    # ── Brazil / Portuguese ──────────────────────────────────────────────────
    ("BR", "electricity_price"):
        "Brasil tarifa energia elétrica industrial ANEEL {year} R$/MWh",
    ("BR", "renewable_share"):
        "Brasil matriz elétrica participação renovável ONS EPE {year}",
    ("BR", "grid_capacity"):
        "Brasil capacidade instalada geração elétrica GW ANEEL ONS {year}",
    ("BR", "reserve_margin"):
        "Brasil margem de reserva eficiente operativa ONS EPE {year}",
    ("BR", "energy_investment"):
        "Brasil investimento infraestrutura setor elétrico bilhões próximos anos {year}",
    ("BR", "interconnection_queue_depth"):
        "Brasil fila de acesso transmissão ANEEL MW Resolução 1069 outorgas {year}",

    # ── UAE / Arabic (supplementary) ─────────────────────────────────────────
    ("AE", "reserve_margin"):
        "الإمارات هامش الاحتياطي الكهرباء السعة المركبة الذروة {year}",
    ("AE", "energy_investment"):
        "الإمارات استثمار البنية التحتية الطاقة مليار درهم {year}",
    ("AE", "interconnection_queue_depth"):
        "الإمارات مشاريع الطاقة الشمسية قيد الإنشاء ميجاوات {year}",

    # ═══ WATER (SI2) ══════════════════════════════════════════════════════════
    # ── Brazil / Portuguese — ANA is the primary publisher ───────────────────
    ("BR", "freshwater_per_capita"):
        "Brasil recursos hídricos renováveis per capita m3 ANA AQUASTAT {year}",
    ("BR", "baseline_water_stress"):
        "Brasil estresse hídrico retirada oferta razão bacia hidrográfica {year}",
    ("BR", "projected_water_stress_2050"):
        "Brasil projeção estresse hídrico 2050 mudança climática CMIP6",
    ("BR", "projected_water_stress_change"):
        "Brasil variação disponibilidade hídrica projeção 2050 cenário climático",
    ("BR", "regulatory_restrictions_score"):
        "Brasil outorga direito uso recursos hídricos Conjuntura ANA {year}",

    # ── UAE / Arabic — MOCCAE + EAD are primary publishers ───────────────────
    ("AE", "freshwater_per_capita"):
        "الإمارات الموارد المائية المتجددة للفرد متر مكعب {year}",
    ("AE", "baseline_water_stress"):
        "الإمارات الإجهاد المائي نسبة السحب إلى العرض موارد متجددة {year}",
    ("AE", "regulatory_restrictions_score"):
        "الإمارات تقرير حالة الموارد المائية سياسة الأمن المائي تصاريح {year}",

    # ── India / Hindi + domain English — CGWA is primary ────────────────────
    # (Hindi script is supplementary — CGWA and PIB publish primarily in English
    #  but with Hindi cross-references)
    ("IN", "regulatory_restrictions_score"):
        "भारत औद्योगिक भूजल निकासी अनापत्ति प्रमाण पत्र CGWA {year}",
    ("IN", "projected_water_stress_2050"):
        "India groundwater over-exploited block categorization CGWB Dynamic Assessment {year}",

    # ═══ FOOD (SI4) — native-language templates ════════════════════════════════
    # ── Brazil / Portuguese — MAPA, CONAB, IBGE are primary publishers ──────
    ("BR", "net_food_trade_balance"):
        "Brasil balança comercial agronegócio exportações importações alimentos USD {year} MAPA",
    ("BR", "caloric_self_sufficiency_ratio"):
        "Brasil autossuficiência alimentar produção doméstica consumo calórico {year}",
    ("BR", "share_global_staple_exports"):
        "Brasil participação exportações mundiais soja milho açúcar trigo {year} CONAB",
    ("BR", "arable_land_per_capita"):
        "Brasil área agricultável hectares per capita IBGE {year}",

    # ── UAE / Arabic — FCSC, MOEC, ADAFSA are primary publishers ────────────
    ("AE", "net_food_trade_balance"):
        "الإمارات الميزان التجاري للغذاء الصادرات الواردات الزراعية {year}",
    ("AE", "caloric_self_sufficiency_ratio"):
        "الإمارات الاكتفاء الذاتي الغذائي الإنتاج المحلي {year}",
}

PLAYWRIGHT_DOMAINS = {
    "ema.gov.sg", "data.gov.sg", "greenplan.gov.sg",
    "doe.gov.ph", "ngcp.ph", "moei.gov.ae", "dewa.gov.ae",
    "aneel.gov.br", "epe.gov.br", "ons.org.br",
    "mnre.gov.in", "powermin.gov.in", "npp.gov.in",
}

# Domains that are NOT acceptable as primary sources for any metric. Social
# media posts (reels, tweets, LinkedIn updates) are too easy to fabricate or
# cherry-pick to survive production-grade scrutiny. Reject at the search-result
# layer so they never reach Claude's synthesis step.
# Match on substring against the URL host (post-www-strip).
BLOCKED_SOURCE_DOMAINS = (
    "facebook.com", "fb.com", "m.facebook.com",
    "twitter.com", "x.com",
    "linkedin.com",
    "instagram.com",
    "tiktok.com",   # included pre-emptively; same class of source
    "reddit.com",   # discussion threads, not authoritative
    "quora.com",
    "pinterest.com",
)

def _is_blocked_source(url: str) -> bool:
    """Return True if `url` is from a non-acceptable production source (social
    media etc.). See BLOCKED_SOURCE_DOMAINS for the list."""
    try:
        host = url.split("/")[2].lower().lstrip("www.")
    except (IndexError, AttributeError):
        return False
    return any(host == d or host.endswith("." + d) for d in BLOCKED_SOURCE_DOMAINS)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

# ── Token tracking ─────────────────────────────────────────────────────────────
_token_usage: dict = {"input": 0, "output": 0, "calls": 0}

def get_token_usage() -> dict:
    return dict(_token_usage)

def _track(resp: dict):
    u = resp.get("usage", {})
    _token_usage["input"]  += u.get("input_tokens", 0)
    _token_usage["output"] += u.get("output_tokens", 0)
    _token_usage["calls"]  += 1


# ── Claude call helper ────────────────────────────────────────────────────────
def _extract_json(text: str) -> dict:
    """Robustly extract the first complete JSON object from Claude's response."""
    # Strip markdown fences
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE).strip()
    # Find the outermost { ... } to handle trailing prose
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in response")
    depth = 0
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return json.loads(text[start:i + 1])
    raise ValueError("Incomplete JSON object in response")


def _claude(messages: list, system: str, tools: list = None,
            max_tokens: int = 1024) -> dict:
    payload = {
        "model":      CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system":     system,
        "messages":   messages,
    }
    if tools:
        payload["tools"] = tools

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json=payload,
        timeout=120,
    )
    r.raise_for_status()
    data = r.json()
    _track(data)
    return data


# ── Web search ────────────────────────────────────────────────────────────────
def _strip_site_operators(query: str) -> str:
    """Remove site: operators which Tavily does not support."""
    return re.sub(r'\bsite:\S+', '', query).strip()


def web_search(query: str, count: int = 3) -> list[dict]:
    """Search via Tavily. Returns list of {url, title, description, content}."""
    clean = _strip_site_operators(query).strip()
    if not clean:
        return []
    if not TAVILY_API_KEY or TAVILY_API_KEY == "your_tavily_key_here":
        raise ValueError("TAVILY_API_KEY not configured")
    from tavily import TavilyClient
    client = TavilyClient(api_key=TAVILY_API_KEY)
    resp = client.search(clean, max_results=count,
                         include_raw_content=False, search_depth="advanced")
    return [
        {
            "url":         r.get("url", ""),
            "title":       r.get("title", ""),
            "description": r.get("content", "") or "",
            "content":     r.get("content", "") or "",
        }
        for r in resp.get("results", [])
    ]


# ── Page fetching ─────────────────────────────────────────────────────────────
def _fetch_pdf_text(url: str) -> str:
    """Download a PDF and extract its text via pdfplumber.
    Returns the text or '' if anything fails."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        from io import BytesIO
        try:
            import pdfplumber
            with pdfplumber.open(BytesIO(r.content)) as pdf:
                # Cap at first ~10 pages — most authoritative numbers are in
                # the executive summary / first tables.
                pages = pdf.pages[:10]
                text = "\n".join(p.extract_text() or "" for p in pages)
                if text and len(text.strip()) > 200:
                    return text.strip()[:8000]
        except ImportError:
            from pypdf import PdfReader
            reader = PdfReader(BytesIO(r.content))
            text = "\n".join((p.extract_text() or "") for p in reader.pages[:10])
            if text and len(text.strip()) > 200:
                return text.strip()[:8000]
    except Exception:
        pass
    return ""


def fetch_page(url: str) -> str:
    """Fetch a URL and return clean text. PDFs go through pdfplumber so the
    agent reads actual table content (not just landing-page snippets).
    HTML uses Trafilatura, falls back to Playwright (JS-heavy sites) or
    BeautifulSoup."""
    # PDF? Download + extract first (Trafilatura's PDF support is unreliable).
    if url.lower().split("?")[0].endswith(".pdf"):
        txt = _fetch_pdf_text(url)
        if txt:
            return txt
        # Fall through to other paths if PDF extraction failed

    domain = url.split("/")[2].lstrip("www.")
    use_playwright = any(d in domain for d in PLAYWRIGHT_DOMAINS)

    # Try Trafilatura's own downloader first (fast path)
    if not use_playwright:
        try:
            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                text = trafilatura.extract(
                    downloaded, include_tables=True, include_links=False,
                    deduplicate=True, favor_recall=True,
                )
                if text and len(text.strip()) > 200:
                    return text.strip()[:5000]
        except Exception:
            pass

    # Playwright for JS-heavy sites
    if use_playwright:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.set_extra_http_headers({"User-Agent": HEADERS["User-Agent"]})
                page.goto(url, wait_until="networkidle", timeout=30000)
                html = page.content()
                browser.close()
            text = trafilatura.extract(html, include_tables=True, favor_recall=True)
            if text and len(text.strip()) > 200:
                return text.strip()[:5000]
        except Exception:
            pass

    # Fallback: requests + BeautifulSoup
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = trafilatura.extract(r.text, include_tables=True, favor_recall=True)
        if text and len(text.strip()) > 200:
            return text.strip()[:5000]
        return soup.get_text(separator="\n", strip=True)[:5000]
    except Exception as e:
        return f"[fetch failed: {e}]"


def fetch_pages_parallel(urls: list[str], max_workers: int = 3) -> dict[str, str]:
    """Fetch multiple pages concurrently."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_page, url): url for url in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                results[url] = future.result()
            except Exception as e:
                results[url] = f"[error: {e}]"
    return results


# ── Date extraction ───────────────────────────────────────────────────────────
def _infer_date(extracted_dstr: str, source_url: str) -> date:
    """
    Smart date extraction — never return a future date, infer from URL if needed.
    """
    today = date.today()

    if extracted_dstr:
        try:
            d = datetime.fromisoformat(extracted_dstr).date()
            if d <= today:
                return d
        except Exception:
            pass

    # Extract year from URL (e.g. NERC_SRA_2024.pdf → 2024).
    # Take the most recent valid year (≤ today, ≥ 2015) rather than the last
    # match — multi-year paths like "/2010/report/2024/energy.pdf" should
    # resolve to 2024, not 2010 (which last-match would return for reversed input).
    years = re.findall(r"20\d{2}", source_url)
    if years:
        valid_years = [int(y) for y in years if 2015 <= int(y) <= today.year]
        if valid_years:
            return date(max(valid_years), 1, 1)

    # Default: previous year (data is rarely current-year)
    return date(today.year - 1, 1, 1)


# ── Research Agent ─────────────────────────────────────────────────────────────
class EnergyResearchAgent:
    """
    Deep research agent that iteratively searches, reflects, and synthesizes
    to find energy metric values. Equivalent to Gemini Deep Research.

    Loop:
      1. Generate 3-5 diverse search queries (English + native-language)
      2. Search via Tavily, fetch top pages with Trafilatura (PDFs via pdfplumber)
      3. Reflect — sufficient data? What's missing?
      4. If not confident: generate follow-up queries, repeat (max 3 rounds)
      5. Synthesize: extract value, convert units, cite source
    """

    MAX_ROUNDS        = 2   # max search rounds
    MAX_RESULTS       = 3   # results per query (conserve quota)
    MAX_PAGES         = 2   # pages to fetch if content missing

    UNIT_HINTS = {
        # Energy (SI1)
        "electricity_price":           "typically 0.05–0.50 USD/kWh; may appear as local currency/MWh or cents/kWh",
        "renewable_share":             "a percentage 0–100%; labeled as % of generation, capacity, or energy mix",
        "grid_capacity":               "total installed generation in GW; may appear as MW — divide by 1000",
        "reserve_margin":              "percentage 10–40%; labeled as reserve margin, adequacy ratio, or capacity surplus",
        "energy_investment":           "USD billions planned over 5 years; may appear in local currency — convert",
        "interconnection_queue_depth": "MW or GW of projects awaiting grid connection; labeled as queue, pipeline, or backlog",
        # Food (SI4)
        "net_food_trade_balance":         "USD value of food exports minus food imports; positive = net exporter; HS chapters 01-24; may appear in local currency or billions/millions — convert to USD; if only one of exports/imports is reported, return that side's signed contribution if interpretable",
        "caloric_self_sufficiency_ratio": "ratio 0.0–1.5+ (or % 0–150); domestic food production calories ÷ food supply calories; FAO Food Balance Sheets are the canonical source; >1.0 means surplus exporter",
        "share_global_staple_exports":    "fraction or percentage 0.00–1.00 (or 0–100%) of world exports for staple commodities (wheat, maize, rice, soy, palm oil, sugar); may be reported per commodity — sum or convert to single basket share",
        "arable_land_per_capita":         "hectares per person; typically 0.001–0.5; arable_land_hectares ÷ population; FAO Land Use ÷ World Bank SP.POP.TOTL",
    }

    def __init__(self, country_iso: str, metric_key: str,
                 country_name: str, currency: str, metric_label: str,
                 metric_unit: str, fx_rates: dict,
                 trusted_urls: list[str] = None):
        self.country_iso   = country_iso
        self.metric_key    = metric_key
        self.country_name  = country_name
        self.currency      = currency
        self.metric_label  = metric_label
        self.metric_unit   = metric_unit
        self.fx_rates      = fx_rates
        self.trusted_urls  = trusted_urls or []
        self.year          = date.today().year
        self.trusted_domains = TRUSTED_SOURCES.get(country_iso, [])
        self.findings: list[dict] = []   # accumulates across rounds
        self.fetched_urls: set    = set()

    def _system_prompt(self) -> str:
        fx_str    = ", ".join(f"1 {k} = {v:.4f} USD" for k, v in self.fx_rates.items())
        site_str  = " OR ".join(f"site:{d}" for d in self.trusted_domains)
        return f"""You are an expert energy data research agent.

Mission: Find the most recent, accurate value for:
  Country:  {self.country_name} ({self.country_iso})
  Metric:   {self.metric_label}
  Unit:     {self.metric_unit}
  Hint:     {self.UNIT_HINTS.get(self.metric_key, "")}
  Year:     {self.year}

Live FX rates: {fx_str}
Local currency: {self.currency}
Trusted sources: {site_str}
Also accept: Reuters, Bloomberg, IEA, World Bank, reputable news with cited data

Conversion rules (apply automatically):
- {self.currency}/MWh → USD/kWh: divide by 1000, multiply by {self.currency}→USD rate
- MW → GW: divide by 1000
- Local currency billions → USD billions: multiply by FX rate
- cents/kWh → USD/kWh: divide by 100
- NEVER return a future date. Use the year from the source document, not today.
- If data is from a 2024 report → data_date is 2024-xx-xx"""

    # ── Fixed query templates per metric ─────────────────────────────────────
    _PRIMARY_QUERIES: dict = {
        "electricity_price": [
            '{country} industrial electricity price {year}',
            '{country} electricity tariff rate kWh {year} site:reuters.com',
            '{country} electricity cost businesses {year} site:bloomberg.com',
            '{country} industrial electricity tariff {year} USD kWh statistics',
        ],
        "renewable_share": [
            '{country} renewable energy share grid {year}',
            '{country} renewable electricity percentage {year} site:reuters.com',
            '{country} clean energy share power generation {year} site:bloomberg.com',
            '{country} renewable energy percentage grid {year} statistics report',
        ],
        "grid_capacity": [
            '{country} total electricity generation capacity GW {year}',
            '{country} installed power capacity gigawatt {year} site:reuters.com',
            '{country} electricity generation capacity {year} site:bloomberg.com',
            '{country} total power plants capacity MW GW {year} report',
        ],
        "reserve_margin": [
            '{country} electricity grid reserve margin {year}',
            '{country} power grid reserve margin capacity {year} site:reuters.com',
            '{country} electricity supply reserve adequacy {year} site:bloomberg.com',
            '{country} peak demand capacity reserve margin {year} percent',
        ],
        "energy_investment": [
            '{country} energy investment billion {year}',
            '{country} energy sector investment billion dollars {year} site:reuters.com',
            '{country} energy infrastructure investment plan {year} site:bloomberg.com',
            '{country} power sector investment spending billion {year}',
        ],
        "interconnection_queue_depth": [
            '{country} grid interconnection queue MW {year}',
            '{country} power grid connection queue megawatt {year} site:reuters.com',
            '{country} grid interconnection backlog projects {year} site:bloomberg.com',
            '{country} grid connection applications pending MW {year}',
        ],

        # ═══ FOOD (SI4) ════════════════════════════════════════════════════════
        "net_food_trade_balance": [
            '{country} agricultural trade balance USD {year}',
            '{country} food exports imports USD billion {year} site:fas.usda.gov',
            '{country} food trade balance {year} site:reuters.com',
            '{country} agricultural exports imports {year} site:fao.org',
        ],
        "caloric_self_sufficiency_ratio": [
            '{country} caloric self-sufficiency ratio {year}',
            '{country} food self-sufficiency percentage domestic production {year}',
            '{country} food balance sheet kcal supply production {year} site:fao.org',
            '{country} food self sufficiency index {year} agriculture',
        ],
        "share_global_staple_exports": [
            '{country} share global staple exports wheat maize rice soybean {year}',
            '{country} agricultural commodity export volume tonnes {year} site:fao.org',
            '{country} grain oilseed exports global market share {year}',
            '{country} cereals exports world market share {year} statistics',
        ],
        "arable_land_per_capita": [
            '{country} arable land per capita hectares {year}',
            '{country} arable land area hectares {year} site:fao.org',
            '{country} agricultural land per person {year} site:worldbank.org',
            '{country} cropland hectares per capita {year}',
        ],
    }

    # NOTE: an earlier version had a hardcoded _QUERY_ENRICHMENTS dict that
    # appended specific report names ("DEWA integrated report", "Plano
    # Decenal") to Round 1 queries. It was removed because:
    #   (a) it made Round 1 queries long + narrow, often returning fewer
    #       relevant Tavily results than the broader generic query;
    #   (b) it pre-decided which document is authoritative, which the user
    #       explicitly rejected for production — the agent should find the
    #       best source dynamically, not via hardcoded report names.
    # Progressive refinement now happens organically:
    #   Round 1 → generic broad query + native-language + govt-site:filter
    #   Round 2 → Claude reflects on gaps and generates targeted follow-ups
    #   (Round 2 generation already lives in _generate_queries below).

    # ── SI3 mineral query templates ─────────────────────────────────────────
    # Keyed by base metric (without mineral suffix). Used when metric_key
    # matches the SI3 pattern "{base}_{mineral}" — e.g. production_share_copper,
    # refining_share_lithium. {mineral} is filled with the human-readable mineral
    # name (Copper / Lithium / Nickel / Cobalt / Rare Earths / Silicon).
    _MINERAL_QUERY_TEMPLATES: dict = {
        "production_share": [
            '{country} {mineral} mine production tonnes {year} site:usgs.gov',
            '{country} {mineral} production statistics {year} site:bgs.ac.uk',
            '{country} {mineral} mine output {year} world-mining-data.info',
            'world {mineral} production {country} share {year}',
        ],
        "reserves_share": [
            '{country} {mineral} reserves tonnes {year} site:usgs.gov',
            '{country} {mineral} mineral reserves estimate {year} site:bgs.ac.uk',
            '{country} {mineral} proven reserves {year} report',
            'global {mineral} reserves country share {year}',
        ],
        "refining_share": [
            '{country} {mineral} refinery production capacity {year}',
            '{country} refined {mineral} output tonnes {year} site:usgs.gov',
            '{country} {mineral} smelter refinery {year} site:icsg.org',
            '{country} {mineral} processing capacity {year}',
        ],
        "yoy_growth": [
            '{country} {mineral} production growth year over year {year}',
            '{country} {mineral} mine output change {year} vs prior year',
            '{country} {mineral} production trend {year} site:usgs.gov',
        ],
        "value_add_ratio": [
            '{country} {mineral} processed exports vs raw exports {year}',
            '{country} {mineral} value added share exports {year}',
            '{country} {mineral} downstream processing exports {year}',
        ],
    }

    _SI3_MINERALS = {
        "copper": "copper", "lithium": "lithium", "nickel": "nickel",
        "cobalt": "cobalt", "rare_earths": "rare earths", "silicon": "silicon",
    }

    @classmethod
    def _resolve_mineral_template(cls, metric_key: str):
        """If metric_key looks like '{base}_{mineral}' (SI3), return the mineral
        templates with the mineral name substituted. Else return None."""
        for slug, display in cls._SI3_MINERALS.items():
            suffix = f"_{slug}"
            if metric_key.endswith(suffix):
                base = metric_key[:-len(suffix)]
                tmpls = cls._MINERAL_QUERY_TEMPLATES.get(base)
                if tmpls:
                    return [t.replace("{mineral}", display) for t in tmpls]
        return None

    # ── Phase 1: Generate search queries ─────────────────────────────────────
    def _generate_queries(self, round_num: int = 1, context: str = "") -> list[str]:
        """
        Round 1: English query + (if applicable) a native-language query.
        Round 2+: Claude generates 2 gap-filling queries based on what's missing.
        """
        c = self.country_name
        y = self.year
        templates = self._PRIMARY_QUERIES.get(self.metric_key)
        if not templates:
            templates = self._resolve_mineral_template(self.metric_key)
        if not templates:
            templates = [f'{self.metric_label} {c} {y}']
        native_tmpl = NATIVE_QUERY_TEMPLATES.get((self.country_iso, self.metric_key))

        if round_num == 1:
            # Broad generic query first — cast wide net. Tavily ranks results;
            # adding narrow document-specific keywords here just reduces hits.
            # Targeted follow-up queries (with specific publication names if
            # appropriate) are generated by Claude in round 2 based on what
            # was actually missing, not hardcoded ahead of time.
            queries = [templates[0].format(country=c, year=y)]
            # For non-English authoritative-source countries, also issue one
            # native-language query so the agent can retrieve local-language
            # regulator docs (e.g. Portuguese "margem de reserva" PDFs)
            if native_tmpl:
                queries.append(native_tmpl.format(country=c, year=y))
            # CRITICAL: bias toward the country's canonical regulators. The
            # fixed templates use site:reuters.com / site:bloomberg.com which
            # surfaces press articles, not government statistics. Build a
            # site:-filter from the country's first 3 trusted (govt) domains
            # so we directly hit, e.g., doe.gov.ph for PH reserve_margin.
            govt_domains = [d for d in (self.trusted_domains or [])
                            if d.endswith((".gov", ".gov.ph", ".gov.ae",
                                           ".gov.br", ".gov.in", ".gov.sg",
                                           ".gov.uk", ".nic.in", ".org.br"))
                                or any(d.endswith(s) for s in (
                                       "gov.ph", "gov.ae", "gov.br", "gov.in",
                                       "gov.sg", "nic.in", "org.br", ".gov"))][:5]
            if govt_domains:
                site_filter = " OR ".join(f"site:{d}" for d in govt_domains)
                queries.append(f"{self.metric_label} {c} {y} ({site_filter})")
            return queries

        # Round 2: progressive refinement. The _reflect step (called between
        # rounds in run()) already generates Claude-driven follow_up_queries
        # from the actual gap and fetches them. Here we only contribute the
        # remaining hardcoded press templates as a low-priority backstop.
        return [t.format(country=c, year=y) for t in templates[1:3]]

    # ── Phase 2: Search and fetch ─────────────────────────────────────────────
    def _search_and_fetch(self, queries: list[str]) -> list[dict]:
        """
        Search all queries via Tavily. Tavily returns snippets + URLs; we fetch
        the top pages with Trafilatura (or pdfplumber for .pdf URLs) to get the
        actual table text for Claude synthesis.
        """
        all_results = []
        seen_urls   = set()
        blocked     = 0

        # Always fetch trusted_urls directly (highest priority).
        # Trusted URLs are configured by hand, but still defensive-filter.
        if self.trusted_urls:
            allowed = [u for u in self.trusted_urls
                       if u not in self.fetched_urls and not _is_blocked_source(u)]
            page_texts = fetch_pages_parallel(allowed)
            self.fetched_urls.update(self.trusted_urls)
            for url in self.trusted_urls:
                if _is_blocked_source(url):
                    blocked += 1
                    continue
                text = page_texts.get(url, "")
                if text and len(text) > 50:
                    all_results.append({
                        "url": url, "title": "Priority URL",
                        "text": text[:3000], "description": "",
                    })
                    seen_urls.add(url)

        # Search each query
        for query in queries:
            try:
                results = web_search(query, count=self.MAX_RESULTS)
                for r in results:
                    if r["url"] in seen_urls:
                        continue
                    if _is_blocked_source(r["url"]):
                        blocked += 1
                        continue
                    seen_urls.add(r["url"])
                    # Tavily returns "content" — use it as the snippet when present
                    content = r.get("content", "").strip()
                    if content and len(content) > 100:
                        all_results.append({
                            "url":         r["url"],
                            "title":       r.get("title", ""),
                            "text":        content[:3000],
                            "description": r.get("description", ""),
                        })
                    elif r.get("description"):
                        # Fallback to snippet
                        all_results.append({
                            "url":         r["url"],
                            "title":       r.get("title", ""),
                            "text":        r["description"],
                            "description": r["description"],
                        })
            except Exception as e:
                print(f"    [search error] {query}: {e}")

        if blocked:
            print(f"    [source filter] dropped {blocked} blocked-domain results "
                  f"(social media / non-authoritative)")
        return all_results

    # ── Phase 3: Reflect ──────────────────────────────────────────────────────
    def _reflect(self) -> dict:
        """
        Ask Claude to evaluate all findings and decide:
        - Is there enough data to extract the metric?
        - What's missing / unclear?
        - What follow-up queries would help?
        """
        summary = ""
        for i, f in enumerate(self.findings, 1):
            text = f.get("text") or f.get("description") or ""
            summary += f"\n--- Source {i}: {f['url']} ---\n{str(text)[:1500]}\n"

        sources_seen = list({f.get("url","").split("/")[2]
                             for f in self.findings if f.get("url")})[:8]

        # Recomputation hints — when the published metric isn't available
        # directly, tell Claude what underlying quantities to search for
        # so it can compute the metric from components.
        recompute_hints = {
            "reserve_margin": (
                "If no direct reserve-margin % is published, search for the two "
                "components SEPARATELY: installed/firm generation capacity (MW or GW) "
                "AND annual peak demand (MW or GW). The reserve margin is "
                "(firm_capacity - peak) / peak × 100. Components are usually in "
                "the national grid operator's annual report even when the % isn't."
            ),
            "yoy_growth": (
                "If yoy_growth % isn't directly published, look for the metric's "
                "raw value in two consecutive years and compute (curr - prior) / prior."
            ),
            "share_global_staple_exports": (
                "If global-share % isn't published, look for the country's exports "
                "AND the world total for the same basket and year, then divide."
            ),
            "value_add_ratio": (
                "If processed/(raw+processed) isn't directly published, look for "
                "processed exports AND raw exports of the same commodity, then compute."
            ),
        }
        recompute_hint = recompute_hints.get(self.metric_key, "")

        prompt = f"""You are evaluating research findings to extract: {self.metric_label} for {self.country_name} in {self.metric_unit}.

Here are all findings gathered so far:
{summary}

Domains we already fetched: {", ".join(sources_seen) if sources_seen else "none"}

{f"Recomputation hint: {recompute_hint}" if recompute_hint else ""}

Evaluate and respond with ONLY valid JSON:
{{
  "has_answer":       <true if you can extract a confident numeric value from the gathered findings>,
  "confidence":       "<high|medium|low>",
  "best_value_found": <float or null>,
  "best_source_url":  "<url where best value was found, or null>",
  "data_date_found":  "<YYYY-MM-DD or null — date FROM the source, not today>",
  "what_is_missing":  "<concrete description of what data is still needed — e.g. 'no breakdown by sector', 'only historical 2020 data', 'source is press release citing unknown primary'>",
  "follow_up_queries": ["<query 1>", "<query 2>"]
}}

Rules for follow_up_queries (only fill if has_answer is false or confidence is low):
  • Each query: 5-12 words, no site:reuters.com / site:bloomberg.com (those
    are press, not primary sources)
  • Target a DIFFERENT angle than what's already been tried:
    - Primary regulator/agency by name (NERC, EIA, IEA, DOE, ANEEL, CEA,
      EMA, DEWA, etc.) — pick one specifically credible for {self.country_name}
    - National 5-year plan / official strategy document
    - Try filetype:pdf to find report PDFs
    - For non-English countries, ONE query in the local language
  • Don't repeat domains we already fetched
  • If a "Recomputation hint" was provided above and direct numbers weren't
    found, generate queries for the UNDERLYING COMPONENTS instead — e.g.
    "{self.country_name} installed power capacity GW {self.year}" AND
    "{self.country_name} peak electricity demand GW {self.year}". Synthesis
    will compute the final metric from them."""

        data = _claude(
            [{"role": "user", "content": prompt}],
            system=self._system_prompt(),
            max_tokens=800,  # bumped — JSON with 2 follow-up queries + reason was truncating at 400
        )
        try:
            return _extract_json(data["content"][0]["text"])
        except Exception:
            return {"has_answer": False, "confidence": "low", "follow_up_queries": []}

    # ── Phase 4: Synthesize ───────────────────────────────────────────────────
    def _synthesize(self) -> dict:
        """Final extraction: find the value, convert units, cite source."""
        all_text = ""
        for f in self.findings:
            text = f.get("text") or f.get("description") or ""
            if text:
                all_text += f"\n=== {f['url']} ===\n{str(text)[:2000]}\n"

        fx_str = ", ".join(f"1 {k} = {v:.4f} USD" for k, v in self.fx_rates.items())

        prompt = f"""From all gathered research, extract the final answer for:
Country: {self.country_name} ({self.country_iso})
Metric: {self.metric_label}
Output unit: {self.metric_unit}
Hint: {self.UNIT_HINTS.get(self.metric_key, "")}
FX rates: {fx_str}

CRITICAL RULES — every value MUST have solid backing:
0. SOLID BACKING RULE — non-negotiable:
   • The value MUST appear LITERALLY in one of the source documents above,
     or be a direct deterministic calculation from numbers that literally
     appear in those documents.
   • raw_text MUST be a verbatim quote from the chosen source that contains
     the value (or the components used to compute it). Not paraphrased.
   • If you cannot find a source that LITERALLY states the value (or its
     direct components), return value: null. NEVER ESTIMATE, GUESS, or
     INTERPOLATE — return null and a clear what_is_missing reason.
   • Single source preferred. If aggregating multiple sources, the
     conversion_note MUST list every source URL used and what each
     contributed (e.g. "capacity from URL_A=51 GW; peak from URL_B=31 GW").
1. Use the most recent data found (prefer {self.year}, then {self.year-1})
2. Convert to {self.metric_unit} using FX rates if needed
3. data_date MUST come from the SOURCE document — never use today's date
4. If source is a 2024 PDF → data_date = "2024-01-01"
5. SOURCE AUTHORITY RANKING — prefer higher tiers, even at the cost of slight
   recency loss. When 2+ sources disagree, take the higher-tier source unless
   it is materially older (>2 years). When same tier, take the most recent.
     TIER 1 (highest authority — use whenever available):
       - National statistics agency for the country (USGS, EIA, ANEEL, ONS,
         EPE, CEA, EMA, DEWA, Abu Dhabi DoE, DOE Philippines, NEDA, MNRE,
         MoP India, etc.)
       - National central bank / treasury for currency / investment data
     TIER 2 (international authoritative):
       - IGOs: IEA, IRENA, World Bank, UN Comtrade, FAOSTAT, OECD
       - WRI, IRENA, USGS, FAO when not the country's own agency
     TIER 3 (industry / analyst):
       - BloombergNEF, S&P Global Platts, Wood Mackenzie (ACTUAL data only —
         NEVER their "required investment for net-zero" projections, those
         are aspirational)
       - JORC-compliant reserves reports (mining)
     TIER 4 (acceptable corroboration only):
       - Major financial press: FT, Reuters, Bloomberg, WSJ — only when
         they explicitly cite a Tier 1-3 source
     REJECT:
       - Social media (facebook, twitter/x, linkedin, instagram, tiktok,
         reddit, quora, pinterest)
       - Aggregator sites that don't cite their source (indexmundi,
         statranker, worldpopulationreview, etc.)
       - Think-tank "required investment" / "needed to reach 2050"
         projections — these are NOT actual plans, NOT actual investments
6. RECENCY FALLBACK: if {self.year} data isn't in the sources, use the most
   recent year that IS available. Don't return null just because the current
   year is missing — a 2023 value from the national regulator is more useful
   than no value. Set data_date to the actual year you used.
7. For reserve_margin: THIS IS THE PLANNING RESERVE MARGIN — an annual
   metric, NOT an operating/real-time reserve. The formula is:
       reserve_margin (%) = ((installed_capacity_MW - annual_peak_demand_MW)
                              / annual_peak_demand_MW) * 100
   CRITICAL distinctions — REJECT these as substitutes:
     • "Operating margin" / "operating reserve" — real-time spare
       capacity (spinning + non-spinning + contingency reserves), typically
       5-15% of current load. NOT the planning metric. If a source reports
       these in MW (regulating reserves, contingency reserve, dispatchable
       reserve), they are operating reserves — do NOT divide them by demand
       and label as reserve_margin.
     • "RRM" / "Required Reserve Margin" — regulatory minimum target, not
       actual margin.
   If no direct planning reserve margin % is found, COMPUTE it from
   installed capacity AND annual peak demand across ALL gathered sources
   (they don't have to be in the same article). Prefer firm/dependable
   capacity (NOT nameplate); if only nameplate is available, use it and
   flag as approximate. Reject any final value > 60% or < 0% (industry
   planning-reserve range is 10-40%; outside that means the underlying
   numbers are wrong or you've confused metrics).
7. For energy_investment (5-year planned investment):
   - Prefer a single authoritative aggregate from national 5-year energy plan,
     IEA WEI, IRENA Investment Trends, or major MDB disclosure.
   - If aggregating multiple announced sources, DEDUPLICATE: do not
     double-count the same announced project across two reports. Pick the
     larger figure when one source restates another's announcement.
   - Reject WB PPI alone (private-only, misses public spending).
   - Reject single press releases or single-fund announcements as the total.
   - Plausibility band: $1B–$2T over 5 years for these 6 economies.

All research:
{all_text[:8000]}

Respond ONLY with valid JSON:
{{
  "value": <float>,
  "data_date": "<YYYY-MM-DD — from source, not today>",
  "frequency": "<monthly|quarterly|annual|irregular>",
  "source_url": "<best source URL>",
  "raw_text": "<exact snippet containing the number>",
  "conversion_note": "<conversion applied or 'none'>",
  "confidence": "<high|medium|low>"
}}"""

        data = _claude(
            [{"role": "user", "content": prompt}],
            system=self._system_prompt(),
            # Bumped from 512 → 2048: the expanded authority-ranking prompt
            # invites Claude to write longer raw_text quotes from the source,
            # and 512 was truncating mid-JSON ("Incomplete JSON object"
            # errors on AE/PH cells).
            max_tokens=2048,
        )
        result = _extract_json(data["content"][0]["text"])

        if result.get("value") is None:
            raise ValueError(f"Synthesis returned null value for {self.metric_key}/{self.country_iso}")

        # SOLID BACKING VALIDATION — confirm the raw_text actually contains
        # the value (or its components for derived metrics). Rejects "vibes"
        # synthesis where Claude estimated a number not literally in any
        # source. Gap is honest; an unverifiable number isn't.
        value     = result.get("value")
        raw_text  = (result.get("raw_text") or "").strip()
        conv_note = (result.get("conversion_note") or "").strip()
        if not raw_text or len(raw_text) < 20:
            raise ValueError(
                f"Synthesis for {self.metric_key}/{self.country_iso}: raw_text "
                f"too short to back the value (got {len(raw_text)} chars). "
                f"Production requires a verifiable source quote."
            )
        if not self._raw_text_supports_value(value, raw_text, conv_note):
            raise ValueError(
                f"Synthesis for {self.metric_key}/{self.country_iso}: value "
                f"{value} not found literally in raw_text or conversion_note "
                f"— production policy rejects unbacked numbers. "
                f"raw_text excerpt: {raw_text[:200]}"
            )
        if not result.get("source_url"):
            raise ValueError(
                f"Synthesis for {self.metric_key}/{self.country_iso}: "
                f"source_url is empty — every value needs a citable source."
            )

        # Validate and fix date
        result["data_date"] = _infer_date(
            result.get("data_date"),
            result.get("source_url", ""),
        ).isoformat()

        return result

    @staticmethod
    def _raw_text_supports_value(value, raw_text: str, conv_note: str = "") -> bool:
        """True if `value` (or a near-decimal variant) appears literally in
        raw_text or in the conversion_note. Handles common formats:
        commas, decimals, scientific notation, % signs, multiplier suffixes
        (k/M/B/bn/billion/million).
        """
        if value is None:
            return False
        haystack = (raw_text + " " + conv_note).lower()
        try:
            v = float(value)
        except (TypeError, ValueError):
            return False
        # Try the value itself, scaled variants, and rounded forms.
        candidates = set()
        for scale, suffixes in [
            (1,           ["", " "]),
            (1e3,         ["", "k", "thousand"]),
            (1e6,         ["", "m", "million", "mn"]),
            (1e9,         ["", "b", "bn", "billion"]),
            (1e12,        ["t", "trillion"]),
        ]:
            scaled = v / scale
            # only check scales producing reasonably-sized numbers
            if not (0.001 <= abs(scaled) <= 1e7):
                continue
            for n_dec in (0, 1, 2, 3, 4):
                s = f"{scaled:,.{n_dec}f}".rstrip("0").rstrip(".")
                if s:
                    candidates.add(s)
                    candidates.add(s.replace(",", ""))
        for c in candidates:
            if c in haystack:
                return True
        return False

    # ── Main entry point ──────────────────────────────────────────────────────
    def run(self) -> dict:
        """
        Execute the full deep research loop:
        Generate → Search → Reflect → [loop] → Synthesize
        """
        context = ""
        for round_num in range(1, self.MAX_ROUNDS + 1):
            print(f"    [Agent round {round_num}/{self.MAX_ROUNDS}] searching...")

            # Generate queries (round 1 = single best shot; round 2+ = gap fill)
            queries = self._generate_queries(round_num=round_num, context=context)
            print(f"    [Agent] queries: {queries}...")

            # Search and fetch
            new_findings = self._search_and_fetch(queries)
            self.findings.extend(new_findings)
            print(f"    [Agent] {len(new_findings)} sources gathered (total: {len(self.findings)})")

            if not self.findings:
                raise ValueError("No sources found after search")

            # Reflect
            reflection = self._reflect()
            print(f"    [Agent] reflection: has_answer={reflection.get('has_answer')}, "
                  f"confidence={reflection.get('confidence')}")

            if reflection.get("has_answer") and reflection.get("confidence") in ("high", "medium"):
                break  # Confident enough — synthesize

            # Update context for next round
            context = reflection.get("what_is_missing", "")
            follow_ups = reflection.get("follow_up_queries", [])
            if follow_ups and round_num < self.MAX_ROUNDS:
                # Fetch follow-up pages immediately
                extra_findings = self._search_and_fetch(follow_ups)
                self.findings.extend(extra_findings)

        # Synthesize final answer
        print(f"    [Agent] synthesizing from {len(self.findings)} sources...")
        return self._synthesize()


# ── Public interface (called from 02_pipeline.py) ────────────────────────────
def run_research_agent(
    country_iso:   str,
    metric_key:    str,
    country_name:  str,
    currency:      str,
    metric_label:  str,
    metric_unit:   str,
    fx_rates:      dict,
    trusted_urls:  list[str] = None,
) -> dict:
    """
    Entry point for the pipeline. Returns a result dict with:
    value, data_date, frequency, source_url, raw_text, conversion_note
    """
    agent = EnergyResearchAgent(
        country_iso=country_iso,
        metric_key=metric_key,
        country_name=country_name,
        currency=currency,
        metric_label=metric_label,
        metric_unit=metric_unit,
        fx_rates=fx_rates,
        trusted_urls=trusted_urls,
    )
    return agent.run()
