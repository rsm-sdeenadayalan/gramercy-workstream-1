# SI4 Pipeline — Metric Methodology (Updated)

---

## Metric 1: Net Food Trade Balance

### Definition
USD value of food exports minus food imports for a given period.
Positive = net food exporter. Negative = net food importer.

### Calculation
```
Trade Balance (USD) = Food Exports (USD) − Food Imports (USD)
```

### Commodity Scope
UN Comtrade HS chapters 01–24 (live animals, meat, fish, dairy, eggs, honey,
vegetables, fruits, cereals, milling products, oilseeds, fats & oils, sugar,
cocoa, preparations, beverages, tobacco).

---

### United States

| Tier | Source | Method | Frequency | Confidence |
|---|---|---|---|---|
| 1 — primary | USDA ERS FATUS | Scrape .xlsx from ERS website; parse "Calendar Year" section; iterate ALL monthly columns (not just latest); multiply by 1e9 (values in billions USD) | **Monthly** | 0.75 |

**Historical approach:** `_fatus_all_months` parses every column in the Excel sheet —
the same file that provides the latest month also contains all historical months back
to ~2004. No separate API call needed.

**Last snapshot:** `2026-02-01` (most recent monthly release)
**Historical coverage:** Monthly from `2020-01` to present

---

### Brazil

| Tier | Source | Method | Frequency | Confidence |
|---|---|---|---|---|
| 1 — primary | UN Comtrade C/M/HS | `_hist_trade_monthly`: loop year-by-year (Jan–Jun, Jul–Dec per call to stay under 500-record API limit); reporter=76; HS 01–24; flowCode=X,M; 1.2s delay between calls | **Monthly** | 1.00 |
| 2 — quarterly | UN Comtrade C/Q/HS | `_hist_trade_quarterly`: 8-quarter chunks; same reporter/commodity scope. Activates only if monthly returns empty for a chunk | **Quarterly** | 0.92 |
| 3 — fallback | UN Comtrade C/A/HS | Annual total if both monthly and quarterly calls return empty | Annual | 0.88 |

> **Quarterly note:** Brazil does submit quarterly data to Comtrade public preview. The quarterly tier is the most likely to activate here — BR monthly can fail due to the public API rate limit, and quarterly provides ~3-month granularity as a meaningful middle step.

**Last snapshot:** `2026-03-01`
**Historical coverage:** Monthly from `2020-01` to present (where Comtrade has data); quarterly fills gaps

---

### Philippines

| Tier | Source | Method | Frequency | Confidence |
|---|---|---|---|---|
| 1 — primary | UN Comtrade C/M/HS | Same as Brazil; reporter=608 | **Monthly** | 1.00 |
| 2 — quarterly | UN Comtrade C/Q/HS | 8-quarter chunks; same scope | **Quarterly** | 0.92 |
| 3 — fallback | UN Comtrade C/A/HS | Annual fallback | Annual | 0.88 |

> **Quarterly note:** Philippines monthly is generally reliable; quarterly is a rarely-triggered safety net.

**Last snapshot:** `2025-12-01`
**Historical coverage:** Monthly from `2020-01` to present

---

### India

| Tier | Source | Method | Frequency | Confidence |
|---|---|---|---|---|
| 1 — monthly (scrape) | Ministry of Commerce press releases + PIB.gov.in via Tavily+Jina | Tavily searches for monthly press notes; Jina reads the page; regex extracts total exports/imports in USD billions; scales by WB food-trade % (TX.VAL.FOOD.ZS.UN / TM.VAL.FOOD.ZS.UN for IND) | **Monthly** (last ~2 yrs) | 0.60 |
| 2 — quarterly | UN Comtrade C/Q/HS | `_hist_trade_quarterly`; reporter=699; 8-quarter chunks | **Quarterly** | 0.92 |
| 3 — fallback | World Bank API | TX.VAL.MRCH.CD.WT × TX.VAL.FOOD.ZS.UN/100 for exports; TM equivalent for imports | Annual | 0.88 |

> **Requires:** `JINA_API_KEY` and `TAVILY_API_KEY` set in `.env`. If keys are absent, tier-1 is skipped.
>
> **Quarterly caveat:** India (reporter=699) does not submit monthly HS data to the Comtrade **public** preview. Whether it submits quarterly depends on Comtrade's data-sharing agreement with India. In practice, the quarterly tier is likely to also return empty, falling through to the World Bank annual. A **paid Comtrade subscription key** would unlock both monthly and quarterly HS data for India.

**Last snapshot:** World Bank annual, `2022–2023`
**Historical coverage:** Scrape tier covers last ~2 years (monthly); quarterly attempted but likely falls through; WB annual covers 2020–2023

#### Bottleneck — India monthly
India's official monthly food trade data exists in **TRADESTAT** (`tradestat.commerce.gov.in`)
and **DGCIS** (`ftddp.dgciskol.gov.in`). Both portals have HS-chapter-level monthly data
going back to 2018. However:
- Neither portal has a public REST API
- Data is locked behind a web UI with session-based queries
- Export requires manual selection + download (Excel/PDF)
- The portals require government/institutional login for bulk downloads

The scrape tier (`_scrape_india_trade_monthly`) is a **partial workaround**: it finds
Ministry of Commerce press releases which report total merchandise trade (not food-specific),
then scales by the World Bank food-trade percentage. This introduces estimation error
(confidence 0.60). True monthly food trade at HS-chapter granularity would require
either a paid TRADESTAT data license or a Comtrade subscription API key (reporter=699
monthly data exists in the subscription tier but not the public preview).

---

### Singapore

| Tier | Source | Method | Frequency | Confidence |
|---|---|---|---|---|
| 1 — monthly | UN Comtrade C/M/HS | reporter=702 | Monthly | 1.00 |
| 2 — quarterly | UN Comtrade C/Q/HS | reporter=702; 8-quarter chunks | Quarterly | 0.92 |
| 3 — primary (effective) | UN Comtrade C/A/HS | reporter=702; annual; last 3 years | **Annual** | 0.88 |
| 4 — fallback | World Bank API | Same formula as India | Annual | 0.88 |

> **Quarterly/monthly caveat:** Singapore (reporter=702) is an **annual-only reporter** in the Comtrade public preview. Both the monthly (C/M/HS) and quarterly (C/Q/HS) endpoints return empty for SG. The cascade falls through to annual Comtrade every time. Tiers 1–2 are in the code as speculative fallbacks in case Comtrade ever adds SG sub-annual data, but they are not expected to succeed with the current public API.

**Last snapshot:** Comtrade annual, `2024-01-01`
**Historical coverage:** Annual 2020–2024 (quarterly/monthly not available in public Comtrade)

#### Bottleneck — Singapore monthly
Three sources were investigated:
1. **SingStat TableBuilder** (`tablebuilder.singstat.gov.sg`) — confirmed annual-only for
   merchandise trade by HS 2-digit. The monthly time-series tables cover total trade value
   but not broken down by HS chapter. No monthly HS-level food data available.
2. **UN Comtrade public API** — reporter=702 returns no data on the monthly endpoint
   (`C/M/HS`). Singapore does not submit monthly HS data to the public Comtrade preview;
   it appears only in the subscription tier.
3. **Singapore Customs / Enterprise Singapore** — publish aggregate monthly trade statistics
   but not at HS-chapter level.

**No accessible monthly food-specific trade source exists for Singapore in the public domain.**
Annual Comtrade data is the best available without a paid data subscription.

---

### UAE

| Tier | Source | Method | Frequency | Confidence |
|---|---|---|---|---|
| 1 — monthly (scrape) | UAE statistics portals via Tavily+Jina | Tavily searches FCSC/UAE stats sites; Jina reads page; regex extracts trade figures; scaled by WB food % for ARE | Annual (year-level estimates) | 0.60 |
| 2 — quarterly | UN Comtrade C/Q/HS | reporter=784; 8-quarter chunks | Quarterly | 0.92 |
| 3 — fallback | UN Comtrade C/A/HS | reporter=784; annual | Annual | 0.88 |
| 4 — fallback | World Bank API | TX/TM.VAL formula | Annual | 0.88 |

> **Requires:** `JINA_API_KEY` and `TAVILY_API_KEY` set in `.env`.
>
> **Quarterly caveat:** UAE (reporter=784) is an **annual-only reporter** in the Comtrade public preview — same constraint as Singapore. The quarterly tier (tier 2) will return empty and fall through to Comtrade annual. It is included as a speculative future-proof step only. The Tavily+Jina scrape (tier 1) extracts year-level aggregate figures, not true monthly data.

**Last snapshot:** Comtrade annual, `2023-01-01`
**Historical coverage:** Annual 2020–2023 (quarterly not available in public Comtrade)

#### Bottleneck — UAE monthly
UAE is the hardest country in this dataset for trade data:
1. **UN Comtrade public API** — reporter=784 returns no data on monthly endpoint.
   UAE does not submit monthly HS data to the Comtrade public preview.
2. **UAE FCSC** (`fcsc.gov.ae`, `opendata.fcsc.gov.ae`) — the official statistics authority.
   Portal returned HTTP 403 on all tested endpoints. API documentation is not publicly
   accessible. Data portals appear to require institutional registration.
3. **Dubai Customs** — publishes aggregate trade data but not at food HS-chapter granularity
   in a machine-readable format.
4. **IMF Direction of Trade Statistics (DOTS)** — has bilateral monthly trade but only
   total merchandise, not food-specific.

The scrape tier is limited to year-level estimates from whatever statistics pages
Tavily can find, scaled by WB food-trade %. Monthly granularity for UAE food trade
is **not achievable without a paid Comtrade API subscription or direct access to FCSC**.

---
---

## Metric 2: Caloric Self-Sufficiency Ratio (CSR)

### Definition
Fraction of a country's food caloric supply that is domestically produced.
> 1.0 = caloric surplus (exports more food energy than consumed).
< 1.0 = caloric deficit (depends on imports).

### Calculation
```
For each food item i:
  production_kcal[i] = food_supply_kcal_per_capita[i]
                       × (production_qty[i] / domestic_supply_qty[i])

CSR = Σ production_kcal[i]  /  Σ food_supply_kcal_per_capita[i]
        (all leaf items)           (Grand Total from FBS)
```
Only leaf-level FBS items are used (not aggregate groups) to avoid double-counting.
Items with zero domestic supply are excluded.

### Source: FAOSTAT Food Balance Sheets (FBS)
- Bulk ZIP: `bulks-faostat.fao.org/production/FoodBalanceSheets_E_All_Data_(Normalized).zip`
- Cached: `fao_cache/FBS_normalized.csv` (~633 MB)
- **Frequency: Annual (all countries)**
- **Coverage: 2020–2023** (FAO publishes FBS with ~1–2 year lag)

### All countries (same method)
| Country | Source | Frequency | Last date |
|---|---|---|---|
| US, BR, IN, PH, AE | FAOSTAT FBS | Annual | 2023-01-01 |
| SG | World Bank AG.PRD.FOOD.XD proxy: `CSR = (FPI/100) × 0.30` | Annual | 2022-01-01 |

> Singapore has no domestic food production entries in FBS. The 0.30 scalar reflects
> the city-state's estimated maximum self-sufficiency ceiling. Confidence = 0.30 (imputed).

#### Bottleneck — CSR monthly (all countries)
The Food Balance Sheet is a **derived, modelled dataset** published once per year.
It is constructed from multiple sub-surveys (production statistics, trade statistics,
utilization surveys, waste estimates) that are themselves annual.

There is **no monthly equivalent** for food balance sheets anywhere in the world:
- FAO does not publish monthly FBS
- No national statistical agency publishes a monthly food-kcal balance sheet
- USDA PSD (Production, Supply & Distribution) has monthly forecasts but only for
  selected commodities (grains, oilseeds), not a full caloric balance
- Deriving a monthly CSR from sub-component data would require monthly production,
  monthly waste, and monthly utilization surveys — none of which are publicly available
  at country level

**CSR will remain annual.** This is a fundamental constraint of the metric, not a data gap.

---
---

## Metric 3: Share of Global Staple Exports

### Definition
Country's monthly export volume of key staple commodities as a fraction of
total world monthly exports of the same basket.

### Staple Basket (HS4 codes)
| HS Code | Commodity |
|---|---|
| 1001 | Wheat | 1003 | Barley |
| 1005 | Maize (corn) | 1006 | Rice |
| 1201 | Soybeans | 1205 | Rapeseed / Canola |
| 1206 | Sunflower seed | 1507 | Soybean oil |
| 1511 | Palm oil | 1701 | Raw sugar |

### Calculation
```
share = country_export_qty (tonnes, monthly equivalent)
        ─────────────────────────────────────────────────────────
        world_annual_export_qty (FAOSTAT TCL, 1000t × 1000) ÷ 12
```
When annual country data is used, it is also ÷ 12 before dividing (consistent units).

### World Denominator
FAOSTAT TCL (Trade Crops & Livestock) bulk download — cached as `fao_cache/TCL_normalized.csv` (~2.4 GB).
Denominator year is matched to closest available year ≤ data_date.
**This denominator is always annual** — no monthly world total exists in FAOSTAT TCL.

---

### United States, Brazil, Philippines

| Tier | Source | Method | Frequency | Confidence |
|---|---|---|---|---|
| 1 — primary | UN Comtrade C/M/HS | `_hist_export_share_monthly`: loop year-by-year, query HS4 basket codes, sum export qty by period | **Monthly** | 1.00 |
| 2 — quarterly | UN Comtrade C/Q/HS | `_hist_export_share_quarterly`: 8-quarter chunks, same basket codes | **Quarterly** | 0.92 |
| 3 — fallback | UN Comtrade C/A/HS + FAOSTAT TCL | Annual basket qty ÷ 12 | Annual | 0.88 |

> **Quarterly note:** For US, BR, PH, monthly basket data is reliably available. The quarterly tier activates only if monthly returns empty (e.g. BR rate-limit gaps). Quarterly basket works for these countries since they submit monthly and quarterly HS data.

**Coverage:** Monthly from `2020-01` to present (US, BR, PH have Comtrade monthly basket data)

---

### India, Singapore, UAE

| Tier | Source | Method | Frequency | Confidence |
|---|---|---|---|---|
| 1 — quarterly | UN Comtrade C/Q/HS | `_hist_export_share_quarterly`; reporter=699/702/784; 8-quarter chunks | **Quarterly** | 0.92 |
| 2 — primary (effective) | UN Comtrade C/A/HS | Annual basket qty ÷ 12; reporter=699/702/784 | **Annual** | 0.88 |
| 3 — fallback | FAOSTAT TCL country basket | Sum country TCL export qty for staple items; latest year ÷ 12 | Annual | 0.75 |

**Coverage:** Annual 2020–2024 (IN, SG); 2020–2023 (AE)

#### Bottleneck — export share monthly/quarterly for IN/SG/AE
The Comtrade public preview API does not return monthly **or quarterly** basket data for:
- **India (reporter=699)** — does not submit sub-annual HS4-level data to the public Comtrade preview
- **Singapore (reporter=702)** — annual-only reporter (same as trade balance)
- **UAE (reporter=784)** — annual-only reporter (same as trade balance)

The quarterly tier (C/Q/HS) is included in the cascade but will likely also return empty for the same root cause — these countries do not submit quarterly HS-level data to the public API tier. In practice, the cascade falls through to annual Comtrade every time for all three.

The basket data for these countries exists in the **Comtrade subscription API** (paid). A subscription key would immediately unlock monthly and quarterly export quantities at HS4 level for all three countries.

The FAOSTAT TCL country basket fallback only provides annual data and only covers the staple commodities FAO tracks (requires alias matching to HS4 codes).

---
---

## Metric 4: Arable Land per Capita

### Definition
Hectares of arable land per person. Measures agricultural land resource
availability relative to population.

### Calculation
```
arable_land_per_capita (ha/person) = arable_land_ha / population

arable_land_ha = FAOSTAT Land Use value (1000 ha) × 1000
population     = World Bank SP.POP.TOTL for the same year (or nearest ±1 yr)
```

### Sources
| Data | Source | URL |
|---|---|---|
| Arable land | FAOSTAT Land Use (bulk) | `Inputs_LandUse_E_All_Data_(Normalized).zip` → `fao_cache/LandUse_normalized.csv` (~49 MB) |
| Population | World Bank API | `api.worldbank.org/v2/country/{iso3}/indicator/SP.POP.TOTL` |

- **All countries: Annual. No monthly alternative exists.**
- **Coverage: 2020–2023** (FAO Land Use published through 2023 as of 2025)

#### Bottleneck — arable land monthly (all countries)
Arable land is measured via **national agricultural censuses and land surveys**, which are
conducted at most annually and in many countries only every 5–10 years. FAO interpolates
between census years to produce annual estimates.

There is no monthly equivalent anywhere:
- Land does not change meaningfully month to month
- FAO, World Bank, and all national statistics agencies publish land use annually
- Even if population were interpolated monthly (trivial with WB annual data), the
  land area numerator is still annual

**Arable land per capita will always be annual.** Monthly granularity is meaningless
for this metric.

---
---

## Summary: Frequency Achieved per Country × Metric

| Country | Trade Balance | Export Share | CSR | Arable Land |
|---|---|---|---|---|
| **US** | ✅ **Monthly** (FATUS) | ✅ **Monthly** (Comtrade) | Annual (FBS) | Annual (FAO+WB) |
| **BR** | ✅ **Monthly** → 🟡 Quarterly gap-fill | ✅ **Monthly** → 🟡 Quarterly gap-fill | Annual (FBS) | Annual (FAO+WB) |
| **PH** | ✅ **Monthly** (Comtrade) | ✅ **Monthly** (Comtrade) | Annual (FBS) | Annual (FAO+WB) |
| **IN** | ⚠️ Monthly (scrape) → ❌ Quarterly → Annual | ❌ Quarterly → Annual | Annual (FBS) | Annual (FAO+WB) |
| **SG** | ❌ Monthly → ❌ Quarterly → Annual | ❌ Quarterly → Annual | Annual (WB proxy) | Annual (FAO+WB) |
| **AE** | ⚠️ Scrape → ❌ Quarterly → Annual | ❌ Quarterly → Annual | Annual (FBS) | Annual (FAO+WB) |

**Legend:**
- ✅ = genuine monthly/quarterly API data retrieved
- 🟡 = quarterly tier activates as gap-fill (BR rate-limit fallback)
- ⚠️ = estimated via Tavily+Jina scrape (confidence 0.60, requires API keys)
- ❌ = tier attempted but not expected to return data with public API

---

## Sub-Annual Data Bottleneck Summary

| Country/Metric | Best achieved | Quarterly helps? | What would unlock sub-annual |
|---|---|---|---|
| IN trade balance | Annual (WB) | ❌ IN doesn't submit quarterly to public Comtrade | Comtrade subscription key (reporter=699) or TRADESTAT license |
| SG trade balance | Annual (Comtrade) | ❌ SG is annual-only in public Comtrade | Comtrade subscription key (reporter=702) |
| AE trade balance | Annual (Comtrade) | ❌ AE is annual-only in public Comtrade | Comtrade subscription key (reporter=784) or FCSC access |
| BR trade balance | Monthly (Comtrade) — quarterly fills rate-limit gaps | 🟡 Yes — activates when monthly rate-limited | Paid Comtrade key removes rate limit entirely |
| IN/SG/AE export share | Annual (Comtrade) | ❌ Same root cause as trade balance | Comtrade subscription key for all three |
| BR/PH/US export share | Monthly (Comtrade) | 🟡 Fallback only — rarely triggered | Already at best available |
| CSR (all countries) | Annual (FAOSTAT FBS) | N/A | **Fundamental constraint — FBS is inherently annual** |
| Arable land (all) | Annual (FAOSTAT + WB) | N/A | **Fundamental constraint — land surveys are annual** |
| SG CSR specifically | Annual (WB FPI proxy) | N/A | No domestic production data in FAO FBS for SG |

---

## Confidence Score Reference

| Score | Method |
|---|---|
| 1.00 | Official API, monthly (Comtrade C/M/HS, USDA FATUS) |
| 0.92 | Official API, quarterly (Comtrade C/Q/HS) |
| 0.88 | Official API, annual (Comtrade C/A/HS, World Bank) |
| 0.75 | Bulk file download (FAOSTAT FBS, TCL, Land Use) |
| 0.60 | Web scrape via Tavily+Jina (India, UAE — total trade scaled by WB food %) |
| 0.30 | Imputed / proxy (SG CSR via WB Food Production Index) |

---

## Latest Snapshot Values (from most recent run)

| Country | Trade Balance | Last date | CSR | Export Share | Last date | Arable (ha/p) |
|---|---|---|---|---|---|---|
| US | −$967M/mo | 2026-02 | 0.877 | 21.3% | 2026-02 | 0.450 |
| BR | +$16.8B/mo | 2026-03 | 1.554 | 60.0% | 2026-03 | 0.264 |
| PH | −$854M/mo | 2025-12 | 0.752 | 0.002% | 2025-12 | 0.049 |
| IN | +$12.9B/yr | 2022–23 | 0.966 | 5.0% | 2024 | 0.107 |
| SG | −$2.7B/yr | 2024 | 0.393* | 0.026% | 2024 | 0.000095 |
| AE | −$4.6B/yr | 2023 | 0.307 | 0.16% | 2023 | 0.005 |

*SG CSR = WB FPI proxy (imputed, confidence 0.30)

---

## Historical Coverage After Monthly Upgrade

| Country | Trade | Export Share | CSR | Arable |
|---|---|---|---|---|
| US | **Monthly** 2020-01 → 2026-02 | **Monthly** 2020-01 → 2026-02 | Annual 2020–2023 | Annual 2020–2023 |
| BR | **Monthly** 2020-01 → 2026-03 | **Monthly** 2020-01 → 2026-03 | Annual 2020–2023 | Annual 2020–2023 |
| PH | **Monthly** 2020-01 → 2025-12 | **Monthly** 2020-01 → 2025-12 | Annual 2020–2023 | Annual 2020–2023 |
| IN | Monthly (scrape, last 2 yrs) + Annual 2020–2023 | Annual 2020–2024 | Annual 2020–2023 | Annual 2020–2023 |
| SG | Annual 2020–2024 | Annual 2020–2024 | — (no data) | Annual 2020–2023 |
| AE | Annual (scrape) + Annual 2020–2023 | Annual 2020–2023 | Annual 2020–2023 | Annual 2020–2023 |
