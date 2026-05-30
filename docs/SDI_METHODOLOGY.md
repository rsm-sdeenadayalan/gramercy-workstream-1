# Strategic Datacenter Index (SDI) — Methodology

**Version:** 1.0 · **Last refreshed:** 2026-05-29
**Pipeline repo:** `gramercy-workstream-1`
**Country universe:** United States, United Arab Emirates, Brazil, India, Singapore, Philippines

---

## 1 · Purpose

The SDI scores each country on its **physical-resource readiness for AI compute
infrastructure** — energy, water, critical minerals, food. It is the substrate
half of a coupled framework:

> **S-C Gap = SDI − CII**, where CII (Compute Infrastructure Index) measures the
> AI compute actually built. Negative gap → over-converting (UAE pattern);
> near-zero → parity (US); positive → under-converting (Brazil pattern).

SDI is intentionally narrow: only **physical inputs**. Talent, capital markets,
regulatory speed, and grid politics are out of scope by design — those belong in
adjacent indices, not this one.

---

## 2 · Composition

The composite SDI is the weighted sum of four sub-indices:

| Sub-index | Theme | Weight | Metrics |
|---|---|---:|---:|
| **SI1** | Energy Substrate | **0.35** | 5 scored + 2 reference |
| **SI2** | Water Availability | **0.20** | 4 scored + 1 reference |
| **SI3** | Critical Mineral Endowment | **0.30** | 6 minerals × 3 sub-metrics |
| **SI4** | Food & Agricultural Security | **0.15** | 4 scored |

**Final formula:**
$$SDI = 0.35 \cdot SI_1 + 0.20 \cdot SI_2 + 0.30 \cdot SI_3 + 0.15 \cdot SI_4$$

Each sub-index is itself a weighted, normalized composite of its metrics
(details below). All scores are on a 0–100 scale.

---

## 3 · Sub-Index 1 — Energy Substrate

Measures the cost, scalability, sovereignty, and reliability of the energy
available for AI compute.

| Metric | Weight | Inverted? | Source | Unit |
|---|---:|---|---|---|
| `electricity_price` (industrial) | 0.35 | ✓ (lower = better) | EIA (US), GlobalPetrolPrices.com (others) | USD/kWh |
| `renewable_share` | 0.20 | — | IRENA RE-SHARE table (capacity indicator) | % |
| `reserve_margin` | 0.20 | — | NERC (US), EMA (SG), CEA (IN), regulator/agent for others | % |
| `energy_investment` (planned, 5yr) | 0.10 | — | National 5-yr energy plans + IEA WEI via research agent, dedup'd | USD billions |
| `energy_import_dependency` | 0.15 | ✓ (lower = better) | World Bank `EG.IMP.CONS.ZS` (IEA-sourced) | % of energy use |

**Tracked but not scored** (collected for client transparency / methodology
audits, but not in the scoring formula):
- `grid_capacity` (GW) — IRENA, used as derived input for reserve_margin computation
- `interconnection_queue_depth` (MW) — LBNL Queued Up (US), regulator/agent (others)
- `electricity_price_residential` (USD/kWh) — collected alongside industrial for reference

### Weight rationale
The spec originally weighted renewable_share at 0.30 and energy_investment at
0.15. The 2026-05-29 reweight pulls 0.10 from those two to fund the new
`energy_import_dependency` metric. The new metric captures **substrate
sovereignty** — a country that imports 280% of its energy use (Singapore) is
materially more vulnerable than one at −170% (UAE). This was previously
unmeasured in SI1.

---

## 4 · Sub-Index 2 — Water Availability

Measures the freshwater available for datacenter cooling and the regulatory
environment governing industrial water use.

| Metric | Weight | Inverted? | Source | Unit |
|---|---:|---|---|---|
| `freshwater_per_capita` | 0.30 | — | WB `ER.H2O.INTR.PC` (FAO AQUASTAT) | m³/cap/year |
| `baseline_water_stress` | 0.40 | ✓ | WRI Aqueduct 4.0 | 0-5 score |
| `projected_water_stress_change` | 0.20 | ✓ | WRI Aqueduct 4.0 (SSP3-7.0 minus baseline) | delta |
| `regulatory_restrictions_score` | 0.10 | — | Claude NLP classification of national water-regulator documents | 1-5 score |

**Tracked but not scored:** `projected_water_stress_2050` (used to derive
`projected_water_stress_change`, kept for transparency).

---

## 5 · Sub-Index 3 — Critical Mineral Endowment

Measures national control over the minerals essential to AI infrastructure
(grid copper, battery minerals, magnet rare earths, semiconductor silicon).

### Mineral weights (sum to 1.00)
| Mineral | Weight | Rationale |
|---|---:|---|
| Copper | 0.30 | Grid, wiring, transformers — backbone of any datacenter buildout |
| Lithium | 0.20 | Battery storage for grid stabilization |
| Nickel | 0.15 | Battery cathodes (NMC) + alloys |
| Cobalt | 0.15 | Battery cathodes (NMC/LCO) |
| Rare earths | 0.10 | Permanent magnets for wind, motors |
| Silicon | 0.10 | Semiconductors, solar PV |

### Per-mineral sub-metrics (sum to 1.00 within each mineral)
| Metric | Weight | Source |
|---|---:|---|
| `production_share` | 0.40 | USGS Mineral Commodity Summaries (ScienceBase CSV / PDF parse) |
| `reserves_share` | 0.30 | USGS MCS |
| `refining_share` | 0.30 | UN Comtrade HS6 processed exports / world total |

### Additional tracked metrics (not directly in SI3 score)
- `yoy_growth` (year-over-year production growth, derived from USGS)
- `value_add_ratio` (processed / (processed + raw) exports — derived from UN Comtrade)

### Composition
For each country:
$$SI_3 = \sum_{m \in minerals} w_m \cdot \big(0.40 \cdot prod\_share_m + 0.30 \cdot reserves\_share_m + 0.30 \cdot refining\_share_m\big)$$

Each share is min-max normalized across the 6 countries before the weighting.

---

## 6 · Sub-Index 4 — Food and Agricultural Security

Strategic value for a country that can feed itself (or export) during global
disruption.

| Metric | Weight | Source | Unit |
|---|---:|---|---|
| `net_food_trade_balance` | 0.30 | USDA FATUS (US), UN Comtrade HS 01-24 (BR/IN/PH/SG/AE) | USD |
| `caloric_self_sufficiency_ratio` | 0.30 | FAOSTAT Food Balance Sheets: ∑production_tons / ∑domestic_supply_tons across leaf food items | ratio |
| `share_global_staple_exports` | 0.20 | UN Comtrade HS basket (1001/1003/1005/1006/1201/1205/1206/1507/1511/1701) annual ÷ 12 / FAOSTAT TCL world basket annual ÷ 12 | ratio |
| `arable_land_per_capita` | 0.20 | FAOSTAT LandUse ÷ WB `SP.POP.TOTL` | ha/person |

---

## 7 · Scoring Pipeline

### 7.1 Normalization
Each metric is **min-max normalized** to [0, 1] across the 6 countries:

$$n_{c,m} = \frac{x_{c,m} - \min_c(x_{c,m})}{\max_c(x_{c,m}) - \min_c(x_{c,m})}$$

When `invert=True` (lower raw value is better — e.g. electricity_price,
energy_import_dependency, water_stress), the normalized score is `1 − n`.

### 7.2 Sub-index composition
$$SI_k = 100 \cdot \sum_{m \in metrics(k)} w_m \cdot n_{c,m}$$

### 7.3 Composite SDI
$$SDI_c = 0.35 \cdot SI_1 + 0.20 \cdot SI_2 + 0.30 \cdot SI_3 + 0.15 \cdot SI_4$$

### 7.4 Handling missing data
- A cell with no canonical source AND no agent synthesis is stored as an
  **open gap** in `si{N}_data_gaps`.
- During normalization, missing cells are excluded from `min`/`max` calculation
  for that metric (a country's score is computed using only metrics with data).
- A country's sub-index score is computed only from metrics it has data for;
  weights are renormalized within the available set.

---

## 8 · Data Quality Framework

### 8.1 Source authority hierarchy
The collection pipeline ranks sources by tier and prefers higher-tier
sources when multiple agree:

| Tier | Examples | Confidence range |
|---|---|---|
| 1 — National stats agencies | EIA, NERC, ANEEL, ONS, CEA, EMA, DEWA, DOE PH | 0.85 – 1.00 |
| 2 — IGOs / official multi-laterals | IEA, IRENA, World Bank, UN Comtrade, FAOSTAT, OECD, WRI | 0.75 – 0.90 |
| 3 — Industry / analysts | BloombergNEF, S&P Global Platts, Wood Mackenzie (*actuals only*) | 0.55 – 0.70 |
| 4 — Major financial press | FT, Reuters, Bloomberg (*only when citing Tier 1-3*) | 0.50 – 0.65 |
| 0 — Rejected at source-filter layer | Facebook, Twitter/X, LinkedIn, Instagram, TikTok, Reddit, Quora, aggregator sites | n/a |

### 8.2 Solid-backing rule
Every stored value must have:
- A **single citable** `source_url`
- A **literal raw quote** from that source containing the value (or, for
  derived metrics, the components used in the computation)
- A `confidence_score` reflecting the source's tier

The research agent's synthesis step rejects any value that cannot be backed by a
literal source quote. Synthesis must either find a value verbatim in one
gathered source or compute it deterministically from components that appear
verbatim — never estimate or interpolate.

### 8.3 Confidence-first view ordering
The `v_si{1,2,4}_latest` SQL views (and the SI3 scoring fetch) order by:
`confidence_score DESC, data_date DESC, collected_at DESC`. This ensures that
when both a canonical API (e.g. IRENA conf 0.85) and the agent supplement
(conf 0.60) both succeed, the canonical wins and feeds scoring. The agent row
is retained in `si{N}_raw_metrics` for transparency / audit.

### 8.4 Cross-cutting filters
- **No social-media sources** — explicit domain blocklist
- **FX-as-of-date** — every USD conversion uses the Frankfurter rate on the
  observation's `data_date`, not the latest spot rate
- **Source-quality validator** — synthesis output rejected if `raw_text` is
  empty/short, if value isn't literally in the text, or if source_url is empty

---

## 9 · Known Limitations & Gaps

### 9.1 SI3 (Critical Minerals) — partial coverage
- The `refining_share` metric depends on UN Comtrade HS6 processed-export data.
  Comtrade's free tier (10K calls/day) is consumed by a single full SI3 run,
  so consecutive runs may have refining_share gaps until daily quota resets.
- The USGS MCS PDF parser handles 4 of 6 minerals cleanly (lithium, nickel,
  cobalt, rare_earths). Copper has a different table shape (mine + refinery
  columns); silicon has ferrosilicon-plus-silicon-metal layout. Both currently
  fall back to ScienceBase CSV (MCS 2023 data, 3 years older than the PDF).
- Country-specific reserve definitions vary in USGS source: Australia uses
  in-ground "resources" estimates rather than JORC-compliant reserves, making
  the AU reserves number look ~40× higher than JORC for nickel.

### 9.2 SI1 (Energy) — methodology gaps where regulators don't publish
- `reserve_margin` for AE, BR, IN, PH: regulators don't publish a clean
  planning-reserve-margin %. Computation from IRENA nameplate capacity +
  agent-discovered peak demand often yields >60% (rejected by plausibility
  cap because nameplate ≠ derated firm capacity).
- `energy_investment` for US, AE, SG, PH: no single 5-year-plan figure
  exists in the public record meeting the strict dedup criteria. Cells stay
  as honest gaps rather than report incomparable proxy numbers.
- `interconnection_queue_depth` for AE, BR, SG, PH: those countries don't
  publish a MW-denominated queue depth; the metric is structurally a
  US/IN/EU-style concept.

### 9.3 SI2 / SI4 — minor caveats
- SI4 `share_global_staple_exports` for BR comes out at ~51% because the 10
  HS4 codes the spec specifies (1001/1003/1005/1006/1201/1205/1206/1507/1511/1701)
  happen to be commodities BR genuinely dominates (soybeans, sugar, corn).
  This is the spec answer, not an error.
- SI2 `regulatory_restrictions_score` is Claude-NLP-derived from each
  country's water-regulator publications. Confidence 0.55-0.65 reflects the
  qualitative judgement involved.

### 9.4 Cross-country comparability
All metrics are normalized against the same 6-country reference set. Adding
or removing countries will rescale every score. The composite SDI is
meaningful only within this fixed reference universe; absolute scores are
not transferable to other country sets without re-normalization.

---

## 10 · Reproducibility

### 10.1 Re-running the full pipeline
```bash
cd gramercy-workstream-1
python setup.py                 # idempotent — creates DB + applies schemas
python run_all.py               # runs SI1+SI2+SI3+SI4 in parallel (~30-60 min)
python score_pipeline.py        # computes scores → score_sdi table
```

### 10.2 Refreshing a single sub-index
```bash
python run_all.py --only si1    # or si2 / si3 / si4
python score_pipeline.py        # always re-score after a refresh
```

### 10.3 Environment configuration
All API keys and database settings are in `.env` (see `.env.example`).
Required minimum:
- `POSTGRES_*` (DB connection)
- `ANTHROPIC_API_KEY` (Claude — research agent synthesis + SI2 NLP)
- `TAVILY_API_KEY` (sole search backend)
- `EIA_API_KEY` (US electricity data)
- `DATAGOV_SG_API_KEY` (SG electricity)
- `COMTRADE_KEY` (SI3 refining_share — free 10K calls/day from comtradeplus.un.org)

### 10.4 Operational knobs
- `SI3_COMTRADE_TIMEOUT` (default 120s) — per-call hard timeout
- `SI3_COMTRADE_QUOTA_WAIT_MAX` (default 600s) — cap on quota-replenishment sleep
- `SI3_SKIP_COMTRADE=1` — bypass Comtrade entirely (research-agent for SI3 export metrics)
- `WS1_PIPELINE_IDLE_TIMEOUT` (default 600s) — orchestrator watchdog
- `SI1_IRENA_INDICATOR=generation` — flip renewable_share to generation share
  (default is capacity share, matching IRENA's public-facing table)

### 10.5 Source code anchor
Every commit ships with a Co-Authored-By trailer attributing the assistant's
contributions for traceability. Methodology decisions are documented in
`docs/METHODOLOGY_DECISIONS.md`.

---

## 11 · Change Log

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-05-29 | Initial methodology paper. Captures the SI1 reweight (5th metric `energy_import_dependency`), confidence-first view ordering, solid-backing validator, progressive web-search agent, MCS PDF parser, and all cross-cutting source filters. |

---

## 12 · References

- IRENA PxWeb API — https://pxweb.irena.org/api/v1/en/IRENASTAT/
- IRENA RE-SHARE table — https://pxweb.irena.org/pxweb/en/IRENASTAT/IRENASTAT__Power%20Capacity%20and%20Generation/RE-SHARE_2026_H1_v-PX%201.px
- US EIA Open Data — https://www.eia.gov/opendata/
- USGS Mineral Commodity Summaries — https://pubs.usgs.gov/periodicals/
- UN Comtrade — https://comtradeplus.un.org/
- World Bank WDI — https://data.worldbank.org/
- WRI Aqueduct 4.0 — https://www.wri.org/data/aqueduct-global-maps-40-data
- FAOSTAT — https://www.fao.org/faostat/
- NERC Reliability Assessments — https://www.nerc.com/pa/RAPA/ra/
- IEA Energy Statistics — https://www.iea.org/data-and-statistics/
