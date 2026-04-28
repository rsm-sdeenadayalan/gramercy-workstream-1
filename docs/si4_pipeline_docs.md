# WS1 Food Sub-Index (SI4) — Pipeline Documentation

## Overview

This pipeline collects, cleans, and stores food security metrics for 6 countries into a
PostgreSQL database. It is part of the WS1 Food Sub-Index (SI4) project. Data is collected
from public APIs and bulk file downloads, then stored in a college PostgreSQL server.

**Countries:** United States (US), UAE (AE), Brazil (BR), India (IN), Singapore (SG), Philippines (PH)

**Metrics (4 total):**
| Metric Key | Label | Unit |
|---|---|---|
| `net_food_trade_balance` | Net Food Trade Balance | USD |
| `caloric_self_sufficiency_ratio` | Caloric Self-Sufficiency Ratio | ratio (0–1+) |
| `share_global_staple_exports` | Share of Global Exports in Key Staples | ratio |
| `arable_land_per_capita` | Arable Land per Capita | ha/person |

---

## Repository Structure

```
Abhigna_v2/
├── 02_pipeline.ipynb       # Main pipeline notebook (run this)
├── .env                    # Local DB credentials
├── college.env             # College server DB credentials
├── fao_cache/              # Cached FAO bulk CSVs (auto-created on first run)
│   ├── FBS_normalized.csv       # Food Balance Sheets (~633 MB)
│   ├── TCL_normalized.csv       # Trade Crops & Livestock (~2.4 GB)
│   └── LandUse_normalized.csv   # Land Use (~49 MB)
│
../db-connect.sh            # SSH tunnel + psql session (interactive)
../tunnel_only.sh           # SSH tunnel only (for notebook use)
```

---

## Database

**College server:** `rsm-compute-02.ucsd.edu`
**Port:** `5433`
**Database:** `subindex_4`
**User:** `abhigna`

### Tables

| Table | Purpose |
|---|---|
| `si4_raw_metrics` | Non-trade metrics (CSR, export share, arable land) |
| `si4_food_trade_raw` | Trade balance metrics (exports, imports, balance) |
| `si4_collection_log` | Per-attempt log (success/failure, duration) |
| `si4_collection_runs` | One row per pipeline run with summary stats |
| `si4_data_gaps` | Tracks metrics where all collectors failed |

---

## How to Run

### Option A — Run locally, push to college server (recommended)

**Prerequisites:** SSH access to `rsm-compute-02.ucsd.edu` as `aakkipeddi`

**Step 1 — Start SSH tunnel** (keep this terminal open):
```bash
cd /path/to/CLAUDE_me
./tunnel_only.sh
# Stop when done: ./tunnel_only.sh stop
```

**Step 2 — In `02_pipeline.ipynb`, set:**
```python
DB_TARGET = "college"
```

**Step 3 — Run all cells** (Kernel → Restart Kernel and Run All Cells)

The notebook runs two pipelines:
- `run_pipeline()` — latest snapshot for all 24 country/metric combos
- `run_pipeline_historical(start_year=2020)` — one row per year from 2020 to present

---

### Option B — Run directly on the college server

**Step 1 — SSH into the server:**
```bash
ssh aakkipeddi@rsm-compute-02.ucsd.edu
```

**Step 2 — Copy the project folder** (if not already there):
```bash
# From local machine:
scp -r /path/to/Abhigna_v2 aakkipeddi@rsm-compute-02.ucsd.edu:~/
```
Include `fao_cache/` to avoid re-downloading 3 GB of FAO data.

**Step 3 — In `02_pipeline.ipynb`, change:**
```python
DB_TARGET = "local"
_dir = Path("/home/aakkipeddi/Abhigna_v2")   # adjust to actual path
```

No tunnel needed — PostgreSQL is already on `localhost:5433`.

**Step 4 — Install dependencies** (if not already available):
```bash
pip install pandas psycopg2-binary requests beautifulsoup4 openpyxl python-dotenv
```

---

## Data Sources & Cascade Logic

Each metric uses a **cascade** — if the primary source fails, the pipeline automatically
tries the next fallback. Only the first successful result is stored.

### Net Food Trade Balance

| Country | Step 1 | Step 2 | Step 3 |
|---|---|---|---|
| US | USDA ERS FATUS (monthly .xlsx) | — | — |
| BR | UN Comtrade monthly (HS 01–24) | — | — |
| IN | UN Comtrade monthly | World Bank TX/TM.VAL | — |
| PH | UN Comtrade monthly | — | — |
| SG | UN Comtrade monthly | UN Comtrade annual | World Bank TX/TM.VAL |
| AE | UN Comtrade monthly | UN Comtrade annual | World Bank TX/TM.VAL |

> SG and AE do not publish monthly data to the Comtrade public API — they always fall through to annual or World Bank.

### Caloric Self-Sufficiency Ratio (CSR)

All countries: **FAOSTAT Food Balance Sheets (FBS)** bulk download.

> Singapore has no domestic food production entries in FBS. The snapshot pipeline uses a
> World Bank Food Production Index (AG.PRD.FOOD.XD) proxy. The historical pipeline skips SG
> (returns no data) because the WB indicator is not suitable for year-by-year historical series.

**Formula:** `CSR = Σ(production_kcal per item) / Σ(food_supply_kcal per item)`

### Share of Global Staple Exports

**Staple basket (HS4 codes):** wheat, maize, soybeans, palm oil, rice, sugar, barley,
soybean oil, rapeseed, sunflower seed (`1001, 1003, 1005, 1006, 1201, 1205, 1206, 1507, 1511, 1701`)

**Formula:** `share = country_monthly_export_qty_t / world_monthly_export_qty_t`
- World denominator: FAOSTAT TCL (Trade Crops & Livestock) annual ÷ 12

| Country | Step 1 | Step 2 | Step 3 |
|---|---|---|---|
| US, BR, PH | Comtrade monthly basket | — | — |
| IN, SG, AE | Comtrade monthly basket | Comtrade annual basket ÷ 12 | FAOSTAT TCL country basket |

### Arable Land per Capita

All countries: **FAOSTAT Land Use** (arable land in 1000 ha) ÷ **World Bank SP.POP.TOTL**

---

## Data Availability (after running historical pipeline)

Results from running `run_pipeline_historical(start_year=2020)`:

| Country | Trade Balance | CSR | Export Share | Arable Land |
|---|---|---|---|---|
| US | 2020–2024 (5 yrs) | 2020–2023 (4 yrs) | 2020–2024 (5 yrs) | 2020–2023 (4 yrs) |
| AE | 2020–2023 (4 yrs) | 2020–2023 (4 yrs) | 2020–2023 (4 yrs) | 2020–2023 (4 yrs) |
| BR | **2020–2021 (2 yrs)** | 2020–2023 (4 yrs) | 2020–2025 (6 yrs) | 2020–2023 (4 yrs) |
| IN | 2020–2023 (4 yrs) | 2020–2023 (4 yrs) | 2020–2024 (5 yrs) | 2020–2023 (4 yrs) |
| SG | 2020–2024 (5 yrs) | **no historical data** | 2020–2024 (5 yrs) | 2020–2023 (4 yrs) |
| PH | 2020–2024 (5 yrs) | 2020–2023 (4 yrs) | 2020–2024 (5 yrs) | 2020–2023 (4 yrs) |

**Why ranges differ:**
- Arable land and CSR cap at **2023** — FAO bulk files (FBS, Land Use) are only published through 2023
- Trade and export share reach **2024–2025** — Comtrade annual data is more current
- BR trade only 2 years — Comtrade public preview API rate-limited; a paid API key would fix this
- SG CSR has no historical series — by design (no production data in FBS)

---

## Confidence Scores

Each stored row has a `confidence_score` (0–1) reflecting data quality:

| Score | Method |
|---|---|
| 1.00 | API monthly (most current) |
| 0.88 | API annual |
| 0.75 | Bulk file download (FAO) |
| 0.30 | Imputed / proxy |

---

## Useful SQL Queries

**Check what's in the DB:**
```sql
SELECT country_iso, metric_key, COUNT(*) AS years,
       MIN(data_date) AS from, MAX(data_date) AS to
FROM si4_raw_metrics
GROUP BY country_iso, metric_key
ORDER BY country_iso, metric_key;
```

**Check trade data:**
```sql
SELECT country_iso, COUNT(*) AS years,
       MIN(data_date) AS from, MAX(data_date) AS to
FROM si4_food_trade_raw
GROUP BY country_iso
ORDER BY country_iso;
```

**View all metric values:**
```sql
SELECT country_iso, metric_key, data_date, metric_value, confidence_score, source_name
FROM si4_raw_metrics
ORDER BY country_iso, metric_key, data_date;
```

**Connect to college DB (from local machine):**
```bash
# Start tunnel first:
./tunnel_only.sh

# Then connect:
psql -h localhost -p 15433 -U abhigna -d subindex_4
# Password: abhi_ucsd

# Exit psql:
\q

# Stop tunnel:
./tunnel_only.sh stop
```

---

## Known Limitations

| Issue | Cause | Workaround |
|---|---|---|
| AE/SG no monthly trade | Don't publish to Comtrade monthly | Falls back to annual Comtrade, then World Bank |
| IN/SG/AE no monthly export share | Comtrade monthly basket returns no data | Falls back to annual Comtrade, then FAOSTAT TCL |
| BR trade history short (2 yrs) | Comtrade public API rate limit | Paid Comtrade API subscription |
| SG CSR no historical series | No production data in FAO FBS | World Bank FPI proxy (snapshot only) |
| FAO data lags by ~1 year | FAO publishes annually with delay | Expected — 2023 is current as of 2025 |

---

## VS Code / Jupyter Gotcha

VS Code's Jupyter extension caches cell definitions in memory. If you edit a cell and re-run
it, the kernel may still use the old version. If results look wrong after editing:

1. `Cmd+Shift+P` → **Revert File** (forces VS Code to reload from disk)
2. **Kernel → Restart Kernel and Run All Cells**

This is especially important when editing collector functions or the cascade definition.
