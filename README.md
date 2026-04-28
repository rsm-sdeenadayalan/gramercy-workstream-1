# Gramercy Sub-Index Pipelines

Four independent data pipelines that collect strategic indicators for six
countries (US, AE, BR, IN, SG, PH) and store them in PostgreSQL. Each
pipeline runs as its own process; `run_all.py` orchestrates them in parallel.

| Sub-index | Theme | Database | Entry script | Metrics |
|---|---|---|---|---|
| **SI1** | Energy | `subindex_1` | `si1_pipeline.py` | electricity_price, renewable_share, grid_capacity, reserve_margin, energy_investment, interconnection_queue_depth |
| **SI2** | Water | `subindex_2` | `si2_pipeline.py` | freshwater_per_capita, baseline_water_stress, projected_water_stress_2050, projected_water_stress_change, regulatory_restrictions_score |
| **SI3** | Critical Minerals | `subindex_3` | `si3_pipeline.py` | production_share, reserves_share, refining_share, yoy_growth, value_add_ratio (× 6 minerals: copper, lithium, nickel, cobalt, rare earths, silicon) |
| **SI4** | Food | `subindex_4` | `si4_pipeline.py` | net_food_trade_balance, caloric_self_sufficiency_ratio, share_global_staple_exports, arable_land_per_capita |
| **Scoring** | Composite | `csi_scores` | `score_pipeline.py` | Min-max normalized 0-100 per metric → weighted sub-index scores → final SDI ranked across the 6 countries |

All four pipelines share `research_agent.py` — a Tavily/Brave + Claude deep-research
loop that fires as a universal fallback when direct-API collectors fail. The scoring
pipeline is fed by all four sub-indexes once their data is collected.

---

## 1 · Install

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium   # only if collectors that use Playwright are needed
```

Python 3.11+ recommended.

---

## 2 · Configure

```bash
cp .env.example .env
$EDITOR .env
```

Required at minimum:

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD` — DB access
- `ANTHROPIC_API_KEY` — Claude (research-agent synthesis + SI2 NLP)
- `TAVILY_API_KEY` *or* `BRAVE_API_KEY` *or* `JINA_API_KEY` — at least one search backend

Recommended:

- `EIA_API_KEY` — US energy data (SI1)
- `DATAGOV_SG_API_KEY` — Singapore data (SI1)

---

## 3 · Connect to the database

The pipelines connect to PostgreSQL using `POSTGRES_HOST`, `POSTGRES_PORT`,
`POSTGRES_USER`, and `POSTGRES_PASSWORD` from your `.env`. Set them to your
server's actual address.

**If your DB is behind an SSH bastion** (as in our UCSD development setup),
open a tunnel in a separate terminal and point `.env` at `localhost`:

```bash
ssh -L 5433:localhost:5433 <ssh_user>@<ssh_host> -N \
    -o ServerAliveInterval=60 -o ServerAliveCountMax=10
```

Then in `.env`:
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
```

`ServerAliveInterval=60` keeps the tunnel up during long agent runs.

---

## 4 · One-time DB setup

```bash
python setup.py
```

That's it. The script creates all four databases (`subindex_1`, `subindex_2`,
`subindex_3`, `subindex_4`) if they don't already exist and applies the
corresponding schemas. Idempotent — safe to re-run any time.

Requires the `POSTGRES_USER` in `.env` to have `CREATEDB` privilege. If your DBA
already created the databases, just running this script will skip the
creation step and only apply the schemas.

If you'd rather do it manually with `psql`:

```bash
psql -h localhost -p 5433 -U <user> -d subindex_1 -f schema.sql
psql -h localhost -p 5433 -U <user> -d subindex_2 -f schema.sql
psql -h localhost -p 5433 -U <user> -d subindex_3 -f si3_schema.sql
psql -h localhost -p 5433 -U <user> -d subindex_4 -f schema.sql
psql -h localhost -p 5433 -U <user> -d csi_scores  -f score_schema.sql
```

---

## 5 · Run

```bash
# Step 1 — collect raw data (all four sub-indexes in parallel)
python run_all.py

# Or one at a time
python run_all.py --only si1
python run_all.py --only si2
python run_all.py --only si3
python run_all.py --only si4

# Step 2 — compute composite scores from the collected data
python score_pipeline.py
```

Each collection pipeline writes a per-run log to `siN_run.log` and a summary to
stdout. The scoring pipeline reads from all four sub-index databases, normalizes
each metric (min-max 0-100 across the 6 countries), applies the methodology
weights from `csi_scores.score_methodology` / `score_mineral_weights` /
`score_subindex_weights`, and writes the final SDI ranking to `csi_scores.score_sdi`
(view: `v_sdi_ranked`).

Tweak any weight or inversion by `UPDATE`-ing the config tables — no code change needed.

For SI4, an opt-in historical mode fills 2020 → present:

```bash
python si4_pipeline.py --historical                 # default start year 2020
python si4_pipeline.py --historical --start-year=2018
```

---

## 6 · Verify & report

After a run:

```bash
python si1_gap_report.py                          # SI1 gaps + completeness
python si1_verify.py                              # SI1 bounds + freshness
python si4_gap_report.py                          # SI4 open gaps + latest + coverage
python si4_verify.py                              # SI4 bounds + freshness + confidence
python si3_gap_report.py  # SI3 gaps + latest + coverage
python si3_verify.py      # SI3 [0,1] bounds + EST flags + freshness
```

SI2 reporting is queried directly from the `v_si2_latest` view.

---

## 7 · How the cascade works

For each `(country, metric)` pair, the pipeline tries collectors in order:

1. **Direct-API tiers** (most authoritative first — official APIs like EIA, World Bank,
   FAOSTAT, UN Comtrade, WRI Aqueduct).
2. **Bulk-file tiers** (cached FAO bulks for SI4, WRI shapefiles for SI2).
3. **Research-agent fallback** — `research_agent.py` is the universal last resort.
   Fires when (a) all cascade steps fail, **or** (b) no cascade is defined for the
   pair. The agent runs a Tavily/Brave search → Claude reflection loop, scoped to a
   per-country list of trusted publishers.
4. **Open gap** — only if direct collectors *and* the research agent both fail.

A staleness check at the start of `run_cascade` skips combos whose existing data
is fresher than the per-method threshold (45 d for monthly APIs, 90 d for web
scrapes, 400 d for annual sources, etc.). Re-runs are cheap.

---

## 8 · Project layout

```
Gramercy/
├── README.md                    # this file
├── .env.example                 # config template
├── requirements.txt             # pinned deps
├── schema.sql                   # CREATE TABLE for SI1, SI2, SI4
│
├── setup.py                     # one-time DB + schema bootstrap
├── run_all.py                   # main entry — runs all 4 pipelines in parallel
├── research_agent.py            # shared Tavily/Brave + Claude research loop
│
├── si1_pipeline.py              # SI1 — Energy
├── si1_gap_report.py            # SI1 gap report
├── si1_verify.py                # SI1 verification
│
├── si2_pipeline.py              # SI2 — Water
├── si2_collectors.py            # SI2 collector functions
│
├── si3_pipeline.py              # SI3 — Critical Minerals
├── si3_gap_report.py            # SI3 gap report
├── si3_verify.py                # SI3 verification
├── si3_schema.sql               # SI3 schema (apply to subindex_3 DB)
│
├── si4_pipeline.py              # SI4 — Food
├── si4_gap_report.py            # SI4 gap report
└── si4_verify.py                # SI4 verification
```

---

## 9 · Costs (typical full parallel run)

| Component | Cost |
|---|---|
| Claude API (research-agent + SI2 NLP) | ~$0.10–0.20 USD |
| Tavily search | ~30–40 calls / 1 000 free monthly quota |
| All other APIs (EIA, World Bank, FAOSTAT, Comtrade, WRI) | free |

---

## 10 · Notes

- **No hardcoding of values or specific document URLs.** Discovery is dynamic
  — the research agent's `TRUSTED_SOURCES` and `_PRIMARY_QUERIES` are the
  knobs to expand. Stable API endpoints (EIA, World Bank, FAOSTAT, Comtrade,
  WRI Aqueduct) are infrastructure, not hardcoding.
- **FAO bulk files** for SI4 (~3 GB total) auto-download into `fao_cache/`
  on first run.
- **Re-running is safe** — every UPSERT uses `ON CONFLICT DO UPDATE`.
