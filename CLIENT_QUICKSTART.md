# Gramercy SDI (Workstream 1) — Client Quickstart

This repo collects, scores, and publishes the **Strategic Datacenter Index (SDI)**
for 6 countries (US, AE, BR, IN, SG, PH). It is one half of a coupled framework;
the other half is `gramercy-workstream-2` (CII). The two together produce the
**S-C Gap** analysis.

For full methodology see `docs/SDI_METHODOLOGY.md`.

---

## 1 · Prerequisites

- **Python 3.11+** (developed on 3.14)
- **PostgreSQL 14+** reachable on a host you control
- API keys (all free-tier acceptable):
  - **Anthropic** (Claude) — research-agent synthesis + SI2 NLP
  - **Tavily** — web search backend
  - **EIA** — US electricity data
  - **data.gov.sg** — Singapore electricity (free signup at data.gov.sg)
  - **UN Comtrade** — SI3 minerals trade data (free 10K calls/day at comtradeplus.un.org)

---

## 2 · Setup (5 minutes from a fresh clone)

```bash
# 1. Clone
git clone https://github.com/rsm-sdeenadayalan/gramercy-workstream-1
cd gramercy-workstream-1

# 2. Python env
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium   # only needed if you'll use Playwright collectors

# 3. Configure
cp .env.example .env
$EDITOR .env                       # fill in POSTGRES_* + API keys

# 4. Bootstrap DB (auto-creates database if missing, applies all 4 schemas)
python setup.py

# 5. Smoke-test a single sub-index (no minerals data needed):
python run_all.py --only si2       # ~1 min, free
python score_pipeline.py
```

If `python setup.py` reports `"Created '<db>' (via admin DB 'postgres')"` and then
4 `"✓ schema applied"` lines, you're good.

---

## 3 · Full pipeline run

```bash
caffeinate -i nohup python -u run_all.py > /tmp/ws1_run.log 2>&1 &
python score_pipeline.py
```

Expected runtime:
- **SI1 (Energy)**: ~10-15 min
- **SI2 (Water)**: ~1-3 min
- **SI3 (Minerals)**: ~25-50 min (Comtrade-limited — see §5)
- **SI4 (Food)**: ~10-15 min
- **Scoring**: <1 min

SI1+SI2 run in parallel, then SI3+SI4. Total wall time: ~40-70 min.

---

## 4 · Where the answers live

After running scoring, the canonical client-facing tables:

```sql
-- Final SDI per country (0-100, ranked)
SELECT * FROM score_sdi ORDER BY sdi_score DESC;

-- Per-sub-index breakdown
SELECT * FROM score_subindex ORDER BY country_iso, sub_index;

-- Every metric value with source + confidence
SELECT * FROM v_si1_latest;        -- energy
SELECT * FROM v_si2_latest;        -- water
SELECT * FROM si3_pipeline_metrics;  -- minerals (per-mineral)
SELECT * FROM v_si4_latest;        -- food

-- Open data gaps
SELECT * FROM si1_data_gaps WHERE status='open';
-- (similar for si2/si3/si4)
```

For S-C Gap analysis (requires WS2's `cii` database also populated), see
the CII repo's `v_cii_sc_gap_ranked` view.

---

## 5 · Known gotchas

### 5a. UN Comtrade quota
The free tier has both a daily cap AND a per-minute throttle. Defaults are tuned
conservatively (3s between calls = ~20 calls/min, ~26 min for a full SI3 run).
Override via `SI3_COMTRADE_DELAY_S` if you have a paid tier.

The pipeline has a **circuit breaker**: if Comtrade ever returns 403, all
subsequent Comtrade calls in that run return empty without an API call. Cells
that can't be filled from Comtrade route through the research agent — which
will gap honestly rather than fabricate values for cells like `value_add_ratio`
where the agent has no defensible source.

### 5b. IRENA PxWeb rate limits
IRENA occasionally returns 429 under burst. The pipeline retries 3× with
backoff. No knob to tune.

### 5c. Data-availability gaps
Some metrics legitimately don't exist for some countries:
- **`reserve_margin`** for AE/BR/IN/PH: regulators don't publish a clean
  planning-reserve-margin %. The pipeline tries to derive from capacity + peak
  demand but rejects results > 60% (nameplate vs derated inflation).
- **`energy_investment`** for US/AE/SG/PH: no single 5-yr-plan figure exists
  meeting the strict aggregation/dedup criteria.
- **`interconnection_queue_depth`** for AE/SG: countries don't publish this.

These are documented in `docs/SDI_METHODOLOGY.md` §9.2 as honest gaps.

### 5d. Re-running specific sub-indices
```bash
python run_all.py --only si1       # or si2 / si3 / si4
python score_pipeline.py           # always re-score after a refresh
```

---

## 6 · Configuration knobs

All in `.env`:

| Variable | Default | Purpose |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | Postgres server |
| `POSTGRES_PORT` | `5432` | Postgres port |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | (empty) | DB credentials (needs CREATEDB if you let setup.py auto-create) |
| `POSTGRES_DB` | `gramercy_workstream1` | Target database name |
| `ANTHROPIC_API_KEY` | — | Required |
| `TAVILY_API_KEY` | — | Required |
| `EIA_API_KEY` | — | Required for US SI1 |
| `DATAGOV_SG_API_KEY` | — | Required for SG SI1 |
| `COMTRADE_KEY` | — | Required for SI3 (free at comtradeplus.un.org) |
| `SI3_COMTRADE_DELAY_S` | `3.0` | Seconds between Comtrade calls |
| `SI3_COMTRADE_QUOTA_WAIT_MAX` | `600` | Cap on quota-replenishment sleep (s) |
| `SI3_SKIP_COMTRADE` | (unset) | Set to `1` to bypass Comtrade entirely |
| `WS1_PIPELINE_IDLE_TIMEOUT` | `600` | Per-pipeline watchdog (s) |
| `SI1_IRENA_INDICATOR` | `capacity` | Flip to `generation` for RE-share alternative |

---

## 7 · Methodology decisions you should know

Captured in `docs/METHODOLOGY_DECISIONS.md`:

1. **Electricity price** uses **industrial** rate for scoring; residential is collected in parallel for reference.
2. **Energy investment** is the planned 5-year figure, aggregated across credible sources, deduplicated.
3. **All currency conversions** use the FX rate on the observation's `data_date`, not the latest spot.
4. **Source filter**: social media domains (Facebook, X/Twitter, LinkedIn, Instagram, TikTok, Reddit, Quora, Pinterest) are rejected at the search layer.

Plus the full methodology paper: `docs/SDI_METHODOLOGY.md`.

---

## 8 · Support

Questions or issues: open an issue at
https://github.com/rsm-sdeenadayalan/gramercy-workstream-1/issues

For the paired CII repo: https://github.com/rsm-sdeenadayalan/gramercy-workstream-2
