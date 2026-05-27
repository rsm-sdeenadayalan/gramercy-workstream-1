# WS1 (SDI) — Production Methodology Decisions

These are durable, project-wide rules the SDI pipeline must honor. Last updated 2026-05-27.

## 1. Electricity price — industrial scored, residential stored alongside

The spec calls for "average industrial electricity cost (USD/kWh)" — only the industrial value
flows into the scoring formula (35% weight, inverted).

Residential prices are also collected and stored in `si1_raw_metrics` for transparency and
client-facing comparison, but they are **not** included in `score_metric_inputs`.

## 2. Energy investment — "planned, next 5 years," aggregated, deduplicated

The spec calls for "planned energy infrastructure investment (USD, next 5 years)". The
collector must:

- Aggregate credible authoritative sources per country (national 5-year energy strategy
  documents, IEA *World Energy Investment*, IRENA *Investment Trends*, multilateral
  development bank disclosures).
- **Deduplicate**: an announced project that appears across two sources counts once.
  Provenance for every contributing source goes in `raw_value`.
- **Reject**: WB PPI alone (private-only, misses public/utility spending), single press
  releases, aspirational "required investment" projections from think tanks (e.g. Wood
  Mackenzie net-zero pathways), social-media posts.

## 3. Currency conversion — FX rate as of the data point's date

Whenever a non-USD value is converted to USD, use the FX rate on the observation's
`data_date`. Not the latest spot rate. Not the annual average.

Store both the original local-currency value and the USD-converted value. The
`currency_conversion` field in `si1_raw_metrics` (and equivalents in si2/si3/si4)
must document the exact rate used and its date.

## 4. Source-quality filter — no social media

Sources whose domain matches `(facebook|twitter|x|linkedin|instagram)\.com` are
**not acceptable** for any metric and must be rejected by the research_agent layer.

Background: prior runs accepted an Instagram reel as the source for UAE
`energy_investment` ($43.6B). Production deliverables to clients/auditors require
sources that survive scrutiny.
