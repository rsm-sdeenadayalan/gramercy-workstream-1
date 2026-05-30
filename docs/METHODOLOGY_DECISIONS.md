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

## 5. SI1 reweight — 5th metric (energy_import_dependency)

Added 2026-05-29 per client direction. New SI1 weights (sum to 1.00):

| Metric | Old weight | New weight | Source / definition |
|---|---:|---:|---|
| `electricity_price` (industrial) | 0.35 | 0.35 | EIA / GPP — inverted |
| `renewable_share` | 0.30 | **0.20** | IRENA RE-SHARE (capacity) |
| `reserve_margin` | 0.20 | 0.20 | Planning reserve margin (NERC-style) |
| `energy_investment` | 0.15 | **0.10** | Aggregate 5-yr planned investment |
| `energy_import_dependency` | — | **0.15** | **NEW** — WB EG.IMP.CONS.ZS (IEA-sourced), inverted |

The new metric captures *substrate sovereignty* — net energy imports as % of
energy use. Negative = net exporter (sovereign, high score after inversion);
positive = net importer (vulnerable, low score). Re-weight pulls from the two
metrics least defensible at their old weight (renewable_share over-rewarded
hydro-rich countries; energy_investment had cross-country comparability issues).

## 6. Solid backing — every value must be source-citable

Every stored row must have:
- A single citable `source_url`
- A `raw_value` field containing a literal quote from that source (or, for
  derived metrics, the components used in the computation)
- A `confidence_score` reflecting the source authority tier

The research agent's synthesis step rejects values that cannot be backed by a
literal source quote (validator: `EnergyResearchAgent._raw_text_supports_value`).
For computed metrics (`reserve_margin` derived, `share_global_staple_exports`
derived), the `conversion_note` field must show every component value and the
URL it came from.
