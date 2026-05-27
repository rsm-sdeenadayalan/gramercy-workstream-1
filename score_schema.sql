-- ─────────────────────────────────────────────────────────────────────────────
-- Chessboard Sovereign Index — Scoring Database (csi_scores)
--
-- Reads latest values from subindex_1..4 source DBs, applies min-max
-- normalization (0-100) per metric across the 6 target countries, applies
-- inversions where specified, then composes weighted sub-index scores and
-- the final SDI per the project methodology.
--
-- Apply with:  psql -d csi_scores -f score_schema.sql
-- (or use python setup.py)
-- ─────────────────────────────────────────────────────────────────────────────

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. METHODOLOGY CONFIG (weights + inversions per sub-index)
--    These rows drive the scoring math; tweak them here, no code change needed.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS score_methodology (
    id              SERIAL PRIMARY KEY,
    sub_index       VARCHAR(10) NOT NULL,    -- SI1 | SI2 | SI3 | SI4
    metric_key      VARCHAR(100) NOT NULL,
    weight          NUMERIC(5,4) NOT NULL,   -- within-sub-index weight (sum to 1)
    invert          BOOLEAN NOT NULL DEFAULT FALSE,  -- TRUE if lower raw = higher score
    notes           TEXT,
    UNIQUE (sub_index, metric_key),
    CHECK (weight >= 0 AND weight <= 1)
);

-- Per-mineral weights for SI3 aggregation
CREATE TABLE IF NOT EXISTS score_mineral_weights (
    mineral         VARCHAR(50) PRIMARY KEY,
    weight          NUMERIC(5,4) NOT NULL,
    notes           TEXT,
    CHECK (weight >= 0 AND weight <= 1)
);

-- Final SDI weights per sub-index
CREATE TABLE IF NOT EXISTS score_subindex_weights (
    sub_index       VARCHAR(10) PRIMARY KEY,
    weight          NUMERIC(5,4) NOT NULL,
    label           VARCHAR(100),
    CHECK (weight >= 0 AND weight <= 1)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. INPUT SNAPSHOTS (raw values pulled from source DBs)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS score_metric_inputs (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    country_iso     CHAR(2) NOT NULL,
    sub_index       VARCHAR(10) NOT NULL,
    metric_key      VARCHAR(100) NOT NULL,
    mineral         VARCHAR(50),               -- only set for SI3
    raw_value       DOUBLE PRECISION,
    unit            VARCHAR(50),
    data_date       DATE,
    source_db       VARCHAR(50),
    confidence      DOUBLE PRECISION,
    pulled_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_iso, sub_index, metric_key, mineral)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. NORMALIZED + WEIGHTED METRIC SCORES (0-100)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS score_metric_normalized (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    country_iso     CHAR(2) NOT NULL,
    sub_index       VARCHAR(10) NOT NULL,
    metric_key      VARCHAR(100) NOT NULL,
    mineral         VARCHAR(50),
    raw_value       DOUBLE PRECISION,
    normalized      DOUBLE PRECISION NOT NULL,    -- 0-100, post-inversion
    inverted        BOOLEAN NOT NULL DEFAULT FALSE,
    weight          DOUBLE PRECISION NOT NULL,
    weighted_score  DOUBLE PRECISION NOT NULL,    -- normalized × weight
    computed_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_iso, sub_index, metric_key, mineral)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. PER-MINERAL SCORES (SI3 only — intermediate step)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS score_mineral (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    country_iso     CHAR(2) NOT NULL,
    mineral         VARCHAR(50) NOT NULL,
    mineral_score   DOUBLE PRECISION NOT NULL,    -- 0-100, weighted sum of 3 metrics
    mineral_weight  DOUBLE PRECISION NOT NULL,
    weighted_score  DOUBLE PRECISION NOT NULL,    -- mineral_score × mineral_weight
    computed_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_iso, mineral)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. SUB-INDEX COMPOSITE SCORES
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS score_subindex (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    country_iso     CHAR(2) NOT NULL,
    sub_index       VARCHAR(10) NOT NULL,
    score           DOUBLE PRECISION NOT NULL,    -- 0-100
    weight          DOUBLE PRECISION NOT NULL,    -- sub-index weight in final SDI
    weighted_score  DOUBLE PRECISION NOT NULL,
    data_date_min   DATE,                          -- oldest underlying metric date
    data_date_max   DATE,                          -- newest underlying metric date
    computed_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_iso, sub_index)
);
ALTER TABLE score_subindex ADD COLUMN IF NOT EXISTS data_date_min DATE;
ALTER TABLE score_subindex ADD COLUMN IF NOT EXISTS data_date_max DATE;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. FINAL SDI (Sovereign Development Index)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS score_sdi (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    country_iso     CHAR(2) NOT NULL UNIQUE,
    si1_energy      DOUBLE PRECISION,
    si2_water       DOUBLE PRECISION,
    si3_minerals    DOUBLE PRECISION,
    si4_food        DOUBLE PRECISION,
    sdi_score       DOUBLE PRECISION NOT NULL,
    rank            INTEGER,
    data_date_min   DATE,                          -- oldest underlying metric date
    data_date_max   DATE,                          -- newest underlying metric date
    computed_at     TIMESTAMP DEFAULT NOW()
);
ALTER TABLE score_sdi ADD COLUMN IF NOT EXISTS data_date_min DATE;
ALTER TABLE score_sdi ADD COLUMN IF NOT EXISTS data_date_max DATE;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. RUN LOG
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS score_runs (
    id              SERIAL PRIMARY KEY,
    run_uuid        UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    status          VARCHAR(20) DEFAULT 'running',  -- running | success | failed
    countries_scored INTEGER DEFAULT 0,
    notes           TEXT,
    CHECK (status IN ('running','success','partial','failed'))
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. CONVENIENCE VIEW — final ranked SDI table
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_sdi_ranked AS
SELECT country_iso,
       ROUND(si1_energy::numeric,   2) AS energy,
       ROUND(si2_water::numeric,    2) AS water,
       ROUND(si3_minerals::numeric, 2) AS minerals,
       ROUND(si4_food::numeric,     2) AS food,
       ROUND(sdi_score::numeric,    2) AS sdi,
       RANK() OVER (ORDER BY sdi_score DESC NULLS LAST) AS rank,
       data_date_min  AS as_of_oldest,
       data_date_max  AS as_of_newest,
       computed_at
FROM score_sdi
ORDER BY sdi_score DESC NULLS LAST;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. SEED — METHODOLOGY CONFIG
-- ─────────────────────────────────────────────────────────────────────────────
-- Some metrics are collected by the pipelines but intentionally NOT scored:
--   SI1: grid_capacity, interconnection_queue_depth — surfaced for dashboards/
--        diagnostics; the 4 metrics below sum to weight 1.00 by design.
--   SI2: projected_water_stress_2050 — raw input used to derive the scored
--        projected_water_stress_change (= 2050 − baseline).
-- Re-add them here only if you also re-balance the existing weights.

-- SI1 — Energy Substrate (35% of SDI)
INSERT INTO score_methodology (sub_index, metric_key, weight, invert, notes) VALUES
    ('SI1', 'electricity_price',  0.35, TRUE,  'Lower cost → higher score'),
    ('SI1', 'renewable_share',    0.30, FALSE, NULL),
    ('SI1', 'reserve_margin',     0.20, FALSE, NULL),
    ('SI1', 'energy_investment',  0.15, FALSE, 'Planned 5-yr energy infrastructure investment')
ON CONFLICT (sub_index, metric_key) DO UPDATE SET
    weight = EXCLUDED.weight, invert = EXCLUDED.invert, notes = EXCLUDED.notes;

-- SI2 — Water Availability (20% of SDI)
INSERT INTO score_methodology (sub_index, metric_key, weight, invert, notes) VALUES
    ('SI2', 'freshwater_per_capita',          0.30, FALSE, 'Per-capita resources'),
    ('SI2', 'baseline_water_stress',          0.40, TRUE,  'Lower stress → higher score'),
    ('SI2', 'projected_water_stress_change',  0.20, TRUE,  'Smaller increase → higher score'),
    ('SI2', 'regulatory_restrictions_score',  0.10, FALSE, 'Higher governance score → higher score')
ON CONFLICT (sub_index, metric_key) DO UPDATE SET
    weight = EXCLUDED.weight, invert = EXCLUDED.invert, notes = EXCLUDED.notes;

-- SI3 — Critical Minerals (30% of SDI). Per-mineral weights below.
-- Within each mineral: 0.40 prod_share + 0.30 reserves_share + 0.30 refining_share
INSERT INTO score_methodology (sub_index, metric_key, weight, invert, notes) VALUES
    ('SI3', 'production_share',  0.40, FALSE, 'Within-mineral weight'),
    ('SI3', 'reserves_share',    0.30, FALSE, 'Within-mineral weight'),
    ('SI3', 'refining_share',    0.30, FALSE, 'Within-mineral weight')
ON CONFLICT (sub_index, metric_key) DO UPDATE SET
    weight = EXCLUDED.weight, invert = EXCLUDED.invert, notes = EXCLUDED.notes;

-- SI4 — Food Security (15% of SDI)
INSERT INTO score_methodology (sub_index, metric_key, weight, invert, notes) VALUES
    ('SI4', 'net_food_trade_balance',         0.30, FALSE, 'Min-max handles negative values'),
    ('SI4', 'caloric_self_sufficiency_ratio', 0.30, FALSE, NULL),
    ('SI4', 'share_global_staple_exports',    0.20, FALSE, NULL),
    ('SI4', 'arable_land_per_capita',         0.20, FALSE, NULL)
ON CONFLICT (sub_index, metric_key) DO UPDATE SET
    weight = EXCLUDED.weight, invert = EXCLUDED.invert, notes = EXCLUDED.notes;

-- SI3 mineral weights (AI-infrastructure relevance)
INSERT INTO score_mineral_weights (mineral, weight, notes) VALUES
    ('copper',      0.30, 'AI / power infrastructure backbone'),
    ('lithium',     0.20, 'Battery storage'),
    ('nickel',      0.15, 'Battery cathodes'),
    ('cobalt',      0.15, 'Battery cathodes'),
    ('rare_earths', 0.10, 'Magnets, semiconductors'),
    ('silicon',     0.10, 'Chips, solar')
ON CONFLICT (mineral) DO UPDATE SET
    weight = EXCLUDED.weight, notes = EXCLUDED.notes;

-- Final SDI weights per sub-index
INSERT INTO score_subindex_weights (sub_index, weight, label) VALUES
    ('SI1', 0.35, 'Energy Substrate'),
    ('SI2', 0.20, 'Water Availability'),
    ('SI3', 0.30, 'Critical Mineral Endowment'),
    ('SI4', 0.15, 'Food and Agricultural Security')
ON CONFLICT (sub_index) DO UPDATE SET
    weight = EXCLUDED.weight, label = EXCLUDED.label;

COMMIT;
