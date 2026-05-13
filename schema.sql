-- ─────────────────────────────────────────────────────────────────────────────
-- Gramercy Sub-Index Pipelines — database schema
--
-- Three databases on the same PostgreSQL server, one per sub-index:
--   subindex_1  →  Energy        (tables/views si1_*)
--   subindex_2  →  Water         (tables/views si2_*)
--   subindex_4  →  Food          (tables/views si4_*)
--
-- Apply once per database. Tables use IF NOT EXISTS so this is safe to re-run.
--
-- Apply with:
--     psql -h localhost -p 5433 -U <user> -d subindex_1 -f schema.sql
--     psql -h localhost -p 5433 -U <user> -d subindex_2 -f schema.sql
--     psql -h localhost -p 5433 -U <user> -d subindex_4 -f schema.sql
-- ─────────────────────────────────────────────────────────────────────────────


-- ═════════════════════════════════════════════════════════════════════════════
-- SI1 — ENERGY  (subindex_1)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS si1_raw_metrics (
    id                  SERIAL PRIMARY KEY,
    country_iso         CHAR(2)           NOT NULL,
    country_name        VARCHAR(100)      NOT NULL,
    metric_key          VARCHAR(100)      NOT NULL,
    metric_label        VARCHAR(200),
    metric_value        DOUBLE PRECISION  NOT NULL,
    unit                VARCHAR(50),
    data_date           DATE              NOT NULL,
    data_frequency      VARCHAR(20),
    source_name         VARCHAR(200),
    source_url          TEXT,
    access_method       VARCHAR(50),
    confidence_score    DOUBLE PRECISION,
    raw_value           TEXT,
    currency_conversion TEXT,
    is_imputed          BOOLEAN           DEFAULT FALSE,
    run_id              UUID,
    collected_at        TIMESTAMP         DEFAULT NOW(),
    UNIQUE (country_iso, metric_key, data_date, source_name)
);

CREATE TABLE IF NOT EXISTS si1_collection_log (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    country_iso     CHAR(2),
    metric_key      VARCHAR(100),
    collector_name  VARCHAR(100),
    cascade_step    INTEGER,
    status          VARCHAR(20),
    source_url      TEXT,
    error_type      VARCHAR(100),
    error_message   TEXT,
    duration_ms     INTEGER,
    attempted_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS si1_data_gaps (
    id               SERIAL PRIMARY KEY,
    country_iso      CHAR(2),
    country_name     VARCHAR(100),
    metric_key       VARCHAR(100),
    metric_label     VARCHAR(200),
    failure_reason   TEXT,
    collectors_tried TEXT[],
    severity         VARCHAR(20),
    first_detected   TIMESTAMP DEFAULT NOW(),
    last_attempted   TIMESTAMP DEFAULT NOW(),
    attempt_count    INTEGER   DEFAULT 1,
    status           VARCHAR(20) DEFAULT 'open',
    UNIQUE (country_iso, metric_key)
);

CREATE TABLE IF NOT EXISTS si1_collection_runs (
    run_id       UUID PRIMARY KEY,
    started_at   TIMESTAMP DEFAULT NOW(),
    finished_at  TIMESTAMP,
    total_tasks  INTEGER DEFAULT 0,
    succeeded    INTEGER DEFAULT 0,
    failed       INTEGER DEFAULT 0,
    gaps_opened  INTEGER DEFAULT 0
);

CREATE OR REPLACE VIEW v_si1_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key, metric_label,
    metric_value, unit, data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si1_raw_metrics
ORDER BY country_iso, metric_key, data_date DESC, collected_at DESC;

CREATE OR REPLACE VIEW v_si1_completeness AS
SELECT
    country_iso, country_name, metric_key,
    COUNT(*)                              AS total_rows,
    MAX(collected_at)                     AS last_collected,
    ROUND(AVG(confidence_score)::numeric, 3) AS avg_confidence,
    CASE WHEN COUNT(*) > 0 THEN 100.0 ELSE 0.0 END AS coverage_pct
FROM si1_raw_metrics
GROUP BY country_iso, country_name, metric_key
ORDER BY country_iso, metric_key;


-- ═════════════════════════════════════════════════════════════════════════════
-- SI2 — WATER  (subindex_2)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS si2_raw_metrics (
    id                 SERIAL PRIMARY KEY,
    country_iso        CHAR(2) NOT NULL,
    country_name       VARCHAR(120) NOT NULL,
    metric_key         VARCHAR(120) NOT NULL,
    metric_label       VARCHAR(255) NOT NULL,
    metric_value       DOUBLE PRECISION NOT NULL,
    unit               VARCHAR(80) NOT NULL,
    data_date          DATE NOT NULL,
    data_frequency     VARCHAR(80) NOT NULL,
    source_name        VARCHAR(255) NOT NULL,
    source_url         TEXT,
    access_method      VARCHAR(80) NOT NULL,
    confidence_score   DOUBLE PRECISION NOT NULL,
    raw_value          TEXT,
    is_manual_override BOOLEAN DEFAULT FALSE,
    override_note      TEXT,
    run_id             UUID,
    collected_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_iso, metric_key, data_date, source_name)
);

CREATE TABLE IF NOT EXISTS si2_collection_runs (
    run_id       UUID PRIMARY KEY,
    started_at   TIMESTAMP DEFAULT NOW(),
    finished_at  TIMESTAMP,
    total_tasks  INTEGER DEFAULT 0,
    succeeded    INTEGER DEFAULT 0,
    failed       INTEGER DEFAULT 0,
    gaps_opened  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS si2_collection_log (
    id             SERIAL PRIMARY KEY,
    run_id         UUID,
    country_iso    CHAR(2),
    metric_key     VARCHAR(120),
    collector_name VARCHAR(120),
    cascade_step   INTEGER,
    status         VARCHAR(20),
    source_url     TEXT,
    error_type     VARCHAR(120),
    error_message  TEXT,
    duration_ms    INTEGER,
    attempted_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS si2_data_gaps (
    id               SERIAL PRIMARY KEY,
    country_iso      CHAR(2),
    country_name     VARCHAR(120),
    metric_key       VARCHAR(120),
    metric_label     VARCHAR(255),
    failure_reason   TEXT,
    collectors_tried TEXT[],
    severity         VARCHAR(20) DEFAULT 'medium',
    first_detected   TIMESTAMP DEFAULT NOW(),
    last_attempted   TIMESTAMP DEFAULT NOW(),
    attempt_count    INTEGER DEFAULT 1,
    status           VARCHAR(20) DEFAULT 'open',
    UNIQUE (country_iso, metric_key)
);

CREATE OR REPLACE VIEW v_si2_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key, metric_label,
    metric_value, unit, data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si2_raw_metrics
ORDER BY country_iso, metric_key, data_date DESC, collected_at DESC;


-- ═════════════════════════════════════════════════════════════════════════════
-- SI4 — FOOD  (subindex_4)
-- Trade values are split into si4_food_trade_raw (exports/imports/balance);
-- other metrics live in si4_raw_metrics with a single value column.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS si4_food_trade_raw (
    id                  SERIAL PRIMARY KEY,
    country_iso         CHAR(2)           NOT NULL,
    country_name        VARCHAR(100)      NOT NULL,
    metric_key          VARCHAR(100)      NOT NULL,
    exports_usd         DOUBLE PRECISION,
    imports_usd         DOUBLE PRECISION,
    trade_balance_usd   DOUBLE PRECISION,
    data_date           DATE              NOT NULL,
    data_frequency      VARCHAR(20),
    source_name         VARCHAR(200),
    source_url          TEXT,
    access_method       VARCHAR(50),
    confidence_score    DOUBLE PRECISION,
    run_id              UUID,
    collected_at        TIMESTAMP         DEFAULT NOW(),
    UNIQUE (country_iso, metric_key, data_date, source_name)
);

CREATE TABLE IF NOT EXISTS si4_raw_metrics (
    id                  SERIAL PRIMARY KEY,
    country_iso         CHAR(2)           NOT NULL,
    country_name        VARCHAR(100)      NOT NULL,
    metric_key          VARCHAR(100)      NOT NULL,
    metric_label        VARCHAR(200),
    metric_value        DOUBLE PRECISION  NOT NULL,
    unit                VARCHAR(50),
    data_date           DATE              NOT NULL,
    data_frequency      VARCHAR(20),
    source_name         VARCHAR(200),
    source_url          TEXT,
    access_method       VARCHAR(50),
    confidence_score    DOUBLE PRECISION,
    raw_value           TEXT,
    is_imputed          BOOLEAN           DEFAULT FALSE,
    run_id              UUID,
    collected_at        TIMESTAMP         DEFAULT NOW(),
    UNIQUE (country_iso, metric_key, data_date, source_name)
);

CREATE TABLE IF NOT EXISTS si4_collection_log (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    country_iso     CHAR(2),
    metric_key      VARCHAR(100),
    collector_name  VARCHAR(100),
    cascade_step    INTEGER,
    status          VARCHAR(20),
    source_url      TEXT,
    error_type      VARCHAR(100),
    error_message   TEXT,
    duration_ms     INTEGER,
    attempted_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS si4_data_gaps (
    id               SERIAL PRIMARY KEY,
    country_iso      CHAR(2),
    country_name     VARCHAR(100),
    metric_key       VARCHAR(100),
    metric_label     VARCHAR(200),
    failure_reason   TEXT,
    collectors_tried TEXT[],
    severity         VARCHAR(20),
    first_detected   TIMESTAMP DEFAULT NOW(),
    last_attempted   TIMESTAMP DEFAULT NOW(),
    attempt_count    INTEGER   DEFAULT 1,
    status           VARCHAR(20) DEFAULT 'open',
    UNIQUE (country_iso, metric_key)
);

CREATE TABLE IF NOT EXISTS si4_collection_runs (
    run_id       UUID PRIMARY KEY,
    started_at   TIMESTAMP DEFAULT NOW(),
    finished_at  TIMESTAMP,
    total_tasks  INTEGER DEFAULT 0,
    succeeded    INTEGER DEFAULT 0,
    failed       INTEGER DEFAULT 0,
    gaps_opened  INTEGER DEFAULT 0
);

CREATE OR REPLACE VIEW v_si4_trade_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key,
    exports_usd, imports_usd, trade_balance_usd,
    data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si4_food_trade_raw
ORDER BY country_iso, metric_key, data_date DESC, collected_at DESC;

CREATE OR REPLACE VIEW v_si4_latest AS
SELECT DISTINCT ON (country_iso, metric_key)
    country_iso, country_name, metric_key, metric_label,
    metric_value, unit, data_date, data_frequency,
    source_name, source_url, confidence_score, collected_at
FROM si4_raw_metrics
ORDER BY country_iso, metric_key, data_date DESC, collected_at DESC;

CREATE OR REPLACE VIEW v_si4_completeness AS
SELECT country_iso, country_name, metric_key,
    COUNT(*) AS total_rows, MAX(collected_at) AS last_collected,
    ROUND(AVG(confidence_score)::numeric, 3) AS avg_confidence
FROM si4_food_trade_raw
GROUP BY country_iso, country_name, metric_key
UNION ALL
SELECT country_iso, country_name, metric_key,
    COUNT(*), MAX(collected_at),
    ROUND(AVG(confidence_score)::numeric, 3)
FROM si4_raw_metrics
GROUP BY country_iso, country_name, metric_key
ORDER BY country_iso, metric_key;
