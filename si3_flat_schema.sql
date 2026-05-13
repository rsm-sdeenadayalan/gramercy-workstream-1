-- Flat pipeline tables for SI3 (mirrors the SI1/SI2/SI4 schema pattern).
-- The normalized si3_raw_metrics / si3_annual_metrics tables remain untouched.

CREATE TABLE IF NOT EXISTS si3_pipeline_metrics (
    id               SERIAL           PRIMARY KEY,
    country_iso      CHAR(2)          NOT NULL,
    country_name     VARCHAR(100)     NOT NULL,
    metric_key       VARCHAR(100)     NOT NULL,
    metric_label     TEXT,
    mineral          VARCHAR(50),
    metric_value     DOUBLE PRECISION NOT NULL,
    unit             VARCHAR(50),
    data_date        DATE,
    data_frequency   VARCHAR(20),
    source_name      VARCHAR(200),
    source_url       TEXT,
    access_method    VARCHAR(50),
    confidence_score DOUBLE PRECISION,
    raw_value        TEXT,
    flag             VARCHAR(10),
    is_imputed       BOOLEAN          NOT NULL DEFAULT FALSE,
    run_id           TEXT,
    collected_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    UNIQUE (country_iso, metric_key, mineral, data_date, source_name)
);

CREATE INDEX IF NOT EXISTS idx_si3_pm_country_metric
    ON si3_pipeline_metrics (country_iso, metric_key, mineral, collected_at DESC);

CREATE TABLE IF NOT EXISTS si3_pipeline_log (
    id             SERIAL      PRIMARY KEY,
    run_id         TEXT,
    country_iso    CHAR(2),
    metric_key     VARCHAR(100),
    mineral        VARCHAR(50),
    collector_name VARCHAR(200),
    cascade_step   INT,
    status         VARCHAR(20),
    source_url     TEXT,
    error_type     VARCHAR(100),
    error_message  TEXT,
    duration_ms    INT,
    logged_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS si3_pipeline_gaps (
    id               SERIAL      PRIMARY KEY,
    country_iso      CHAR(2)     NOT NULL,
    country_name     VARCHAR(100),
    metric_key       VARCHAR(100) NOT NULL,
    mineral          VARCHAR(50),
    metric_label     TEXT,
    failure_reason   TEXT,
    collectors_tried TEXT[],
    severity         VARCHAR(20),
    status           VARCHAR(20)  NOT NULL DEFAULT 'open',
    last_attempted   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    attempt_count    INT          NOT NULL DEFAULT 1,
    UNIQUE (country_iso, metric_key, mineral)
);

CREATE TABLE IF NOT EXISTS si3_pipeline_runs (
    id         SERIAL      PRIMARY KEY,
    run_id     TEXT        NOT NULL UNIQUE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);
