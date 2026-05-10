-- =====================================================================
-- Jodhpur Export Intelligence System — analytical star schema
-- =====================================================================
-- Target: PostgreSQL 16 (Supabase free tier, region ap-south-1)
-- Design: Kimball-style star schema, optimised for the Power BI
--         GROUP BY / window-function workload described in PRD §5.3.
--
-- Idempotency: every CREATE statement is IF NOT EXISTS. Running this
-- script twice produces no errors and no duplicate objects.
--
-- Why a star schema (not 3NF, not a Data Vault)?
--   * The query workload is OLAP — aggregations across time, country,
--     product. A star schema is the canonical fit: fewer joins per
--     dashboard query, predictable cardinality, easy for Power BI to
--     auto-detect relationships.
--   * 3NF would force Power BI to navigate 4-5 hop join paths for every
--     panel — slow and error-prone.
--   * Data Vault would be over-engineered: we have one source of truth
--     per entity, no need for hub/satellite versioning at this scale.
-- =====================================================================

-- ----- 0. Sanity ----------------------------------------------------
SET client_min_messages = WARNING;
SET search_path = public;

-- ----- 1. Reference / dimension tables ------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id     SERIAL      PRIMARY KEY,
    iso_alpha3     CHAR(3)     NOT NULL UNIQUE,    -- ISO 3166-1 alpha-3
    country_name   TEXT        NOT NULL,
    region         TEXT,                            -- e.g. 'Europe', 'North America'
    cluster_label  TEXT,                            -- written by 03_market_segmentation.ipynb
    last_updated   TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE dim_country IS
  'Destination country dimension. cluster_label is rewritten by the K-means '
  'segmentation notebook on every weekly run.';

CREATE TABLE IF NOT EXISTS dim_product (
    product_id    SERIAL      PRIMARY KEY,
    hs_code       VARCHAR(10) NOT NULL UNIQUE,     -- 6 or 8 digit HS 2017
    hs_desc       TEXT        NOT NULL,
    category      TEXT        NOT NULL,            -- 'Handicraft-Wood', 'Guar-Refined', ...
    last_updated  TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE dim_product IS
  'HS code dimension. Limited to PRD §8.1 codes: 440900, 330749, 442090, '
  '130232, 130239. Extensible if scope expands.';

CREATE TABLE IF NOT EXISTS dim_company (
    company_id        SERIAL      PRIMARY KEY,
    company_name      TEXT        NOT NULL,
    company_type      TEXT,                          -- 'EXPORTER' | 'BUYER'
    country_id        INTEGER     REFERENCES dim_country(country_id),
    risk_score        NUMERIC(4,2),                  -- 1.00 - 10.00, written by 05_buyer_risk.ipynb
    avg_payment_days  NUMERIC(6,1),
    last_updated      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (company_name, company_type)
);
COMMENT ON TABLE dim_company IS
  'Both Indian exporters and overseas buyers. company_type discriminates.';

CREATE TABLE IF NOT EXISTS dim_time (
    time_id          INTEGER     PRIMARY KEY,        -- YYYYMMDD as integer
    date             DATE        NOT NULL UNIQUE,
    year             SMALLINT    NOT NULL,
    month            SMALLINT    NOT NULL,
    quarter          SMALLINT    NOT NULL,
    financial_year   VARCHAR(7)  NOT NULL,           -- 'FY24-25' (Apr-Mar IST)
    is_peak_season   BOOLEAN     NOT NULL,           -- TRUE for Sep–Nov per PRD §5.4
    iso_week         SMALLINT    NOT NULL
);
COMMENT ON TABLE dim_time IS
  'Pre-populated calendar 2018-01-01 → 2026-12-31. is_peak_season encodes the '
  'pre-Christmas demand window identified by EPCH.';

-- ----- 2. Fact table -----------------------------------------------
CREATE TABLE IF NOT EXISTS fact_shipment (
    shipment_id      BIGSERIAL   PRIMARY KEY,
    shipment_date    DATE        NOT NULL,
    time_id          INTEGER     NOT NULL REFERENCES dim_time(time_id),
    exporter_id      INTEGER     REFERENCES dim_company(company_id),
    buyer_id         INTEGER     REFERENCES dim_company(company_id),
    product_id       INTEGER     NOT NULL REFERENCES dim_product(product_id),
    dest_country_id  INTEGER     NOT NULL REFERENCES dim_country(country_id),
    fob_usd          NUMERIC(14,2) NOT NULL CHECK (fob_usd >= 0 AND fob_usd <= 10000000),
    quantity_kg      NUMERIC(14,3) NOT NULL CHECK (quantity_kg > 0),
    unit_price_usd   NUMERIC(12,4) NOT NULL CHECK (unit_price_usd > 0),
    is_outlier       BOOLEAN     NOT NULL DEFAULT FALSE,
    source           TEXT        NOT NULL,           -- 'COMTRADE' | 'VOLZA' | 'ZAUBA'
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Composite natural key for dedup (PRD §8.3)
    UNIQUE (shipment_date, exporter_id, buyer_id, product_id, fob_usd)
);
COMMENT ON TABLE fact_shipment IS
  'Grain: one shipment record. Some sources (Comtrade) provide aggregated '
  'monthly flows; in those cases shipment_date is the first of the month and '
  'exporter_id is NULL.';

-- ----- 3. Indexes (analytical workload) ----------------------------
CREATE INDEX IF NOT EXISTS idx_fact_shipment_time      ON fact_shipment (time_id);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_country   ON fact_shipment (dest_country_id);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_product   ON fact_shipment (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_buyer     ON fact_shipment (buyer_id);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_date      ON fact_shipment (shipment_date);

-- Covering index for the most common Power BI query: shipment value by date + country.
-- We index on shipment_date itself (not date_trunc('month', ...)) because Postgres
-- forbids STABLE functions in index expressions — date_trunc is STABLE, not IMMUTABLE,
-- since its output technically depends on the session timezone for timestamptz inputs.
-- The query planner uses this index for monthly aggregations via range scans on
-- shipment_date, so we lose nothing in practice.
CREATE INDEX IF NOT EXISTS idx_fact_shipment_date_country
    ON fact_shipment (shipment_date, dest_country_id)
    INCLUDE (fob_usd, quantity_kg);

-- ----- 4. Auxiliary tables (non-shipment) --------------------------
CREATE TABLE IF NOT EXISTS rig_count_weekly (
    week_start_date  DATE        PRIMARY KEY,
    rig_count        INTEGER     NOT NULL CHECK (rig_count >= 0),
    region           TEXT        NOT NULL DEFAULT 'NORTH_AMERICA',
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monsoon_yearly (
    year             SMALLINT    NOT NULL,
    state            TEXT        NOT NULL,           -- 'RAJASTHAN'
    rainfall_mm      NUMERIC(7,1) NOT NULL,
    lpa_pct          NUMERIC(5,1) NOT NULL,           -- % of long period average
    PRIMARY KEY (year, state)
);

-- ----- 5. Pipeline run audit log -----------------------------------
CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id           BIGSERIAL   PRIMARY KEY,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    status           TEXT        NOT NULL DEFAULT 'RUNNING',  -- RUNNING | SUCCESS | FAILED
    rows_ingested    INTEGER,
    rows_loaded      INTEGER,
    failed_step      TEXT,
    error_message    TEXT,
    git_sha          VARCHAR(40)
);
COMMENT ON TABLE pipeline_run IS
  'Audit row per pipeline invocation. Used by the dashboard staleness '
  'indicator (PRD §6) and the failure-alert email.';

-- ----- 6. Useful analytical views ----------------------------------

-- Monthly aggregate by country & product — used by Power BI seasonal panel.
CREATE OR REPLACE VIEW v_monthly_export AS
SELECT
    date_trunc('month', f.shipment_date)::DATE AS month,
    dc.iso_alpha3,
    dc.country_name,
    dp.hs_code,
    dp.category,
    SUM(f.fob_usd)        AS total_fob_usd,
    SUM(f.quantity_kg)    AS total_quantity_kg,
    AVG(f.unit_price_usd) AS avg_unit_price_usd,
    COUNT(*)              AS shipment_count
FROM fact_shipment f
JOIN dim_country dc ON dc.country_id = f.dest_country_id
JOIN dim_product dp ON dp.product_id = f.product_id
GROUP BY 1, 2, 3, 4, 5;

COMMENT ON VIEW v_monthly_export IS
  'Power BI primary dataset. Materialised as a view (not a matview) because '
  'fact_shipment is small (<10M rows expected at horizon).';

-- High-risk buyer register — used by Power BI risk panel.
CREATE OR REPLACE VIEW v_buyer_risk_register AS
SELECT
    c.company_id,
    c.company_name,
    dc.country_name AS buyer_country,
    c.risk_score,
    c.avg_payment_days,
    SUM(f.fob_usd) FILTER (WHERE f.shipment_date >= CURRENT_DATE - INTERVAL '12 months') AS fob_last_12m,
    COUNT(*) FILTER (WHERE f.shipment_date >= CURRENT_DATE - INTERVAL '12 months') AS shipments_last_12m
FROM dim_company c
LEFT JOIN dim_country dc ON dc.country_id = c.country_id
LEFT JOIN fact_shipment f ON f.buyer_id = c.company_id
WHERE c.company_type = 'BUYER'
  AND c.risk_score IS NOT NULL
GROUP BY c.company_id, c.company_name, dc.country_name, c.risk_score, c.avg_payment_days;
