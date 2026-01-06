
-- CO₂ EMISSIONS CAPSTONE — LAKEFLOW DLT PIPELINE
-- ============================================================
-- BRONZE LAYER — RAW INGESTION
-- ============================================================

CREATE OR REFRESH STREAMING TABLE dlt_bronze_co2
COMMENT "Bronze: Raw CO₂ emissions Delta data (DLT)"
AS
SELECT *
FROM STREAM(
  delta.`/Volumes/co2_emissions/default/vol1/co2_emissions_delta`
);


-- ============================================================
-- SILVER LAYER 1 — CLEANING & STANDARDIZATION
-- ============================================================

CREATE OR REFRESH STREAMING TABLE dlt_silver_co2_cleaned
COMMENT "Silver: Cleaned CO₂ emissions data (DLT)"
AS
SELECT
  TRIM(country) AS country,
  TRIM(region) AS region,
  TRIM(income_level) AS income_level,
  CAST(year AS INT) AS year,
  CAST(co2_emissions_mt AS DOUBLE) AS co2_emissions_mt,
  CAST(population AS DOUBLE) AS population
FROM STREAM(LIVE.dlt_bronze_co2)
WHERE year IS NOT NULL
  AND co2_emissions_mt >= 0;


-- ============================================================
-- SILVER LAYER 2 — ENRICHMENT
-- ============================================================

CREATE OR REFRESH STREAMING TABLE dlt_silver_co2_enriched
COMMENT "Silver: Enriched CO₂ data with per-capita metric (DLT)"
AS
SELECT
  COALESCE(country, 'Unknown') AS country,
  COALESCE(income_level, 'Unknown') AS income_level,
  region,
  year,
  co2_emissions_mt,
  population,
  CASE
    WHEN population > 0 THEN co2_emissions_mt / population
    ELSE NULL
  END AS co2_per_capita
FROM STREAM(LIVE.dlt_silver_co2_cleaned);


-- ============================================================
-- SILVER DIMENSION — COUNTRY–REGION FIX
-- ============================================================

CREATE MATERIALIZED VIEW dlt_silver_country_region_fixed
COMMENT "Silver: Normalized country-to-region mapping"
AS
SELECT
    country,
    FIRST(region, true) AS region
FROM LIVE.dlt_silver_co2_enriched
GROUP BY country;


-- ============================================================
-- GOLD LAYER 1 — GLOBAL YEAR-OVER-YEAR EMISSIONS
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dlt_gold_global_yoy_emissions
COMMENT "Gold: Global year-over-year CO₂ emissions trend"
AS
SELECT
  year,
  SUM(co2_emissions_mt) AS total_co2_emissions_mt
FROM LIVE.dlt_silver_co2_enriched
GROUP BY year
ORDER BY year;


-- ============================================================
-- GOLD LAYER 2 — COUNTRY-WISE YEARLY EMISSIONS
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dlt_gold_country_yearly_emissions
COMMENT "Gold: Country-wise yearly CO₂ emissions"
AS
SELECT
  country,
  year,
  SUM(co2_emissions_mt) AS total_co2_emissions_mt,
  AVG(co2_per_capita) AS avg_co2_per_capita
FROM LIVE.dlt_silver_co2_enriched
GROUP BY country, year;


-- ============================================================
-- GOLD LAYER 3 — REGIONAL EMISSIONS SUMMARY
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dlt_gold_regional_emissions_summary
COMMENT "Gold: Regional CO₂ emissions summary"
AS
SELECT
  region,
  SUM(co2_emissions_mt) AS total_co2_emissions_mt,
  AVG(co2_per_capita) AS avg_co2_per_capita
FROM LIVE.dlt_silver_co2_enriched
GROUP BY region;


-- ============================================================
-- GOLD LAYER 4 — HIGH-EMISSION REGIONS
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dlt_gold_high_emission_regions
COMMENT "Gold: Regions ranked by total CO₂ emissions"
AS
SELECT
  region,
  total_co2_emissions_mt
FROM LIVE.dlt_gold_regional_emissions_summary
ORDER BY total_co2_emissions_mt DESC;


-- ============================================================
-- GOLD LAYER 5 — POPULATION vs CO₂ EMISSIONS CORRELATION
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dlt_gold_population_emissions_correlation
COMMENT "Gold: Correlation between population and CO₂ emissions"
AS
SELECT
  country,
  CORR(population, co2_emissions_mt) AS population_co2_emissions_correlation
FROM LIVE.dlt_silver_co2_enriched
GROUP BY country;


-- ============================================================
-- GOLD LAYER 6 — INCOME-LEVEL EMISSIONS ANALYSIS
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dlt_gold_income_level_emissions
COMMENT "Gold: CO₂ emissions analysis by income level"
AS
SELECT
  income_level,
  AVG(co2_per_capita) AS avg_co2_per_capita,
  SUM(co2_emissions_mt) AS total_co2_emissions_mt
FROM LIVE.dlt_silver_co2_enriched
GROUP BY income_level;


-- ============================================================
-- GOLD LAYER 7 — COUNTRY EMISSION CLUSTERS (MAP)
-- ============================================================

CREATE MATERIALIZED VIEW dlt_gold_country_cluster_summary
COMMENT "Gold: Country-level emission clusters for map visualization"
AS
SELECT
    ct.country,
    cr.region,
    ct.total_co2_emissions_mt,
    CASE
        WHEN ct.total_co2_emissions_mt >= p.p66 THEN 'High Polluter'
        WHEN ct.total_co2_emissions_mt >= p.p33 THEN 'Moderate'
        ELSE 'Eco-Friendly'
    END AS emission_cluster
FROM
(
    SELECT
        country,
        SUM(co2_emissions_mt) AS total_co2_emissions_mt
    FROM LIVE.dlt_silver_co2_enriched
    GROUP BY country
) ct
LEFT JOIN LIVE.dlt_silver_country_region_fixed cr
    ON ct.country = cr.country
CROSS JOIN
(
    SELECT
        percentile_approx(total_co2_emissions_mt, 0.33) AS p33,
        percentile_approx(total_co2_emissions_mt, 0.66) AS p66
    FROM
    (
        SELECT
            SUM(co2_emissions_mt) AS total_co2_emissions_mt
        FROM LIVE.dlt_silver_co2_enriched
        GROUP BY country
    )
) p;
--alert spiking mechanism
CREATE MATERIALIZED VIEW dlt_gold_emission_alerts
COMMENT "Gold: Threshold-based CO₂ emission spike alerts"
AS
SELECT
    e.country,
    e.region,
    e.year,
    e.total_co2_emissions_mt,
    t.threshold_value,
    CASE
        WHEN e.total_co2_emissions_mt >= t.threshold_value
             THEN 'ALERT'
        ELSE 'NORMAL'
    END AS alert_status
FROM
(
    -- Aggregate emissions at country-year level
    SELECT
        country,
        region,
        year,
        SUM(co2_emissions_mt) AS total_co2_emissions_mt
    FROM LIVE.dlt_silver_co2_enriched
    GROUP BY country, region, year
) e
CROSS JOIN
(
    -- Calculate dynamic alert threshold (95th percentile)
    SELECT
        percentile_approx(total_co2_emissions_mt, 0.95) AS threshold_value
    FROM
    (
        SELECT
            SUM(co2_emissions_mt) AS total_co2_emissions_mt
        FROM LIVE.dlt_silver_co2_enriched
        GROUP BY country, year
    )
) t;

