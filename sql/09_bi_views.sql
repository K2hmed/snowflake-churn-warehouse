-- ============================================================
-- CHURN_DB / ANALYTICS: Power BI semantic layer (views)
-- Fixes empty latest-score views by using RUN_ID (not timestamps)
-- Adds missing KPI snapshot view
-- ============================================================

USE DATABASE CHURN_DB;
USE SCHEMA ANALYTICS;

-- ----------------------------
-- 1) KPI snapshot (Cards in Power BI)
-- ----------------------------
CREATE OR REPLACE VIEW V_KPI_SNAPSHOT AS
SELECT
  COUNT(*) AS customer_count,
  COUNT(DISTINCT customer_id) AS distinct_customers,
  AVG(churn_label) AS churn_rate,
  AVG(IFF(total_charges IS NULL, 1, 0)) AS total_charges_null_rate,
  AVG(monthly_charges) AS avg_monthly_charges,
  AVG(tenure_months) AS avg_tenure_months
FROM CUSTOMER_CHURN_FEATURES;

-- ----------------------------
-- 2) Churn by contract
-- ----------------------------
CREATE OR REPLACE VIEW V_CHURN_BY_CONTRACT AS
SELECT
  contract_type,
  COUNT(*) AS customers,
  AVG(churn_label) AS churn_rate
FROM CUSTOMER_CHURN_FEATURES
GROUP BY 1
ORDER BY churn_rate DESC;

-- ----------------------------
-- 3) Churn by tenure bucket
-- ----------------------------
CREATE OR REPLACE VIEW V_CHURN_BY_TENURE_BUCKET AS
SELECT
  CASE
    WHEN tenure_months < 6 THEN '0-5'
    WHEN tenure_months < 12 THEN '6-11'
    WHEN tenure_months < 24 THEN '12-23'
    WHEN tenure_months < 48 THEN '24-47'
    ELSE '48+'
  END AS tenure_bucket,
  COUNT(*) AS customers,
  AVG(churn_label) AS churn_rate
FROM CUSTOMER_CHURN_FEATURES
GROUP BY 1
ORDER BY
  CASE tenure_bucket
    WHEN '0-5' THEN 1
    WHEN '6-11' THEN 2
    WHEN '12-23' THEN 3
    WHEN '24-47' THEN 4
    ELSE 5
  END;

-- ----------------------------
-- 4) Churn by monthly charges band
-- ----------------------------
CREATE OR REPLACE VIEW V_CHURN_BY_MONTHLY_CHARGE_BAND AS
SELECT
  CASE
    WHEN monthly_charges < 40 THEN '<40'
    WHEN monthly_charges < 60 THEN '40-59'
    WHEN monthly_charges < 80 THEN '60-79'
    ELSE '80+'
  END AS monthly_charge_band,
  COUNT(*) AS customers,
  AVG(churn_label) AS churn_rate
FROM CUSTOMER_CHURN_FEATURES
GROUP BY 1
ORDER BY
  CASE monthly_charge_band
    WHEN '<40' THEN 1
    WHEN '40-59' THEN 2
    WHEN '60-79' THEN 3
    ELSE 4
  END;

-- ----------------------------
-- 5) Latest model run (metadata)
-- ----------------------------
CREATE OR REPLACE VIEW V_LATEST_MODEL_RUN AS
SELECT *
FROM MODEL_RUNS
QUALIFY ran_at = MAX(ran_at) OVER ();

-- ----------------------------
-- 6) Latest churn scores (FIXED: RUN_ID-based, not timestamp-based)
-- ----------------------------
CREATE OR REPLACE VIEW V_LATEST_CHURN_SCORES AS
SELECT s.*
FROM CUSTOMER_CHURN_SCORES s
JOIN (
  SELECT run_id
  FROM MODEL_RUNS
  QUALIFY ran_at = MAX(ran_at) OVER ()
) r
ON s.run_id = r.run_id;

-- ----------------------------
-- 7) Top risk customers (derived from latest scores)
-- ----------------------------
CREATE OR REPLACE VIEW V_TOP_RISK_CUSTOMERS AS
SELECT
  customer_id,
  churn_proba,
  churn_pred,
  scored_at,
  model_name
FROM V_LATEST_CHURN_SCORES
ORDER BY churn_proba DESC
LIMIT 200;

-- ============================================================
-- Quick verification (run results should be non-empty)
-- ============================================================

-- KPI snapshot should return 1 row
SELECT 'V_KPI_SNAPSHOT' AS object_name, COUNT(*) AS row_count FROM V_KPI_SNAPSHOT;

-- Latest scores should return ~7043 rows
SELECT 'V_LATEST_CHURN_SCORES' AS object_name, COUNT(*) AS row_count FROM V_LATEST_CHURN_SCORES;

-- Top risk should return 200 rows
SELECT 'V_TOP_RISK_CUSTOMERS' AS object_name, COUNT(*) AS row_count FROM V_TOP_RISK_CUSTOMERS;

-- Peek at top 10
SELECT * FROM V_TOP_RISK_CUSTOMERS LIMIT 10;