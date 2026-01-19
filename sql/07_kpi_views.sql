USE DATABASE CHURN_DB;
USE SCHEMA ANALYTICS;

-- Churn by contract type
CREATE OR REPLACE VIEW V_CHURN_BY_CONTRACT AS
SELECT
  contract_type,
  COUNT(*) AS customers,
  AVG(churn_label) AS churn_rate
FROM CUSTOMER_CHURN_FEATURES
GROUP BY 1
ORDER BY churn_rate DESC;

-- Churn by tenure bucket
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

-- Churn by monthly charge band
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