USE DATABASE CHURN_DB;
USE SCHEMA ANALYTICS;

-- 1) Primary key uniqueness
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT customer_id) AS distinct_customers
FROM CUSTOMER_CHURN_FEATURES;

-- 2) Label sanity
SELECT churn_label, COUNT(*) cnt
FROM CUSTOMER_CHURN_FEATURES
GROUP BY 1
ORDER BY 1;

-- 3) Null checks on critical fields
SELECT
  SUM(IFF(customer_id IS NULL,1,0)) AS null_customer_id,
  SUM(IFF(tenure_months IS NULL,1,0)) AS null_tenure,
  SUM(IFF(monthly_charges IS NULL,1,0)) AS null_monthly
FROM CUSTOMER_CHURN_FEATURES;

-- 4) TotalCharges null rate (matches local data-quality check)
SELECT
  AVG(IFF(total_charges IS NULL, 1, 0)) AS total_charges_null_rate
FROM CUSTOMER_CHURN_FEATURES;