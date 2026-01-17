USE DATABASE CHURN_DB;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE CUSTOMER_CHURN_FEATURES AS
SELECT
  customer_id,
  gender,
  senior_citizen,
  has_partner,
  has_dependents,
  tenure_months,
  contract_type,
  internet_service,
  payment_method,
  paperless_billing,
  monthly_charges,
  total_charges,

  -- Simple business features
  IFF(tenure_months < 6, 1, 0) AS is_new_customer,
  IFF(contract_type ILIKE '%Month-to-month%', 1, 0) AS is_month_to_month,
  IFF(monthly_charges >= 80, 1, 0) AS is_high_monthly_charges,

  churn_label
FROM STG.V_TELCO_CHURN_STG;
