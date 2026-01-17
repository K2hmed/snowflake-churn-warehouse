USE DATABASE CHURN_DB;
USE SCHEMA STG;

CREATE OR REPLACE VIEW V_TELCO_CHURN_STG AS
SELECT
  customerID                                         AS customer_id,
  gender                                             AS gender,
  TRY_TO_NUMBER(SeniorCitizen)                       AS senior_citizen,
  IFF(Partner='Yes', TRUE, FALSE)                    AS has_partner,
  IFF(Dependents='Yes', TRUE, FALSE)                 AS has_dependents,
  TRY_TO_NUMBER(tenure)                              AS tenure_months,
  PhoneService                                       AS phone_service,
  MultipleLines                                      AS multiple_lines,
  InternetService                                    AS internet_service,
  OnlineSecurity                                     AS online_security,
  OnlineBackup                                       AS online_backup,
  DeviceProtection                                   AS device_protection,
  TechSupport                                        AS tech_support,
  StreamingTV                                        AS streaming_tv,
  StreamingMovies                                    AS streaming_movies,
  Contract                                           AS contract_type,
  IFF(PaperlessBilling='Yes', TRUE, FALSE)           AS paperless_billing,
  PaymentMethod                                      AS payment_method,
  TRY_TO_DECIMAL(MonthlyCharges, 10, 2)              AS monthly_charges,
  TRY_TO_DECIMAL(NULLIF(TRIM(TotalCharges),''), 10, 2) AS total_charges,
  IFF(Churn='Yes', 1, 0)                             AS churn_label
FROM RAW.TELCO_CHURN_RAW
WHERE customerID IS NOT NULL;
