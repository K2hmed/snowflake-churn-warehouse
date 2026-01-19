USE DATABASE CHURN_DB;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE CUSTOMER_CHURN_SCORES (
  run_id STRING,
  customer_id STRING,
  scored_at TIMESTAMP_NTZ,
  model_name STRING,
  churn_proba FLOAT,
  churn_pred INT
);

CREATE OR REPLACE TABLE MODEL_RUNS (
  run_id STRING,
  ran_at TIMESTAMP_NTZ,
  model_name STRING,
  train_rows INT,
  test_rows INT,
  roc_auc FLOAT,
  pr_auc FLOAT,
  chosen_threshold FLOAT,
  notes STRING
);
