USE DATABASE CHURN_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE TELCO_CHURN_RAW (
  customerID STRING,
  gender STRING,
  SeniorCitizen STRING,
  Partner STRING,
  Dependents STRING,
  tenure STRING,
  PhoneService STRING,
  MultipleLines STRING,
  InternetService STRING,
  OnlineSecurity STRING,
  OnlineBackup STRING,
  DeviceProtection STRING,
  TechSupport STRING,
  StreamingTV STRING,
  StreamingMovies STRING,
  Contract STRING,
  PaperlessBilling STRING,
  PaymentMethod STRING,
  MonthlyCharges STRING,
  TotalCharges STRING,
  Churn STRING
);
