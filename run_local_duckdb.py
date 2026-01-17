import duckdb
import pandas as pd
from pathlib import Path
import csv

def read_any_delimiter(path: Path) -> pd.DataFrame:
    # Sniff delimiter from a sample
    sample = path.read_text(encoding="utf-8", errors="ignore")[:5000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        delim = dialect.delimiter
    except Exception:
        delim = "\t"  # safe default for your file

    df = pd.read_csv(path, sep=delim, engine="python")
    df.columns = [c.strip() for c in df.columns]
    return df, delim

def main():
    csv_path = Path("data/telco_churn.csv")
    if not csv_path.exists():
        raise FileNotFoundError("Missing data/telco_churn.csv")

    df, delim = read_any_delimiter(csv_path)

    print("Detected delimiter:", repr(delim))
    print("Columns detected in CSV:", df.columns.tolist())

    con = duckdb.connect(":memory:")
    con.register("raw", df)

    features = con.execute("""
        SELECT
            customerID AS customer_id,
            gender,
            CAST(SeniorCitizen AS INTEGER) AS senior_citizen,
            CASE WHEN Partner='Yes' THEN TRUE ELSE FALSE END AS has_partner,
            CASE WHEN Dependents='Yes' THEN TRUE ELSE FALSE END AS has_dependents,
            CAST(tenure AS INTEGER) AS tenure_months,
            Contract AS contract_type,
            InternetService AS internet_service,
            PaymentMethod AS payment_method,
            CASE WHEN PaperlessBilling='Yes' THEN TRUE ELSE FALSE END AS paperless_billing,
            CAST(MonthlyCharges AS DOUBLE) AS monthly_charges,
            TRY_CAST(NULLIF(TRIM(TotalCharges), '') AS DOUBLE) AS total_charges,
            CASE WHEN CAST(tenure AS INTEGER) < 6 THEN 1 ELSE 0 END AS is_new_customer,
            CASE WHEN Contract ILIKE '%Month-to-month%' THEN 1 ELSE 0 END AS is_month_to_month,
            CASE WHEN CAST(MonthlyCharges AS DOUBLE) >= 80 THEN 1 ELSE 0 END AS is_high_monthly_charges,
            CASE WHEN Churn='Yes' THEN 1 ELSE 0 END AS churn_label
        FROM raw
        WHERE customerID IS NOT NULL
    """).df()

    # Data-quality check: TotalCharges parsing
    bad_total = features["total_charges"].isna().mean()
    print(f"TotalCharges parse failure rate: {bad_total:.2%}")

    print(features.head(10))
    print("\nRows:", len(features), "Churn rate:", float(features["churn_label"].mean()))

if __name__ == "__main__":
    main()