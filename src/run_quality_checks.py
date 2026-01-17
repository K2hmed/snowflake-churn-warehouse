from __future__ import annotations
from typing import List, Tuple, Any
from src.config import get_config
from src.snowflake_client import snowflake_conn

def fetch_all(conn, sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return cols, rows
    finally:
        cur.close()

def print_result(title: str, cols: List[str], rows: List[Tuple[Any, ...]]):
    print(f"\n[{title}]")
    print(" | ".join(cols))
    for r in rows:
        print(" | ".join(str(x) for x in r))

def main():
    cfg = get_config()
    with snowflake_conn(cfg) as conn:
        cols, rows = fetch_all(conn, f"""
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT customer_id) AS distinct_customers
            FROM {cfg.database}.{cfg.analytics_schema}.CUSTOMER_CHURN_FEATURES
        """)
        print_result("Uniqueness", cols, rows)

        cols, rows = fetch_all(conn, f"""
            SELECT churn_label, COUNT(*) AS cnt
            FROM {cfg.database}.{cfg.analytics_schema}.CUSTOMER_CHURN_FEATURES
            GROUP BY 1
            ORDER BY 1
        """)
        print_result("Label distribution", cols, rows)

        cols, rows = fetch_all(conn, f"""
            SELECT
              SUM(IFF(customer_id IS NULL,1,0)) AS null_customer_id,
              SUM(IFF(tenure_months IS NULL,1,0)) AS null_tenure,
              SUM(IFF(monthly_charges IS NULL,1,0)) AS null_monthly
            FROM {cfg.database}.{cfg.analytics_schema}.CUSTOMER_CHURN_FEATURES
        """)
        print_result("Null checks", cols, rows)

        # Add the quality metric you already measured locally
        cols, rows = fetch_all(conn, f"""
            SELECT
              AVG(IFF(total_charges IS NULL, 1, 0)) AS total_charges_null_rate
            FROM {cfg.database}.{cfg.analytics_schema}.CUSTOMER_CHURN_FEATURES
        """)
        print_result("TotalCharges null rate", cols, rows)

if __name__ == "__main__":
    main()
