from pathlib import Path
from src.config import get_config
from src.snowflake_client import snowflake_conn, exec_sql

SQL_SETUP = Path("sql/00_setup.sql").read_text(encoding="utf-8")
SQL_STAGE = Path("sql/01_file_formats_and_stage.sql").read_text(encoding="utf-8")
SQL_RAW_TABLES = Path("sql/02_raw_tables.sql").read_text(encoding="utf-8")
SQL_TRANSFORMS = Path("sql/04_transform_views.sql").read_text(encoding="utf-8")
SQL_MART = Path("sql/05_feature_mart.sql").read_text(encoding="utf-8")

def main():
    cfg = get_config()
    csv_path = Path("data/telco_churn.csv")
    if not csv_path.exists():
        raise FileNotFoundError("Missing data/telco_churn.csv (add it or generate one).")

    with snowflake_conn(cfg) as conn:
        # 1) Create warehouse/db/schemas
        exec_sql(conn, SQL_SETUP)
        exec_sql(conn, SQL_STAGE)
        exec_sql(conn, SQL_RAW_TABLES)

        # 2) Upload file to internal stage
        cur = conn.cursor()
        try:
            cur.execute(f"USE DATABASE {cfg.database}")
            # stage created in PUBLIC by the SQL (CHURN_DB.PUBLIC.STG_CHURN_FILES)
            stage_fqn = f"@{cfg.database}.PUBLIC.{cfg.stage}"
            put_cmd = f"PUT file://{csv_path.resolve().as_posix()} {stage_fqn} OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
            cur.execute(put_cmd)
        finally:
            cur.close()

        # 3) COPY INTO raw table
        copy_sql = Path("sql/03_load_copy_into.sql").read_text(encoding="utf-8")
        # Ensure the stage name in SQL matches cfg.stage if you renamed it
        copy_sql = copy_sql.replace("STG_CHURN_FILES", cfg.stage)
        exec_sql(conn, copy_sql)

        # 4) Transform + feature mart
        exec_sql(conn, SQL_TRANSFORMS)
        exec_sql(conn, SQL_MART)

    print("✅ Loaded RAW → STG view → ANALYTICS feature mart in Snowflake.")

if __name__ == "__main__":
    main()
