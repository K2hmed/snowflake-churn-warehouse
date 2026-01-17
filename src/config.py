from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    raw_schema: str
    stg_schema: str
    analytics_schema: str
    stage: str

def get_config() -> SnowflakeConfig:
    missing = [k for k in [
        "SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE","SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE",
        "SNOWFLAKE_RAW_SCHEMA","SNOWFLAKE_STG_SCHEMA","SNOWFLAKE_ANALYTICS_SCHEMA",
        "SNOWFLAKE_STAGE"
    ] if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}. Copy .env.example -> .env and fill values.")

    return SnowflakeConfig(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        raw_schema=os.environ["SNOWFLAKE_RAW_SCHEMA"],
        stg_schema=os.environ["SNOWFLAKE_STG_SCHEMA"],
        analytics_schema=os.environ["SNOWFLAKE_ANALYTICS_SCHEMA"],
        stage=os.environ["SNOWFLAKE_STAGE"],
    )
