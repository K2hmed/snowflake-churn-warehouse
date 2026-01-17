from __future__ import annotations
from contextlib import contextmanager
import snowflake.connector
from src.config import SnowflakeConfig

@contextmanager
def snowflake_conn(cfg: SnowflakeConfig):
    conn = snowflake.connector.connect(
        account=cfg.account,
        user=cfg.user,
        password=cfg.password,
        role=cfg.role,
        warehouse=cfg.warehouse,
        database=cfg.database,
    )
    try:
        yield conn
    finally:
        conn.close()

def exec_sql(conn, sql: str):
    cur = conn.cursor()
    try:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            cur.execute(stmt)
    finally:
        cur.close()
