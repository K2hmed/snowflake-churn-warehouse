from __future__ import annotations

import uuid
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, List, Tuple

import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from src.config import get_config
from src.snowflake_client import snowflake_conn


FEATURE_TABLE_FQN = "ANALYTICS.CUSTOMER_CHURN_FEATURES"


def ensure_scoring_tables(conn, cfg) -> None:
    """
    Creates scoring / metadata tables if they don't exist.
    Must run BEFORE any inserts.
    """
    ddl_path = Path("sql/08_scoring_tables.sql")
    if not ddl_path.exists():
        raise FileNotFoundError("Missing sql/08_scoring_tables.sql")

    ddl = ddl_path.read_text(encoding="utf-8")
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {cfg.database}")
        cur.execute(f"USE SCHEMA {cfg.analytics_schema}")

        # Run each statement (idempotent due to CREATE OR REPLACE)
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            cur.execute(stmt)
    finally:
        cur.close()


def fetch_features(conn, database: str) -> pd.DataFrame:
    """
    Pull feature mart from Snowflake into pandas.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {database}.{FEATURE_TABLE_FQN}")
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


def choose_threshold_max_f1(y_true: np.ndarray, proba: np.ndarray) -> float:
    """
    Choose threshold that maximizes F1 on PR curve.
    Good practical approach for churn (imbalanced classification).
    """
    precision, recall, thresholds = precision_recall_curve(y_true, proba)
    # thresholds length is (len(precision)-1)
    f1 = (2 * precision[:-1] * recall[:-1]) / (precision[:-1] + recall[:-1] + 1e-12)
    best_idx = int(np.nanargmax(f1))
    return float(thresholds[best_idx])


def write_model_runs(
    conn,
    cfg,
    run_id: str,
    model_name: str,
    train_rows: int,
    test_rows: int,
    roc_auc: float,
    pr_auc: float,
    threshold: float,
    notes: str,
) -> None:
    """
    Insert a single model run record.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {cfg.database}")
        cur.execute(f"USE SCHEMA {cfg.analytics_schema}")
        cur.execute(
            f"""
            INSERT INTO {cfg.database}.{cfg.analytics_schema}.MODEL_RUNS
              (run_id, ran_at, model_name, train_rows, test_rows, roc_auc, pr_auc, chosen_threshold, notes)
            VALUES
              (%s, CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, model_name, train_rows, test_rows, roc_auc, pr_auc, threshold, notes),
        )
    finally:
        cur.close()


def write_scores(conn, cfg, rows: List[Tuple[Any, ...]]) -> None:
    """
    Bulk insert scores. `rows` must contain ONLY plain Python types.
    scored_at is passed as string, cast in SQL via TO_TIMESTAMP_NTZ().
    """
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {cfg.database}")
        cur.execute(f"USE SCHEMA {cfg.analytics_schema}")

        cur.executemany(
            f"""
            INSERT INTO {cfg.database}.{cfg.analytics_schema}.CUSTOMER_CHURN_SCORES
                (run_id, customer_id, scored_at, model_name, churn_proba, churn_pred)
            VALUES
                (%s, %s, TO_TIMESTAMP_NTZ(%s), %s, %s, %s)
            """,
            rows,
        )
    finally:
        cur.close()


def main() -> None:
    cfg = get_config()
    model_name = "logreg_ohe_v1"
    run_id = str(uuid.uuid4())

    with snowflake_conn(cfg) as conn:
        # 1) Ensure target tables exist BEFORE inserts
        ensure_scoring_tables(conn, cfg)

        # 2) Fetch mart
        df = fetch_features(conn, cfg.database)

        # Normalize column names (just in case Snowflake returns uppercase)
        df.columns = [c.lower() for c in df.columns]
        if "customer_id" not in df.columns or "churn_label" not in df.columns:
            raise RuntimeError(
                f"Expected columns not found. Got columns: {df.columns.tolist()}"
            )

        y = df["churn_label"].astype(int).values
        X = df.drop(columns=["churn_label"])

        # Feature typing
        numeric_cols = [
            c for c in X.columns
            if c in {
                "senior_citizen",
                "tenure_months",
                "monthly_charges",
                "total_charges",
                "is_new_customer",
                "is_month_to_month",
                "is_high_monthly_charges",
            }
        ]
        categorical_cols = [c for c in X.columns if c not in numeric_cols and c != "customer_id"]

        # 3) Preprocess + model pipeline
        pre = ColumnTransformer(
            transformers=[
                ("num", Pipeline(steps=[
                    ("imputer", SimpleImputer(strategy="median")),
                ]), numeric_cols),
                ("cat", Pipeline(steps=[
                    ("imputer", SimpleImputer(strategy="most_frequent")),
                    ("ohe", OneHotEncoder(handle_unknown="ignore")),
                ]), categorical_cols),
            ],
            remainder="drop",
        )

        clf = LogisticRegression(max_iter=2000, class_weight="balanced")
        pipe = Pipeline(steps=[("pre", pre), ("clf", clf)])

        # 4) Train/test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        pipe.fit(X_train, y_train)
        proba_test = pipe.predict_proba(X_test)[:, 1]

        roc_auc = float(roc_auc_score(y_test, proba_test))
        pr_auc = float(average_precision_score(y_test, proba_test))
        threshold = choose_threshold_max_f1(y_test, proba_test)

        notes = "Snowflake feature mart → sklearn pipeline; threshold chosen by max-F1 on PR curve."
        write_model_runs(
            conn=conn,
            cfg=cfg,
            run_id=run_id,
            model_name=model_name,
            train_rows=len(X_train),
            test_rows=len(X_test),
            roc_auc=roc_auc,
            pr_auc=pr_auc,
            threshold=threshold,
            notes=notes,
        )

        # 5) Score all customers and write back
        proba_all = pipe.predict_proba(X)[:, 1]
        pred_all = (proba_all >= threshold).astype(int)

        # Use string timestamp to avoid Snowflake connector timestamp binding issues
        scored_at_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        # Build rows with ONLY plain Python types
        customer_ids = X["customer_id"].astype(str).tolist()
        rows = [
            (run_id, str(cid), scored_at_str, model_name, float(p), int(pred))
            for cid, p, pred in zip(customer_ids, proba_all, pred_all)
        ]

        write_scores(conn, cfg, rows)

    print("✅ Model trained, evaluated, and scores written back to Snowflake.")
    print(f"Run ID: {run_id}")
    print(f"ROC AUC: {roc_auc:.4f} | PR AUC: {pr_auc:.4f} | Threshold: {threshold:.4f}")


if __name__ == "__main__":
    main()