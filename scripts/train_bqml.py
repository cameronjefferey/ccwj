"""Retrain BQML behavioral-observation artifacts.

Runs three statements in order:

    1. CREATE OR REPLACE MODEL ccwj-dbt.ml_models.account_behavior_model
    2. CREATE OR REPLACE VIEW  ccwj-dbt.ml_models.trade_anomaly_scores
    3. CREATE OR REPLACE VIEW  ccwj-dbt.ml_models.account_trade_insights

Invoked from .github/workflows/bigquery_update.yml after both dbt build passes.
Uses the same BigQuery credential resolution as the Flask app (see
app/bigquery_client.py), so no new secrets are required.

Safe to run repeatedly — every statement is CREATE OR REPLACE and does not
delete or alter any analytics dataset tables.

The ml_models dataset must already exist in project ccwj-dbt.  It is
assumed to have been created once, out of band (see the project prompt).
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Make `app.bigquery_client` importable when this script is run directly.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from app.bigquery_client import get_bigquery_client  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("train_bqml")


SQL_DIR = REPO_ROOT / "scripts" / "bqml"

STATEMENTS = [
    ("account_behavior_model (CREATE OR REPLACE MODEL)", "01_account_behavior_model.sql"),
    ("trade_anomaly_scores (CREATE OR REPLACE VIEW)",   "02_trade_anomaly_scores.sql"),
    ("account_trade_insights (CREATE OR REPLACE VIEW)", "03_account_trade_insights.sql"),
]


def _load_sql(filename: str) -> str:
    path = SQL_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"BQML SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def main() -> int:
    client = get_bigquery_client()
    log.info("BigQuery client ready (project=%s)", client.project)

    for label, filename in STATEMENTS:
        sql = _load_sql(filename)
        log.info("Running %s", label)
        job = client.query(sql)
        job.result()  # block until complete
        log.info("  done (job_id=%s)", job.job_id)

    log.info("All BQML artifacts refreshed successfully.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        log.exception("BQML retrain failed")
        raise
