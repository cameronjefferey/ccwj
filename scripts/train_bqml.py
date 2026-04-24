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

import importlib.util
import logging
import os
import sys
from pathlib import Path

# Load app/bigquery_client.py as a standalone module so we don't trigger
# app/__init__.py (which pulls in Flask config + requires SECRET_KEY).
# This script only needs BigQuery credentials, not a Flask app.
REPO_ROOT = Path(__file__).resolve().parent.parent
_BQ_CLIENT_PATH = REPO_ROOT / "app" / "bigquery_client.py"
_spec = importlib.util.spec_from_file_location("_bqml_bq_client", _BQ_CLIENT_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover - defensive
    raise ImportError(f"Could not load bigquery_client from {_BQ_CLIENT_PATH}")
_bq_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bq_mod)
get_bigquery_client = _bq_mod.get_bigquery_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("train_bqml")


SQL_DIR = REPO_ROOT / "scripts" / "bqml"

# Match the dbt project location (dbt/profiles.yml -> location: us). All
# BQML artifacts live alongside the analytics dataset in the US multi-region,
# so we must pin every job here — otherwise BigQuery assumes the runner's
# default region (e.g. us-west1) and fails with 404 Dataset not found.
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# Dataset that holds all BQML artifacts the script creates. Kept separate
# from `analytics` so a dbt full-refresh never wipes the trained model.
BQML_DATASET = os.environ.get("BQML_DATASET", "ml_models")

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


def _ensure_dataset(client, dataset_id: str, location: str) -> None:
    """Create the ml_models dataset in the right location if it doesn't exist.

    Idempotent: NotFound -> create; already-exists -> no-op. Keeps CI from
    needing a one-time out-of-band setup step."""
    from google.cloud import bigquery  # local import avoids top-level dep
    from google.api_core.exceptions import NotFound

    full_id = f"{client.project}.{dataset_id}"
    try:
        ds = client.get_dataset(full_id)
        if (ds.location or "").upper() != location.upper():
            raise RuntimeError(
                f"Dataset {full_id} exists in location {ds.location!r}, "
                f"expected {location!r}. Refusing to proceed — move or "
                f"recreate it manually to avoid cross-region joins."
            )
        log.info("Dataset %s already exists in %s", full_id, ds.location)
        return
    except NotFound:
        pass

    ref = bigquery.Dataset(full_id)
    ref.location = location
    ref.description = (
        "BQML artifacts trained nightly by scripts/train_bqml.py. "
        "Isolated from `analytics` so dbt full-refresh doesn't wipe "
        "the trained model."
    )
    client.create_dataset(ref, exists_ok=True)
    log.info("Created dataset %s in %s", full_id, location)


def main() -> int:
    client = get_bigquery_client()
    log.info("BigQuery client ready (project=%s, location=%s)",
             client.project, BQ_LOCATION)

    _ensure_dataset(client, BQML_DATASET, BQ_LOCATION)

    for label, filename in STATEMENTS:
        sql = _load_sql(filename)
        log.info("Running %s", label)
        job = client.query(sql, location=BQ_LOCATION)
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
