import os
import json
import base64
import logging
import time
from google.cloud import bigquery
from google.oauth2 import service_account


_log = logging.getLogger(__name__)


class _CostTrackingBigQueryClient(bigquery.Client):
    """Thin subclass that emits a COST_EVENT log line per query.

    We override .query() instead of wrapping callers because the app calls
    client.query(...) from ~30 places — wrapping at the source lets us
    capture every read with one edit. The behaviour is otherwise
    transparent: same return type (QueryJob), same kwargs.

    The log line is emitted after the job completes so we can include
    actual bytes processed/billed; an early failure still logs the
    duration and an error tag so the operator can spot stuck queries.
    """

    def query(self, query, job_config=None, **kwargs):
        t0 = time.monotonic()
        try:
            job = super().query(query, job_config=job_config, **kwargs)
        except Exception:
            duration_ms = int((time.monotonic() - t0) * 1000)
            _emit_bq_cost_event(
                kind=_query_kind(query),
                duration_ms=duration_ms,
                error=1,
            )
            raise

        # Submission timing is the single number we can log every time:
        # the post-execution counters (total_bytes_billed) are only
        # populated after the job finishes, but the app accesses results
        # via several different code paths (to_dataframe, .result(),
        # iteration). Rather than monkey-patch every entry point, we add
        # a one-shot listener on `_done_or_raise` via add_done_callback
        # so we get the actual cost numbers on whichever path completes
        # the job first. If the job is already done at submission time
        # (cached / instant), the callback fires synchronously below.
        kind = _query_kind(query)

        def _on_done(j):
            try:
                duration_ms = int((time.monotonic() - t0) * 1000)
                if j.exception() is not None:
                    _emit_bq_cost_event(
                        kind=kind,
                        duration_ms=duration_ms,
                        error=1,
                    )
                    return
                _emit_bq_cost_event(
                    kind=kind,
                    duration_ms=duration_ms,
                    bytes_processed=getattr(j, "total_bytes_processed", None),
                    bytes_billed=getattr(j, "total_bytes_billed", None),
                    cache_hit=int(bool(getattr(j, "cache_hit", False) or False)),
                )
            except Exception:  # pragma: no cover (defensive)
                pass

        try:
            job.add_done_callback(_on_done)
        except Exception:
            # Defensive: if add_done_callback is unavailable on this SDK
            # version, log submission-time only so we still see the call.
            _emit_bq_cost_event(
                kind=kind,
                duration_ms=int((time.monotonic() - t0) * 1000),
            )
        return job


def _query_kind(sql: str) -> str:
    """Heuristic short tag for the COST_EVENT log line.

    We don't care about the full SQL — just enough to group by mart so the
    operator can see which page is driving the bill. Falls back to
    'other' if nothing matches.
    """
    s = (sql or "").lower()
    for needle, tag in (
        ("positions_summary", "positions_summary"),
        ("int_strategy_classification", "int_strategy_classification"),
        ("mart_weekly_summary", "weekly_summary"),
        ("mart_daily_pnl", "daily_pnl"),
        ("mart_strategy_performance", "strategy_performance"),
        ("mart_coaching_signals", "coaching_signals"),
        ("mart_benchmark", "benchmark"),
        ("mart_option_trades_by_kind", "option_trades_by_kind"),
        ("stg_history", "stg_history"),
        ("stg_daily_prices", "stg_daily_prices"),
        ("snapshot_options_market_values_daily", "snapshot_options_mv"),
        ("snapshot_account_balances_daily", "snapshot_account_balances"),
    ):
        if needle in s:
            return tag
    return "other"


def _emit_bq_cost_event(*, kind, duration_ms, **fields):
    """Log a single COST_EVENT for a BigQuery query.

    We import lazily to avoid a circular import (cost_tracking depends on
    flask_login.current_user, which depends on the app being loaded; this
    module is imported during app startup before that).
    """
    try:
        from app.cost_tracking import log_cost_event
    except Exception:
        return
    log_cost_event(
        "bigquery",
        kind,
        duration_ms=duration_ms,
        **{k: v for k, v in fields.items() if v is not None},
    )


def get_bigquery_client():
    """Create a BigQuery client using the best available credentials.

    Credential resolution order:
      1. GOOGLE_APPLICATION_CREDENTIALS_JSON_BASE64  (Render / CI)
      2. GOOGLE_APPLICATION_CREDENTIALS file path    (explicit service-account)
      3. Application Default Credentials             (gcloud auth / GCE / Cloud Run)
    """

    # 1. Render / CI: base64-encoded service-account JSON
    b64_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON_BASE64")
    if b64_creds:
        creds_dict = json.loads(base64.b64decode(b64_creds).decode())
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        return _CostTrackingBigQueryClient(credentials=credentials, project=credentials.project_id)

    # 2. Explicit service-account key file
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and os.path.exists(sa_path):
        credentials = service_account.Credentials.from_service_account_file(sa_path)
        return _CostTrackingBigQueryClient(credentials=credentials, project=credentials.project_id)

    # 3. Application Default Credentials (gcloud auth application-default login)
    return _CostTrackingBigQueryClient(project="ccwj-dbt")
