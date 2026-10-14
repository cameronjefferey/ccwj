#!/usr/bin/env python3
"""Read-only health flags for the private Vanguard account.

Prints PASS/FAIL lines only. Does not print balances, tickers, ids, or names.
Does not write to Postgres, BigQuery, or SnapTrade.
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
import re
import sys
from datetime import date, datetime

PROJECT = "ccwj-dbt"
PROD_DATASET = "analytics"

_WRITE_RE = re.compile(
    r"\b(insert|update|delete|merge|create|drop|truncate|alter|grant|revoke|copy|call)\b",
    re.I,
)

_HIDDEN_VANGUARD_SQL = """
SELECT h.user_id, h.active
FROM admin_hidden_users h
JOIN snaptrade_accounts a ON a.user_id = h.user_id
WHERE strpos(lower(coalesce(a.broker_slug, '')), 'vanguard') > 0
   OR left(lower(coalesce(a.account_name, '')), 8) = 'vanguard'
"""

_ACCOUNTS_SQL = """
SELECT tenant_id, snaptrade_account_id, connection_broken_at,
       holdings_last_successful_sync, created_at, broker_slug, account_name
FROM snaptrade_accounts
WHERE user_id = %s
"""

# Vanguard partition is the account label's first token, the same rule as
# the per-broker staging models. Tenants are discovered here. No id is
# passed in, and the outer select returns only aggregates.
_WAREHOUSE_SQL = f"""
WITH labeled AS (
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_history`
  UNION ALL
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_current`
  UNION ALL
  SELECT tenant_id, account
    FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances`
),
tenants AS (
  SELECT DISTINCT tenant_id
    FROM labeled
   WHERE tenant_id IS NOT NULL
     AND tenant_id != 'demo:demo-account'
     AND lower(split(trim(cast(account AS STRING)), ' ')[SAFE_OFFSET(0)]) = 'vanguard'
),
per_tenant AS (
  SELECT
    (SELECT COUNT(*)
       FROM `{PROJECT}.{PROD_DATASET}.stg_history` h
      WHERE h.tenant_id = t.tenant_id) AS history_n,
    (SELECT COUNT(*)
       FROM `{PROJECT}.{PROD_DATASET}.stg_current` c
      WHERE c.tenant_id = t.tenant_id) AS current_n,
    (SELECT COUNT(*)
       FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances` b
      WHERE b.tenant_id = t.tenant_id
        AND b.row_type = 'account_total'
        AND b.market_value IS NOT NULL) AS total_n,
    (SELECT SUM(c.market_value)
       FROM `{PROJECT}.{PROD_DATASET}.stg_current` c
      WHERE c.tenant_id = t.tenant_id) AS position_value,
    (SELECT SUM(b.market_value)
       FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances` b
      WHERE b.tenant_id = t.tenant_id
        AND b.row_type = 'cash') AS cash_value,
    (SELECT SUM(b.market_value)
       FROM `{PROJECT}.{PROD_DATASET}.stg_account_balances` b
      WHERE b.tenant_id = t.tenant_id
        AND b.row_type = 'account_total') AS account_total
  FROM tenants t
)
SELECT
  COUNT(*) AS tenant_n,
  COUNTIF(
    history_n > 0
    AND current_n > 0
    AND total_n > 0
    AND position_value IS NOT NULL
    AND account_total IS NOT NULL
  ) AS complete_n,
  SUM(position_value + IFNULL(cash_value, 0)) AS staged_book,
  SUM(account_total) AS balance_book
FROM per_tenant
"""

_WAREHOUSE_LINE = re.compile(r"^(PASS|FAIL|FAIL off by about [1-9][0-9]*%)$")


def _code_only(sql: str) -> str:
    """Drop comments and quoted strings so a comment cannot hide a write."""
    out = []
    i = 0
    text = sql or ""
    n = len(text)
    while i < n:
        if text.startswith("--", i):
            nl = text.find("\n", i)
            if nl < 0:
                break
            i = nl + 1
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            if end < 0:
                break
            i = end + 2
            continue
        if text[i] == "'":
            i += 1
            while i < n:
                if text[i] == "'":
                    if i + 1 < n and text[i + 1] == "'":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        out.append(text[i])
        i += 1
    return "".join(out)


def statement_is_read(sql: str) -> bool:
    """True when the statement is a single read.

    Words inside comments or quotes do not count. ``updated_at`` does not
    count as a write. A ``WITH`` that then inserts does.
    """
    body = _code_only(sql).strip()
    if not body:
        return False
    body = body.rstrip().rstrip(";").strip()
    if ";" in body or _WRITE_RE.search(body):
        return False
    head = body.lstrip("(").lstrip().lower()
    return head.startswith(("select", "with", "show", "explain"))


def as_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def is_vanguard(row) -> bool:
    slug = (row.get("broker_slug") or "").lower()
    name = (row.get("account_name") or "").lower()
    return "vanguard" in slug or name.startswith("vanguard")


def holdings_is_fresh(*, last, created, today, threshold, unavailable=False) -> bool:
    """Match the product banner: unavailable or pre-connection stamps do not flag.

    A missing stamp is not fresh. ``threshold <= 0`` disables the backstop.
    """
    if threshold <= 0 or unavailable:
        return True
    if last is None:
        return False
    if created is not None and last < created:
        return True
    days = (today - last).days
    # A future stamp is not a stall. The product treats it the same way.
    if days < 0:
        return True
    return days < threshold


def connections_ok(rows, *, today, threshold, live) -> bool:
    if not rows:
        return False
    for row in rows:
        if row.get("connection_broken_at") is not None:
            return False
        info = (live or {}).get(row.get("snaptrade_account_id"))
        created = as_date(row.get("created_at"))
        stored_last = as_date(row.get("holdings_last_successful_sync"))
        if info and (info.get("last") is not None or info.get("unavailable")):
            fresh = holdings_is_fresh(
                last=info.get("last"),
                created=created,
                today=today,
                threshold=threshold,
                unavailable=bool(info.get("unavailable")),
            )
        else:
            fresh = holdings_is_fresh(
                last=stored_last,
                created=created,
                today=today,
                threshold=threshold,
                unavailable=False,
            )
        if not fresh:
            return False
    return True


def reconcile_line(staged, live) -> str:
    """PASS within one dollar, otherwise a coarse percent. Never a dollar amount."""
    if staged is None or live is None:
        return "FAIL"
    try:
        gap = abs(float(staged) - float(live))
        live_abs = abs(float(live))
        staged_abs = abs(float(staged))
    except (TypeError, ValueError):
        return "FAIL"
    if gap <= 1:
        return "PASS"
    base = live_abs if live_abs > 1 else staged_abs
    if base <= 0:
        return "FAIL"
    shown = int(round((gap / base) * 100))
    if shown < 1:
        shown = 1
    return f"off by about {shown}%"


def warehouse_status(*, complete: bool, staged, live) -> str:
    if not complete:
        return "FAIL"
    return reconcile_line(staged, live)


def warehouse_report_line(*, complete: bool, staged, balance) -> str:
    """PASS, or FAIL, or FAIL with a coarse percent. No dollar amount."""
    if not complete:
        return "FAIL"
    status = reconcile_line(staged, balance)
    if status == "PASS":
        return "PASS"
    if isinstance(status, str) and status.startswith("off by about "):
        return f"FAIL {status}"
    return "FAIL"


def format_report(report) -> list[str]:
    def flag(ok) -> str:
        return "PASS" if ok else "FAIL"

    warehouse = report.get("warehouse")
    if warehouse == "PASS" or (
        isinstance(warehouse, str) and warehouse.startswith("off by about ")
    ):
        warehouse_line = f"warehouse={warehouse}"
    else:
        warehouse_line = "warehouse=FAIL"
    return [
        f"connections={flag(report.get('connections'))}",
        f"privacy={flag(report.get('privacy'))}",
        warehouse_line,
        f"positions={flag(report.get('positions'))}",
        f"overview={flag(report.get('overview'))}",
        f"settings={flag(report.get('settings'))}",
    ]


def _blank_report():
    return {
        "connections": False,
        "privacy": False,
        "warehouse": "FAIL",
        "positions": False,
        "overview": False,
        "settings": False,
    }


def _install_read_only(dbmod) -> None:
    original = dbmod._connect

    def _connect():
        conn = original()
        previous = conn.autocommit
        conn.autocommit = True
        conn.execute("SET default_transaction_read_only = on")
        shown = conn.execute("SHOW default_transaction_read_only").fetchone()
        flag = shown["default_transaction_read_only"] if isinstance(shown, dict) else shown[0]
        conn.autocommit = previous
        if str(flag).lower() != "on":
            raise RuntimeError("read-only")
        real_cursor = conn.cursor
        real_execute_conn = getattr(conn, "execute", None)

        def cursor(*args, **kwargs):
            cur = real_cursor(*args, **kwargs)
            real_execute = cur.execute

            def execute(query, *exec_args, **exec_kwargs):
                sql = query.decode("utf-8", "replace") if isinstance(query, bytes) else str(query)
                if not statement_is_read(sql):
                    raise RuntimeError("read-only")
                return real_execute(query, *exec_args, **exec_kwargs)

            cur.execute = execute
            return cur

        def commit():
            conn.rollback()

        conn.cursor = cursor
        if real_execute_conn is not None:
            def execute_conn(query, *exec_args, **exec_kwargs):
                sql = query.decode("utf-8", "replace") if isinstance(query, bytes) else str(query)
                if not statement_is_read(sql):
                    raise RuntimeError("read-only")
                return real_execute_conn(query, *exec_args, **exec_kwargs)

            try:
                conn.execute = execute_conn
            except Exception:
                pass
        try:
            conn.commit = commit
        except Exception:
            # The session is already read-only. A failed shadow still
            # cannot persist a write when the server enforces that.
            pass
        return conn

    dbmod._connect = _connect


def _install_bq_guard() -> None:
    from google.cloud import bigquery

    original = bigquery.Client.query

    def query(self, query_text, *args, **kwargs):
        sql = query_text if isinstance(query_text, str) else str(query_text)
        if not statement_is_read(sql):
            raise RuntimeError("read-only")
        job_config = kwargs.get("job_config")
        if job_config is None and args:
            job_config = args[0]
        if job_config is not None and getattr(job_config, "destination", None) is not None:
            raise RuntimeError("read-only")
        return original(self, query_text, *args, **kwargs)

    def _refuse(self, *args, **kwargs):
        raise RuntimeError("read-only")

    bigquery.Client.query = query
    for name in (
        "load_table_from_dataframe",
        "load_table_from_json",
        "load_table_from_file",
        "insert_rows",
        "insert_rows_json",
        "create_table",
        "delete_table",
        "update_table",
        "copy_table",
    ):
        if hasattr(bigquery.Client, name):
            setattr(bigquery.Client, name, _refuse)


def _prepare_runtime():
    os.environ["HAPPYTRADER_SKIP_DB_INIT"] = "1"
    os.environ.setdefault("SECRET_KEY", "read-only-health-check")
    os.environ["WTF_CSRF_ENABLED"] = "false"
    os.environ["RATELIMIT_ENABLED"] = "false"
    os.environ.pop("BQ_DATASET", None)
    os.environ.pop("QUERY_CACHE_REDIS_URL", None)
    os.environ.pop("SENTRY_DSN", None)
    logging.disable(logging.CRITICAL)
    _install_bq_guard()
    import app.db as dbmod
    from app import app as flask_app

    _install_read_only(dbmod)
    flask_app.testing = True
    flask_app.config["PROPAGATE_EXCEPTIONS"] = True
    flask_app.config["RATELIMIT_ENABLED"] = False
    flask_app.config["SESSION_IDLE_TIMEOUT_MINUTES"] = 0
    flask_app.logger.disabled = True
    logging.getLogger("happytrader.timing").disabled = True
    logging.getLogger("happytrader.cost").disabled = True
    import app.admin_overview as overview
    import app.cost_tracking as cost
    import app.funnel as funnel

    overview.record_page_view = lambda response: None
    funnel.note_internal_visit = lambda reason: None
    funnel.log_event = lambda *args, **kwargs: None
    cost.log_cost_event = lambda *args, **kwargs: None
    _block_production_writes()
    for handler in logging.getLogger("happytrader.timing").handlers:
        handler.setStream(io.StringIO())
    return flask_app, dbmod


def _block_production_writes() -> None:
    """Page renders must not start a sync, seed write, or warehouse rebuild."""
    import app.snaptrade as snap
    import app.upload as upload

    def _no(*args, **kwargs):
        return None

    snap.queue_account_read_sync = _no
    snap._sync_one_connection = _no
    snap._kick_post_connect_sync = _no
    upload.merge_and_push_seeds = lambda *args, **kwargs: (False, "read-only", None, None, None, True)
    upload.merge_and_push_seeds_batch = lambda *args, **kwargs: (False, "read-only", None, [])
    if hasattr(upload, "_dispatch_warehouse_rebuild"):
        upload._dispatch_warehouse_rebuild = _no
    import sys
    paper = sys.modules.get("app.paper_practice")
    if paper is not None:
        paper.queue_account_read_sync = _no


_SECRET_MANAGER_MISSING = False
_LAST_SECRET_STATUS = "skip"


def _status_from_error(exc: BaseException) -> str:
    """Coarse Secret Manager result. The exception text is never used."""
    if type(exc).__name__ == "NotFound":
        return "miss"
    return "denied"


def _secret_text(name: str) -> str:
    """Read one allow-listed secret. Empty when Secret Manager is unavailable."""
    global _SECRET_MANAGER_MISSING, _LAST_SECRET_STATUS
    if _LAST_SECRET_STATUS == "denied" or _SECRET_MANAGER_MISSING:
        if _SECRET_MANAGER_MISSING:
            _LAST_SECRET_STATUS = "unavailable"
        return ""
    try:
        from google.cloud import secretmanager
    except Exception:
        _SECRET_MANAGER_MISSING = True
        _LAST_SECRET_STATUS = "unavailable"
        return ""
    project = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCLOUD_PROJECT")
        or PROJECT
    ).strip()
    if not project or not name:
        _LAST_SECRET_STATUS = "unavailable"
        return ""
    try:
        client = secretmanager.SecretManagerServiceClient()
        path = f"projects/{project}/secrets/{name}/versions/latest"
        resp = client.access_secret_version(name=path)
        text = (resp.payload.data or b"").decode("utf-8").strip()
        _LAST_SECRET_STATUS = "hit" if text else "miss"
        return text
    except Exception as exc:
        _LAST_SECRET_STATUS = _status_from_error(exc)
        return ""


def _hydrate_env() -> None:
    """Fill connection env from Action secrets or an allow-listed secret name.

    Never prints the value. Missing names stay empty.
    """
    aliases = {
        "DATABASE_URL": (
            "DATABASE_URL",
            "PROD_DATABASE_URL",
            "database-url",
            "prod-database-url",
            "happytrader-database-url",
            "ccwj-database-url",
        ),
        "SNAPTRADE_CLIENT_ID": (
            "SNAPTRADE_CLIENT_ID",
            "snaptrade-client-id",
            "SNAPTRADE_CLIENT_SECRET",
        ),
        "SNAPTRADE_CONSUMER_KEY": (
            "SNAPTRADE_CONSUMER_KEY",
            "snaptrade-consumer-key",
            "snaptrade-client-secret",
        ),
    }
    for target, names in aliases.items():
        current = ""
        for name in names:
            current = (os.environ.get(name) or "").strip()
            if current:
                break
        if not current:
            for name in names:
                current = _secret_text(name)
                if current:
                    break
        if current and not (os.environ.get(target) or "").strip():
            os.environ[target] = current


def _database_url() -> str:
    _hydrate_env()
    value = (os.environ.get("DATABASE_URL") or "").strip()
    return value


def _live_reads(accounts):
    """Read SnapTrade details and balances. Returns (live_map, vanguard_total or None)."""
    from app.models import get_snaptrade_user
    from app.snaptrade import (
        SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS,
        _fetch_account_summary,
        _fetch_balances,
        _get_snaptrade_client,
        _holdings_last_successful_sync,
        _holdings_unavailable,
        snaptrade_sync_timeout,
    )
    from app.snaptrade_normalize import _reported_cash_and_total

    del SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS
    user_ids = {row.get("user_id") for row in accounts}
    if len(user_ids) != 1:
        return {}, None
    user_id = next(iter(user_ids))
    creds = get_snaptrade_user(user_id)
    client = _get_snaptrade_client()
    if not creds or client is None:
        return {}, None
    snap_user = creds.get("snaptrade_user_id")
    snap_secret = creds.get("snaptrade_secret")
    if not snap_user or not snap_secret:
        return {}, None
    live = {}
    totals = []
    balance_missing = False
    saw_vanguard = False
    for row in accounts:
        account_id = row.get("snaptrade_account_id")
        vanguard = is_vanguard(row)
        if vanguard:
            saw_vanguard = True
        try:
            with snaptrade_sync_timeout():
                summary = _fetch_account_summary(client, snap_user, snap_secret, account_id)
                balances = _fetch_balances(client, snap_user, snap_secret, account_id)
        except Exception:
            if vanguard:
                balance_missing = True
            continue
        live[account_id] = {
            "last": _holdings_last_successful_sync(summary),
            "unavailable": _holdings_unavailable(summary),
        }
        if not vanguard:
            continue
        _cash, total = _reported_cash_and_total(balances, summary)
        if total is None:
            balance_missing = True
            continue
        totals.append(float(total))
    if balance_missing or not saw_vanguard:
        return live, None
    return live, sum(totals)


def _staged_total(tenants):
    from google.cloud import bigquery

    if not tenants or not statement_is_read(_WAREHOUSE_SQL):
        return None
    client = bigquery.Client(project=PROJECT)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tenants", "STRING", list(tenants))]
    )
    rows = list(client.query(_WAREHOUSE_SQL, job_config=job_config, location="US").result())
    if len(rows) != 1:
        return None
    return rows[0]


def _render_pages(flask_app, user_id) -> dict:
    results = {"positions": False, "overview": False, "settings": False}
    expected = {
        "positions": "/positions",
        "overview": "/overview",
        "settings": "/profile",
    }
    client = flask_app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = str(user_id)
        sess["_fresh"] = True
    for name, path in (
        ("positions", "/positions"),
        ("overview", "/overview"),
        ("settings", "/settings"),
    ):
        try:
            resp = client.get(path, headers={"X-HT-Full": "1"}, follow_redirects=True)
            final = getattr(resp.request, "path", "")
            results[name] = resp.status_code == 200 and final == expected[name]
        except Exception:
            results[name] = False
    return results


def run() -> dict:
    report = _blank_report()
    try:
        _fill_report(report)
    except Exception:
        return report
    return report


def _fill_report(report) -> None:
    if os.environ.get("HAPPYTRADER_SKIP_DB_INIT") != "1":
        os.environ["HAPPYTRADER_SKIP_DB_INIT"] = "1"
    if not _database_url():
        return
    if not statement_is_read(_HIDDEN_VANGUARD_SQL) or not statement_is_read(_ACCOUNTS_SQL):
        return
    flask_app, dbmod = _prepare_runtime()
    from app.snaptrade import SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS

    hidden = dbmod.fetch_all(_HIDDEN_VANGUARD_SQL)
    user_ids = {row.get("user_id") for row in hidden}
    if len(user_ids) != 1:
        return
    user_id = next(iter(user_ids))
    report["privacy"] = all(bool(row.get("active")) for row in hidden)
    accounts = dbmod.fetch_all(_ACCOUNTS_SQL, (user_id,))
    for row in accounts:
        row["user_id"] = user_id
    vanguard_rows = [row for row in accounts if is_vanguard(row)]
    tenants = []
    seen = set()
    for row in vanguard_rows:
        tenant = (row.get("tenant_id") or "").strip()
        if not tenant or tenant in seen:
            if not tenant:
                tenants = []
                break
            continue
        seen.add(tenant)
        tenants.append(tenant)
    live, snap_total = _live_reads(accounts)
    today = date.today()
    report["connections"] = connections_ok(
        accounts,
        today=today,
        threshold=SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS,
        live=live,
    )
    staged_row = None
    if tenants:
        try:
            staged_row = _staged_total(tenants)
        except Exception:
            staged_row = None
    if staged_row is None or not tenants:
        report["warehouse"] = "FAIL"
    else:
        expected = len(tenants)
        complete = (
            int(staged_row["history_n"] or 0) == expected
            and int(staged_row["current_n"] or 0) == expected
            and int(staged_row["balance_n"] or 0) == expected
        )
        report["warehouse"] = warehouse_status(
            complete=complete,
            staged=staged_row["staged_total"],
            live=snap_total,
        )
    rendered = _render_pages(flask_app, user_id)
    report.update(rendered)


def run_warehouse() -> str:
    """Read prod analytics only. Returns one flag line."""
    os.environ.pop("BQ_DATASET", None)
    if not statement_is_read(_WAREHOUSE_SQL):
        return "FAIL"
    _install_bq_guard()
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    rows = list(client.query(_WAREHOUSE_SQL, location="US").result())
    if len(rows) != 1:
        return "FAIL"
    row = rows[0]
    tenant_n = int(row["tenant_n"] or 0)
    complete_n = int(row["complete_n"] or 0)
    complete = tenant_n > 0 and complete_n == tenant_n
    line = warehouse_report_line(
        complete=complete,
        staged=row["staged_book"],
        balance=row["balance_book"],
    )
    if not _WAREHOUSE_LINE.match(line):
        return "FAIL"
    return line


def main(argv: list[str] | None = None) -> int:
    del argv
    sys.tracebacklimit = 0
    logging.disable(logging.CRITICAL)
    line = "FAIL"
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        try:
            line = run_warehouse()
        except Exception:
            line = "FAIL"
    if not _WAREHOUSE_LINE.match(line):
        line = "FAIL"
    print(line, flush=True)
    return 0 if line == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
