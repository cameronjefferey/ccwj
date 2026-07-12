"""
CLI for lifecycle / product-marketing email digests. Run via cron:

  python -m app.email_digests_cli weekly_summary
  python -m app.email_digests_cli weekly_preview
  python -m app.email_digests_cli reengagement
  python -m app.email_digests_cli connection_reminder

Each kind:
  - weekly_summary : recap of the user's most recent trading week
                     (source: mart_weekly_summary). Gated by
                     user_profiles.digest_email.
  - weekly_preview : look-ahead — upcoming earnings, option expirations,
                     and projected ex-dividends for currently-held symbols.
                     Gated by user_profiles.weekly_preview_email.
  - reengagement   : a nudge for users who haven't opened the app in a
                     while. Gated by user_profiles.product_update_email.
  - connection_reminder : recurring "still disconnected — reconnect" nudge
                     for users with a broken broker connection (Postgres
                     only, no BigQuery). Transactional account-health mail —
                     no opt-out. Daily cron, weekly per episode via dedupe.

Tenancy: every BigQuery read is scoped to ONE recipient at a time by
``CAST(user_id AS STRING) = @user_id`` AND
``tenant_id IN UNNEST(@tenant_ids)`` (the user's own broker-stable
tenant_ids). tenant_id is the v2 isolation boundary — it never collides
across physical accounts the way the display ``account`` label can (e.g.
several "Schwab Account"s). Because each digest email goes to a single
user, the row set is provably a subset of that user's data — no other
tenant's rows can appear. Per .cursor/rules/bigquery-tenant-isolation.mdc.

Idempotency: every send is guarded by ``record_email_send(kind, dedupe_key)``
so a daily cron never double-sends. The dedupe key encodes the week (digests)
or the dormancy episode (re-engagement).

Exit codes:
  0  — ran to completion (some sends may have been skipped/failed individually).
  2  — bad usage (unknown kind).
"""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("FLASK_APP", "app:app")

_PROJECT = "ccwj-dbt.analytics"

# Re-engagement dormancy window (days). A user is nudged when their last
# visit falls in this window; the email_sends dedupe (keyed on the visit
# anchor) keeps a daily cron from re-nudging the same dormancy episode.
REENGAGE_MIN_DAYS = 14
REENGAGE_MAX_DAYS = 45


def _money(val):
    try:
        n = float(val or 0)
    except (TypeError, ValueError):
        n = 0.0
    return f"{'-' if n < 0 else ''}${abs(n):,.2f}"


def _scope_params(bigquery, user_id, tenant_ids):
    return [
        bigquery.ScalarQueryParameter("user_id", "STRING", str(user_id)),
        bigquery.ArrayQueryParameter("tenant_ids", "STRING", list(tenant_ids)),
    ]


# ---------------------------------------------------------------------------
# weekly_summary
# ---------------------------------------------------------------------------

_WEEKLY_SUMMARY_SQL = f"""
SELECT account, week_start, total_return, total_pnl, dividends_amount,
       trades_closed, num_winners, num_losers,
       best_symbol, best_pnl, worst_symbol, worst_pnl
FROM `{_PROJECT}.mart_weekly_summary`
WHERE CAST(user_id AS STRING) = @user_id
  AND tenant_id IN UNNEST(@tenant_ids)
  AND week_start = (
      SELECT MAX(week_start) FROM `{_PROJECT}.mart_weekly_summary`
      WHERE CAST(user_id AS STRING) = @user_id
        AND tenant_id IN UNNEST(@tenant_ids)
  )
"""


def _build_weekly_summary(client, bigquery, user_id, tenant_ids):
    """Aggregate the user's most-recent-week rows (one per tenant) into a
    single summary dict, or None if there's no week to report."""
    cfg = bigquery.QueryJobConfig(query_parameters=_scope_params(bigquery, user_id, tenant_ids))
    rows = list(client.query(_WEEKLY_SUMMARY_SQL, job_config=cfg).result())
    if not rows:
        return None

    week_start = rows[0]["week_start"]
    total_return = sum(float(r["total_return"] or 0) for r in rows)
    total_pnl = sum(float(r["total_pnl"] or 0) for r in rows)
    dividends = sum(float(r["dividends_amount"] or 0) for r in rows)
    trades_closed = sum(int(r["trades_closed"] or 0) for r in rows)
    num_winners = sum(int(r["num_winners"] or 0) for r in rows)
    num_losers = sum(int(r["num_losers"] or 0) for r in rows)

    best = max(
        (r for r in rows if r["best_symbol"]),
        key=lambda r: float(r["best_pnl"] or 0),
        default=None,
    )
    worst = min(
        (r for r in rows if r["worst_symbol"]),
        key=lambda r: float(r["worst_pnl"] or 0),
        default=None,
    )

    week_label = ""
    if week_start is not None:
        try:
            week_label = f"{week_start:%b %d}"
        except Exception:
            week_label = str(week_start)

    return {
        "week_start": week_start.isoformat() if hasattr(week_start, "isoformat") else str(week_start),
        "week_label": f"week of {week_label}" if week_label else "this past week",
        "total_return": total_return,
        "total_pnl": total_pnl,
        "dividends": dividends,
        "trades_closed": trades_closed,
        "num_winners": num_winners,
        "num_losers": num_losers,
        "best_symbol": best["best_symbol"] if best else None,
        "best_pnl": float(best["best_pnl"]) if best else None,
        "worst_symbol": worst["worst_symbol"] if worst else None,
        "worst_pnl": float(worst["worst_pnl"]) if worst else None,
    }


def run_weekly_summary(client, bigquery):
    from app.email import send_weekly_summary_email, app_base_url
    from app.models import (
        get_tenant_ids_for_user,
        list_email_recipients_for_kind,
        record_email_send,
    )

    sent = skipped = empty = 0
    for rec in list_email_recipients_for_kind("weekly_summary"):
        user_id = rec["user_id"]
        tenant_ids = get_tenant_ids_for_user(user_id)
        if not tenant_ids:
            empty += 1
            continue
        try:
            summary = _build_weekly_summary(client, bigquery, user_id, tenant_ids)
        except Exception as exc:
            print(f"User {user_id}: weekly_summary query failed: {exc}", file=sys.stderr)
            continue
        if not summary or not summary.get("trades_closed") and not summary.get("dividends"):
            empty += 1
            continue

        dedupe_key = f"{user_id}:{summary['week_start']}"
        if not record_email_send("weekly_summary", dedupe_key, user_id=user_id, to_email=rec["email"]):
            skipped += 1
            continue

        unsub = f"{app_base_url()}/email/unsubscribe/{rec.get('email_unsubscribe_token') or ''}"
        send_weekly_summary_email(
            to=rec["email"],
            username=rec["username"],
            summary=summary,
            dashboard_url=f"{app_base_url()}/daily-review",
            unsubscribe_url=unsub,
        )
        sent += 1
        print(f"User {user_id}: weekly_summary sent to {rec['email']}")
    print(f"weekly_summary: {sent} sent, {skipped} already-sent, {empty} no-data")


# ---------------------------------------------------------------------------
# weekly_preview
# ---------------------------------------------------------------------------

_EARNINGS_SQL = f"""
WITH holdings AS (
    SELECT DISTINCT UPPER(TRIM(underlying_symbol)) AS symbol
    FROM `{_PROJECT}.int_enriched_current`
    WHERE quantity IS NOT NULL AND quantity != 0
      AND CAST(user_id AS STRING) = @user_id
      AND tenant_id IN UNNEST(@tenant_ids)
)
SELECT e.symbol, e.next_earnings_date,
       DATE_DIFF(e.next_earnings_date, CURRENT_DATE(), DAY) AS days_until
FROM `{_PROJECT}.stg_earnings_calendar` e
JOIN holdings h USING (symbol)
WHERE e.next_earnings_date BETWEEN CURRENT_DATE()
                              AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
ORDER BY e.next_earnings_date, e.symbol
"""

_EXPIRATIONS_SQL = f"""
SELECT underlying_symbol AS symbol, instrument_type, option_strike AS strike, option_expiry,
       DATE_DIFF(option_expiry, CURRENT_DATE(), DAY) AS days_until
FROM `{_PROJECT}.int_enriched_current`
WHERE instrument_type IN ('Call', 'Put')
  AND option_expiry BETWEEN CURRENT_DATE()
                       AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
  AND quantity IS NOT NULL AND quantity != 0
  AND CAST(user_id AS STRING) = @user_id
  AND tenant_id IN UNNEST(@tenant_ids)
ORDER BY option_expiry, underlying_symbol
"""

# Projected ex-dividends: same cadence heuristic as weekly_review's
# UPCOMING_DIVIDENDS_QUERY (median spacing of the last ~6 ex-div events),
# scoped to the user's currently-held equity.
_EX_DIVS_SQL = f"""
WITH holdings AS (
    SELECT DISTINCT UPPER(TRIM(underlying_symbol)) AS symbol
    FROM `{_PROJECT}.int_enriched_current`
    WHERE quantity IS NOT NULL AND quantity != 0
      AND instrument_type = 'Equity'
      AND CAST(user_id AS STRING) = @user_id
      AND tenant_id IN UNNEST(@tenant_ids)
),
ex_divs AS (
    SELECT UPPER(TRIM(symbol)) AS symbol, date AS ex_div_date,
           ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(symbol)) ORDER BY date DESC) AS rn
    FROM `{_PROJECT}.stg_daily_prices`
    WHERE dividend IS NOT NULL AND dividend > 0
),
recent AS (
    SELECT symbol, ex_div_date,
           LAG(ex_div_date) OVER (PARTITION BY symbol ORDER BY ex_div_date) AS prev_ex_div_date
    FROM ex_divs WHERE rn <= 6
),
cadence AS (
    SELECT symbol, APPROX_QUANTILES(DATE_DIFF(ex_div_date, prev_ex_div_date, DAY), 2)[OFFSET(1)] AS median_spacing_days
    FROM recent WHERE prev_ex_div_date IS NOT NULL GROUP BY symbol
),
last_event AS (
    SELECT symbol, ex_div_date AS last_ex_div_date FROM ex_divs WHERE rn = 1
),
projected AS (
    SELECT le.symbol,
           DATE_ADD(le.last_ex_div_date, INTERVAL COALESCE(c.median_spacing_days, 91) DAY) AS projected_next_ex_div_date
    FROM last_event le LEFT JOIN cadence c USING (symbol)
)
SELECT h.symbol, p.projected_next_ex_div_date,
       DATE_DIFF(p.projected_next_ex_div_date, CURRENT_DATE(), DAY) AS days_until
FROM holdings h JOIN projected p USING (symbol)
WHERE p.projected_next_ex_div_date BETWEEN CURRENT_DATE()
                                      AND DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY p.projected_next_ex_div_date
"""


def _build_weekly_preview(client, bigquery, user_id, tenant_ids):
    cfg = bigquery.QueryJobConfig(query_parameters=_scope_params(bigquery, user_id, tenant_ids))

    def _q(sql):
        try:
            return list(client.query(sql, job_config=cfg).result())
        except Exception as exc:
            print(f"User {user_id}: preview sub-query failed: {exc}", file=sys.stderr)
            return []

    earnings = [
        f"{r['symbol']} reports in {int(r['days_until'])}d ({r['next_earnings_date']:%b %d})"
        for r in _q(_EARNINGS_SQL)
    ]
    expirations = [
        f"{r['symbol']} {r['instrument_type']} ${float(r['strike']):g} expires in {int(r['days_until'])}d"
        for r in _q(_EXPIRATIONS_SQL)
    ]
    ex_divs = [
        f"{r['symbol']} ~{r['projected_next_ex_div_date']:%b %d} (in {int(r['days_until'])}d)"
        for r in _q(_EX_DIVS_SQL)
    ]
    return {"earnings": earnings, "expirations": expirations, "ex_dividends": ex_divs}


def run_weekly_preview(client, bigquery):
    from app.email import send_weekly_preview_email, app_base_url
    from app.models import (
        get_tenant_ids_for_user,
        list_email_recipients_for_kind,
        record_email_send,
    )

    this_week = date.today() - timedelta(days=date.today().weekday())
    sent = skipped = empty = 0
    for rec in list_email_recipients_for_kind("weekly_preview"):
        user_id = rec["user_id"]
        tenant_ids = get_tenant_ids_for_user(user_id)
        if not tenant_ids:
            empty += 1
            continue
        preview = _build_weekly_preview(client, bigquery, user_id, tenant_ids)
        if not (preview["earnings"] or preview["expirations"] or preview["ex_dividends"]):
            empty += 1
            continue

        dedupe_key = f"{user_id}:{this_week.isoformat()}"
        if not record_email_send("weekly_preview", dedupe_key, user_id=user_id, to_email=rec["email"]):
            skipped += 1
            continue

        unsub = f"{app_base_url()}/email/unsubscribe/{rec.get('email_unsubscribe_token') or ''}"
        send_weekly_preview_email(
            to=rec["email"],
            username=rec["username"],
            preview=preview,
            dashboard_url=f"{app_base_url()}/daily-review",
            unsubscribe_url=unsub,
        )
        sent += 1
        print(f"User {user_id}: weekly_preview sent to {rec['email']}")
    print(f"weekly_preview: {sent} sent, {skipped} already-sent, {empty} no-data")


# ---------------------------------------------------------------------------
# reengagement
# ---------------------------------------------------------------------------


def run_reengagement(client, bigquery):
    from app.email import send_reengagement_email, app_base_url
    from app.models import list_dormant_email_recipients, record_email_send

    sent = skipped = 0
    for rec in list_dormant_email_recipients(REENGAGE_MIN_DAYS, REENGAGE_MAX_DAYS):
        user_id = rec["user_id"]
        last_visit = rec.get("last_visit_at")
        # Dedupe on the dormancy episode: one nudge per (user, last-visit
        # anchor). If they return and lapse again, last_visit moves and a
        # fresh nudge becomes eligible.
        anchor = last_visit.date().isoformat() if hasattr(last_visit, "date") else str(last_visit)[:10]
        dedupe_key = f"{user_id}:{anchor}"
        if not record_email_send("reengagement", dedupe_key, user_id=user_id, to_email=rec["email"]):
            skipped += 1
            continue

        unsub = f"{app_base_url()}/email/unsubscribe/{rec.get('email_unsubscribe_token') or ''}"
        send_reengagement_email(
            to=rec["email"],
            username=rec["username"],
            days_away=int(rec.get("days_away") or REENGAGE_MIN_DAYS),
            dashboard_url=f"{app_base_url()}/daily-review",
            unsubscribe_url=unsub,
        )
        sent += 1
        print(f"User {user_id}: reengagement sent to {rec['email']}")
    print(f"reengagement: {sent} sent, {skipped} already-sent")


# ---------------------------------------------------------------------------
# connection_reminder — recurring "still disconnected" nudge
# ---------------------------------------------------------------------------


def run_connection_reminder(client, bigquery):
    """Weekly follow-up email for users whose broker connection is still
    broken. Pure Postgres (no BigQuery).

    Cadence = once-then-weekly: the one-time ``connection_dropped`` email
    (fired by ``app/snaptrade_sync_cli.py`` the moment the break is detected)
    covers week 0; this cron covers weeks 1, 2, 3, ... until the user
    reconnects. ``week_index = stale_days // 7`` and the ``email_sends`` dedupe
    key embeds it, so a DAILY cron sends at most one reminder per 7-day band
    per break episode (``connection_broken_at`` is preserved across syncs, so
    the episode anchor is stable; a reconnect+re-break starts a fresh anchor).
    Transactional account-health mail — no opt-out.
    """
    from datetime import date as _date
    from app.email import send_connection_reminder_email, app_base_url
    from app.models import list_broken_snaptrade_connections, record_email_send

    reconnect_url = f"{app_base_url()}/profile?tab=account#snaptrade-sync"
    today = _date.today()
    sent = skipped = early = 0
    for rec in list_broken_snaptrade_connections():
        broken_at = rec.get("connection_broken_at")
        broken_on = broken_at.date() if hasattr(broken_at, "date") else None
        if broken_on is None:
            continue
        stale_days = max(0, (today - broken_on).days)
        week_index = stale_days // 7
        # Week 0 is owned by the one-time connection_dropped email.
        if week_index < 1:
            early += 1
            continue

        broken_key = broken_at.isoformat() if hasattr(broken_at, "isoformat") else str(broken_at)
        dedupe_key = f"{rec['snaptrade_account_id']}:{broken_key}:w{week_index}"
        if not record_email_send(
            "connection_reminder", dedupe_key,
            user_id=rec["user_id"], to_email=rec["email"],
        ):
            skipped += 1
            continue

        try:
            send_connection_reminder_email(
                to=rec["email"],
                username=rec["username"],
                broker_label=(rec.get("broker_slug") or "").title(),
                account_label=rec.get("display_nickname") or rec.get("account_name") or "",
                stale_days=stale_days,
                reconnect_url=reconnect_url,
            )
            sent += 1
            print(f"User {rec['user_id']} ({rec['snaptrade_account_id']}): "
                  f"connection_reminder sent (day {stale_days}) to {rec['email']}")
        except Exception as exc:
            print(f"User {rec['user_id']} ({rec['snaptrade_account_id']}): "
                  f"connection_reminder failed: {exc}", file=sys.stderr)
    print(f"connection_reminder: {sent} sent, {skipped} already-sent, {early} within-week-0")


_RUNNERS = {
    "weekly_summary": run_weekly_summary,
    "weekly_preview": run_weekly_preview,
    "reengagement": run_reengagement,
    "connection_reminder": run_connection_reminder,
}


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 1 or argv[0] not in _RUNNERS:
        print(
            "Usage: python -m app.email_digests_cli "
            "{weekly_summary|weekly_preview|reengagement|connection_reminder}",
            file=sys.stderr,
        )
        return 2

    kind = argv[0]
    from app.models import init_db
    init_db()

    # re-engagement and connection_reminder are pure Postgres (no digest
    # content from BigQuery), so skip building the BQ client — those crons
    # need not carry BQ creds.
    _NO_BQ = {"reengagement", "connection_reminder"}
    client = None
    bigquery = None
    if kind not in _NO_BQ:
        from google.cloud import bigquery
        from app.bigquery_client import get_bigquery_client
        client = get_bigquery_client()

    _RUNNERS[kind](client, bigquery)
    return 0


if __name__ == "__main__":
    sys.exit(main())
