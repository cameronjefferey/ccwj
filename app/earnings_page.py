"""Earnings Watch page (/earnings) — EarningsFollower tandem surface.

Extracted verbatim from app/routes.py (routes.py refactor, Aug 2026).
Endpoint name unchanged (`earnings_watch`).
"""

from datetime import date

import pandas as pd
from flask import render_template, request, abort, url_for
# current_user / is_admin are not referenced directly, but the earnings test
# fixture patch.object()s them on this module — keep them importable.
from flask_login import login_required, current_user  # noqa: F401
from app.models import is_admin  # noqa: F401
from google.cloud import bigquery

from app import app
from app.bigquery_client import get_bigquery_client
from app.query_cache import cached_query_df
from app.tenant_scope import tenant_sql_and as _tenant_sql_and
from app.utils import earnings_follower_url, user_local_today
from app.routes import (
    _bq_parallel,
    _redirect_if_no_accounts,
    _tenants_for_scope,
    _user_account_list,
)


# ════════════════════════════════════════════════════════════════════════
# Earnings Watch (/earnings) — tandem-product surface for EarningsFollower
# ════════════════════════════════════════════════════════════════════════
#
# Two scoped sections, both deep-linking out to the separately-deployed
# EarningsFollower web app (deep-links only — see app.utils.earnings_follower_url):
#   1. "Your positions reporting soon" — held symbols with an upcoming
#      next_earnings_date. Same shape/source as the Daily Review earnings
#      block (stg_earnings_calendar ⋈ currently-held holdings), tenant-scoped
#      via {tenant_filter} on the holdings CTE.
#   2. "Movers in your sectors" — recent big % movers (mart_sector_movers,
#      symbol-grain market data) restricted to the sectors the user actually
#      holds. The sector list is derived from tenant-scoped holdings, so the
#      selection is user-scoped even though mart_sector_movers carries no
#      tenant columns (it's public price data — nothing to leak).

# Held holdings + their sector context (tenant-scoped). Drives both the
# earnings join symbol set and the "your sectors" list for the movers query.
EARNINGS_WATCH_HELD_QUERY = """
SELECT DISTINCT
    UPPER(TRIM(ec.underlying_symbol)) AS symbol,
    COALESCE(m.sector, 'Unknown')     AS sector,
    COALESCE(m.subsector, 'Unknown')  AS subsector,
    m.long_name
FROM `ccwj-dbt.analytics.int_enriched_current` ec
LEFT JOIN `ccwj-dbt.analytics.stg_symbol_metadata` m
    ON UPPER(TRIM(ec.underlying_symbol)) = m.symbol
WHERE ec.quantity IS NOT NULL AND ec.quantity != 0
  {tenant_filter}
"""

# Upcoming earnings for currently-held holdings (next 21 days). Mirrors
# weekly_review.EARNINGS_UPCOMING_QUERY; the symbol set is narrowed to the
# user's holdings inside the CTE so the result is already tenant-safe.
EARNINGS_WATCH_UPCOMING_QUERY = """
WITH holdings AS (
    SELECT DISTINCT UPPER(TRIM(underlying_symbol)) AS symbol
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE quantity IS NOT NULL AND quantity != 0
      {tenant_filter}
)
-- No DATE_DIFF "days until" column ON PURPOSE: CURRENT_DATE() is UTC and
-- rolls to tomorrow at 5pm PT / 8pm ET, so a SQL day count is off by one
-- every US evening (plus query-cache staleness). Window padded a day each
-- side; Python computes days_until vs the user's local today and drops
-- anything already past.
SELECT
    e.symbol,
    e.next_earnings_date,
    m.long_name,
    COALESCE(m.sector, 'Unknown')    AS sector,
    COALESCE(m.subsector, 'Unknown') AS subsector
FROM `ccwj-dbt.analytics.stg_earnings_calendar` e
JOIN holdings h USING (symbol)
LEFT JOIN `ccwj-dbt.analytics.stg_symbol_metadata` m USING (symbol)
WHERE e.next_earnings_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                              AND DATE_ADD(CURRENT_DATE(), INTERVAL 22 DAY)
ORDER BY e.next_earnings_date, e.symbol
"""

# Big recent movers in a set of sectors (symbol-grain market data; no tenant
# columns). @sectors is a user-derived held-sector list bound as a query
# parameter (no string interpolation). @min_abs_move gates "big" moves.
# No tight LIMIT here: the mart is small (one row per platform-traded symbol)
# and the per-sector display cap is applied in Python after grouping, so one
# noisy sector can't starve the others.
EARNINGS_WATCH_MOVERS_QUERY = """
SELECT
    symbol,
    latest_close,
    pct_change,
    abs_pct_change,
    sector,
    subsector,
    long_name
FROM `ccwj-dbt.analytics.mart_sector_movers`
WHERE sector IN UNNEST(@sectors)
  AND abs_pct_change >= @min_abs_move
ORDER BY abs_pct_change DESC
LIMIT 200
"""

# Display cap per sector group on /earnings ("show me what's impacted",
# not "show me everything that moved").
EARNINGS_WATCH_MOVERS_PER_SECTOR = 8


@app.route("/earnings")
@login_required
def earnings_watch():
    """Earnings Watch — held positions reporting soon + same-sector movers,
    with deep-links out to the EarningsFollower tandem product."""
    if not app.config.get("EARNINGS_FOLLOWER_ENABLED", True):
        abort(404)

    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce

    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    context = {
        "title": "Earnings Watch",
        "upcoming_earnings": [],
        "mover_groups": [],
        "held_sectors": [],
        "accounts": sorted(user_accounts) if user_accounts else [],
        "selected_account": selected_account,
        "error": None,
    }

    try:
        client = get_bigquery_client()

        # "held" and "upcoming" are independent — run them in one parallel
        # wave through the query cache (this page used to run 3 serial
        # uncached queries and clocked 7-14s in production).
        batch = _bq_parallel(client, {
            "earnings_held": EARNINGS_WATCH_HELD_QUERY.format(tenant_filter=tenant_filter),
            "earnings_upcoming": EARNINGS_WATCH_UPCOMING_QUERY.format(tenant_filter=tenant_filter),
        })
        held_df = batch.get("earnings_held", pd.DataFrame())
        # Held symbols are scoped in SQL; the symbol set drives the rest.
        held_symbols = set()
        held_sectors = set()
        if not held_df.empty:
            held_symbols = {
                str(s).strip().upper() for s in held_df["symbol"].dropna().tolist()
            }
            held_sectors = {
                str(s).strip() for s in held_df["sector"].dropna().tolist()
                if str(s).strip() and str(s).strip() != "Unknown"
            }
        context["held_sectors"] = sorted(held_sectors)

        # ── Upcoming earnings (held positions) ────────────────────────
        try:
            earn_df = batch.get("earnings_upcoming", pd.DataFrame())
            today = user_local_today()
            for _, row in earn_df.iterrows():
                ed = row.get("next_earnings_date")
                if ed is None or (hasattr(ed, "__float__") and pd.isna(ed)):
                    continue
                ed_date = ed.date() if hasattr(ed, "date") and not isinstance(ed, date) else ed
                # Day count vs the USER's today (profile tz), not SQL's UTC
                # CURRENT_DATE() — see EARNINGS_WATCH_UPCOMING_QUERY comment.
                try:
                    days_until = (ed_date - today).days
                except TypeError:
                    days_until = None
                if days_until is not None and days_until < 0:
                    continue  # already reported in the user's timezone
                sector = str(row.get("sector") or "")
                subsector = str(row.get("subsector") or "")
                context["upcoming_earnings"].append({
                    "symbol": str(row.get("symbol") or ""),
                    "company": str(row.get("long_name") or ""),
                    "sector": sector if sector != "Unknown" else "",
                    "subsector": subsector if subsector != "Unknown" else "",
                    "days_until": days_until,
                    "earnings_date_display": (
                        ed_date.strftime("%a %b %-d") if hasattr(ed_date, "strftime") else str(ed_date)[:10]
                    ),
                    "ef_url": earnings_follower_url(
                        symbol=row.get("symbol"), sector=sector, subsector=subsector
                    ),
                })
        except Exception as e:
            app.logger.warning("Earnings Watch upcoming query failed: %s", e)

        # ── Movers grouped by YOUR holdings ───────────────────────────
        # Shape: one group per held sector — "you hold MU, NVDA (Technology);
        # here's what's moving around them." Holdings sharing a sector are
        # deliberately merged into one group; the point is "which of my
        # symbols are impacted," not a market-wide mover feed.
        if held_sectors:
            try:
                cfg = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ArrayQueryParameter("sectors", "STRING", sorted(held_sectors)),
                    bigquery.ScalarQueryParameter("min_abs_move", "FLOAT64", 0.05),
                ])
                mov_df = cached_query_df(
                    client, EARNINGS_WATCH_MOVERS_QUERY, job_config=cfg,
                    label="earnings_movers",
                )

                # Held symbols per sector (the group headers).
                held_by_sector = {}
                for _, hrow in held_df.iterrows():
                    hsec = str(hrow.get("sector") or "").strip()
                    if not hsec or hsec == "Unknown":
                        continue
                    hsym = str(hrow.get("symbol") or "").strip().upper()
                    if hsym:
                        held_by_sector.setdefault(hsec, set()).add(hsym)

                # Peer movers per sector (held symbols excluded — those are
                # covered by the position pages / Today's movers).
                movers_by_sector = {}
                for _, row in mov_df.iterrows():
                    sym = str(row.get("symbol") or "").strip().upper()
                    if not sym or sym in held_symbols:
                        continue
                    sector = str(row.get("sector") or "")
                    bucket = movers_by_sector.setdefault(sector, [])
                    if len(bucket) >= EARNINGS_WATCH_MOVERS_PER_SECTOR:
                        continue
                    pct = row.get("pct_change")
                    pct = float(pct) if pct is not None and not pd.isna(pct) else None
                    subsector = str(row.get("subsector") or "")
                    bucket.append({
                        "symbol": sym,
                        "company": str(row.get("long_name") or ""),
                        "sector": sector if sector != "Unknown" else "",
                        "subsector": subsector if subsector != "Unknown" else "",
                        "pct_change": pct,
                        "ef_url": earnings_follower_url(
                            symbol=sym, sector=sector, subsector=subsector
                        ),
                    })

                # Assemble groups: every held sector appears (even with no
                # movers — "nothing notable around these" is information),
                # ordered by biggest peer move so the loudest group leads.
                groups = []
                for sec in sorted(held_by_sector):
                    movers = movers_by_sector.get(sec, [])
                    groups.append({
                        "sector": sec,
                        "held": sorted(held_by_sector[sec]),
                        "movers": movers,
                        "max_abs_move": max(
                            (abs(m["pct_change"]) for m in movers
                             if m["pct_change"] is not None),
                            default=0.0,
                        ),
                    })
                groups.sort(key=lambda g: g["max_abs_move"], reverse=True)
                context["mover_groups"] = groups
            except Exception as e:
                app.logger.warning("Earnings Watch movers query failed: %s", e)
    except Exception as e:
        if app.debug:
            raise
        app.logger.warning("Earnings Watch page failed: %s", e)
        context["error"] = "Couldn't load earnings data right now."

    return render_template("earnings_watch.html", **context)


# ════════════════════════════════════════════════════════════════════════
# EarningsFollower → HappyTrader bridge (/earningsfollower[/<symbol>])
# ════════════════════════════════════════════════════════════════════════
#
# The INBOUND half of the tandem-product loop. /earnings and the Daily
# Review deep-link OUT to EarningsFollower; this is where EarningsFollower
# links back IN. It is a public, branded interstitial (no login) that
# explains how the two products fit together and then hands the visitor
# into the live demo.
#
# The demo is a mirror of the EarningsFollower trading bot's own account
# (see AGENTS.md "The public demo is a MIRROR of a real tenant"), so the
# story is literally true: EarningsFollower finds the setup, the bot
# trades it, HappyTrader shows how it actually went.
#
# TENANCY: this page is unauthenticated, so it may ONLY ever read the demo
# tenant. The query below hardcodes tenant_id = 'demo:demo-account' — it
# takes no account/tenant input from the request and must never be
# generalized to accept one. The symbol comes from the URL and is bound as
# a query PARAMETER, never interpolated.
DEMO_TENANT_ID = "demo:demo-account"

EF_BRIDGE_SYMBOL_QUERY = """
SELECT
    symbol,
    ANY_VALUE(company_name)          AS company_name,
    SUM(total_pnl)                   AS total_pnl,
    SUM(realized_pnl)                AS realized_pnl,
    SUM(num_individual_trades)       AS num_trades,
    MIN(first_trade_date)            AS first_trade_date,
    MAX(last_trade_date)             AS last_trade_date,
    STRING_AGG(DISTINCT strategy ORDER BY strategy) AS strategies,
    MAX(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) AS is_open
FROM `ccwj-dbt.analytics.positions_summary`
WHERE tenant_id = @tenant_id
  AND UPPER(TRIM(symbol)) = @symbol
GROUP BY symbol
"""


@app.route("/earningsfollower")
@app.route("/earningsfollower/<symbol>")
def earnings_follower_bridge(symbol=None):
    """Branded interstitial for visitors arriving from EarningsFollower.

    Public by design — this is a top-of-funnel landing surface. It reads
    ONLY the demo tenant (see the tenancy note above).
    """
    if not app.config.get("EARNINGS_FOLLOWER_ENABLED", True):
        abort(404)

    sym = (symbol or request.args.get("symbol") or "").strip().upper()
    # Cheap sanity bound so a junk path can't reach BigQuery at all.
    if sym and (len(sym) > 12 or not sym.replace(".", "").replace("-", "").isalnum()):
        sym = ""

    position = None
    if sym:
        try:
            client = get_bigquery_client()
            job_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("tenant_id", "STRING", DEMO_TENANT_ID),
                bigquery.ScalarQueryParameter("symbol", "STRING", sym),
            ])
            df = client.query(
                EF_BRIDGE_SYMBOL_QUERY, job_config=job_config
            ).to_dataframe()
            if not df.empty:
                r = df.iloc[0]
                position = {
                    "symbol": sym,
                    "company_name": (str(r.get("company_name"))
                                     if r.get("company_name") else None),
                    "total_pnl": float(r.get("total_pnl") or 0),
                    "realized_pnl": float(r.get("realized_pnl") or 0),
                    "num_trades": int(r.get("num_trades") or 0),
                    "first_trade_date": r.get("first_trade_date"),
                    "last_trade_date": r.get("last_trade_date"),
                    "strategies": (str(r.get("strategies"))
                                   if r.get("strategies") else None),
                    "is_open": bool(r.get("is_open")),
                }
        except Exception as e:
            # Never 500 a marketing landing page over a teaser query.
            app.logger.warning("EF bridge teaser query failed for %s: %s", sym, e)

    # Where the CTA sends them. An already-signed-in real user should land
    # in THEIR OWN data, not be bounced into the demo (demo_start refuses
    # to switch an authenticated session anyway).
    if sym and position:
        demo_target = url_for("position_detail", symbol=sym)
    else:
        demo_target = url_for("weekly_review")

    if current_user.is_authenticated:
        cta_url = demo_target
        cta_label = "View in your account" if sym and position else "Go to your dashboard"
    else:
        cta_url = url_for("demo_start", next=demo_target)
        cta_label = f"See how {sym} played out" if position else "Explore the live demo"

    return render_template(
        "ef_bridge.html",
        title=f"{sym} — EarningsFollower + HappyTrader" if sym else "EarningsFollower + HappyTrader",
        symbol=sym,
        position=position,
        cta_url=cta_url,
        cta_label=cta_label,
        ef_url=earnings_follower_url(symbol=sym or None),
    )


