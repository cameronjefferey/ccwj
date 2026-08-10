"""Shared per-symbol queries + the Cmd+K nav API.

The Daily P&L page (/symbols, endpoint `symbols_detail`) was retired in the
Aug 2026 surface audit — Position Detail answers everything it did, one
symbol at a time, with the tab strip for flipping between symbols. The URL
301s to /positions so bookmarks keep working.

What survives here:
- TRADES_QUERY / CURRENT_POSITIONS_QUERY — consumed by /accounts
  (app/accounts_page.py imports them).
- NAV_SYMBOLS_QUERY + /api/nav/symbols — backs the Cmd+K quick-switcher
  (app/static/js/nav.js).
"""

from flask import jsonify, redirect, request, url_for
from flask_login import login_required

from app import app
from app.bigquery_client import get_bigquery_client
from app.query_cache import cached_query_df
from app.tenant_scope import tenant_sql_and as _tenant_sql_and
from app.routes import _tenants_for_scope


TRADES_QUERY = """
    SELECT
        account,
        tenant_id,
        underlying_symbol AS symbol,
        trade_date,
        action,
        action_raw,
        trade_symbol,
        instrument_type,
        description,
        quantity,
        price,
        fees,
        amount
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE underlying_symbol IS NOT NULL
      AND trade_date IS NOT NULL
      {tenant_filter}
    ORDER BY underlying_symbol, trade_date
"""

CURRENT_POSITIONS_QUERY = """
    SELECT
        account,
        tenant_id,
        underlying_symbol AS symbol,
        instrument_type,
        trade_symbol,
        description,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        unrealized_pnl_pct
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE 1=1 {tenant_filter}
"""


@app.route("/symbols")
@login_required
def symbols_detail():
    """Legacy Daily P&L page — permanently moved to /positions."""
    account = request.args.get("account", "")
    return redirect(url_for("positions", account=account or None), code=301)


NAV_SYMBOLS_QUERY = """
SELECT
    symbol,
    MAX(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) AS has_open
FROM `ccwj-dbt.analytics.positions_summary`
WHERE symbol IS NOT NULL AND symbol != ''
  {tenant_filter}
GROUP BY symbol
ORDER BY has_open DESC, symbol
"""


@app.route("/api/nav/symbols")
@login_required
def api_nav_symbols():
    """Symbols the current user can open in Position Detail, for the
    Cmd+K quick-switcher. Open positions first, then closed. Cached via
    the shared query cache (10 min L1 / flushed-on-rebuild L2), so the
    palette costs ~nothing after the first open."""
    tenant_ids = _tenants_for_scope(request.args.get("account", ""))
    tenant_filter = _tenant_sql_and(tenant_ids)
    try:
        client = get_bigquery_client()
        df = cached_query_df(client, NAV_SYMBOLS_QUERY.format(tenant_filter=tenant_filter))
    except Exception as exc:
        app.logger.warning("nav symbols query failed: %s", exc)
        return jsonify({"symbols": []})
    out = []
    for _, r in df.iterrows():
        sym = str(r.get("symbol") or "").strip()
        if sym:
            out.append({"s": sym, "open": bool(int(r.get("has_open") or 0))})
    return jsonify({"symbols": out})
