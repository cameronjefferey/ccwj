"""Tests for the EarningsFollower deep-link integration.

Two layers:
  1. Pure unit tests for the deep-link helper (app.utils.earnings_follower_url
     + earnings_follower_theme_for) — no DB, no app context required.
  2. Offline render tests for the /earnings (earnings_watch) route, with
     BigQuery + auth stubbed (same approach as test_positions_filter_discipline).
     These pin the wiring: held-earnings + same-sector movers render, peers
     that the user already holds are excluded, and the flag gates the page.
"""

from contextlib import contextmanager
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# --- Helper unit tests (no DB / no app context) ----------------------

class TestEarningsFollowerThemeFor:
    def test_semiconductors_subsector_maps_to_semis(self):
        from app.utils import earnings_follower_theme_for
        assert earnings_follower_theme_for(subsector="Semiconductors") == "semis"

    def test_case_insensitive(self):
        from app.utils import earnings_follower_theme_for
        assert earnings_follower_theme_for(subsector="  semiconductors  ") == "semis"

    def test_subsector_wins_over_sector(self):
        from app.utils import earnings_follower_theme_for
        # Sector is unmapped, subsector is mapped → subsector wins.
        assert earnings_follower_theme_for(sector="Technology", subsector="Semiconductors") == "semis"

    def test_unmapped_returns_none(self):
        from app.utils import earnings_follower_theme_for
        assert earnings_follower_theme_for(sector="Healthcare", subsector="Biotech") is None

    def test_empty_returns_none(self):
        from app.utils import earnings_follower_theme_for
        assert earnings_follower_theme_for() is None


class TestEarningsFollowerUrl:
    def test_no_args_returns_base(self):
        from app.utils import earnings_follower_url
        url = earnings_follower_url()
        assert url == "https://www.earningsfollower.com"
        assert "?" not in url

    def test_symbol_targets_company_page(self):
        from app.utils import earnings_follower_url
        assert earnings_follower_url(symbol="nvda") == \
            "https://www.earningsfollower.com/company/NVDA"

    def test_symbol_deeplink_ignores_theme(self):
        # A per-symbol link goes straight to the clean company page — no
        # ?theme / ?tab query string tacked on.
        from app.utils import earnings_follower_url
        url = earnings_follower_url(symbol="AMD", subsector="Semiconductors")
        assert url == "https://www.earningsfollower.com/company/AMD"
        assert "?" not in url

    def test_theme_derived_from_subsector_on_calendar_link(self):
        from app.utils import earnings_follower_url
        url = earnings_follower_url(subsector="Semiconductors")
        assert url == "https://www.earningsfollower.com?theme=semis"

    def test_explicit_theme_overrides_derivation(self):
        from app.utils import earnings_follower_url
        url = earnings_follower_url(theme="ai", subsector="Semiconductors")
        assert "theme=ai" in url
        assert "theme=semis" not in url

    def test_tab_param(self):
        from app.utils import earnings_follower_url
        assert earnings_follower_url(tab="this-week") == \
            "https://www.earningsfollower.com?tab=this-week"

    def test_uses_config_base_url_in_app_context(self):
        from app import app
        from app.utils import earnings_follower_url
        prev = app.config.get("EARNINGS_FOLLOWER_URL")
        app.config["EARNINGS_FOLLOWER_URL"] = "https://ef.example.test"
        try:
            with app.test_request_context():
                assert earnings_follower_url(symbol="TSLA") == \
                    "https://ef.example.test/company/TSLA"
        finally:
            app.config["EARNINGS_FOLLOWER_URL"] = prev


# --- Offline route render tests --------------------------------------

def _stub_user(user_id=42, username="acme"):
    u = MagicMock()
    u.is_authenticated = True
    u.is_active = True
    u.is_anonymous = False
    u.id = user_id
    u.username = username
    u.get_id = lambda: str(user_id)
    return u


_HELD_DF = pd.DataFrame([
    {"symbol": "AAPL", "sector": "Technology", "subsector": "Consumer Electronics", "long_name": "Apple Inc."},
    {"symbol": "JEPI", "sector": "Financial Services", "subsector": "ETF", "long_name": "JPMorgan Equity Premium"},
])

# next_earnings_date is dynamic (today+6): days_until is now computed in
# Python vs the user's local today (the SQL query no longer ships a UTC
# DATE_DIFF), and past dates are dropped — a fixed date would age out.
_EARNINGS_DF = pd.DataFrame([
    {"symbol": "AAPL", "next_earnings_date": date.today() + timedelta(days=6),
     "long_name": "Apple Inc.", "sector": "Technology", "subsector": "Consumer Electronics"},
])

# MSFT is a Technology peer the user does NOT hold → should render.
# AAPL is held → must be excluded from the movers section.
_MOVERS_DF = pd.DataFrame([
    {"symbol": "MSFT", "latest_close": 410.0, "pct_change": 0.082, "abs_pct_change": 0.082,
     "sector": "Technology", "subsector": "Software", "long_name": "Microsoft Corp."},
    {"symbol": "AAPL", "latest_close": 200.0, "pct_change": 0.07, "abs_pct_change": 0.07,
     "sector": "Technology", "subsector": "Consumer Electronics", "long_name": "Apple Inc."},
])


class _StubClient:
    """Return a different DataFrame per query based on SQL content."""

    def query(self, sql, **_kw):
        text = sql or ""
        if "mart_sector_movers" in text:
            df = _MOVERS_DF.copy()
        elif "stg_earnings_calendar" in text:
            df = _EARNINGS_DF.copy()
        else:
            df = _HELD_DF.copy()

        class _Job:
            def to_dataframe(self_inner):
                return df
        return _Job()


def _mock_tenants_for_scope(selected_account=None):
    return ["snaptrade:acme-fixture"]


@pytest.fixture
def earnings_client():
    from app import app
    import app.earnings_page as routes  # earnings_watch() lives here now

    user = _stub_user()
    with patch.object(routes, "current_user", user), \
         patch("flask_login.utils._get_user", lambda: user), \
         patch.object(routes, "_redirect_if_no_accounts", lambda: None), \
         patch.object(routes, "_user_account_list", lambda: ["Acme Investment"]), \
         patch.object(routes, "_tenants_for_scope", _mock_tenants_for_scope), \
         patch.object(routes, "is_admin", lambda u: False), \
         patch.object(routes, "get_bigquery_client", lambda: _StubClient()):
        with app.test_client() as c:
            yield c


def test_earnings_watch_renders_upcoming_and_movers(earnings_client):
    r = earnings_client.get("/earnings")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    # Held position reporting soon renders.
    assert "AAPL" in html
    # Same-sector peer (not held) renders in movers.
    assert "MSFT" in html
    # Deep-links out to EarningsFollower are present.
    assert "earningsfollower.com" in html


def test_earnings_watch_movers_grouped_by_holdings(earnings_client):
    """Movers are grouped under the user's holdings: a 'You hold' header
    with the held symbol pills, then that sector's peer movers below."""
    r = earnings_client.get("/earnings")
    html = r.get_data(as_text=True)
    assert "You hold" in html
    # AAPL (Technology holding) leads the group containing the MSFT mover.
    assert html.index("You hold") < html.index("MSFT")
    # Held pill links to the position page, not EF.
    assert 'class="ew-held-pill" href="/position/AAPL"' in html


def test_earnings_watch_excludes_held_symbols_from_movers(earnings_client):
    """AAPL is held, so even though it's in the movers fixture it must not
    appear as a 'peer mover' (that section is peers, not your holdings)."""
    r = earnings_client.get("/earnings")
    html = r.get_data(as_text=True)
    # MSFT mover move string shown; AAPL's +7.0% mover card must NOT be.
    assert "8.2%" in html
    assert "7.0%" not in html


def test_earnings_watch_404_when_flag_off(earnings_client):
    from app import app
    prev = app.config.get("EARNINGS_FOLLOWER_ENABLED", True)
    app.config["EARNINGS_FOLLOWER_ENABLED"] = False
    try:
        r = earnings_client.get("/earnings")
        assert r.status_code == 404
    finally:
        app.config["EARNINGS_FOLLOWER_ENABLED"] = prev


# --- Inbound bridge: /earningsfollower[/<symbol>] --------------------
#
# This is a PUBLIC (unauthenticated) surface that reads BigQuery, so the
# tenancy assertions below are the load-bearing ones: it must read the
# demo tenant and nothing else, and must never accept a tenant/account
# from the request. See .cursor/rules/bigquery-tenant-isolation.mdc.

_BRIDGE_DF = pd.DataFrame([{
    "symbol": "MU", "company_name": "Micron Technology",
    "total_pnl": 1369.0, "realized_pnl": 1369.0, "num_trades": 20,
    "first_trade_date": date(2026, 7, 1), "last_trade_date": date(2026, 8, 4),
    "strategies": "Cash-Secured Put,Strangle", "is_open": 0,
}])

_BRIDGE_ACCOUNT_DF = pd.DataFrame([{
    "symbols": 210, "total_pnl": -14960.87, "num_trades": 571,
    "winners": 134, "losers": 180, "since": date(2026, 6, 23),
}])

_BRIDGE_STRATEGY_DF = pd.DataFrame([
    {"strategy": "Buy and Hold", "pnl": 3343.91, "symbols": 176},
    {"strategy": "Strangle", "pnl": 3239.0, "symbols": 3},
    {"strategy": "Put Spread", "pnl": 785.0, "symbols": 9},
    {"strategy": "Naked Call", "pnl": -6414.0, "symbols": 9},
    {"strategy": "Cash-Secured Put", "pnl": -6845.0, "symbols": 4},
    {"strategy": "Call Spread", "pnl": -9045.0, "symbols": 17},
])


class _BridgeClient:
    """Capture every SQL + bound parameter set the bridge sends to BigQuery.

    The page issues several queries in one parallel wave, so the tenancy
    assertions have to hold for ALL of them, not just the last one.
    """

    def __init__(self, df, account_df=None, strategy_df=None):
        self.df = df
        self.account_df = (_BRIDGE_ACCOUNT_DF if account_df is None else account_df)
        self.strategy_df = (_BRIDGE_STRATEGY_DF if strategy_df is None else strategy_df)
        self.calls = []  # [(sql, {param: value}), ...]

    def query(self, sql, job_config=None, **_kw):
        params = {}
        if job_config is not None:
            params = {p.name: p.value for p in job_config.query_parameters}
        self.calls.append((sql, params))

        if "num_winners" in sql:
            df = self.account_df
        elif "GROUP BY\n    strategy" in sql or "GROUP BY strategy" in sql:
            df = self.strategy_df
        else:
            df = self.df

        class _Job:
            def to_dataframe(self_inner):
                return df
        return _Job()

    # -- convenience views over the captured calls --
    @property
    def symbol_calls(self):
        return [(s, p) for s, p in self.calls if "symbol" in p]

    def all_params(self, name):
        return [p.get(name) for _s, p in self.calls]


@contextmanager
def _bridge(df=_BRIDGE_DF, **kw):
    from app import app
    import app.earnings_page as routes

    stub = _BridgeClient(df, **kw)
    with patch.object(routes, "get_bigquery_client", lambda: stub):
        with app.test_client() as c:
            yield c, stub


def test_bridge_scopes_to_demo_tenant_only():
    """The unauthenticated bridge may only ever read the demo tenant, and
    the symbol must be a bound PARAMETER (never interpolated)."""
    with _bridge() as (c, stub):
        assert c.get("/earningsfollower/MU").status_code == 200
        assert stub.calls, "bridge issued no queries"
        # EVERY query in the wave — symbol, account scorecard, strategy
        # attribution — is pinned to the demo tenant.
        for sql, params in stub.calls:
            assert params.get("tenant_id") == "demo:demo-account", sql
            assert "@tenant_id" in sql
        sym_sql, sym_params = stub.symbol_calls[0]
        assert sym_params["symbol"] == "MU"
        # Symbol is bound, not concatenated into the SQL text.
        assert "MU" not in sym_sql
        assert "@symbol" in sym_sql


def test_bridge_ignores_request_supplied_tenant_and_account():
    """A visitor must not be able to widen the scope via query params."""
    with _bridge() as (c, stub):
        c.get("/earningsfollower/MU?tenant=snaptrade:someone-else&account=Schwab%20Account")
        assert set(stub.all_params("tenant_id")) == {"demo:demo-account"}


def test_bridge_renders_symbol_teaser_and_demo_cta():
    with _bridge() as (c, _stub):
        html = c.get("/earningsfollower/MU").get_data(as_text=True)
        assert "MU" in html
        assert "1,369.00" in html
        assert 'href="/demo/start?next=/position/MU"' in html


def test_bridge_shows_whole_account_not_just_the_clicked_symbol():
    """The bridge promises it isn't curated, so the unfiltered account
    result and the losing strategies must both be on the page."""
    with _bridge() as (c, _stub):
        html = c.get("/earningsfollower/MU").get_data(as_text=True)
        assert "-$14,961" in html          # account total, losses and all
        assert "43%" in html               # 134 / (134 + 180)
        assert "Call Spread" in html       # worst strategy shown
        assert "-$9,045" in html
        assert "Buy and Hold" in html      # best strategy shown


def test_bridge_handles_symbol_the_bot_never_traded():
    with _bridge(df=pd.DataFrame()) as (c, _stub):
        r = c.get("/earningsfollower/ZZZZ")
        assert r.status_code == 200
        html = r.get_data(as_text=True)
        # No teaser card, and the CTA falls back to the demo dashboard.
        assert "1,369.00" not in html
        assert "/demo/start" in html


def test_bridge_rejects_junk_symbol_without_querying_bigquery():
    """A junk path must never reach BigQuery as a symbol lookup (the
    account-level panels still render)."""
    with _bridge() as (c, stub):
        r = c.get("/earningsfollower/" + "A" * 40)
        assert r.status_code == 200
        assert stub.symbol_calls == []
        assert not any("A" * 40 in sql for sql, _p in stub.calls)


def test_bridge_survives_bigquery_failure():
    """A marketing landing page must never 500 over a teaser query."""
    from app import app
    import app.earnings_page as routes

    class _Boom:
        def query(self, *a, **k):
            raise RuntimeError("BQ down")

    with patch.object(routes, "get_bigquery_client", lambda: _Boom()):
        with app.test_client() as c:
            r = c.get("/earningsfollower/MU")
            assert r.status_code == 200
            assert "/demo/start" in r.get_data(as_text=True)


def test_bridge_404_when_flag_off():
    from app import app
    prev = app.config.get("EARNINGS_FOLLOWER_ENABLED", True)
    app.config["EARNINGS_FOLLOWER_ENABLED"] = False
    try:
        with app.test_client() as c:
            assert c.get("/earningsfollower/MU").status_code == 404
    finally:
        app.config["EARNINGS_FOLLOWER_ENABLED"] = prev


# --- demo_start ?next= open-redirect guard ---------------------------

@pytest.mark.parametrize("hostile", [
    "https://evil.com",
    "//evil.com",
    "/\\evil.com",
    "http://evil.com/path",
])
def test_demo_start_rejects_offsite_next(hostile):
    """?next= must never become an open redirect out of the product."""
    from app import app
    with app.test_client() as c:
        r = c.get(f"/demo/start?next={hostile}", follow_redirects=False)
        loc = r.headers.get("Location", "")
        assert "evil.com" not in loc
