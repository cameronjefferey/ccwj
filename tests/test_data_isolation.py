"""
Integration tests for user data isolation.

Verifies that user A cannot access user B's data. Every query that returns
user-scoped rows must scope by ``user_id`` (Postgres app tables) or by the
caller's account list (BigQuery-derived DataFrames).

These tests cover the two layers we rely on:

1. **Postgres app tables** (insights, mirror scores, profiles, etc.) — every
   read takes a ``user_id`` and the route uses ``current_user.id`` only.
   We log in as user B and assert user A's content doesn't appear.

2. **BigQuery DataFrames** (``_filter_df_by_accounts``,
   ``_account_sql_filter``) — every BQ result is account-scoped before
   render. The dataset is multi-tenant, so a missing scope leaks. We
   exercise the helpers directly with adversarial inputs.

There is no journal coverage anymore: the journal feature was removed
from the product (see ``AGENTS.md`` → "Journal — REMOVED"); the routes
and models no longer exist.

Run: ``pytest tests/test_data_isolation.py -v``

Requires ``TEST_DATABASE_URL`` to point at a throwaway Postgres database;
``conftest.py`` skips the suite when it isn't set.
"""
import os
import uuid

import pytest

# Importing anything from ``app.*`` triggers ``app/__init__.py`` → ``init_db()``,
# which needs a real Postgres connection. Skip the whole module up front when
# the throwaway test DB isn't configured (same gate ``conftest.py`` uses).
pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL not set; skipping DB-dependent tests",
)


def _unique_username(prefix: str = "test_iso") -> str:
    """Generate a unique username to avoid collisions across test runs."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_user(conn, username: str, password: str = "testpass123") -> int:
    """Create a user via the same Postgres connection the test fixture yields.

    The ``db_conn`` fixture hands us a psycopg connection wrapped in
    ``with conn:`` (auto-commits on clean exit), so we must use a cursor
    rather than ``conn.execute`` (which does not exist on psycopg) and we
    do not call ``conn.commit`` ourselves — the fixture's context handles
    that when the test function returns.
    """
    from werkzeug.security import generate_password_hash

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id",
            (username, generate_password_hash(password)),
        )
        row = cur.fetchone()
    return int(row["id"])


def _login(client, username: str, password: str = "testpass123"):
    """Log in via the test client. follow_redirects so we land on the post-login page."""
    return client.post(
        "/login",
        data={"username": username, "password": password},
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# Postgres app-table isolation
# ---------------------------------------------------------------------------


class TestInsightDataIsolation:
    """Insights (AI analysis cache) must be scoped by user_id."""

    def test_user_cannot_access_another_users_insight(self, client, db_conn):
        """Insights page for User B must not show User A's cached insight."""
        user_a = _unique_username("insight_a")
        user_b = _unique_username("insight_b")
        user_a_id = _create_user(db_conn, user_a)
        user_b_id = _create_user(db_conn, user_b)

        from app.models import add_account_for_user, save_insight

        add_account_for_user(user_a_id, "AcctA")
        add_account_for_user(user_b_id, "AcctB")
        save_insight(user_a_id, "User A secret summary", "User A full analysis content")

        _login(client, user_b)
        r = client.get("/insights")

        assert r.status_code == 200
        assert b"User A secret summary" not in r.data
        assert b"User A full analysis" not in r.data


class TestMirrorScoreDataIsolation:
    """Mirror scores must be scoped by user_id."""

    def test_mirror_score_uses_current_user_only(self, client, db_conn):
        """Mirror score route must use current_user.id, not any request parameter."""
        user_a = _unique_username("ms_a")
        user_b = _unique_username("ms_b")
        user_a_id = _create_user(db_conn, user_a)
        user_b_id = _create_user(db_conn, user_b)

        from app.models import add_account_for_user, save_mirror_score

        add_account_for_user(user_a_id, "AcctA")
        add_account_for_user(user_b_id, "AcctB")
        save_mirror_score(
            user_a_id, "2025-01-06", 90, 85, 88, 92, 89, "High",
            "User A mirror diagnostic",
        )

        _login(client, user_b)
        r = client.get("/mirror-score?week=2025-01-06")

        # User B has no data; must NOT see User A's diagnostic sentence even
        # though the URL has an explicit week parameter.
        assert r.status_code == 200
        assert b"User A mirror diagnostic" not in r.data


class TestSharedAccountLabel:
    """Two users CAN share an account label — common case is family
    members (a parent and a kid both calling theirs "Schwab Account").
    The security boundary is ``user_id``, not ``account_name``: every
    BQ query / DataFrame is scoped by ``user_id`` (see
    docs/USER_ID_TENANCY.md), so neither user can see the other's
    rows even when the label is identical.

    These tests pin the new contract so a future regression that
    re-introduces a ``account_name`` global-unique index is caught.
    """

    def test_two_users_can_claim_same_label(self, db_conn):
        from app.models import add_account_for_user, get_accounts_for_user

        a = _unique_username("share_a")
        b = _unique_username("share_b")
        a_id = _create_user(db_conn, a)
        b_id = _create_user(db_conn, b)

        label = f"Family Brokerage {uuid.uuid4().hex[:6]}"
        add_account_for_user(a_id, label)
        add_account_for_user(b_id, label)  # must not raise

        assert label in get_accounts_for_user(a_id)
        assert label in get_accounts_for_user(b_id)

    def test_same_user_relinking_is_idempotent(self, db_conn):
        """Re-linking the same (user, label) pair stays a no-op — that's
        what the per-user uniqueness on (user_id, account_name) gives us."""
        from app.models import add_account_for_user, get_accounts_for_user

        user_id = _create_user(db_conn, _unique_username("acct_repeat"))
        label = f"MyBrokerage_{uuid.uuid4().hex[:6]}"

        add_account_for_user(user_id, label)
        add_account_for_user(user_id, label)

        accounts = get_accounts_for_user(user_id)
        assert accounts.count(label) == 1

    def test_account_is_claimed_by_other_is_deprecated_no_op(self):
        """The pre-flight check is now always False. Kept callable so old
        upload/Schwab call sites don't crash if they linger."""
        from app.models import account_is_claimed_by_other

        assert account_is_claimed_by_other(1, "Anything") is False
        assert account_is_claimed_by_other(2, "Anything") is False

    def test_find_cross_tenant_account_conflicts_is_deprecated_no_op(self):
        from app.models import find_cross_tenant_account_conflicts

        assert find_cross_tenant_account_conflicts(["A", "B"]) == set()
        assert find_cross_tenant_account_conflicts([]) == set()

    def test_user_account_list_returns_shared_label(self, app, db_conn):
        """``_user_account_list()`` is THE gate to multi-tenant data — it
        used to strip labels that collided across users. Now it returns
        the user's labels as-is (the user_id-aware SQL/DataFrame
        filters downstream are what keep the data separate)."""
        from app.models import User, add_account_for_user
        from app.routes import _user_account_list
        from flask_login import login_user

        legit_id = _create_user(db_conn, _unique_username("share_legit"))
        other_id = _create_user(db_conn, _unique_username("share_other"))
        shared = f"Shared_{uuid.uuid4().hex[:6]}"
        solo = f"SoloLegit_{uuid.uuid4().hex[:6]}"

        add_account_for_user(legit_id, solo)
        add_account_for_user(legit_id, shared)
        add_account_for_user(other_id, shared)  # collision is allowed now

        with app.test_request_context("/positions"):
            login_user(User.get_by_id(legit_id))
            allowed = _user_account_list()
            assert solo in allowed
            assert shared in allowed, (
                "Shared labels are intentionally allowed under user_id "
                "tenancy. If this assertion ever fails, somebody re-"
                "introduced cross-user account-name uniqueness without "
                "first removing the user_id-scoped tenant filters that "
                "make sharing safe (see docs/USER_ID_TENANCY.md)."
            )


class TestPasswordResetTokens:
    """One-time password reset tokens: single-use, expiring, hashed at rest."""

    def test_mint_then_consume_returns_user_id(self, db_conn):
        from app.models import (
            consume_password_reset_token,
            mint_password_reset_token,
        )

        user_id = _create_user(db_conn, _unique_username("rst_ok"))
        raw = mint_password_reset_token(user_id, requester_ip="10.0.0.1")
        assert isinstance(raw, str) and len(raw) > 20

        consumed = consume_password_reset_token(raw)
        assert consumed == user_id

    def test_token_is_single_use(self, db_conn):
        from app.models import (
            consume_password_reset_token,
            mint_password_reset_token,
        )

        user_id = _create_user(db_conn, _unique_username("rst_used"))
        raw = mint_password_reset_token(user_id)

        first = consume_password_reset_token(raw)
        second = consume_password_reset_token(raw)
        assert first == user_id
        assert second is None

    def test_unknown_token_returns_none(self):
        from app.models import consume_password_reset_token

        assert consume_password_reset_token("definitely-not-a-real-token") is None
        assert consume_password_reset_token("") is None

    def test_minting_revokes_prior_active_tokens(self, db_conn):
        """If a user requests two reset emails in a row, only the latest
        link should work; the older one is invalidated immediately."""
        from app.models import (
            consume_password_reset_token,
            mint_password_reset_token,
        )

        user_id = _create_user(db_conn, _unique_username("rst_revoke"))
        old = mint_password_reset_token(user_id)
        new = mint_password_reset_token(user_id)

        # Old link no longer works.
        assert consume_password_reset_token(old) is None
        # New link works once.
        assert consume_password_reset_token(new) == user_id

    def test_token_is_hashed_at_rest(self, db_conn):
        """The raw URL token must never be stored in plaintext — a DB
        leak would otherwise hand out reset access for every live link."""
        from app.models import mint_password_reset_token

        user_id = _create_user(db_conn, _unique_username("rst_hash"))
        raw = mint_password_reset_token(user_id)

        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT token_hash FROM password_reset_tokens "
                "WHERE user_id = %s ORDER BY id DESC LIMIT 1",
                (user_id,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["token_hash"] != raw
        assert raw not in (row["token_hash"] or "")


class TestLoginLockout:
    """Per-username lockout: 5 failures within 15 min → cooldown."""

    def test_under_threshold_returns_zero(self, db_conn):
        from app.models import (
            login_lockout_remaining_seconds,
            record_login_attempt,
        )

        username = _unique_username("ll_under")
        _create_user(db_conn, username)

        for _ in range(3):
            record_login_attempt(username, success=False, ip_address="10.0.0.1")
        assert login_lockout_remaining_seconds(username) == 0

    def test_threshold_triggers_cooldown(self, db_conn):
        from app.models import (
            LOGIN_FAILURE_LIMIT,
            LOGIN_LOCKOUT_MINUTES,
            login_lockout_remaining_seconds,
            record_login_attempt,
        )

        username = _unique_username("ll_lock")
        _create_user(db_conn, username)
        for _ in range(LOGIN_FAILURE_LIMIT):
            record_login_attempt(username, success=False, ip_address="10.0.0.1")

        remaining = login_lockout_remaining_seconds(username)
        assert remaining > 0
        # Cap should be at most the configured window (sliding from now).
        assert remaining <= LOGIN_LOCKOUT_MINUTES * 60 + 5

    def test_successful_login_clears_failures(self, db_conn):
        """A correct login resets the counter — typo'd legitimate users
        do not accumulate a permanent hole toward lockout."""
        from app.models import (
            LOGIN_FAILURE_LIMIT,
            login_lockout_remaining_seconds,
            record_login_attempt,
        )

        username = _unique_username("ll_clear")
        _create_user(db_conn, username)
        for _ in range(LOGIN_FAILURE_LIMIT - 1):
            record_login_attempt(username, success=False, ip_address="10.0.0.1")

        record_login_attempt(username, success=True, ip_address="10.0.0.1")
        # Now even a fresh batch of failures shouldn't already be locked.
        for _ in range(LOGIN_FAILURE_LIMIT - 1):
            record_login_attempt(username, success=False, ip_address="10.0.0.1")
        assert login_lockout_remaining_seconds(username) == 0

    def test_lookup_is_case_insensitive(self, db_conn):
        """Lockout key normalizes username so 'Alice' and 'alice' are one."""
        from app.models import (
            LOGIN_FAILURE_LIMIT,
            login_lockout_remaining_seconds,
            record_login_attempt,
        )

        username = "Alice_" + uuid.uuid4().hex[:6]
        _create_user(db_conn, username)
        for _ in range(LOGIN_FAILURE_LIMIT):
            record_login_attempt(username.lower(), success=False, ip_address="10.0.0.1")
        # Querying with mixed case still sees the lock.
        assert login_lockout_remaining_seconds(username.upper()) > 0
        assert login_lockout_remaining_seconds(username) > 0


class TestEmailAddressUniqueness:
    """users.email is globally unique (case-insensitive); also nullable."""

    def test_lookup_by_email_is_case_insensitive(self, db_conn):
        from app.models import User

        user_id = _create_user(db_conn, _unique_username("em_case"))
        unique = uuid.uuid4().hex[:6]
        canonical = f"Foo.Bar.{unique}@Example.com"
        User.update_email(user_id, canonical)

        # Stored lowercased on update; lookup is case-insensitive both ways.
        a = User.get_by_email(canonical.lower())
        b = User.get_by_email(canonical.upper())
        assert a is not None
        assert b is not None
        assert int(a.id) == int(b.id) == user_id


class TestUserProfileIsolation:
    """User profile reads/writes must be scoped to current_user.id."""

    def test_profile_read_returns_only_own_row(self, db_conn):
        user_a = _unique_username("prof_a")
        user_b = _unique_username("prof_b")
        user_a_id = _create_user(db_conn, user_a)
        user_b_id = _create_user(db_conn, user_b)

        from app.models import update_user_profile, get_user_profile

        update_user_profile(
            user_a_id,
            display_name="User A Display",
            headline="A's headline",
            bio="A's private bio",
        )

        prof_b = get_user_profile(user_b_id)
        assert prof_b["display_name"] is None
        assert prof_b["headline"] is None
        assert (prof_b["bio"] or "") != "A's private bio"


# ---------------------------------------------------------------------------
# BigQuery / DataFrame defense-in-depth
# ---------------------------------------------------------------------------
#
# These tests do NOT need TEST_DATABASE_URL because they exercise pure
# Python helpers. They guard the fix that was put in place after the
# previous incident where unscoped symbol-only BQ queries leaked other
# users' positions onto a logged-in user's pages.
# ---------------------------------------------------------------------------


class TestBigQueryFrameAccountFilter:
    """BQ symbol-scoped queries return all accounts in the dataset.

    The app must filter every DataFrame down to the caller's accounts
    before merge or render. ``_filter_df_by_accounts`` is the choke point.
    """

    def test_filter_df_by_accounts_keeps_only_linked_accounts(self):
        import pandas as pd

        from app.routes import _filter_df_by_accounts

        df = pd.DataFrame(
            {
                "account": ["Schwab Account", "General", "investment1"],
                "total_pnl": [1.0, 2.0, 3.0],
            }
        )
        out = _filter_df_by_accounts(df, ["Schwab Account"])
        assert len(out) == 1
        assert str(out["account"].iloc[0]) == "Schwab Account"

    def test_filter_empty_accounts_returns_empty_frame(self):
        import pandas as pd

        from app.routes import _filter_df_by_accounts

        df = pd.DataFrame({"account": ["Other"], "x": [1]})
        out = _filter_df_by_accounts(df, [])
        assert len(out) == 0

    def test_filter_none_means_admin_no_filter(self):
        """None means admin → no filter (rule documents this explicitly)."""
        import pandas as pd

        from app.routes import _filter_df_by_accounts

        df = pd.DataFrame({"account": ["A", "B", "C"], "x": [1, 2, 3]})
        out = _filter_df_by_accounts(df, None)
        assert len(out) == 3

    def test_filter_normalizes_int_account_ids_to_strings(self):
        """BQ may return account as int (Schwab numeric id) while the app
        carries string labels. Comparison must coerce both sides to trimmed
        str so an int 12345 still matches the string '12345'."""
        import pandas as pd

        from app.routes import _filter_df_by_accounts

        df = pd.DataFrame({"account": [12345, 67890], "x": [1, 2]})
        out = _filter_df_by_accounts(df, ["12345"])
        assert len(out) == 1

    def test_filter_does_not_partial_match_other_user_accounts(self):
        """Substring of another user's account label must NOT match."""
        import pandas as pd

        from app.routes import _filter_df_by_accounts

        df = pd.DataFrame(
            {
                "account": ["Schwab Account", "Schwab Account 2", "General"],
                "x": [1, 2, 3],
            }
        )
        out = _filter_df_by_accounts(df, ["Schwab Account"])
        # Only the exact-match row, not "Schwab Account 2".
        assert len(out) == 1
        assert str(out["account"].iloc[0]) == "Schwab Account"


class TestAccountSqlFilter:
    """SQL-side scoping fragment must be safe even when input is empty/odd."""

    def test_empty_account_list_emits_where_1_eq_0(self):
        """Empty list → user has no accounts → SQL must return zero rows."""
        from app.routes import _account_sql_filter, _account_sql_and

        assert _account_sql_filter([]) == "WHERE 1 = 0"
        assert _account_sql_and([]) == "AND 1 = 0"

    def test_none_account_list_emits_no_filter(self):
        """None → admin → SQL fragment is empty (no WHERE/AND added)."""
        from app.routes import _account_sql_filter, _account_sql_and

        assert _account_sql_filter(None) == ""
        assert _account_sql_and(None) == ""

    def test_account_list_quotes_and_escapes_single_quote(self):
        """A label containing a single quote must be escaped to prevent SQL
        injection through the user_accounts table."""
        from app.routes import _account_sql_filter

        sql = _account_sql_filter(["O'Brien Brokerage"])
        # Doubled apostrophe is the SQL-standard escape and is what the
        # helper produces. The label appears once, with no broken string.
        assert "O''Brien Brokerage" in sql
        # And no stray unescaped apostrophe immediately after the closing
        # paren that would let a payload break out.
        assert "'O'Brien Brokerage'" not in sql

    def test_account_list_uses_trimmed_string_compare(self):
        """The fragment normalizes the BQ side with TRIM(CAST(... AS STRING))
        so int account ids and right-padded strings still match."""
        from app.routes import _account_sql_filter

        sql = _account_sql_filter(["Schwab Account"])
        assert "TRIM(CAST(account AS STRING))" in sql


class TestDailyPnlChartDedup:
    """The /accounts and /position/<symbol> charts iterate ``mart_daily_pnl``
    row by row. If the same (account, symbol, date) shows up more than
    once — e.g. a legacy user_id-NULL row plus a backfilled populated
    row both passing through ``_filter_df_by_user``'s Stage 0/1
    leniency — the buy/sell ledger accumulates the same trade N times
    AND the unrealized loop runs N times, multiplying equity P&L by
    N**2.

    A real bug (May 2026): a real ~$700k LEAP spike rendered as $17M on
    /accounts because the user's data had ~5 dups per business key.
    These tests pin the dedup so the helpers stay defensive.
    """

    def _make_dup_row(self, *, account, symbol, date_, user_id):
        return {
            "account": account,
            "user_id": user_id,
            "symbol": symbol,
            "date": date_,
            "options_amount": 0.0,
            "dividends_amount": 0.0,
            "equity_buy_qty": 100.0,
            "equity_buy_cost": 50_000.0,
            "equity_sell_qty": 0.0,
            "equity_sell_proceeds": 0.0,
            "other_amount": 0.0,
            "close_price": 1000.0,
            "has_trade": True,
            "option_market_value": 0.0,
            "option_cost_basis": 0.0,
            "cumulative_options_pnl": 0.0,
            "cumulative_dividends_pnl": 0.0,
            "cumulative_other_pnl": 0.0,
        }

    def test_account_chart_dedupes_duplicate_business_keys(self):
        """5 dups on the same (account, symbol, date) must NOT
        N**2-amplify equity P&L. Without dedup the equity series for
        this fixture would land in the millions; with dedup it lands
        on the single-row value."""
        import pandas as pd
        from datetime import date

        from app.routes import _build_account_chart_from_daily_pnl

        # 5 dups: 1 with populated user_id, 4 with NULL
        rows = [
            self._make_dup_row(
                account="Shared", symbol="X", date_=date(2025, 1, 15),
                user_id=6,
            ),
            *[
                self._make_dup_row(
                    account="Shared", symbol="X", date_=date(2025, 1, 15),
                    user_id=None,
                )
                for _ in range(4)
            ],
        ]
        daily_df = pd.DataFrame(rows)
        out = _build_account_chart_from_daily_pnl(daily_df, pd.DataFrame())

        # Single row would give: shares=100, cost=50000, close=1000
        # → unrealized = 100*1000 - 50000 = 50_000.
        # 5 dups WITHOUT dedup would give: shares=500, cost=250000
        # → per-row unrealized = 500*1000 - 250000 = 250_000
        # → loop runs 5 times → 5*250_000 = 1_250_000 (25x).
        assert out["equity"] == [50_000.0], (
            f"Equity P&L was amplified by duplicate business keys: "
            f"got {out['equity']}, expected [50000.0]. The dedup at "
            f"the top of _build_account_chart_from_daily_pnl is the "
            f"only thing preventing N**2 inflation when "
            f"_filter_df_by_user keeps NULL user_id legacy rows."
        )

    def test_position_chart_dedupes_duplicate_business_keys(self):
        """Same defense for the per-symbol chart used on /position/<symbol>."""
        import pandas as pd
        from datetime import date

        from app.routes import _build_chart_from_daily_pnl

        rows = [
            self._make_dup_row(
                account="Shared", symbol="X", date_=date(2025, 1, 15),
                user_id=6,
            ),
            *[
                self._make_dup_row(
                    account="Shared", symbol="X", date_=date(2025, 1, 15),
                    user_id=None,
                )
                for _ in range(4)
            ],
        ]
        daily_df = pd.DataFrame(rows)
        out = _build_chart_from_daily_pnl(daily_df, pd.DataFrame())

        # Same arithmetic: a single-row chart gives shares=100, cost=50000,
        # close=1000 → unrealized 50_000.
        assert out["equity"] == [50_000.0], (
            f"Per-symbol chart equity P&L was amplified: got "
            f"{out['equity']}, expected [50000.0]"
        )

    def test_dedupe_prefers_populated_user_id(self):
        """When both NULL and populated user_id rows exist, the
        populated one must win — we want the row with full tenancy
        info so any downstream re-filter still has a user_id to match."""
        import pandas as pd
        from datetime import date

        from app.routes import _build_account_chart_from_daily_pnl

        # The populated-user_id row has different numbers from the NULL
        # row; if we keep the wrong one, the chart shows the wrong P&L.
        populated = self._make_dup_row(
            account="Shared", symbol="X", date_=date(2025, 1, 15), user_id=6,
        )
        null_row = self._make_dup_row(
            account="Shared", symbol="X", date_=date(2025, 1, 15), user_id=None,
        )
        null_row["equity_buy_qty"] = 999.0
        null_row["equity_buy_cost"] = 999_000.0

        daily_df = pd.DataFrame([null_row, populated])
        out = _build_account_chart_from_daily_pnl(daily_df, pd.DataFrame())
        # If dedup picked the populated row: 100*1000 - 50000 = 50_000.
        # If dedup picked the NULL row: 999*1000 - 999000 = 0.
        assert out["equity"] == [50_000.0]


class TestStrategyFitQueryTenancy:
    """Strategy-fit reads from positions_summary AND int_option_trade_kinds —
    both are multi-tenant marts. Each query template MUST carry an
    {account_filter} placeholder (see .cursor/rules/bigquery-tenant-isolation.mdc)
    so the dispatcher can scope by the user's accounts."""

    def test_summary_query_has_account_filter_placeholder(self):
        from app.routes import STRATEGY_FIT_QUERY
        assert "{account_filter}" in STRATEGY_FIT_QUERY

    def test_options_query_has_account_filter_placeholder(self):
        from app.routes import STRATEGY_FIT_OPTIONS_QUERY
        assert "{account_filter}" in STRATEGY_FIT_OPTIONS_QUERY

    def test_options_query_selects_account_column(self):
        # The DataFrame-side belt (`_filter_df_by_accounts`) needs an
        # `account` column. If a future refactor drops it from SELECT
        # the safety net silently disappears.
        from app.routes import STRATEGY_FIT_OPTIONS_QUERY
        assert "account" in STRATEGY_FIT_OPTIONS_QUERY.lower()


class TestWealthQueryTenancy:
    """The /wealth page reads from ``mart_wealth_daily`` which is shared
    across tenants in BigQuery. The query template must carry the
    standard ``{account_filter}`` placeholder so the dispatcher can
    scope by the caller's accounts; the route must then run a
    DataFrame-side filter for defense-in-depth (per
    ``.cursor/rules/bigquery-tenant-isolation.mdc``)."""

    def test_query_has_account_filter_placeholder(self):
        from app.wealth import WEALTH_DAILY_QUERY
        assert "{account_filter}" in WEALTH_DAILY_QUERY

    def test_query_selects_account_and_user_id_columns(self):
        # Both columns are required for ``_filter_df_by_accounts`` to
        # apply its user_id-aware filter. Dropping either silently
        # removes the safety net.
        from app.wealth import WEALTH_DAILY_QUERY
        sql = WEALTH_DAILY_QUERY.lower()
        assert "account" in sql
        assert "user_id" in sql

    def test_chart_payload_aggregates_per_date_only_within_scope(self):
        """``_build_chart_payload`` is what feeds the chart JSON. If a
        cross-tenant row ever slipped through to the route, the chart
        would inflate. The route already filters by account before
        calling this helper, so we verify the helper correctly sums
        only the rows it's handed (rather than e.g. dropping the
        account column and pulling extra rows from somewhere else)."""
        import pandas as pd
        from datetime import date

        from app.wealth import _build_chart_payload

        df = pd.DataFrame({
            "account": ["A", "A", "B", "B"],
            "user_id": [1, 1, 1, 1],
            "date": [date(2025, 1, 2), date(2025, 1, 3),
                     date(2025, 1, 2), date(2025, 1, 3)],
            "account_value": [100.0, 110.0, 200.0, 220.0],
            "cash_value":    [10.0,  10.0,  20.0,  20.0],
            "equity_value":  [80.0,  90.0,  170.0, 180.0],
            "option_value":  [10.0,  10.0,  10.0,  20.0],
        })
        payload = _build_chart_payload(df)
        # Two distinct dates, sums combine A+B because we already
        # trust the upstream filter to have stripped foreign rows.
        assert payload["dates"] == ["2025-01-02", "2025-01-03"]
        assert payload["account_value"] == [300.0, 330.0]
        assert payload["cash"] == [30.0, 30.0]
        assert payload["equity"] == [250.0, 270.0]
        assert payload["options"] == [20.0, 30.0]

    def test_route_uses_filter_df_by_accounts(self):
        """Defense-in-depth: the route must call ``_filter_df_by_accounts``
        on every BQ DataFrame before render. Source-level check so a
        future refactor can't silently drop the post-filter."""
        import inspect
        from app import wealth

        src = inspect.getsource(wealth)
        assert "_filter_df_by_accounts" in src, (
            "wealth.py must call _filter_df_by_accounts on the BQ "
            "DataFrame before render — see "
            ".cursor/rules/bigquery-tenant-isolation.mdc"
        )


class TestUserIdTenancyHelpers:
    """``account_name`` alone is not a security boundary — two users can
    register the same nickname (the original ``investment1`` leak).
    The user_id tenancy helpers add a ``user_id`` predicate so a row
    is only returned when **both** ``user_id`` and ``account_name``
    match. See docs/USER_ID_TENANCY.md.
    """

    def test_user_scoped_filter_emits_user_id_predicate(self):
        from app.routes import _user_scoped_filter

        sql = _user_scoped_filter(7, ["Acct A"])
        # The user_id predicate is the actual security boundary.
        assert "user_id = 7" in sql
        # OR (user_id IS NULL) is the Stage 0/1 leniency leg for
        # legacy rows that have not been backfilled yet.
        assert "user_id IS NULL" in sql
        # Account predicate must still be present (defense in depth).
        assert "Acct A" in sql

    def test_user_scoped_filter_admin_skips_user_id(self):
        """user_id=None means admin → no user_id predicate."""
        from app.routes import _user_scoped_filter

        sql = _user_scoped_filter(None, ["Acct A"])
        assert "user_id" not in sql
        assert "Acct A" in sql

    def test_user_scoped_filter_qualifies_alias_in_join_context(self):
        """When the column is qualified (``sc.account``), the helper must
        prefix ``user_id`` with the same alias so JOINs aren't
        ambiguous."""
        from app.routes import _user_scoped_filter

        sql = _user_scoped_filter(11, ["Acct"], col="sc.account")
        assert "sc.user_id = 11" in sql
        assert "sc.user_id IS NULL" in sql

    def test_user_scoped_filter_empty_accounts_fails_closed(self):
        from app.routes import _user_scoped_filter

        sql = _user_scoped_filter(7, [])
        assert "1 = 0" in sql

    def test_filter_df_by_user_drops_rows_owned_by_other_users(self):
        """The actual ``investment1`` leak: two users register the same
        account label, BQ returns rows for both, and account-only
        filtering passes everything through. ``_filter_df_by_user`` must
        drop rows whose ``user_id`` is a different populated id."""
        import pandas as pd

        from app.routes import _filter_df_by_user

        df = pd.DataFrame(
            {
                "account": ["investment1", "investment1", "investment1"],
                "user_id": [7, 8, 7],
                "total_pnl": [100, -200, 50],
            }
        )
        out = _filter_df_by_user(df, 7, ["investment1"])
        # User 8's row must be dropped even though account_name matches.
        assert sorted(out["total_pnl"].tolist()) == [50, 100]

    def test_filter_df_by_user_keeps_legacy_null_rows_for_owned_account(self):
        """Stage 0/1 leniency: legacy rows with NULL user_id are kept
        as long as the account belongs to the current user. This is the
        bridge that lets the migration land before backfill."""
        import pandas as pd

        from app.routes import _filter_df_by_user

        df = pd.DataFrame(
            {
                "account": ["Acct A", "Acct A", "Acct B"],
                "user_id": [None, 7, None],
                "total_pnl": [10, 20, 30],
            }
        )
        out = _filter_df_by_user(df, 7, ["Acct A"])
        assert sorted(out["total_pnl"].tolist()) == [10, 20]

    def test_filter_df_by_user_drops_legacy_null_for_unowned_account(self):
        """A NULL user_id row whose account is NOT in the user's allowed
        list must still be dropped. Otherwise the leniency leg becomes
        a leak."""
        import pandas as pd

        from app.routes import _filter_df_by_user

        df = pd.DataFrame(
            {
                "account": ["other_user_acct"],
                "user_id": [None],
                "total_pnl": [999],
            }
        )
        out = _filter_df_by_user(df, 7, ["my_acct"])
        assert out.empty

    def test_filter_df_by_user_admin_bypasses_user_check(self):
        import pandas as pd

        from app.routes import _filter_df_by_user

        df = pd.DataFrame(
            {
                "account": ["A", "B"],
                "user_id": [1, 2],
                "x": [10, 20],
            }
        )
        out = _filter_df_by_user(df, None, None)
        assert len(out) == 2

    def test_two_users_sharing_account_name_no_longer_leak(self):
        """The end-to-end regression for the ``investment1`` leak: User
        7 and User 8 both have an account labeled ``investment1``. BQ
        returns rows for both users. The frame coming back must contain
        ONLY rows whose ``user_id`` matches the caller — even though
        ``account_name`` is identical."""
        import pandas as pd

        from app.routes import _filter_df_by_user

        df = pd.DataFrame(
            {
                "account": ["investment1"] * 4,
                "user_id": [7, 8, 7, 8],
                "symbol": ["AAPL", "AAPL", "TSLA", "TSLA"],
                "total_pnl": [100, 999, 50, -777],
            }
        )

        u7 = _filter_df_by_user(df, 7, ["investment1"])
        u8 = _filter_df_by_user(df, 8, ["investment1"])

        assert sorted(u7["total_pnl"].tolist()) == [50, 100]
        assert sorted(u8["total_pnl"].tolist()) == [-777, 999]
        # Critical: neither user's frame contains the other's rows.
        assert 999 not in u7["total_pnl"].tolist()
        assert -777 not in u7["total_pnl"].tolist()
        assert 100 not in u8["total_pnl"].tolist()
        assert 50 not in u8["total_pnl"].tolist()


class TestStrategyFitMatrixBuilder:
    """`_build_strategy_fit_matrix` is dimension-agnostic and is the shared
    aggregation core for sector / subsector / dte / moneyness / market-cap
    matrices. These tests pin down the contract so a future refactor can't
    silently change cell counts, ordering, or equity-N/A handling."""

    def _df(self, rows):
        import pandas as pd
        return pd.DataFrame(rows)

    def test_sector_aggregation_matches_synthetic_input(self):
        from app.routes import _build_strategy_fit_matrix
        df = self._df([
            {"account": "A", "symbol": "AAPL", "strategy": "Wheel",
             "sector": "Tech", "total_pnl": 500, "realized_pnl": 500,
             "unrealized_pnl": 0, "num_individual_trades": 5,
             "num_winners": 4, "num_losers": 1},
            {"account": "A", "symbol": "XOM", "strategy": "Wheel",
             "sector": "Energy", "total_pnl": -200, "realized_pnl": -200,
             "unrealized_pnl": 0, "num_individual_trades": 3,
             "num_winners": 1, "num_losers": 2},
        ])
        m = _build_strategy_fit_matrix(df, col_field="sector")
        assert "Wheel" in m["row_labels"]
        # Cols sorted by total P&L desc.
        assert m["col_labels"] == ["Tech", "Energy"]
        assert m["cells"]["Wheel"]["Tech"]["total_pnl"] == 500
        # baseline_expectancy = total / trades.
        assert round(m["baseline_expectancy"], 4) == round(300 / 8, 4)

    def test_dte_uses_fixed_column_order_and_appends_equity_rows(self):
        from app.routes import _build_strategy_fit_matrix, DIM_FIXED_COL_ORDER
        df = self._df([
            {"account": "A", "symbol": "AAPL", "strategy": "Wheel",
             "dte_bucket": "31-60 DTE", "total_pnl": 100, "realized_pnl": 100,
             "unrealized_pnl": 0, "num_individual_trades": 1,
             "num_winners": 1, "num_losers": 0},
            {"account": "A", "symbol": "AAPL", "strategy": "Wheel",
             "dte_bucket": "0-7 DTE", "total_pnl": 50, "realized_pnl": 50,
             "unrealized_pnl": 0, "num_individual_trades": 1,
             "num_winners": 1, "num_losers": 0},
        ])
        m = _build_strategy_fit_matrix(
            df, col_field="dte_bucket",
            col_order_override=DIM_FIXED_COL_ORDER["dte"],
            equity_strategies=["Buy and Hold"],
        )
        assert m["col_labels"] == ["0-7 DTE", "31-60 DTE"]
        assert "Buy and Hold" in m["row_labels"]
        assert "Buy and Hold" in m["equity_strategies"]
        # Equity strategy must NOT have a row total (template renders N/A).
        assert "Buy and Hold" not in m["row_totals"]

    def test_empty_data_with_equity_still_lists_equity_rows(self):
        import pandas as pd
        from app.routes import _build_strategy_fit_matrix
        empty = pd.DataFrame(columns=[
            "account", "symbol", "strategy", "dte_bucket",
            "total_pnl", "realized_pnl", "unrealized_pnl",
            "num_individual_trades", "num_winners", "num_losers",
        ])
        m = _build_strategy_fit_matrix(
            empty, col_field="dte_bucket",
            equity_strategies=["Buy and Hold"],
        )
        assert m["row_labels"] == ["Buy and Hold"]
        assert m["col_labels"] == []

    def test_unknown_bucket_never_appears_in_sweet_or_soft_spots(self):
        """The Unknown bucket holds delisted / unclassified tickers and
        isn't actionable to call out — naming "Buy and Hold in Unknown"
        as edge or "Long Call in Unknown" as a drag is just noise. The
        cell stays in the matrix (user can toggle it visually) but must
        NEVER show up in the sweet/soft narrative."""
        from app.routes import _build_strategy_fit_matrix
        df = self._df([
            # Unknown sector: highest expectancy AND most negative — the
            # winner if we didn't filter Unknown out.
            {"account": "A", "symbol": "DWAC", "strategy": "Buy and Hold",
             "sector": "Unknown", "total_pnl": 50_000, "realized_pnl": 50_000,
             "unrealized_pnl": 0, "num_individual_trades": 10,
             "num_winners": 10, "num_losers": 0},
            {"account": "A", "symbol": "MGOL", "strategy": "Long Call",
             "sector": "Unknown", "total_pnl": -20_000, "realized_pnl": -20_000,
             "unrealized_pnl": 0, "num_individual_trades": 8,
             "num_winners": 0, "num_losers": 8},
            # Real, named sector — modest results, but should win sweet
            # because Unknown is excluded from narratives.
            {"account": "A", "symbol": "AAPL", "strategy": "Wheel",
             "sector": "Technology", "total_pnl": 600, "realized_pnl": 600,
             "unrealized_pnl": 0, "num_individual_trades": 6,
             "num_winners": 5, "num_losers": 1},
            {"account": "A", "symbol": "XOM", "strategy": "Long Call",
             "sector": "Energy", "total_pnl": -300, "realized_pnl": -300,
             "unrealized_pnl": 0, "num_individual_trades": 5,
             "num_winners": 1, "num_losers": 4},
        ])
        m = _build_strategy_fit_matrix(df, col_field="sector")
        sweet_sectors = {s["sector"] for s in m["sweet_spots"]}
        soft_sectors = {s["sector"] for s in m["soft_spots"]}
        assert "Unknown" not in sweet_sectors, (
            "Unknown must never be celebrated as a sweet spot — it's noise."
        )
        assert "Unknown" not in soft_sectors, (
            "Unknown must never be flagged as a soft spot — it's noise."
        )
        # Verify the named-sector winners actually surface.
        assert "Technology" in sweet_sectors
        assert "Energy" in soft_sectors
        # Sanity: the Unknown cells are still IN the matrix (just not narrated).
        assert "Unknown" in m["col_labels"]
