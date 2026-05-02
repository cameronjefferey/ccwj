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


class TestAccountNameClaim:
    """An account label can be linked to at most one user."""

    def test_second_user_cannot_claim_same_label(self, db_conn):
        from app.models import (
            AccountClaimedError,
            account_is_claimed_by_other,
            add_account_for_user,
        )

        owner = _unique_username("acct_owner")
        intruder = _unique_username("acct_intruder")
        owner_id = _create_user(db_conn, owner)
        intruder_id = _create_user(db_conn, intruder)

        label = f"Brokerage_{uuid.uuid4().hex[:6]}"
        add_account_for_user(owner_id, label)

        # The challenger sees the label as claimed before they try.
        assert account_is_claimed_by_other(intruder_id, label) is True

        # And actually trying to insert raises.
        with pytest.raises(AccountClaimedError):
            add_account_for_user(intruder_id, label)

    def test_same_user_can_re_link_same_label(self, db_conn):
        """Idempotent for the same user — re-linking is a no-op, not an error."""
        from app.models import add_account_for_user, get_accounts_for_user

        user = _unique_username("acct_repeat")
        user_id = _create_user(db_conn, user)
        label = f"MyBrokerage_{uuid.uuid4().hex[:6]}"

        add_account_for_user(user_id, label)
        add_account_for_user(user_id, label)  # second call must not raise

        accounts = get_accounts_for_user(user_id)
        assert accounts.count(label) == 1

    def test_case_and_whitespace_variants_collide(self, db_conn):
        """ 'Brokerage', ' brokerage ', 'BROKERAGE' all map to the same claim."""
        from app.models import AccountClaimedError, add_account_for_user

        owner = _unique_username("acct_case_owner")
        intruder = _unique_username("acct_case_intruder")
        owner_id = _create_user(db_conn, owner)
        intruder_id = _create_user(db_conn, intruder)

        suffix = uuid.uuid4().hex[:6]
        add_account_for_user(owner_id, f"Brokerage{suffix}")

        with pytest.raises(AccountClaimedError):
            add_account_for_user(intruder_id, f"  brokerage{suffix}  ")
        with pytest.raises(AccountClaimedError):
            add_account_for_user(intruder_id, f"BROKERAGE{suffix}")


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
