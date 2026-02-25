"""
Integration tests for user data isolation.

Verifies that user A cannot access user B's data. Every query must scope by
user_id = current_user_id. No exceptions.

Run: pytest tests/test_data_isolation.py -v
"""
import uuid

import pytest

# Ensure conftest runs first (sets DATABASE_PATH, SECRET_KEY)


def _unique_username(prefix: str = "test_iso") -> str:
    """Generate a unique username to avoid collisions across test runs."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_user(conn, username: str, password: str = "testpass123") -> int:
    """Create a user and return their id."""
    from werkzeug.security import generate_password_hash
    conn.execute(
        "INSERT INTO users (username, password_hash) VALUES (?, ?)",
        (username, generate_password_hash(password)),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    return row["id"]


def _close_db(conn):
    """Ensure DB connection is closed to avoid locks."""
    try:
        conn.close()
    except Exception:
        pass


def _login(client, username: str, password: str = "testpass123"):
    """Log in and return the response (to verify redirect)."""
    return client.post(
        "/login",
        data={"username": username, "password": password},
        follow_redirects=True,
    )


class TestJournalDataIsolation:
    """Journal entries must be scoped by user_id."""

    def test_user_cannot_view_another_users_journal_entry(self, client, db_conn):
        """User B requests User A's journal entry by ID -> must get 404/redirect, not A's data."""
        user_a = _unique_username("user_a")
        user_b = _unique_username("user_b")
        user_a_id = _create_user(db_conn, user_a)
        user_b_id = _create_user(db_conn, user_b)
        _close_db(db_conn)

        from app.models import add_account_for_user, create_journal_entry

        add_account_for_user(user_a_id, "AcctA")
        entry_id = create_journal_entry(
            user_a_id,
            account="AcctA",
            symbol="AAPL",
            strategy="Covered Call",
            trade_open_date="2025-01-15",
        )

        # User B logs in and tries to view User A's entry
        _login(client, user_b)
        r = client.get(f"/journal/{entry_id}", follow_redirects=True)

        # Must NOT show the entry - redirect to journal list with "not found"
        assert r.status_code == 200
        assert b"Journal entry not found" in r.data or b"not found" in r.data.lower()
        # Must NOT leak user A's data (thesis, notes, etc.)
        assert b"user_a_secret" not in r.data.lower()

    def test_user_cannot_edit_another_users_journal_entry(self, client, db_conn):
        """User B POSTs to update User A's journal entry -> must not modify A's data."""
        user_a = _unique_username("edit_a")
        user_b = _unique_username("edit_b")
        user_a_id = _create_user(db_conn, user_a)
        user_b_id = _create_user(db_conn, user_b)
        _close_db(db_conn)

        from app.models import add_account_for_user, create_journal_entry, get_journal_entry

        add_account_for_user(user_a_id, "AcctA")
        entry_id = create_journal_entry(
            user_a_id,
            account="AcctA",
            symbol="AAPL",
            strategy="Covered Call",
            trade_open_date="2025-01-15",
            thesis="User A secret thesis",
        )

        _login(client, user_b)
        r = client.post(
            f"/journal/{entry_id}",
            data={
                "trade_close_date": "",
                "trade_symbol": "",
                "thesis": "HACKED BY USER B",
                "notes": "",
                "reflection": "",
                "confidence": "",
                "mood": "",
                "sleep_quality": "",
                "entry_time": "",
                "tags": "",
            },
            follow_redirects=True,
        )

        # Entry must be unchanged - User A's thesis still intact
        entry = get_journal_entry(entry_id, user_a_id)
        assert entry is not None
        assert "HACKED BY USER B" not in (entry.get("thesis") or "")

    def test_user_cannot_delete_another_users_journal_entry(self, client, db_conn):
        """User B POSTs to delete User A's journal entry -> must not delete."""
        user_a = _unique_username("del_a")
        user_b = _unique_username("del_b")
        user_a_id = _create_user(db_conn, user_a)
        user_b_id = _create_user(db_conn, user_b)
        _close_db(db_conn)

        from app.models import add_account_for_user, create_journal_entry, get_journal_entry

        add_account_for_user(user_a_id, "AcctA")
        entry_id = create_journal_entry(
            user_a_id,
            account="AcctA",
            symbol="AAPL",
            strategy="Covered Call",
            trade_open_date="2025-01-15",
        )

        _login(client, user_b)
        r = client.post(f"/journal/{entry_id}/delete", follow_redirects=True)

        # Entry must still exist for User A
        entry = get_journal_entry(entry_id, user_a_id)
        assert entry is not None


class TestInsightDataIsolation:
    """Insights (AI analysis cache) must be scoped by user_id."""

    def test_user_cannot_access_another_users_insight(self, client, db_conn):
        """Insights page for User B must not show User A's cached insight."""
        user_a = _unique_username("insight_a")
        user_b = _unique_username("insight_b")
        user_a_id = _create_user(db_conn, user_a)
        user_b_id = _create_user(db_conn, user_b)
        _close_db(db_conn)

        from app.models import add_account_for_user, save_insight

        add_account_for_user(user_a_id, "AcctA")
        add_account_for_user(user_b_id, "AcctB")
        save_insight(user_a_id, "User A secret summary", "User A full analysis content")

        _login(client, user_b)
        r = client.get("/insights")

        assert r.status_code == 200
        # Must NOT contain User A's content
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
        _close_db(db_conn)

        from app.models import add_account_for_user, save_mirror_score

        add_account_for_user(user_a_id, "AcctA")
        add_account_for_user(user_b_id, "AcctB")
        save_mirror_score(
            user_a_id, "2025-01-06", 90, 85, 88, 92, 89, "High",
            "User A mirror diagnostic",
        )

        # User B logs in - must not see User A's cached mirror score
        _login(client, user_b)
        r = client.get("/mirror-score?week=2025-01-06")

        # User B has no data - should get "No Mirror Score" or similar, NOT User A's score
        assert r.status_code == 200
        assert b"User A mirror diagnostic" not in r.data
