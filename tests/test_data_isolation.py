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


class TestBigQueryFrameAccountFilter:
    """Symbol-scoped BQ queries return all accounts in the dataset; app must filter."""

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
