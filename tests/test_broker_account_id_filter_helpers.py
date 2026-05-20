"""Tests for the Stage 3 broker_account_id filter helpers in
``app/routes.py``. See ``docs/BROKER_ACCOUNT_ID_MIGRATION.md`` Stage 3.

Goals:
- The new SQL helpers (``_broker_account_sql_and``,
  ``_broker_account_sql_filter``) emit predicates with the right
  shape for the defense-in-depth filter flip — additive to the
  legacy ``(account, user_id)`` clause, never narrowing admin reads.
- The new DataFrame helper (``_filter_df_by_broker_account_ids``)
  drops rows whose ``broker_account_id`` doesn't match the user's
  set, AND drops NULL rows (the orphan-tenant security guarantee),
  but tolerates the Stage 2 deploy gap when a mart hasn't
  propagated the column yet (no column → no filter).
- Admin bypass semantics match the existing
  ``_resolve_filter_user_id`` behavior.
"""
from __future__ import annotations

import pandas as pd
import pytest

from app.routes import (
    _broker_account_sql_and,
    _broker_account_sql_filter,
    _filter_df_by_broker_account_ids,
)


# ---------------------------------------------------------------------------
# SQL predicate shape
# ---------------------------------------------------------------------------


def test_broker_account_sql_and_admin_returns_empty():
    """Admin (``broker_account_ids is None``) must not narrow the query."""
    assert _broker_account_sql_and(None) == ""


def test_broker_account_sql_and_empty_list_is_lock_closed():
    """Authenticated user with NO broker_accounts must see no rows."""
    assert _broker_account_sql_and([]) == "AND 1 = 0"


def test_broker_account_sql_and_single_id():
    """One id renders as a clean IN clause."""
    assert _broker_account_sql_and([42]) == "AND broker_account_id IN (42)"


def test_broker_account_sql_and_multiple_ids_sorted_input():
    """Multiple ids preserve input order (caller-controlled)."""
    assert (
        _broker_account_sql_and([18, 19, 20])
        == "AND broker_account_id IN (18, 19, 20)"
    )


def test_broker_account_sql_and_with_custom_col():
    """Allow `dim.broker_account_id` form for joins."""
    assert (
        _broker_account_sql_and([42], col="d.broker_account_id")
        == "AND d.broker_account_id IN (42)"
    )


def test_broker_account_sql_and_coerces_to_int_strings_disallowed():
    """Defense against SQL injection — the helper casts to ``int(...)``,
    so any string that doesn't parse as int raises rather than being
    interpolated. The test is the security guarantee: a malicious
    caller can't pass ``'1) OR 1=1--'``."""
    with pytest.raises((ValueError, TypeError)):
        _broker_account_sql_and(["1) OR 1=1--"])


def test_broker_account_sql_filter_admin_returns_empty():
    assert _broker_account_sql_filter(None) == ""


def test_broker_account_sql_filter_authenticated_no_accounts_locks_closed():
    assert _broker_account_sql_filter([]) == "WHERE 1 = 0"


def test_broker_account_sql_filter_renders_where_prefix():
    assert _broker_account_sql_filter([99]) == "WHERE broker_account_id IN (99)"


# ---------------------------------------------------------------------------
# DataFrame filter
# ---------------------------------------------------------------------------


def _df(rows):
    return pd.DataFrame(rows)


def test_filter_df_admin_bypasses():
    """Admin (``None``) returns the df unchanged."""
    df = _df([
        {"broker_account_id": 42, "value": 1},
        {"broker_account_id": 99, "value": 2},
    ])
    out = _filter_df_by_broker_account_ids(df, None)
    assert len(out) == 2
    assert list(out["value"]) == [1, 2]


def test_filter_df_keeps_matching_rows():
    df = _df([
        {"broker_account_id": 42, "value": 1},
        {"broker_account_id": 99, "value": 2},
        {"broker_account_id": 42, "value": 3},
    ])
    out = _filter_df_by_broker_account_ids(df, [42])
    assert list(out["value"]) == [1, 3]


def test_filter_df_drops_null_broker_account_id():
    """NULL broker_account_id rows are NEVER returned to a signed-in
    user — that's the orphan-tenant security guarantee. See the
    Stage 1 broker-sync-safety SKILL entry."""
    df = _df([
        {"broker_account_id": 42, "value": 1},
        {"broker_account_id": None, "value": 2},
        {"broker_account_id": float("nan"), "value": 3},
    ])
    out = _filter_df_by_broker_account_ids(df, [42, 99])
    assert list(out["value"]) == [1], (
        "NULL and NaN broker_account_id rows must be dropped for "
        "signed-in users; admin path keeps them via the None bypass"
    )


def test_filter_df_authenticated_no_accounts_returns_empty_same_shape():
    """An authenticated user with no broker_accounts gets an empty
    frame with the same columns — caller code that does
    ``df[df.foo > 0]`` doesn't crash on the empty case."""
    df = _df([
        {"broker_account_id": 42, "value": 1},
        {"broker_account_id": 99, "value": 2},
    ])
    out = _filter_df_by_broker_account_ids(df, [])
    assert len(out) == 0
    assert list(out.columns) == list(df.columns)


def test_filter_df_handles_string_broker_account_ids():
    """BigQuery STRING / Postgres BIGINT round-trip can produce a mix of
    int and str. Filter must coerce both sides."""
    df = _df([
        {"broker_account_id": "42", "value": 1},
        {"broker_account_id": 42, "value": 2},
        {"broker_account_id": 99, "value": 3},
    ])
    out = _filter_df_by_broker_account_ids(df, [42])
    assert sorted(out["value"]) == [1, 2]


def test_filter_df_returns_unchanged_when_column_missing():
    """Stage 2 deploy gap: a mart that hasn't yet propagated
    ``broker_account_id``. The helper must NOT fail-closed — the
    legacy `(account, user_id)` filter still carries the security
    boundary for that surface."""
    df = _df([
        {"account": "Schwab ••••0044", "user_id": 7, "value": 1},
        {"account": "Schwab ••••0044", "user_id": 7, "value": 2},
    ])
    out = _filter_df_by_broker_account_ids(df, [42])
    assert len(out) == 2, (
        "missing column must NOT empty the frame — that would silently "
        "wipe out user data the moment a Stage 2 mart deploy hasn't "
        "caught up yet; the existing (account, user_id) filter still "
        "guards this surface"
    )


def test_filter_df_empty_input_returns_empty():
    df = _df([])
    out = _filter_df_by_broker_account_ids(df, [42])
    assert out.empty


def test_filter_df_none_input_returns_none():
    assert _filter_df_by_broker_account_ids(None, [42]) is None
