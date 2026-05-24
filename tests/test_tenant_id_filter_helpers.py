"""Tests for the v2 tenant_id filter helpers in ``app/routes.py``.

See ``docs/V2_TENANT_KEY_DESIGN.md``.

Goals (mirrors the v1 broker_account_id test suite):
- SQL helpers (``_tenant_sql_and``, ``_tenant_sql_filter``) emit
  well-formed predicates, fail-closed on empty list, bypass cleanly
  on admin (``tenant_ids is None``).
- DataFrame helper (``_filter_df_by_tenant_ids``) drops rows whose
  ``tenant_id`` isn't in the user's allowlist, drops NULL rows
  (the structural orphan-tenancy guarantee), tolerates missing
  column gracefully for the deploy-gap case.
- Defensive sanitization rejects malformed tenant_ids that could
  inject SQL.
"""
from __future__ import annotations

import pandas as pd

from app.routes import (
    _filter_df_by_tenant_ids,
    _sanitize_tenant_id,
    _tenant_sql_and,
    _tenant_sql_filter,
)


# ---------------------------------------------------------------------------
# Sanitization
# ---------------------------------------------------------------------------


def test_sanitize_tenant_id_accepts_well_formed_snaptrade_uuid():
    """The canonical tenant_id shape passes through unchanged."""
    t = "snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661"
    assert _sanitize_tenant_id(t) == t


def test_sanitize_tenant_id_accepts_schwab_hex_style():
    """Schwab account_hash form (64-char hex) also passes."""
    t = "snaptrade:7456275292A8A909CE7CD7423D9ABC910A82D72B284D65138BD2D9B59397CDE7"
    assert _sanitize_tenant_id(t) == t


def test_sanitize_tenant_id_strips_whitespace():
    """Leading/trailing whitespace is tolerated."""
    t = "  snaptrade:abc-123  "
    assert _sanitize_tenant_id(t) == "snaptrade:abc-123"


def test_sanitize_tenant_id_rejects_sql_injection():
    """A tenant_id containing space/quote/semicolon is rejected."""
    assert _sanitize_tenant_id("snaptrade:abc'; DROP TABLE--") is None
    assert _sanitize_tenant_id("snaptrade:abc OR 1=1") is None
    assert _sanitize_tenant_id("snaptrade:abc;") is None


def test_sanitize_tenant_id_rejects_empty_and_none():
    """Empty / None / whitespace-only return None."""
    assert _sanitize_tenant_id(None) is None
    assert _sanitize_tenant_id("") is None
    assert _sanitize_tenant_id("   ") is None


# ---------------------------------------------------------------------------
# _tenant_sql_and (AND-shaped predicate)
# ---------------------------------------------------------------------------


def test_tenant_sql_and_admin_returns_empty():
    """Admin (``tenant_ids is None``) must NOT narrow the query."""
    assert _tenant_sql_and(None) == ""


def test_tenant_sql_and_empty_list_is_lock_closed():
    """Authenticated user with NO tenants must see no rows."""
    assert _tenant_sql_and([]) == "AND 1 = 0"


def test_tenant_sql_and_single_tenant():
    """One tenant_id renders as a clean IN clause with quotes."""
    t = "snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661"
    assert _tenant_sql_and([t]) == f"AND tenant_id IN ('{t}')"


def test_tenant_sql_and_multiple_tenants():
    """Multiple tenant_ids preserve input order."""
    a = "snaptrade:aaa"
    b = "snaptrade:bbb"
    c = "snaptrade:ccc"
    assert (
        _tenant_sql_and([a, b, c])
        == f"AND tenant_id IN ('{a}', '{b}', '{c}')"
    )


def test_tenant_sql_and_with_custom_col():
    """Allow `dim.tenant_id` form for joins."""
    t = "snaptrade:abc"
    assert (
        _tenant_sql_and([t], col="d.tenant_id")
        == f"AND d.tenant_id IN ('{t}')"
    )


def test_tenant_sql_and_drops_malformed_inputs_silently():
    """Defense against injection: malformed entries are dropped, the
    remainder is rendered. If EVERY entry is malformed → fail-closed."""
    good = "snaptrade:abc-123"
    bad = "snaptrade:abc'; DROP TABLE--"
    assert _tenant_sql_and([good, bad]) == f"AND tenant_id IN ('{good}')"


def test_tenant_sql_and_all_malformed_locks_closed():
    """Every entry malformed → ``AND 1 = 0`` (don't render an empty
    IN clause which would be invalid SQL)."""
    assert _tenant_sql_and(["bad'; DROP", "other OR 1=1"]) == "AND 1 = 0"


def test_tenant_sql_and_sanitizes_col_name():
    """Column name is also sanitized against injection — the regex strips
    every character that could break out of the identifier (space, ``;``,
    ``--``, quotes). Alphanumeric content like ``DROP`` survives but as
    part of a single identifier (e.g. ``tenant_idDROPTABLEusers``) which
    BigQuery treats as an unknown column and rejects, NOT as executable
    DDL. The security guarantee is "no statement-level escape", not
    "no scary words"."""
    t = "snaptrade:abc"
    result = _tenant_sql_and([t], col="tenant_id; DROP TABLE users--")
    # space and ; and -- are the actual statement-escape vectors.
    assert ";" not in result
    assert " DROP" not in result  # no space before DROP = no SQL keyword
    assert "--" not in result
    assert "'" + t + "'" in result


# ---------------------------------------------------------------------------
# _tenant_sql_filter (WHERE-prefixed sibling)
# ---------------------------------------------------------------------------


def test_tenant_sql_filter_admin_returns_empty():
    assert _tenant_sql_filter(None) == ""


def test_tenant_sql_filter_empty_list_locks_closed():
    assert _tenant_sql_filter([]) == "WHERE 1 = 0"


def test_tenant_sql_filter_single_tenant():
    t = "snaptrade:abc"
    assert _tenant_sql_filter([t]) == f"WHERE tenant_id IN ('{t}')"


# ---------------------------------------------------------------------------
# _filter_df_by_tenant_ids (DataFrame-side belt-and-suspenders)
# ---------------------------------------------------------------------------


def test_filter_df_admin_returns_unchanged():
    """Admin (``tenant_ids is None``) returns the df unchanged."""
    df = pd.DataFrame({"tenant_id": ["snaptrade:a", "snaptrade:b"]})
    out = _filter_df_by_tenant_ids(df, None)
    assert len(out) == 2


def test_filter_df_empty_tenant_list_returns_empty_frame():
    """Authenticated user with no tenants → empty frame, same shape."""
    df = pd.DataFrame({"tenant_id": ["snaptrade:a"], "x": [1]})
    out = _filter_df_by_tenant_ids(df, [])
    assert out.empty
    assert list(out.columns) == ["tenant_id", "x"]


def test_filter_df_drops_rows_not_in_allowlist():
    """Rows whose tenant_id isn't in the allowlist are dropped."""
    df = pd.DataFrame({
        "tenant_id": ["snaptrade:a", "snaptrade:b", "snaptrade:c"],
        "x": [1, 2, 3],
    })
    out = _filter_df_by_tenant_ids(df, ["snaptrade:a", "snaptrade:c"])
    assert list(out["x"]) == [1, 3]


def test_filter_df_drops_null_tenant_id():
    """Rows with NULL tenant_id are DROPPED for non-admin callers —
    under v2 every legitimate row carries a tenant_id, NULL means
    pre-cutover legacy data or an ingestion bug, neither of which
    should leak to a signed-in user."""
    df = pd.DataFrame({
        "tenant_id": ["snaptrade:a", None, "snaptrade:b"],
        "x": [1, 2, 3],
    })
    out = _filter_df_by_tenant_ids(df, ["snaptrade:a", "snaptrade:b"])
    assert list(out["x"]) == [1, 3]


def test_filter_df_missing_column_returns_unchanged():
    """Deploy-gap: a mart that hasn't propagated tenant_id yet returns
    the frame unchanged (route-level legacy filter is the active
    security boundary until the mart is migrated)."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    out = _filter_df_by_tenant_ids(df, ["snaptrade:a"])
    assert len(out) == 3


def test_filter_df_empty_df_returns_empty():
    """Empty df is a no-op for any tenant_ids argument."""
    df = pd.DataFrame({"tenant_id": [], "x": []})
    out = _filter_df_by_tenant_ids(df, ["snaptrade:a"])
    assert out.empty


def test_filter_df_none_df_returns_none():
    """None df returns None (don't synthesize an empty frame)."""
    assert _filter_df_by_tenant_ids(None, ["snaptrade:a"]) is None


def test_filter_df_all_malformed_returns_empty():
    """If every tenant_id in the filter is malformed, treat as 'no
    allowed tenants' → empty frame."""
    df = pd.DataFrame({"tenant_id": ["snaptrade:a"], "x": [1]})
    out = _filter_df_by_tenant_ids(df, ["bad'; DROP"])
    assert out.empty
