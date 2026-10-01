"""v2 tenant_id isolation helpers — single security boundary for warehouse reads.

See ``docs/V2_TENANT_KEY_DESIGN.md``. Every user-facing BigQuery DataFrame
must pass through ``filter_df_by_tenant_ids`` (or the SQL siblings) before
merge/render. Missing ``tenant_id`` column fails CLOSED for non-admin
callers — deploy-gap passthrough was retired after a prior cross-tenant
leak on this surface.
"""
from __future__ import annotations

import logging
import re

import pandas as pd

_log = logging.getLogger(__name__)

_TENANT_ID_VALID_CHAR_RE = re.compile(r"^[A-Za-z0-9_:.-]+$")


def sanitize_tenant_id(tenant_id):
    """Defensive escape: only allow ``A-Z a-z 0-9 _ : . -`` characters.

    tenant_ids are well-formed by construction (broker_slug + ':' +
    broker UUID), so this is belt-and-suspenders. Anything else is
    dropped on the floor.
    """
    if not tenant_id:
        return None
    t = str(tenant_id).strip()
    if not t or not _TENANT_ID_VALID_CHAR_RE.match(t):
        return None
    return t


def resolve_filter_tenant_ids(requested=None):
    """Return the list of ``tenant_id`` strings the current request is
    allowed to read, or ``None`` for admin bypass (no filter).

    Signed-in users return the intersection of
    ``get_tenant_ids_for_user(current_user.id)`` and the optional
    ``requested`` list (typically from ``?tenant=``). Tenant ids the
    user doesn't own are dropped — URL params never widen tenancy.

    Empty list → fail-closed at the SQL boundary (``AND 1 = 0``).
    """
    try:
        from flask_login import current_user
        from app.models import get_tenant_ids_for_user, is_admin

        if not getattr(current_user, "is_authenticated", False):
            return []
        from app.demo_guard import (
            DEMO_TENANT_ID,
            is_ephemeral_demo_id,
            is_ephemeral_demo_user,
            numeric_user_id,
        )
        ephemeral = (
            is_ephemeral_demo_user(current_user)
            or is_ephemeral_demo_id(getattr(current_user, "id", None))
        )
        if ephemeral:
            owned = {DEMO_TENANT_ID}
        else:
            if is_admin(getattr(current_user, "username", None)):
                return None
            uid = numeric_user_id(getattr(current_user, "id", None))
            if uid is None:
                return []
            owned = set(get_tenant_ids_for_user(uid) or [])
        if requested is None:
            return sorted(owned)
        requested_set = {str(t).strip() for t in requested if t}
        return sorted(owned & requested_set)
    except Exception:
        _log.exception("resolve_filter_tenant_ids failed; failing closed")
        return []


def _known_paper_ids():
    """Paper tenant ids to keep out of an unscoped real-book read.

    Empty when none are known, including a database miss. Callers then
    keep the historical admin bypass instead of failing the page.
    """
    try:
        from app.paper_accounts import all_paper_tenant_ids

        raw = all_paper_tenant_ids() or []
    except Exception:
        _log.exception("paper tenant exclusion lookup failed")
        return []
    safe = []
    for tenant_id in raw:
        cleaned = sanitize_tenant_id(tenant_id)
        if cleaned and cleaned not in safe:
            safe.append(cleaned)
    return safe


def _paper_exclusion_sql(col, prefix):
    """``AND``/``WHERE`` clause dropping known paper tenants. Empty when none."""
    paper = _known_paper_ids()
    if not paper:
        return ""
    safe_col = re.sub(r"[^A-Za-z0-9_.]", "", str(col))
    quoted = ", ".join(f"'{t}'" for t in paper)
    return f"{prefix} {safe_col} NOT IN ({quoted})"


def tenant_sql_and(tenant_ids, col="tenant_id"):
    """``AND``-shaped predicate scoping a BigQuery read to ``tenant_id`` values.

    Admin (``tenant_ids is None``) stays unscoped except for known Alpaca
    Paper tenants, which are excluded from real totals. When that list
    cannot be loaded the predicate is ``""`` (historical admin bypass).
    Empty list returns ``AND 1 = 0`` (fail-closed).
    """
    if tenant_ids is None:
        return _paper_exclusion_sql(col, "AND")
    if not tenant_ids:
        return "AND 1 = 0"
    safe = [sanitize_tenant_id(t) for t in tenant_ids]
    safe = [t for t in safe if t]
    if not safe:
        return "AND 1 = 0"
    safe_col = re.sub(r"[^A-Za-z0-9_.]", "", str(col))
    quoted = ", ".join(f"'{t}'" for t in safe)
    return f"AND {safe_col} IN ({quoted})"


def tenant_sql_filter(tenant_ids, col="tenant_id"):
    """``WHERE``-prefixed sibling for queries without an existing ``WHERE``.

    Admin (``tenant_ids is None``) excludes known paper tenants, same as
    ``tenant_sql_and``. A lookup miss returns ``""``.
    """
    if tenant_ids is None:
        return _paper_exclusion_sql(col, "WHERE")
    if not tenant_ids:
        return "WHERE 1 = 0"
    safe = [sanitize_tenant_id(t) for t in tenant_ids]
    safe = [t for t in safe if t]
    if not safe:
        return "WHERE 1 = 0"
    safe_col = re.sub(r"[^A-Za-z0-9_.]", "", str(col))
    quoted = ", ".join(f"'{t}'" for t in safe)
    return f"WHERE {safe_col} IN ({quoted})"


def _drop_known_paper_rows(df, col):
    """Drop known paper rows from an unscoped frame. Misses leave it alone."""
    if col not in df.columns:
        return df
    paper = set(_known_paper_ids())
    if not paper:
        return df
    series = df[col].astype(str)
    return df.loc[~series.isin(paper)].reset_index(drop=True)


def filter_df_by_tenant_ids(df, tenant_ids, col="tenant_id"):
    """DataFrame-side belt-and-suspenders filter.

    Admin (``tenant_ids is None``) keeps every row except known Alpaca
    Paper tenants. A lookup miss, or a frame with no ``tenant_id``
    column, returns the frame unchanged.
    Empty list returns an empty same-shape frame.
    Rows with NULL/missing ``tenant_id`` are DROPPED for non-admin
    callers — under v2 every legitimate row carries a tenant_id.

    If ``col`` is missing on the frame, fail CLOSED (empty same-shape
    frame) for non-admin callers and log loudly. A mart without
    ``tenant_id`` must not silently render unscoped rows.
    """
    if df is None or df.empty:
        return df
    if tenant_ids is None:
        return _drop_known_paper_rows(df, col)
    if col not in df.columns:
        _log.error(
            "filter_df_by_tenant_ids: column %r missing — failing closed "
            "(empty frame). Columns=%s",
            col,
            list(df.columns),
        )
        return df.iloc[0:0]
    if not tenant_ids:
        return df.iloc[0:0]
    safe = {sanitize_tenant_id(t) for t in tenant_ids}
    safe.discard(None)
    if not safe:
        return df.iloc[0:0]
    series = df[col].astype(str)
    keep = series.isin(safe)
    return df.loc[keep].reset_index(drop=True)
