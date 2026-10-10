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


def _unscoped_admin_mode():
    """How an admin/unscoped read is narrowed.

    ``("exclude", ids)`` drops known Alpaca Paper tenants and tenants on
    the private-from-admin list. An empty id list means both lookups
    succeeded and there is nothing to drop (historical admin bypass).

    ``("allow", ids)`` is the private-list outage. The ids are the
    signed-in admin's own tenants plus the demo tenant. A paper-list
    miss still fails open on its own; a private-list miss never does.
    Showing every tenant when the hidden-user list cannot be loaded
    would publish the books this feature exists to hide.
    """
    try:
        from app.admin_privacy import (
            fail_closed_allow_tenant_ids,
            hidden_snapshot,
            note_privacy_list_unavailable,
        )

        snap = hidden_snapshot()
    except Exception:
        _log.exception("private-from-admin tenant exclusion lookup failed")
        snap = {"available": False}
    if not snap.get("available"):
        try:
            from app.admin_privacy import note_privacy_list_unavailable
            note_privacy_list_unavailable()
        except Exception:
            _log.exception("private-from-admin outage notice failed")
        try:
            from app.admin_privacy import fail_closed_allow_tenant_ids
            allow = list(fail_closed_allow_tenant_ids())
        except Exception:
            _log.exception("private-from-admin fail-closed allow-list failed")
            allow = []
        return "allow", allow
    ids = list(_known_paper_ids())
    for tenant_id in snap.get("tenant_ids") or ():
        cleaned = sanitize_tenant_id(tenant_id)
        if cleaned and cleaned not in ids:
            ids.append(cleaned)
    return "exclude", ids


def _paper_exclusion_sql(col, prefix):
    """``AND``/``WHERE`` clause for an admin/unscoped read.

    Loaded list: ``NOT IN`` paper and private tenants, or ``""`` when
    both sets are empty. Outage: ``IN`` the admin's own tenants and the
    demo tenant, or ``1 = 0`` when that allow-list is empty. Never an
    empty predicate on a private-list failure.
    """
    mode, ids = _unscoped_admin_mode()
    safe_col = re.sub(r"[^A-Za-z0-9_.]", "", str(col))
    if mode == "allow":
        if not ids:
            return f"{prefix} 1 = 0"
        quoted = ", ".join(f"'{t}'" for t in ids)
        return f"{prefix} {safe_col} IN ({quoted})"
    if not ids:
        return ""
    quoted = ", ".join(f"'{t}'" for t in ids)
    return f"{prefix} {safe_col} NOT IN ({quoted})"


def tenant_sql_and(tenant_ids, col="tenant_id"):
    """``AND``-shaped predicate scoping a BigQuery read to ``tenant_id`` values.

    Admin (``tenant_ids is None``) stays unscoped except for known Alpaca
    Paper tenants and tenants on the private-from-admin list. A paper-list
    miss leaves that exclusion off. A private-list miss fail-closes to
    the signed-in admin's own tenants plus the demo tenant (or
    ``AND 1 = 0``). An explicit empty list returns ``AND 1 = 0``.
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

    Admin (``tenant_ids is None``) excludes known paper tenants and
    private-from-admin tenants, same as ``tenant_sql_and``. A private-list
    miss fail-closes to the admin's own tenants plus the demo tenant.
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
    """Drop paper and private-from-admin rows from an unscoped frame.

    A paper-list miss leaves paper rows in place. A private-list miss
    keeps only the admin's own tenants and the demo tenant. A frame
    with no tenant column stays unchanged when the list loaded, and is
    emptied when the list did not (those rows cannot be classified).
    """
    mode, ids = _unscoped_admin_mode()
    if mode == "allow":
        if col not in df.columns or not ids:
            return df.iloc[0:0]
        series = df[col].astype(str)
        return df.loc[series.isin(set(ids))].reset_index(drop=True)
    if col not in df.columns:
        return df
    excluded = set(ids)
    if not excluded:
        return df
    series = df[col].astype(str)
    return df.loc[~series.isin(excluded)].reset_index(drop=True)


def filter_df_by_tenant_ids(df, tenant_ids, col="tenant_id"):
    """DataFrame-side belt-and-suspenders filter.

    Admin (``tenant_ids is None``) keeps every row except known Alpaca
    Paper tenants and private-from-admin tenants. A private-list miss
    keeps only the admin's own tenants and the demo tenant. A frame
    with no ``tenant_id`` column returns unchanged when that list
    loaded, and empty when it did not. An explicit empty list returns
    an empty same-shape frame.
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
