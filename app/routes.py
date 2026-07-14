from flask import render_template, request, redirect, url_for, Response, flash, abort
from werkzeug.exceptions import RequestEntityTooLarge
from flask_login import login_required, current_user
from app import app
from app.extensions import limiter
from app.bigquery_client import get_bigquery_client
from app.query_cache import cached_query_df, cached_payload, frame_fingerprint, timed
from app.utils import earnings_follower_url
from app.llm import llm_available as _llm_available
from app.models import (
    get_broker_tenants_for_user,
    get_strategy_fit_insight_for_user,
    get_tenant_ids_for_user,
    is_admin,
)
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor
import math
import os
import re
import pandas as pd
import json
from urllib.parse import quote_plus


def _bq_parallel(client, queries):
    """Run multiple BigQuery queries in parallel and return results dict.

    queries: dict of {name: sql_string} or {name: (sql_string, job_config)}
    Returns: dict of {name: DataFrame}

    Resilience contract: a failure in ONE query must not blank the entire
    page. Pre-fix, a SQL typo in the Daily Review "attribution" query
    (`stg_history.symbol` instead of `underlying_symbol`) crashed the
    whole batch — the caller's outer `except` swallowed it, set
    ``batch = {}``, and EVERY downstream section (snapshots, positions,
    movers, breakdowns) rendered em-dashes. Per-key isolation here means
    one bad query produces one empty DataFrame, logged loudly, and the
    other eight sections still render real data.
    """
    from app.query_cache import propagate_context

    results = {}

    def _run(name, spec):
        try:
            if isinstance(spec, tuple):
                sql, cfg = spec
                return name, cached_query_df(client, sql, job_config=cfg, label=name), None
            return name, cached_query_df(client, spec, label=name), None
        except Exception as exc:
            return name, pd.DataFrame(), exc

    with ThreadPoolExecutor(max_workers=min(len(queries), 8)) as pool:
        # Copy the request context per task so the query-cache stats
        # ContextVar reaches the worker thread (per-query timing).
        futures = [
            pool.submit(propagate_context().run, _run, n, s)
            for n, s in queries.items()
        ]
        for f in futures:
            name, df, exc = f.result()
            results[name] = df
            if exc is not None:
                try:
                    from flask import current_app
                    current_app.logger.error(
                        "_bq_parallel: query %r failed: %s", name, exc,
                    )
                except Exception:
                    pass

    return results


def _redirect_if_no_accounts():
    """Bounce a freshly signed-up user to /get-started instead of letting
    them land on a data-driven page where every BigQuery query gets
    AND 1=0'd and the UI shows "we're calculating…" forever.

    Returns a Flask redirect response when the current user has zero
    linked broker tenants (and isn't an admin), or None when the caller
    should continue rendering normally.
    """
    if current_user.is_authenticated and not is_admin(current_user.username):
        if len(get_tenant_ids_for_user(current_user.id)) == 0:
            # Skip if they're already on /get-started or coming back from
            # an upload; the upload-processing screen redirects through
            # weekly-review with from_upload=1 during the 3–5 min lag.
            if request.endpoint == "get_started":
                return None
            if request.args.get("from_upload") == "1" or request.args.get("from_sync") == "1":
                return None
            return redirect(url_for("get_started"))
    return None


def _norm_account_label(val) -> str:
    """Normalize free-form account labels for URL / display matching."""
    return " ".join(str(val or "").strip().split())


def _tenant_display_label(row) -> str:
    """Base display label for a broker_tenants row (nickname wins over account_name).

    This is the *base* label only. When a user holds several physical
    accounts that share the same base label (e.g. multiple Schwab
    accounts all labeled "Schwab Account" because SnapTrade returned no
    distinct mask), use ``_disambiguated_tenant_labels`` to get
    per-tenant unique labels for the picker / scoping.
    """
    return (row.get("display_nickname") or row.get("account_name") or "").strip()


def _tenant_label_suffix(row) -> str:
    """Stable, human-ish suffix to tell two same-base-label tenants apart.

    Prefers the broker ``account_mask`` (shows the last 4, like
    "••6342"); falls back to a short tail of the broker-stable
    ``broker_uuid`` / ``tenant_id`` so the suffix never changes across
    Postgres resets or re-syncs.
    """
    mask = (row.get("account_mask") or "").strip()
    if mask:
        tail = mask[-4:] if len(mask) >= 4 else mask
        return f"\u2022\u2022{tail}"
    uuid = (row.get("broker_uuid") or "").strip()
    if uuid:
        return "\u00b7" + uuid.replace("-", "")[-6:]
    tid = (row.get("tenant_id") or "").strip()
    return ("\u00b7" + tid[-6:]) if tid else ""


def _disambiguated_tenant_labels(rows) -> dict:
    """Map ``tenant_id -> unique display label`` for a user's tenants.

    When two physical accounts share a base label, append a stable
    per-tenant suffix (from ``account_mask`` / ``broker_uuid``) so the
    account picker can address each one individually. Non-colliding
    labels pass through unchanged. This is the display/URL-layer fix for
    the SnapTrade "all 5 Schwab accounts labeled 'Schwab Account'"
    collision — the warehouse already keys on ``tenant_id``.
    """
    from collections import Counter

    base_counts = Counter()
    for row in rows or []:
        base = _tenant_display_label(row)
        if base:
            base_counts[base] += 1

    out = {}
    for row in rows or []:
        tid = row.get("tenant_id")
        base = _tenant_display_label(row)
        if not tid or not base:
            continue
        if base_counts[base] > 1:
            suffix = _tenant_label_suffix(row)
            out[tid] = f"{base} ({suffix})" if suffix else base
        else:
            out[tid] = base
    return out


def _tenant_label_map_for_user(user_id) -> dict:
    """``tenant_id -> disambiguated display label`` for one user, or ``{}``.

    Convenience wrapper used by per-tenant groupby surfaces so the mart's
    ``tenant_id`` can be rendered with a unique, human-readable label.
    """
    if user_id is None:
        return {}
    try:
        rows = get_broker_tenants_for_user(user_id) or []
    except Exception:
        return {}
    return _disambiguated_tenant_labels(rows)


def _account_label_map(user_id) -> dict:
    """Return ``{broker_account_name: user_display_label}`` for one user.

    Mart columns (``mart_account_snapshots_enriched.account``,
    ``positions_summary.account``, etc.) carry the broker-derived
    label (e.g. "Alpaca Paper Account" or "Schwab ••••6342") because
    that's what the seed writes. Users set nicknames via
    ``/snaptrade/accounts``; those land on
    ``broker_tenants.display_nickname``. This map is the bridge —
    every UI surface that renders the mart's ``account`` value must
    pass it through this lookup so the nickname (when set) shadows
    the broker label.

    The map is identity-valued for any tenant without a nickname so
    callers can do ``df["account"].map(lambda x: m.get(x, x))``
    without losing rows. Admin / unauthenticated returns ``{}`` —
    no translation, just pass the raw broker label through.

    See ``docs/V2_TENANT_KEY_DESIGN.md`` for the broader v2 contract.
    """
    if user_id is None:
        return {}
    try:
        from app.models import get_broker_tenants_for_user
    except Exception:
        return {}
    # COLLISION GUARD (mirrors get_snaptrade_account_nicknames): when the
    # user owns several tenants sharing one account_name but carrying
    # different nicknames, a {name: nick} map would relabel ALL of them
    # with one arbitrary nickname. Ambiguous names are dropped — the raw
    # broker label passes through. Per-tenant surfaces should use
    # _tenant_label_map_for_user (tenant_id-keyed) instead.
    out = {}
    ambiguous = set()
    for row in get_broker_tenants_for_user(user_id) or []:
        name = (row.get("account_name") or "").strip()
        nick = (row.get("display_nickname") or "").strip()
        if not (name and nick and nick != name):
            continue
        if name in out and out[name] != nick:
            ambiguous.add(name)
        else:
            out[name] = nick
    for name in ambiguous:
        out.pop(name, None)
    return out


def _apply_account_labels(target, user_id, col: str = "account"):
    """Translate the broker ``account`` label → user nickname in-place.

    Accepts either a pandas DataFrame (translates the ``col`` column)
    or a list of dicts (translates ``d[col]`` per item) or a single
    string (returns the translated string). Returns ``target``
    (mutated when possible) so callers can write
    ``df = _apply_account_labels(df, user_id)``.

    No-op when no nickname is set or the column is missing — the
    mart's broker label flows through unchanged, matching what every
    pre-nickname surface used to render.
    """
    label_map = _account_label_map(user_id)
    if not label_map:
        return target
    if target is None:
        return target
    if isinstance(target, str):
        return label_map.get(target, target)
    if isinstance(target, list):
        for item in target:
            if isinstance(item, dict) and col in item:
                item[col] = label_map.get(item[col], item[col])
        return target
    try:
        if hasattr(target, "columns") and col in target.columns and not target.empty:
            target[col] = target[col].map(lambda x: label_map.get(x, x))
    except Exception:
        pass
    return target


def _user_tenant_list():
    """Return tenant_ids the current user may read, or None for admin bypass."""
    if is_admin(current_user.username):
        return None
    return get_tenant_ids_for_user(current_user.id) or []


def _tenants_for_scope(selected_account=None):
    """Resolve tenant_ids for the current request scope.

    Resolution order:
      1. ``?tenant=<tenant_id>`` — direct, broker-stable addressing. A
         single physical account, even when its display label collides
         with siblings. Validated against the user's owned tenants
         (never let a URL widen tenancy); admin may address any tenant.
      2. ``?account=<label>`` (legacy alias) — matches a base label OR a
         disambiguated label (e.g. "Schwab Account (\u2022\u20226342)").
         A bare colliding base label still selects all matching tenants
         for backward compatibility.
      3. No selection → admin: ``None`` (no SQL filter); user: all owned.

    Unknown selections fall back to all of the user's tenants (same safe
    default as the v2 design doc).
    """
    from app.db import fetch_all

    admin = is_admin(current_user.username)
    selected = (selected_account or "").strip()

    # 1. Direct tenant addressing (?tenant=) wins over label matching.
    try:
        requested_tenant = (request.args.get("tenant") or "").strip()
    except Exception:
        requested_tenant = ""
    if requested_tenant:
        if admin:
            return [requested_tenant]
        owned = [
            row["tenant_id"]
            for row in (get_broker_tenants_for_user(current_user.id) or [])
        ]
        if requested_tenant in owned:
            return [requested_tenant]
        # Not owned → ignore the param and fall through to safe defaults.

    if admin and not selected:
        return None

    def _match_label(row, want_lower: str) -> bool:
        for label in (row.get("display_nickname"), row.get("account_name")):
            if label and _norm_account_label(label).lower() == want_lower:
                return True
        return False

    if admin:
        want = _norm_account_label(selected).lower()
        rows = fetch_all(
            "SELECT tenant_id, account_name, account_mask, broker_uuid, "
            "display_nickname FROM broker_tenants"
        )
        # Admin matches raw labels across all users (existing behavior);
        # to target one colliding account admin should use ?tenant=.
        matched = [
            row["tenant_id"]
            for row in rows
            if _match_label(row, want)
        ]
        return sorted(set(matched))

    tenants = get_broker_tenants_for_user(current_user.id) or []
    all_ids = [row["tenant_id"] for row in tenants]
    if not selected:
        return all_ids

    want = _norm_account_label(selected).lower()
    label_map = _disambiguated_tenant_labels(tenants)
    matched = []
    for row in tenants:
        tid = row.get("tenant_id")
        if _match_label(row, want):
            matched.append(tid)
            continue
        dis = label_map.get(tid)
        if dis and _norm_account_label(dis).lower() == want:
            matched.append(tid)
    return matched if matched else all_ids


def _user_account_list():
    """Return display account names for the account picker, or None for admin.

    Names come from ``broker_tenants`` (SnapTrade sync), not legacy
    ``schwab_connections``. Warehouse isolation uses ``tenant_id`` via
    ``_tenants_for_scope`` / ``_tenant_sql_and``; this list is UI-only.
    """
    if is_admin(current_user.username):
        return None
    rows = get_broker_tenants_for_user(current_user.id) or []
    # Disambiguate colliding base labels (e.g. several "Schwab Account"s)
    # so each physical account is independently selectable in the picker.
    label_map = _disambiguated_tenant_labels(rows)
    names = sorted(set(label_map.values()))
    return names


def _resolve_filter_user_id():
    """Return the ``user_id`` to scope BigQuery reads by for the current
    request, or ``None`` for admin / unauthenticated paths (no scoping).

    The legacy ``_account_sql_*`` and ``_filter_df_by_accounts`` helpers
    use this to automatically add a ``user_id`` predicate to every read
    they shape, so two users sharing an ``account_name`` cannot see each
    other's rows. See ``docs/USER_ID_TENANCY.md`` for the full story.
    """
    try:
        from flask_login import current_user
        if not getattr(current_user, "is_authenticated", False):
            return None
        if is_admin(current_user.username):
            return None
        return int(current_user.id)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Stage 3 — broker_account_id filter helpers (see docs/BROKER_ACCOUNT_ID_MIGRATION.md)
#
# These helpers add a SECOND tenant predicate alongside the legacy
# `(account, user_id)` filter — defense in depth. The legacy filter
# stays in place through Stage 3; both predicates must agree.
#
# Stage 4 will (a) drop the legacy filter and the `OR user_id IS NULL`
# leniency, (b) make these the primary tenancy boundary.
#
# Until Stage 2 propagates `broker_account_id` through every mart, only
# specific routes opt into defense-in-depth via these helpers. The
# orphan rows in the seed (~7,200 with NULL broker_account_id; see
# the Stage 1 broker-sync-safety SKILL entry) become invisible to any
# query that uses this filter — that's the security upgrade.
# ---------------------------------------------------------------------------


def _resolve_filter_broker_account_ids():
    """Return the list of ``broker_accounts.id`` the current request is
    allowed to read. Admin returns ``None`` (no filter).

    Mirrors ``_resolve_filter_user_id``'s admin bypass semantics so the
    two filters can be composed without one accidentally narrowing
    admin reads.
    """
    try:
        from flask_login import current_user
        from app.auth import is_admin
        from app.models import get_broker_account_ids_for_user
        if not getattr(current_user, "is_authenticated", False):
            return []
        if is_admin(getattr(current_user, "username", None)):
            return None
        return get_broker_account_ids_for_user(int(current_user.id))
    except Exception:
        return []


def _broker_account_sql_and(broker_account_ids, col="broker_account_id"):
    """``AND``-shaped predicate that scopes a BQ read to a set of
    ``broker_account_id`` values. Returns ``""`` for admin
    (``broker_account_ids is None``) so the helper composes safely.

    Empty list intentionally produces ``AND 1 = 0`` — an authenticated
    user with no broker_accounts should see no rows, not all rows.

    Usage::

        sql = QUERY.format(
            tenant_filter=_tenant_sql_and(tenant_ids),
            broker_tenant_filter=_broker_tenant_sql_and(
                _resolve_filter_broker_account_ids()
            ),
        )
    """
    if broker_account_ids is None:
        return ""
    if not broker_account_ids:
        return "AND 1 = 0"
    ids = ", ".join(str(int(i)) for i in broker_account_ids)
    return f"AND {col} IN ({ids})"


def _broker_account_sql_filter(broker_account_ids, col="broker_account_id"):
    """``WHERE``-prefixed sibling for queries that don't already have a
    ``WHERE`` clause. Returns ``""`` for admin.
    """
    if broker_account_ids is None:
        return ""
    if not broker_account_ids:
        return "WHERE 1 = 0"
    ids = ", ".join(str(int(i)) for i in broker_account_ids)
    return f"WHERE {col} IN ({ids})"


def _filter_df_by_broker_account_ids(df, broker_account_ids, col="broker_account_id"):
    """DataFrame analogue of ``_broker_account_sql_and``.

    Drops rows whose ``col`` is a populated id not in
    ``broker_account_ids``. Rows with ``col`` NULL are dropped UNLESS
    the caller is admin (``broker_account_ids is None``) — orphan rows
    with NULL ``broker_account_id`` are by design invisible to every
    signed-in user (see ``docs/BROKER_ACCOUNT_ID_MIGRATION.md``).

    Admin (``broker_account_ids is None``) bypasses the filter.
    """
    if df is None or df.empty:
        return df
    if broker_account_ids is None:
        return df
    if col not in df.columns:
        # Stage 2 deploy gap: a mart that hasn't propagated the column
        # yet. Don't fail-closed — return the df unchanged and let the
        # legacy `(account, user_id)` filter carry the security boundary
        # for this surface until the mart is migrated.
        return df
    if not broker_account_ids:
        return df.iloc[0:0]  # empty same-shape frame
    target = {int(i) for i in broker_account_ids}
    series = pd.to_numeric(df[col], errors="coerce")
    keep = series.isin(target)
    return df.loc[keep].reset_index(drop=True)


# ---------------------------------------------------------------------------
# v2 — tenant_id filter helpers (see docs/V2_TENANT_KEY_DESIGN.md)
#
# These replace _account_sql_and / _filter_df_by_accounts and the short-lived
# Stage 3A broker_account_id helpers above. Wired into routes in Phase 5;
# additive in Phase 2 (this commit) so the legacy helpers continue to work
# until the per-route migration is complete.
#
# tenant_id format: "<broker_slug>:<broker_uuid>" — broker-stable, never
# minted by us, collision-proof across Postgres / dataset resets. The
# structural property that retires the orphan-tenancy and SERIAL-collision
# bug classes entirely.
# ---------------------------------------------------------------------------


_TENANT_ID_VALID_CHAR_RE = re.compile(r"^[A-Za-z0-9_:.-]+$")


def _resolve_filter_tenant_ids(requested=None):
    """Return the list of ``tenant_id`` strings the current request is
    allowed to read, or ``None`` for admin bypass (no filter).

    Admin / unauthenticated returns ``None`` — same semantics as
    ``_resolve_filter_user_id``. The intent is: composable with other
    filters without accidentally narrowing admin reads.

    Signed-in users return the intersection of
    ``get_tenant_ids_for_user(current_user.id)`` and the optional
    ``requested`` list (which would typically come from a URL
    ``?tenant=`` param). If ``requested`` includes a tenant_id the
    user doesn't own, it's silently dropped — never allow a URL
    parameter to widen tenancy.

    Empty list → fail-closed at the SQL boundary (``AND 1 = 0``).
    """
    try:
        from flask_login import current_user
        from app.auth import is_admin
        from app.models import get_tenant_ids_for_user
        if not getattr(current_user, "is_authenticated", False):
            return []
        if is_admin(getattr(current_user, "username", None)):
            return None
        owned = set(get_tenant_ids_for_user(int(current_user.id)) or [])
        if requested is None:
            return sorted(owned)
        requested_set = {str(t).strip() for t in requested if t}
        return sorted(owned & requested_set)
    except Exception:
        return []


def _sanitize_tenant_id(tenant_id):
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


def _tenant_sql_and(tenant_ids, col="tenant_id"):
    """``AND``-shaped predicate scoping a BigQuery read to a set of
    ``tenant_id`` values. Returns ``""`` for admin (``tenant_ids is None``).

    Empty list returns ``AND 1 = 0`` (fail-closed): an authenticated
    user with no broker connections sees no rows, not all rows.
    """
    if tenant_ids is None:
        return ""
    if not tenant_ids:
        return "AND 1 = 0"
    safe = [_sanitize_tenant_id(t) for t in tenant_ids]
    safe = [t for t in safe if t]
    if not safe:
        return "AND 1 = 0"
    safe_col = re.sub(r"[^A-Za-z0-9_.]", "", str(col))
    quoted = ", ".join(f"'{t}'" for t in safe)
    return f"AND {safe_col} IN ({quoted})"


def _tenant_sql_filter(tenant_ids, col="tenant_id"):
    """``WHERE``-prefixed sibling for queries that don't already have a
    ``WHERE`` clause. Returns ``""`` for admin.
    """
    if tenant_ids is None:
        return ""
    if not tenant_ids:
        return "WHERE 1 = 0"
    safe = [_sanitize_tenant_id(t) for t in tenant_ids]
    safe = [t for t in safe if t]
    if not safe:
        return "WHERE 1 = 0"
    safe_col = re.sub(r"[^A-Za-z0-9_.]", "", str(col))
    quoted = ", ".join(f"'{t}'" for t in safe)
    return f"WHERE {safe_col} IN ({quoted})"


def _filter_df_by_tenant_ids(df, tenant_ids, col="tenant_id"):
    """DataFrame-side belt-and-suspenders filter.

    Admin (``tenant_ids is None``) bypasses the filter.
    Empty list returns an empty same-shape frame.
    Rows with NULL/missing ``tenant_id`` are DROPPED for non-admin
    callers — under v2 every legitimate row carries a tenant_id, so
    NULL is either pre-cutover legacy data or an ingestion bug, both
    of which are non-tenant data and must not leak to a signed-in user.

    If the column doesn't exist on the frame (deploy-gap: a mart that
    hasn't propagated tenant_id yet), the helper returns the frame
    unchanged — the route-level legacy filter is still the active
    security boundary during the migration window.
    """
    if df is None or df.empty:
        return df
    if tenant_ids is None:
        return df
    if col not in df.columns:
        return df
    if not tenant_ids:
        return df.iloc[0:0]
    safe = {_sanitize_tenant_id(t) for t in tenant_ids}
    safe.discard(None)
    if not safe:
        return df.iloc[0:0]
    series = df[col].astype(str)
    keep = series.isin(safe)
    return df.loc[keep].reset_index(drop=True)


def _dedupe_enriched_current_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate open rows from ``int_enriched_current`` (same contract or
    equity line merged twice). Seed/snapshot regressions can emit byte-near
    duplicates; the UI should not show twin 200-share lines with identical cost.

    The dedup key leads with ``tenant_id``: the same symbol held in multiple
    physical accounts that share a display ``account`` label (e.g. 5 SnapTrade
    "Schwab Account" tenants all holding QTUM) is NOT a duplicate — each is a
    real, separately-held lot. Without ``tenant_id`` in the key all 5 collapse
    to one row, which silently undercounts the Hero total, Breakdown-by-Type
    equity, and the open-legs table to a single tenant's P&L.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "trade_symbol" in out.columns:
        out["trade_symbol"] = (
            out["trade_symbol"].astype(str).str.strip().replace({"nan": ""})
        )
    key = [c for c in ("tenant_id", "account", "user_id", "instrument_type", "trade_symbol") if c in out.columns]
    if len(key) < 2:
        return df
    return out.drop_duplicates(subset=key, keep="last").reset_index(drop=True)


def _narrow_mart_daily_pnl_chart_df_to_summary_tenant(
    chart_df: pd.DataFrame, summary_df: pd.DataFrame
) -> pd.DataFrame:
    """When admin scope merges two Postgres tenants under one ``account`` label,
    ``mart_daily_pnl`` can return parallel date spines. Stateful
    ``_build_chart_from_daily_pnl`` would process every row and double-count
    equity fills. Align the chart frame to the same ``user_id`` distribution
    as ``positions_summary`` for this page (mode wins on ties)."""
    if chart_df is None or chart_df.empty or "user_id" not in chart_df.columns:
        return chart_df
    m_ids = pd.to_numeric(chart_df["user_id"], errors="coerce").dropna().unique()
    if len(m_ids) <= 1:
        return chart_df
    if summary_df is None or summary_df.empty or "user_id" not in summary_df.columns:
        app.logger.warning(
            "mart_daily_pnl chart has %s distinct user_ids but summary lacks user_id; "
            "cannot narrow chart tenant",
            len(m_ids),
        )
        return chart_df
    s_ids = pd.to_numeric(summary_df["user_id"], errors="coerce").dropna()
    if s_ids.empty:
        return chart_df
    uid_keep = int(s_ids.astype(int).value_counts().index[0])
    m_num = pd.to_numeric(chart_df["user_id"], errors="coerce")
    narrowed = chart_df.loc[m_num.eq(uid_keep)].copy()
    if narrowed.empty:
        app.logger.warning(
            "chart tenant narrow: summary user_id=%s absent from mart chart; "
            "keeping un-narrowed frame",
            uid_keep,
        )
        return chart_df
    return narrowed


def _filter_current_for_chart_partition(
    current_df: pd.DataFrame, account, user_id_key, tenant_id_key=None
) -> pd.DataFrame:
    """Slice ``int_enriched_current`` rows for one chart partition
    (``account`` × optional ``user_id``). Required when ``mart_daily_pnl``
    spans multiple partitions for the same symbol — the live today-row patch
    must not mix snapshots across tenants.

    When the mart partition has a populated ``user_id`` but Schwab/sync
    snapshot rows still have ``user_id IS NULL`` (Stage 0 backfill lag),
    strict equality would yield an **empty** slice, skipping the entire
    live MTM patch — chart terminal sticks on realized-only while hero
    and Breakdown-by-type include broker unrealized (IYW-style gap).
    Prefer exact ``user_id`` match; fall back to NULL-id rows **only**
    for the same ``account`` (DataFrame already passed
    ``_filter_df_by_accounts``)."""
    if current_df is None or current_df.empty or "account" not in current_df.columns:
        return pd.DataFrame()
    # When the mart partition is keyed by the broker-stable tenant_id (the
    # v2 grain), prefer matching the snapshot on tenant_id so two physical
    # accounts sharing an ``account`` label (e.g. several "Schwab Account"s)
    # don't pool their live snapshot rows into one chart partition.
    if tenant_id_key is not None and "tenant_id" in current_df.columns:
        m = current_df["tenant_id"].astype(str) == str(tenant_id_key).strip()
        return current_df.loc[m].copy()
    m = current_df["account"].astype(str) == str(account).strip()
    if "user_id" in current_df.columns:
        uid_series = pd.to_numeric(current_df["user_id"], errors="coerce")
        if user_id_key is None or pd.isna(user_id_key):
            m &= uid_series.isna()
        else:
            uk = float(pd.to_numeric(pd.Series([user_id_key]), errors="coerce").iloc[0])
            m_uk = uid_series == uk
            if m_uk.any():
                m &= m_uk
            else:
                m &= uid_series.isna()
    return current_df.loc[m].copy()


def _drop_phantom_equity_writeoffs(
    closed_equity_df: pd.DataFrame, current_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Strip ``int_closed_equity_legs`` "Cost Written Off" rows when the
    broker snapshot still shows the symbol held on the same ``account``.

    Returns ``(kept_df, removed_df)`` so callers can reverse the bogus
    realized contribution out of any other mart (e.g. ``positions_summary``)
    that aggregated the same writeoff into a strategy rollup.

    Mirrors the ``account_symbol_holdings`` suppression added to
    ``int_closed_equity_legs.sql`` so the position page renders correctly
    even before BigQuery has been re-built. Failure mode pinned by IYW /
    Emmory Investment (May 2026): an orphan-tenant split (Schwab synced
    before ``user_id`` was linked to the masked ``account``) leaves a
    session with ``total_buy_qty > total_sell_qty`` under one
    ``(account, user_id)`` partition while the **same shares** are still
    open under another partition. dbt emits a phantom *Cost Written Off*
    for the residual; that row poisons everything downstream — Position
    Legs adds a bogus 10-share -$1,966 line, Breakdown by Type rolls
    realized down to -$1,957, Strategy Breakdown shows a fake
    "Dividend (Closed)" -$1,957 row, the chart-substitute path inherits
    that cliff, and the reconciliation banner fires.

    Suppression rule (matches dbt): drop the writeoff row when
    ``int_enriched_current`` shows shares of the same ``(account, symbol)``
    >= the writeoff row's residual quantity. Otherwise the row may be a
    real loss (genuine off-platform transfer / corporate action) and is
    left alone."""
    empty_removed = pd.DataFrame()
    if closed_equity_df is None or closed_equity_df.empty:
        return closed_equity_df, empty_removed
    if "description" not in closed_equity_df.columns:
        return closed_equity_df, empty_removed
    desc = closed_equity_df["description"].astype(str).str.strip().str.lower()
    is_writeoff = desc.eq("cost written off")
    if not is_writeoff.any():
        return closed_equity_df, empty_removed
    sym_col = next(
        (c for c in ("symbol", "trade_symbol") if c in closed_equity_df.columns),
        None,
    )
    if sym_col is None or "account" not in closed_equity_df.columns \
            or "quantity" not in closed_equity_df.columns:
        return closed_equity_df, empty_removed
    if current_df is None or current_df.empty \
            or "instrument_type" not in current_df.columns:
        return closed_equity_df, empty_removed
    it = current_df["instrument_type"].astype(str).str.strip().str.lower()
    eq_open = current_df.loc[it.eq("equity")]
    if eq_open.empty:
        return closed_equity_df, empty_removed
    cur_sym_col = next(
        (c for c in ("symbol", "underlying_symbol") if c in eq_open.columns),
        None,
    )
    if cur_sym_col is None or "account" not in eq_open.columns \
            or "quantity" not in eq_open.columns:
        return closed_equity_df, empty_removed
    held = (
        pd.DataFrame({
            "account": eq_open["account"].astype(str).str.strip(),
            "symbol": eq_open[cur_sym_col].astype(str).str.strip(),
            "qty": pd.to_numeric(eq_open["quantity"], errors="coerce")
                .fillna(0).abs(),
        })
        .groupby(["account", "symbol"], as_index=False)["qty"].sum()
    )
    held_map = {
        (r.account, r.symbol): float(r.qty)
        for r in held.itertuples(index=False)
    }
    cw_acct = closed_equity_df["account"].astype(str).str.strip()
    cw_sym = closed_equity_df[sym_col].astype(str).str.strip()
    cw_qty = pd.to_numeric(
        closed_equity_df["quantity"], errors="coerce"
    ).fillna(0).abs()
    held_for_row = pd.Series(
        [held_map.get((a, s), 0.0) for a, s in zip(cw_acct, cw_sym)],
        index=closed_equity_df.index,
        dtype=float,
    )
    drop_mask = is_writeoff & (cw_qty > 0) & (held_for_row >= cw_qty)
    if not drop_mask.any():
        return closed_equity_df, empty_removed
    return (
        closed_equity_df.loc[~drop_mask].copy(),
        closed_equity_df.loc[drop_mask].copy(),
    )


def _addback_phantom_writeoffs_to_summary(
    summary_df: pd.DataFrame, removed_writeoffs: pd.DataFrame
) -> pd.DataFrame:
    """Reverse the bogus realized contribution from
    ``_drop_phantom_equity_writeoffs`` out of ``positions_summary``.

    ``positions_summary`` aggregates ``int_closed_equity_legs`` into per
    ``(account, symbol, strategy, status)`` rollups. When dbt still
    emits a phantom "Cost Written Off" row, ``positions_summary``'s
    Closed strategy row for that ``(account, symbol)`` carries the
    bogus realized P&L. After the Python strip, that strategy row would
    still show the phantom number — so Strategy Breakdown disagrees
    with Position Legs + Breakdown by Type.

    Per ``(account, symbol)`` writeoff bucket: find the Closed strategy
    row with the realized P&L closest to ``-addback`` and add the
    writeoff back to ``realized_pnl`` / ``total_pnl`` / ``total_return``.
    Trade and date counters are left alone — the underlying fills are
    real, only the writeoff *amount* was the dbt artifact."""
    if summary_df is None or summary_df.empty:
        return summary_df
    if removed_writeoffs is None or removed_writeoffs.empty:
        return summary_df
    if "realized_pnl" not in summary_df.columns:
        return summary_df
    if "realized_pnl" not in removed_writeoffs.columns:
        return summary_df
    sym_col_wo = next(
        (c for c in ("symbol", "trade_symbol")
         if c in removed_writeoffs.columns),
        None,
    )
    sym_col_s = next(
        (c for c in ("symbol", "trade_symbol") if c in summary_df.columns),
        None,
    )
    if sym_col_wo is None or sym_col_s is None \
            or "account" not in removed_writeoffs.columns \
            or "account" not in summary_df.columns:
        return summary_df
    addbacks = (
        removed_writeoffs.assign(
            _addback=pd.to_numeric(
                removed_writeoffs["realized_pnl"], errors="coerce"
            ).fillna(0).abs()
        )
        .groupby(
            [
                removed_writeoffs["account"].astype(str).str.strip(),
                removed_writeoffs[sym_col_wo].astype(str).str.strip(),
            ],
            as_index=True,
        )["_addback"]
        .sum()
    )
    if addbacks.empty:
        return summary_df
    out = summary_df.copy()
    s_acct = out["account"].astype(str).str.strip()
    s_sym = out[sym_col_s].astype(str).str.strip()
    s_status = (
        out["status"].astype(str).str.strip().str.lower()
        if "status" in out.columns else None
    )
    money_cols = [
        c for c in (
            "realized_pnl", "total_pnl", "total_return"
        ) if c in out.columns
    ]
    for (acct, sym), addback in addbacks.items():
        if not addback or addback <= 0:
            continue
        mask = s_acct.eq(acct) & s_sym.eq(sym)
        if s_status is not None:
            mask = mask & s_status.eq("closed")
        candidates = out.loc[mask]
        if candidates.empty:
            continue
        # Pick the row whose realized_pnl is most-closely the writeoff
        # carrier (realized ≈ -addback). On exact match this lands on
        # the dominant carrier; on partial overlap it still shrinks the
        # row that absorbed the most writeoff.
        cand_realized = pd.to_numeric(
            candidates["realized_pnl"], errors="coerce"
        ).fillna(0)
        target_idx = (cand_realized + addback).abs().idxmin()
        for col in money_cols:
            cur = float(pd.to_numeric(
                pd.Series([out.at[target_idx, col]]), errors="coerce"
            ).fillna(0).iloc[0])
            out.at[target_idx, col] = round(cur + float(addback), 2)
    return out


def _equity_slice_for_live_chart(current_df: pd.DataFrame) -> pd.DataFrame:
    """Rows that carry equity MTM for the LIVE today-row patch.

    Match case-insensitively and strip whitespace — BQ/pandas sometimes
    surface ``\"equity\"`` or padded values; strict ``== \"Equity\"``
    skipped the patch so the chart terminal stayed at the walker's
    realized-only value while KPIs used broker ``unrealized_pnl``."""
    if current_df is None or current_df.empty:
        return pd.DataFrame()
    if "instrument_type" not in current_df.columns:
        return pd.DataFrame()
    it = current_df["instrument_type"].astype(str).str.strip().str.lower()
    return current_df.loc[it.eq("equity")].copy()


def _merge_position_pnl_chart_payloads(parts: list) -> dict:
    """Sum cumulative position-chart series across partitions (each partition
    was built with its own equity cost-basis state machine).

    Rows missing on sparse partitions forward-fill within that partition before
    summing so inactive accounts contribute zero before their first date."""
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    parts = [p for p in (parts or []) if p and (p.get("dates") or [])]
    if not parts:
        return empty
    if len(parts) == 1:
        return parts[0]
    all_dates = sorted(set(d for p in parts for d in p["dates"]))
    idx = pd.Index(all_dates)
    keys = ["equity", "options", "dividends", "total"]
    merged = {k: pd.Series(0.0, index=idx, dtype=float) for k in keys}
    price_acc = pd.Series(index=idx, dtype=float)
    for p in parts:
        ds = p["dates"]
        for k in keys:
            vals = p[k][: len(ds)]
            s = pd.Series(vals, index=pd.Index(ds))
            s = s[~s.index.duplicated(keep="last")].sort_index()
            s = s.reindex(idx).ffill().fillna(0.0)
            merged[k] = merged[k].add(s, fill_value=0.0)
        pr = (p.get("underlying_price") or [None] * len(ds))[: len(ds)]
        ps = pd.Series(pr, index=pd.Index(ds))
        ps = ps[~ps.index.duplicated(keep="last")].sort_index()
        ps = ps.reindex(idx)
        price_acc = ps.combine_first(price_acc)

    def _rnd_series(s):
        return [round(float(x), 2) for x in s.tolist()]

    prices_out = []
    for x in price_acc.tolist():
        if x is None or pd.isna(x):
            prices_out.append(None)
        else:
            prices_out.append(round(float(x), 2))
    return {
        "dates": list(idx),
        "equity": _rnd_series(merged["equity"]),
        "options": _rnd_series(merged["options"]),
        "dividends": _rnd_series(merged["dividends"]),
        "total": _rnd_series(merged["total"]),
        "underlying_price": prices_out,
        "has_underlying_price": bool(price_acc.notna().any()),
    }


# ------------------------------------------------------------------
# User-id-aware tenancy helpers — see docs/USER_ID_TENANCY.md.
#
# These are the security boundary going forward. The legacy
# ``_account_sql_*`` and ``_filter_df_by_accounts`` helpers above filter
# only by ``account`` (a free-form label) — and that string can collide
# across users. Two users with ``account_name = 'investment1'`` would
# each see the other's rows on every page. The cross-tenant guard in
# ``_user_account_list`` hides the conflict at request time, but the
# correct fix is to scope every BigQuery read by the row owner's
# ``user_id`` (Postgres ``users.id``), which is now stamped onto every
# user-tied row through the dbt pipeline.
#
# Stage 0 / 1 leniency: legacy rows in BigQuery still have
# ``user_id IS NULL`` until the operator runs
# ``scripts/backfill_seed_user_ids.py``. The helpers below admit
# ``user_id IS NULL`` rows whose ``account`` matches the user's allowed
# list so the app keeps working during the backfill window. Stage 4
# drops the NULL leg once every seed cell is populated.
# ------------------------------------------------------------------


def _qualified_user_col(col, user_col):
    """If ``col`` is qualified (e.g. ``sc.account``) and ``user_col`` is
    the bare default ``user_id``, prefix ``user_col`` with the same alias
    so the predicate isn't ambiguous in JOINs. Callers can still pass an
    explicit ``user_col`` to override.
    """
    if user_col != "user_id":
        return user_col
    if "." not in col:
        return user_col
    alias = col.rsplit(".", 1)[0]
    return f"{alias}.user_id"


def _user_scoped_filter(user_id, accounts, *, col="account", user_col="user_id"):
    """Return a ``WHERE``-prefixed clause that scopes a BQ read to a tenant.

    Tenant = ``(user_id, account_name)``. ``account_name`` alone is not
    a security boundary — see ``docs/USER_ID_TENANCY.md``.

    Args:
        user_id: ``int`` Postgres ``users.id`` of the current user.
            ``None`` means admin (no user_id predicate).
        accounts: list of account labels the user is allowed to see, or
            ``None`` for admin (no account predicate).
        col: BQ column for ``account``. Defaults to ``account``.
        user_col: BQ column for ``user_id``. Defaults to the alias of
            ``col`` (``sc.user_id`` when ``col="sc.account"``).

    Returns ``""`` when both filters are skipped (admin), else a string
    starting with ``WHERE``.
    """
    user_col = _qualified_user_col(col, user_col)
    parts = []
    if user_id is not None:
        # OR (user_id IS NULL) is the Stage 0/1 leniency leg — drops in
        # Stage 4 once all legacy rows are backfilled.
        parts.append(f"({user_col} = {int(user_id)} OR {user_col} IS NULL)")
    if accounts is None:
        pass
    elif not accounts:
        parts.append("1 = 0")
    else:
        quoted = ", ".join(
            f"'{a.replace(chr(39), chr(39) + chr(39))}'" for a in accounts
        )
        expr = f"TRIM(CAST({col} AS STRING))"
        parts.append(f"{expr} IN ({quoted})")
    if not parts:
        return ""
    return "WHERE " + " AND ".join(parts)


def _user_scoped_and(user_id, accounts, *, col="account", user_col="user_id"):
    """Same shape as ``_user_scoped_filter`` but as an ``AND`` clause for
    joining onto an existing ``WHERE``. Returns ``""`` when both filters
    are skipped.
    """
    user_col = _qualified_user_col(col, user_col)
    parts = []
    if user_id is not None:
        parts.append(f"({user_col} = {int(user_id)} OR {user_col} IS NULL)")
    if accounts is None:
        pass
    elif not accounts:
        parts.append("1 = 0")
    else:
        quoted = ", ".join(
            f"'{a.replace(chr(39), chr(39) + chr(39))}'" for a in accounts
        )
        expr = f"TRIM(CAST({col} AS STRING))"
        parts.append(f"{expr} IN ({quoted})")
    if not parts:
        return ""
    return "AND " + " AND ".join(parts)


def _filter_df_by_user(df, user_id, accounts, *, col="account", user_col="user_id"):
    """DataFrame analogue of ``_user_scoped_filter``.

    Drops rows whose ``user_col`` is a populated id different from
    ``user_id``. Rows with ``user_col`` NULL are kept *only* when their
    ``col`` matches one of ``accounts`` (Stage 0/1 leniency for legacy
    rows in BigQuery that haven't been backfilled yet). Admin
    (``user_id is None``) bypasses the user check.
    """
    if df is None:
        return df
    if df.empty:
        return df
    if user_id is None and accounts is None:
        return df

    out = df

    if user_id is not None and user_col in out.columns:
        target = int(user_id)

        def _norm_uid(v):
            if v is None:
                return None
            if isinstance(v, float) and pd.isna(v):
                return None
            try:
                return int(v)
            except (TypeError, ValueError):
                s = str(v).strip()
                if not s:
                    return None
                try:
                    return int(float(s))
                except (TypeError, ValueError):
                    return None

        norm = out[user_col].map(_norm_uid)
        # Keep rows where user_id matches, OR where user_id is NULL AND
        # the row's account is in the user's allowed list (legacy lenience).
        match_user = norm == target
        if accounts is None:
            keep_null = norm.isna()
        else:
            want = {
                str(a).strip()
                for a in accounts
                if a is not None and str(a).strip() != ""
            }
            if col in out.columns:
                acc_str = out[col].map(
                    lambda v: None
                    if v is None or (isinstance(v, float) and pd.isna(v))
                    else str(v).strip()
                )
                keep_null = norm.isna() & acc_str.isin(want)
            else:
                keep_null = norm.isna()
        out = out[match_user | keep_null]

    if accounts is not None and col in out.columns:
        if not accounts:
            return out.iloc[0:0]
        want = {
            str(a).strip()
            for a in accounts
            if a is not None and str(a).strip() != ""
        }

        def _norm_acc(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return str(v).strip()

        m = out[col].map(_norm_acc).isin(want)
        out = out[m]

    return out


def _df_normalize_account_column(df):
    """BigQuery to_dataframe() sometimes returns Account; app filters on account."""
    if df is None or df.empty:
        return df
    if "Account" in df.columns and "account" not in df.columns:
        return df.rename(columns={"Account": "account"})
    return df


def _legs_df_to_sessions_list(legs_df):
    """Reshape int_position_legs rows into the legacy ``sessions_list`` dict
    shape that the position_detail template and downstream helpers consume.

    Maintains the historic key contract:
      - ``session_id`` ← ``leg_id``       (positive for equity sessions,
                                           negative for options-only legs)
      - ``display_leg`` ← ``display_leg_num`` (chronological 1..N)
      - ``last_trade_date`` ← ``last_activity_date`` (string YYYY-MM-DD)
      - ``options_pnl`` ← ``closed_options_pnl + open_options_pnl``

    Replaces ~150 lines of stateful Python (orphan-grouping, gap-id
    assignment, P&L overlap re-aggregation) — the dbt mart owns all of
    that now. Returns ``[]`` for an empty / None DataFrame.
    """
    if legs_df is None or legs_df.empty:
        return []

    df = legs_df.copy()
    for col in (
        "equity_pnl", "closed_options_pnl", "open_options_pnl",
        "combined_pnl", "max_quantity_held",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in (
        "options_count", "open_options_count", "num_trades",
        "leg_id", "display_leg_num", "days_held",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if "display_leg_num" in df.columns:
        df = df.sort_values("display_leg_num")

    out = []
    for _, r in df.iterrows():
        od = r.get("open_date")
        ld = r.get("last_activity_date")
        equity_pnl = round(float(r.get("equity_pnl") or 0), 2)
        options_pnl = round(
            float(r.get("closed_options_pnl") or 0) + float(r.get("open_options_pnl") or 0),
            2,
        )
        combined = round(
            float(r.get("combined_pnl") or (equity_pnl + options_pnl)), 2
        )
        out.append({
            "session_id": int(r["leg_id"]),
            "display_leg": int(r["display_leg_num"]),
            "status": str(r.get("status") or "Closed"),
            "open_date": str(od) if od is not None and not pd.isna(od) else "",
            "last_trade_date": str(ld) if ld is not None and not pd.isna(ld) else "",
            "equity_pnl": equity_pnl,
            "options_pnl": options_pnl,
            "options_count": int(r.get("options_count") or 0),
            "combined_pnl": combined,
            "total_pnl": combined,
            "days_held": int(r.get("days_held") or 0),
            "max_quantity_held": float(r.get("max_quantity_held") or 0),
            "num_trades": int(r.get("num_trades") or 0),
            "options_only": bool(r.get("options_only") or False),
            "open_options_count": int(r.get("open_options_count") or 0),
        })
    return out


def _iter_symbols_for_daily_detail(trades_df, pnl_df, current_df, open_pairs):
    """
    Row keys (account, symbol) for /symbols. dbt can classify open options from
    the current snapshot alone (int_option_contracts.snapshot_only_options) so
    positions_summary has a row with no stg_history rows — the Positions page
    still works. This iterator unions trade-history keys with positions_summary
    and current so Daily Detail matches that catalog.
    """
    seen = set()
    out = []
    if (
        not trades_df.empty
        and "account" in trades_df.columns
        and "symbol" in trades_df.columns
    ):
        for (acc, sym), _ in trades_df.groupby(["account", "symbol"]):
            k = (str(acc), str(sym))
            if open_pairs is not None and k not in open_pairs:
                continue
            if k not in seen:
                seen.add(k)
                out.append((acc, sym))
    for df in (pnl_df, current_df):
        if df is None or df.empty or "account" not in df.columns or "symbol" not in df.columns:
            continue
        for _, row in df.drop_duplicates(["account", "symbol"]).iterrows():
            acc, sym = row["account"], row["symbol"]
            k = (str(acc), str(sym))
            if open_pairs is not None and k not in open_pairs:
                continue
            if k in seen:
                continue
            seen.add(k)
            out.append((acc, sym))
    return out


# ------------------------------------------------------------------
# SQL: date-filtered re-aggregation of positions_summary
# This CANNOT be a dbt model because it requires runtime date parameters
# from the user's filter selection. It re-aggregates int_strategy_classification
# with a WHERE clause on dates — essentially positions_summary with a date window.
# ------------------------------------------------------------------
DATE_FILTERED_QUERY = """
-- Date-filtered re-aggregation that mirrors positions_summary so the date
-- picker on /positions stays consistent with the un-filtered mart. Mirrors
-- the dividends-as-first-class semantics:
--   * total_pnl folds in attributed dividend income
--   * Buy-and-Hold reclassified to "Dividend" when div income > price gain
--   * total_return preserved as alias of total_pnl for back-compat
--
-- ATTRIBUTION_INVARIANT: The dividend ranking + attribution + Buy-and-Hold
-- reclassification logic below MUST stay in sync with the canonical
-- definition in dbt/macros/attribute_dividends_to_strategy.sql (which is
-- imported by dbt/models/marts/positions_summary.sql). This runtime SQL
-- can't call the dbt macro directly because dbt macros compile at
-- `dbt build` time, not at request time, and we need the start_date /
-- end_date URL params to flow into the source filter. The duplication is
-- intentional and documented; if you change the macro, mirror the change
-- here AND verify with the integration test
-- tests/test_positions_filter_discipline.py::test_date_filtered_at_full_window_matches_mart.
WITH classified AS (
    SELECT *
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE open_date <= @end_date
      AND COALESCE(close_date, CURRENT_DATE()) >= @start_date
      {tenant_filter}
),

-- Read dividends from int_dividend_events (per-event). int_dividend_events
-- UNIONs CSV-reported dividends (from stg_history.action='dividend') with
-- yfinance-synthesized ex-div × holdings events. Reading stg_history
-- directly here was broken for ~99% of users: Schwab Connect drops
-- DIVIDEND_OR_INTEREST transactions and most users have never run a manual
-- CSV upload, so JEPI / JEPQ / SCHD positions reported $0 dividend income
-- on /position even when the user clearly owned thousands of shares for
-- years. Going through int_dividend_events instead respects the date
-- range filter while picking up synthetic dividends.
dividends AS (
    SELECT
        tenant_id,
        account,
        user_id,
        symbol,
        SUM(amount) AS total_dividend_income,
        COUNT(*) AS dividend_count
    FROM `ccwj-dbt.analytics.int_dividend_events`
    WHERE trade_date >= @start_date
      AND trade_date <= @end_date
      {tenant_filter}
    GROUP BY 1, 2, 3, 4
),

strategy_summary AS (
    SELECT
        tenant_id,
        account,
        user_id,
        symbol,
        strategy,

        -- Match positions_summary's 2-state status. The mart deliberately
        -- folds "both open and closed positions for this (account, symbol,
        -- strategy)" into 'Open' rather than emitting a 3rd 'Mixed' state,
        -- per its inline comment "to keep the UX simple". This runtime
        -- query used to emit 'Mixed' too, so the same page showed
        -- different status counts in the all-time view (mart, no Mixed)
        -- vs the date-filtered view (runtime, with Mixed). Folding here
        -- restores ATTRIBUTION_INVARIANT and stops users from seeing
        -- chips that vanish when they clear the date filter.
        CASE
            WHEN COUNTIF(status = 'Open') > 0 THEN 'Open'
            ELSE 'Closed'
        END AS status,

        SUM(total_pnl) AS total_pnl,
        -- Use pre-split realized_pnl / unrealized_pnl from
        -- int_strategy_classification rather than deriving from total_pnl
        -- by status. The pre-split version correctly attributes the
        -- already-realized portion of a still-open equity session (one
        -- with interim sells) to realized_pnl. The old "CASE WHEN
        -- status='Closed' THEN total_pnl" derivation lumped 100% of an
        -- Open session's P&L into unrealized — even after the trader
        -- had banked $X selling half the position. positions_summary has
        -- always done it this way; this restores ATTRIBUTION_INVARIANT.
        SUM(realized_pnl)   AS realized_pnl,
        SUM(unrealized_pnl) AS unrealized_pnl,

        SUM(premium_received) AS total_premium_received,
        SUM(ABS(premium_paid)) AS total_premium_paid,

        COUNT(*) AS num_trade_groups,
        SUM(num_trades) AS num_individual_trades,
        COUNTIF(is_winner AND status = 'Closed') AS num_winners,
        COUNTIF(NOT is_winner AND status = 'Closed') AS num_losers,

        SAFE_DIVIDE(
            COUNTIF(is_winner AND status = 'Closed'),
            NULLIF(COUNTIF(status = 'Closed'), 0)
        ) AS win_rate,

        SAFE_DIVIDE(
            SUM(CASE WHEN status = 'Closed' THEN total_pnl ELSE 0 END),
            NULLIF(COUNTIF(status = 'Closed'), 0)
        ) AS avg_pnl_per_trade,

        ROUND(AVG(days_in_trade), 1) AS avg_days_in_trade,
        MIN(open_date) AS first_trade_date,
        MAX(COALESCE(close_date, CURRENT_DATE())) AS last_trade_date

    FROM classified
    GROUP BY 1, 2, 3, 4, 5
),

with_dividend_rank AS (
    SELECT
        ss.*,
        ROW_NUMBER() OVER (
            PARTITION BY ss.tenant_id, ss.account, ss.user_id, ss.symbol
            ORDER BY
                CASE ss.strategy
                    WHEN 'Wheel'        THEN 1
                    WHEN 'Covered Call'  THEN 2
                    WHEN 'Buy and Hold'  THEN 3
                    ELSE 99
                END
        ) AS dividend_rank
    FROM strategy_summary ss
),

with_attributed AS (
    SELECT
        wdr.*,
        CASE WHEN wdr.dividend_rank = 1
            THEN COALESCE(d.total_dividend_income, 0)
            ELSE 0
        END AS attributed_dividend_income,
        CASE WHEN wdr.dividend_rank = 1
            THEN COALESCE(d.dividend_count, 0)
            ELSE 0
        END AS attributed_dividend_count
    FROM with_dividend_rank wdr
    LEFT JOIN dividends d
        ON (wdr.tenant_id IS NOT DISTINCT FROM d.tenant_id)
        AND wdr.account = d.account
        AND (wdr.user_id IS NOT DISTINCT FROM d.user_id)
        AND wdr.symbol = d.symbol
),

final AS (
    SELECT
        wa.tenant_id,
        wa.account,
        wa.user_id,
        wa.symbol,
        CASE
            WHEN wa.dividend_rank = 1
                 AND wa.strategy = 'Buy and Hold'
                 AND wa.attributed_dividend_income > GREATEST(wa.total_pnl, 0)
                THEN 'Dividend'
            ELSE wa.strategy
        END AS strategy,
        wa.status,
        ROUND(wa.total_pnl + wa.attributed_dividend_income, 2) AS total_pnl,
        ROUND(wa.total_pnl, 2)        AS trade_only_pnl,
        ROUND(wa.realized_pnl, 2)     AS realized_pnl,
        ROUND(wa.unrealized_pnl, 2)   AS unrealized_pnl,
        ROUND(wa.total_premium_received, 2) AS total_premium_received,
        ROUND(wa.total_premium_paid, 2) AS total_premium_paid,
        wa.num_trade_groups,
        wa.num_individual_trades,
        wa.num_winners,
        wa.num_losers,
        ROUND(wa.win_rate, 4) AS win_rate,
        ROUND(wa.avg_pnl_per_trade, 2) AS avg_pnl_per_trade,
        wa.avg_days_in_trade,
        wa.first_trade_date,
        wa.last_trade_date,
        ROUND(wa.attributed_dividend_income, 2) AS total_dividend_income,
        wa.attributed_dividend_count            AS dividend_count,
        ROUND(wa.total_pnl + wa.attributed_dividend_income, 2) AS total_return
    FROM with_attributed wa
)

SELECT * FROM final
ORDER BY tenant_id, account, user_id, symbol, strategy
"""

# ------------------------------------------------------------------
# Default (no date filter): use the pre-built mart
# ------------------------------------------------------------------
DEFAULT_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {tenant_filter}
    ORDER BY account, symbol, strategy
"""

ERROR_DEFAULTS = dict(
    error="",
    rows=[],
    symbol_rows=[],
    kpis={},
    strategy_chart=[],
    accounts=[],
    strategies=[],
    symbols=[],
    subsectors=[],
    sectors=[],
    user_accounts=[],
    status_counts={"Open": 0, "Closed": 0, "Mixed": 0},
    selected_account="",
    selected_strategy="",
    selected_statuses=[],
    selected_symbol="",
    selected_subsector="",
    selected_sector="",
    selected_start_date="",
    selected_end_date="",
    date_filtered=False,
    page=1,
    total_pages=1,
    total_rows=0,
    per_page=25,
    today=date.today(),
    timedelta=timedelta,
)


def _parse_date(value):
    """Return a date object if value is a valid YYYY-MM-DD string, else None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None



# ------------------------------------------------------------------
# Feature pages for marketing (logged-out)
# ------------------------------------------------------------------
FEATURES = {
    "strategy-auto-detection": {
        "title": "Strategy auto-detection",
        "subtitle": "Every position classified automatically—no manual tagging.",
        "demo_partial": "features/_demo_strategy.html",
        "value_bullets": [
            "Covered Calls, Cash-Secured Puts, Wheels, spreads, and Buy and Hold—all identified from your trade data.",
            "See exactly which strategies drive your returns and which drain performance.",
            "Stop guessing. Know whether the Wheel is outperforming CSPs for your portfolio.",
        ],
    },
    "ai-trading-insights": {
        "title": "AI trading insights",
        "subtitle": "Personalized analysis of your trading style and performance.",
        "demo_partial": "features/_demo_insights.html",
        "value_bullets": [
            "Get a data-driven overview: what's working, what's leaking, and why.",
            "Observations grounded in your actual trades—not generic advice.",
            "The 'wow' moment when the app shows it truly understands your trading.",
        ],
    },
    "performance-charts": {
        "title": "Performance charts",
        "subtitle": "Cumulative P&L over time, broken down by equity, options, and dividends.",
        "demo_partial": "features/_demo_charts.html",
        "value_bullets": [
            "Visualize your progress. See how each strategy contributes over time.",
            "Portfolio-wide and per-account charts so nothing stays hidden.",
            "The full picture—not just today's balance, but the journey.",
        ],
    },
    "position-detail": {
        "title": "Position detail",
        "subtitle": "Drill into any symbol: trades, strategies, and cumulative P&L.",
        "demo_partial": "features/_demo_position.html",
        "value_bullets": [
            "Click any symbol to see its full story: every trade, every strategy, every dollar.",
            "Understand why a position performed the way it did—before your next move.",
            "Trade history, current positions, and charts in one place.",
        ],
    },
    "multi-account": {
        "title": "Multi-account",
        "subtitle": "Track all your Schwab accounts in one place.",
        "demo_partial": "features/_demo_multiaccount.html",
        "value_bullets": [
            "IRA, taxable, joint—see portfolio-wide metrics and per-account breakdowns.",
            "Filter by account on every view: positions, tax center, performance.",
            "One dashboard. All your accounts.",
        ],
    },
}


@app.route("/features/<slug>")
def feature_detail(slug):
    """Feature detail page with demo and value prop."""
    if slug == "ai-trading-insights" and not app.config.get("INSIGHTS_ENABLED", True):
        abort(404)
    feature = FEATURES.get(slug)
    if not feature:
        abort(404)
    return render_template(
        "features/detail.html",
        title=feature["title"],
        feature=feature,
        all_features=FEATURES,
        current_slug=slug,
    )


@app.route("/pricing")
def pricing():
    """Pricing placeholder for marketing."""
    waitlisted = False
    try:
        if current_user.is_authenticated:
            from app.models import is_user_on_pro_waitlist
            waitlisted = is_user_on_pro_waitlist(current_user.id)
    except Exception:
        waitlisted = False
    return render_template(
        "pricing.html",
        title="Pricing",
        pro_waitlisted=waitlisted,
    )


@app.route("/pro/waitlist", methods=["POST"])
def pro_waitlist():
    """Add an email (or current user) to the Pro tier waitlist."""
    from app.models import add_pro_waitlist_entry
    from app.utils import demo_block_writes

    # Demo: every visitor would be 'demo' on the waitlist, which is noise
    # and would confuse outreach later.
    blocked = demo_block_writes("joining the Pro waitlist")
    if blocked:
        return blocked

    email = (request.form.get("email") or "").strip().lower()
    user_id = current_user.id if current_user.is_authenticated else None

    if not user_id and not email:
        flash("Enter an email address so we can notify you.", "warning")
        return redirect(url_for("pricing"))

    if not user_id:
        # Light email validation
        if "@" not in email or "." not in email or len(email) > 320:
            flash("That email doesn't look right. Try again?", "warning")
            return redirect(url_for("pricing"))

    try:
        add_pro_waitlist_entry(user_id=user_id, email=email or None)
        flash("You're on the waitlist. We'll be in touch when Pro is ready.", "success")
    except Exception as exc:
        app.logger.exception("Pro waitlist signup failed: %s", exc)
        flash("Couldn't add you to the waitlist right now. Try again in a moment.", "danger")

    return redirect(url_for("pricing"))


# ------------------------------------------------------------------
# Beta feedback inbox
# ------------------------------------------------------------------


@app.route("/feedback", methods=["POST"])
@limiter.limit("5 per minute; 30 per hour")
def submit_feedback():
    """
    Footer Send-Feedback button posts here.

    Anonymous users CAN submit (we capture their IP for spam triage) so
    a tester who hits a 500 on a logged-out page can still report it.
    Demo user is allowed — feedback from the demo seat is signal, not
    noise. We hard-cap the body at 4 KB in the model layer.

    Returns JSON for XHR clients (the modal uses fetch) and redirects
    for plain form submits so the route degrades gracefully without JS.
    """
    from app.models import save_feedback

    body = (request.form.get("body") or request.form.get("message") or "").strip()
    page_path = (request.form.get("page_path") or request.referrer or "")[:512]

    user_id = current_user.id if current_user.is_authenticated else None
    username = current_user.username if current_user.is_authenticated else None

    wants_json = (
        request.accept_mimetypes.best == "application/json"
        or request.headers.get("X-Requested-With", "") == "XMLHttpRequest"
    )

    if not body:
        if wants_json:
            return {"ok": False, "error": "Tell us what's up — the message can't be empty."}, 400
        flash("Tell us what's up — the message can't be empty.", "warning")
        return redirect(request.referrer or url_for("index"))

    new_id = save_feedback(
        user_id=user_id,
        username=username,
        body=body,
        page_path=page_path or None,
        user_agent=(request.headers.get("User-Agent") or "")[:512] or None,
        ip_address=request.remote_addr,
    )

    if new_id is None:
        if wants_json:
            return {"ok": False, "error": "We couldn't save that just now. Try again in a minute."}, 500
        flash("We couldn't save that just now. Try again in a minute.", "danger")
        return redirect(request.referrer or url_for("index"))

    if wants_json:
        return {"ok": True, "id": new_id}
    flash("Thanks — feedback received. We read every message.", "success")
    return redirect(request.referrer or url_for("index"))


# ------------------------------------------------------------------
# Onboarding survey (multi-section wizard during first sync wait)
# ------------------------------------------------------------------
#
# Posted by the wizard on /sync/processing. Validates that every
# required question has an answer, packages the form into a single
# JSONB blob via save_onboarding_response, and returns JSON. The
# form-side JS swaps to a thank-you note on success and clears the
# "hold redirect" flag so the sync poll on the same page can take
# the user to Daily Review.

# Required radio/textarea keys the wizard MUST answer before submit.
# Free-text "_other" siblings are optional and only saved when the
# matching radio's value is "other". The list lives next to the route
# (not in models.py) on purpose: the form's contract is a
# request-layer concern, while the storage shape is a single JSONB
# blob — see AGENTS.md note on JSONB-flexibility for this table.
_ONBOARDING_REQUIRED_KEYS: tuple[str, ...] = (
    "why_here",
    "worth_paying_for",
    "trading_years",
    "primary_style",
    "trade_frequency",
    "position_count",
    "best_at",
    "worst_at",
    "discipline_self",
    "trade_notes",
    "help_most",          # multi-select; at least one option required
    "one_thing",          # textarea, min 10 non-whitespace chars
    "comfort",
)

# Optional adjunct keys — saved only when present and non-empty.
_ONBOARDING_OPTIONAL_KEYS: tuple[str, ...] = (
    "why_here_other",
    "worth_paying_for_other",
    "best_at_other",
    "worst_at_other",
    "help_most_other",
)

_ONBOARDING_MAX_FIELD_LEN = 1000
_ONBOARDING_MIN_ONE_THING_LEN = 10


@app.route("/onboarding/why-here", methods=["POST"])
@login_required
@limiter.limit("10 per minute; 60 per hour")
def submit_onboarding_why_here():
    """Save the wizard's full answer set for the current user (upsert)."""
    from app.models import save_onboarding_response

    wants_json = (
        request.accept_mimetypes.best == "application/json"
        or request.headers.get("X-Requested-With", "") == "XMLHttpRequest"
    )

    answers: dict[str, object] = {}

    # Required scalar fields (radios / textareas). ``help_most`` is
    # the one multi-select; pull both bracketed and bare names so the
    # form can use either ``name="help_most"`` or ``help_most[]``.
    for key in _ONBOARDING_REQUIRED_KEYS:
        if key == "help_most":
            vals = request.form.getlist("help_most[]") or request.form.getlist("help_most")
            cleaned = [v.strip()[:_ONBOARDING_MAX_FIELD_LEN] for v in vals if v and v.strip()]
            if cleaned:
                answers[key] = cleaned
        else:
            v = (request.form.get(key) or "").strip()
            if v:
                answers[key] = v[:_ONBOARDING_MAX_FIELD_LEN]

    # Optional free-text adjuncts (the "Something else: ___" boxes).
    for key in _ONBOARDING_OPTIONAL_KEYS:
        v = (request.form.get(key) or "").strip()
        if v:
            answers[key] = v[:_ONBOARDING_MAX_FIELD_LEN]

    missing = [k for k in _ONBOARDING_REQUIRED_KEYS if not answers.get(k)]
    one_thing = answers.get("one_thing")
    if isinstance(one_thing, str) and len(one_thing.strip()) < _ONBOARDING_MIN_ONE_THING_LEN:
        missing.append("one_thing")

    if missing:
        msg = "A couple of answers are still missing — finish those and resend."
        payload = {"ok": False, "error": msg, "missing": missing}
        if wants_json:
            return payload, 400
        flash(msg, "warning")
        return redirect(request.referrer or url_for("index"))

    ok = save_onboarding_response(user_id=current_user.id, answers=answers)
    if not ok:
        msg = "We couldn't save that just now. Try again in a minute."
        if wants_json:
            return {"ok": False, "error": msg}, 500
        flash(msg, "danger")
        return redirect(request.referrer or url_for("index"))

    # Weekly summary email opt-out toggle from the onboarding wizard.
    # The checkbox ships checked (opt-out): present => opted in, absent =>
    # the user turned it off before finishing. Mirrors the Weekly summary
    # control on the profile notifications tab (digest_email).
    try:
        from app.models import update_user_profile

        update_user_profile(
            current_user.id,
            digest_email=(request.form.get("digest_email") == "on"),
        )
    except Exception as exc:  # pragma: no cover - best-effort, non-blocking
        app.logger.warning("onboarding digest_email opt-in save failed: %s", exc)

    if wants_json:
        return {"ok": True}
    flash("Thanks — saved.", "success")
    return redirect(request.referrer or url_for("index"))


@app.route("/faq")
def faq():
    """FAQ page for marketing."""
    return render_template("faq.html", title="FAQ")


@app.route("/privacy")
def privacy():
    """Plain-English privacy policy."""
    return render_template("privacy.html", title="Privacy")


@app.route("/terms")
def terms():
    """Plain-English terms of service."""
    return render_template("terms.html", title="Terms")


@app.route("/contact")
def contact():
    """Contact / support page."""
    return render_template("contact.html", title="Contact")


@app.route("/sitemap.xml")
def sitemap():
    """Simple sitemap for SEO."""
    base = request.url_root.rstrip("/")
    pages = [
        ("", "daily", "1.0"),
        ("/pricing", "monthly", "0.8"),
        ("/faq", "monthly", "0.7"),
    ]
    for slug in FEATURES:
        if slug == "ai-trading-insights" and not app.config.get("INSIGHTS_ENABLED", True):
            continue
        pages.append((f"/features/{slug}", "monthly", "0.7"))
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for path, freq, prio in pages:
        xml += f"  <url><loc>{base}{path}</loc><changefreq>{freq}</changefreq><priority>{prio}</priority></url>\n"
    xml += "</urlset>"
    return Response(xml, mimetype="application/xml")


@app.route("/robots.txt")
def robots():
    """Basic robots.txt for crawlers."""
    base = request.url_root.rstrip("/")
    return Response(
        f"User-agent: *\nAllow: /\nDisallow: /positions\nDisallow: /upload\nDisallow: /insights\nDisallow: /settings\nDisallow: /accounts\nDisallow: /symbols\nDisallow: /position/\nSitemap: {base}/sitemap.xml\n",
        mimetype="text/plain",
    )


@app.route("/")
@app.route("/index")
def index():
    """Public landing page, or redirect to weekly review (home) if logged in."""
    if current_user.is_authenticated:
        return redirect(url_for("weekly_review"))
    return render_template("landing.html", title="Home")


@app.route("/healthz")
def healthz():
    """Liveness probe — does NOT touch DB or BigQuery so it stays green even
    if Postgres is briefly unreachable. Render uses this to know the worker
    process itself is alive."""
    return ("ok", 200, {"Content-Type": "text/plain", "Cache-Control": "no-store"})


@app.route("/healthz/db")
def healthz_db():
    """Readiness probe — confirms Postgres pool can hand out a connection
    in well under gunicorn's request timeout. Returns 503 fast on failure
    rather than hanging the request."""
    from app.db import get_conn
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return ("ok", 200, {"Content-Type": "text/plain", "Cache-Control": "no-store"})
    except Exception as exc:
        app.logger.warning("healthz/db failed: %s", exc)
        return (f"db_unavailable: {exc.__class__.__name__}", 503,
                {"Content-Type": "text/plain", "Cache-Control": "no-store"})



@app.route("/get-started")
@login_required
def get_started():
    """Onboarding checklist for new users — tracks real progress."""
    tenant_ids = get_tenant_ids_for_user(current_user.id) or []
    has_uploaded = len(tenant_ids) > 0

    # Check if data is actually available in BigQuery. We swallow the
    # exception so a transient BQ outage doesn't break the onboarding
    # page (the user can still see step 1/2/3 and the "refresh to check"
    # link), but the failure is logged so the operator can spot a
    # genuinely stuck pipeline. AGENTS.md flagged the silent pass as
    # known debt — replace with a logged warning.
    has_data = False
    if has_uploaded:
        try:
            client = get_bigquery_client()
            where = _tenant_sql_filter(tenant_ids)
            check_q = f"SELECT COUNT(*) AS cnt FROM `ccwj-dbt.analytics.positions_summary` {where}"
            result = client.query(check_q).to_dataframe()
            has_data = int(result.iloc[0]["cnt"]) > 0 if not result.empty else False
        except Exception as exc:
            app.logger.warning(
                "get_started has_data check failed for user_id=%s: %s",
                current_user.id, exc,
            )

    snaptrade_enabled = False
    snaptrade_connected = False
    snaptrade_full_history_days = 1825
    snaptrade_routine_days = 60
    try:
        from app.snaptrade import (
            snaptrade_enabled as _snaptrade_enabled_fn,
            _routine_lookback_days,
            SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS,
        )
        from app.models import get_snaptrade_accounts as _get_snaptrade_accounts

        snaptrade_enabled = bool(_snaptrade_enabled_fn())
        snaptrade_full_history_days = int(SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS)
        snaptrade_routine_days = int(_routine_lookback_days())
        snaptrade_connected = bool(_get_snaptrade_accounts(current_user.id))
    except Exception as exc:
        app.logger.warning(
            "get_started snaptrade enable check failed for user_id=%s: %s",
            current_user.id, exc,
        )

    return render_template(
        "get_started.html",
        title="Get Started",
        has_uploaded=has_uploaded,
        has_data=has_data,
        snaptrade_enabled=snaptrade_enabled,
        snaptrade_connected=snaptrade_connected,
        snaptrade_full_history_days=snaptrade_full_history_days,
        snaptrade_routine_days=snaptrade_routine_days,
    )


@app.route("/ping")
@limiter.exempt
def ping():
    return "Flask app is alive"


@app.route("/positions")
@login_required
def positions():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()

    # ------------------------------------------------------------------
    # 1. Read filter params
    # ------------------------------------------------------------------
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)
    selected_strategy = request.args.get("strategy", "")
    # Multi-select status; default is all (current + history) so users see their
    # full book unless they explicitly narrow it.
    selected_statuses = request.args.getlist("status")
    selected_symbol = request.args.get("symbol", "")
    # 'subsector' is the new param; 'industry' is the pre-rename alias and is
    # still accepted so any old bookmarks / external links keep working.
    selected_subsector = (
        request.args.get("subsector", "") or request.args.get("industry", "")
    )
    selected_sector = request.args.get("sector", "")
    selected_start_date = request.args.get("start_date", "")
    selected_end_date = request.args.get("end_date", "")
    page = max(1, int(request.args.get("page", 1)))

    start_date = _parse_date(selected_start_date)
    end_date = _parse_date(selected_end_date)
    date_filtered = start_date is not None or end_date is not None

    # ------------------------------------------------------------------
    # 2. Query BigQuery
    # ------------------------------------------------------------------
    try:
        if date_filtered:
            # Fill open boundaries with wide defaults
            effective_start = start_date or date(2000, 1, 1)
            effective_end = end_date or date.today()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", effective_start),
                    bigquery.ScalarQueryParameter("end_date", "DATE", effective_end),
                ]
            )
            df = cached_query_df(client, DATE_FILTERED_QUERY.format(tenant_filter=tenant_filter), job_config=job_config)
        else:
            df = cached_query_df(client, DEFAULT_QUERY.format(tenant_filter=tenant_filter))
    except Exception as exc:
        ctx = dict(ERROR_DEFAULTS)
        ctx["error"] = str(exc)
        # Even on error, pass the auth account list so the hero can render
        # the right "you have N accounts but couldn't load data" message
        # rather than the generic "no accounts linked" copy.
        ctx["user_accounts"] = user_accounts or []
        return render_template("positions.html", **ctx)

    # ------------------------------------------------------------------
    # 3. Tenant-scope BEFORE any aggregation or coercion
    #
    # IMPORTANT tenancy rule (keep): the hero, KPIs, chart, and every table
    # below MUST read off DataFrames that have already been scoped to the
    # logged-in user's accounts. The SQL is already account-scoped via
    # _account_sql_and, but the BQ-tenant rule requires a Python re-filter
    # before any re-aggregation (which includes the numeric coercion below
    # — fillna/to_numeric are arguably re-aggregation work). Do not move
    # this back below the coercion. See
    # .cursor/rules/bigquery-tenant-isolation.mdc.
    # ------------------------------------------------------------------
    df = _filter_df_by_tenant_ids(df, tenant_ids)

    # ------------------------------------------------------------------
    # 4. Clean up types (now safe — frame is tenant-scoped)
    # ------------------------------------------------------------------
    numeric_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "dividend_count", "total_return",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in ["first_trade_date", "last_trade_date"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("NaT", "")

    accounts = sorted(df["account"].dropna().unique())
    strategies = sorted(df["strategy"].dropna().unique())
    symbols = sorted(df["symbol"].dropna().unique())
    subsectors = (
        sorted(df["subsector"].dropna().unique())
        if "subsector" in df.columns else []
    )
    sectors = (
        sorted(df["sector"].dropna().unique())
        if "sector" in df.columns else []
    )

    filtered = df.copy()
    # NOTE: no secondary ``account == selected_account`` narrowing here.
    # ``_tenants_for_scope(selected_account)`` already resolved the
    # selected display label (incl. disambiguated colliding labels like
    # "Schwab Account (\u2022\u20226342)") to specific tenant_ids, and the
    # SQL ``tenant_filter`` + ``_filter_df_by_tenant_ids`` already scoped
    # the frame to them. A label-equality filter here would wrongly empty
    # the frame for disambiguated labels (the mart's raw ``account`` is
    # still "Schwab Account").
    if selected_strategy:
        filtered = filtered[filtered["strategy"] == selected_strategy]
    if selected_statuses:
        filtered = filtered[filtered["status"].isin(selected_statuses)]
    if selected_symbol:
        filtered = filtered[filtered["symbol"] == selected_symbol]
    if selected_subsector and "subsector" in filtered.columns:
        filtered = filtered[filtered["subsector"] == selected_subsector]
    if selected_sector and "sector" in filtered.columns:
        filtered = filtered[filtered["sector"] == selected_sector]

    # Status counts for hero chips. Must read from `filtered`, NOT `df`,
    # so the chips agree with the body. Reading from `df` was a long-
    # standing UI lie: the chip said "12 open" even when the user had
    # filtered to one symbol with 1 open position. Hero / body
    # disagreement on the same page is exactly the same bug class as
    # Position Detail's "Strategy Breakdown didn't update" — a
    # sub-aggregation reading from the wrong source.
    status_counts = {"Open": 0, "Closed": 0, "Mixed": 0}
    if "status" in filtered.columns and not filtered.empty:
        vc = filtered["status"].fillna("").value_counts()
        for k in list(status_counts.keys()):
            status_counts[k] = int(vc.get(k, 0))

    # ------------------------------------------------------------------
    # 5. KPIs
    # ------------------------------------------------------------------
    total_winners = int(filtered["num_winners"].sum())
    total_losers = int(filtered["num_losers"].sum())
    total_closed = total_winners + total_losers

    kpis = {
        "total_return": float(filtered["total_return"].sum()),
        "realized_pnl": float(filtered["realized_pnl"].sum()),
        "unrealized_pnl": float(filtered["unrealized_pnl"].sum()),
        "dividend_income": (
            float(filtered["total_dividend_income"].sum())
            if "total_dividend_income" in filtered.columns
            else 0.0
        ),
        "premium_collected": float(filtered["total_premium_received"].sum()),
        "win_rate": total_winners / total_closed if total_closed else 0,
        "num_positions": len(filtered),
        "total_trades": int(filtered["num_individual_trades"].sum()),
        # Closed-trade-group counts. Distinct from total_trades, which sums
        # num_individual_trades (each open + close + roll fill counts). The
        # template's Quick Stats card used to derive winners as
        # total_trades * win_rate, which is wrong: win_rate is the
        # winner-share of *closed groups*, so multiplying by per-fill trade
        # count over-reports winners by 2-3x. Pass the raw counts through
        # and let the template render them directly.
        "num_winners": total_winners,
        "num_losers": total_losers,
        "num_closed_groups": total_closed,
    }

    # ------------------------------------------------------------------
    # 6. Chart data: total P&L by strategy
    # ------------------------------------------------------------------
    strategy_chart = (
        filtered.groupby("strategy")["total_pnl"]
        .sum()
        .sort_values(ascending=True)
        .reset_index()
        .rename(columns={"total_pnl": "pnl"})
        .to_dict(orient="records")
    )

    # ------------------------------------------------------------------
    # 7. Symbol-level summary (grouped by account + symbol)
    # ------------------------------------------------------------------
    if not filtered.empty:
        # Carry sector / subsector through the symbol-level rollup. Each
        # (account, symbol) maps to a single sector/subsector, so 'first' is
        # safe and fast.
        agg_kwargs = dict(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            total_premium_received=("total_premium_received", "sum"),
            total_dividend_income=("total_dividend_income", "sum"),
            total_return=("total_return", "sum"),
            num_individual_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_strategies=("strategy", "nunique"),
            strategies=("strategy", lambda x: ", ".join(sorted(x.unique()))),
        )
        if "sector" in filtered.columns:
            agg_kwargs["sector"] = ("sector", "first")
        if "subsector" in filtered.columns:
            agg_kwargs["subsector"] = ("subsector", "first")
        # Grain on tenant_id (not the broker `account` string) so a symbol
        # held in several physical accounts that share one display label
        # (e.g. 5 "Schwab Account" tenants holding QTUM) shows one row per
        # account instead of collapsing into a single misleading nickname.
        # `account` rides along for display/fallback. Falls back to the
        # account string only if the frame predates the tenant_id grain.
        _sym_grain = (
            (["tenant_id"] if "tenant_id" in filtered.columns else [])
            + ["account", "symbol"]
        )
        symbol_agg = (
            filtered.groupby(_sym_grain)
            .agg(**agg_kwargs)
            .reset_index()
        )
        closed = symbol_agg["num_winners"] + symbol_agg["num_losers"]
        symbol_agg["win_rate"] = symbol_agg["num_winners"] / closed.replace(0, pd.NA)
        symbol_agg["win_rate"] = symbol_agg["win_rate"].fillna(0)
        symbol_agg = symbol_agg.sort_values("total_return", ascending=False)
        symbol_rows = symbol_agg.to_dict(orient="records")
    else:
        symbol_rows = []

    # ------------------------------------------------------------------
    # 8. Strategy detail rows (aggregated by account × strategy, paginated)
    # ------------------------------------------------------------------
    if not filtered.empty:
        # Same tenant_id grain as the symbol rollup above so each physical
        # account's strategy line is distinct (see _sym_grain comment).
        _strat_grain = (
            (["tenant_id"] if "tenant_id" in filtered.columns else [])
            + ["account", "strategy"]
        )
        strat_agg = (
            filtered.groupby(_strat_grain)
            .agg(
                status=("status", lambda xs: "Open" if (xs == "Open").any() else "Closed"),
                total_pnl=("total_pnl", "sum"),
                realized_pnl=("realized_pnl", "sum"),
                unrealized_pnl=("unrealized_pnl", "sum"),
                total_premium_received=("total_premium_received", "sum"),
                total_dividend_income=("total_dividend_income", "sum"),
                total_return=("total_return", "sum"),
                num_individual_trades=("num_individual_trades", "sum"),
                num_winners=("num_winners", "sum"),
                num_losers=("num_losers", "sum"),
                avg_pnl_per_trade=("avg_pnl_per_trade", "mean"),
                avg_days_in_trade=("avg_days_in_trade", "mean"),
            )
            .reset_index()
        )
        closed_ct = strat_agg["num_winners"] + strat_agg["num_losers"]
        strat_agg["win_rate"] = strat_agg["num_winners"] / closed_ct.replace(0, pd.NA)
        strat_agg["win_rate"] = strat_agg["win_rate"].fillna(0)
        strat_agg = strat_agg.sort_values("total_return", ascending=False)
        all_rows = strat_agg.to_dict(orient="records")
    else:
        all_rows = []

    per_page = 25
    total_rows = len(all_rows)
    total_pages = max(1, (total_rows + per_page - 1) // per_page)
    page = min(page, total_pages)
    start_idx = (page - 1) * per_page
    rows = all_rows[start_idx : start_idx + per_page]

    # Resolve a per-row display label off the broker-stable tenant_id so the
    # Account column shows each account's own nickname (Emmory / Sara 401k /
    # ...) rather than the colliding broker `account` string. Falls back to
    # the raw account label when a row carries no tenant_id (admin browsing
    # or pre-grain frames).
    _tenant_labels = _tenant_label_map_for_user(getattr(current_user, "id", None))

    def _label_rows(_rows):
        for _r in _rows:
            _tid = _r.get("tenant_id")
            _r["account_display"] = (
                (_tenant_labels.get(_tid) if _tid else None)
                or _norm_account_label(_r.get("account"))
            )
        return _rows

    _label_rows(rows)
    _label_rows(symbol_rows)

    return render_template(
        "positions.html",
        rows=rows,
        symbol_rows=symbol_rows,
        kpis=kpis,
        strategy_chart=strategy_chart,
        accounts=accounts,
        strategies=strategies,
        symbols=symbols,
        subsectors=subsectors,
        sectors=sectors,
        # `user_accounts` is the auth list (every account the user has
        # linked), used by the hero to decide between "you haven't
        # connected anything yet" and "your filter just returned nothing".
        # `accounts` is the data list (accounts that have positions in the
        # current view) and powers the Account dropdown. Distinct names
        # because they answer different questions.
        user_accounts=user_accounts,
        status_counts=status_counts,
        selected_account=selected_account,
        selected_strategy=selected_strategy,
        selected_statuses=selected_statuses,
        selected_symbol=selected_symbol,
        selected_subsector=selected_subsector,
        selected_sector=selected_sector,
        selected_start_date=selected_start_date,
        selected_end_date=selected_end_date,
        date_filtered=date_filtered,
        page=page,
        total_pages=total_pages,
        total_rows=total_rows,
        per_page=per_page,
        today=date.today(),
        timedelta=timedelta,
    )


# ======================================================================
# Position Detail  (/position/<symbol>)
# ======================================================================

POSITION_SUMMARY_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
    ORDER BY account, strategy
"""

POSITION_TRADES_QUERY = """
    SELECT
        h.account,
        h.tenant_id,
        h.underlying_symbol AS symbol,
        h.trade_date,
        -- Surface DRIPs as their own action so the Raw Transaction Log
        -- and trade-history aggregations can show "I didn't choose to
        -- buy this — Schwab reinvested my dividend" rather than a
        -- chaotic stream of tiny equity_buy fills. Detection lives
        -- in `int_drip_fills` (downstream of `stg_daily_prices` so
        -- stg_history stays out of the price-dependent build pass).
        CASE WHEN d.matched_ex_div_date IS NOT NULL
             THEN 'dividend_reinvest'
             ELSE h.action
        END AS action,
        h.action_raw,
        h.trade_symbol,
        h.instrument_type,
        h.description,
        h.quantity,
        h.price,
        h.fees,
        h.amount,
        (d.matched_ex_div_date IS NOT NULL) AS is_dividend_reinvestment
    FROM `ccwj-dbt.analytics.stg_history` h
    LEFT JOIN `ccwj-dbt.analytics.int_drip_fills` d
        ON  (d.tenant_id IS NOT DISTINCT FROM h.tenant_id)
        AND d.account            = h.account
        AND (d.user_id IS NOT DISTINCT FROM h.user_id)
        AND d.trade_date         = h.trade_date
        AND d.underlying_symbol  = h.underlying_symbol
    WHERE h.trade_date IS NOT NULL
      AND (
        UPPER(TRIM(COALESCE(h.underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
        OR UPPER(TRIM(SPLIT(COALESCE(h.trade_symbol, ''), ' ')[SAFE_OFFSET(0)])) = UPPER(TRIM('{symbol}'))
      )
    {tenant_filter}
    ORDER BY h.trade_date DESC
"""

POSITION_CURRENT_QUERY = """
    SELECT
        account,
        user_id,
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
        unrealized_pnl_pct,
        -- option_expiry / option_strike / option_type are needed by the
        -- chart's live-today override so it can defensively drop any
        -- past-expiry option rows from open-MTM addition (the dbt layer
        -- already filters auto-closed contracts via
        -- int_enriched_current.option_contract_status, but selecting
        -- these columns also lets test fixtures and post-build readers
        -- run the same expiry mask). See _build_chart_from_daily_pnl
        -- and the OTM-at-expiry inference in int_option_contracts.
        option_expiry,
        option_strike,
        option_type
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE UPPER(TRIM(COALESCE(underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

POSITION_CLOSED_LEGS_QUERY = """
    SELECT
        sc.account,
        sc.symbol,
        sc.strategy,
        sc.trade_symbol,
        sc.open_date,
        sc.close_date,
        sc.total_pnl,
        sc.status,
        oc.contracts_sold_to_open + oc.contracts_bought_to_open AS quantity,
        oc.premium_received,
        oc.premium_paid,
        oc.cost_to_close,
        oc.proceeds_from_close,
        oc.direction,
        oc.close_type,
        oc.days_in_trade
    FROM `ccwj-dbt.analytics.int_strategy_classification` sc
    JOIN `ccwj-dbt.analytics.int_option_contracts` oc
      ON (sc.tenant_id IS NOT DISTINCT FROM oc.tenant_id)
     AND sc.account = oc.account
     AND sc.trade_symbol = oc.trade_symbol
     AND sc.user_id IS NOT DISTINCT FROM oc.user_id
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      AND UPPER(TRIM(COALESCE(sc.symbol, ''))) = UPPER(TRIM('{symbol}'))
    {sc_tenant_filter}
"""

POSITION_CLOSED_EQUITY_QUERY = """
    SELECT
        account,
        symbol,
        trade_symbol,
        session_id,
        open_date,
        close_date,
        quantity,
        sale_price_per_share,
        sell_proceeds,
        cost_basis,
        realized_pnl,
        description
    FROM `ccwj-dbt.analytics.int_closed_equity_legs`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

POSITION_LEGS_QUERY = """
    SELECT
        account,
        user_id,
        symbol,
        leg_id,
        leg_type,
        status,
        open_date,
        last_activity_date,
        equity_pnl,
        closed_options_pnl,
        open_options_pnl,
        combined_pnl,
        options_count,
        open_options_count,
        max_quantity_held,
        num_trades,
        options_only,
        display_leg_num,
        days_held
    FROM `ccwj-dbt.analytics.int_position_legs`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
    ORDER BY account, display_leg_num
"""

# Lightweight per-(account,symbol) rollup for the position-detail tab strip.
# Reads `positions_summary` (the precomputed mart) so the tab strip lists
# every symbol the user has ever traded with one trip to BQ. We deliberately
# keep the projection minimal — the strip only needs total_return for the
# pill, num_trades for the title, and a single "Open if any leg open"
# status for the dot. SCOPED with `_account_sql_and` per
# `.cursor/rules/bigquery-tenant-isolation.mdc`; the resulting frame also
# passes through `_filter_df_by_accounts` in Python before serialization.
SYMBOL_TABS_QUERY = """
    SELECT
        account,
        symbol,
        SUM(COALESCE(total_return, 0)) AS total_return,
        SUM(COALESCE(num_individual_trades, 0)) AS num_trades,
        MAX(IF(LOWER(TRIM(COALESCE(status, ''))) = 'open', 1, 0)) AS has_open_leg,
        STRING_AGG(DISTINCT strategy, '|' ORDER BY strategy) AS strategies_pipe
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE symbol IS NOT NULL
      {tenant_filter}
    GROUP BY account, symbol
"""

# Win/Loss matrix cells are PRE-BUCKETED in dbt (mart_option_win_matrix).
# Flask no longer loops over raw contracts to build the DTE x strike grid;
# it just reshapes these aggregated cells into the template's nested dict.
# The mart already restricts to closed contracts with a known strike
# distance (the old ``status='Closed' AND strike_distance IS NOT NULL``).
POSITION_MATRIX_QUERY = """
    SELECT
        account,
        user_id,
        tenant_id,
        underlying_symbol,
        trade_symbol,
        strategy,
        dte_label,
        dte_order,
        strike_col,
        strike_order,
        trade_count,
        wins,
        sum_pnl
    FROM `ccwj-dbt.analytics.mart_option_win_matrix`
    WHERE UPPER(TRIM(COALESCE(underlying_symbol, ''))) = UPPER(TRIM('{symbol}'))
    {tenant_filter}
"""

# Next-earnings date for a single symbol. Symbol-level public market data
# (yfinance via scripts/refresh_earnings_calendar.py) — no account or
# user_id column in stg_earnings_calendar, so no tenant filter is needed
# or possible here. The page itself is already tenant-scoped via the
# other position queries above; this just decorates the hero with
# "next earnings in N days" context.
POSITION_EARNINGS_QUERY = """
    SELECT
        next_earnings_date,
        earnings_window_start,
        earnings_window_end,
        DATE_DIFF(next_earnings_date, CURRENT_DATE(), DAY) AS days_until
    FROM `ccwj-dbt.analytics.stg_earnings_calendar`
    WHERE UPPER(TRIM(symbol)) = UPPER(TRIM('{symbol}'))
      AND next_earnings_date >= CURRENT_DATE()
    ORDER BY next_earnings_date
    LIMIT 1
"""

def _equity_raw_trades_for_partial_close_outcome(
    trades: list,
    *,
    trade_symbol: str,
    account: str,
    session_range,
    close_milestone,
):
    """``int_closed_equity_legs`` is one mart row PER partial sell inside a chapter.
    When attaching ``raw_trades`` for drill-down, include only fills chronological
    through this row's realization date — otherwise each partial shows the SAME
    full session history (duplicate Leg 1 + duplicate buy + later sells visible
    everywhere). JEPI bought 2000 sold 1000 twice was the canonical bug."""
    ts = str(trade_symbol or "").strip()
    acct_o = str(account or "").strip()

    def _row_date(tv):
        try:
            return pd.to_datetime(tv).date()
        except Exception:
            return None

    out = []
    for t in trades or []:
        if str(t.get("instrument_type") or "") != "Equity":
            continue
        if str(t.get("trade_symbol") or "").strip() != ts:
            continue
        if acct_o and str(t.get("account") or "").strip() != acct_o:
            continue
        td = _row_date(t.get("trade_date"))
        if td is None:
            continue
        if session_range and session_range[0]:
            end = session_range[1] or date.today()
            if not (session_range[0] <= td <= end):
                continue
        cm = _row_date(close_milestone) if close_milestone is not None else None
        if cm is not None and td > cm:
            continue
        out.append(t)
    return sorted(out, key=lambda r: str(r.get("trade_date") or ""))


def _merge_position_strategy_breakdown(
    symbol: str,
    summary_df: pd.DataFrame,
    closed_legs_df: pd.DataFrame,
    closed_equity_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return a strategy table that includes any (account, strategy) in closed legs/equity
    missing from positions_summary, so the breakdown matches the Position Legs / history.

    positions_summary is one row per (account, symbol, strategy); in edge cases a closed
    strategy can be absent from the mart while int_strategy_classification still has legs.
    """
    existing = set()
    if summary_df is not None and not summary_df.empty and "account" in summary_df.columns:
        for _, r in summary_df.iterrows():
            a = r.get("account")
            s = r.get("strategy")
            if a is None or (isinstance(s, float) and pd.isna(s)) or s is None:
                continue
            st = str(s).strip()
            if not st:
                continue
            existing.add((str(a).strip(), st))

    def _row_from_option_group(acct: str, strat: str, sub: pd.DataFrame) -> dict:
        total = float(sub["total_pnl"].sum()) if "total_pnl" in sub.columns else 0.0
        prem_r = float(sub["premium_received"].sum()) if "premium_received" in sub.columns else 0.0
        prem_p = float(sub["premium_paid"].sum()) if "premium_paid" in sub.columns else 0.0
        n = len(sub)
        wins = int((sub["total_pnl"] > 0).sum()) if "total_pnl" in sub.columns else 0
        losses = n - wins
        wr = wins / n if n else 0.0
        days_mean = 0.0
        if "days_in_trade" in sub.columns:
            days_mean = float(sub["days_in_trade"].fillna(0).mean() or 0.0)
        od = (
            sub["open_date"].dropna().min() if "open_date" in sub.columns else None
        )
        cd = (
            sub["close_date"].dropna().max() if "close_date" in sub.columns else None
        )
        avg_pnl = total / n if n else 0.0
        return {
            "account": acct,
            "symbol": symbol,
            "strategy": strat,
            "status": "Closed",
            "total_pnl": round(total, 2),
            "realized_pnl": round(total, 2),
            "unrealized_pnl": 0.0,
            "total_premium_received": round(prem_r, 2),
            "total_premium_paid": round(prem_p, 2),
            "num_trade_groups": n,
            "num_individual_trades": n,
            "num_winners": wins,
            "num_losers": losses,
            "win_rate": wr,
            "avg_pnl_per_trade": round(avg_pnl, 2),
            "avg_days_in_trade": round(days_mean, 1),
            "first_trade_date": od,
            "last_trade_date": cd,
            "total_dividend_income": 0.0,
            "dividend_count": 0,
            "total_return": round(total, 2),
        }

    def _row_from_equity_group(acct: str, lbl: str, sub: pd.DataFrame) -> dict:
        real = float(sub["realized_pnl"].sum()) if "realized_pnl" in sub.columns else 0.0
        n = len(sub)
        wins = int((sub["realized_pnl"] > 0).sum()) if "realized_pnl" in sub.columns else 0
        losses = n - wins
        wr = wins / n if n else 0.0
        od = sub["open_date"].dropna().min() if "open_date" in sub.columns else None
        cd = sub["close_date"].dropna().max() if "close_date" in sub.columns else None
        days_mean = 0.0
        for _, er in sub.iterrows():
            o = er.get("open_date")
            c = er.get("close_date")
            if pd.notna(o) and pd.notna(c):
                try:
                    days_mean += (pd.to_datetime(c) - pd.to_datetime(o)).days
                except Exception:
                    pass
        if n:
            days_mean = round(days_mean / n, 1)
        avg_pnl = real / n if n else 0.0
        return {
            "account": acct,
            "symbol": symbol,
            "strategy": lbl,
            "status": "Closed",
            "total_pnl": round(real, 2),
            "realized_pnl": round(real, 2),
            "unrealized_pnl": 0.0,
            "total_premium_received": 0.0,
            "total_premium_paid": 0.0,
            "num_trade_groups": n,
            "num_individual_trades": n,
            "num_winners": wins,
            "num_losers": losses,
            "win_rate": wr,
            "avg_pnl_per_trade": round(avg_pnl, 2),
            "avg_days_in_trade": days_mean,
            "first_trade_date": od,
            "last_trade_date": cd,
            "total_dividend_income": 0.0,
            "dividend_count": 0,
            "total_return": round(real, 2),
        }

    # Equity bucket: positions_summary's "Buy and Hold" row gets reclassified
    # to "Dividend" when dividend income > trade gain — and Coinbase / crypto
    # holdings come through as "Crypto". All three occupy the same
    # equity-strategy slot in the breakdown — only one of them can ever exist
    # for a given (account, symbol). Track which accounts already have one
    # so we don't synthesize a duplicate Buy-and-Hold row alongside a real
    # Dividend / Crypto row from the mart.
    EQUITY_BUCKET = ("Buy and Hold", "Dividend", "Crypto")
    equity_covered_accounts: set[str] = set()
    for acct_existing, strat_existing in existing:
        if strat_existing in EQUITY_BUCKET:
            equity_covered_accounts.add(acct_existing)

    extra: list[dict] = []

    if closed_legs_df is not None and not closed_legs_df.empty and "strategy" in closed_legs_df.columns:
        g = closed_legs_df.copy()
        g = g[g["strategy"].notna() & (g["strategy"].astype(str).str.strip() != "")]
        for (acct, strat), sub in g.groupby(
            [g["account"].astype(str), g["strategy"].astype(str)]
        ):
            acct, strat = str(acct).strip(), str(strat).strip()
            if (acct, strat) in existing:
                continue
            extra.append(_row_from_option_group(acct, strat, sub))
            existing.add((acct, strat))

    # NOTE: `closed_equity_df` is `int_closed_equity_legs`, whose `description`
    # column is the LEG TYPE ("Equity Sold" / "Cost Written Off"), NOT a strategy.
    # Promoting the description into the strategy breakdown was creating spurious
    # rows: a single Buy-and-Hold session would render as three rows in the
    # Strategy Breakdown table (Buy and Hold + Equity Sold + Cost Written Off),
    # each one looking like a separate strategy outcome. The Position Legs section
    # already surfaces individual sells/transfers — the strategy breakdown should
    # stick to one row per real (account, strategy) classification.
    #
    # The original intent was: if positions_summary lacks a row for a closed
    # equity session that is recorded in int_closed_equity_legs, synthesize a
    # "Buy and Hold"-shaped row so the table isn't blank. We preserve that
    # narrow fallback by labeling synthetic equity rows "Buy and Hold" rather
    # than borrowing the leg description.
    if closed_equity_df is not None and not closed_equity_df.empty and "account" in closed_equity_df.columns:
        g = closed_equity_df.copy()
        for acct, sub in g.groupby(g["account"].astype(str)):
            acct = str(acct).strip()
            # Skip if positions_summary already has any equity-bucket row for
            # this account (Buy and Hold or its Dividend reclassification).
            # Otherwise we'd render two rows for the same closed equity session
            # — one "Dividend" with $16k divs, one synthetic "Buy and Hold"
            # with $0 divs — and they'd look like separate strategies.
            if acct in equity_covered_accounts:
                continue
            extra.append(_row_from_equity_group(acct, "Buy and Hold", sub))
            existing.add((acct, "Buy and Hold"))
            equity_covered_accounts.add(acct)

    if not extra:
        return summary_df if summary_df is not None else pd.DataFrame()

    extra_df = pd.DataFrame(extra)
    if summary_df is None or summary_df.empty:
        out = extra_df
    else:
        extra_df = extra_df.reindex(columns=list(summary_df.columns))
        # Drop all-NA columns from extra_df before concat to avoid pandas 2.x
        # FutureWarning about dtype-inferring through empty/all-NA columns.
        extra_df = extra_df.dropna(axis=1, how="all")
        out = pd.concat([summary_df, extra_df], ignore_index=True)

    if "status" in out.columns:
        _open = out["status"].astype(str).str.lower().eq("open")
        out = out.assign(_o=_open)
        if "total_return" in out.columns:
            out = out.sort_values(["_o", "total_return"], ascending=[False, False])
        else:
            out = out.sort_values("_o", ascending=False)
        out = out.drop(columns=["_o"])
    return out


def _fetch_int_strategy_classification_by_symbol(
    client, safe_symbol: str, tenant_ids
) -> pd.DataFrame:
    """User-scoped rows from int_strategy_classification for one symbol. Used when
    positions_summary is empty but we still need strategy breakdown (mart lag / path gaps).
    """
    if tenant_ids is not None and not tenant_ids:
        return pd.DataFrame()
    acct = _tenant_sql_and(tenant_ids)
    sql = f"""
    SELECT
        account, symbol, strategy, status, total_pnl, num_trades, is_winner,
        premium_received, premium_paid, days_in_trade, open_date, close_date
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{safe_symbol}'))
    {acct}
    """
    try:
        df = client.query(sql).to_dataframe()
    except Exception as exc:
        app.logger.exception(
            "int_strategy_classification by symbol failed for %s: %s", safe_symbol, exc
        )
        return pd.DataFrame()
    df = _df_normalize_account_column(df)
    return _filter_df_by_tenant_ids(df, tenant_ids)


def _fetch_closed_option_legs_from_classification(
    client, safe_symbol: str, tenant_ids
) -> pd.DataFrame:
    """Closed option contract rows from int_strategy_classification only (no join).

    POSITION_CLOSED_LEGS joins to int_option_contracts. When that join misses (drift,
    renames, or partial loads), the page loses all closed option history. This query
    matches the P&L in classification and is the same grain as the join: one row per
    closed option trade group.
    """
    if tenant_ids is not None and not tenant_ids:
        return pd.DataFrame()
    acct = _tenant_sql_and(tenant_ids, col="sc.tenant_id")
    sql = f"""
    SELECT
        sc.account,
        sc.symbol,
        sc.strategy,
        sc.trade_symbol,
        sc.open_date,
        sc.close_date,
        sc.total_pnl,
        sc.status,
        CAST(COALESCE(sc.num_trades, 1) AS INT64) AS quantity,
        sc.premium_received,
        sc.premium_paid,
        CAST(NULL AS FLOAT64) AS cost_to_close,
        CAST(NULL AS FLOAT64) AS proceeds_from_close,
        sc.direction,
        sc.close_type,
        sc.days_in_trade
    FROM `ccwj-dbt.analytics.int_strategy_classification` sc
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      AND UPPER(TRIM(COALESCE(sc.symbol, ''))) = UPPER(TRIM('{safe_symbol}'))
    {acct}
    """
    try:
        df = client.query(sql).to_dataframe()
    except Exception as exc:
        app.logger.exception(
            "closed option legs fallback (classification) failed for %s: %s",
            safe_symbol,
            exc,
        )
        return pd.DataFrame()
    df = _df_normalize_account_column(df)
    return _filter_df_by_tenant_ids(df, tenant_ids)


def _rollup_int_strategy_to_summary_shape(cdf: pd.DataFrame) -> pd.DataFrame:
    """Replicate the strategy_summary grain of positions_summary from raw classification rows."""
    if cdf is None or cdf.empty or "account" not in cdf.columns or "strategy" not in cdf.columns:
        return pd.DataFrame()
    cdf = cdf.copy()
    for c in (
        "total_pnl", "num_trades", "premium_received", "premium_paid", "days_in_trade",
    ):
        if c in cdf.columns:
            cdf[c] = pd.to_numeric(cdf[c], errors="coerce").fillna(0.0)
    if "is_winner" in cdf.columns:
        cdf["is_winner"] = cdf["is_winner"].fillna(False).astype(bool)
    else:
        cdf = cdf.assign(is_winner=False)
    if "status" in cdf.columns:
        cdf["_st"] = cdf["status"].astype(str).str.strip().str.lower()
    else:
        cdf["_st"] = "unknown"
    if "symbol" not in cdf.columns:
        return pd.DataFrame()
    out = []
    for (acct, sym, strat), sub in cdf.groupby(
        [cdf["account"].astype(str), cdf["symbol"].astype(str), cdf["strategy"].astype(str)]
    ):
        ssub = sub.copy()
        is_open = ssub["_st"].eq("open")
        n_closed = int((~is_open).sum())
        c_real = float(ssub.loc[~is_open, "total_pnl"].sum()) if n_closed else 0.0
        c_unrl = float(ssub.loc[is_open, "total_pnl"].sum()) if is_open.any() else 0.0
        tot = float(ssub["total_pnl"].sum())
        pcr = float(ssub["premium_received"].sum()) if "premium_received" in ssub else 0.0
        ppd = float(ssub["premium_paid"].sum()) if "premium_paid" in ssub else 0.0
        n_groups = len(ssub)
        n_indiv = int(ssub["num_trades"].sum()) if "num_trades" in ssub else n_groups
        closed_mask = ~is_open
        if "is_winner" in ssub.columns:
            w_m = ssub[closed_mask & ssub["is_winner"]]
            l_m = ssub[closed_mask & ~ssub["is_winner"]]
            n_w = int(len(w_m))
            n_l = int(len(l_m))
        else:
            closed_pn = ssub.loc[closed_mask, "total_pnl"]
            n_w = int((closed_pn > 0).sum())
            n_l = int((closed_pn <= 0).sum())
        win_rate = n_w / (n_w + n_l) if (n_w + n_l) else 0.0
        avg_p = c_real / n_closed if n_closed else 0.0
        avg_d = 0.0
        if "days_in_trade" in ssub.columns:
            avg_d = float(ssub["days_in_trade"].fillna(0).mean() or 0.0)
        ftd, ltd = None, None
        if "open_date" in ssub.columns:
            ftd = ssub["open_date"].min()
        if "close_date" in ssub.columns:
            ltd = ssub["close_date"].max()
        row_status = "Open" if is_open.any() else "Closed"
        out.append(
            {
                "account": str(acct).strip(),
                "symbol": str(sym).strip(),
                "strategy": str(strat).strip(),
                "status": row_status,
                "total_pnl": round(tot, 2),
                "realized_pnl": round(c_real, 2),
                "unrealized_pnl": round(c_unrl, 2),
                "total_premium_received": round(pcr, 2),
                "total_premium_paid": round(ppd, 2),
                "num_trade_groups": n_groups,
                "num_individual_trades": n_indiv,
                "num_winners": n_w,
                "num_losers": n_l,
                "win_rate": win_rate,
                "avg_pnl_per_trade": round(avg_p, 2),
                "avg_days_in_trade": round(avg_d, 1) if avg_d else 0.0,
                "first_trade_date": ftd,
                "last_trade_date": ltd,
                "total_dividend_income": 0.0,
                "dividend_count": 0,
                "total_return": round(tot, 2),
            }
        )
    return pd.DataFrame(out) if out else pd.DataFrame()


def _supplement_summary_with_rolled(
    summary_df: pd.DataFrame, rolled_df: pd.DataFrame
) -> pd.DataFrame:
    """Return summary_df with rows from rolled_df whose (account, strategy) are
    missing. Keeps the mart as source of truth when it has the pair; fills gaps
    from int_strategy_classification so closed history shows up even when the
    mart lags (common right after a Schwab/CSV seed commit, before dbt rebuilds).

    **Equity slot (Buy and Hold / Dividend):** ``positions_summary`` renames a
    top dividend-ranking ``Buy and Hold`` row to strategy label ``Dividend``
    post-aggregation — but rolled rows from ``int_strategy_classification``
    always say ``Buy and Hold``. Supplements previously keyed only on
    ``(account, strategy)``, so they'd add a second equity row with the realized
    P&L while the mart row already folded trade + dividends. That summed to
    ~trade_return + dividends + trade_return in the Strategy Breakdown and
    tripped the reconciliation invariant ($4,312 = exactly the double-count).
    Skip rolling in ``Buy and Hold`` when this account × symbol already has
    *either* label from the mart.
    """
    if rolled_df is None or rolled_df.empty:
        return summary_df if summary_df is not None else pd.DataFrame()
    if summary_df is None or summary_df.empty:
        return rolled_df
    # ``Crypto`` joins the equity-strategy slot for the same reason
    # ``Dividend`` does: it's the rename ``positions_summary`` applies
    # to a ``Buy and Hold`` row whose symbol is on the crypto whitelist
    # (Coinbase via SnapTrade). Without it, a rolled ``Buy and Hold``
    # from ``int_strategy_classification`` (which already says
    # ``Crypto`` for crypto symbols) would supplement on top of the
    # mart's ``Crypto`` row and double-count the realized P&L for
    # BTC / ETH / etc.
    _EQUITY_STRAT_SLOT = frozenset({"Buy and Hold", "Dividend", "Crypto"})
    existing: set[tuple[str, str]] = set()
    equity_slot_covered: set[tuple[str, str]] = set()
    for _, r in summary_df.iterrows():
        a = r.get("account")
        s = r.get("strategy")
        sym = (
            str(r.get("symbol") or "").strip()
            if r.get("symbol") is not None
            else ""
        )
        if a is None or s is None or (isinstance(s, float) and pd.isna(s)):
            continue
        st = str(s).strip()
        if not st:
            continue
        ac = str(a).strip()
        existing.add((ac, st))
        if sym and st in _EQUITY_STRAT_SLOT:
            equity_slot_covered.add((ac, sym))
    mask = []
    for _, r in rolled_df.iterrows():
        a = str(r.get("account") or "").strip()
        s = str(r.get("strategy") or "").strip()
        sym = (
            str(r.get("symbol") or "").strip()
            if r.get("symbol") is not None
            else ""
        )
        if not a or not s:
            mask.append(False)
            continue
        if (a, s) in existing:
            mask.append(False)
            continue
        # Mart already occupies the lone equity-slot row for this symbol.
        if s in _EQUITY_STRAT_SLOT and sym and (a, sym) in equity_slot_covered:
            mask.append(False)
            continue
        mask.append(True)
    add = rolled_df[mask] if mask else rolled_df.iloc[0:0]
    if add.empty:
        return summary_df
    add = add.reindex(columns=list(summary_df.columns))
    add = add.dropna(axis=1, how="all")
    return pd.concat([summary_df, add], ignore_index=True)


def _synthetic_open_strategy_from_current(current_df: pd.DataFrame) -> pd.DataFrame:
    """When there is a live snapshot in int_enriched_current but no mart / classification rows
    (only unrealized in positions_summary or empty), show one Open row so Strategy Breakdown is not empty.
    """
    if current_df is None or current_df.empty:
        return pd.DataFrame()
    from app.upload import is_crypto_symbol
    rows = []
    for _, r in current_df.iterrows():
        acct = str(r.get("account", "") or "").strip()
        it = str(r.get("instrument_type", "") or "")
        sym = str(r.get("symbol", "") or "").strip()
        if it == "Call":
            lab = "Long Call"
        elif it == "Put":
            lab = "Long Put"
        elif it == "Equity":
            # Equity rows for crypto symbols (Coinbase via SnapTrade
            # currently ship as security_type='Equity') get the Crypto
            # label so the strategy breakdown matches what the warehouse
            # would have surfaced via int_strategy_classification.
            lab = "Crypto" if is_crypto_symbol(sym) else "Buy and Hold"
        else:
            lab = "Open"
        u = float(r.get("unrealized_pnl") or 0)
        rows.append(
            {
                "account": acct,
                "symbol": sym,
                "strategy": lab,
                "status": "Open",
                "total_pnl": round(u, 2),
                "realized_pnl": 0.0,
                "unrealized_pnl": round(u, 2),
                "total_premium_received": 0.0,
                "total_premium_paid": 0.0,
                "num_trade_groups": 1,
                "num_individual_trades": 0,
                "num_winners": 0,
                "num_losers": 0,
                "win_rate": 0.0,
                "avg_pnl_per_trade": 0.0,
                "avg_days_in_trade": 0.0,
                "first_trade_date": None,
                "last_trade_date": None,
                "total_dividend_income": 0.0,
                "dividend_count": 0,
                "total_return": round(u, 2),
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _compute_breakdown_by_type(
    *,
    client,
    safe_symbol: str,
    tenant_scope,
    closed_equity_df: pd.DataFrame,
    closed_legs_df: pd.DataFrame,
    current_df: pd.DataFrame,
    leg_predicate,
):
    """Build the Equity / Options / Dividends rollup the position page renders
    above Strategy Breakdown.

    All P&L source frames passed in here are already leg-filtered by the
    caller (closed_legs_df, closed_equity_df by date overlap; current_df is
    cleared in routes.py when no selected leg is Open). For dividends we
    have to do the leg-scope here because there is no per-row dividend
    frame upstream — int_dividend_events is queried directly and filtered
    by ``trade_date`` against ``leg_predicate``.

    leg_predicate: callable(date) -> bool when leg-filtered, else None.
    When None, every dividend event for the symbol counts.

    Returns a list of dict rows ready for Jinja:
        type, total, realized, unrealized, count, count_label, count_open
    Empty list when there is no activity at all (page won't render the card).

    Crypto positions (``safe_symbol`` on the ``CRYPTO_SYMBOLS`` whitelist
    — Coinbase via SnapTrade today) emit a ``Crypto`` row in place of the
    Equity row and suppress the Dividends row. The mechanics of crypto
    holdings on this product are identical to a long equity sit-and-hold
    (buy → hold → sell, no options, no ex-div) so the math is the same;
    relabeling preserves the asset-class signal in the UI. See
    ``app.upload.CRYPTO_SYMBOLS`` and ``stg_crypto_symbols`` for the
    source of truth.

    Numbers should sum to the page-level kpis['total_return'] within
    rounding (positions_summary uses rounded P&L per strategy; the mart's
    open_options unrealized has full precision).
    """
    from app.upload import is_crypto_symbol
    is_crypto = is_crypto_symbol(safe_symbol)
    eq_realized = 0.0
    eq_unrealized = 0.0
    eq_session_count = 0
    eq_open_count = 0
    if closed_equity_df is not None and not closed_equity_df.empty:
        if "realized_pnl" in closed_equity_df.columns:
            eq_realized = float(
                pd.to_numeric(closed_equity_df["realized_pnl"], errors="coerce")
                .fillna(0)
                .sum()
            )
        # int_closed_equity_legs has one row per *closure event* (each sell
        # in a session), not per session. Count distinct session_ids so the
        # UI says "1 session" when a trader sold their PLTR position over
        # three trips, not "3 sessions".
        if "session_id" in closed_equity_df.columns:
            eq_session_count += int(
                closed_equity_df[["account", "session_id"]].drop_duplicates().shape[0]
            )
        else:
            eq_session_count += len(closed_equity_df)
    if current_df is not None and not current_df.empty and "instrument_type" in current_df.columns:
        eq_open = current_df[current_df["instrument_type"] == "Equity"]
        if not eq_open.empty and "unrealized_pnl" in eq_open.columns:
            eq_unrealized = float(
                pd.to_numeric(eq_open["unrealized_pnl"], errors="coerce")
                .fillna(0)
                .sum()
            )
            eq_session_count += len(eq_open)
            eq_open_count += len(eq_open)

    opt_realized = 0.0
    opt_unrealized = 0.0
    opt_count = 0
    opt_open_count = 0
    if closed_legs_df is not None and not closed_legs_df.empty and "total_pnl" in closed_legs_df.columns:
        opt_realized = float(
            pd.to_numeric(closed_legs_df["total_pnl"], errors="coerce")
            .fillna(0)
            .sum()
        )
        opt_count += len(closed_legs_df)
    if current_df is not None and not current_df.empty and "instrument_type" in current_df.columns:
        opt_open = current_df[current_df["instrument_type"].isin(["Call", "Put"])]
        if not opt_open.empty and "unrealized_pnl" in opt_open.columns:
            opt_unrealized = float(
                pd.to_numeric(opt_open["unrealized_pnl"], errors="coerce")
                .fillna(0)
                .sum()
            )
            opt_count += len(opt_open)
            opt_open_count += len(opt_open)

    div_total = 0.0
    div_count = 0
    # Admin (`tenant_scope is None`) must run the query unscoped so
    # `_tenant_sql_and(None)` returns an empty filter and the admin sees
    # every tenant's data — same precedent as the rest of the position page.
    # Pre-fix the `is not None` guard short-circuited admin browsers and
    # `breakdown_rows.Dividends.total = 0` then OVERRODE the correctly-
    # computed Hero `dividend_income` (line ~3216 sync block) with $0,
    # producing the May 2026 JEPI bug: $0 dividends in Hero / Breakdown-
    # by-Type while Strategy Breakdown showed $77,780 (the same data).
    # Empty list `[]` (logged-in user with zero linked accounts) still
    # short-circuits — that's the correct "no data to show" path.
    #
    # Crypto holdings don't pay dividends in our pipeline (no ex-div
    # calendar from yfinance, no broker dividend rows for BTC/ETH/etc.).
    # Skip the query entirely so the breakdown card doesn't render a
    # noisy ``$0 dividends`` row for every crypto position page. If
    # staking yield ever lands as a dividend event we'll revisit.
    if is_crypto:
        pass
    elif tenant_scope is None or len(tenant_scope) > 0:
        try:
            tenant_filter = _tenant_sql_and(tenant_scope)
            div_df = cached_query_df(
                client,
                """
                SELECT account, user_id, symbol, trade_date, amount
                FROM `ccwj-dbt.analytics.int_dividend_events`
                WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
                {tenant_filter}
                """.format(symbol=safe_symbol, tenant_filter=tenant_filter)
            )
            # Belt-and-suspenders tenancy guard. The SQL is already user_id +
            # account scoped via _account_sql_and, but the BQ-tenant rule
            # requires a Python filter on every BQ result before any
            # re-aggregation. See .cursor/rules/bigquery-tenant-isolation.mdc.
            div_df = _filter_df_by_tenant_ids(div_df, tenant_scope)
            if not div_df.empty:
                if leg_predicate is not None and "trade_date" in div_df.columns:
                    div_df = div_df.copy()
                    div_df["_d"] = pd.to_datetime(div_df["trade_date"]).dt.date
                    div_df = div_df[div_df["_d"].apply(leg_predicate)]
                if not div_df.empty and "amount" in div_df.columns:
                    div_total = float(
                        pd.to_numeric(div_df["amount"], errors="coerce")
                        .fillna(0)
                        .sum()
                    )
                    div_count = len(div_df)
        except Exception as exc:
            # Dividends are a nice-to-have on the breakdown; if int_dividend_events
            # is unavailable or schema-drifted, log and show a 0 row rather than
            # crashing the whole position page.
            app.logger.exception(
                "breakdown by-type dividends fetch failed for %s: %s", safe_symbol, exc
            )

    eq_total = eq_realized + eq_unrealized
    opt_total = opt_realized + opt_unrealized

    if (
        eq_session_count == 0
        and opt_count == 0
        and div_count == 0
    ):
        return []

    equity_or_crypto_row = {
        # Relabel the equity row as Crypto for crypto positions. The
        # underlying math is identical (sessions / realized / unrealized
        # all come from the same int_equity_sessions →
        # int_closed_equity_legs path); only the row label changes so
        # the user sees their BTC / ETH / USDC bucketed by asset class
        # instead of fused into "Equity" alongside their VOO and JEPI.
        "type": "Crypto" if is_crypto else "Equity",
        "total": round(eq_total, 2),
        "realized": round(eq_realized, 2),
        "unrealized": round(eq_unrealized, 2),
        "count": eq_session_count,
        "count_label": (
            ("holding" if eq_session_count == 1 else "holdings")
            if is_crypto
            else ("session" if eq_session_count == 1 else "sessions")
        ),
        "count_open": eq_open_count,
    }
    rows = [
        equity_or_crypto_row,
        {
            "type": "Options",
            "total": round(opt_total, 2),
            "realized": round(opt_realized, 2),
            "unrealized": round(opt_unrealized, 2),
            "count": opt_count,
            "count_label": "contract" if opt_count == 1 else "contracts",
            "count_open": opt_open_count,
        },
    ]
    if not is_crypto:
        # Suppress the Dividends row for crypto — we never query for it
        # above (no ex-div feed) and rendering ``$0 dividends`` would
        # be noisy on every BTC / ETH page.
        rows.append({
            "type": "Dividends",
            "total": round(div_total, 2),
            "realized": round(div_total, 2),
            # Dividends are realized cash income — no mark-to-market component,
            # so leave a sentinel the template can render as an em-dash.
            "unrealized": None,
            "count": div_count,
            "count_label": "event" if div_count == 1 else "events",
            "count_open": 0,
        })
    return rows


def _realized_pnl_from_closed_frames(
    closed_legs_df: pd.DataFrame, closed_equity_df: pd.DataFrame
) -> float:
    """Sum realized P&L from closed option contract legs and closed equity lots."""
    r = 0.0
    if (
        closed_legs_df is not None
        and not closed_legs_df.empty
        and "total_pnl" in closed_legs_df.columns
    ):
        r += float(closed_legs_df["total_pnl"].sum())
    if (
        closed_equity_df is not None
        and not closed_equity_df.empty
        and "realized_pnl" in closed_equity_df.columns
    ):
        r += float(closed_equity_df["realized_pnl"].sum())
    return r


def _premium_totals_from_closed_options(closed_legs_df: pd.DataFrame) -> tuple:
    if closed_legs_df is None or closed_legs_df.empty:
        return 0.0, 0.0
    pr = (
        float(closed_legs_df["premium_received"].sum())
        if "premium_received" in closed_legs_df.columns
        else 0.0
    )
    pp = (
        float(closed_legs_df["premium_paid"].sum())
        if "premium_paid" in closed_legs_df.columns
        else 0.0
    )
    return pr, pp


# Pre-aggregated daily P&L data for chart rendering (single symbol)
CHART_DATA_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.mart_daily_pnl`
    WHERE UPPER(TRIM(COALESCE(symbol, ''))) = UPPER(TRIM('{symbol}'))
      {tenant_filter}
    ORDER BY date
"""

# Pre-aggregated daily P&L data for all symbols (account-level charts)
CHART_DATA_ALL_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.mart_daily_pnl`
    WHERE 1=1 {tenant_filter}
    ORDER BY symbol, date
"""


@app.route("/position/<symbol>")
@login_required
def position_detail(symbol):
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()

    # Escape symbol for SQL (prevent injection)
    safe_symbol = symbol.replace("'", "''")

    # `_tenant_sql_and` scopes by broker-stable `tenant_id`; `?account=`
    # maps to tenant_ids via `_tenants_for_scope`.
    selected_account = request.args.get("account", "").strip()
    tenant_scope = _tenants_for_scope(selected_account)

    try:
        _pos_acct = _tenant_sql_and(tenant_scope)
        _pos_sc_acct = _tenant_sql_and(tenant_scope, col="sc.tenant_id")
        # POSITION_TRADES_QUERY joins stg_history (alias h) to int_drip_fills (alias d);
        # both tables have an `account` column so the filter must be scoped to h.
        _pos_h_acct = _tenant_sql_and(tenant_scope, col="h.tenant_id")
        dfs = _bq_parallel(client, {
            "summary": POSITION_SUMMARY_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            "trades": POSITION_TRADES_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_h_acct
            ),
            "current": POSITION_CURRENT_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            "closed_legs": POSITION_CLOSED_LEGS_QUERY.format(
                symbol=safe_symbol, sc_tenant_filter=_pos_sc_acct
            ),
            "closed_equity": POSITION_CLOSED_EQUITY_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            "matrix": POSITION_MATRIX_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            "legs": POSITION_LEGS_QUERY.format(
                symbol=safe_symbol, tenant_filter=_pos_acct
            ),
            # Lightweight all-symbols rollup that powers the symbol tab strip
            # at the top of the page. Scoped by `tenant_scope` so the
            # tabs match the page's account filter (when ?account= is set the
            # strip narrows; otherwise it spans the viewer's accounts).
            "tabs": SYMBOL_TABS_QUERY.format(tenant_filter=_pos_acct),
            # Symbol-level next-earnings date for the hero pill. No account
            # filter — stg_earnings_calendar is symbol-grain public data.
            "earnings": POSITION_EARNINGS_QUERY.format(symbol=safe_symbol),
        })
        summary_df = dfs["summary"]
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        closed_legs_df = dfs["closed_legs"]
        closed_equity_df = dfs["closed_equity"]
        matrix_df = dfs["matrix"]
        legs_df = dfs["legs"]
        tabs_df = dfs["tabs"]
        earnings_df = dfs["earnings"]
        summary_df = _df_normalize_account_column(summary_df)
        trades_df = _df_normalize_account_column(trades_df)
        current_df = _df_normalize_account_column(current_df)
        closed_legs_df = _df_normalize_account_column(closed_legs_df)
        closed_equity_df = _df_normalize_account_column(closed_equity_df)
        matrix_df = _df_normalize_account_column(matrix_df)
        legs_df = _df_normalize_account_column(legs_df)
        tabs_df = _df_normalize_account_column(tabs_df)
    except Exception as exc:
        return render_template(
            "position_detail.html",
            symbol=symbol,
            error=str(exc),
            kpis={},
            strategy_rows=[],
            breakdown_rows=[],
            trades=[],
            trade_outcomes=[],
            current_positions=[],
            option_matrices=[],
            sessions=[],
            selected_legs=[],
            leg_param="",
            chart_data_json="{}",
            has_underlying_price=False,
            symbol_sector="",
            symbol_subsector="",
            symbol_company="",
            symbol_next_earnings=None,
            tabs=[],
            active_symbol=symbol,
            tab_href_base="/position/",
            tab_href_suffix="",
            mode="navigate",
        )

    # Clean numeric types for summary
    num_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "dividend_count", "total_return",
    ]
    for col in num_cols:
        if col in summary_df.columns:
            summary_df[col] = pd.to_numeric(summary_df[col], errors="coerce").fillna(0)

    # Clean trades
    for col in ["amount", "quantity", "price", "fees"]:
        if col in trades_df.columns:
            trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
    if "trade_date" in trades_df.columns:
        trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    # Clean current positions
    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)
    if "unrealized_pnl_pct" in current_df.columns:
        current_df["unrealized_pnl_pct"] = pd.to_numeric(
            current_df["unrealized_pnl_pct"], errors="coerce"
        ).fillna(0)

    # Filter to user's accounts (must run on every BQ frame — queries are by symbol
    # only, so unfiltered closed_legs/closed_equity/matrix would include all tenants.)
    # Use ``tenant_scope`` so admin viewing a non-personal selected_account
    # doesn't strip the just-fetched rows. ``_filter_df_by_accounts`` still
    # enforces the user_id boundary for non-admins.
    summary_df = _filter_df_by_tenant_ids(summary_df, tenant_scope)
    trades_df = _filter_df_by_tenant_ids(trades_df, tenant_scope)
    current_df = _filter_df_by_tenant_ids(current_df, tenant_scope)
    closed_legs_df = _filter_df_by_tenant_ids(closed_legs_df, tenant_scope)
    closed_equity_df = _filter_df_by_tenant_ids(closed_equity_df, tenant_scope)
    matrix_df = _filter_df_by_tenant_ids(matrix_df, tenant_scope)
    # Tab strip data has the same tenancy boundary as everything else above.
    tabs_df = _filter_df_by_tenant_ids(tabs_df, tenant_scope)
    # earnings_df is symbol-grain public market data (no account / user_id
    # columns) so this call is a no-op today — keep it for parity with the
    # rest of the batch and future-proofing if the table ever gains tenancy
    # columns. Per .cursor/rules/bigquery-tenant-isolation.mdc: "no exceptions
    # for 'this query is just for one symbol.'"
    earnings_df = _filter_df_by_tenant_ids(earnings_df, tenant_scope)

    # Joined closed legs are empty: int_option_contracts can fail to match while
    # int_strategy_classification still has closed option P&L — use classification only.
    if closed_legs_df.empty and (
        tenant_scope is None
        or (isinstance(tenant_scope, list) and len(tenant_scope) > 0)
    ):
        _cl_sup = _fetch_closed_option_legs_from_classification(
            client, safe_symbol, tenant_scope
        )
        if not _cl_sup.empty:
            closed_legs_df = _cl_sup
            for col in ["total_pnl", "premium_received", "premium_paid", "days_in_trade"]:
                if col in closed_legs_df.columns:
                    closed_legs_df[col] = pd.to_numeric(
                        closed_legs_df[col], errors="coerce"
                    ).fillna(0)

    # Optional filters carried from Positions page
    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")
    selected_statuses = request.args.getlist("status")
    selected_start_date = request.args.get("start_date", "")
    selected_end_date = request.args.get("end_date", "")

    start_date = _parse_date(selected_start_date)
    end_date = _parse_date(selected_end_date)

    # No secondary ``account == selected_account`` narrowing: tenant scope
    # (resolved from the selected display label, incl. disambiguated
    # colliding labels) already filtered these frames by tenant_id.
    if not current_df.empty:
        current_df = _dedupe_enriched_current_positions(current_df)
    if selected_strategy:
        if "strategy" in summary_df.columns:
            summary_df = summary_df[summary_df["strategy"] == selected_strategy]
        if "strategy" in trades_df.columns:
            trades_df = trades_df[trades_df["strategy"] == selected_strategy]
    if selected_statuses and "status" in summary_df.columns:
        summary_df = summary_df[summary_df["status"].isin(selected_statuses)]
    if start_date is not None and "trade_date" in trades_df.columns:
        trades_df = trades_df[trades_df["trade_date"] >= start_date]
    if end_date is not None and "trade_date" in trades_df.columns:
        trades_df = trades_df[trades_df["trade_date"] <= end_date]

    # ── Position legs (read from int_position_legs mart) ──
    # The mart owns the canonical leg definition (equity sessions + option-only
    # orphan legs, with Open status whenever any attached option is still live
    # so the pill agrees with the banner). _legs_df_to_sessions_list reshapes
    # the mart rows into the legacy dict shape the template + downstream
    # helpers consume, preserving the leg_id ↔ session_id contract that keeps
    # bookmarked ?leg=<n> URLs working.
    legs_df = _filter_df_by_tenant_ids(legs_df, tenant_scope)
    # Tenant scope already narrowed legs to the selected account's tenant.

    sessions_list = _legs_df_to_sessions_list(legs_df)

    leg_param = request.args.get("leg", "")
    if leg_param:
        selected_legs = []
        for x in leg_param.split(","):
            x = x.strip()
            try:
                selected_legs.append(int(x))
            except ValueError:
                pass
    else:
        selected_legs = [s["session_id"] for s in sessions_list]

    # Build date ranges for selected sessions
    _leg_ranges = []
    _has_open_leg = False
    for s in sessions_list:
        if s["session_id"] in selected_legs:
            od = pd.to_datetime(s["open_date"]).date() if s["open_date"] else None
            ltd = pd.to_datetime(s["last_trade_date"]).date() if s["last_trade_date"] else None
            is_open = str(s.get("status", "")).strip().lower() == "open"
            if is_open:
                _has_open_leg = True
            _leg_ranges.append((od, ltd if not is_open else date.today()))

    def _in_leg_range(d):
        """Return True if date d falls within any selected leg's date range."""
        if not _leg_ranges:
            return True
        for lo, hi in _leg_ranges:
            if lo and hi and lo <= d <= hi:
                return True
            if lo and not hi and d >= lo:
                return True
        return False

    # Snapshot before leg filter so hero + chart can use full symbol history
    # when the selected leg has no trade rows in-range (common for new option legs).
    trades_pre_leg = trades_df.copy()

    # Apply leg filter to trades
    if leg_param and "trade_date" in trades_df.columns and _leg_ranges:
        trades_df = trades_df[trades_df["trade_date"].apply(_in_leg_range)]

    # Apply leg filter to current positions (only show if an open leg is selected)
    if leg_param and not _has_open_leg:
        current_df = current_df.iloc[0:0]

    # For open equity positions: if cost_basis is missing/zero, derive from trade history
    # so unrealized P&L = market_value - cost_basis (true P/L for open positions)
    if not current_df.empty and not trades_df.empty and "action" in trades_df.columns:
        for idx, row in current_df.iterrows():
            if row.get("instrument_type") != "Equity":
                continue
            cost_basis = float(row.get("cost_basis") or 0)
            market_value = float(row.get("market_value") or 0)
            if market_value <= 0:
                continue
            if cost_basis is None or cost_basis == 0:
                acct, sym = row.get("account"), row.get("symbol")
                buys = trades_df[
                    (trades_df["account"] == acct)
                    & (trades_df["symbol"] == sym)
                    & (trades_df["action"].astype(str).str.lower().str.strip() == "buy")
                ]
                if not buys.empty:
                    cost_basis = abs(float(buys["amount"].sum()))
                    current_df.at[idx, "cost_basis"] = cost_basis
                    current_df.at[idx, "unrealized_pnl"] = market_value - cost_basis
                    if cost_basis:
                        current_df.at[idx, "unrealized_pnl_pct"] = 100.0 * (market_value - cost_basis) / cost_basis

    # ── Filter closed legs early so KPIs can use them ──
    # (tenant scope already narrowed to the selected account's tenant)
    if selected_strategy and not closed_legs_df.empty and "strategy" in closed_legs_df.columns:
        closed_legs_df = closed_legs_df[closed_legs_df["strategy"] == selected_strategy]
    # Defense in depth: drop ``int_closed_equity_legs`` "Cost Written Off"
    # rows when ``int_enriched_current`` still shows the symbol held on
    # the same account. dbt's ``account_symbol_holdings`` suppression is
    # the source of truth, but BigQuery may serve stale rows until the
    # next ``dbt build``. Doing the same suppression here prevents
    # phantom writeoffs from poisoning legs / breakdown / chart-substitute
    # in the meantime. See ``_drop_phantom_equity_writeoffs`` doc.
    pre_strip_n = len(closed_equity_df)
    closed_equity_df, _stripped_writeoffs = _drop_phantom_equity_writeoffs(
        closed_equity_df, current_df
    )
    if len(closed_equity_df) != pre_strip_n:
        try:
            app.logger.info(
                "position_detail: stripped %d phantom Cost Written Off "
                "row(s) for %s/%s — broker still holds the shares; "
                "dbt int_closed_equity_legs.account_symbol_holdings will "
                "make this redundant after the next build.",
                pre_strip_n - len(closed_equity_df),
                selected_account or "ALL",
                safe_symbol,
            )
        except Exception:
            pass
        # ``positions_summary`` aggregates the same phantom row into a
        # Closed strategy rollup. Reverse it so Strategy Breakdown
        # agrees with Position Legs + Breakdown by Type until dbt
        # rebuilds and the source row is gone.
        summary_df = _addback_phantom_writeoffs_to_summary(
            summary_df, _stripped_writeoffs
        )
    # Before leg scoping, keep copies for "first/last activity" on the page
    closed_legs_pre_leg = closed_legs_df.copy()
    closed_equity_pre_leg = closed_equity_df.copy()
    if leg_param and _leg_ranges:
        if not closed_legs_df.empty and "open_date" in closed_legs_df.columns:
            closed_legs_df["_od"] = pd.to_datetime(closed_legs_df["open_date"]).dt.date
            closed_legs_df = closed_legs_df[closed_legs_df["_od"].apply(_in_leg_range)]
            closed_legs_df = closed_legs_df.drop(columns=["_od"])
        # Equity session leg-filter: use open_date overlap, NOT session_id.
        # int_closed_equity_legs.session_id is the int_equity_sessions
        # session number (1, 2, ...), which used to also be our leg pill's
        # session_id. Under the merged-interval int_position_legs the pill
        # leg_id is sequential per merged chapter and may not equal the
        # equity session_id at all (a single equity session can be merged
        # into a leg labeled 2 because an earlier orphan-options leg got
        # leg_id 1). Filtering by session_id collisions used to spill the
        # equity session into the wrong leg's tables — visible bug for
        # PLTR / Cameron Investment ?leg=1 (Buy and Hold appeared in the
        # Nov 2024 orphan leg's strategy table).
        if not closed_equity_df.empty and "open_date" in closed_equity_df.columns:
            closed_equity_df["_od"] = pd.to_datetime(closed_equity_df["open_date"]).dt.date
            closed_equity_df = closed_equity_df[closed_equity_df["_od"].apply(_in_leg_range)]
            closed_equity_df = closed_equity_df.drop(columns=["_od"])

    # Min/max activity for hero + chart when summary/leg filter hides dates (e.g. open option leg).
    _activity_all_dates = _collect_activity_candidate_dates(
        trades_pre_leg, closed_legs_pre_leg, closed_equity_pre_leg, sessions_list
    )
    _activity_date_min = min(_activity_all_dates) if _activity_all_dates else None
    _activity_date_max = max(_activity_all_dates) if _activity_all_dates else None

    # Status (needed for open-only realized logic)
    status_col = None
    for c in ("status", "Status", "STATUS"):
        if c in (summary_df.columns if not summary_df.empty else []):
            status_col = c
            break
    statuses = summary_df[status_col].unique().tolist() if status_col and not summary_df.empty else []
    _has_open = any(str(s).strip().lower() == "open" for s in statuses if s is not None)
    _has_closed = any(str(s).strip().lower() == "closed" for s in statuses if s is not None)
    # Open equity/options from snapshots have no positions_summary row until trades exist in stg_history.
    if not _has_open and not current_df.empty:
        _has_open = True
    if _has_open:
        overall_status = "Open"
    else:
        overall_status = "Closed"

    # When leg filter is active, override overall_status based on selected sessions
    if leg_param:
        overall_status = "Open" if _has_open_leg else "Closed"

    # ── KPIs and Strategy Rows ──
    # When leg filter is active, recompute from filtered trade data instead of summary_df
    if leg_param and _leg_ranges:
        # Filter summary_df by date overlap with selected leg ranges
        if not summary_df.empty and "first_trade_date" in summary_df.columns:
            summary_df["_ftd"] = pd.to_datetime(summary_df["first_trade_date"]).dt.date
            summary_df = summary_df[summary_df["_ftd"].apply(_in_leg_range)]
            summary_df = summary_df.drop(columns=["_ftd"])

    total_winners = int(summary_df["num_winners"].sum()) if not summary_df.empty else 0
    total_losers = int(summary_df["num_losers"].sum()) if not summary_df.empty else 0
    total_closed = total_winners + total_losers

    _sell_actions = ("equity_sell", "option_sell_to_close", "option_buy_to_close")
    has_sell_trades = (
        not trades_df.empty
        and "action" in trades_df.columns
        and trades_df["action"].astype(str).str.strip().isin(_sell_actions).any()
    )
    # True only for snapshot-only / no-history edge cases (not "any open position").
    is_open_only = (total_closed == 0 and not current_df.empty) or (
        not has_sell_trades and not current_df.empty
    )

    if leg_param and _leg_ranges:
        realized_for_display = _realized_pnl_from_closed_frames(
            closed_legs_df, closed_equity_df
        )
    else:
        has_closed_frame = (not closed_legs_pre_leg.empty) or (
            not closed_equity_pre_leg.empty
        )
        if has_closed_frame:
            realized_for_display = _realized_pnl_from_closed_frames(
                closed_legs_pre_leg, closed_equity_pre_leg
            )
        else:
            realized_for_display = (
                float(summary_df["realized_pnl"].sum()) if not summary_df.empty else 0.0
            )

    if app.debug and symbol == "ATZAF":
        app.logger.warning(
            "position_detail ATZAF: status_col=%s overall_status=%s total_closed=%s is_open_only=%s realized_for_display=%s",
            status_col, overall_status, total_closed, is_open_only, realized_for_display,
        )

    kpis = {}
    # positions_summary is trade-derived; open lots synced without matching history have current_df only.
    _show_position_kpis = (
        leg_param
        or not summary_df.empty
        or not current_df.empty
        or not trades_df.empty
    )
    if _show_position_kpis:
        # Prefer positions_summary's unrealized_pnl when we have it — it is trade-derived
        # and rolls up *every* open leg (equity + each option contract). int_enriched_current
        # can be partial for a symbol (e.g. broker positions feed has the open option but not
        # the long stock, or vice versa) which is what was making the hero disagree with the
        # strategy-breakdown row underneath it. Only fall back to current_df when summary is
        # empty (positions imported with no transaction history at all).
        if not summary_df.empty and "unrealized_pnl" in summary_df.columns:
            unrealized_from_summary = float(summary_df["unrealized_pnl"].sum())
        elif not current_df.empty and "unrealized_pnl" in current_df.columns:
            unrealized_from_summary = float(current_df["unrealized_pnl"].sum())
        else:
            unrealized_from_summary = 0.0

        # When leg-filtered, premium = filtered closed options only (never full-history
        # legs when the filtered frame is empty for that range).
        if leg_param and _leg_ranges:
            if not closed_legs_df.empty:
                pr, pp = _premium_totals_from_closed_options(closed_legs_df)
            else:
                pr, pp = 0.0, 0.0
            premium_collected, premium_paid = pr, pp
        else:
            pr, pp = _premium_totals_from_closed_options(closed_legs_pre_leg)
            premium_collected, premium_paid = pr, pp
            if (premium_collected == 0.0 and premium_paid == 0.0) and not summary_df.empty:
                premium_collected = float(summary_df["total_premium_received"].sum())
                premium_paid = float(summary_df["total_premium_paid"].sum())

        # Trade count: use row count when summary is empty (e.g. Schwab positions-only path)
        if leg_param or summary_df.empty:
            trade_count = len(trades_df)
            if trade_count == 0 and not trades_pre_leg.empty:
                trade_count = len(trades_pre_leg)
        else:
            trade_count = int(
                summary_df["num_individual_trades"].sum()
            ) if "num_individual_trades" in summary_df.columns else 0
            if trade_count == 0 and not trades_pre_leg.empty:
                trade_count = len(trades_pre_leg)

        # Date range: prefer stg (trades_pre_leg) when present — positions_summary can lag
        # and show 0 trades + a bogus same-day "first" and "last" as-of stamp.
        if leg_param and not trades_df.empty and "trade_date" in trades_df.columns:
            first_trade = str(trades_df["trade_date"].min())[:10]
            last_trade = str(trades_df["trade_date"].max())[:10]
        elif (not leg_param) and (not trades_pre_leg.empty) and "trade_date" in trades_pre_leg.columns:
            first_trade = str(trades_pre_leg["trade_date"].min())[:10]
            last_trade = str(trades_pre_leg["trade_date"].max())[:10]
        elif not summary_df.empty and "first_trade_date" in summary_df.columns:
            first_trade = str(pd.to_datetime(summary_df["first_trade_date"].min()).date())
            last_trade = str(pd.to_datetime(summary_df["last_trade_date"].max()).date())
        elif not trades_df.empty and "trade_date" in trades_df.columns:
            first_trade = str(trades_df["trade_date"].min())[:10]
            last_trade = str(trades_df["trade_date"].max())[:10]
        else:
            first_trade = ""
            last_trade = ""
        if (not first_trade) and _activity_date_min is not None:
            first_trade = str(_activity_date_min)
        if (not last_trade) and _activity_date_max is not None:
            last_trade = str(_activity_date_max)

        # Open, still no real range (e.g. only summary as-of) — session open + through today
        if (
            not leg_param
            and overall_status == "Open"
            and sessions_list
            and (not first_trade or (first_trade == last_trade and trade_count == 0))
        ):
            ods = []
            for s in sessions_list:
                if str(s.get("status", "")).strip().lower() == "open" and s.get("open_date"):
                    try:
                        ods.append(pd.to_datetime(s["open_date"]).date())
                    except Exception:
                        pass
            if ods:
                d0 = min(ods)
                first_trade = str(d0)[:10]
                last_trade = str(date.today())

        # Stg row count for hero; if summary says 0 trades but legs exist, show leg count.
        _fills = len(trades_pre_leg) if not trades_pre_leg.empty else 0
        _n_legs = (
            (len(closed_legs_pre_leg) if not closed_legs_pre_leg.empty else 0)
            + (len(closed_equity_pre_leg) if not closed_equity_pre_leg.empty else 0)
            + (len(current_df) if not current_df.empty else 0)
        )
        if _fills > 0:
            trade_count = _fills
        elif trade_count == 0 and _n_legs > 0:
            trade_count = _n_legs

        # Win/loss: from filtered closed legs when leg-filtered; otherwise from all
        # symbol closed legs (positions_summary is wrong when open rows mask closed stats).
        if leg_param and _leg_ranges:
            opt_wins = int((closed_legs_df["total_pnl"] > 0).sum()) if not closed_legs_df.empty and "total_pnl" in closed_legs_df.columns else 0
            opt_losses = int((closed_legs_df["total_pnl"] <= 0).sum()) if not closed_legs_df.empty and "total_pnl" in closed_legs_df.columns else 0
            eq_wins = int((closed_equity_df["realized_pnl"] > 0).sum()) if not closed_equity_df.empty and "realized_pnl" in closed_equity_df.columns else 0
            eq_losses = int((closed_equity_df["realized_pnl"] <= 0).sum()) if not closed_equity_df.empty and "realized_pnl" in closed_equity_df.columns else 0
            total_winners = opt_wins + eq_wins
            total_losers = opt_losses + eq_losses
            total_closed = total_winners + total_losers
        elif (not closed_legs_pre_leg.empty) or (not closed_equity_pre_leg.empty):
            opt_wins = int((closed_legs_pre_leg["total_pnl"] > 0).sum()) if not closed_legs_pre_leg.empty and "total_pnl" in closed_legs_pre_leg.columns else 0
            opt_losses = int((closed_legs_pre_leg["total_pnl"] <= 0).sum()) if not closed_legs_pre_leg.empty and "total_pnl" in closed_legs_pre_leg.columns else 0
            eq_wins = int((closed_equity_pre_leg["realized_pnl"] > 0).sum()) if not closed_equity_pre_leg.empty and "realized_pnl" in closed_equity_pre_leg.columns else 0
            eq_losses = int((closed_equity_pre_leg["realized_pnl"] <= 0).sum()) if not closed_equity_pre_leg.empty and "realized_pnl" in closed_equity_pre_leg.columns else 0
            total_winners = opt_wins + eq_wins
            total_losers = opt_losses + eq_losses
            total_closed = total_winners + total_losers

        avg_days_val = float(summary_df["avg_days_in_trade"].mean()) if not summary_df.empty else 0.0
        if pd.isna(avg_days_val):
            avg_days_val = 0.0
        if (not closed_legs_pre_leg.empty) and "days_in_trade" in closed_legs_pre_leg.columns:
            d_alt = float(closed_legs_pre_leg["days_in_trade"].fillna(0).mean() or 0.0)
            if d_alt > 0 and avg_days_val == 0.0:
                avg_days_val = d_alt

        div_income = (
            float(summary_df["total_dividend_income"].sum()) if not summary_df.empty else 0.0
        )

        kpis = {
            "total_return": realized_for_display + unrealized_from_summary + div_income,
            "realized_pnl": realized_for_display,
            "unrealized_pnl": unrealized_from_summary,
            "premium_collected": premium_collected,
            "premium_paid": premium_paid,
            "dividend_income": div_income,
            "win_rate": total_winners / total_closed if total_closed else 0,
            "avg_days": avg_days_val,
            "total_trades": trade_count,
            "num_winners": total_winners,
            "num_losers": total_losers,
            "first_trade": first_trade,
            "last_trade": last_trade,
        }

    # Strategy rows.
    #
    # Two distinct data paths because the question "what are my strategy
    # results?" has different right answers depending on scope:
    #
    #  • No leg filter (whole symbol) — positions_summary is the source of
    #    truth, supplemented from int_strategy_classification when the mart
    #    lags by a dbt run (common right after a Schwab/CSV seed commit).
    #
    #  • Leg-filtered — positions_summary CANNOT be used. It aggregates per
    #    (account, symbol, strategy) across the entire symbol history, so its
    #    per-strategy P&L, trade count, win-rate are full-symbol numbers
    #    that don't move when you click a leg pill (which is exactly the
    #    "Strategy Breakdown didn't update" bug). Rebuild the strategy
    #    rollup from int_strategy_classification rows whose open_date falls
    #    inside the selected leg(s) — same grain as positions_summary, but
    #    scoped correctly. Also skip the supplement step (it would re-inject
    #    full-history numbers).
    #
    # See `_compute_breakdown_by_type`'s gate comment — `is None` means
    # admin and must NOT short-circuit.
    # Empty list still short-circuits (genuine "no tenants" state).
    if leg_param and _leg_ranges:
        summary_for_strat = pd.DataFrame()
        if tenant_scope is None or len(tenant_scope) > 0:
            int_raw = _fetch_int_strategy_classification_by_symbol(
                client, safe_symbol, tenant_scope
            )
            if not int_raw.empty and "open_date" in int_raw.columns:
                int_raw = int_raw.copy()
                int_raw["_od"] = pd.to_datetime(int_raw["open_date"]).dt.date
                int_raw = int_raw[int_raw["_od"].apply(_in_leg_range)].drop(
                    columns=["_od"]
                )
                if not int_raw.empty:
                    summary_for_strat = _rollup_int_strategy_to_summary_shape(int_raw)
    else:
        summary_for_strat = summary_df
        if tenant_scope is None or len(tenant_scope) > 0:
            int_raw = _fetch_int_strategy_classification_by_symbol(
                client, safe_symbol, tenant_scope
            )
            if not int_raw.empty:
                rolled = _rollup_int_strategy_to_summary_shape(int_raw)
                if not rolled.empty:
                    summary_for_strat = _supplement_summary_with_rolled(
                        summary_for_strat, rolled
                    )
    _cl_for_strat = closed_legs_pre_leg if not leg_param else closed_legs_df
    _eq_for_strat = closed_equity_pre_leg if not leg_param else closed_equity_df
    merged_strategy_df = _merge_position_strategy_breakdown(
        safe_symbol, summary_for_strat, _cl_for_strat, _eq_for_strat
    )
    if merged_strategy_df.empty and not current_df.empty:
        syn = _synthetic_open_strategy_from_current(current_df)
        if not syn.empty:
            merged_strategy_df = syn
    strategy_rows = (
        merged_strategy_df.to_dict(orient="records")
        if not merged_strategy_df.empty
        else []
    )

    # Disambiguate the Strategy Breakdown's Account column by tenant_id.
    # N physical accounts can share one broker `account` string (e.g. 5
    # SnapTrade "Schwab Account" tenants), so labeling off the account
    # string collapses them all to a single nickname (last-write-wins in
    # `_account_label_map`) — the page then renders 5 identical-looking
    # "Sara Investment" rows for what are really 5 distinct accounts
    # (Emmory / Sara 401k / Sara Investment / Cameron Investment /
    # Cameron 401k). Resolve the label off the broker-stable tenant_id so
    # each row shows its own nickname; fall back to the raw account label
    # when no per-tenant mapping exists (admin browsing, synthesized
    # cross-tenant closed rows that carry no tenant_id).
    _tenant_labels = _tenant_label_map_for_user(getattr(current_user, "id", None))
    for _sr in strategy_rows:
        _tid = _sr.get("tenant_id")
        _lbl = _tenant_labels.get(_tid) if _tid else None
        _sr["account_display"] = _lbl or _norm_account_label(_sr.get("account"))

    # ── Breakdown by type (equity / options / dividends) ──
    # Sums roll up across the selected legs (or the whole symbol when no
    # leg filter is active). Sources:
    #   - equity realized:    closed_equity_df (already leg-filtered)
    #   - equity unrealized:  current_df rows where instrument_type='Equity'
    #   - options realized:   closed_legs_df (already leg-filtered)
    #   - options unrealized: current_df rows where instrument_type in (Call, Put)
    #   - dividends:          int_dividend_events filtered by leg date range
    # See _compute_breakdown_by_type for the full contract.
    breakdown_rows = _compute_breakdown_by_type(
        client=client,
        safe_symbol=safe_symbol,
        tenant_scope=tenant_scope,
        closed_equity_df=closed_equity_df,
        closed_legs_df=closed_legs_df,
        current_df=current_df,
        leg_predicate=(_in_leg_range if (leg_param and _leg_ranges) else None),
    )

    # Headline KPI used ``Σ positions_summary.total_dividend_income`` + realized
    # frames + unreal — but Breakdown-by-type / mart chart fold dividends from
    # ``int_dividend_events`` (synthesised ex-div × holdings etc.). Those streams
    # can materially diverge (~12k on BE Schwab •••0044): hero read low while
    # ledger + chart agreed. Pin hero ``total_return`` to the same Σ as the card
    # above Strategy Breakdown so reconciliation and user trust aren't split.
    if kpis and breakdown_rows:
        ledger_total = sum(float(r.get("total") or 0) for r in breakdown_rows)
        kpis["total_return"] = round(ledger_total, 2)
        for _br in breakdown_rows:
            if str(_br.get("type") or "") == "Dividends":
                kpis["dividend_income"] = round(float(_br.get("total") or 0), 2)
                break

    # Build chart data from pre-aggregated mart_daily_pnl
    chart_data = {"dates": [], "equity": [], "options": [], "dividends": [], "total": [], "underlying_price": [], "has_underlying_price": False}
    prices_through_date = None
    try:
        tenant_filter = _tenant_sql_and(_tenants_for_scope(selected_account))
        chart_df = cached_query_df(
            client,
            CHART_DATA_QUERY.format(symbol=safe_symbol, tenant_filter=tenant_filter)
        )
        chart_df = _filter_df_by_tenant_ids(chart_df, tenant_scope)
        chart_df = _narrow_mart_daily_pnl_chart_df_to_summary_tenant(
            chart_df, summary_df
        )
        # Filter chart data by selected session date ranges and re-zero cumulative columns
        if leg_param and _leg_ranges and not chart_df.empty and "date" in chart_df.columns:
            chart_df["_d"] = pd.to_datetime(chart_df["date"]).dt.date
            chart_df = chart_df[chart_df["_d"].apply(_in_leg_range)].copy()
            chart_df = chart_df.drop(columns=["_d"])
            if not chart_df.empty:
                # Re-zero cumulative columns relative to the leg's
                # first day so the chart starts at $0 inside the
                # filtered window. ``cumulative_options_pnl`` is now
                # realize-on-close cumulative (see mart_daily_pnl
                # header) — its baseline subtraction still produces a
                # well-defined "delta during this leg" series.
                for cum_col in (
                    "cumulative_options_pnl",
                    "cumulative_dividends_pnl",
                    "cumulative_other_pnl",
                ):
                    if cum_col in chart_df.columns:
                        baseline = float(chart_df[cum_col].iloc[0] or 0)
                        chart_df[cum_col] = chart_df[cum_col].astype(float) - baseline
                # Open MTM and snapshot diagnostics cover ALL open
                # options for the symbol, not just those in the
                # selected leg. Zero them out so the chart's
                # within-leg series isn't inflated by other legs'
                # open contracts. Realized contributions inside the
                # leg window are still attributed via the rezeroed
                # cumulative.
                for col in (
                    "open_options_unrealized_pnl",
                    "option_market_value",
                    "option_cost_basis",
                ):
                    if col in chart_df.columns:
                        chart_df[col] = 0 if col == "open_options_unrealized_pnl" else None
        if not chart_df.empty:
            # Cache the computed chart payload keyed on the (tenant- and
            # leg-scoped) input frames + today. The equity P&L walk is a
            # heavy row-by-row Python state machine; on a warm cache we skip
            # it and only pay the vectorized fingerprint hash. Tenant-safe:
            # the key is a content hash of the already tenant-scoped inputs.
            _chart_key = (
                "pos_chart",
                str(date.today()),
                frame_fingerprint(chart_df, current_df),
            )
            with timed("chart"):
                chart_data = cached_payload(
                    _chart_key,
                    lambda: _build_chart_from_daily_pnl(chart_df, current_df),
                )
            # Latest date we have close_price for (from pipeline); user can run current_position_stock_price.py to refresh
            if "date" in chart_df.columns:
                prices_through_date = str(chart_df["date"].max())[:10]
    except Exception as exc:
        app.logger.exception(
            "position_detail chart query or build failed for %s: %s", safe_symbol, exc
        )

    # Prefer stg/leg when mart is unusably short — but NEVER replace a mart chart
    # whose terminal agrees with KPI with ``_cumulative_pnl_from_*`` substitutes.
    #
    # Those substitutes are legacy cash-close stepping (only closed legs / raw
    # stg HISTORY amounts): they omit open unrealized MTM, realize-on-close option
    # shape, ``int_dividend_events``, etc. After a Schwab sync, ``trades_pre_leg``
    # often spans *more calendar days than mart_daily_pnl* while the mart spine
    # still reconciles KPI + breakdown. The naive rule ``best_n > n_m`` then
    # threw away the correct mart series (~\$85k) for a truncated cash ladder
    # (~\$20k) — reconciliation invariant explosion (May 2026 BE).
    _chart_dates = chart_data.get("dates") or []
    n_m = len(_chart_dates)
    kp_ref = float(kpis.get("total_return") or 0) if kpis else None
    mart_term = _chart_data_terminal(chart_data)

    ch_stg = (
        _cumulative_pnl_from_stg_trades(trades_pre_leg, current_df)
        if not trades_pre_leg.empty else None
    )
    n_stg = len(ch_stg["dates"]) if ch_stg and ch_stg.get("dates") else 0
    ch_leg = _cumulative_pnl_from_leg_closes(closed_legs_pre_leg, closed_equity_pre_leg)
    n_leg = len(ch_leg["dates"]) if ch_leg and ch_leg.get("dates") else 0

    cands_src = []
    if ch_leg and n_leg >= 2:
        cands_src.append(("leg", ch_leg, n_leg))
    if ch_stg and n_stg >= 2:
        cands_src.append(("stg", ch_stg, n_stg))

    if cands_src:
        # Tie-break: prefer candidates with more x-points, leg path over stg.
        cands_src.sort(key=lambda t: (-t[2], 0 if t[0] == "leg" else 1))
        _, cand_data, best_n = cands_src[0]
        cand_term = _chart_data_terminal(cand_data)
        mart_useless = n_m <= 2
        substitute = False

        if mart_useless:
            # Mart spine is insufficient — pick whichever substitute lands closest to
            # KPI (prefer longer tie-break among equally-close substitutes).
            if kp_ref is not None:
                scored = []
                for _nm, cd, bn in cands_src:
                    g = abs(_chart_data_terminal(cd) - kp_ref)
                    scored.append((g, -bn, 0 if _nm == "leg" else 1, cd))
                scored.sort(key=lambda z: z[:3])
                chart_data = scored[0][3]
            else:
                chart_data = cand_data
        elif kp_ref is not None:
            gap_mart_k = abs(mart_term - kp_ref)
            gap_cand_k = abs(cand_term - kp_ref)
            materially_better_cand = gap_cand_k + 5 < gap_mart_k
            extended_but_not_worse = (
                best_n > n_m
                and gap_cand_k <= gap_mart_k + CHART_SUBSTITUTION_KPI_MARGIN
                and gap_cand_k
                <= max(250.0, 0.01 * max(abs(kp_ref), 1.0))
            )
            substitute = materially_better_cand or extended_but_not_worse
            # Never discard a KPI-aligned mart spine for cash-flow substitutes that
            # miss open unreal / realize-on-close / synthesized dividends (~\$65k on BE).
            if substitute and gap_cand_k > gap_mart_k + CHART_SUBSTITUTION_KPI_MARGIN:
                substitute = False
            if substitute:
                chart_data = cand_data

    # Chart.js needs at least two x values to draw a line; a single mart day
    # (e.g. new option leg) would otherwise show only a blank chart.
    _chart_dates = chart_data.get("dates") or []
    if kpis and (not _chart_dates or len(_chart_dates) < 2):
        chart_data = _synthetic_cumulative_pnl_for_position(
            kpis, sessions_list, leg_param, selected_legs, current_df
        )

    if kpis:
        _align_position_pnl_chart_with_kpi(chart_data, kpis)
        _snap_position_chart_terminal_to_breakdown(
            chart_data, breakdown_rows
        )

    # Trade history rows
    trades_for_table = trades_df.copy()
    if "trade_date" in trades_for_table.columns:
        trades_for_table["trade_date"] = trades_for_table["trade_date"].astype(str)
    trades = trades_for_table.to_dict(orient="records") if not trades_for_table.empty else []
    # Disambiguate each trade's Account cell by tenant_id (same reason as the
    # Strategy Breakdown — all of a user's "Schwab Account" tenants share one
    # broker label). `_tenant_labels` was built above for strategy_rows.
    for _t in trades:
        _tid = _t.get("tenant_id")
        _t["account_display"] = (
            (_tenant_labels.get(_tid) if _tid else None)
            or _norm_account_label(_t.get("account"))
        )

    # Current positions
    current_positions = current_df.to_dict(orient="records") if not current_df.empty else []
    for _p in current_positions:
        _tid = _p.get("tenant_id")
        _p["account_display"] = (
            (_tenant_labels.get(_tid) if _tid else None)
            or _norm_account_label(_p.get("account"))
        )

    # ── Closed option legs (with cost/proceeds) ──
    closed_legs_list = []
    if not closed_legs_df.empty:
        closed_legs_list = closed_legs_df.sort_values("close_date").to_dict(orient="records")
        for r in closed_legs_list:
            r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
            r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
            r["total_pnl"] = round(float(r.get("total_pnl") or 0), 2)

    # ── Closed equity legs ──
    closed_equity_list = []
    if not closed_equity_df.empty:
        closed_equity_list = closed_equity_df.sort_values("close_date").to_dict(orient="records")
        for r in closed_equity_list:
            r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
            r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
            r["realized_pnl"] = round(float(r.get("realized_pnl") or 0), 2)

    # ── Trade Outcomes ──
    trade_outcomes = []
    for leg in closed_legs_list:
        direction = str(leg.get("direction") or "")
        prem_recv = float(leg.get("premium_received") or 0)
        prem_paid = float(leg.get("premium_paid") or 0)
        cost_close = float(leg.get("cost_to_close") or 0)
        proceeds_close = float(leg.get("proceeds_from_close") or 0)
        if direction == "Sold":
            o_cost = abs(cost_close)
            o_proceeds = abs(prem_recv)
        else:
            o_cost = abs(prem_paid)
            o_proceeds = abs(proceeds_close)
        o_pnl = float(leg.get("total_pnl") or 0)
        o_return = round(o_pnl / o_cost * 100, 1) if o_cost else None
        trade_outcomes.append({
            "trade_symbol": leg.get("trade_symbol"),
            "strategy": leg.get("strategy") or "",
            "direction": direction,
            "close_type": str(leg.get("close_type") or ""),
            "open_date": leg.get("open_date") or "",
            "close_date": leg.get("close_date") or "",
            "days_held": leg.get("days_in_trade"),
            "quantity": leg.get("quantity"),
            "cost": round(o_cost, 2),
            "proceeds": round(o_proceeds, 2),
            "pnl": round(o_pnl, 2),
            "return_pct": o_return,
            "is_winner": o_pnl > 0,
            "type": "option",
            "tenant_id": leg.get("tenant_id"),
            "account": str(leg.get("account") or "").strip(),
        })
    for leg in closed_equity_list:
        eq_proceeds = float(leg.get("sell_proceeds") or 0)
        eq_cost = float(leg.get("cost_basis") or 0)
        eq_pnl = float(leg.get("realized_pnl") or 0)
        eq_return = round(eq_pnl / eq_cost * 100, 1) if eq_cost else None
        od = leg.get("open_date") or ""
        cd = leg.get("close_date") or ""
        try:
            days = (pd.to_datetime(cd) - pd.to_datetime(od)).days if od and cd else None
        except Exception:
            days = None
        trade_outcomes.append({
            "trade_symbol": leg.get("trade_symbol") or symbol,
            "strategy": leg.get("description") or "Equity Sold",
            "direction": "Sold",
            "close_type": "Sold",
            "open_date": od,
            "close_date": cd,
            "days_held": days,
            "quantity": leg.get("quantity"),
            "cost": round(eq_cost, 2),
            "proceeds": round(eq_proceeds, 2),
            "pnl": round(eq_pnl, 2),
            "return_pct": eq_return,
            "is_winner": eq_pnl > 0,
            "type": "equity",
            "session_id": leg.get("session_id"),
            "tenant_id": leg.get("tenant_id"),
            "account": str(leg.get("account") or "").strip(),
        })
    trade_outcomes.sort(key=lambda x: x.get("close_date") or "", reverse=True)
    for _o in trade_outcomes:
        _tid = _o.get("tenant_id")
        _o["account_display"] = (
            (_tenant_labels.get(_tid) if _tid else None)
            or _norm_account_label(_o.get("account"))
        )

    # Attach raw transactions to each outcome for drill-down
    # Build session date range lookup for scoping equity trades
    _session_ranges = {}
    for s in sessions_list:
        sid = s.get("session_id")
        if sid is not None:
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_open = str(s.get("status", "")).strip().lower() == "open"
            _session_ranges[sid] = (s_od, s_ltd if not s_open else date.today())

    trades_by_symbol = {}
    for t in trades:
        ts = str(t.get("trade_symbol") or "")
        trades_by_symbol.setdefault(ts, []).append(t)

    for o in trade_outcomes:
        ts = str(o.get("trade_symbol") or "")
        if o["type"] == "option":
            matching = trades_by_symbol.get(ts, [])
        else:
            sid = o.get("session_id")
            s_range = _session_ranges.get(sid)
            matching = _equity_raw_trades_for_partial_close_outcome(
                trades,
                trade_symbol=ts,
                account=str(o.get("account") or "").strip(),
                session_range=s_range,
                close_milestone=o.get("close_date"),
            )
        o["raw_trades"] = matching

    # Assign leg numbers to trade outcomes and open positions
    def _date_to_leg(d_str):
        """Return display_leg number for a date string, or None.
        Prefers equity sessions over orphan (options-only) sessions to avoid
        the orphan's wide date range swallowing trades that belong to a real session."""
        if not d_str or not sessions_list:
            return None
        try:
            d = pd.to_datetime(d_str).date()
        except Exception:
            return None
        # First pass: check equity sessions (non-orphan)
        for s in sessions_list:
            if s.get("options_only"):
                continue
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_open = str(s.get("status", "")).strip().lower() == "open"
            s_end = s_ltd if not s_open else date.today()
            if s_od and s_end and s_od <= d <= s_end:
                return s["display_leg"]
        # Second pass: fall back to orphan (options-only) sessions
        for s in sessions_list:
            if not s.get("options_only"):
                continue
            s_od = pd.to_datetime(s["open_date"]).date() if s.get("open_date") else None
            s_ltd = pd.to_datetime(s["last_trade_date"]).date() if s.get("last_trade_date") else None
            s_end = s_ltd or date.today()
            if s_od and s_end and s_od <= d <= s_end:
                return s["display_leg"]
        return None

    for o in trade_outcomes:
        o["leg_num"] = _date_to_leg(o.get("open_date") or o.get("close_date"))
    # ``int_closed_equity_legs`` emits one outcome row per sell inside the same
    # equity chapter; merged ``int_position_legs`` assigns one display leg for that
    # whole span → every partial closure gets the SAME leg_num. Label partials so
    # it reads as intentional (one chapter, sequential exits), not buggy duplication.
    _eq_sess = {}
    for o in trade_outcomes:
        if o.get("type") != "equity" or o.get("session_id") is None:
            continue
        k = (o.get("account"), o["session_id"])
        _eq_sess.setdefault(k, []).append(o)
    for lst in _eq_sess.values():
        lst_chrono = sorted(lst, key=lambda x: x.get("close_date") or "")
        n = len(lst_chrono)
        for i, o in enumerate(lst_chrono, start=1):
            o["equity_partial_ix"] = i
            o["equity_partial_n"] = n
    for p in current_positions:
        # Open positions belong to the latest open session
        open_sessions = [s for s in sessions_list if str(s.get("status", "")).strip().lower() == "open"]
        p["leg_num"] = open_sessions[-1]["display_leg"] if open_sessions else (sessions_list[-1]["display_leg"] if sessions_list else None)

    # ── Option matrices (DTE × Strike Distance heatmap) ──
    # (tenant scope already narrowed matrix_df to the selected account's tenant)
    # Filter matrix by selected legs (date range overlap via trade_symbol matching closed legs)
    if leg_param and _leg_ranges and not matrix_df.empty:
        filtered_trade_syms = set(r.get("trade_symbol") for r in closed_legs_list)
        if "trade_symbol" in matrix_df.columns:
            matrix_df = matrix_df[matrix_df["trade_symbol"].isin(filtered_trade_syms)]
    # matrix_df is tenant-scoped to the current ?account/?tenant selection
    # upstream, so the matrices honor the filter by construction.
    with timed("matrix"):
        option_matrices = (
            _build_option_matrices(matrix_df, symbol) if not matrix_df.empty else []
        )

    # Available accounts for filter. Non-admin: the full disambiguated
    # account set so each physical account (incl. colliding "Schwab
    # Account"s) is selectable even after tenant scope narrowed the data
    # to one. Admin: data-derived (summary may be empty for open-only lots).
    if user_accounts:
        all_accounts = sorted(user_accounts)
    elif not summary_df.empty and "account" in summary_df.columns:
        all_accounts = sorted(summary_df["account"].dropna().unique())
    elif not current_df.empty and "account" in current_df.columns:
        all_accounts = sorted(current_df["account"].dropna().unique())
    else:
        all_accounts = []

    # Sector / subsector: take the first non-Unknown value we can find from
    # either summary or current. Both sources are joined to stg_symbol_metadata
    # in dbt, so they should agree — falling through is just defensive.
    def _first_nonempty(df_, col):
        if df_ is None or df_.empty or col not in df_.columns:
            return ""
        vals = df_[col].dropna().astype(str).str.strip()
        vals = vals[(vals != "") & (vals.str.lower() != "unknown")]
        if vals.empty:
            # Fall back to whatever we have, including 'Unknown', so the UI
            # can still render a label rather than nothing.
            any_vals = df_[col].dropna().astype(str).str.strip()
            return any_vals.iloc[0] if not any_vals.empty else ""
        return vals.iloc[0]

    symbol_sector = _first_nonempty(summary_df, "sector") or _first_nonempty(current_df, "sector")
    symbol_subsector = _first_nonempty(summary_df, "subsector") or _first_nonempty(current_df, "subsector")
    symbol_company = _first_nonempty(summary_df, "company_name") or _first_nonempty(current_df, "company_name")

    # Next-earnings pill for the hero. dict form: {"date": "YYYY-MM-DD",
    # "display": "Tue Jun 15", "days_until": 28} or None if the symbol
    # has no upcoming earnings (ETFs, indices, crypto, or a symbol whose
    # last yfinance fetch returned no calendar). Template hides the pill
    # entirely when None — no "NaT" / "None" leaks to the UI.
    symbol_next_earnings = None
    try:
        if earnings_df is not None and not earnings_df.empty:
            erow = earnings_df.iloc[0]
            ed = erow.get("next_earnings_date")
            if ed is not None and not (hasattr(ed, "__float__") and pd.isna(ed)):
                ed_date = ed.date() if hasattr(ed, "date") and not isinstance(ed, date) else ed
                days_until_raw = erow.get("days_until")
                try:
                    days_until = int(days_until_raw) if days_until_raw is not None else None
                except (TypeError, ValueError):
                    days_until = None
                symbol_next_earnings = {
                    "date": ed_date.strftime("%Y-%m-%d") if hasattr(ed_date, "strftime") else str(ed_date)[:10],
                    "display": ed_date.strftime("%a %b %-d") if hasattr(ed_date, "strftime") else str(ed_date)[:10],
                    "days_until": days_until,
                }
    except Exception:
        symbol_next_earnings = None

    # Cross-source reconciliation invariant.
    #
    # Σ strategy_rows.total_pnl is NOT a reliable ledger rollup — attribution
    # spreads equity realization across strategies (Wheel, CSP, Dividend/Buy &
    # Hold, …). Summing labeled rows may disagree with ledger paths while still
    # being "correct by label" (May 2026 BE: breakdown ≈ chart; strategy rows
    # lower by ~ dividends + equity credited elsewhere).
    #
    # Compare three full-symbol measures grounded in fills + mart spine:
    #   - Hero KPI total_return — realized (+ unreal + Σ summary dividends).
    #   - Breakdown by Type — Σ equity/options/dividend rollups above Strategy.
    #   - Chart terminal — mart_daily_pnl walk.
    #
    # Partition drift (Σ strategies vs KPI) logs at INFO for debugging only.
    invariant_warning = None
    try:
        strategy_partition_sum = round(
            sum(float(r.get("total_pnl") or 0) for r in strategy_rows), 2
        )
        kpi_total = round(float(kpis.get("total_return") or 0), 2) if kpis else 0.0
        # ``breakdown_rows`` dicts come from ``_compute_breakdown_by_type``,
        # which emits ``"total"`` (not ``"total_pnl"`` — that key belongs to the
        # strategy_rows shape from positions_summary).
        bt_total = round(sum(float(r.get("total") or 0) for r in breakdown_rows), 2)
        chart_terminal = round(float((chart_data.get("total") or [0.0])[-1] or 0.0), 2)
        if abs(strategy_partition_sum - kpi_total) > 1.0:
            app.logger.info(
                "position_detail strategy partition sum vs KPI: %s/%s "
                "partition=%.2f kpi=%.2f (labels need not match ledger rollups)",
                selected_account or "ALL",
                safe_symbol,
                strategy_partition_sum,
                kpi_total,
            )
        # Skip when the by-type card didn't render — nothing to reconcile.
        if breakdown_rows:
            worst_gap = max(
                abs(kpi_total - bt_total),
                abs(bt_total - chart_terminal),
                abs(kpi_total - chart_terminal),
            )
            if worst_gap > 1.0:
                invariant_warning = {
                    "hero_total_return": kpi_total,
                    "breakdown_by_type_total": bt_total,
                    "chart_terminal": chart_terminal,
                    "worst_gap": round(worst_gap, 2),
                }
                app.logger.warning(
                    "position_detail invariant: %s/%s ledger totals disagree — "
                    "kpi=%.2f, breakdown_by_type=%.2f, chart_terminal=%.2f (gap=%.2f)",
                    selected_account or "ALL",
                    safe_symbol,
                    kpi_total,
                    bt_total,
                    chart_terminal,
                    worst_gap,
                )
    except Exception as exc:
        # Invariant computation must never break the page render. Log and move
        # on — the worst case here is "no canary" not "broken page".
        app.logger.exception(
            "position_detail invariant calc failed for %s: %s", safe_symbol, exc
        )

    # Build the symbol tab strip payload from the lightweight `tabs_df`
    # rollup. One row per (symbol) — when the user spans multiple accounts we
    # collapse so each ticker shows up once in the strip with combined P&L
    # and trade count, and "open" wins over "closed" for the dot.
    tabs = []
    if not tabs_df.empty:
        tdf = tabs_df.copy()
        for col in ("total_return", "num_trades"):
            if col in tdf.columns:
                tdf[col] = pd.to_numeric(tdf[col], errors="coerce").fillna(0)
        if "has_open_leg" in tdf.columns:
            tdf["has_open_leg"] = pd.to_numeric(tdf["has_open_leg"], errors="coerce").fillna(0).astype(int)
        # Collapse to one row per symbol across the in-scope accounts.
        agg_funcs = {
            "total_return": "sum",
            "num_trades": "sum",
            "has_open_leg": "max",
        }
        if "strategies_pipe" in tdf.columns:
            agg_funcs["strategies_pipe"] = lambda s: "|".join(sorted({
                p for v in s.dropna() for p in str(v).split("|") if p
            }))
        if "account" in tdf.columns:
            agg_funcs["account"] = lambda s: ", ".join(sorted({str(v) for v in s.dropna() if str(v).strip()}))
        rolled = tdf.groupby("symbol", as_index=False).agg(agg_funcs)
        rolled = rolled.sort_values("total_return", ascending=False)
        for r in rolled.to_dict(orient="records"):
            strats_pipe = str(r.get("strategies_pipe") or "")
            tabs.append({
                "symbol": r.get("symbol"),
                "account": r.get("account") or "",
                "total_return": round(float(r.get("total_return") or 0.0), 2),
                "num_trades": int(r.get("num_trades") or 0),
                "status": "Open" if int(r.get("has_open_leg") or 0) else "Closed",
                "strategies": [s for s in strats_pipe.split("|") if s] if strats_pipe else [],
            })

    # Tab strip uses navigate-mode anchors. Preserve ?account= so the
    # destination page stays in the same scope (admin + non-admin tenancy
    # reasoning above continues to hold).
    tab_qs = ""
    if selected_account:
        tab_qs = "?account=" + quote_plus(selected_account)

    return render_template(
        "position_detail.html",
        symbol=symbol,
        kpis=kpis,
        overall_status=overall_status,
        strategy_rows=strategy_rows,
        breakdown_rows=breakdown_rows,
        trades=trades,
        trade_outcomes=trade_outcomes,
        current_positions=current_positions,
        option_matrices=option_matrices,
        sessions=sessions_list,
        selected_legs=selected_legs,
        leg_param=leg_param,
        chart_data_json=json.dumps(_chart_data_for_json(chart_data)),
        has_underlying_price=chart_data.get("has_underlying_price", False),
        prices_through_date=prices_through_date,
        accounts=all_accounts,
        selected_account=selected_account,
        symbol_sector=symbol_sector,
        symbol_subsector=symbol_subsector,
        symbol_company=symbol_company,
        symbol_next_earnings=symbol_next_earnings,
        invariant_warning=invariant_warning,
        viewer_is_admin=is_admin(current_user.username),
        tabs=tabs,
        active_symbol=symbol,
        tab_href_base="/position/",
        tab_href_suffix=tab_qs,
        mode="navigate",
    )


# ======================================================================
# Daily Position Detail  (/symbols)
# ======================================================================

TRADES_QUERY = """
    SELECT
        account,
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

OPEN_SESSION_START_QUERY = """
    SELECT
        account,
        symbol,
        MIN(open_date) AS open_start
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE status = 'Open'
      {tenant_filter}
    GROUP BY account, symbol
"""


CLOSED_LEGS_QUERY = """
    SELECT
        sc.account,
        sc.symbol,
        sc.strategy,
        sc.trade_symbol,
        sc.open_date,
        sc.close_date,
        sc.total_pnl,
        sc.status,
        oc.contracts_sold_to_open + oc.contracts_bought_to_open AS quantity,
        oc.premium_received,
        oc.premium_paid,
        oc.cost_to_close,
        oc.proceeds_from_close,
        oc.direction,
        oc.close_type,
        oc.days_in_trade
    FROM `ccwj-dbt.analytics.int_strategy_classification` sc
    JOIN `ccwj-dbt.analytics.int_option_contracts` oc
      ON (sc.tenant_id IS NOT DISTINCT FROM oc.tenant_id)
     AND sc.account = oc.account
     AND sc.trade_symbol = oc.trade_symbol
     AND sc.user_id IS NOT DISTINCT FROM oc.user_id
    WHERE sc.status = 'Closed'
      AND sc.trade_group_type = 'option_contract'
      {closed_legs_tenant_filter}
"""

CLOSED_EQUITY_LEGS_QUERY = """
    SELECT
        account,
        symbol,
        trade_symbol,
        open_date,
        close_date,
        quantity,
        sale_price_per_share,
        sell_proceeds,
        cost_basis,
        realized_pnl,
        description
    FROM `ccwj-dbt.analytics.int_closed_equity_legs`
    WHERE 1=1 {tenant_filter}
"""

CURRENT_POSITIONS_QUERY = """
    SELECT
        account,
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

STRATEGIES_MAP_QUERY = """
    SELECT account, symbol, strategy
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {tenant_filter}
"""

SYMBOLS_PNL_QUERY = """
    SELECT account, symbol, status, realized_pnl, unrealized_pnl
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {tenant_filter}
"""

def _build_option_matrices(matrix_df, symbol):
    """Reshape pre-bucketed matrix cells into per-strategy heatmaps.

    The DTE x Strike-Distance bucketing now happens in dbt
    (``mart_option_win_matrix``); ``matrix_df`` already carries one row per
    (tenant, account, user, strategy, dte_label, strike_col) with raw
    ``trade_count`` / ``wins`` / ``sum_pnl``. This function only:
      1. combines cells across tenants/accounts for the scoped view, then
      2. rounds avg P&L and win rate ONCE (after the union), matching the
         old per-contract math exactly.

    ``matrix_df`` arrives ALREADY tenant-scoped (SQL ``{tenant_filter}`` +
    ``_filter_df_by_tenant_ids`` in the caller), so no account predicate is
    applied here — the old display-label filter never matched the warehouse
    broker label and re-fused colliding-label accounts.
    """
    import math

    df = matrix_df[matrix_df["underlying_symbol"] == symbol].copy()
    if df.empty:
        return []

    for col in ("trade_count", "wins", "sum_pnl"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Canonical label ordering (mirrors the dbt bucket labels). Columns read
    # left-to-right ITM -> OTM; the em-dash "unknown" column is appended last.
    PCT_ORDER = ["<-10%", "-10 to -5%", "-5 to -2%", "ATM ±2%", "+2 to +5%", "+5 to +10%", ">+10%"]
    DTE_ORDER = ["0–7", "8–14", "15–30", "31–60", "61+"]

    matrices = []
    for strategy, grp in df.groupby("strategy"):
        # Combine duplicate cells across tenants/accounts. sum_pnl + count
        # aggregate cleanly: mean over the union == Σsum_pnl / Σcount.
        agg = grp.groupby(["dte_label", "strike_col"], as_index=False).agg(
            count=("trade_count", "sum"),
            wins=("wins", "sum"),
            sum_pnl=("sum_pnl", "sum"),
        )
        present_cols = set(agg["strike_col"])
        col_range = [lbl for lbl in PCT_ORDER if lbl in present_cols]
        if "—" in present_cols:
            col_range.append("—")

        present_dtes = set(agg["dte_label"])
        dte_order = [lbl for lbl in DTE_ORDER if lbl in present_dtes]

        cell_map = {(r["dte_label"], r["strike_col"]): r for _, r in agg.iterrows()}

        rows = []
        for dte_lbl in dte_order:
            cells = []
            for col_val in col_range:
                r = cell_map.get((dte_lbl, col_val))
                total = int(r["count"]) if r is not None else 0
                if total <= 0:
                    cells.append({"count": 0, "avg_pnl": None, "win_rate": None})
                else:
                    wins = int(r["wins"])
                    avg_pnl_dollar = float(r["sum_pnl"]) / total
                    cells.append({
                        "count": total,
                        "avg_pnl": round(avg_pnl_dollar, 0) if not math.isnan(avg_pnl_dollar) else None,
                        "win_rate": round(wins / total * 100, 0),
                        "wins": wins,
                    })
            rows.append({"dte_label": dte_lbl, "cells": cells})

        matrices.append({
            "strategy": strategy,
            "trade_count": int(agg["count"].sum()),
            "col_headers": col_range,
            "rows": rows,
        })

    return matrices


def _chart_data_for_json(obj):
    """Recursively make chart data JSON/JS-safe (NaN/Inf break Chart.js parsing)."""
    if isinstance(obj, dict):
        return {k: _chart_data_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_chart_data_for_json(x) for x in obj]
    if isinstance(obj, bool) or obj is None:
        return obj
    if isinstance(obj, int):
        return obj
    try:
        f = float(obj)
    except (TypeError, ValueError):
        return obj
    if not math.isfinite(f):
        return None
    return f


def _collect_activity_candidate_dates(
    trades_pre_leg, closed_legs_pre_leg, closed_equity_pre_leg, sessions_list
):
    """
    Dates that represent when the user first/last touched this symbol, using
    trades and strategy metadata before leg scoping. Used when summary/stg rows
    are missing or leg-filtered to empty.
    """
    out = []
    if (
        trades_pre_leg is not None
        and not trades_pre_leg.empty
        and "trade_date" in trades_pre_leg.columns
    ):
        td = pd.to_datetime(trades_pre_leg["trade_date"], errors="coerce")
        out.extend([x for x in td.dropna().dt.date.tolist() if x is not None])
    for df, cols in (
        (closed_legs_pre_leg, ("open_date", "close_date")),
        (closed_equity_pre_leg, ("open_date", "close_date")),
    ):
        if df is None or df.empty:
            continue
        for c in cols:
            if c not in df.columns:
                continue
            for v in df[c].dropna():
                ts = pd.to_datetime(v, errors="coerce")
                if pd.isna(ts):
                    continue
                try:
                    out.append(ts.date())
                except Exception:
                    pass
    for s in sessions_list or []:
        for key in ("open_date", "last_trade_date"):
            v = s.get(key)
            if not v:
                continue
            ts = pd.to_datetime(v, errors="coerce")
            if pd.isna(ts):
                continue
            try:
                out.append(ts.date())
            except Exception:
                pass
    return [d for d in out if d is not None]


def _synthetic_cumulative_pnl_for_position(kpis, sessions_list, leg_param, selected_legs, current_df):
    """
    When mart_daily_pnl has no rows in-range (new leg, pipeline lag, leg filter) or
    the chart query failed, draw a 2-point cumulative P&L line consistent with KPIs.
    """
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    if not kpis:
        return empty

    realized = float(kpis.get("realized_pnl") or 0)
    unreal = float(kpis.get("unrealized_pnl") or 0)
    div_d = float(kpis.get("dividend_income") or 0)
    tot_end = round(float(kpis.get("total_return") or 0), 2)

    eq_unreal = 0.0
    opt_unreal = 0.0
    if (
        not current_df.empty
        and "instrument_type" in current_df.columns
        and "unrealized_pnl" in current_df.columns
    ):
        eq_df = current_df[current_df["instrument_type"] == "Equity"]
        op_df = current_df[current_df["instrument_type"].isin(["Call", "Put"])]
        if not eq_df.empty:
            eq_unreal = float(eq_df["unrealized_pnl"].sum())
        if not op_df.empty:
            opt_unreal = float(op_df["unrealized_pnl"].sum())
    if abs(eq_unreal + opt_unreal - unreal) > 0.02:
        eq_unreal, opt_unreal = unreal, 0.0

    eq_end = round(realized + eq_unreal, 2)
    opt_end = round(opt_unreal, 2)

    start_d = None
    if leg_param and sessions_list and selected_legs:
        ods = []
        for s in sessions_list:
            if s.get("session_id") in selected_legs and s.get("open_date"):
                try:
                    ods.append(pd.to_datetime(s["open_date"]).date())
                except Exception:
                    pass
        if ods:
            start_d = min(ods)
    if start_d is None and kpis.get("first_trade"):
        try:
            start_d = pd.to_datetime(kpis["first_trade"]).date()
        except Exception:
            start_d = None

    end_d = date.today()
    if start_d is None:
        start_d = end_d - timedelta(days=1) if end_d > date(2000, 1, 2) else end_d
    if start_d > end_d:
        start_d = end_d
    if start_d == end_d:
        start_d = end_d - timedelta(days=1) if end_d > date(2000, 1, 2) else end_d

    d0, d1 = str(start_d), str(end_d)

    p0, p1 = None, None
    if not current_df.empty and "instrument_type" in current_df.columns and "current_price" in current_df.columns:
        eqp = current_df[current_df["instrument_type"] == "Equity"]
        if not eqp.empty:
            c = float(eqp["current_price"].iloc[0] or 0)
            if c > 0:
                p1 = round(c, 2)

    return {
        "dates": [d0, d1],
        "equity": [0.0, eq_end],
        "options": [0.0, opt_end],
        "dividends": [0.0, div_d],
        "total": [0.0, tot_end],
        "underlying_price": [p0, p1],
        "has_underlying_price": p1 is not None,
    }


CHART_SUBSTITUTION_KPI_MARGIN = 25.0  # slack when judging mart vs substitute vs KPI headline


def _snap_position_chart_terminal_to_breakdown(
    chart_data: dict | None, breakdown_rows: list | None
) -> None:
    """Pinned hero + Breakdown-by-type both use Σ ``breakdown_rows`` totals.

    The cumulative chart can still end on a mart-only realization (broker
    open-equity unreal and/or dividend cumulative missing on the spine)
    when substitution or LIVE patching does not fully apply — IYW Emmory:
    hero ≈ -$1.607 vs chart ≈ realized -$1.957 .

    Bump **only** the last plotted bucket (not proportional history
    rescale): adjust ``total[-1]`` to the ledger and apply the delta to the
    equity stream so stacked components remain consistent."""
    tol = 1.0  # Must match CHART_KPI_ALIGN_TOLERANCE_DOLLARS.
    if not chart_data or not breakdown_rows or not chart_data.get("total"):
        return
    totals = chart_data["total"]
    n = len(totals)
    if n < 1:
        return
    ledger = round(
        sum(float(r.get("total") or 0) for r in breakdown_rows), 2,
    )
    tail = round(float(totals[-1] or 0), 2)
    if abs(ledger - tail) <= tol:
        return
    delta = round(ledger - tail, 2)
    try:
        app.logger.info(
            "snap chart terminal to breakdown ledger: Δ=%.2f ledger=%.2f "
            "was_tail=%.2f",
            delta,
            ledger,
            tail,
        )
    except Exception:
        pass
    totals[-1] = round(float(totals[-1] or 0) + delta, 2)
    eq = chart_data.get("equity")
    if eq is not None and len(eq) == n:
        eq[-1] = round(float(eq[-1] or 0) + delta, 2)


def _chart_data_terminal(chart_data):
    """Last ``total`` point from cumulative P&amp;L chart payload, or 0."""
    if not chart_data:
        return 0.0
    pts = chart_data.get("total")
    if not pts:
        return 0.0
    try:
        return round(float(pts[-1] or 0), 2)
    except Exception:
        return 0.0


CHART_KPI_ALIGN_TOLERANCE_DOLLARS = 1.00


def _align_position_pnl_chart_with_kpi(chart_data, kpis):
    """
    Cosmetic rounding-noise reconciliation between the chart's terminal value
    and the page's KPI ``total_return``. Bounded: above
    ``CHART_KPI_ALIGN_TOLERANCE_DOLLARS`` of disagreement we DO NOT rescale —
    we leave the chart untouched so the page-level invariant card surfaces
    the structural disagreement instead of silently distorting the series.

    History (May 2026):
      This function used to unconditionally rescale the chart's equity /
      options / dividends streams by ``f = kpi / chart_total[-1]``,
      effectively forcing the chart's terminal value to match the KPI no
      matter how big the gap. That hid a real bug in BE/Sara where
      ``mart_daily_pnl`` was sourcing today's close from yfinance ($283.92)
      while the KPI sourced today's price from broker ($262.70),
      producing a chart_total of $11,709 silently rescaled to $7,465.
      Every per-day equity/options point on the chart was then ~36%
      smaller than the math actually produced — meaningless cosmetic
      values that "happened" to sum to the KPI. The rescale was a band-aid
      over a structural bug; removing the band-aid surfaced the bug, which
      was then fixed at source (`mart_daily_pnl.sql` "PRICE PRECEDENCE"
      comment + `int_option_contracts.sql` open-contract total_pnl).

      After those source fixes, the chart's terminal value reconciles to
      the KPI by construction. The only legitimate disagreement is
      sub-dollar rounding noise (sequential 2dp rounding through several
      pandas / Jinja layers), which this function still absorbs.

      If you find this function firing on a real position, that's signal:
      either a new yfinance/broker source split has been introduced, or
      another rounding-precision drift has appeared upstream. Investigate
      the upstream source rather than widening the tolerance here.
    """
    if not chart_data or not kpis or not chart_data.get("total"):
        return
    n = len(chart_data["total"])
    if n < 1:
        return
    t_end = float(chart_data["total"][-1] or 0.0)
    k = float(kpis.get("total_return", 0) or 0.0)
    gap = abs(t_end - k)
    if gap <= 0.02:
        return
    if gap > CHART_KPI_ALIGN_TOLERANCE_DOLLARS:
        # Structural disagreement, not rounding. DO NOT rescale.
        # The page-level invariant card in position_detail will surface
        # this on the rendered page (admin-only). Log here too so the
        # disagreement is searchable in production logs even when the
        # admin canary doesn't fire (e.g. non-admin viewer, or the
        # invariant card itself has a bug).
        try:
            app.logger.warning(
                "_align_position_pnl_chart_with_kpi: refusing to rescale "
                "chart series \u2014 gap of $%.2f exceeds tolerance $%.2f. "
                "chart_terminal=$%.2f, kpi_total_return=$%.2f. "
                "This indicates a real source disagreement (broker vs "
                "yfinance, rounding-precision drift, or duplicate rows). "
                "Investigate upstream rather than widening the tolerance.",
                gap, CHART_KPI_ALIGN_TOLERANCE_DOLLARS, t_end, k,
            )
        except Exception:
            pass
        return

    # Sub-dollar gap: real rounding noise. Apply the legacy rescale logic
    # so the chart cosmetically agrees with the KPI to the cent.
    if abs(t_end) < 1e-9:
        # Edge case: chart terminal is ~0 but KPI isn't (e.g. all-realized
        # closed-leg series with open-only KPI). Can't compute a scale
        # factor; place the KPI delta on the most-active stream so the
        # stacked sum matches `total`.
        if abs(k) > 0.02 and n >= 1:
            tlist = [0.0] * (n - 1) + [round(k, 2)]
            chart_data["total"] = tlist
            e_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("equity") or [0.0] * n)[:n]
            )
            o_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("options") or [0.0] * n)[:n]
            )
            d_abs = sum(
                abs(float(x or 0)) for x in (chart_data.get("dividends") or [0.0] * n)[:n]
            )
            for key in ("equity", "options", "dividends"):
                if key in chart_data and len(chart_data.get(key) or []) == n:
                    chart_data[key] = [0.0] * n
            mx = max(d_abs, e_abs, o_abs)
            if mx < 1e-9:
                if "options" in chart_data and len(chart_data["options"]) == n:
                    chart_data["options"][-1] = round(k, 2)
                elif "equity" in chart_data and len(chart_data["equity"]) == n:
                    chart_data["equity"][-1] = round(k, 2)
                elif "dividends" in chart_data and len(chart_data["dividends"]) == n:
                    chart_data["dividends"][-1] = round(k, 2)
            else:
                _tie = {"options": 0, "equity": 1, "dividends": 2}
                streams = [
                    (d_abs, "dividends"),
                    (e_abs, "equity"),
                    (o_abs, "options"),
                ]
                streams.sort(key=lambda t: (-t[0], _tie.get(t[1], 9)))
                for _score, sname in streams:
                    if sname in chart_data and len(chart_data[sname]) == n:
                        chart_data[sname][-1] = round(k, 2)
                        break
        return
    f = k / t_end
    if not all(
        len(chart_data.get(skey) or []) == n
        for skey in ("equity", "options", "dividends")
    ):
        chart_data["total"] = [round(float(x) * f, 2) for x in chart_data["total"]]
        return
    for key in ("equity", "options", "dividends"):
        arr = chart_data.get(key) or []
        chart_data[key] = [round(float(x) * f, 2) for x in arr]
    chart_data["total"] = [
        round(
            float(chart_data["equity"][i] or 0)
            + float(chart_data["options"][i] or 0)
            + float(chart_data["dividends"][i] or 0),
            2,
        )
        for i in range(n)
    ]


def _cumulative_pnl_from_stg_trades(trades_df, current_df):
    """
    Cumulative P&L by calendar day from stg_history (cash flow per row). Used when
    mart_daily_pnl is sparse but stg has years of RDDT fills (symbol match quirks).
    """
    empty = {
        "dates": [],
        "equity": [],
        "options": [],
        "dividends": [],
        "total": [],
        "underlying_price": [],
        "has_underlying_price": False,
    }
    if trades_df is None or trades_df.empty or "amount" not in trades_df.columns:
        return None
    t = trades_df.copy()
    if "trade_date" not in t.columns or "instrument_type" not in t.columns:
        return None
    t["td"] = pd.to_datetime(t["trade_date"], errors="coerce").dt.normalize()
    t = t[pd.notna(t["td"])]
    if t.empty:
        return None
    t["amount"] = pd.to_numeric(t["amount"], errors="coerce").fillna(0.0)
    it = t["instrument_type"].fillna("").str.strip()
    a = t["amount"]
    t["_div"] = a.where(
        (it == "Dividend") | it.str.contains("ividend", case=False, na=False), 0.0
    )
    t["_eq"] = a.where(it == "Equity", 0.0)
    t["_op"] = a.where(it.isin(["Call", "Put"]), 0.0)
    t["_oth"] = a - t["_div"] - t["_eq"] - t["_op"]
    g = t.groupby("td", as_index=False).agg(
        {"_eq": "sum", "_op": "sum", "_div": "sum", "_oth": "sum"}
    )
    g = g.sort_values("td")
    g["c_eq"] = g["_eq"].cumsum()
    g["c_op"] = (g["_op"] + g["_oth"]).cumsum()  # fees/margin in with options line for chart
    g["c_div"] = g["_div"].cumsum()
    g["tot"] = g["c_eq"] + g["c_op"] + g["c_div"]
    dates = [str(pd.Timestamp(x).date()) for x in g["td"].tolist()]
    return {
        "dates": dates,
        "equity": [round(x, 2) for x in g["c_eq"]],
        "options": [round(x, 2) for x in g["c_op"]],
        "dividends": [round(x, 2) for x in g["c_div"]],
        "total": [round(x, 2) for x in g["tot"]],
        "underlying_price": [None] * len(dates),
        "has_underlying_price": False,
    }


def _cumulative_pnl_from_leg_closes(closed_legs_pre_leg, closed_equity_pre_leg):
    """
    Step cumulative P&L from closed option legs and closed equity by close_date.
    Fallback when stg is empty but int_* legs exist.
    """
    events = []  # (date, d_eq, d_op, d_div)
    if closed_legs_pre_leg is not None and not closed_legs_pre_leg.empty and "close_date" in closed_legs_pre_leg.columns:
        for _, r in closed_legs_pre_leg.iterrows():
            d = r.get("close_date")
            if pd.isna(d):
                continue
            pnl = float(r.get("total_pnl") or 0)
            d0 = pd.to_datetime(d).date()
            events.append((d0, 0.0, pnl, 0.0))
    if closed_equity_pre_leg is not None and not closed_equity_pre_leg.empty and "close_date" in closed_equity_pre_leg.columns:
        for _, r in closed_equity_pre_leg.iterrows():
            d = r.get("close_date")
            if pd.isna(d):
                continue
            pnl = float(r.get("realized_pnl") or 0)
            d0 = pd.to_datetime(d).date()
            events.append((d0, pnl, 0.0, 0.0))
    if not events:
        return None
    events.sort(key=lambda x: x[0])
    byd = {}
    for d0, e, o, di in events:
        byd.setdefault(d0, [0.0, 0.0, 0.0])
        byd[d0][0] += e
        byd[d0][1] += o
        byd[d0][2] += di
    d_sorted = sorted(byd)
    c_eq, c_op, c_div = 0.0, 0.0, 0.0
    dates, eq, op, div, tot = [], [], [], [], []
    for d0 in d_sorted:
        c_eq += byd[d0][0]
        c_op += byd[d0][1]
        c_div += byd[d0][2]
        dates.append(str(d0))
        eq.append(round(c_eq, 2))
        op.append(round(c_op, 2))
        div.append(round(c_div, 2))
        tot.append(round(c_eq + c_op + c_div, 2))
    return {
        "dates": dates,
        "equity": eq,
        "options": op,
        "dividends": div,
        "total": tot,
        "underlying_price": [None] * len(dates),
        "has_underlying_price": False,
    }


def _collapse_mart_daily_pnl_duplicate_grain(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Collapse duplicate ``mart_daily_pnl`` rows before stateful equity P&L.

    Natural grain is ``(tenant_id, account, user_id, symbol, date)``.
    Sync/backfill bugs can emit identical twins —
    ``_build_chart_from_daily_pnl`` processes each row and sums
    ``equity_*`` deltas, doubling buys/sells and inflating terminal P&L
    (May 2026 BE chart ~2× hero).

    CRITICAL: ``tenant_id`` leads the dedup key when present. Several
    physical accounts can share an ``account`` display label (e.g. multiple
    "Schwab Account"s); deduping on ``(account, symbol, date)`` alone would
    collapse those distinct tenants' same-symbol/day rows into one and drop
    the rest. Prefers populated ``user_id`` over ``NULL`` when deduping,
    then merges strict full-key collisions with ``keep=\"last\"`` (later
    ingestion wins).
    """
    if daily_df is None or daily_df.empty:
        return daily_df
    if not {"account", "symbol", "date"}.issubset(daily_df.columns):
        return daily_df
    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    stab = "__r_i__"
    df[stab] = range(len(df))
    _tenant_key = ["tenant_id"] if "tenant_id" in df.columns else []
    ks3 = _tenant_key + ["account", "symbol", "date"]

    if "user_id" in df.columns:
        uid_col = pd.to_numeric(df["user_id"], errors="coerce")
        df["__prefer_uid__"] = uid_col.notna().astype(int)
        df = df.sort_values(
            by=ks3 + ["__prefer_uid__", "user_id", stab],
            # ks3 cols ascending, then prefer populated uid (desc), then
            # user_id asc, then stable index asc.
            ascending=([True] * len(ks3)) + [False, True, True],
            na_position="last",
        )
        df = df.drop_duplicates(subset=ks3, keep="first").drop(
            columns=["__prefer_uid__"]
        )
        ks4 = ks3 + ["user_id"]
        df = df.sort_values(by=ks4 + [stab]).drop_duplicates(
            subset=ks4, keep="last"
        )
    else:
        df = df.sort_values(by=ks3 + [stab]).drop_duplicates(subset=ks3, keep="last")

    return df.drop(columns=[stab]).reset_index(drop=True)


def _build_chart_from_daily_pnl(daily_df, current_df):
    """Chart builder entrypoint — partitions ``mart_daily_pnl`` rows so each
    ``(account × user_id)`` slice runs its **own** equity cost-basis state
    machine.

    Without partitioning, ``position_detail`` with multiple brokerage labels
    merged into one symbol view feeds interleaved rows into a single walker —
    sells on account A partially consume basis accumulated from account B,
    corrupting cumulative equity (often reading as ~2× hero KPI vs mart).
    """
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    if daily_df is None or daily_df.empty:
        return empty
    work = _collapse_mart_daily_pnl_duplicate_grain(daily_df.copy())
    if work.empty:
        return empty
    # Partition leads with the broker-stable tenant_id (v2 grain) when
    # present so several physical accounts sharing a display label each run
    # their OWN equity cost-basis state machine. ``account``/``user_id``
    # remain in the key for legacy / NULL-tenant rows.
    part_cols = []
    if "tenant_id" in work.columns:
        part_cols.append("tenant_id")
    part_cols.append("account")
    if "user_id" in work.columns:
        part_cols.append("user_id")
    gb = work.groupby(part_cols, dropna=False)
    if gb.ngroups <= 1:
        return _build_chart_from_daily_pnl_partition(work.sort_values("date"), current_df)
    parts = []
    for key, sub in gb:
        key = key if isinstance(key, tuple) else (key,)
        keyed = dict(zip(part_cols, key))
        tenant_k = keyed.get("tenant_id")
        acct = keyed.get("account")
        uid_k = keyed.get("user_id")
        cdf = _filter_current_for_chart_partition(current_df, acct, uid_k, tenant_k)
        parts.append(
            _build_chart_from_daily_pnl_partition(sub.sort_values("date"), cdf)
        )
    return _merge_position_pnl_chart_payloads(parts)


def _build_chart_from_daily_pnl_partition(daily_df, current_df):
    """
    Build cumulative P&L chart from pre-aggregated mart_daily_pnl data.

    Options, dividends, and other: read pre-computed cumulative sums.
    Equity: compute running average-cost P&L (stateful — buy/sell events
    from the mart, with daily mark-to-market via close_price).
    """
    empty = {
        "dates": [], "equity": [], "options": [], "dividends": [],
        "total": [], "underlying_price": [], "has_underlying_price": False,
    }
    if daily_df.empty:
        return empty

    daily_df = daily_df.sort_values("date")

    shares_held = 0.0
    total_cost = 0.0
    cum_realized = 0.0
    short_shares = 0.0
    short_cost_basis = 0.0
    position_is_closed = current_df.empty
    last_trade_date = None
    # mart_daily_pnl's dense spine starts at the account's earliest trade
    # date ACROSS ALL SYMBOLS, so a per-symbol slice can carry a long
    # flat-$0 prefix before this symbol's first fill (e.g. BE's chart
    # opening on 1/29 when the first BE trade was in April). Each position
    # chart should begin when the position actually opened. Skip leading
    # rows until the first activity; the closed-position branch below
    # already trims these for closed positions, this covers OPEN ones.
    position_started = False

    dates, equity_s, options_s, dividends_s, total_s, price_s = (
        [], [], [], [], [], [],
    )
    last_cumulative_options_realized = 0.0
    last_open_options_unrealized = 0.0
    last_cumulative_other_pnl = 0.0
    # Track when option series steps (realization or MTM change) so the
    # "skip quiet days for closed positions" branch doesn't drop a real
    # event day. Without this an OTM-expiry crystallization (no fill in
    # stg_history → has_trade=False on close_date) would be silently
    # skipped from the rendered series.
    prev_options_realized_for_skip = 0.0
    prev_options_open_mtm_for_skip = 0.0

    for _, row in daily_df.iterrows():
        buy_qty = float(row.get("equity_buy_qty") or 0)
        buy_cost = float(row.get("equity_buy_cost") or 0)
        sell_qty = float(row.get("equity_sell_qty") or 0)
        sell_proceeds = float(row.get("equity_sell_proceeds") or 0)
        has_trade = bool(row.get("has_trade"))

        if has_trade:
            last_trade_date = row["date"]

        # Skip quiet days for closed positions — but DO NOT skip days
        # where the options series steps. Realization-on-close days
        # (especially OTM expiries that have no fill in stg_history)
        # would otherwise vanish from the chart. Compare today's
        # mart-side option fields against the most recent rendered
        # values: any change is a real event the user should see.
        cur_realized_for_skip = float(row.get("cumulative_options_pnl") or 0)
        cur_open_mtm_for_skip = float(row.get("open_options_unrealized_pnl") or 0)
        options_step_today = (
            cur_realized_for_skip != prev_options_realized_for_skip
            or cur_open_mtm_for_skip != prev_options_open_mtm_for_skip
        )
        if (position_is_closed
                and shares_held == 0
                and short_shares == 0
                and not has_trade
                and not options_step_today):
            continue
        prev_options_realized_for_skip = cur_realized_for_skip
        prev_options_open_mtm_for_skip = cur_open_mtm_for_skip

        # Trim the leading pre-open prefix. Until the position's first
        # activity, every series value is 0 and there are no holdings —
        # rendering those days makes the chart start before the position
        # existed. Mark "started" on the first fill (equity or option) or
        # the first non-zero cumulative series, then render every day after.
        if not position_started:
            _div_now = float(row.get("cumulative_dividends_pnl") or 0)
            _oth_now = float(row.get("cumulative_other_pnl") or 0)
            if (has_trade or buy_qty > 0 or sell_qty > 0
                    or cur_realized_for_skip != 0 or cur_open_mtm_for_skip != 0
                    or _div_now != 0 or _oth_now != 0):
                position_started = True
            else:
                continue

        # Process sells first (may create short position)
        if sell_qty > 0:
            remaining_sell = sell_qty
            remaining_proceeds = sell_proceeds
            if shares_held > 0:
                sold_long = min(remaining_sell, shares_held)
                avg = total_cost / shares_held if shares_held > 0 else 0
                frac = sold_long / sell_qty if sell_qty > 0 else 1
                sold_long_proceeds = sell_proceeds * frac
                cum_realized += sold_long_proceeds - avg * sold_long
                total_cost = max(0, total_cost - avg * sold_long)
                shares_held = max(0, shares_held - sold_long)
                remaining_sell -= sold_long
                remaining_proceeds -= sold_long_proceeds
            if remaining_sell > 0:
                short_shares += remaining_sell
                short_cost_basis += remaining_proceeds

        # Process buys (may cover short position)
        if buy_qty > 0:
            remaining_buy = buy_qty
            remaining_cost = buy_cost
            if short_shares > 0:
                covered = min(remaining_buy, short_shares)
                frac = covered / buy_qty if buy_qty > 0 else 1
                cover_cost = buy_cost * frac
                avg_short = short_cost_basis / short_shares if short_shares > 0 else 0
                cum_realized += avg_short * covered - cover_cost
                short_cost_basis = max(0, short_cost_basis - avg_short * covered)
                short_shares = max(0, short_shares - covered)
                remaining_buy -= covered
                remaining_cost -= cover_cost
            if remaining_buy > 0:
                shares_held += remaining_buy
                total_cost += remaining_cost

        close = float(row.get("close_price") or 0)
        # If no close price on a buy day, use avg cost so open position doesn't show full cost as "loss"
        if close <= 0 and buy_qty > 0 and buy_cost > 0 and shares_held > 0:
            close = buy_cost / buy_qty
        unrealized = 0
        if close > 0:
            if shares_held > 0:
                unrealized = shares_held * close - total_cost
            if short_shares > 0:
                unrealized -= (short_shares * close - short_cost_basis)
        eq_pnl = cum_realized + unrealized

        # Options P&L = realize-on-close cumulative + open-contract MTM
        # at this date. mart_daily_pnl exposes both halves separately
        # (see model header for the attribution rule); the chart simply
        # sums them. Post-fix this means a STO premium does NOT appear
        # as a step on STO date — instead the option contributes daily
        # MTM until close_date, then crystallizes at the realized total.
        # See AGENTS.md "Option P&L Attribution".
        cum_realized_opt = float(row.get("cumulative_options_pnl") or 0)
        open_unreal_opt = float(row.get("open_options_unrealized_pnl") or 0)
        opt_pnl = cum_realized_opt + open_unreal_opt
        div_pnl = float(row.get("cumulative_dividends_pnl") or 0)
        oth_pnl = float(row.get("cumulative_other_pnl") or 0)
        last_cumulative_other_pnl = oth_pnl
        last_cumulative_options_realized = cum_realized_opt
        last_open_options_unrealized = open_unreal_opt

        dates.append(str(row["date"])[:10])
        equity_s.append(round(eq_pnl, 2))
        options_s.append(round(opt_pnl, 2))
        dividends_s.append(round(div_pnl, 2))
        total_s.append(round(eq_pnl + opt_pnl + div_pnl + oth_pnl, 2))
        # Underlying close for the chart: use whenever the mart has a price.
        # Do not require shares_held > 0 here — that failed when the chart date range
        # starts after the equity open (leg filter) or carry-forward is missing rows.
        price_s.append(round(close, 2) if close > 0 else None)

    if not dates:
        return empty

    today_str = str(date.today())

    # Guard: BigQuery's ``current_date()`` runs in UTC and can be one
    # calendar day ahead of US local time after ~5pm PT. The mart's
    # dense spine therefore sometimes includes a "tomorrow" row from
    # the trader's perspective. Trim any rows past today so the chart
    # x-axis stops at today and the LIVE override below patches the
    # right cell. Pre-fix, the spine ended on UTC-tomorrow with stale
    # carry-forward values, the append-today branch added a duplicate
    # row out-of-order ([..., 5/11, 5/12, 5/11]), and the chart's
    # "terminal" sat on the wrong index — DELL ••••0044 stayed on
    # pre-fix int_equity_sessions arithmetic instead of the live
    # snapshot mv − cb.
    while dates and dates[-1] > today_str:
        dates.pop()
        equity_s.pop()
        options_s.pop()
        dividends_s.pop()
        total_s.pop()
        price_s.pop()

    if not current_df.empty:
        # LIVE TODAY OVERRIDE.
        #
        # The mart's dense date spine emits a row for current_date()
        # (and the contract daily-pnl spine extends to today for
        # currently-owned contracts via the ``currently_owned`` CTE
        # in ``int_option_contract_daily_pnl``). That row reflects
        # the LATEST DAILY SNAPSHOT, which can be 1-3 trading days
        # stale (Schwab's nightly sync hasn't booked today yet, or
        # the user's connection paused). For "today" we override the
        # mart's row with values computed from ``current_df`` (which
        # comes from ``int_enriched_current``) so the chart's terminal
        # matches the headline KPIs / positions_summary / Breakdown-by-
        # type — all of which read the SAME ``int_enriched_current``.
        #
        # CLOSE-BASED REPORTING (June 2026): ``int_enriched_current``
        # now prices today's EQUITY at the official yfinance close once
        # it is published (after the bell), falling back to the broker
        # live mark only intraday — see int_enriched_current header +
        # AGENTS.md "Pricing Precedence". So this override automatically
        # uses the close when published; we no longer paint the broker's
        # transient after-hours mark onto the terminal. Options/cash stay
        # broker-derived. Because both the mart today-row and this override
        # resolve to close-when-published, the chart terminal == hero by
        # construction (the reconciliation invariant) with no rescaling.
        #
        # When the chart already ends at today (mart spine), REPLACE
        # the last row's equity/options/total with the live-derived
        # numbers. When the chart ends before today (rare — happens
        # when the position has zero mart history), APPEND today.
        #
        # Pre-fix the patch only fired on APPEND (``dates[-1] != today``)
        # because the mart used to leave today empty. After the dense-
        # spine rework, today is always present and the patch was being
        # silently skipped, so the chart "snapped to 0" or "stuck on
        # the last snapshot" while positions_summary read live MTM.
        # That tripped the reconciliation invariant on every position
        # whose snapshot table lagged stg_current (real example May
        # 2026: JPM 0044 chart=$320 vs strategy_breakdown=$30,940).
        #
        # Using ``unrealized_pnl`` (not ``market_value``) matches the
        # snapshot-derived MTM used in mart_daily_pnl; current_df came
        # from int_enriched_current which has the corrected sign.
        # See AGENTS.md "Option P&L Attribution".
        opt_mask = current_df["instrument_type"].isin(["Call", "Put"])
        if "option_expiry" in current_df.columns:
            today_ts = pd.Timestamp(date.today())
            opt_expiry_series = pd.to_datetime(
                current_df["option_expiry"], errors="coerce"
            )
            opt_mask = opt_mask & (
                opt_expiry_series.isna() | (opt_expiry_series >= today_ts)
            )
        if "unrealized_pnl" in current_df.columns:
            opt_unreal_today = float(
                current_df.loc[opt_mask, "unrealized_pnl"].sum()
            )
        elif "market_value" in current_df.columns:
            opt_unreal_today = float(
                current_df.loc[opt_mask, "market_value"].sum()
            )
        else:
            opt_unreal_today = last_open_options_unrealized
        today_option_pnl = last_cumulative_options_realized + opt_unreal_today
        eq_row = _equity_slice_for_live_chart(current_df)
        today_eq = equity_s[-1]
        # When the broker's live snapshot has equity AND a current
        # price, prefer the snapshot's unrealized columns. Sum of
        # ``unrealized_pnl`` matches positions_summary / Breakdown-by-
        # type and works even when the mart trade-history walker thinks
        # shares_flat (e.g. bogus same-day churn in ``mart_daily_pnl``).
        #
        # If we only trusted mv−cb and (mv,cb) were both falsy because
        # columns were missing, we fell through to ``shares_held > 0`` —
        # but that's false when the walker already flattened the lot —
        # so we'd leave ``today_eq`` at the walker terminal (pure
        # realized −\$1,957) while KPIs added +\$349 broker unreal —
        # IYW invariant gap (May 2026).
        if not eq_row.empty:
            if "unrealized_pnl" in eq_row.columns:
                ur_sum = pd.to_numeric(
                    eq_row["unrealized_pnl"], errors="coerce"
                ).fillna(0.0).sum()
                today_eq = cum_realized + float(ur_sum)
            else:
                mv_col = (
                    float(eq_row["market_value"].sum())
                    if "market_value" in eq_row.columns else 0.0
                )
                # ``cost_basis`` is the canonical name (int_enriched_current,
                # CURRENT_POSITIONS_QUERY). ``cost_bases`` is the original
                # CSV-seed typo that survives in some test fixtures and the
                # raw ``current_positions`` seed schema; accept either so
                # this helper works against both production and test data.
                cb_col = 0.0
                for cb_name in ("cost_basis", "cost_bases"):
                    if cb_name in eq_row.columns:
                        cb_col = float(eq_row[cb_name].sum())
                        break
                unreal_snap = (mv_col - cb_col) if (mv_col or cb_col) else None
                if unreal_snap is not None:
                    today_eq = cum_realized + unreal_snap
                elif shares_held > 0 or short_shares > 0:
                    p = float(eq_row["current_price"].iloc[0] or 0)
                    if p:
                        unreal = 0
                        if shares_held > 0:
                            unreal = shares_held * p - total_cost
                        if short_shares > 0:
                            unreal -= (short_shares * p - short_cost_basis)
                        today_eq = cum_realized + unreal
        today_price = None
        if not eq_row.empty and "current_price" in eq_row.columns:
            cp_nonnull = pd.to_numeric(eq_row["current_price"], errors="coerce").dropna()
            today_price = float(cp_nonnull.iloc[0]) if len(cp_nonnull) else None

        if dates[-1] == today_str:
            equity_s[-1] = round(today_eq, 2)
            options_s[-1] = round(today_option_pnl, 2)
            total_s[-1] = round(
                today_eq + today_option_pnl + dividends_s[-1]
                + last_cumulative_other_pnl,
                2,
            )
            if today_price is not None:
                price_s[-1] = round(today_price, 2)
        else:
            dates.append(today_str)
            equity_s.append(round(today_eq, 2))
            options_s.append(round(today_option_pnl, 2))
            dividends_s.append(dividends_s[-1])
            price_s.append(round(today_price, 2) if today_price else None)
            total_s.append(
                round(
                    today_eq + today_option_pnl + dividends_s[-1]
                    + last_cumulative_other_pnl,
                    2,
                )
            )

    return {
        "dates": dates,
        "equity": equity_s,
        "options": options_s,
        "dividends": dividends_s,
        "total": total_s,
        "underlying_price": price_s,
        "has_underlying_price": any(p is not None for p in price_s),
    }


@app.route("/symbols")
@login_required
def symbols_detail():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    try:
        dfs = _bq_parallel(client, {
            "trades": TRADES_QUERY.format(tenant_filter=tenant_filter),
            "current": CURRENT_POSITIONS_QUERY.format(tenant_filter=tenant_filter),
            "strat": STRATEGIES_MAP_QUERY.format(tenant_filter=tenant_filter),
            "pnl": SYMBOLS_PNL_QUERY.format(tenant_filter=tenant_filter),
            "open_start": OPEN_SESSION_START_QUERY.format(tenant_filter=tenant_filter),
            "closed_legs": CLOSED_LEGS_QUERY.format(
                closed_legs_tenant_filter=_tenant_sql_and(tenant_ids, col="sc.tenant_id")),
            "closed_equity": CLOSED_EQUITY_LEGS_QUERY.format(tenant_filter=tenant_filter),
        })
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        strat_df = dfs["strat"]
        pnl_df = dfs["pnl"]
        open_start_df = dfs["open_start"]
        closed_legs_df = dfs["closed_legs"]
        closed_equity_df = dfs["closed_equity"]
    except Exception as exc:
        app.logger.exception("Daily P&L load failed: %s", exc)
        return render_template(
            "symbols.html",
            title="Daily P&L",
            error=str(exc),
            symbol_data=[],
            chart_data_json="[]",
            accounts=[],
            selected_account="",
            open_only=False,
            linked_brokerage_accounts=(user_accounts or []),
            viewer_is_admin=is_admin(current_user.username),
        )

    trades_df = _df_normalize_account_column(trades_df)
    current_df = _df_normalize_account_column(current_df)
    strat_df = _df_normalize_account_column(strat_df)
    pnl_df = _df_normalize_account_column(pnl_df)
    open_start_df = _df_normalize_account_column(open_start_df)
    closed_legs_df = _df_normalize_account_column(closed_legs_df)
    closed_equity_df = _df_normalize_account_column(closed_equity_df)

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    if not trades_df.empty:
        for col in ["amount", "quantity", "price", "fees"]:
            if col in trades_df.columns:
                trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
        if "trade_date" in trades_df.columns:
            trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)
    if "unrealized_pnl_pct" in current_df.columns:
        current_df["unrealized_pnl_pct"] = pd.to_numeric(current_df["unrealized_pnl_pct"], errors="coerce").fillna(0)

    # Strategy map: (account, symbol) → sorted list of strategies
    strat_map = (
        strat_df.groupby(["account", "symbol"])["strategy"]
        .apply(lambda x: sorted(x.unique().tolist()))
        .to_dict()
    )

    # Open session start map: (account, symbol) → open_start date
    open_start_map = {}
    if "open_start_df" in locals() and not open_start_df.empty:
        for _, row in open_start_df.iterrows():
            key = (str(row["account"]), str(row["symbol"]))
            open_start_map[key] = row["open_start"]

    # Normalize closed_legs_df for date filtering
    if "closed_legs_df" in locals() and not closed_legs_df.empty:
        closed_legs_df["close_date"] = pd.to_datetime(closed_legs_df["close_date"], errors="coerce").dt.date
    else:
        closed_legs_df = pd.DataFrame()

    # ------------------------------------------------------------------
    # Safety-belt: re-filter in Python (SQL already filtered by account)
    # ------------------------------------------------------------------
    trades_df = _filter_df_by_tenant_ids(trades_df, tenant_ids)
    current_df = _filter_df_by_tenant_ids(current_df, tenant_ids)
    strat_df = _filter_df_by_tenant_ids(strat_df, tenant_ids)
    pnl_df = _filter_df_by_tenant_ids(pnl_df, tenant_ids)
    if not open_start_df.empty:
        open_start_df = _filter_df_by_tenant_ids(open_start_df, tenant_ids)
    if not closed_legs_df.empty:
        closed_legs_df = _filter_df_by_tenant_ids(closed_legs_df, tenant_ids)
    if not closed_equity_df.empty:
        closed_equity_df = _filter_df_by_tenant_ids(closed_equity_df, tenant_ids)

    def _unique_accounts(*frames):
        s = set()
        for f in frames:
            if f is not None and not f.empty and "account" in f.columns:
                for v in f["account"].dropna().unique():
                    t = str(v).strip()
                    if t:
                        s.add(t)
        return sorted(s)

    accounts = _unique_accounts(trades_df, pnl_df, current_df, strat_df)
    # Picker lists the full disambiguated account set (non-admin) so every
    # physical account stays selectable after tenant scope narrows the data.
    if user_accounts:
        accounts = sorted(
            {str(a).strip() for a in user_accounts if a and str(a).strip()}
        )
    selected_account = request.args.get("account", "")
    # Use getlist so duplicate params (e.g. open_only=1&open_only=0 from checkbox+hidden) don't break the filter
    open_only = "1" in request.args.getlist("open_only")
    positions_only = "1" in request.args.getlist("positions_only")

    # Redirect to canonical URL if duplicate params present (cleans bookmarks/cached URLs)
    open_list = request.args.getlist("open_only")
    pos_list = request.args.getlist("positions_only")
    if len(open_list) > 1 or len(pos_list) > 1:
        q = {"account": selected_account} if selected_account else {}
        if open_only:
            q["open_only"] = "1"
        if positions_only:
            q["positions_only"] = "1"
        return redirect(url_for("symbols_detail", **q))

    # "Open positions only" implies "symbols with open positions" filter.
    # If the user checks the second box but not the first, we still want
    # to restrict the symbol list to those with an open position.
    if positions_only and not open_only:
        open_only = True

    # No secondary ``account == selected_account`` narrowing: tenant scope
    # already filtered every frame to the selected account's tenant_id
    # (handles disambiguated colliding labels too).

    # Restrict to symbols that have a current open position (match current_positions / int_enriched_current)
    if open_only:
        open_pairs = set(zip(current_df["account"].astype(str), current_df["symbol"].astype(str))) if not current_df.empty else set()
    else:
        open_pairs = None

    # Fetch pre-aggregated chart data from mart
    try:
        tenant_filter = _tenant_sql_and(_tenants_for_scope(selected_account))
        all_chart_df = cached_query_df(
            client,
            CHART_DATA_ALL_QUERY.format(tenant_filter=tenant_filter)
        )
        all_chart_df = _filter_df_by_tenant_ids(all_chart_df, tenant_ids)
        # tenant scope already narrowed to the selected account's tenant
    except Exception:
        all_chart_df = pd.DataFrame()

    # ------------------------------------------------------------------
    # Build per-symbol data
    # ------------------------------------------------------------------
    symbol_data = []
    chart_data_list = []

    for account, symbol in _iter_symbols_for_daily_detail(
        trades_df, pnl_df, current_df, open_pairs
    ):
        group = trades_df[
            (trades_df["account"] == account) & (trades_df["symbol"] == symbol)
        ]
        if not group.empty and "trade_date" in group.columns:
            group = group.sort_values("trade_date")

        sym_current = current_df[
            (current_df["account"] == account) & (current_df["symbol"] == symbol)
        ]

        # Realized P&L: use positions_summary when available so mixed
        # open/closed symbols (e.g. RKLB) show historical realized plus
        # current unrealized. For symbols that are purely open (only
        # Open status and no closed trades), treat realized as 0.
        sym_pnl = pnl_df[
            (pnl_df["account"] == account) & (pnl_df["symbol"] == symbol)
        ]
        if not sym_pnl.empty:
            statuses = (
                sym_pnl["status"]
                .dropna()
                .astype(str)
                .str.lower()
                .str.strip()
                .unique()
                .tolist()
            )
            has_open = any(s == "open" for s in statuses)
            has_closed = any(s == "closed" for s in statuses)
            realized_val = float(sym_pnl["realized_pnl"].sum() or 0.0)
            # Purely open symbol (no closed legs): no realized yet, even
            # if the mart currently reports a negative net cash flow.
            if has_open and not has_closed:
                realized_val = 0.0
            total_realized = round(realized_val, 2)
        else:
            # Fallback: net cash flow from trades if mart row missing.
            total_realized = (
                round(float(group["amount"].sum()), 2)
                if not group.empty and "amount" in group.columns
                else 0.0
            )

        # Unrealized from current open positions (matches current positions table)
        unrealized = round(float(sym_current["unrealized_pnl"].sum()), 2) if not sym_current.empty else 0.0
        equity_open_pnl = round(
            float(sym_current.loc[sym_current["instrument_type"] == "Equity", "unrealized_pnl"].sum()), 2
        ) if not sym_current.empty else 0.0
        options_open_pnl = round(
            float(sym_current.loc[sym_current["instrument_type"].isin(["Call", "Put"]), "unrealized_pnl"].sum()), 2
        ) if not sym_current.empty else 0.0

        # Closed legs that belong to this position (closed on or after open_start).
        # For \"open positions only\", prefer the precomputed open_start_map; if it's
        # missing, fall back to the first trade date so we anchor to the current run.
        open_key = (str(account), str(symbol))
        open_start_val = open_start_map.get(open_key) if positions_only else None
        if positions_only and open_start_val is None and not group.empty:
            open_start_val = group["trade_date"].min()

        strategies = strat_map.get((account, symbol), [])

        if not closed_legs_df.empty and open_start_val is not None:
            open_start_date = pd.to_datetime(open_start_val).date()
            # The date range (open_start_date to present) is already anchored to the
            # current position's equity session start date (from int_strategy_classification).
            # That is the correct and sufficient filter — any option that closed on or
            # after the position opened belongs to this position.  Strategy-label
            # filtering is removed because it excluded legs whose classification differed
            # slightly from the live open strategy (e.g. expired-worthless covered calls
            # inferred as Closed via option_expiry, or PMCC short legs labelled differently
            # from the open long-call anchor).
            legs = closed_legs_df[
                (closed_legs_df["account"] == account)
                & (closed_legs_df["symbol"] == symbol)
                & (closed_legs_df["close_date"] >= open_start_date)
            ]
            closed_legs_list = legs.sort_values("close_date").to_dict(orient="records")
            for r in closed_legs_list:
                r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
                r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
                r["total_pnl"] = round(float(r.get("total_pnl") or 0), 2)
        else:
            closed_legs_list = []

        # Closed equity legs (shares sold / called away) within this position.
        closed_equity_list = []
        if not closed_equity_df.empty and open_start_val is not None:
            open_start_date = pd.to_datetime(open_start_val).date()
            eq_legs = closed_equity_df[
                (closed_equity_df["account"] == account)
                & (closed_equity_df["symbol"] == symbol)
                & (closed_equity_df["close_date"] >= open_start_date)
            ]
            closed_equity_list = eq_legs.sort_values("close_date").to_dict(orient="records")
            for r in closed_equity_list:
                r["open_date"] = str(r["open_date"]) if pd.notna(r.get("open_date")) else ""
                r["close_date"] = str(r["close_date"]) if pd.notna(r.get("close_date")) else ""
                r["realized_pnl"] = round(float(r.get("realized_pnl") or 0), 2)

        # Total closed P&L = option legs + equity legs
        closed_options_pnl = round(sum(float(r.get("total_pnl") or 0) for r in closed_legs_list), 2)
        closed_equity_pnl = round(sum(float(r.get("realized_pnl") or 0) for r in closed_equity_list), 2)
        closed_legs_pnl = round(closed_options_pnl + closed_equity_pnl, 2)

        # Display semantics:
        # - Default view: total_return = realized (history) + unrealized (current)
        # - "Open positions only" view: show this position's closed legs + current open P&L.
        display_realized = total_realized
        display_total = round(total_realized + unrealized, 2)
        if positions_only:
            display_realized = closed_legs_pnl
            display_total = round(closed_legs_pnl + unrealized, 2)

        if not group.empty and "trade_date" in group.columns:
            num_trades = len(group)
            first_date = str(group["trade_date"].min())
            last_date = str(group["trade_date"].max())
        else:
            num_trades = 0
            first_date = ""
            last_date = ""

        sym_chart_df = all_chart_df[
            (all_chart_df["account"] == account) & (all_chart_df["symbol"] == symbol)
        ] if not all_chart_df.empty else pd.DataFrame()

        # For "Open positions only", clip the daily P&L series to the open
        # session start so the chart focuses on the live leg while still using
        # true end-of-day prices from mart_daily_pnl.
        if positions_only and open_start_val is not None and not sym_chart_df.empty and "date" in sym_chart_df.columns:
            sym_chart_df = sym_chart_df[sym_chart_df["date"] >= pd.to_datetime(open_start_val)]
            if not sym_chart_df.empty and not group.empty and "trade_date" in group.columns:
                first_date = str(
                    min(group["trade_date"].max(), sym_chart_df["date"].min())
                )

        with timed("symbol_charts"):
            chart = cached_payload(
                ("sym_chart", str(date.today()), frame_fingerprint(sym_chart_df, sym_current)),
                lambda sdf=sym_chart_df, scur=sym_current: _build_chart_from_daily_pnl(sdf, scur),
            )

        # When viewing "this position only", rebase chart so it starts at 0
        # (first point = start of position, not cumulative from prior history)
        if positions_only and chart.get("dates") and len(chart["dates"]) > 0:
            base_equity = chart["equity"][0] if chart["equity"] else 0
            base_options = chart["options"][0] if chart["options"] else 0
            base_dividends = chart["dividends"][0] if chart["dividends"] else 0
            base_total = chart["total"][0] if chart["total"] else 0
            chart["equity"] = [round(x - base_equity, 2) for x in chart["equity"]]
            chart["options"] = [round(x - base_options, 2) for x in chart["options"]]
            chart["dividends"] = [round(x - base_dividends, 2) for x in chart["dividends"]]
            chart["total"] = [round(x - base_total, 2) for x in chart["total"]]
            # If this position has no open equity (options-only), strip equity from the
            # chart so we don't show phantom spikes from past equity trades in the mart.
            has_open_equity = not sym_current.empty and (
                (sym_current["instrument_type"] == "Equity").any()
            )
            if not has_open_equity:
                n = len(chart["dates"])
                for i in range(n):
                    chart["total"][i] = round(chart["total"][i] - chart["equity"][i], 2)
                    chart["equity"][i] = 0
            # Anchor the last options point to closed OPTION legs + current open
            # option unrealized only.  Equity realized P&L (shares sold/called away)
            # is already captured by the natural avg-cost equity calculation and must
            # not be added to the options series — doing so double-counts it and
            # causes a spurious drop to -$3k on the final data point.
            chart["options"][-1] = round(closed_options_pnl + options_open_pnl, 2)
            chart["total"][-1] = round(
                chart["equity"][-1] + chart["options"][-1] + chart["dividends"][-1], 2
            )

        chart_data_list.append(chart)

        # Trade table rows (convert dates to str for Jinja)
        trades_table = group.copy()
        trades_table["trade_date"] = trades_table["trade_date"].astype(str)
        trades_list = trades_table.to_dict(orient="records")

        # Positions table rows: combine open positions from current snapshot
        # with closed legs for this position, and add a status column.
        current_list = sym_current.to_dict(orient="records") if not sym_current.empty else []
        combined_positions = []
        # Position-level open date (for equity / fallback) — reuse open_start_val
        open_start_str = None
        if open_start_val is not None:
            try:
                open_start_str = str(pd.to_datetime(open_start_val).date())
            except Exception:
                open_start_str = None

        # Per-option open date from transaction history (sell_to_open / buy_to_open).
        # The current snapshot doesn't carry open dates, so we look up each
        # option's trade_symbol in the trade history to find its opening trade.
        option_open_date_map: dict = {}
        if not group.empty and "action" in group.columns and "trade_symbol" in group.columns:
            open_actions = {"option_sell_to_open", "option_buy_to_open"}
            opt_opens = group[
                group["action"].astype(str).str.lower().str.strip().isin(open_actions)
            ]
            for _, trade_row in opt_opens.iterrows():
                ts = str(trade_row.get("trade_symbol", "")).strip()
                td = trade_row.get("trade_date")
                if ts and td is not None:
                    td_str = str(td)
                    if ts not in option_open_date_map or td_str < option_open_date_map[ts]:
                        option_open_date_map[ts] = td_str

        for row in current_list:
            r = dict(row)
            r["status"] = "Open"
            ts = str(r.get("trade_symbol", "")).strip()
            if r.get("instrument_type") in ("Call", "Put") and ts in option_open_date_map:
                r["open_date"] = option_open_date_map[ts]
            else:
                r["open_date"] = open_start_str
            r["close_date"] = ""
            combined_positions.append(r)

        # Closed legs within the current open session always show in the
        # Positions table so you can see the full story of the live position.
        for leg in closed_legs_list:
            direction = str(leg.get("direction") or "")
            prem_recv = float(leg.get("premium_received") or 0)
            prem_paid = float(leg.get("premium_paid") or 0)
            cost_close = float(leg.get("cost_to_close") or 0)
            proceeds_close = float(leg.get("proceeds_from_close") or 0)
            if direction == "Sold":
                leg_cost = abs(cost_close)
                leg_proceeds = abs(prem_recv)
            else:
                leg_cost = abs(prem_paid)
                leg_proceeds = abs(proceeds_close)
            opt_pnl = float(leg.get("total_pnl") or 0)
            opt_return_pct = round(opt_pnl / leg_cost * 100, 2) if leg_cost else None
            combined_positions.append({
                "status": "Closed",
                "trade_symbol": leg.get("trade_symbol"),
                "description": leg.get("strategy") or "",
                "quantity": leg.get("quantity"),
                "current_price": None,
                "market_value": round(leg_proceeds, 2) if leg_proceeds else None,
                "cost_basis": round(leg_cost, 2) if leg_cost else None,
                "unrealized_pnl": opt_pnl,
                "unrealized_pnl_pct": opt_return_pct,
                "open_date": leg.get("open_date") or "",
                "close_date": leg.get("close_date") or "",
            })

        # Closed equity legs (shares sold / called away).
        for leg in closed_equity_list:
            eq_proceeds = float(leg.get("sell_proceeds") or 0)
            eq_cost = float(leg.get("cost_basis") or 0)
            eq_pnl = float(leg.get("realized_pnl") or 0)
            eq_return_pct = round(eq_pnl / eq_cost * 100, 2) if eq_cost else None
            combined_positions.append({
                "status": "Closed",
                "trade_symbol": leg.get("trade_symbol") or symbol,
                "description": leg.get("description") or "Equity Sold",
                "quantity": leg.get("quantity"),
                "current_price": leg.get("sale_price_per_share"),
                "market_value": round(eq_proceeds, 2) if eq_proceeds else None,
                "cost_basis": round(eq_cost, 2) if eq_cost else None,
                "unrealized_pnl": eq_pnl,
                "unrealized_pnl_pct": eq_return_pct,
                "open_date": leg.get("open_date") or "",
                "close_date": leg.get("close_date") or "",
            })

        # Quick story stats for this symbol/position (across option + equity legs)
        all_closed_for_stats = [
            *closed_legs_list,
            *[{
                "trade_symbol": r.get("trade_symbol") or symbol,
                "strategy": r.get("description") or "Equity Sold",
                "close_date": r.get("close_date") or "",
                "total_pnl": r.get("realized_pnl", 0),
            } for r in closed_equity_list],
        ]
        best_leg = None
        worst_leg = None
        if all_closed_for_stats:
            best_leg = max(all_closed_for_stats, key=lambda r: r.get("total_pnl", 0))
            worst_leg = min(all_closed_for_stats, key=lambda r: r.get("total_pnl", 0))

        open_start_val = open_start_map.get((str(account), str(symbol)))
        days_in_position = None
        if open_start_val is not None:
            try:
                days_in_position = (date.today() - pd.to_datetime(open_start_val).date()).days
            except Exception:
                days_in_position = None

        open_legs_count = sum(1 for r in combined_positions if r.get("status") == "Open")
        closed_legs_count = sum(1 for r in combined_positions if r.get("status") == "Closed")

        symbol_data.append({
            "account": account,
            "symbol": symbol,
            "total_realized": display_realized,
            "unrealized": unrealized,
            "total_return": display_total,
            "num_trades": num_trades,
            "first_date": first_date,
            "last_date": last_date,
            "strategies": strategies,
            "trades": trades_list,
            "current_positions": combined_positions,
            "story_days_in_position": days_in_position,
            "story_open_legs": open_legs_count,
            "story_closed_legs": closed_legs_count,
            "story_best_leg": best_leg,
            "story_worst_leg": worst_leg,
            "_chart_idx": len(chart_data_list) - 1,
        })

    # Sort by total return descending; rebuild chart list in matching order
    symbol_data.sort(key=lambda x: x["total_return"], reverse=True)
    sorted_charts = [chart_data_list[item["_chart_idx"]] for item in symbol_data]
    for item in symbol_data:
        del item["_chart_idx"]

    # Resolve the active symbol for the tab strip. Honor ?symbol= when it
    # matches a tab (cheap defense against stale bookmarks); otherwise fall
    # back to the top-of-sort row (current "P&L desc" default).
    requested_symbol = (request.args.get("symbol") or "").strip().upper()
    available_symbols = {str(s.get("symbol") or "").upper() for s in symbol_data}
    active_symbol = (
        requested_symbol
        if requested_symbol and requested_symbol in available_symbols
        else (symbol_data[0]["symbol"] if symbol_data else "")
    )

    return render_template(
        "symbols.html",
        title="Daily P&L",
        symbol_data=symbol_data,
        # `tabs` is the same list of dicts; the partial reads
        # {symbol, account, total_return, num_trades, story_open_legs, strategies}
        tabs=symbol_data,
        active_symbol=active_symbol,
        mode="swap",
        chart_data_json=json.dumps(sorted_charts),
        accounts=accounts,
        selected_account=selected_account,
        open_only=open_only,
        positions_only=positions_only,
        linked_brokerage_accounts=(user_accounts or []),
        viewer_is_admin=is_admin(current_user.username),
    )


# ======================================================================
# Account Performance  (/accounts)
# ======================================================================

ACCOUNT_BALANCES_QUERY = """
    SELECT account, row_type, market_value, cost_basis,
           unrealized_pnl, unrealized_pnl_pct, percent_of_account
    FROM `ccwj-dbt.analytics.stg_account_balances`
    WHERE 1=1 {tenant_filter}
"""

STRATEGY_CLASSIFICATION_QUERY = """
    SELECT account, symbol, strategy, status, open_date, close_date,
           total_pnl, num_trades
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE 1=1 {tenant_filter}
"""

ACCOUNT_POSITIONS_SUMMARY_QUERY = """
    SELECT account, strategy,
           SUM(total_pnl) AS total_pnl,
           SUM(realized_pnl) AS realized_pnl,
           SUM(unrealized_pnl) AS unrealized_pnl,
           SUM(total_premium_received) AS premium_received,
           SUM(total_premium_paid) AS premium_paid,
           SUM(num_individual_trades) AS num_trades,
           SUM(num_winners) AS num_winners,
           SUM(num_losers) AS num_losers,
           SUM(total_dividend_income) AS dividend_income,
           SUM(total_return) AS total_return
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {tenant_filter}
    GROUP BY account, strategy
    ORDER BY account, strategy
"""


def _build_account_chart_from_daily_pnl(daily_df, current_df):
    """
    Build account-level cumulative P&L chart from mart_daily_pnl.

    Aggregates across all symbols.  Options/dividends/other use running
    sums of daily amounts.  Equity requires per-symbol average-cost tracking.
    """
    empty = {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}
    if daily_df.empty:
        return empty

    daily_df = _collapse_mart_daily_pnl_duplicate_grain(daily_df)
    daily_df = daily_df.sort_values("date")
    all_dates = sorted(daily_df["date"].dropna().unique())

    # Equity cost-basis state and per-symbol realized options are keyed by
    # the broker-stable tenant_id (v2 grain) when present, so several
    # physical accounts sharing a display label (e.g. multiple "Schwab
    # Account"s) don't fuse one symbol's running average-cost state.
    _has_tenant = "tenant_id" in daily_df.columns

    def _eq_key(r):
        if _has_tenant and pd.notna(r.get("tenant_id")):
            return (r.get("tenant_id"), r["symbol"])
        return (r["account"], r["symbol"])

    eq_state = {}
    cum_div = cum_oth = 0.0
    dates_out, equity_s, options_s, dividends_s, total_s = [], [], [], [], []

    # Account-level options P&L follows the same realize-on-close +
    # MTM-while-open rule as the position page (see AGENTS.md
    # "Option P&L Attribution"). For each day:
    #   - cumulative_options_pnl is already realized cumulative across
    #     all closed contracts as of that date. Per-symbol values are
    #     additive across symbols (each contract appears in exactly one
    #     symbol's series).
    #   - open_options_unrealized_pnl is point-in-time MTM of all open
    #     contracts on this date. Sum across symbols.
    # Pre-fix this routine ran ``cum_opt += sum(options_amount)``,
    # which credited STO premium on STO date — the position-page bug
    # except worse because it couldn't even mark-to-market.
    options_per_symbol_realized = {}  # (account, symbol) -> last realized cum

    for d in all_dates:
        day = daily_df[daily_df["date"] == d]

        # Update per-symbol realized cumulative from the mart (carried
        # forward across days when no new realization happened).
        for _, r in day.iterrows():
            key = _eq_key(r)
            options_per_symbol_realized[key] = float(
                r.get("cumulative_options_pnl") or 0
            )
        realized_total = sum(options_per_symbol_realized.values())

        # Open MTM at this date is sum across the (account, symbol)
        # rows present today. Symbols with no row today contribute 0
        # (per-contract spine ends at close_date — see
        # int_option_contract_daily_pnl).
        open_mtm_total = float(day.get(
            "open_options_unrealized_pnl",
            pd.Series(dtype=float),
        ).fillna(0).sum()) if "open_options_unrealized_pnl" in day.columns else 0.0

        cum_opt = realized_total + open_mtm_total
        cum_div += float(day["dividends_amount"].sum())
        cum_oth += float(day["other_amount"].sum())

        for _, row in day.iterrows():
            key = _eq_key(row)
            if key not in eq_state:
                eq_state[key] = {"shares": 0.0, "cost": 0.0, "realized": 0.0}
            s = eq_state[key]
            bq = float(row.get("equity_buy_qty") or 0)
            bc = float(row.get("equity_buy_cost") or 0)
            sq = float(row.get("equity_sell_qty") or 0)
            sp = float(row.get("equity_sell_proceeds") or 0)

            if bq > 0:
                s["shares"] += bq
                s["cost"] += bc
            if sq > 0 and s["shares"] > 0:
                avg = s["cost"] / s["shares"]
                sold = min(sq, s["shares"])
                s["realized"] += sp - avg * sold
                s["cost"] = max(0, s["cost"] - avg * sold)
                s["shares"] = max(0, s["shares"] - sold)
            elif sq > 0:
                s["realized"] += sp

        eq_total = sum(s["realized"] for s in eq_state.values())
        for _, row in day.iterrows():
            key = _eq_key(row)
            s = eq_state[key]
            close = float(row.get("close_price") or 0)
            if close > 0 and s["shares"] > 0:
                eq_total += s["shares"] * close - s["cost"]

        dates_out.append(str(d)[:10])
        equity_s.append(round(eq_total, 2))
        options_s.append(round(cum_opt, 2))
        dividends_s.append(round(cum_div, 2))
        total_s.append(round(eq_total + cum_opt + cum_div + cum_oth, 2))

    today = date.today()
    today_str = str(today)
    if not current_df.empty and dates_out and dates_out[-1] != today_str:
        # Synthetic today row when the mart hasn't been built yet for
        # today (sync ran but dbt hasn't refreshed yet).
        #
        # Equity: keep the legacy behavior of adding today's snapshot
        # unrealized to the last mart-day equity value. There's a
        # well-known dimensional issue here (equity_s[-1] already
        # includes mark-to-market at the mart's close price for that
        # day, so adding today's unrealized double-counts when the
        # mart is fresh as of yesterday). Pre-existing; out of scope
        # for the option-attribution rewrite.
        #
        # CLOSE-BASED REPORTING (June 2026): ``current_df`` comes from
        # ``int_enriched_current``, whose equity unrealized is now priced
        # at the official close once published (broker live mark only
        # intraday). So this synthetic today row snaps to the close too,
        # matching mart_account_equity_daily / the account hero. See
        # AGENTS.md "Pricing Precedence".
        #
        # Options: under realize-on-close, the right value is
        #   today_options = (last realized cumulative across symbols)
        #                 + (LIVE open MTM from current_df today)
        # This is a REPLACEMENT not an addition: the last loop
        # iteration's options_s value already had open MTM for the
        # mart's last day, and we want today's broker MTM instead.
        eq_unreal = float(current_df.loc[current_df["instrument_type"] == "Equity", "unrealized_pnl"].sum())
        # Filter to genuinely-open option contracts (calendar beats
        # stale snapshot — see _build_chart_from_daily_pnl for the
        # same rationale).
        opt_mask = current_df["instrument_type"].isin(["Call", "Put"])
        if "option_expiry" in current_df.columns:
            today_ts = pd.Timestamp(date.today())
            opt_expiry_series = pd.to_datetime(
                current_df["option_expiry"], errors="coerce"
            )
            opt_mask = opt_mask & (
                opt_expiry_series.isna() | (opt_expiry_series >= today_ts)
            )
        opt_unreal_today = float(
            current_df.loc[opt_mask, "unrealized_pnl"].sum()
        )
        last_realized_total = sum(options_per_symbol_realized.values())
        today_options = round(last_realized_total + opt_unreal_today, 2)
        if eq_unreal != 0 or today_options != options_s[-1]:
            dates_out.append(today_str)
            equity_s.append(round(equity_s[-1] + eq_unreal, 2))
            options_s.append(today_options)
            dividends_s.append(dividends_s[-1])
            total_s.append(round(equity_s[-1] + today_options + dividends_s[-1] + cum_oth, 2))

    return {
        "dates": dates_out,
        "equity": equity_s,
        "options": options_s,
        "dividends": dividends_s,
        "total": total_s,
    }


def _build_strategy_time_chart(strat_df):
    """
    Build cumulative P&L over time per strategy from trade-group data.
    Closed groups → P&L attributed to close_date.
    Open groups   → P&L attributed to today.
    """
    if strat_df.empty:
        return {"dates": [], "series": {}}

    today = date.today()
    rows = []
    for _, r in strat_df.iterrows():
        pnl_date = r["close_date"] if r["status"] == "Closed" and pd.notna(r["close_date"]) else today
        rows.append({"strategy": r["strategy"], "pnl_date": pnl_date, "pnl": float(r["total_pnl"])})

    events = pd.DataFrame(rows)
    events["pnl_date"] = pd.to_datetime(events["pnl_date"]).dt.date

    # Sum P&L per (strategy, date)
    grouped = events.groupby(["strategy", "pnl_date"])["pnl"].sum().reset_index()
    strategies = sorted(grouped["strategy"].unique())
    all_dates = sorted(grouped["pnl_date"].unique())

    series = {}
    for strat in strategies:
        strat_data = grouped[grouped["strategy"] == strat].set_index("pnl_date")["pnl"]
        cum = 0.0
        vals = []
        for d in all_dates:
            cum += float(strat_data.get(d, 0))
            vals.append(round(cum, 2))
        series[strat] = vals

    return {
        "dates": [str(d) for d in all_dates],
        "series": series,
    }


# ======================================================================
# Sectors  (/sectors)
# ======================================================================
#
# Sector / subsector rollup of positions_summary, scoped to the logged-in
# user's accounts. Powers the "Sectors" page in the Portfolio nav.
# (Originally /industries — renamed to standardize on the finance term
# "sector → subsector" hierarchy. The /industries URL still resolves via
# a redirect for old bookmarks.)
# Tenancy: positions_summary is multi-tenant -> we MUST scope the SQL with
# _account_sql_and AND filter the resulting DataFrame with
# _filter_df_by_accounts before aggregating, per
# .cursor/rules/bigquery-tenant-isolation.mdc.
# ----------------------------------------------------------------------

SECTORS_QUERY = """
    SELECT
        account,
        symbol,
        strategy,
        status,
        total_pnl,
        realized_pnl,
        unrealized_pnl,
        total_premium_received,
        total_dividend_income,
        total_return,
        num_individual_trades,
        num_winners,
        num_losers,
        sector,
        subsector,
        company_name
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1
    {tenant_filter}
"""


@app.route("/industries")
@login_required
def industries_legacy():
    """Backward-compatible redirect for the old /industries URL. The page
    moved to /sectors when we renamed industry → subsector."""
    return redirect(url_for("sectors", **request.args.to_dict(flat=True)), code=301)


@app.route("/sectors")
@login_required
def sectors():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    try:
        df = client.query(
            SECTORS_QUERY.format(tenant_filter=tenant_filter)
        ).to_dataframe()
    except Exception as exc:
        return render_template(
            "sectors.html",
            error=str(exc),
            sectors=[],
            sector_rows=[],
            subsector_rows=[],
            subsectors_by_sector={},
            unknown_count=0,
            kpis={},
            accounts=[],
            selected_account="",
        )

    df = _df_normalize_account_column(df)
    df = _filter_df_by_tenant_ids(df, tenant_ids)
    # tenant scope already narrowed to the selected account's tenant_id

    for col in (
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_dividend_income", "total_return",
        "num_individual_trades", "num_winners", "num_losers",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in ("sector", "subsector"):
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")

    accounts_for_filter = (
        sorted(user_accounts)
        if user_accounts
        else (sorted(df["account"].dropna().unique().tolist()) if not df.empty else [])
    )

    if df.empty:
        return render_template(
            "sectors.html",
            error=None,
            sectors=[],
            sector_rows=[],
            subsector_rows=[],
            subsectors_by_sector={},
            unknown_count=0,
            kpis={
                "total_pnl": 0.0, "realized_pnl": 0.0, "unrealized_pnl": 0.0,
                "num_subsectors": 0, "num_symbols": 0, "num_trades": 0,
                "win_rate": 0.0,
            },
            accounts=accounts_for_filter,
            selected_account=selected_account,
        )

    overall_winners = int(df["num_winners"].sum())
    overall_losers = int(df["num_losers"].sum())
    overall_closed = overall_winners + overall_losers
    kpis = {
        "total_pnl": float(df["total_pnl"].sum()),
        "realized_pnl": float(df["realized_pnl"].sum()),
        "unrealized_pnl": float(df["unrealized_pnl"].sum()),
        "num_subsectors": int(df["subsector"].nunique()),
        "num_symbols": int(df.groupby(["account", "symbol"]).ngroups),
        "num_trades": int(df["num_individual_trades"].sum()),
        "win_rate": (overall_winners / overall_closed) if overall_closed else 0.0,
    }

    # Per-subsector rollup: collapse strategy granularity, aggregate over the
    # user's accounts. One row per (sector, subsector).
    subsector_agg = (
        df.groupby(["sector", "subsector"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            premium_received=("total_premium_received", "sum"),
            dividend_income=("total_dividend_income", "sum"),
            total_return=("total_return", "sum"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_symbols=("symbol", "nunique"),
        )
        .reset_index()
    )
    closed = subsector_agg["num_winners"] + subsector_agg["num_losers"]
    subsector_agg["win_rate"] = subsector_agg["num_winners"] / closed.replace(0, pd.NA)
    subsector_agg["win_rate"] = subsector_agg["win_rate"].fillna(0)

    # Top symbol per (sector, subsector) by total_return — useful "what's
    # actually carrying this subsector?" tooltip on the card.
    sym_in_sub = (
        df.groupby(["sector", "subsector", "symbol"], dropna=False)["total_return"]
        .sum()
        .reset_index()
    )
    if not sym_in_sub.empty:
        sym_in_sub = sym_in_sub.sort_values(
            ["sector", "subsector", "total_return"], ascending=[True, True, False]
        )
        top_symbol_map = (
            sym_in_sub.groupby(["sector", "subsector"])
            .first()
            .reset_index()[["sector", "subsector", "symbol", "total_return"]]
            .rename(columns={"symbol": "top_symbol", "total_return": "top_symbol_return"})
        )
        subsector_agg = subsector_agg.merge(
            top_symbol_map, on=["sector", "subsector"], how="left"
        )
    else:
        subsector_agg["top_symbol"] = ""
        subsector_agg["top_symbol_return"] = 0.0

    subsector_agg = subsector_agg.sort_values("total_return", ascending=False)
    subsector_rows = subsector_agg.to_dict(orient="records")

    # Sector rollup — this is now the primary view on the page, so it carries
    # the same shape as subsector_rows: realized / unrealized / premium /
    # dividends / total_return so the sector cards have everything at a glance.
    sector_agg = (
        df.groupby(["sector"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            premium_received=("total_premium_received", "sum"),
            dividend_income=("total_dividend_income", "sum"),
            total_return=("total_return", "sum"),
            num_subsectors=("subsector", "nunique"),
            num_symbols=("symbol", "nunique"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
        )
        .reset_index()
    )
    s_closed = sector_agg["num_winners"] + sector_agg["num_losers"]
    sector_agg["win_rate"] = sector_agg["num_winners"] / s_closed.replace(0, pd.NA)
    sector_agg["win_rate"] = sector_agg["win_rate"].fillna(0)

    # Best / worst symbol per sector — drives the "what's carrying this?" /
    # "what's dragging?" callouts on each sector card.
    sym_per_sector = (
        df.groupby(["sector", "symbol"], dropna=False)["total_return"]
        .sum()
        .reset_index()
    )
    if not sym_per_sector.empty:
        top_per_sector = (
            sym_per_sector.sort_values(["sector", "total_return"], ascending=[True, False])
            .groupby("sector").first().reset_index()
            .rename(columns={"symbol": "top_symbol", "total_return": "top_symbol_return"})
        )
        worst_per_sector = (
            sym_per_sector.sort_values(["sector", "total_return"], ascending=[True, True])
            .groupby("sector").first().reset_index()
            .rename(columns={"symbol": "worst_symbol", "total_return": "worst_symbol_return"})
        )
        sector_agg = sector_agg.merge(top_per_sector[["sector", "top_symbol", "top_symbol_return"]], on="sector", how="left")
        sector_agg = sector_agg.merge(worst_per_sector[["sector", "worst_symbol", "worst_symbol_return"]], on="sector", how="left")
    else:
        sector_agg["top_symbol"] = ""
        sector_agg["top_symbol_return"] = 0.0
        sector_agg["worst_symbol"] = ""
        sector_agg["worst_symbol_return"] = 0.0

    sector_agg = sector_agg.sort_values("total_pnl", ascending=False)
    sector_rows = sector_agg.to_dict(orient="records")
    sectors_list = sector_agg["sector"].tolist()

    # Group subsectors under their sector for the collapsible drill-down on
    # the page. Order each sector's subsectors by total_return desc.
    subsectors_by_sector: dict[str, list[dict]] = {}
    for r in subsector_rows:
        subsectors_by_sector.setdefault(r["sector"], []).append(r)
    for sec in subsectors_by_sector:
        subsectors_by_sector[sec].sort(
            key=lambda x: x.get("total_return", 0), reverse=True
        )

    unknown_count = int(
        ((df["sector"] == "Unknown") | (df["subsector"] == "Unknown"))
        .pipe(lambda s: s.groupby([df["account"], df["symbol"]]).any())
        .sum()
    )

    return render_template(
        "sectors.html",
        error=None,
        sectors=sectors_list,
        sector_rows=sector_rows,
        subsector_rows=subsector_rows,
        subsectors_by_sector=subsectors_by_sector,
        unknown_count=unknown_count,
        kpis=kpis,
        accounts=accounts_for_filter,
        selected_account=selected_account,
    )


# ======================================================================
# Strategy fit  (/strategy-fit)
# ======================================================================
#
# Cross-tab of strategy x sector (or strategy x subsector when drilled into
# a single sector) so users can see "what strategies work best in what
# kinds of companies?". Same tenancy guarantees as /sectors — query is
# scoped by _account_sql_and AND the DataFrame is _filter_df_by_accounts'd
# before any aggregation.
# ----------------------------------------------------------------------

STRATEGY_FIT_QUERY = """
    SELECT
        account,
        symbol,
        strategy,
        status,
        total_pnl,
        realized_pnl,
        unrealized_pnl,
        total_return,
        num_individual_trades,
        num_winners,
        num_losers,
        sector,
        subsector
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1
    {tenant_filter}
"""

# Per-option-contract grain for the DTE / Moneyness slices. Shaped so the
# matrix builder can consume it identically to the positions_summary path:
# realized = closed contracts, unrealized = open contracts; winners/losers
# only counted on closed contracts so win-rate semantics match the rest of
# the app. underlying_symbol is exposed as `symbol` to keep the per-cell
# symbol drill-down code path uniform.
STRATEGY_FIT_OPTIONS_QUERY = """
    SELECT
        account,
        UPPER(TRIM(underlying_symbol)) AS symbol,
        COALESCE(strategy, 'Other Option') AS strategy,
        status,
        dte_bucket,
        moneyness_at_open,
        total_pnl,
        CASE WHEN status = 'Closed' THEN total_pnl ELSE 0 END AS realized_pnl,
        CASE WHEN status = 'Open'   THEN total_pnl ELSE 0 END AS unrealized_pnl,
        num_trades AS num_individual_trades,
        CASE WHEN status = 'Closed' AND total_pnl >  0 THEN 1 ELSE 0 END AS num_winners,
        CASE WHEN status = 'Closed' AND total_pnl <= 0 THEN 1 ELSE 0 END AS num_losers
    FROM `ccwj-dbt.analytics.int_option_trade_kinds`
    WHERE 1=1
    {tenant_filter}
"""

# Fixed display order for non-categorical buckets so the dimension reads
# left-to-right naturally regardless of P&L. Anything not listed here
# (e.g. an unexpected bucket value) falls through and is appended after,
# sorted by total P&L desc, by the matrix builder.
DIM_FIXED_COL_ORDER = {
    "dte":        ["0-7 DTE", "8-30 DTE", "31-60 DTE", "61-90 DTE", "91+ DTE", "Unknown"],
    "moneyness":  ["ITM", "ATM", "OTM", "Unknown"],
}

# Map dim -> (column field in DataFrame, human label for headers/lede).
DIM_META = {
    "sector":     ("sector",            "Sector",     "sectors"),
    "subsector":  ("subsector",         "Subsector",  "subsectors"),
    "dte":        ("dte_bucket",        "DTE",        "DTE buckets"),
    "moneyness":  ("moneyness_at_open", "Moneyness",  "moneyness buckets"),
}


def _build_strategy_fit_matrix(
    df,
    *,
    col_field: str,
    col_order_override: list | None = None,
    equity_strategies: list | None = None,
):
    """Aggregate a normalized trade DataFrame into the dict of template
    variables that strategy_fit.html consumes (cells, row/col totals,
    sweet/soft callouts, baselines, color scales).

    Pure aggregation — no I/O, no tenancy logic. The caller is responsible
    for scoping `df` to the user's accounts (SQL `account_filter` AND
    `_filter_df_by_tenant_ids(df, tenant_ids)`) BEFORE handing it in.

    Required columns on `df`:
        account, symbol, strategy, <col_field>,
        total_pnl, realized_pnl, unrealized_pnl,
        num_individual_trades, num_winners, num_losers

    Args:
        col_field:           name of the column that becomes the matrix
                             columns (e.g. "sector", "dte_bucket").
        col_order_override:  fixed left-to-right column order (e.g. for
                             DTE buckets). Unknown bucket values that
                             show up in the data but aren't in the
                             override are appended after, P&L-sorted.
        equity_strategies:   strategies that have no rows in `df` (e.g.
                             equity-only Buy and Hold on the DTE slice)
                             but should still appear as N/A rows so the
                             user can see why nothing's there.
    """
    empty = {
        "row_labels": [],
        "col_labels": [],
        "cells": {},
        "cell_symbols_map": {},
        "row_totals": {},
        "col_totals": {},
        "grand_total": None,
        "max_abs_pnl": 1.0,
        "max_abs_expectancy": 1.0,
        "max_abs_edge": 1.0,
        "baseline_expectancy": 0.0,
        "baseline_win_rate": 0.0,
        "sweet_spots": [],
        "soft_spots": [],
        "equity_strategies": sorted(equity_strategies or []),
    }
    if df is None or df.empty:
        # Even with no cell data we still want equity-N/A rows visible so
        # the user sees the dimension is meaningful but doesn't apply.
        if equity_strategies:
            empty["row_labels"] = sorted(equity_strategies)
        return empty

    cell_agg = (
        df.groupby(["strategy", col_field], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_symbols=("symbol", "nunique"),
        )
        .reset_index()
    )
    closed = cell_agg["num_winners"] + cell_agg["num_losers"]
    cell_agg["win_rate"] = cell_agg["num_winners"] / closed.replace(0, pd.NA)
    cell_agg["win_rate"] = cell_agg["win_rate"].fillna(0)

    # Expectancy = avg P&L per trade. The single most decision-relevant metric
    # because it normalizes for volume — "per trade I take, am I making money?"
    cell_agg["expectancy"] = cell_agg["total_pnl"] / cell_agg["num_trades"].replace(0, pd.NA)
    cell_agg["expectancy"] = cell_agg["expectancy"].fillna(0)

    overall_total_pnl = float(df["total_pnl"].sum())
    overall_trades = int(df["num_individual_trades"].sum())
    overall_winners = int(df["num_winners"].sum())
    overall_losers = int(df["num_losers"].sum())
    overall_closed = overall_winners + overall_losers
    baseline_expectancy = (overall_total_pnl / overall_trades) if overall_trades else 0.0
    baseline_win_rate = (overall_winners / overall_closed) if overall_closed else 0.0
    cell_agg["edge_expectancy"] = cell_agg["expectancy"] - baseline_expectancy
    cell_agg["edge_win_rate"] = cell_agg["win_rate"] - baseline_win_rate

    # Row order: best-performing strategies on top.
    row_order = (
        cell_agg.groupby("strategy")["total_pnl"].sum().sort_values(ascending=False)
        .index.tolist()
    )
    # Equity-only strategies (e.g. Buy and Hold on a DTE slice) trail the
    # data rows so the matrix still shows "you traded these too, just not
    # in this dimension." Sorted alphabetically for stable ordering.
    extra_equity = sorted(
        s for s in (equity_strategies or []) if s not in set(row_order)
    )
    row_order = list(row_order) + extra_equity

    # Column order: fixed where the dimension is categorical (DTE,
    # moneyness, market cap), P&L-sorted otherwise.
    if col_order_override is not None:
        present_cols = set(cell_agg[col_field].dropna().unique().tolist())
        col_order = [c for c in col_order_override if c in present_cols]
        # Anything new the data shows that we didn't anticipate — append
        # P&L-sorted so we don't silently drop columns.
        leftover = (
            cell_agg[~cell_agg[col_field].isin(col_order)]
            .groupby(col_field)["total_pnl"].sum().sort_values(ascending=False)
            .index.tolist()
        )
        col_order = col_order + [c for c in leftover if c not in col_order]
    else:
        col_order = (
            cell_agg.groupby(col_field)["total_pnl"].sum().sort_values(ascending=False)
            .index.tolist()
        )

    cells: dict = {}
    for r in cell_agg.to_dict(orient="records"):
        cells.setdefault(r["strategy"], {})[r[col_field]] = r

    # Per-cell symbol breakdown (top 5 by P&L) — the drill-panel uses this
    # so users can answer "what symbols are carrying this cell?" without
    # leaving the page.
    cell_sym_agg = (
        df.groupby(["strategy", col_field, "symbol"], dropna=False)
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_individual_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
        )
        .reset_index()
        .sort_values("total_pnl", ascending=False)
    )
    cell_symbols_map: dict = {}
    for _, r in cell_sym_agg.iterrows():
        key = f"{r['strategy']}||{r[col_field]}"
        cell_symbols_map.setdefault(key, []).append({
            "symbol": str(r["symbol"]),
            "total_pnl": float(r["total_pnl"]),
            "num_trades": int(r["num_trades"]),
            "num_winners": int(r["num_winners"]),
            "num_losers": int(r["num_losers"]),
        })
    cell_symbols_map = {k: v[:5] for k, v in cell_symbols_map.items()}

    row_totals_agg = (
        cell_agg.groupby("strategy")
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_symbols=("num_symbols", "sum"),
        )
        .reset_index()
    )
    rclosed = row_totals_agg["num_winners"] + row_totals_agg["num_losers"]
    row_totals_agg["win_rate"] = (row_totals_agg["num_winners"] / rclosed.replace(0, pd.NA)).fillna(0)
    row_totals = {r["strategy"]: r for r in row_totals_agg.to_dict(orient="records")}

    col_totals_agg = (
        cell_agg.groupby(col_field)
        .agg(
            total_pnl=("total_pnl", "sum"),
            num_trades=("num_trades", "sum"),
            num_winners=("num_winners", "sum"),
            num_losers=("num_losers", "sum"),
            num_symbols=("num_symbols", "sum"),
        )
        .reset_index()
    )
    cclosed = col_totals_agg["num_winners"] + col_totals_agg["num_losers"]
    col_totals_agg["win_rate"] = (col_totals_agg["num_winners"] / cclosed.replace(0, pd.NA)).fillna(0)
    col_totals = {r[col_field]: r for r in col_totals_agg.to_dict(orient="records")}

    grand = {
        "total_pnl": float(cell_agg["total_pnl"].sum()),
        "num_trades": int(cell_agg["num_trades"].sum()),
        "num_winners": int(cell_agg["num_winners"].sum()),
        "num_losers": int(cell_agg["num_losers"].sum()),
        "expectancy": baseline_expectancy,
        "win_rate": baseline_win_rate,
    }

    records = cell_agg.to_dict(orient="records")
    abs_pnls = [abs(c["total_pnl"]) for c in records if c["total_pnl"]]
    abs_exps = [abs(c["expectancy"]) for c in records if c["expectancy"]]
    abs_edges = [abs(c["edge_expectancy"]) for c in records if c["edge_expectancy"]]
    max_abs_pnl = max(abs_pnls) if abs_pnls else 1.0
    max_abs_expectancy = max(abs_exps) if abs_exps else 1.0
    max_abs_edge = max(abs_edges) if abs_edges else 1.0

    # Sample-size and win-rate guarded callouts so we don't celebrate a
    # 1-trade fluke or a coin-flip strategy that lucked into R:R. Cells
    # whose column value is "Unknown" are excluded from the narrative
    # surface (sweet/soft callouts) — naming "Unknown" as edge isn't
    # actionable. The cell stays in the matrix and the user can toggle
    # the Unknown column on/off; we just don't editorialize about it.
    MIN_TRADES_FOR_CALLOUT = 5
    qualified = cell_agg[
        (cell_agg["num_trades"] >= MIN_TRADES_FOR_CALLOUT)
        & (cell_agg[col_field].astype(str) != "Unknown")
    ].copy()

    sweet_spots: list = []
    soft_spots: list = []
    if not qualified.empty:
        sweet_df = qualified[
            (qualified["expectancy"] > 0) & (qualified["win_rate"] >= 0.45)
        ].sort_values("expectancy", ascending=False).head(3)
        soft_df = qualified[qualified["expectancy"] < 0].sort_values(
            "expectancy", ascending=True
        ).head(2)
        sweet_spots = sweet_df.to_dict(orient="records")
        soft_spots = soft_df.to_dict(orient="records")

    return {
        "row_labels": row_order,
        "col_labels": col_order,
        "cells": cells,
        "cell_symbols_map": cell_symbols_map,
        "row_totals": row_totals,
        "col_totals": col_totals,
        "grand_total": grand,
        "max_abs_pnl": max_abs_pnl,
        "max_abs_expectancy": max_abs_expectancy,
        "max_abs_edge": max_abs_edge,
        "baseline_expectancy": baseline_expectancy,
        "baseline_win_rate": baseline_win_rate,
        "sweet_spots": sweet_spots,
        "soft_spots": soft_spots,
        "equity_strategies": sorted(equity_strategies or []),
    }


def _strategy_fit_insight_context(selected_account: str) -> dict:
    """Pull the cached AI strategy-fit insight for the current user/account
    scope and convert its markdown to HTML for the template.

    Returns a small dict that's safe to **-unpack into render_template()
    in all code paths (success, empty, error)."""
    ctx = {
        "ai_summary": None,
        "ai_full_html": None,
        "ai_generated_at": None,
        "ai_enabled": app.config.get("INSIGHTS_ENABLED", True),
        "ai_available": _llm_available(),
    }
    if not ctx["ai_enabled"]:
        return ctx
    try:
        cached = get_strategy_fit_insight_for_user(
            current_user.id, tenant_filter=selected_account or ""
        )
    except Exception:
        cached = None
    if cached:
        from app.insights import _md_to_html
        ctx["ai_summary"] = cached.get("summary")
        ctx["ai_full_html"] = _md_to_html(cached.get("full_analysis") or "")
        ctx["ai_generated_at"] = cached.get("generated_at")
    return ctx


def _strategy_fit_render_payload(
    *,
    matrix: dict,
    dim: str,
    drill_sector: str,
    accounts: list,
    selected_account: str,
    insight_ctx: dict,
    error: str | None = None,
) -> dict:
    """Compose the kwargs to render strategy_fit.html. Centralized so the
    error/empty/data paths share one shape and can't drift."""
    col_field, dim_label, dim_label_plural = DIM_META.get(
        dim, DIM_META["sector"]
    )
    # AI insight payload was built for sector/subsector — null it out on
    # other dims so the template's "AI Insight" card hides cleanly.
    if dim not in ("sector", "subsector"):
        insight_ctx = {
            **insight_ctx,
            "ai_summary": None,
            "ai_full_html": None,
            "ai_generated_at": None,
        }
    return dict(
        error=error,
        row_labels=matrix.get("row_labels", []),
        col_labels=matrix.get("col_labels", []),
        cells=matrix.get("cells", {}),
        cell_symbols_json=json.dumps(matrix.get("cell_symbols_map", {})),
        row_totals=matrix.get("row_totals", {}),
        col_totals=matrix.get("col_totals", {}),
        grand_total=matrix.get("grand_total"),
        max_abs_pnl=matrix.get("max_abs_pnl", 1.0),
        max_abs_expectancy=matrix.get("max_abs_expectancy", 1.0),
        max_abs_edge=matrix.get("max_abs_edge", 1.0),
        baseline_expectancy=matrix.get("baseline_expectancy", 0.0),
        baseline_win_rate=matrix.get("baseline_win_rate", 0.0),
        sweet_spots=matrix.get("sweet_spots", []),
        soft_spots=matrix.get("soft_spots", []),
        equity_strategies=matrix.get("equity_strategies", []),
        col_field=col_field,
        dim=dim,
        # mode is preserved for backward-compat in the template (it used
        # to be sector|subsector only); now mirrors dim 1:1.
        mode=dim,
        dim_label=dim_label,
        dim_label_plural=dim_label_plural,
        drill_sector=drill_sector,
        accounts=accounts,
        selected_account=selected_account,
        **insight_ctx,
    )


@app.route("/strategy-fit")
@login_required
def strategy_fit():
    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    drill_sector = request.args.get("sector", "")  # implies subsector mode

    # Resolve the column dimension. Drilling into a sector wins (for
    # backward URL compat) and forces subsector mode. Otherwise read ?dim=
    # and validate against the supported set. 'industry' is the pre-rename
    # alias for 'subsector' — accept it so old bookmarks keep working.
    requested_dim = (request.args.get("dim", "") or "").strip().lower()
    if requested_dim == "industry":
        requested_dim = "subsector"
    if drill_sector:
        dim = "subsector"
    elif requested_dim in ("dte", "moneyness", "subsector", "sector"):
        dim = requested_dim
    else:
        dim = "sector"

    insight_ctx = _strategy_fit_insight_context(selected_account)

    # Fan out the queries we need. positions_summary is always needed —
    # for sector/subsector it's the data source, and for dte/moneyness
    # it's where we discover the equity-only strategy set so the matrix
    # can show "N/A — equity" rows.
    queries = {"summary": STRATEGY_FIT_QUERY.format(tenant_filter=tenant_filter)}
    if dim in ("dte", "moneyness"):
        queries["options"] = STRATEGY_FIT_OPTIONS_QUERY.format(tenant_filter=tenant_filter)

    try:
        dfs = _bq_parallel(client, queries)
    except Exception as exc:
        # Don't swallow this silently — a schema drift here once shipped a red
        # banner to every Strategy Fit visitor for hours before anyone noticed.
        app.logger.exception("strategy_fit: BigQuery query failed: %s", exc)
        return render_template(
            "strategy_fit.html",
            **_strategy_fit_render_payload(
                matrix={},
                dim=dim,
                drill_sector=drill_sector,
                accounts=[],
                selected_account="",
                insight_ctx=insight_ctx,
                error=str(exc),
            ),
        )

    summary_df = _df_normalize_account_column(dfs["summary"])
    summary_df = _filter_df_by_tenant_ids(summary_df, tenant_ids)
    # tenant scope already narrowed to the selected account's tenant_id

    for col in ("total_pnl", "realized_pnl", "unrealized_pnl", "total_return",
                "num_individual_trades", "num_winners", "num_losers"):
        if col in summary_df.columns:
            summary_df.loc[:, col] = pd.to_numeric(summary_df[col], errors="coerce").fillna(0)
    for col in ("sector", "subsector", "strategy"):
        if col in summary_df.columns:
            summary_df.loc[:, col] = (
                summary_df[col].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")
            )

    accounts_for_filter = (
        sorted(user_accounts)
        if user_accounts
        else (sorted(summary_df["account"].dropna().unique().tolist())
              if not summary_df.empty else [])
    )

    if summary_df.empty:
        return render_template(
            "strategy_fit.html",
            **_strategy_fit_render_payload(
                matrix={},
                dim=dim,
                drill_sector=drill_sector,
                accounts=accounts_for_filter,
                selected_account=selected_account,
                insight_ctx=insight_ctx,
            ),
        )

    if dim in ("dte", "moneyness"):
        options_df = _df_normalize_account_column(dfs["options"])
        # Tenancy belt-and-braces: re-filter the per-contract frame by
        # the user's accounts BEFORE any grouping so a SQL regression
        # can't leak another tenant's contracts into the matrix.
        options_df = _filter_df_by_tenant_ids(options_df, tenant_ids)
        # tenant scope already narrowed to the selected account's tenant_id

        for col in ("total_pnl", "realized_pnl", "unrealized_pnl",
                    "num_individual_trades", "num_winners", "num_losers"):
            if col in options_df.columns:
                options_df.loc[:, col] = pd.to_numeric(options_df[col], errors="coerce").fillna(0)
        for col in ("strategy", "dte_bucket", "moneyness_at_open", "symbol"):
            if col in options_df.columns:
                options_df.loc[:, col] = (
                    options_df[col].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")
                )

        col_field = DIM_META[dim][0]
        # Equity-only strategies = strategies the user has in
        # positions_summary but that have NO option contracts. We mark
        # these as full N/A rows in the template so users see why their
        # equity strategy doesn't appear in the data area.
        all_strategies = set(summary_df["strategy"].dropna().astype(str).unique().tolist())
        option_strategies = set(
            options_df["strategy"].dropna().astype(str).unique().tolist()
        ) if not options_df.empty else set()
        equity_strategies = sorted(all_strategies - option_strategies)

        matrix = _build_strategy_fit_matrix(
            options_df,
            col_field=col_field,
            col_order_override=DIM_FIXED_COL_ORDER[dim],
            equity_strategies=equity_strategies,
        )
    else:
        df = summary_df
        if dim == "subsector":
            # Drill: filter to one sector, columns become subsectors.
            df = df[df["sector"] == drill_sector]
            col_field = "subsector"
            col_order_override = None
        else:
            col_field = "sector"
            col_order_override = None

        matrix = _build_strategy_fit_matrix(
            df,
            col_field=col_field,
            col_order_override=col_order_override,
        )

    return render_template(
        "strategy_fit.html",
        **_strategy_fit_render_payload(
            matrix=matrix,
            dim=dim,
            drill_sector=drill_sector,
            accounts=accounts_for_filter,
            selected_account=selected_account,
            insight_ctx=insight_ctx,
        ),
    )


@app.route("/accounts")
@login_required
def accounts():
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    # Attribution query + breakdown builders live in app.weekly_review.
    # Deferred import: weekly_review imports from app.routes at module
    # load, so a top-level import here would be circular.
    from app.weekly_review import (
        POSITION_ATTRIBUTION_QUERY,
        ATTRIBUTION_LIFETIME_SENTINEL,
        _build_position_breakdown,
        _aggregate_breakdown_by,
        _build_breakdown_totals,
        _strategy_for_symbol,
    )

    try:
        dfs = _bq_parallel(client, {
            "balances": ACCOUNT_BALANCES_QUERY.format(tenant_filter=tenant_filter),
            "trades": TRADES_QUERY.format(tenant_filter=tenant_filter),
            "current": CURRENT_POSITIONS_QUERY.format(tenant_filter=tenant_filter),
            "strat_class": STRATEGY_CLASSIFICATION_QUERY.format(tenant_filter=tenant_filter),
            "strat_summary": ACCOUNT_POSITIONS_SUMMARY_QUERY.format(tenant_filter=tenant_filter),
            # Lifetime view: pass the far-past sentinel so the per-asset-class
            # P&L sums include every closed group (the /accounts page is the
            # full lifetime breakdown; the week scoping is the Daily Review's).
            "attribution": POSITION_ATTRIBUTION_QUERY.format(
                tenant_filter=tenant_filter, week_start=ATTRIBUTION_LIFETIME_SENTINEL),
        })
        balances_df = dfs["balances"]
        trades_df = dfs["trades"]
        current_df = dfs["current"]
        strat_class_df = dfs["strat_class"]
        strat_summary_df = dfs["strat_summary"]
        attribution_df = dfs["attribution"]
    except Exception as exc:
        return render_template(
            "accounts.html",
            error=str(exc),
            kpis={},
            summary_chart_json="{}",
            strategy_chart_json="{}",
            strategy_rows=[],
            position_breakdown=[],
            position_breakdown_totals=None,
            strategy_breakdown=[],
            strategy_breakdown_totals=None,
            sector_breakdown=[],
            subsector_breakdown=[],
            accounts=[],
            selected_account="",
        )

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    for col in ["market_value", "cost_basis", "unrealized_pnl", "unrealized_pnl_pct", "percent_of_account"]:
        if col in balances_df.columns:
            balances_df[col] = pd.to_numeric(balances_df[col], errors="coerce").fillna(0)

    for col in ["amount", "quantity", "price", "fees"]:
        trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
    trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)

    for col in ["total_pnl", "num_trades"]:
        if col in strat_class_df.columns:
            strat_class_df[col] = pd.to_numeric(strat_class_df[col], errors="coerce").fillna(0)
    for col in ["open_date", "close_date"]:
        if col in strat_class_df.columns:
            strat_class_df[col] = pd.to_datetime(strat_class_df[col], errors="coerce").dt.date

    num_cols = ["total_pnl", "realized_pnl", "unrealized_pnl", "premium_received",
                "premium_paid", "num_trades", "num_winners", "num_losers",
                "dividend_income", "total_return"]
    for col in num_cols:
        if col in strat_summary_df.columns:
            strat_summary_df[col] = pd.to_numeric(strat_summary_df[col], errors="coerce").fillna(0)

    # ------------------------------------------------------------------
    # Safety-belt: re-filter in Python (SQL already filtered by account)
    # ------------------------------------------------------------------
    balances_df = _filter_df_by_tenant_ids(balances_df, tenant_ids)
    trades_df = _filter_df_by_tenant_ids(trades_df, tenant_ids)
    current_df = _filter_df_by_tenant_ids(current_df, tenant_ids)
    strat_class_df = _filter_df_by_tenant_ids(strat_class_df, tenant_ids)
    strat_summary_df = _filter_df_by_tenant_ids(strat_summary_df, tenant_ids)
    attribution_df = _filter_df_by_tenant_ids(attribution_df, tenant_ids)

    # Picker lists the full disambiguated account set (non-admin) so every
    # physical account is selectable after tenant scope narrows the data.
    all_accounts = (
        sorted(user_accounts)
        if user_accounts
        else sorted(trades_df["account"].dropna().unique())
    )
    selected_account = request.args.get("account", "")
    # tenant scope (resolved from selected_account → tenant_ids above) already
    # narrowed every frame; no secondary label-equality narrowing needed
    # (which would break for disambiguated colliding labels).

    # ------------------------------------------------------------------
    # KPIs from balances
    # ------------------------------------------------------------------
    cash_rows = balances_df[balances_df["row_type"] == "cash"]
    total_rows = balances_df[balances_df["row_type"] == "account_total"]

    cash_balance = float(cash_rows["market_value"].sum())
    account_value = float(total_rows["market_value"].sum())
    invested_value = account_value - cash_balance
    acct_cost_basis = float(total_rows["cost_basis"].sum())

    # Realized + unrealized + total_return all come from the same source
    # (positions_summary) so the three KPIs reconcile: total_return =
    # realized + unrealized + dividends. Mixing the snapshot's unrealized
    # with positions_summary's realized has shipped a $300+ discrepancy.
    realized_pnl = float(strat_summary_df["realized_pnl"].sum())
    acct_unrealized = float(strat_summary_df["unrealized_pnl"].sum())
    total_return = float(strat_summary_df["total_return"].sum())
    # Surfacing dividends as its own KPI so the math reconciles for the
    # reader: realized + unrealized + dividends = total return. Without
    # this card the row silently failed by ~$200-300 (the missing piece
    # was always dividends), and investors / power users noticed.
    dividend_income = (
        float(strat_summary_df["dividend_income"].sum())
        if "dividend_income" in strat_summary_df.columns else 0.0
    )

    kpis = {
        "account_value": account_value,
        "cash_balance": cash_balance,
        "invested_value": invested_value,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": acct_unrealized,
        "dividend_income": dividend_income,
        "total_return": total_return,
    }

    # ------------------------------------------------------------------
    # Chart 1: Cumulative P&L over time (summary) — from mart_daily_pnl
    # ------------------------------------------------------------------
    try:
        chart_tenant_ids = _tenants_for_scope(selected_account)
        chart_tenant_filter = _tenant_sql_and(chart_tenant_ids)
        chart_df = cached_query_df(
            client,
            CHART_DATA_ALL_QUERY.format(tenant_filter=chart_tenant_filter)
        )
        chart_df = _filter_df_by_tenant_ids(chart_df, chart_tenant_ids)
        # tenant scope already narrowed chart_df to the selected account's tenant
        with timed("acct_chart"):
            summary_chart = cached_payload(
                ("acct_chart", str(date.today()), frame_fingerprint(chart_df, current_df)),
                lambda: _build_account_chart_from_daily_pnl(chart_df, current_df),
            )
    except Exception:
        summary_chart = {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}

    # ------------------------------------------------------------------
    # Chart 2: Strategy P&L over time
    # ------------------------------------------------------------------
    strategy_chart = _build_strategy_time_chart(strat_class_df)

    # ------------------------------------------------------------------
    # Strategy summary table
    # ------------------------------------------------------------------
    if not strat_summary_df.empty:
        strat_summary_df["win_rate"] = strat_summary_df.apply(
            lambda r: r["num_winners"] / (r["num_winners"] + r["num_losers"])
            if (r["num_winners"] + r["num_losers"]) > 0 else 0,
            axis=1,
        )
        strategy_rows = strat_summary_df.to_dict(orient="records")
    else:
        strategy_rows = []

    # ------------------------------------------------------------------
    # Detailed breakdown tables (the per-symbol / strategy / sector /
    # subsector "CC Trading Summary" the Daily Review account scorecard
    # drills into). Same Stock | Options | Dividend | Net | % | Annualized
    # shape, lifetime scope (week_start=None here + the SENTINEL passed to
    # the SQL above, so every closed group counts — this is the full account
    # view). All four pull from POSITION_ATTRIBUTION_QUERY so the totals
    # reconcile with the scorecard row on /daily-review (which scopes to the
    # current week instead).
    # ------------------------------------------------------------------
    position_breakdown = []
    position_breakdown_totals = None
    strategy_breakdown = []
    strategy_breakdown_totals = None
    sector_breakdown = []
    subsector_breakdown = []
    try:
        # strategy_by_symbol: largest abs-P&L strategy label per symbol
        # (matches the "primary strategy" lens on /positions).
        strategy_by_symbol = {}
        if strat_class_df is not None and not strat_class_df.empty:
            sb = (
                strat_class_df.groupby(["symbol", "strategy"], dropna=False)["total_pnl"]
                .sum()
                .reset_index()
            )
            lookup = {}
            for _, r in sb.iterrows():
                sym = str(r.get("symbol") or "")
                lookup.setdefault(sym, []).append(
                    {"strategy": r.get("strategy"), "total_pnl": r.get("total_pnl")}
                )
            for sym, classes in lookup.items():
                strategy_by_symbol[sym] = _strategy_for_symbol(sym, {sym: classes})

        position_breakdown = _build_position_breakdown(
            attribution_df, strategy_by_symbol, week_start=None,
        )
        position_breakdown_totals = _build_breakdown_totals(position_breakdown)
        strategy_breakdown = _aggregate_breakdown_by(
            position_breakdown, "strategy", label_name="strategy"
        )
        strategy_breakdown_totals = _build_breakdown_totals(strategy_breakdown)
        sector_breakdown = _aggregate_breakdown_by(
            position_breakdown, "sector", label_name="sector"
        )
        subsector_breakdown = _aggregate_breakdown_by(
            position_breakdown, "subsector", label_name="subsector"
        )
    except Exception as exc:
        app.logger.warning("Account breakdown tables failed: %s", exc)

    return render_template(
        "accounts.html",
        kpis=kpis,
        summary_chart_json=json.dumps(summary_chart),
        strategy_chart_json=json.dumps(strategy_chart),
        strategy_rows=strategy_rows,
        position_breakdown=position_breakdown,
        position_breakdown_totals=position_breakdown_totals,
        strategy_breakdown=strategy_breakdown,
        strategy_breakdown_totals=strategy_breakdown_totals,
        sector_breakdown=sector_breakdown,
        subsector_breakdown=subsector_breakdown,
        accounts=all_accounts,
        selected_account=selected_account,
    )


# ════════════════════════════════════════════════════════════════════════
# Earnings Watch (/earnings) — tandem-product surface for EarningsFollower
# ════════════════════════════════════════════════════════════════════════
#
# Two scoped sections, both deep-linking out to the separately-deployed
# EarningsFollower web app (deep-links only — see app.utils.earnings_follower_url):
#   1. "Your positions reporting soon" — held symbols with an upcoming
#      next_earnings_date. Same shape/source as the Daily Review earnings
#      block (stg_earnings_calendar ⋈ currently-held holdings), tenant-scoped
#      via {tenant_filter} on the holdings CTE.
#   2. "Movers in your sectors" — recent big % movers (mart_sector_movers,
#      symbol-grain market data) restricted to the sectors the user actually
#      holds. The sector list is derived from tenant-scoped holdings, so the
#      selection is user-scoped even though mart_sector_movers carries no
#      tenant columns (it's public price data — nothing to leak).

# Held holdings + their sector context (tenant-scoped). Drives both the
# earnings join symbol set and the "your sectors" list for the movers query.
EARNINGS_WATCH_HELD_QUERY = """
SELECT DISTINCT
    UPPER(TRIM(ec.underlying_symbol)) AS symbol,
    COALESCE(m.sector, 'Unknown')     AS sector,
    COALESCE(m.subsector, 'Unknown')  AS subsector,
    m.long_name
FROM `ccwj-dbt.analytics.int_enriched_current` ec
LEFT JOIN `ccwj-dbt.analytics.stg_symbol_metadata` m
    ON UPPER(TRIM(ec.underlying_symbol)) = m.symbol
WHERE ec.quantity IS NOT NULL AND ec.quantity != 0
  {tenant_filter}
"""

# Upcoming earnings for currently-held holdings (next 21 days). Mirrors
# weekly_review.EARNINGS_UPCOMING_QUERY; the symbol set is narrowed to the
# user's holdings inside the CTE so the result is already tenant-safe.
EARNINGS_WATCH_UPCOMING_QUERY = """
WITH holdings AS (
    SELECT DISTINCT UPPER(TRIM(underlying_symbol)) AS symbol
    FROM `ccwj-dbt.analytics.int_enriched_current`
    WHERE quantity IS NOT NULL AND quantity != 0
      {tenant_filter}
)
SELECT
    e.symbol,
    e.next_earnings_date,
    DATE_DIFF(e.next_earnings_date, CURRENT_DATE(), DAY) AS days_until,
    m.long_name,
    COALESCE(m.sector, 'Unknown')    AS sector,
    COALESCE(m.subsector, 'Unknown') AS subsector
FROM `ccwj-dbt.analytics.stg_earnings_calendar` e
JOIN holdings h USING (symbol)
LEFT JOIN `ccwj-dbt.analytics.stg_symbol_metadata` m USING (symbol)
WHERE e.next_earnings_date BETWEEN CURRENT_DATE()
                              AND DATE_ADD(CURRENT_DATE(), INTERVAL 21 DAY)
ORDER BY e.next_earnings_date, e.symbol
"""

# Big recent movers in a set of sectors (symbol-grain market data; no tenant
# columns). @sectors is a user-derived held-sector list bound as a query
# parameter (no string interpolation). @min_abs_move gates "big" moves.
# No tight LIMIT here: the mart is small (one row per platform-traded symbol)
# and the per-sector display cap is applied in Python after grouping, so one
# noisy sector can't starve the others.
EARNINGS_WATCH_MOVERS_QUERY = """
SELECT
    symbol,
    latest_close,
    pct_change,
    abs_pct_change,
    sector,
    subsector,
    long_name
FROM `ccwj-dbt.analytics.mart_sector_movers`
WHERE sector IN UNNEST(@sectors)
  AND abs_pct_change >= @min_abs_move
ORDER BY abs_pct_change DESC
LIMIT 200
"""

# Display cap per sector group on /earnings ("show me what's impacted",
# not "show me everything that moved").
EARNINGS_WATCH_MOVERS_PER_SECTOR = 8


@app.route("/earnings")
@login_required
def earnings_watch():
    """Earnings Watch — held positions reporting soon + same-sector movers,
    with deep-links out to the EarningsFollower tandem product."""
    if not app.config.get("EARNINGS_FOLLOWER_ENABLED", True):
        abort(404)

    bounce = _redirect_if_no_accounts()
    if bounce:
        return bounce

    user_accounts = _user_account_list()
    selected_account = request.args.get("account", "")
    tenant_ids = _tenants_for_scope(selected_account)
    tenant_filter = _tenant_sql_and(tenant_ids)

    context = {
        "title": "Earnings Watch",
        "upcoming_earnings": [],
        "mover_groups": [],
        "held_sectors": [],
        "accounts": sorted(user_accounts) if user_accounts else [],
        "selected_account": selected_account,
        "error": None,
    }

    try:
        client = get_bigquery_client()

        held_df = client.query(
            EARNINGS_WATCH_HELD_QUERY.format(tenant_filter=tenant_filter)
        ).to_dataframe()
        # Held symbols are scoped in SQL; the symbol set drives the rest.
        held_symbols = set()
        held_sectors = set()
        if not held_df.empty:
            held_symbols = {
                str(s).strip().upper() for s in held_df["symbol"].dropna().tolist()
            }
            held_sectors = {
                str(s).strip() for s in held_df["sector"].dropna().tolist()
                if str(s).strip() and str(s).strip() != "Unknown"
            }
        context["held_sectors"] = sorted(held_sectors)

        # ── Upcoming earnings (held positions) ────────────────────────
        try:
            earn_df = client.query(
                EARNINGS_WATCH_UPCOMING_QUERY.format(tenant_filter=tenant_filter)
            ).to_dataframe()
            for _, row in earn_df.iterrows():
                ed = row.get("next_earnings_date")
                if ed is None or (hasattr(ed, "__float__") and pd.isna(ed)):
                    continue
                ed_date = ed.date() if hasattr(ed, "date") and not isinstance(ed, date) else ed
                days_until = row.get("days_until")
                days_until = int(days_until) if days_until is not None and not pd.isna(days_until) else None
                sector = str(row.get("sector") or "")
                subsector = str(row.get("subsector") or "")
                context["upcoming_earnings"].append({
                    "symbol": str(row.get("symbol") or ""),
                    "company": str(row.get("long_name") or ""),
                    "sector": sector if sector != "Unknown" else "",
                    "subsector": subsector if subsector != "Unknown" else "",
                    "days_until": days_until,
                    "earnings_date_display": (
                        ed_date.strftime("%a %b %-d") if hasattr(ed_date, "strftime") else str(ed_date)[:10]
                    ),
                    "ef_url": earnings_follower_url(
                        symbol=row.get("symbol"), sector=sector, subsector=subsector
                    ),
                })
        except Exception as e:
            if app.debug:
                app.logger.warning("Earnings Watch upcoming query failed: %s", e)

        # ── Movers grouped by YOUR holdings ───────────────────────────
        # Shape: one group per held sector — "you hold MU, NVDA (Technology);
        # here's what's moving around them." Holdings sharing a sector are
        # deliberately merged into one group; the point is "which of my
        # symbols are impacted," not a market-wide mover feed.
        if held_sectors:
            try:
                cfg = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ArrayQueryParameter("sectors", "STRING", sorted(held_sectors)),
                    bigquery.ScalarQueryParameter("min_abs_move", "FLOAT64", 0.05),
                ])
                mov_df = client.query(EARNINGS_WATCH_MOVERS_QUERY, job_config=cfg).to_dataframe()

                # Held symbols per sector (the group headers).
                held_by_sector = {}
                for _, hrow in held_df.iterrows():
                    hsec = str(hrow.get("sector") or "").strip()
                    if not hsec or hsec == "Unknown":
                        continue
                    hsym = str(hrow.get("symbol") or "").strip().upper()
                    if hsym:
                        held_by_sector.setdefault(hsec, set()).add(hsym)

                # Peer movers per sector (held symbols excluded — those are
                # covered by the position pages / Today's movers).
                movers_by_sector = {}
                for _, row in mov_df.iterrows():
                    sym = str(row.get("symbol") or "").strip().upper()
                    if not sym or sym in held_symbols:
                        continue
                    sector = str(row.get("sector") or "")
                    bucket = movers_by_sector.setdefault(sector, [])
                    if len(bucket) >= EARNINGS_WATCH_MOVERS_PER_SECTOR:
                        continue
                    pct = row.get("pct_change")
                    pct = float(pct) if pct is not None and not pd.isna(pct) else None
                    subsector = str(row.get("subsector") or "")
                    bucket.append({
                        "symbol": sym,
                        "company": str(row.get("long_name") or ""),
                        "sector": sector if sector != "Unknown" else "",
                        "subsector": subsector if subsector != "Unknown" else "",
                        "pct_change": pct,
                        "ef_url": earnings_follower_url(
                            symbol=sym, sector=sector, subsector=subsector
                        ),
                    })

                # Assemble groups: every held sector appears (even with no
                # movers — "nothing notable around these" is information),
                # ordered by biggest peer move so the loudest group leads.
                groups = []
                for sec in sorted(held_by_sector):
                    movers = movers_by_sector.get(sec, [])
                    groups.append({
                        "sector": sec,
                        "held": sorted(held_by_sector[sec]),
                        "movers": movers,
                        "max_abs_move": max(
                            (abs(m["pct_change"]) for m in movers
                             if m["pct_change"] is not None),
                            default=0.0,
                        ),
                    })
                groups.sort(key=lambda g: g["max_abs_move"], reverse=True)
                context["mover_groups"] = groups
            except Exception as e:
                if app.debug:
                    app.logger.warning("Earnings Watch movers query failed: %s", e)
    except Exception as e:
        if app.debug:
            raise
        app.logger.warning("Earnings Watch page failed: %s", e)
        context["error"] = "Couldn't load earnings data right now."

    return render_template("earnings_watch.html", **context)


@app.errorhandler(RequestEntityTooLarge)
def request_entity_too_large(_e):
    """CSV uploads exceed MAX_CONTENT_LENGTH (see config MAX_UPLOAD_MB)."""
    flash(
        "Upload too large. Try a shorter date range in your export, or raise MAX_UPLOAD_MB.",
        "danger",
    )
    if current_user.is_authenticated:
        return redirect(url_for("upload"))
    return redirect(url_for("index"))
