"""Shared page plumbing + back-compat facade (routes.py refactor, Aug 2026).

This used to be an ~8,900-line monolith holding every page. The pages now
live in their own modules — app/marketing.py, app/positions_page.py,
app/position_detail.py, app/symbols_page.py, app/sectors_page.py,
app/strategy_fit.py, app/accounts_page.py, app/earnings_page.py — with the
chart machinery in app/pnl_charts.py. What remains here is:

1. The SHARED helpers every page imports: tenant scoping
   (_tenants_for_scope, _user_account_list, _user_tenant_list), account
   label mapping/disambiguation, _bq_parallel, leg/session/tag helpers,
   and _parse_date.
2. A RE-EXPORT facade (imports at top for pnl_charts, at the BOTTOM for
   the page modules) so the 10+ modules and 40+ tests that do
   ``from app.routes import X`` keep working unchanged. The bottom
   placement is load-bearing: page modules import helpers from
   app.routes, which only works once everything above is defined.

Tenancy contract is unchanged: every BQ read is scoped by tenant_id in
SQL and/or _filter_df_by_tenant_ids on the frame — see
.cursor/rules/bigquery-tenant-isolation.mdc.
"""

from flask import request, redirect, url_for, flash
from werkzeug.exceptions import RequestEntityTooLarge
from flask_login import current_user
from app import app
from app.bigquery_client import get_bigquery_client
from app.query_cache import cached_query_df  # noqa: F401  (re-export; tests patch it here)
from app.models import (
    get_broker_tenants_for_user,
    get_tenant_ids_for_user,
    is_admin,
)
# Re-exported: insights/first_look/strategies/weekly_review/wealth and the
# isolation tests all import these via app.routes.
from app.tenant_scope import (  # noqa: F401
    filter_df_by_tenant_ids as _filter_df_by_tenant_ids,
    resolve_filter_tenant_ids as _resolve_filter_tenant_ids,
    sanitize_tenant_id as _sanitize_tenant_id,
    tenant_sql_and as _tenant_sql_and,
    tenant_sql_filter as _tenant_sql_filter,
)
# Chart builders were extracted to app/pnl_charts.py (routes.py refactor).
# Re-exported here because page routes below call them and tests/back-compat
# callers import them from app.routes.
from app.pnl_charts import (  # noqa: F401
    CHART_KPI_ALIGN_TOLERANCE_DOLLARS,
    CHART_SUBSTITUTION_KPI_MARGIN,
    _addback_phantom_writeoffs_to_summary,
    _align_position_pnl_chart_with_kpi,
    _build_account_chart_from_daily_pnl,
    _build_chart_from_daily_pnl,
    _build_chart_from_daily_pnl_partition,
    _build_option_matrices,
    _chart_data_for_json,
    _chart_data_terminal,
    _collapse_mart_daily_pnl_duplicate_grain,
    _collect_activity_candidate_dates,
    _cumulative_pnl_from_leg_closes,
    _cumulative_pnl_from_stg_trades,
    _dedupe_enriched_current_positions,
    _drop_phantom_equity_writeoffs,
    _equity_slice_for_live_chart,
    _filter_current_for_chart_partition,
    _merge_position_pnl_chart_payloads,
    _narrow_mart_daily_pnl_chart_df_to_summary_tenant,
    _snap_position_chart_terminal_to_breakdown,
    _synthetic_cumulative_pnl_for_position,
    _walk_equity_terminal,
)
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor
import logging
import math
import os
import re
import pandas as pd
import json
from urllib.parse import quote_plus

_log = logging.getLogger(__name__)


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

    # Cap at 16: pages fan out to ~11 tiny queries (each dominated by BQ's
    # ~1-2s fixed per-job latency, NOT scan size — stg_history is ~1MB). With
    # the old cap of 8 an 11-query page ran in 2 waves (~2x the floor); 16
    # lets the whole batch run in a single wave. BQ handles the concurrent
    # small jobs fine and the pool is per-request/short-lived.
    with ThreadPoolExecutor(max_workers=min(len(queries), 16)) as pool:
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
                    _log.error("_bq_parallel: query %r failed: %s", name, exc)

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
    except Exception as exc:
        _log.warning("apply_account_labels failed for col=%r: %s", col, exc)
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
      2. ``?tenants=<tid>,<tid>`` — multi-account on/off toggle set (the
         Position Detail account toggles). A SUBSET of the user's owned
         tenants; validated the same way as ``?tenant=`` so a URL can
         never widen tenancy. Any tenant not owned is dropped; if none of
         the requested ids are owned we fall through to safe defaults.
      3. ``?account=<label>`` (legacy alias) — matches a base label OR a
         disambiguated label (e.g. "Schwab Account (\u2022\u20226342)").
         A bare colliding base label still selects all matching tenants
         for backward compatibility.
      4. No selection → admin: ``None`` (no SQL filter); user: all owned.

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

    # 1b. Multi-tenant addressing (?tenants=) — account on/off toggles.
    #     Encodes "show these accounts, hide the rest". Same never-widen
    #     validation as ?tenant=; intersect with owned for non-admins.
    try:
        requested_tenants_raw = (request.args.get("tenants") or "").strip()
    except Exception:
        requested_tenants_raw = ""
    if requested_tenants_raw:
        requested = [t.strip() for t in requested_tenants_raw.split(",") if t.strip()]
        if requested:
            if admin:
                return list(dict.fromkeys(requested))
            owned = [
                row["tenant_id"]
                for row in (get_broker_tenants_for_user(current_user.id) or [])
            ]
            allowed = [t for t in requested if t in owned]
            if allowed:
                return list(dict.fromkeys(allowed))
            # None owned → ignore the param and fall through to safe defaults.

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


# v2 tenant helpers are imported from ``app.tenant_scope`` (top of file).
# Legacy ``_account_sql_*`` / Stage 3 ``broker_account_id`` helpers were
# removed — see docs/archive/BROKER_ACCOUNT_ID_MIGRATION.md.



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
      - ``tenant_id`` / ``account`` ← carried through so legs can be
        grouped by account when a symbol is traded across several
        accounts (leg_id / display_leg_num restart per tenant, so the
        template groups + labels pills by ``account_display``).

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
            "tenant_id": str(r.get("tenant_id") or ""),
            "account": str(r.get("account") or ""),
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


def _norm_tag_date(v):
    """Coerce a date/datetime/str/Timestamp to a plain ``date`` (or None).

    Used by the position-leg tag matcher — Postgres hands back ``leg_open_date``
    as a ``datetime.date``, while the warehouse leg dates arrive as
    ``YYYY-MM-DD`` strings.
    """
    if v is None or v == "":
        return None
    try:
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        ts = pd.to_datetime(v, errors="coerce")
        if ts is None or pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None


def _tags_for_leg_range(tag_rows, tenant_id, open_date, last_date, symbol=None):
    """Sorted distinct tags whose stored ``leg_open_date`` falls within a leg's
    ``[open_date, last_date]`` for the SAME tenant (and symbol, when given).

    Date-containment (not leg_id equality) is deliberate: a stored tag is
    anchored on the leg's open_date at tag time, and matches back to whatever
    current leg now spans that date. This survives dbt re-chaptering,
    ``display_leg_num`` renumbering, and leg merges (a merged leg's range still
    contains the old open_date). Chapters never overlap, so an anchor date lands
    in at most one leg.

    ``symbol`` MUST be passed whenever ``tag_rows`` can span multiple symbols
    (e.g. ``get_all_leg_tags_for_user``). A tag is stored as
    (tenant_id, symbol, leg_open_date); matching on tenant + date alone lets a
    DIFFERENT symbol's leg whose window happens to contain the anchor date
    steal the tag (real case: an open BP Covered Call spanning 2026-08-03
    cross-matched an ASTS tag anchored on 2026-08-03, so the /positions tag
    filter and /accounts Tag Breakdown attributed BP's P&L to the ASTS tag).
    Position Detail passes already-symbol-scoped rows so it may omit it.
    """
    if not tag_rows:
        return []
    lo = _norm_tag_date(open_date)
    hi = _norm_tag_date(last_date) or lo
    tid = str(tenant_id or "")
    sym = str(symbol or "").upper() if symbol is not None else None
    out = set()
    for r in tag_rows:
        if str(r.get("tenant_id") or "") != tid:
            continue
        if sym is not None and str(r.get("symbol") or "").upper() != sym:
            continue
        anchor = _norm_tag_date(r.get("leg_open_date"))
        if anchor is None:
            continue
        if lo is not None and anchor < lo:
            continue
        if hi is not None and anchor > hi:
            continue
        t = r.get("tag")
        if t:
            out.add(t)
    return sorted(out)


def _resolve_position_leg_filter(sessions_list, leg_param):
    """Resolve legacy ``?leg=`` ids without mixing account-local leg numbers.

    Leg ids restart for every tenant.  The downstream position-detail filter is
    date-range based, so it cannot safely apply a bare leg id while more than
    one tenant is in scope: ``leg=1`` would otherwise select every account's
    first leg and spill each date range across all scoped accounts.  The UI
    narrows to one tenant before setting ``leg``; old/bookmarked ambiguous URLs
    are therefore treated as the unfiltered position view.
    """
    all_leg_ids = [s["session_id"] for s in sessions_list]
    raw = str(leg_param or "").strip()
    if not raw:
        return "", all_leg_ids

    tenant_ids = {
        str(s.get("tenant_id") or "")
        for s in sessions_list
    }
    if len(tenant_ids) > 1:
        return "", all_leg_ids

    selected_legs = []
    for value in raw.split(","):
        try:
            selected_legs.append(int(value.strip()))
        except ValueError:
            pass
    return raw, selected_legs


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


def _parse_date(value):
    """Return a date object if value is a valid YYYY-MM-DD string, else None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None




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


# ----------------------------------------------------------------------
# Extracted page modules (routes.py refactor, Aug 2026).
#
# Importing them here (a) registers their routes exactly as before and
# (b) re-exports names that tests and older callers import from
# app.routes. This import MUST stay at the bottom of the file: the page
# modules import shared helpers from app.routes, which only works once
# everything above this line is defined.
# ----------------------------------------------------------------------
from app.position_detail import (  # noqa: E402,F401
    POSITION_ACCOUNTS_QUERY,
    POSITION_CLOSED_EQUITY_QUERY,
    POSITION_CLOSED_LEGS_QUERY,
    POSITION_CURRENT_QUERY,
    POSITION_DIVIDENDS_QUERY,
    POSITION_EARNINGS_QUERY,
    POSITION_LEGS_QUERY,
    POSITION_MATRIX_QUERY,
    POSITION_SUMMARY_QUERY,
    POSITION_TRADES_QUERY,
    SYMBOL_TABS_QUERY,
    _compute_breakdown_by_type,
    _equity_raw_trades_for_partial_close_outcome,
    _fetch_closed_option_legs_from_classification,
    _fetch_int_strategy_classification_by_symbol,
    _merge_position_strategy_breakdown,
    _realized_pnl_from_closed_frames,
    _rollup_int_strategy_to_summary_shape,
    _supplement_summary_with_rolled,
    _synthetic_open_strategy_from_current,
    add_position_tag,
    position_detail,
    remove_position_tag,
)
# accounts_page must come before positions_page: positions_page imports
# ACCOUNT_LEGS_QUERY via app.routes, which is only bound once this line runs.
from app.accounts_page import (  # noqa: E402,F401
    ACCOUNT_BALANCES_QUERY,
    ACCOUNT_LEGS_QUERY,
    ACCOUNT_POSITIONS_SUMMARY_QUERY,
    ACCOUNTS_RANGE_DAYS,
    ACCOUNTS_VALID_RANGES,
    STRATEGY_CLASSIFICATION_QUERY,
    _accounts_range_start,
    _accounts_scope_query,
    _build_account_breakdowns,
    _build_strategy_time_chart,
    _build_tag_breakdown,
    _validate_accounts_financial_frames,
    accounts,
    accounts_breakdown_fragment,
)
from app.positions_page import (  # noqa: E402,F401
    DATE_FILTERED_QUERY,
    DEFAULT_QUERY,
    ERROR_DEFAULTS,
    POSITIONS_TAG_STRAT_QUERY,
    _tag_scoped_positions_df,
    positions,
)
from app.sectors_page import (  # noqa: E402,F401
    SECTORS_QUERY,
    industries_legacy,
    sectors,
)
from app.strategy_fit import (  # noqa: E402,F401
    DIM_FIXED_COL_ORDER,
    DIM_META,
    STRATEGY_FIT_OPTIONS_QUERY,
    STRATEGY_FIT_QUERY,
    _build_strategy_fit_matrix,
    _strategy_fit_insight_context,
    _strategy_fit_render_payload,
    strategy_fit,
)
from app.earnings_page import (  # noqa: E402,F401
    EARNINGS_WATCH_HELD_QUERY,
    EARNINGS_WATCH_MOVERS_PER_SECTOR,
    EARNINGS_WATCH_MOVERS_QUERY,
    EARNINGS_WATCH_UPCOMING_QUERY,
    earnings_watch,
)
from app.symbols_page import (  # noqa: E402,F401
    CLOSED_EQUITY_LEGS_QUERY,
    CLOSED_LEGS_QUERY,
    CURRENT_POSITIONS_QUERY,
    OPEN_SESSION_START_QUERY,
    STRATEGIES_MAP_QUERY,
    SYMBOLS_PNL_QUERY,
    TRADES_QUERY,
    _finish_symbol_chart,
    symbols_detail,
)
