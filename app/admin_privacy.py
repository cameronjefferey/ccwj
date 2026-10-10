"""Private-from-admin list.

A friend (or an admin, adding them) can put a user on ``admin_hidden_users``.
After that, every admin or unscoped warehouse read drops that user's
tenants, and an admin cannot take them back off the list. Only the user
can. This is an application gate, not encryption: someone with warehouse
or database credentials can still read the rows.

The demo tenant and any account owned by an admin username stay visible.
The signed-in user's own tenants are never treated as hidden from that
user, so their pages do not change.
"""
from __future__ import annotations

import logging

_log = logging.getLogger(__name__)


def _viewer_id():
    """Numeric id of the signed-in user, or None outside a request."""
    try:
        from flask import has_request_context
        from flask_login import current_user
        from app.demo_guard import numeric_user_id

        if not has_request_context():
            return None
        if not getattr(current_user, "is_authenticated", False):
            return None
        return numeric_user_id(getattr(current_user, "id", None))
    except Exception:
        return None


def _demo_tenant_id() -> str:
    from app.demo_guard import DEMO_TENANT_ID
    return DEMO_TENANT_ID


def is_hidden_from_admins(user_id) -> bool:
    """True when this user is on the list. Lookup errors return False."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    try:
        from app.db import fetch_one
        row = fetch_one(
            "SELECT 1 AS ok FROM admin_hidden_users WHERE user_id = %s",
            (uid,),
        )
        return row is not None
    except Exception:
        _log.exception("admin privacy lookup failed for user_id=%s", uid)
        return False


def hidden_record(user_id):
    """``{user_id, added_at, added_by}`` or None."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    try:
        from app.db import fetch_one
        return fetch_one(
            "SELECT user_id, added_at, added_by FROM admin_hidden_users "
            "WHERE user_id = %s",
            (uid,),
        )
    except Exception:
        _log.exception("admin privacy record lookup failed for user_id=%s", uid)
        return None


def format_privacy_since(added_at) -> str:
    """``Oct 10, 2026`` in America/New_York when the timestamp has a zone."""
    if added_at is None:
        return ""
    try:
        from zoneinfo import ZoneInfo
        if getattr(added_at, "tzinfo", None) is not None:
            added_at = added_at.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        pass
    try:
        return f"{added_at.strftime('%b')} {added_at.day}, {added_at.year}"
    except Exception:
        return ""


def list_hidden_users():
    """Username and the date added. No accounts, balances, or symbols."""
    try:
        from app.db import fetch_all
        return fetch_all(
            """
            SELECT h.user_id, u.username, h.added_at
            FROM admin_hidden_users h
            JOIN users u ON u.id = h.user_id
            ORDER BY h.added_at DESC, u.username
            """
        ) or []
    except Exception:
        _log.exception("admin privacy list failed")
        return []


def _target_blocked(user) -> str | None:
    """Why this user cannot be put on the list, or None when they can."""
    if user is None:
        return "No user with that username or email."
    username = (getattr(user, "username", None) or "").strip()
    if username.lower() == "demo":
        return "The demo account can't be put on this list."
    from app.models import is_admin
    if is_admin(username):
        return "Admin accounts stay visible. This list is for other people."
    return None


def add_hidden_user(user_id, added_by) -> tuple[bool, str]:
    """Put ``user_id`` on the list. Idempotent. Returns (ok, message)."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False, "No user with that username or email."
    from app.models import User
    user = User.get_by_id(uid)
    blocked = _target_blocked(user)
    if blocked:
        return False, blocked
    try:
        adder = int(added_by) if added_by is not None else None
    except (TypeError, ValueError):
        adder = None
    try:
        from app.db import execute
        execute(
            """
            INSERT INTO admin_hidden_users (user_id, added_by)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (uid, adder),
        )
    except Exception:
        _log.exception("admin privacy add failed for user_id=%s", uid)
        return False, "Couldn't save that. Try again."
    _clear_request_cache()
    return True, user.username


def remove_hidden_user(user_id, actor_user_id) -> bool:
    """Take ``user_id`` off the list. Only that same user may do it.

    An admin actor, a missing actor, and an impersonation session all
    return False and leave the row in place.
    """
    try:
        uid = int(user_id)
        actor = int(actor_user_id)
    except (TypeError, ValueError):
        return False
    if uid != actor:
        return False
    if _impersonation_active():
        return False
    try:
        from app.db import execute
        execute(
            "DELETE FROM admin_hidden_users WHERE user_id = %s",
            (uid,),
        )
    except Exception:
        _log.exception("admin privacy remove failed for user_id=%s", uid)
        return False
    _clear_request_cache()
    return True


def _impersonation_active() -> bool:
    try:
        from flask import has_request_context, session
        if not has_request_context():
            return False
        return bool(session.get("_impersonator_id"))
    except Exception:
        return False


def _clear_request_cache() -> None:
    try:
        from flask import g, has_request_context
        if has_request_context():
            g._hidden_tenant_ids = None
            g._hidden_user_ids = None
    except Exception:
        pass


def hidden_user_id_set() -> set[int]:
    """User ids on the list. Empty when the lookup fails."""
    try:
        from flask import g, has_request_context
        if has_request_context():
            cached = getattr(g, "_hidden_user_ids", None)
            if cached is not None:
                return cached
    except Exception:
        cached = None
    ids = _load_hidden_user_ids()
    try:
        from flask import g, has_request_context
        if has_request_context():
            g._hidden_user_ids = ids
    except Exception:
        pass
    return ids


def _load_hidden_user_ids() -> set[int]:
    try:
        from app.db import fetch_all
        rows = fetch_all("SELECT user_id FROM admin_hidden_users") or []
    except Exception:
        _log.exception("admin privacy user-id lookup failed")
        return set()
    out = set()
    for row in rows:
        try:
            out.add(int(row["user_id"]))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def hidden_tenant_id_set() -> set[str]:
    """Tenant ids an admin/unscoped read must drop.

    Never includes the demo tenant, an admin's own accounts, or the
    signed-in viewer's own accounts. Empty when the lookup fails (the
    same fail-open as the paper-tenant exclusion: a Postgres blip must
    not blank every admin page). Explicit ``?tenant=`` URLs still 404
    when this set positively contains the id.
    """
    try:
        from flask import g, has_request_context
        if has_request_context():
            cached = getattr(g, "_hidden_tenant_ids", None)
            if cached is not None:
                return cached
    except Exception:
        pass
    ids = _load_hidden_tenant_ids()
    try:
        from flask import g, has_request_context
        if has_request_context():
            g._hidden_tenant_ids = ids
    except Exception:
        pass
    return ids


def _load_hidden_tenant_ids() -> set[str]:
    from app.demo_guard import DEMO_TENANT_ID
    from app.models import is_admin
    from app.tenant_scope import sanitize_tenant_id

    viewer = _viewer_id()
    try:
        from app.db import fetch_all
        rows = fetch_all(
            """
            SELECT bt.tenant_id, bt.user_id, u.username
            FROM admin_hidden_users h
            JOIN broker_tenants bt ON bt.user_id = h.user_id
            JOIN users u ON u.id = h.user_id
            """
        ) or []
    except Exception:
        _log.exception("admin privacy tenant lookup failed")
        return set()
    out = set()
    for row in rows:
        tid = sanitize_tenant_id(row.get("tenant_id"))
        if not tid or tid == DEMO_TENANT_ID:
            continue
        username = (row.get("username") or "").strip()
        if username.lower() == "demo" or is_admin(username):
            continue
        try:
            owner = int(row.get("user_id"))
        except (TypeError, ValueError):
            owner = None
        if viewer is not None and owner == viewer:
            continue
        out.add(tid)
    return out


def find_user_by_username_or_email(raw: str):
    """Resolve an admin add-form value. ``@`` means email, else username."""
    text = (raw or "").strip()
    if not text:
        return None
    from app.models import User
    if "@" in text:
        found = User.get_by_email(text)
        if found is not None:
            return found
    return User.get_by_username(text.lstrip("@"))
