"""Private-from-admin list. Invite-only.

An admin adds someone on ``/admin/private``. That inserts a row in
``admin_hidden_users``. While ``active`` is true, every admin or
unscoped warehouse read drops that user's tenants. The user can turn it
off, which sets ``active`` false and ``opted_out_at`` and keeps the row.
The row is the invite: Settings still shows the card so they can turn
it back on. Someone with no row does not see the card and cannot turn
it on. An admin cannot delete the row or turn privacy off. Adding an
invited user who turned it off turns privacy back on.

This is an application gate, not encryption: someone with warehouse
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


PRIVACY_LIST_UNAVAILABLE = "Private-account list unavailable, try again"
NOT_INVITED = "An admin has to add you to private accounts first."


def is_hidden_from_admins(user_id) -> bool:
    """True when this user's privacy is on.

    An invite with privacy turned off is not hidden. A lookup error
    returns True. An admin action that cannot load the list must not
    open a book it could not classify. This does not flash a message;
    request handlers that render a page do that.
    """
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    snap = hidden_snapshot()
    if not snap["available"]:
        return True
    return uid in snap["user_ids"]


def hidden_record(user_id):
    """Invite row, on or off: ``{user_id, added_at, added_by, active, opted_out_at}``.

    None when this user was never added. ``active`` false means they
    turned privacy off and the invite is still here.
    """
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return None
    try:
        from app.db import fetch_one
        return fetch_one(
            "SELECT user_id, added_at, added_by, active, opted_out_at "
            "FROM admin_hidden_users WHERE user_id = %s",
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
    """Usernames whose privacy is on, and the date added.

    No accounts, balances, or symbols. Invited users who turned it off
    are ``list_opted_out_invites``.
    """
    try:
        from app.db import fetch_all
        return fetch_all(
            """
            SELECT h.user_id, u.username, h.added_at
            FROM admin_hidden_users h
            JOIN users u ON u.id = h.user_id
            WHERE h.active IS TRUE
            ORDER BY h.added_at DESC, u.username
            """
        ) or []
    except Exception:
        _log.exception("admin privacy list failed")
        return []


def list_opted_out_invites():
    """Usernames an admin added who currently have privacy off.

    Username and the date they turned it off. No accounts, balances,
    or symbols.
    """
    try:
        from app.db import fetch_all
        return fetch_all(
            """
            SELECT h.user_id, u.username, h.added_at, h.opted_out_at
            FROM admin_hidden_users h
            JOIN users u ON u.id = h.user_id
            WHERE h.active IS NOT TRUE
            ORDER BY h.opted_out_at DESC NULLS LAST, u.username
            """
        ) or []
    except Exception:
        _log.exception("admin privacy opted-out list failed")
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
    """Invite ``user_id`` and turn privacy on. Idempotent. Returns (ok, message).

    A new row starts active. Adding someone who is already on leaves
    ``added_at`` alone. Adding someone who turned it off turns privacy
    back on and refreshes ``added_at`` so "since" is this on period.
    This is the admin path. A user turning their own privacy back on
    uses ``enable_own_privacy``, which does not create an invite.
    """
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
        from app.db import commit_request_transaction, execute
        execute(
            """
            INSERT INTO admin_hidden_users (user_id, added_by, active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (user_id) DO UPDATE
            SET active = TRUE,
                opted_out_at = NULL,
                added_at = CASE
                    WHEN admin_hidden_users.active IS TRUE
                    THEN admin_hidden_users.added_at
                    ELSE NOW()
                END,
                added_by = CASE
                    WHEN admin_hidden_users.active IS TRUE
                    THEN admin_hidden_users.added_by
                    ELSE EXCLUDED.added_by
                END
            """,
            (uid, adder),
        )
        # Durable before the redirect. A nested request transaction would
        # otherwise roll back when the request socket closes.
        commit_request_transaction()
    except Exception:
        _log.exception("admin privacy add failed for user_id=%s", uid)
        return False, "Couldn't save that. Try again."
    _clear_request_cache()
    return True, user.username


def enable_own_privacy(user_id) -> tuple[bool, str]:
    """Turn privacy on for a user who was already invited.

    Does not create a row. A user with no invite is rejected. An
    impersonation session is rejected. Returns (ok, message).
    """
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False, NOT_INVITED
    from app.models import User
    user = User.get_by_id(uid)
    blocked = _target_blocked(user)
    if blocked:
        return False, blocked
    if _impersonation_active():
        return False, "Couldn't turn that on. An admin can't do it for you."
    if hidden_record(uid) is None:
        return False, NOT_INVITED
    try:
        from app.db import execute_returning
        row = execute_returning(
            """
            UPDATE admin_hidden_users
            SET active = TRUE,
                opted_out_at = NULL,
                added_at = CASE
                    WHEN active IS TRUE THEN added_at
                    ELSE NOW()
                END
            WHERE user_id = %s
            RETURNING user_id
            """,
            (uid,),
        )
    except Exception:
        _log.exception("admin privacy enable failed for user_id=%s", uid)
        return False, "Couldn't save that. Try again."
    if not row:
        return False, NOT_INVITED
    _clear_request_cache()
    return True, user.username


def remove_hidden_user(user_id, actor_user_id) -> bool:
    """Turn privacy off for ``user_id``. Only that same user may do it.

    The invite row stays, with ``active`` false and ``opted_out_at`` set,
    so they can turn it back on. An admin actor, a missing actor, an
    impersonation session, and a user who was never invited all return
    False and leave the table unchanged.
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
        from app.db import execute_returning
        row = execute_returning(
            """
            UPDATE admin_hidden_users
            SET active = FALSE,
                opted_out_at = NOW()
            WHERE user_id = %s
            RETURNING user_id
            """,
            (uid,),
        )
    except Exception:
        _log.exception("admin privacy remove failed for user_id=%s", uid)
        return False
    if not row:
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
    """Drop this request's copy of the list so the next read hits Postgres.

    The warehouse snapshot cache lives only on ``flask.g``. There is no
    Redis copy. An add or remove in this request is visible to the next
    ``hidden_snapshot()`` call, and the next request loads again.
    The in-app page-view skip list is a separate process-local cache;
    this also drops that copy so a newly private user is not logged.
    Warehouse query cache keys include the tenant predicate, so a new
    exclusion set is a different SQL string and cannot reuse the old rows.
    """
    try:
        from flask import g, has_request_context
        if has_request_context() and hasattr(g, "_hidden_snapshot"):
            delattr(g, "_hidden_snapshot")
    except Exception:
        pass
    try:
        from app.funnel import clear_hidden_user_cache
        clear_hidden_user_cache()
    except Exception:
        _log.exception("could not clear app-page privacy cache")


def note_privacy_list_unavailable() -> None:
    """Flash the outage sentence once per request."""
    try:
        from flask import flash, g, has_request_context
        if not has_request_context():
            return
        if getattr(g, "_privacy_list_noted", False):
            return
        g._privacy_list_noted = True
        flash(PRIVACY_LIST_UNAVAILABLE, "warning")
    except Exception:
        _log.exception("could not flash privacy-list outage")


def hidden_snapshot() -> dict:
    """``{available, user_ids, tenant_ids}`` for this request.

    ``available`` is False when Postgres could not answer. That is not
    an empty list: an empty list means the list loaded and nobody (left
    after the exemptions) is on it. A failure is remembered for this
    request only, so one outage does not retry on every query, and the
    next request tries again. ``add_hidden_user`` / ``enable_own_privacy``
    / ``remove_hidden_user`` clear it immediately.
    """
    try:
        from flask import g, has_request_context
        if has_request_context() and hasattr(g, "_hidden_snapshot"):
            return g._hidden_snapshot
    except Exception:
        pass
    try:
        user_ids, tenant_ids = _load_hidden_snapshot()
        snap = {
            "available": True,
            "user_ids": user_ids,
            "tenant_ids": tenant_ids,
        }
    except Exception:
        _log.exception("admin privacy list lookup failed")
        snap = {
            "available": False,
            "user_ids": frozenset(),
            "tenant_ids": frozenset(),
        }
    try:
        from flask import g, has_request_context
        if has_request_context():
            g._hidden_snapshot = snap
    except Exception:
        pass
    return snap


def hidden_user_id_set() -> set[int]:
    """User ids on the list. Raises when the list could not be loaded."""
    snap = hidden_snapshot()
    if not snap["available"]:
        raise RuntimeError(PRIVACY_LIST_UNAVAILABLE)
    return set(snap["user_ids"])


def hidden_tenant_id_set() -> set[str]:
    """Tenant ids an admin/unscoped read must drop.

    Never includes the demo tenant, an admin's own accounts, or the
    signed-in viewer's own accounts. Raises when the list could not be
    loaded — callers must fail closed instead of treating that as an
    empty exclusion set.
    """
    snap = hidden_snapshot()
    if not snap["available"]:
        raise RuntimeError(PRIVACY_LIST_UNAVAILABLE)
    return set(snap["tenant_ids"])


def fail_closed_allow_tenant_ids() -> list[str]:
    """Tenants an unscoped admin read may still show when the list is down.

    The signed-in admin's own accounts, plus the demo tenant. When the
    admin's own accounts cannot be loaded either, the demo tenant is
    still the only id. Never an empty predicate that means everyone.
    """
    from app.demo_guard import DEMO_TENANT_ID
    from app.tenant_scope import sanitize_tenant_id

    ids = []
    demo = sanitize_tenant_id(DEMO_TENANT_ID)
    if demo:
        ids.append(demo)
    viewer = _viewer_id()
    if viewer is None:
        return ids
    try:
        from app.models import get_tenant_ids_for_user
        owned = get_tenant_ids_for_user(viewer) or []
    except Exception:
        _log.exception("admin own-tenant lookup failed during privacy outage")
        return ids
    for tenant_id in owned:
        cleaned = sanitize_tenant_id(tenant_id)
        if cleaned and cleaned not in ids:
            ids.append(cleaned)
    return ids


def redact_from_admin(user_id, username=None) -> bool:
    """True when an admin page should hide this person's book.

    When the list cannot be loaded, everyone except the signed-in admin
    and the demo user is hidden, and the outage sentence is flashed once.
    """
    snap = hidden_snapshot()
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        uid = None
    if not snap["available"]:
        note_privacy_list_unavailable()
        if (username or "").strip().lower() == "demo":
            return False
        viewer = _viewer_id()
        return not (viewer is not None and uid == viewer)
    if uid is None:
        return False
    return uid in snap["user_ids"]


def _load_hidden_snapshot() -> tuple[frozenset[int], frozenset[str]]:
    """Load user ids and tenant ids. Raises on a database error."""
    from app.db import fetch_all
    from app.demo_guard import DEMO_TENANT_ID
    from app.models import is_admin
    from app.tenant_scope import sanitize_tenant_id

    viewer = _viewer_id()
    # Inactive invites stay in the table so the user can turn privacy
    # back on. They are not hidden.
    user_rows = fetch_all(
        "SELECT user_id FROM admin_hidden_users WHERE active IS TRUE"
    ) or []
    user_ids = set()
    for row in user_rows:
        try:
            user_ids.add(int(row["user_id"]))
        except (TypeError, ValueError, KeyError):
            continue
    rows = fetch_all(
        """
        SELECT bt.tenant_id, bt.user_id, u.username
        FROM admin_hidden_users h
        JOIN broker_tenants bt ON bt.user_id = h.user_id
        JOIN users u ON u.id = h.user_id
        WHERE h.active IS TRUE
        """
    ) or []
    tenant_ids = set()
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
        tenant_ids.add(tid)
    return frozenset(user_ids), frozenset(tenant_ids)


def _match_username(name: str):
    """Exact username, then the one account whose name differs only by case.

    ``users.username`` is case-sensitive, so two accounts can share a
    spelling. More than one case-insensitive match is ambiguous and
    returns None. The caller flashes that instead of hiding the wrong
    person.
    """
    name = (name or "").strip()
    if not name:
        return None
    from app.models import User
    exact = User.get_by_username(name)
    if exact is not None:
        return exact
    from app.db import fetch_all
    rows = fetch_all(
        "SELECT id, username, password_hash, email FROM users "
        "WHERE lower(username) = lower(%s)",
        (name,),
    ) or []
    if len(rows) != 1:
        return None
    return User._from_row(rows[0])


def find_user_by_username_or_email(raw: str):
    """Resolve an admin add-form value.

    A single leading ``@`` is a username handle (the signup ping is
    ``@Name``). Any other ``@`` is an email, matched without regard to
    case. Usernames match exactly first, then case-insensitively when
    that spelling belongs to one account. Blank input returns None.
    """
    text = (raw or "").strip()
    if not text:
        return None
    from app.models import User
    # @handle from a signup ping. One leading @ and no other @.
    if text.startswith("@") and text.count("@") == 1:
        return _match_username(text[1:])
    if "@" in text:
        found = User.get_by_email(text)
        if found is not None:
            return found
    return _match_username(text)
