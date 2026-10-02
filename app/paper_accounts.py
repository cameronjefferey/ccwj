"""Alpaca Paper is practice money. It is not part of the real book.

Warehouse ``broker_slug`` is the first word of the account label, so
"Alpaca Paper Account" becomes ``alpaca`` — the same slug as a live Alpaca
account. Paper is identified from the broker row (label / account name),
never from that slug and never by merging tenants across users.
"""
from __future__ import annotations

import logging
import os

_log = logging.getLogger(__name__)

PAPER_LABEL = "Paper"
_PAPER_MARKERS = ("alpaca paper", "alpaca-paper")


def _blob(row) -> str:
    if not row:
        return ""
    parts = (
        row.get("broker_label"),
        row.get("account_name"),
        row.get("display_nickname"),
        row.get("institution_name"),
    )
    return " ".join(str(part or "") for part in parts).casefold()


def is_paper_row(row) -> bool:
    """True for an Alpaca Paper broker row. Demo and live Alpaca are not."""
    text = _blob(row)
    if not text:
        return False
    tid = str((row or {}).get("tenant_id") or "")
    if tid.startswith("demo:"):
        return False
    return any(marker in text for marker in _PAPER_MARKERS)


def _generic_paper_name(text) -> bool:
    cleaned = " ".join(str(text or "").split()).casefold()
    if not cleaned:
        return False
    if cleaned in {PAPER_LABEL.casefold(), "unnamed account"}:
        return True
    return any(marker in cleaned for marker in _PAPER_MARKERS)


def paper_display_label(row) -> str | None:
    """``Paper``, or a nickname the user actually typed. None when not paper."""
    if not is_paper_row(row):
        return None
    nick = " ".join(str((row or {}).get("display_nickname") or "").split())
    name = " ".join(str((row or {}).get("account_name") or "").split())
    if nick and nick != name and not _generic_paper_name(nick):
        return nick
    return PAPER_LABEL


def all_paper_tenant_ids() -> list[str]:
    """Every Alpaca Paper ``tenant_id``. Empty when the lookup cannot run.

    Admin unscoped reads (``tenant_ids is None``) use this list to keep
    paper out of real totals. A database miss returns ``[]`` so that
    read stays unscoped instead of blanking the page.
    """
    g = None
    in_request = False
    try:
        from flask import g as flask_g
        from flask import has_request_context

        in_request = bool(has_request_context())
        if in_request:
            g = flask_g
    except Exception:
        g = None
        in_request = False
    if in_request and g is not None and hasattr(g, "_all_paper_tenant_ids"):
        return list(g._all_paper_tenant_ids)
    ids = _load_paper_tenant_ids()
    if in_request and g is not None:
        g._all_paper_tenant_ids = list(ids)
    return list(ids)


def _load_paper_tenant_ids() -> list[str]:
    if os.environ.get("HAPPYTRADER_SKIP_DB_INIT") == "1":
        return []
    try:
        from app.db import fetch_all

        rows = fetch_all(
            "SELECT tenant_id, account_name, broker_label, display_nickname "
            "FROM broker_tenants "
            "WHERE connection_status IN ('active', 'disconnected')"
        ) or []
    except Exception as exc:
        _log.warning("paper tenant lookup failed: %s", exc)
        return []
    out = []
    for row in rows:
        tid = row.get("tenant_id")
        if tid and is_paper_row(row):
            out.append(tid)
    return out


def drop_paper_from_mixed_book(ids, rows_by_id):
    """Drop paper from a mixed book. A paper-only set stays paper.

    ``None`` is the admin unscoped bypass and is left alone here. The SQL
    and DataFrame filters drop known paper tenants on that path. Ids we
    cannot classify stay in the result so a lookup miss does not blank
    the page.
    """
    if ids is None:
        return None
    paper = {
        tid for tid, row in (rows_by_id or {}).items() if is_paper_row(row)
    }
    real = [tid for tid in ids if tid not in paper]
    if real:
        return real
    return list(ids)


def paper_scope_note(user_id, scoped_ids) -> dict:
    """Whether this scope is the paper book, and which paper ids were left out.

    ``scoped_ids is None`` is the unscoped real book (admin). Paper ids
    sit beside that total instead of inside it.
    """
    empty = {"paper_only": False, "aside_ids": []}
    if scoped_ids is None:
        if not user_id:
            return empty
        return {"paper_only": False, "aside_ids": list(all_paper_tenant_ids())}
    if not user_id:
        return empty
    try:
        from app.models import get_broker_tenants_for_user

        rows = get_broker_tenants_for_user(user_id) or []
    except Exception as exc:
        _log.warning("paper scope note failed: %s", exc)
        return empty
    paper = [row.get("tenant_id") for row in rows if is_paper_row(row) and row.get("tenant_id")]
    scoped = set(scoped_ids or [])
    paper_set = set(paper)
    paper_only = bool(scoped) and scoped <= paper_set
    aside = [tid for tid in paper if tid not in scoped]
    return {"paper_only": paper_only, "aside_ids": aside}


def position_link_symbol(symbol: str) -> str:
    """Index options are stored as SPXW. The position page uses that root."""
    text = str(symbol or "").strip().upper()
    if text in {"SPX", "SPXW"}:
        return "SPXW"
    return text


def _view_state(user_id) -> dict:
    """``view`` is simple or full. ``offer`` is the Full-view prompt.

    A missing column, a missing user, or a DB error is the full app with
    no prompt. The row is reused for the rest of this request.
    """
    empty = {"user_id": user_id, "view": "full", "offer": False}
    if not user_id:
        return empty
    try:
        from flask import g, has_request_context

        in_req = has_request_context()
    except Exception:
        g = None
        in_req = False
    if in_req:
        cached = getattr(g, "_ht_app_view_state", None)
        if isinstance(cached, dict) and cached.get("user_id") == user_id:
            return cached
    try:
        from app.models import _postgres_user_id
        from app.db import fetch_one

        uid = _postgres_user_id(user_id)
        if uid is None:
            return empty
        row = fetch_one(
            "SELECT app_view, full_view_offer FROM users WHERE id = %s",
            (uid,),
        )
    except Exception as exc:
        _log.warning("app_view read failed: %s", exc)
        return empty
    view = str((row or {}).get("app_view") or "full").strip().lower()
    if view not in {"simple", "full"}:
        view = "full"
    state = {
        "user_id": user_id,
        "view": view,
        "offer": bool((row or {}).get("full_view_offer")) and view == "simple",
    }
    if in_req:
        setattr(g, "_ht_app_view_state", state)
    return state


def _clear_view_cache() -> None:
    try:
        from flask import g, has_request_context

        if has_request_context() and hasattr(g, "_ht_app_view_state"):
            delattr(g, "_ht_app_view_state")
    except Exception:
        return


def get_app_view(user_id) -> str:
    """``simple`` or ``full``. Missing column, missing user, or a DB error is full."""
    return _view_state(user_id)["view"]


def full_view_offer_open(user_id) -> bool:
    """True when Simple should show the one-click switch to Full."""
    return bool(_view_state(user_id)["offer"])


def set_app_view(user_id, view: str, *, chosen: bool = False) -> bool:
    """Store Simple or Full.

    ``chosen=True`` is a click in Settings or on the Full-view prompt.
    That sticks: a later paper path will not replace it. ``chosen=False``
    only writes Simple, and only while the user has not picked a view.
    """
    if view not in {"simple", "full"} or not user_id:
        return False
    if not chosen and view != "simple":
        return False
    try:
        from app.models import _postgres_user_id
        from app.db import execute

        uid = _postgres_user_id(user_id)
        if uid is None:
            return False
        if chosen:
            execute(
                "UPDATE users SET app_view = %s, app_view_chosen = TRUE, "
                "full_view_offer = FALSE WHERE id = %s",
                (view, uid),
            )
        else:
            execute(
                "UPDATE users SET app_view = 'simple' "
                "WHERE id = %s AND app_view_chosen = FALSE",
                (uid,),
            )
        _clear_view_cache()
        return True
    except Exception as exc:
        _log.warning("app_view write failed: %s", exc)
        return False


def apply_learning_view(user_id) -> bool:
    """Simple view for a learning or paper path the user has not overridden."""
    return set_app_view(user_id, "simple", chosen=False)


def offer_full_view_prompt(user_id) -> bool:
    """Ask a Simple user to switch. Does not change ``app_view``."""
    if not user_id:
        return False
    try:
        from app.models import _postgres_user_id
        from app.db import execute

        uid = _postgres_user_id(user_id)
        if uid is None:
            return False
        execute(
            "UPDATE users SET full_view_offer = TRUE "
            "WHERE id = %s AND app_view = 'simple'",
            (uid,),
        )
        _clear_view_cache()
        return True
    except Exception as exc:
        _log.warning("full view offer failed: %s", exc)
        return False


def dismiss_full_view_offer(user_id) -> bool:
    """Hide the prompt and leave the current view in place."""
    if not user_id:
        return False
    try:
        from app.models import _postgres_user_id
        from app.db import execute

        uid = _postgres_user_id(user_id)
        if uid is None:
            return False
        execute(
            "UPDATE users SET full_view_offer = FALSE WHERE id = %s",
            (uid,),
        )
        _clear_view_cache()
        return True
    except Exception as exc:
        _log.warning("full view offer dismiss failed: %s", exc)
        return False


def connect_view_action(*, had_real, newly_saved, paper_saved) -> str | None:
    """What a portal return should do to the view.

    ``simple`` — the new accounts are only Alpaca Paper, and the user had
    no real brokerage yet. ``offer_full`` — the first real brokerage just
    landed; the view stays put and Simple gets a switch prompt. Anything
    else, including a lookup failure, leaves the view alone.
    """
    if had_real is not False or not newly_saved:
        return None
    if paper_saved == newly_saved and paper_saved > 0:
        return "simple"
    if newly_saved > paper_saved:
        return "offer_full"
    return None


def note_connect_view(user_id, *, had_real, newly_saved, paper_saved) -> str | None:
    """Apply the portal-return view rule. Never raises."""
    action = connect_view_action(
        had_real=had_real, newly_saved=newly_saved, paper_saved=paper_saved,
    )
    try:
        if action == "simple":
            apply_learning_view(user_id)
        elif action == "offer_full":
            offer_full_view_prompt(user_id)
    except Exception as exc:
        _log.warning("connect view update failed: %s", exc)
    return action


FULL_VIEW_PAGES = {
    "/strategies": "Strategies",
    "/story": "Trader Profile",
    "/insights": "AI Insights",
}


def safe_full_view_next(raw) -> str | None:
    """A same-site Full-view path. Anything else is dropped."""
    text = (raw or "").strip()
    if not text.startswith("/") or text.startswith("//") or "\\" in text:
        return None
    path, _, query = text.partition("?")
    if path not in FULL_VIEW_PAGES:
        return None
    if any(ch in query for ch in " <>\"'#"):
        return None
    return path if not query else f"{path}?{query}"


def simple_view_hold():
    """The switch-to-Full card, or None when this request should load the page."""
    from flask import render_template, request
    from flask_login import current_user

    if not getattr(current_user, "is_authenticated", False):
        return None
    path = request.path.rstrip("/") or "/"
    title = FULL_VIEW_PAGES.get(path)
    if not title or get_app_view(current_user.id) != "simple":
        return None
    nxt = request.full_path[:-1] if request.full_path.endswith("?") else request.full_path
    return render_template(
        "simple_hold.html",
        title=title,
        page_title=title,
        next_path=nxt,
    )


def viewer_flags(user_id) -> tuple[bool, bool]:
    """``(simple_view, has_paper_account)``. Both fail closed to the full app."""
    simple = get_app_view(user_id) == "simple"
    paper = False
    try:
        from app.models import get_broker_tenants_for_user

        paper = any(is_paper_row(row) for row in (get_broker_tenants_for_user(user_id) or []))
    except Exception as exc:
        _log.warning("paper flag failed: %s", exc)
        paper = False
    return simple, paper
