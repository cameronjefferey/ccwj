"""Privacy mode — mask identity and account balances at the data layer.

The toggle is stored on ``user_profiles.privacy_mode`` (and, for the shared
demo login, only in the session so one viewer cannot flip it for everyone).
The same flag is mirrored into localStorage ``ht-privacy`` so the header
control and any client cache key stay in step with what the server rendered.

Masking happens when a label or balance is turned into text. Tables that
re-sort or redraw from ``data-val`` / ``account_display`` keep the neutral
string, because that string is what was written into the cell. This is not
a CSS overlay.

Trade-level P&L and prices are left alone. Only account value, cash, and
per-account balances go through ``privacy_balance`` / ``privacy_signed``.
"""

from __future__ import annotations

import re

PRIVACY_MASK = "••••"
PRIVACY_STORAGE_KEY = "ht-privacy"
_SESSION_KEY = "ht_privacy_mode"


def slots_from_rows(rows) -> tuple[dict, dict]:
    """Stable ``Account N`` labels for a user's brokerage tenants.

    Numbering follows ``tenant_id`` sort order so the same account is
    ``Account 1`` on every page. Nicknames and broker account names map
    to that slot so a template that only has the display string still
    masks. A repeated broker label (several "Schwab Account" rows) keeps
    the first slot; callers that pass ``tenant_id`` get the right number.
    """
    ordered = sorted(
        (r for r in (rows or []) if (r.get("tenant_id") or "").strip()),
        key=lambda r: (r.get("tenant_id") or "").strip(),
    )
    by_tid = {}
    by_name = {}
    for i, row in enumerate(ordered, 1):
        label = f"Account {i}"
        tid = (row.get("tenant_id") or "").strip()
        by_tid[tid] = label
        for key in (row.get("display_nickname"), row.get("account_name")):
            name = (key or "").strip()
            if name and name not in by_name:
                by_name[name] = label
    return by_tid, by_name


def mask_account_label(name, tenant_id=None, *, enabled=None, by_tid=None, by_name=None):
    """Return ``Account N`` when privacy mode is on, else ``None``.

    ``None`` means "leave the real nickname alone" so the existing
    ``account_label`` filter keeps its current behavior.
    """
    if enabled is None:
        if not privacy_mode_on():
            return None
        by_tid, by_name = viewer_slots()
    elif not enabled:
        return None
    else:
        by_tid = by_tid or {}
        by_name = by_name or {}

    tid = str(tenant_id or "").strip()
    if tid and tid in by_tid:
        return by_tid[tid]
    raw = str(name or "").strip()
    if raw and raw in by_name:
        return by_name[raw]
    if len(by_tid) == 1:
        return next(iter(by_tid.values()))
    return "Account"


def shown_account(name, tenant_id=None):
    """Nickname when privacy is off, ``Account N`` when it is on."""
    masked = mask_account_label(name, tenant_id)
    return masked if masked is not None else name


def mask_secret(value, *, enabled=None):
    """Username, email, broker name, or account number → ``••••``."""
    if enabled is None:
        enabled = privacy_mode_on()
    if not enabled:
        return value
    if value is None or value == "":
        return value
    return PRIVACY_MASK


def format_privacy_balance(value, digits=0, *, enabled=None):
    """Account value / cash / per-account balance. ``••••`` when private."""
    if enabled is None:
        enabled = privacy_mode_on()
    if enabled:
        if value is None or value == "":
            return "—"
        return PRIVACY_MASK
    return _format_dollars(value, digits, signed=False)


def format_privacy_signed(value, digits=0, *, enabled=None):
    """Signed account-level dollar change. ``••••`` when private."""
    if enabled is None:
        enabled = privacy_mode_on()
    if enabled:
        if value is None or value == "":
            return "—"
        return PRIVACY_MASK
    return _format_dollars(value, digits, signed=True)


_ACCOUNT_NUM = re.compile(r"^Account\s+(\d+)$")


def masked_label_sort_key(label):
    """``Account 2`` before ``Account 10``. Unnumbered labels follow."""
    text = " ".join(str(label or "").split())
    match = _ACCOUNT_NUM.match(text)
    if match:
        return (0, int(match.group(1)), "")
    return (1, 0, text.lower())


def sort_masked_account_choices(choices, *, enabled=None, by_tid=None, by_name=None):
    """Privacy mode: order a ``{tenant_id, label}`` list by ``Account N``.

    Privacy off keeps the caller's order (nickname sort on the pickers).
    """
    items = [c for c in (choices or [])]
    if enabled is None:
        enabled = privacy_mode_on()
    if not enabled:
        return items
    if by_tid is None or by_name is None:
        by_tid, by_name = viewer_slots()

    def key(choice):
        label = (choice or {}).get("label")
        tid = (choice or {}).get("tenant_id")
        masked = mask_account_label(
            label, tid, enabled=True, by_tid=by_tid, by_name=by_name,
        )
        return masked_label_sort_key(masked if masked is not None else label)

    return sorted(items, key=key)


def sort_rows_by_masked_label(rows, label_of, *, enabled=None, by_tid=None, by_name=None):
    """Same order as ``sort_masked_account_choices`` for rows that aren't picker dicts.

    ``label_of(row)`` returns ``(display_label, tenant_id)``.
    """
    items = list(rows or [])
    if enabled is None:
        enabled = privacy_mode_on()
    if not enabled:
        return items
    if by_tid is None or by_name is None:
        by_tid, by_name = viewer_slots()

    def key(row):
        raw, tid = label_of(row)
        masked = mask_account_label(
            raw, tid, enabled=True, by_tid=by_tid, by_name=by_name,
        )
        return masked_label_sort_key(masked if masked is not None else raw)

    return sorted(items, key=key)


def _format_dollars(value, digits, *, signed):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    places = int(digits)
    if signed:
        sign = "+" if number > 0 else "-" if number < 0 else ""
        return f"{sign}${abs(number):,.{places}f}"
    return f"${number:,.{places}f}"


def privacy_mode_on() -> bool:
    """Whether the signed-in viewer is in privacy mode. False when logged out."""
    try:
        from flask import g, has_request_context, session
        from flask_login import current_user
    except Exception:
        return False
    if not has_request_context():
        return False
    cached = getattr(g, "_privacy_mode", None)
    if cached is not None:
        return bool(cached)
    on = False
    try:
        if getattr(current_user, "is_authenticated", False):
            from app.utils import is_demo_user
            if is_demo_user():
                on = bool(session.get(_SESSION_KEY))
            else:
                from app import _viewer_profile
                profile = _viewer_profile() or {}
                on = bool(profile.get("privacy_mode"))
    except Exception:
        on = False
    g._privacy_mode = on
    return on


def viewer_slots():
    """``(by_tenant_id, by_display_name)`` for the signed-in user, request-cached."""
    from flask import g, has_request_context

    empty = ({}, {})
    if not has_request_context():
        return empty
    cached = getattr(g, "_privacy_slots", None)
    if cached is not None:
        return cached
    rows = []
    try:
        from flask_login import current_user
        if getattr(current_user, "is_authenticated", False):
            from app.models import get_broker_tenants_for_user
            rows = get_broker_tenants_for_user(current_user.id) or []
    except Exception:
        rows = []
    slots = slots_from_rows(rows)
    g._privacy_slots = slots
    return slots


def set_privacy_mode(enabled: bool) -> None:
    """Persist the toggle. Demo stays in the session; everyone else in Postgres."""
    from flask import g, session
    from flask_login import current_user

    enabled = bool(enabled)
    from app.utils import is_demo_user
    if is_demo_user():
        session[_SESSION_KEY] = enabled
        session.modified = True
    else:
        from app.models import update_user_profile
        update_user_profile(current_user.id, privacy_mode=enabled)
    g._privacy_mode = enabled


def register_privacy_routes(app):
    from flask import redirect, request, url_for
    from flask_login import login_required

    from app.utils import safe_internal_next

    @app.route("/privacy-mode", methods=["POST"])
    @login_required
    def set_privacy_mode_route():
        enabled = (request.form.get("enabled") or "").strip().lower() in ("1", "true", "on", "yes")
        set_privacy_mode(enabled)
        nxt = safe_internal_next(request.form.get("next") or request.referrer or "")
        return redirect(nxt or url_for("weekly_review"))
