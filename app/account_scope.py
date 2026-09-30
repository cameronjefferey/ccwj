"""Header account picker: nicknames, ``?tenants=``, and a saved choice.

The warehouse filter is still ``?tenants=`` (see ``_tenants_for_scope``).
The header control writes that query so a link stays shareable. A cookie
remembers the last Apply so a later visit with no query opens the same
URL. An explicit ``?tenants=`` / ``?tenant=`` / ``?account=`` on the
request wins and does not overwrite the cookie — a shared link does not
replace the recipient's saved choice. ``?scope=all`` clears the cookie
and the filter (the Reset links).

Picker labels are nicknames only. Broker masks, account-number tails,
and generic "{Broker} Account" labels are not shown.
"""

import re
from collections import Counter
from urllib.parse import unquote, urlencode

from flask import request
from flask_login import current_user

# Pages whose data follows ``_tenants_for_scope``. Other routes ignore
# the cookie so Settings and Admin are not bounced into a tenant query.
SCOPE_ENDPOINTS = frozenset({
    "weekly_review",
    "overview_below",
    "today_view",
    "day_detail",
    "trader_story",
    "positions",
    "accounts",
    "accounts_breakdown_fragment",
    "strategies",
    "sectors",
    "earnings_watch",
    "insights",
    "position_detail",
})

TENANT_COOKIE = "ht_tenants"
_COOKIE_MAX_AGE = 60 * 60 * 24 * 365

# Last-4 masks and long digit runs are account identifiers, not names.
_LEAK = re.compile(r"[•∙*]{2,}|\d{4,}|[0-9a-f]{8}-[0-9a-f]{4}-", re.I)
_GENERIC_BROKER = re.compile(
    r"^(schwab|fidelity|vanguard|robinhood|alpaca|ibkr|"
    r"interactive brokers|etrade|e\*trade|td ameritrade|webull|"
    r"tastytrade|moomoo|coinbase|tradier|public|chase|"
    r"wells fargo|merrill|morgan stanley)"
    r"(?:\s+paper)?\s+account$",
    re.I,
)


def _leaks_account_id(text) -> bool:
    return bool(_LEAK.search(str(text or "")))


def _is_generic_broker_name(text) -> bool:
    return bool(_GENERIC_BROKER.match(" ".join(str(text or "").split())))


def picker_base_label(row) -> str:
    """One account's picker label. Never a mask, uuid, or account number.

    A nickname the user set (different from the broker's ``account_name``)
    wins. A typed name with no account-number leak is kept (manual CSV
    accounts). Generic broker labels and masked names become
    "Unnamed account" so the menu can still be told apart by index.
    """
    nick = " ".join(str((row or {}).get("display_nickname") or "").split())
    name = " ".join(str((row or {}).get("account_name") or "").split())
    chosen = nick if nick and nick != name else ""
    if chosen and not _leaks_account_id(chosen):
        return chosen
    if (
        name
        and not _leaks_account_id(name)
        and not _is_generic_broker_name(name)
    ):
        return name
    if nick and nick == name and not _leaks_account_id(nick):
        return nick
    return "Unnamed account"


def picker_nickname_choices(rows):
    """``[{tenant_id, label}, ...]`` sorted by label.

    Duplicate nicknames get a ``(2)`` suffix from display order, not from
    an account number or broker uuid.
    """
    prepared = []
    for row in rows or []:
        tid = str((row or {}).get("tenant_id") or "").strip()
        if not tid:
            continue
        prepared.append({"tenant_id": tid, "base": picker_base_label(row)})
    prepared.sort(key=lambda item: (item["base"].lower(), item["tenant_id"]))
    counts = Counter(item["base"] for item in prepared)
    seen = Counter()
    out = []
    for item in prepared:
        base = item["base"]
        seen[base] += 1
        label = base
        if counts[base] > 1:
            label = f"{base} ({seen[base]})"
        out.append({"tenant_id": item["tenant_id"], "label": label})
    return out


def nickname_map(rows) -> dict:
    """``tenant_id -> picker label``."""
    return {
        choice["tenant_id"]: choice["label"]
        for choice in picker_nickname_choices(rows)
    }


def _owned_ids(rows):
    out = []
    seen = set()
    for row in rows or []:
        tid = str((row or {}).get("tenant_id") or "").strip()
        if tid and tid not in seen:
            seen.add(tid)
            out.append(tid)
    return out


def _cookie_ids(cookie_value, owned):
    allowed = set(owned or [])
    out = []
    seen = set()
    # scope-filters.js uses encodeURIComponent when writing document.cookie.
    # Cookie parsing does not percent-decode values, so decode before matching
    # broker tenant ids such as ``snaptrade:<uuid>``.
    decoded = unquote(str(cookie_value or ""))
    for part in decoded.split(","):
        tid = part.strip()
        if not tid or tid not in allowed or tid in seen:
            continue
        seen.add(tid)
        out.append(tid)
    return out


def _args_multi(args):
    """Normalize a request arg mapping to ``{key: [values]}``."""
    if args is None:
        return {}
    if hasattr(args, "lists"):
        return {k: list(vs) for k, vs in args.lists()}
    out = {}
    for key, value in dict(args).items():
        if isinstance(value, (list, tuple)):
            out[key] = [str(v) for v in value if v is not None]
        elif value is None:
            out[key] = []
        else:
            out[key] = [str(value)]
    return out


def account_scope_cache_key(args, selected_account, tenant_ids):
    """Stable cache key for data derived from the effective account scope.

    Legacy ``?account=`` views keep their existing label key. Broker-stable
    tenant and group URLs key on the resolved tenant set, so a subset cannot
    overwrite or read the all-accounts cached result.
    """
    values = _args_multi(args)
    explicit = any(
        _first(values, key)
        for key in ("tenant", "tenants", "groups")
    )
    if not explicit:
        return str(selected_account or "").strip()
    ids = sorted({
        str(tid or "").strip()
        for tid in (tenant_ids or [])
        if str(tid or "").strip()
    })
    return "tenants:" + ",".join(ids)


def _first(args, key):
    values = (args or {}).get(key) or []
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def decide_persisted_scope(endpoint, method, args, cookie, owned_ids):
    """Return an action dict, or ``None`` when the request should proceed.

    ``redirect`` adds ``tenants`` from the cookie (URL stays shareable).
    ``clear`` drops the saved filter because the link said ``scope=all``.
    """
    if (method or "GET").upper() != "GET":
        return None
    if endpoint not in SCOPE_ENDPOINTS:
        return None
    args = _args_multi(args)
    if _first(args, "scope") == "all":
        return {"action": "clear"}
    # A shared or in-page URL already names the scope. Do not rewrite it
    # and do not treat it as a new saved choice.
    if _first(args, "tenants") or _first(args, "tenant") or _first(args, "account"):
        return None
    owned = list(owned_ids or [])
    if len(owned) < 2:
        return None
    chosen = _cookie_ids(cookie, owned)
    if not chosen or len(chosen) >= len(owned):
        return None
    return {"action": "redirect", "tenants": chosen}


def scope_redirect_target(path, args, decision):
    """Path plus query for a persistence decision. No leading host."""
    args = _args_multi(args)
    path = path or "/"
    if not decision:
        return path
    drop = {"scope", "tenants", "tenant", "account"}
    pairs = []
    for key, values in args.items():
        if key in drop:
            continue
        for value in values:
            if value is None or value == "":
                continue
            pairs.append((key, value))
    if decision.get("action") == "redirect":
        tenants = ",".join(decision.get("tenants") or [])
        if tenants:
            pairs.append(("tenants", tenants))
    query = urlencode(pairs)
    return f"{path}?{query}" if query else path


def persist_account_scope_response():
    """before_request hook. Redirect, or ``None`` to continue."""
    if not getattr(current_user, "is_authenticated", False):
        return None
    endpoint = request.endpoint
    if endpoint not in SCOPE_ENDPOINTS:
        return None
    from app.models import get_broker_tenants_for_user

    try:
        rows = get_broker_tenants_for_user(current_user.id) or []
    except Exception:
        return None
    decision = decide_persisted_scope(
        endpoint,
        request.method,
        request.args,
        request.cookies.get(TENANT_COOKIE, ""),
        _owned_ids(rows),
    )
    if not decision:
        return None
    from flask import redirect

    target = scope_redirect_target(request.path, request.args, decision)
    resp = redirect(target)
    if decision.get("action") == "clear":
        resp.delete_cookie(TENANT_COOKIE, path="/")
    return resp


def register_account_scope(app):
    app.before_request(persist_account_scope_response)
