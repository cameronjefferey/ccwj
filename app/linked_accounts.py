"""Linked-account counts for Settings and Upload.

``user_accounts`` is a legacy label list. Nicknames replace masked broker
names, but the old labels are never removed, so that table drifts (10
names, including phantoms, while 7 live connections remain).

The connection a person can open is one ``snaptrade_accounts`` row
(``snaptrade:<snaptrade_account_id>``), plus a CSV-only manual tenant.
Leftover ``broker_tenants`` rows that no longer have a SnapTrade account
are not a second copy of the same book. Distinct SnapTrade institution
names are the broker count — one Schwab login with six accounts is still
one broker.
"""

_NOT_A_BROKER = frozenset({"", "manual", "demo", "snaptrade", "broker"})

# Institutions the Schwab CSV form may attach to. Anything else (Coinbase,
# Fidelity, …) has no parser and must not land in that dropdown.
SCHWAB_CSV_INSTITUTIONS = frozenset({
    "", "schwab", "charles schwab", "snaptrade", "broker", "manual",
})


def distinct_broker_names(snaptrade_accounts):
    """Institution names in first-seen order, case-insensitive.

    ``snaptrade_accounts.broker_slug`` is the brokerage (Schwab, Coinbase).
    ``broker_tenants.broker_slug`` is the aggregator (``snaptrade``) and
    must not be counted here.
    """
    seen = []
    keys = set()
    for row in snaptrade_accounts or []:
        raw = str((row or {}).get("broker_slug") or "").strip()
        key = raw.lower()
        if key in _NOT_A_BROKER or key in keys:
            continue
        keys.add(key)
        seen.append(raw)
    return seen


def snaptrade_tenant_id(row):
    """``snaptrade:<uuid>`` for one SnapTrade account row, or ``""``."""
    aid = str((row or {}).get("snaptrade_account_id") or "").strip()
    return f"snaptrade:{aid}" if aid else ""


def linked_account_entries(tenant_rows, labels=None):
    """One display row per readable broker tenant.

    Prefer :func:`profile_account_rows` for Settings and Upload. This
    helper remains for callers that only have tenant rows (tests, and
    the fallback when the SnapTrade list cannot be read).
    """
    labels = labels or {}
    entries = []
    for row in tenant_rows or []:
        tid = str((row or {}).get("tenant_id") or "").strip()
        if not tid:
            continue
        slug = str(row.get("broker_slug") or "").strip().lower()
        label = (
            labels.get(tid)
            or (row.get("display_nickname") or "")
            or (row.get("account_name") or "")
            or tid
        )
        label = str(label).strip() or tid
        manual = slug == "manual" or tid.startswith("manual:")
        entries.append({
            "tenant_id": tid,
            "label": label,
            "broker_slug": slug,
            "institution": "",
            "removable": manual,
        })
    entries.sort(key=lambda e: (e["label"] or "").lower())
    return entries


def profile_account_rows(snaptrade_accounts, tenant_rows, labels=None):
    """One row per live SnapTrade account, plus CSV-only manual tenants.

    A nickname and the old ``Schwab ••••XXXX`` label are the same
    physical account. Orphan tenant rows (a renamed label that no longer
    has a ``snaptrade_accounts`` row, or a phantom like ``Sara IRA``)
    are omitted. Keeley shows up because she is a SnapTrade row, even
    when ``user_accounts`` never stored her nickname.
    """
    labels = labels or {}
    tenants_by_id = {}
    for row in tenant_rows or []:
        tid = str((row or {}).get("tenant_id") or "").strip()
        if tid:
            tenants_by_id[tid] = row

    entries = []
    seen = set()
    for acc in snaptrade_accounts or []:
        tid = snaptrade_tenant_id(acc)
        if not tid or tid in seen:
            continue
        seen.add(tid)
        tenant = tenants_by_id.get(tid) or {}
        # Nickname wins over the disambiguated warehouse label. A rename
        # leaves ``account_name`` as ``Schwab ••••XXXX``; the nickname is
        # the row the trader recognizes. The label map is the fallback
        # when neither side stored a nickname.
        label = (
            (acc.get("display_nickname") or "")
            or (tenant.get("display_nickname") or "")
            or labels.get(tid)
            or (acc.get("account_name") or "")
            or (tenant.get("account_name") or "")
            or tid
        )
        institution = str(acc.get("broker_slug") or "").strip()
        entries.append({
            "tenant_id": tid,
            "label": str(label).strip() or tid,
            "broker_slug": institution.lower(),
            "institution": institution,
            "removable": False,
        })

    for row in tenant_rows or []:
        tid = str((row or {}).get("tenant_id") or "").strip()
        if not tid or tid in seen:
            continue
        slug = str(row.get("broker_slug") or "").strip().lower()
        manual = slug == "manual" or tid.startswith("manual:")
        if not manual:
            continue
        seen.add(tid)
        label = (
            labels.get(tid)
            or (row.get("display_nickname") or "")
            or (row.get("account_name") or "")
            or tid
        )
        entries.append({
            "tenant_id": tid,
            "label": str(label).strip() or tid,
            "broker_slug": slug,
            "institution": "",
            "removable": True,
        })
    entries.sort(key=lambda e: (e["label"] or "").lower())
    return entries


def format_account_mask(raw):
    """Last four alphanumerics as ``••••`` + tail.

    ``****.437``, ``•••• .437``, and ``.437`` all render ``••••437``.
    ``afc6`` renders ``••••afc6``. Empty or a single character renders
    ``""`` so a missing mask does not become a row of dots.
    """
    alnum = "".join(ch for ch in str(raw or "") if ch.isalnum())
    if len(alnum) < 2:
        return ""
    return "••••" + alnum[-4:]


def csv_broker_slug(institution):
    """Map a SnapTrade institution name onto a ``CSV_EXPORT_BROKERS`` slug."""
    key = str(institution or "").strip().lower()
    aliases = {
        "charles schwab": "schwab",
        "schwab": "schwab",
        "interactive brokers": "interactive",
        "ibkr": "interactive",
    }
    return aliases.get(key, key)


def institution_accepts_schwab_csv(institution):
    """True when a Schwab CSV may be merged into this institution."""
    return str(institution or "").strip().lower() in SCHWAB_CSV_INSTITUTIONS
