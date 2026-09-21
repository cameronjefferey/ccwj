"""Linked-account counts for Settings and Upload.

``user_accounts`` is a legacy label list. Nicknames replace masked broker
names, but the old labels are never removed, so that table drifts (10
names, including phantoms, while 7 live connections remain). Readable
``broker_tenants`` rows are the account a person can open. Distinct
SnapTrade institution names are the broker count — one Schwab login with
five accounts is still one broker.
"""

_NOT_A_BROKER = frozenset({"", "manual", "demo", "snaptrade", "broker"})


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


def linked_account_entries(tenant_rows, labels=None):
    """One display row per readable broker tenant.

    ``removable`` is true only for CSV-only manual tenants. A SnapTrade
    connection is removed from Accounts & data, not by deleting a nickname
    that may not even be in ``user_accounts``.
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
            "removable": manual,
        })
    entries.sort(key=lambda e: (e["label"] or "").lower())
    return entries
