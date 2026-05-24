#!/usr/bin/env python3
"""One-shot mechanical replacements for Phase 5 tenant_id cutover in Flask modules."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

FILES = [
    ROOT / "app/routes.py",
    ROOT / "app/weekly_review.py",
    ROOT / "app/strategies.py",
    ROOT / "app/insights.py",
    ROOT / "app/wealth.py",
    ROOT / "app/first_look.py",
    ROOT / "app/profile_community.py",
    ROOT / "app/strategy_fit_insights.py",
]

REPLACEMENTS = [
    ("{account_filter}", "{tenant_filter}"),
    ("{sc_account_filter}", "{sc_tenant_filter}"),
    ("{account_clause}", "{tenant_clause}"),
    ("closed_legs_account_filter", "closed_legs_tenant_filter"),
    ("account_filter=", "tenant_filter="),
    ("sc_account_filter=", "sc_tenant_filter="),
    ("account_clause=", "tenant_clause="),
    ("_account_sql_and(", "_tenant_sql_and("),
    ("_account_sql_filter(", "_tenant_sql_filter("),
    ("_filter_df_by_accounts(", "_filter_df_by_tenant_ids("),
]

# Route-handler variable renames (order matters — longer first)
VAR_REPLACEMENTS = [
    ("acct_filter = _tenant_sql_and(user_accounts)", "tenant_filter = _tenant_sql_and(tenant_ids)"),
    ("account_filter = _tenant_sql_and(", "tenant_filter = _tenant_sql_and("),
    ("acct_filter = _tenant_sql_and(", "tenant_filter = _tenant_sql_and("),
    ("acct_sql = _tenant_sql_and(", "tenant_sql = _tenant_sql_and("),
    ("acct_and = _tenant_sql_and(", "tenant_and = _tenant_sql_and("),
    ("effective_accounts", "tenant_ids"),
    ("pos_accounts_scope", "tenant_scope"),
    ("strat_accounts_scope", "tenant_scope"),
]

# Col-specific tenant SQL — after generic _account_sql_and -> _tenant_sql_and
COL_FIXES = [
    (
        '_tenant_sql_and(tenant_ids, col="account")',
        '_tenant_sql_and(tenant_ids)',
    ),
    (
        '_tenant_sql_and(tenant_scope, col="account")',
        '_tenant_sql_and(tenant_scope)',
    ),
    (
        '_tenant_sql_and(tenant_ids, col="sc.account")',
        '_tenant_sql_and(tenant_ids, col="sc.tenant_id")',
    ),
    (
        '_tenant_sql_and(tenant_scope, col="sc.account")',
        '_tenant_sql_and(tenant_scope, col="sc.tenant_id")',
    ),
    (
        '_tenant_sql_and(tenant_ids, col="h.account")',
        '_tenant_sql_and(tenant_ids, col="h.tenant_id")',
    ),
    (
        '_tenant_sql_and(tenant_scope, col="h.account")',
        '_tenant_sql_and(tenant_scope, col="h.tenant_id")',
    ),
    (
        '_tenant_sql_and([selected_account] if selected_account else user_accounts)',
        '_tenant_sql_and(_tenants_for_scope(selected_account))',
    ),
    (
        '_tenant_sql_and(tenant_ids, col="e.account", user_col="e.user_id")',
        '_tenant_sql_and(tenant_ids, col="e.tenant_id")',
    ),
    (
        '_tenant_sql_and(tenant_ids, col="s.account", user_col="s.user_id")',
        '_tenant_sql_and(tenant_ids, col="s.tenant_id")',
    ),
]


def migrate_file(path: Path) -> None:
    text = path.read_text()
    original = text

    for old, new in REPLACEMENTS:
        text = text.replace(old, new)

    for old, new in VAR_REPLACEMENTS:
        text = text.replace(old, new)

    for old, new in COL_FIXES:
        text = text.replace(old, new)

    # Fix _tenant_sql_and(user_accounts) leftovers
    text = re.sub(
        r"_tenant_sql_and\(user_accounts\)",
        "_tenant_sql_and(tenant_ids)",
        text,
    )
    text = re.sub(
        r"_tenant_sql_and\(\[selected_account\] if selected_account else tenant_ids\)",
        "_tenant_sql_and(_tenants_for_scope(selected_account))",
        text,
    )
    text = re.sub(
        r"_filter_df_by_tenant_ids\(([^,]+), user_accounts\)",
        r"_filter_df_by_tenant_ids(\1, tenant_ids)",
        text,
    )
    text = re.sub(
        r"_filter_df_by_tenant_ids\(([^,]+), tenant_scope\)",
        r"_filter_df_by_tenant_ids(\1, tenant_scope)",
        text,
    )

    if text != original:
        path.write_text(text)
        print(f"updated {path.relative_to(ROOT)}")


def main() -> None:
    for f in FILES:
        if f.exists():
            migrate_file(f)


if __name__ == "__main__":
    main()
