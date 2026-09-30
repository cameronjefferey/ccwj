"""Accounts & linking audit: inventory, settled value, filter cache scope."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from flask import render_template

from app.linked_accounts import (
    distinct_broker_names,
    format_account_mask,
    profile_account_rows,
)
from app.routes import accounts_view_range
from app.upload import schwab_csv_allowed, schwab_picker_choices
from app.wealth import _resolve_range, settled_book_value


SNAP = [
    {"snaptrade_account_id": "c401", "broker_slug": "Schwab",
     "account_name": "Schwab ••••9437", "display_nickname": "Cameron 401k"},
    {"snaptrade_account_id": "s401", "broker_slug": "Schwab",
     "account_name": "Schwab ••••7686", "display_nickname": "Sara 401k"},
    {"snaptrade_account_id": "emm", "broker_slug": "Schwab",
     "account_name": "Schwab Account", "display_nickname": "Emmory"},
    {"snaptrade_account_id": "cinv", "broker_slug": "Schwab",
     "account_name": "Schwab ••••3852", "display_nickname": "Cameron Investment"},
    {"snaptrade_account_id": "sinv", "broker_slug": "Schwab",
     "account_name": "Schwab ••••5167", "display_nickname": "Sara Investment"},
    {"snaptrade_account_id": "kee", "broker_slug": "Schwab",
     "account_name": "Schwab ••••5989", "display_nickname": "Keeley"},
    {"snaptrade_account_id": "coin", "broker_slug": "Coinbase",
     "account_name": "Coinbase Account", "display_nickname": "Coinbase Account"},
]

TENANTS = [
    {"tenant_id": "snaptrade:c401", "broker_slug": "snaptrade",
     "account_name": "Schwab ••••9437", "display_nickname": "Cameron 401k"},
    {"tenant_id": "snaptrade:s401", "broker_slug": "snaptrade",
     "account_name": "Schwab ••••7686", "display_nickname": "Sara 401k"},
    {"tenant_id": "snaptrade:emm", "broker_slug": "snaptrade",
     "account_name": "Schwab Account", "display_nickname": "Emmory"},
    {"tenant_id": "snaptrade:cinv", "broker_slug": "snaptrade",
     "account_name": "Schwab ••••3852", "display_nickname": "Cameron Investment"},
    {"tenant_id": "snaptrade:sinv", "broker_slug": "snaptrade",
     "account_name": "Schwab ••••5167", "display_nickname": "Sara Investment"},
    {"tenant_id": "snaptrade:kee", "broker_slug": "snaptrade",
     "account_name": "Schwab ••••5989", "display_nickname": "Keeley"},
    {"tenant_id": "snaptrade:coin", "broker_slug": "snaptrade",
     "account_name": "Coinbase Account", "display_nickname": "Coinbase Account"},
    # Orphans a rename left behind. Not a live connection.
    {"tenant_id": "snaptrade:old-ira", "broker_slug": "snaptrade",
     "account_name": "Sara IRA"},
    {"tenant_id": "snaptrade:old-mask", "broker_slug": "snaptrade",
     "account_name": "Schwab ••••3852"},
    {"tenant_id": "manual:manual:9:Paper", "broker_slug": "manual",
     "account_name": "Paper"},
]


def test_linked_inventory_is_one_row_per_live_account():
    rows = profile_account_rows(SNAP, TENANTS)
    labels = [r["label"] for r in rows]
    assert "Keeley" in labels
    assert "Sara IRA" not in labels
    assert labels.count("Cameron Investment") == 1
    assert "Schwab ••••3852" not in labels
    # 7 live connections + the CSV-only manual. Not the two orphans.
    assert len(rows) == 8
    assert distinct_broker_names(SNAP) == ["Schwab", "Coinbase"]
    by_label = {r["label"]: r for r in rows}
    assert by_label["Paper"]["removable"] is True
    assert by_label["Keeley"]["removable"] is False
    assert by_label["Coinbase Account"]["institution"] == "Coinbase"
    # A stale disambiguated mask must not replace the live nickname.
    masked = profile_account_rows(
        SNAP, TENANTS, {"snaptrade:kee": "Schwab ••••5989"},
    )
    assert "Keeley" in [r["label"] for r in masked]
    assert "Schwab ••••5989" not in [r["label"] for r in masked]


def test_linked_inventory_keeps_canonical_tenant_after_reconnect():
    rows = profile_account_rows(
        [{
            "snaptrade_account_id": "new-transport-uuid",
            "tenant_id": "snaptrade:retained-tenant",
            "broker_slug": "Schwab",
            "account_name": "Schwab ••••9437",
            "display_nickname": "Cameron 401k",
        }],
        [{
            "tenant_id": "snaptrade:retained-tenant",
            "broker_slug": "snaptrade",
            "account_name": "Schwab ••••9437",
            "display_nickname": "Cameron 401k",
        }],
    )

    assert rows == [{
        "tenant_id": "snaptrade:retained-tenant",
        "label": "Cameron 401k",
        "broker_slug": "schwab",
        "institution": "Schwab",
        "removable": False,
    }]


def test_complete_account_link_keeps_canonical_tenant_after_reconnect():
    from app import app

    account = {
        "snaptrade_account_id": "new-transport-uuid",
        "tenant_id": "snaptrade:retained-tenant",
        "broker_slug": "Schwab",
        "account_name": "Schwab Account",
        "display_nickname": "Cameron 401k",
        "account_number_masked": "9437",
        "first_sync_completed": True,
        "holdings_last_successful_sync": None,
        "connection_broken_at": None,
    }
    group = {
        "broker_label": "Schwab",
        "authorization_id": None,
        "needs_reconnect": False,
        "accounts": [account],
    }

    with app.test_request_context("/snaptrade/accounts"):
        html = render_template(
            "snaptrade_accounts.html",
            accounts=[account],
            connection_groups=[group],
            any_reconnect_needed=False,
            snaptrade_enabled=True,
        )

    assert "/upload?tenant=snaptrade:retained-tenant" in html
    assert "/upload?tenant=snaptrade:new-transport-uuid" not in html


def test_account_mask_normalizes_punctuation():
    assert format_account_mask("****.437") == "••••437"
    assert format_account_mask("•••• .437") == "••••437"
    assert format_account_mask(".669") == "••••669"
    assert format_account_mask("afc6") == "••••afc6"
    assert format_account_mask("9437") == "••••9437"
    assert format_account_mask("") == ""
    assert format_account_mask("7") == ""


def test_schwab_picker_excludes_coinbase():
    rows = profile_account_rows(SNAP, TENANTS)
    choices = schwab_picker_choices(rows)
    labels = {c["label"] for c in choices}
    assert "Coinbase Account" not in labels
    assert "Keeley" in labels
    assert "Paper" in labels


def test_schwab_csv_rejects_coinbase_tenant(monkeypatch):
    monkeypatch.setattr(
        "app.models.get_snaptrade_account",
        lambda user_id, aid: {"broker_slug": "Coinbase"} if aid == "coin" else {"broker_slug": "Schwab"},
    )
    assert schwab_csv_allowed(9, "snaptrade:coin") is False
    assert schwab_csv_allowed(9, "snaptrade:kee") is True
    assert schwab_csv_allowed(9, "manual:manual:9:Paper") is True

    def _boom(user_id, aid):
        raise RuntimeError("db down")

    monkeypatch.setattr("app.models.get_snaptrade_account", _boom)
    assert schwab_csv_allowed(9, "snaptrade:coin") is True


def test_settled_book_value_uses_last_close_not_later_row():
    df = pd.DataFrame([
        {"date": date(2026, 9, 18), "tenant_id": "snaptrade:a",
         "account_value": 600_000, "cash_value": -10_000,
         "equity_value": 610_000, "option_value": 0},
        {"date": date(2026, 9, 18), "tenant_id": "snaptrade:b",
         "account_value": 411_864, "cash_value": -27_179,
         "equity_value": 439_043, "option_value": 0},
        {"date": date(2026, 9, 21), "tenant_id": "snaptrade:a",
         "account_value": 700_000, "cash_value": 0,
         "equity_value": 700_000, "option_value": 0},
    ])
    book = settled_book_value(df, date(2026, 9, 18))
    assert book["as_of"] == date(2026, 9, 18)
    assert book["account_value"] == 1_011_864
    assert book["cash"] == -37_179
    assert settled_book_value(df.drop(columns=["account_value"]), date(2026, 9, 18)) is None


def test_filter_does_not_change_one_account_pnl():
    """positions_summary is additive. Slicing the full frame to one tenant
    must equal that tenant's own row — the URL must not be a second math."""
    full = pd.DataFrame([
        {"tenant_id": "snaptrade:kee", "unrealized_pnl": -108.78, "total_return": -107.39},
        {"tenant_id": "snaptrade:sara", "unrealized_pnl": 10.0, "total_return": 12.0},
    ])
    from app.tenant_scope import filter_df_by_tenant_ids
    one = filter_df_by_tenant_ids(full, ["snaptrade:kee"])
    assert float(one["unrealized_pnl"].sum()) == -108.78
    assert float(one["total_return"].sum()) == -107.39


def test_query_scope_is_the_full_book_for_a_filtered_url():
    from app import app
    import app.routes as routes

    user = SimpleNamespace(id=9, username="cam", is_authenticated=True)
    with app.test_request_context("/accounts?tenants=snaptrade:kee"):
        with patch.object(routes, "current_user", user), \
             patch.object(routes, "is_admin", lambda u: False), \
             patch.object(
                 routes, "get_tenant_ids_for_user",
                 lambda uid: ["snaptrade:kee", "snaptrade:sara"],
             ):
            query_ids = routes._tenant_ids_for_queries(["snaptrade:kee"])
    assert query_ids == ["snaptrade:kee", "snaptrade:sara"]


def test_admin_query_scope_stays_on_the_display_set():
    from app import app
    import app.routes as routes

    user = SimpleNamespace(id=1, username="admin", is_authenticated=True)
    with app.test_request_context("/accounts?tenant=snaptrade:kee"):
        with patch.object(routes, "current_user", user), \
             patch.object(routes, "is_admin", lambda u: True):
            assert routes._tenant_ids_for_queries(["snaptrade:kee"]) == ["snaptrade:kee"]
            assert routes._tenant_ids_for_queries(None) is None


def test_range_tokens_survive_the_view_toggle():
    assert accounts_view_range("value", "6M") == "180"
    assert accounts_view_range("value", "1M") == "30"
    assert accounts_view_range("value", "ALL") == "all"
    assert accounts_view_range("value", "YTD") == "ytd"
    assert accounts_view_range("performance", "180") == "6M"
    assert accounts_view_range("performance", "30") == "1M"
    assert accounts_view_range("performance", "all") == "ALL"
    start, end = _resolve_range("6m", default_days=180)
    assert (end - start).days == 180
    ytd_start, ytd_end = _resolve_range("ytd", default_days=180)
    assert ytd_start == date(ytd_end.year, 1, 1)
