"""Header account picker: nicknames, cookie restore, shareable URLs."""

from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from app import app
from app.account_scope import (
    decide_persisted_scope,
    nickname_map,
    persist_account_scope_response,
    picker_nickname_choices,
    scope_redirect_target,
)


OWNED = [
    {
        "tenant_id": "snaptrade:aaa",
        "display_nickname": "IRA",
        "account_name": "Schwab Account",
    },
    {
        "tenant_id": "snaptrade:bbb",
        "display_nickname": "Schwab ••••6342",
        "account_name": "Schwab Account",
    },
    {
        "tenant_id": "snaptrade:ccc",
        "display_nickname": "",
        "account_name": "Old IRA",
    },
]


def test_picker_shows_nicknames_and_hides_masks():
    labels = [c["label"] for c in picker_nickname_choices(OWNED)]
    assert "IRA" in labels
    assert "Old IRA" in labels
    assert "Unnamed account" in labels
    blob = " ".join(labels)
    assert "Schwab Account" not in blob
    assert "••••" not in blob
    assert "6342" not in blob
    assert "snaptrade:" not in blob


def test_duplicate_nicknames_are_numbered_not_masked():
    rows = [
        {"tenant_id": "snaptrade:111", "display_nickname": "Kids", "account_name": "Schwab Account"},
        {"tenant_id": "snaptrade:222", "display_nickname": "Kids", "account_name": "Schwab Account"},
    ]
    labels = [c["label"] for c in picker_nickname_choices(rows)]
    assert labels == ["Kids (1)", "Kids (2)"]
    assert nickname_map(rows)["snaptrade:222"] == "Kids (2)"


def test_cookie_subset_redirects_and_keeps_other_query():
    decision = decide_persisted_scope(
        "weekly_review", "GET", {"range": ["1m"]},
        "snaptrade:aaa",
        ["snaptrade:aaa", "snaptrade:bbb"],
    )
    assert decision["action"] == "redirect"
    target = scope_redirect_target("/overview", {"range": ["1m"]}, decision)
    q = parse_qs(urlparse(target).query)
    assert q["tenants"] == ["snaptrade:aaa"]
    assert q["range"] == ["1m"]


def test_explicit_url_wins_and_all_accounts_cookie_is_a_noop():
    owned = ["snaptrade:aaa", "snaptrade:bbb"]
    assert decide_persisted_scope(
        "positions", "GET", {"tenants": ["snaptrade:bbb"]},
        "snaptrade:aaa", owned,
    ) is None
    assert decide_persisted_scope(
        "positions", "GET", {},
        "snaptrade:aaa,snaptrade:bbb", owned,
    ) is None


def test_scope_all_clears_and_drops_unowned_ids():
    assert decide_persisted_scope(
        "trader_story", "GET", {"scope": ["all"], "tenants": ["snaptrade:aaa"]},
        "snaptrade:aaa", ["snaptrade:aaa", "snaptrade:bbb"],
    ) == {"action": "clear"}
    decision = decide_persisted_scope(
        "sectors", "GET", {},
        "snaptrade:hack,snaptrade:aaa",
        ["snaptrade:aaa", "snaptrade:bbb"],
    )
    assert decision["tenants"] == ["snaptrade:aaa"]
    cleared = scope_redirect_target(
        "/story",
        {"scope": ["all"], "strategy": ["Wheel"]},
        {"action": "clear"},
    )
    q = parse_qs(urlparse(cleared).query)
    assert "scope" not in q
    assert "tenants" not in q
    assert q["strategy"] == ["Wheel"]


def test_single_account_and_other_pages_do_not_redirect():
    assert decide_persisted_scope(
        "weekly_review", "GET", {}, "snaptrade:aaa", ["snaptrade:aaa"],
    ) is None
    assert decide_persisted_scope(
        "profile", "GET", {}, "snaptrade:aaa",
        ["snaptrade:aaa", "snaptrade:bbb"],
    ) is None
    assert decide_persisted_scope(
        "weekly_review", "POST", {}, "snaptrade:aaa",
        ["snaptrade:aaa", "snaptrade:bbb"],
    ) is None


def test_persist_hook_redirects_and_clear_deletes_cookie():
    user = SimpleNamespace(id=9, is_authenticated=True)
    rows = [
        {"tenant_id": "snaptrade:aaa", "display_nickname": "IRA", "account_name": "Schwab Account"},
        {"tenant_id": "snaptrade:bbb", "display_nickname": "Taxable", "account_name": "Schwab Account"},
    ]
    with app.test_request_context(
        "/overview?range=1m",
        headers={"Cookie": "ht_tenants=snaptrade:aaa"},
    ):
        from flask import request
        request.url_rule = SimpleNamespace(endpoint="weekly_review")
        with patch("app.account_scope.current_user", user), \
             patch("app.models.get_broker_tenants_for_user", return_value=rows):
            resp = persist_account_scope_response()
    assert resp.status_code == 302
    q = parse_qs(urlparse(resp.location).query)
    assert q["tenants"] == ["snaptrade:aaa"]
    assert q["range"] == ["1m"]

    with app.test_request_context("/positions?scope=all&symbol=JEPI"):
        from flask import request
        request.url_rule = SimpleNamespace(endpoint="positions")
        with patch("app.account_scope.current_user", user), \
             patch("app.models.get_broker_tenants_for_user", return_value=rows):
            resp = persist_account_scope_response()
    assert resp.status_code == 302
    q = parse_qs(urlparse(resp.location).query)
    assert "scope" not in q
    assert q["symbol"] == ["JEPI"]
    assert "ht_tenants=" in resp.headers.get("Set-Cookie", "")


def test_header_picker_template_uses_nicknames():
    choices = [
        {"tenant_id": "snaptrade:aaa", "label": "IRA"},
        {"tenant_id": "snaptrade:bbb", "label": "Unnamed account"},
    ]
    with app.test_request_context("/overview"):
        html = app.jinja_env.get_template("_account_scope_filters.html").render(
            header_account_only=True,
            scope_account_choices=choices,
            selected_tenant_ids=[],
            account_groups=[],
            selected_group_ids=[],
            tenants_query=None,
            account_rename_urls={},
        )
    assert "IRA" in html
    assert "Unnamed account" in html
    assert "All accounts" in html
    assert "Schwab Account" not in html
    assert "••••" not in html

    with app.test_request_context("/positions"):
        page = app.jinja_env.get_template("_account_scope_filters.html").render(
            header_account_only=False,
            scope_account_choices=choices,
            visible_account_choices=choices,
            selected_tenant_ids=[],
            account_groups=[],
            selected_group_ids=[],
            tenants_query=None,
            account_rename_urls={},
        )
    assert "All accounts" not in page
    assert "Group accounts" in page


def test_header_picker_masks_labels_in_privacy_mode(monkeypatch):
    """Dropdown rows and the selected-account button use Account N."""
    choices = [
        {"tenant_id": "snaptrade:aaa", "label": "IRA"},
        {"tenant_id": "snaptrade:bbb", "label": "Unnamed account"},
    ]
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr(
        "app.privacy.viewer_slots",
        lambda: (
            {"snaptrade:aaa": "Account 1", "snaptrade:bbb": "Account 2"},
            {"IRA": "Account 1", "Unnamed account": "Account 2"},
        ),
    )
    with app.test_request_context("/overview?tenants=snaptrade:aaa"):
        html = app.jinja_env.get_template("_account_scope_filters.html").render(
            header_account_only=True,
            scope_account_choices=choices,
            selected_tenant_ids=["snaptrade:aaa"],
            account_groups=[],
            selected_group_ids=[],
            tenants_query="snaptrade:aaa",
            account_rename_urls={},
        )
    assert "Account 1" in html
    assert "Account 2" in html
    assert "IRA" not in html
    assert "Unnamed account" not in html


def test_base_hides_picker_until_two_accounts():
    from pathlib import Path
    base = (Path(app.root_path) / "templates" / "base.html").read_text()
    assert "ht-header-scope" in base
    assert "scope_account_choices|length > 1" in base
    assert "data-ht-persist-tenants" in base
