"""Page-header account picker: nicknames, cookie restore, shareable URLs."""

from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from app import app
from app.account_scope import (
    account_scope_cache_key,
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


def test_browser_encoded_cookie_restores_subset():
    """document.cookie keeps encodeURIComponent output percent-encoded."""
    decision = decide_persisted_scope(
        "weekly_review", "GET", {},
        "snaptrade%3Aaaa%2Csnaptrade%3Abbb",
        ["snaptrade:aaa", "snaptrade:bbb", "snaptrade:ccc"],
    )
    assert decision == {
        "action": "redirect",
        "tenants": ["snaptrade:aaa", "snaptrade:bbb"],
    }


def test_scope_cache_key_separates_tenant_subset_from_all_accounts():
    owned = ["snaptrade:aaa", "snaptrade:bbb"]
    assert account_scope_cache_key({}, "", owned) == ""
    assert account_scope_cache_key(
        {"tenants": ["snaptrade:aaa"]}, "", ["snaptrade:aaa"],
    ) == "tenants:snaptrade:aaa"
    assert account_scope_cache_key(
        {"groups": ["7"]}, "", ["snaptrade:bbb", "snaptrade:aaa"],
    ) == "tenants:snaptrade:aaa,snaptrade:bbb"
    assert account_scope_cache_key(
        {"account": ["IRA"]}, "IRA", ["snaptrade:aaa"],
    ) == "IRA"


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


def _render_picker(**kwargs):
    defaults = dict(
        scope_account_choices=[],
        visible_account_choices=None,
        selected_tenant_ids=[],
        account_groups=[],
        selected_group_ids=[],
        tenants_query=None,
        account_rename_urls={},
    )
    defaults.update(kwargs)
    if defaults["visible_account_choices"] is None:
        defaults["visible_account_choices"] = defaults["scope_account_choices"]
    with app.test_request_context("/overview"):
        return app.jinja_env.get_template("_account_scope_filters.html").render(**defaults)


def test_header_picker_template_uses_nicknames():
    choices = [
        {"tenant_id": "snaptrade:aaa", "label": "IRA"},
        {"tenant_id": "snaptrade:bbb", "label": "Unnamed account"},
    ]
    html = _render_picker(scope_account_choices=choices)
    assert "IRA" in html
    assert "Unnamed account" in html
    assert "All accounts" in html
    assert "ht-page-acct" in html
    assert "Schwab Account" not in html
    assert "••••" not in html
    assert "Group accounts" in html


def test_single_account_hides_the_picker():
    html = _render_picker(
        scope_account_choices=[{"tenant_id": "snaptrade:aaa", "label": "IRA"}],
    )
    assert "All accounts" not in html
    assert "ht-page-acct" not in html
    assert "IRA" not in html


def test_header_picker_limits_accounts_to_selected_group_members():
    choices = [
        {"tenant_id": "snaptrade:aaa", "label": "IRA"},
        {"tenant_id": "snaptrade:bbb", "label": "Taxable"},
    ]
    html = _render_picker(
        scope_account_choices=choices,
        visible_account_choices=choices[:1],
        selected_tenant_ids=["snaptrade:aaa"],
        account_groups=[{"id": 7, "name": "Retirement", "tenant_ids": ["snaptrade:aaa"]}],
        selected_group_ids=[7],
        groups_query="7",
    )
    assert "IRA" in html
    assert "Taxable" not in html
    assert "snaptrade:bbb" not in html


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
    html = _render_picker(
        scope_account_choices=choices,
        selected_tenant_ids=["snaptrade:aaa"],
        tenants_query="snaptrade:aaa",
    )
    assert "Account 1" in html
    assert "Account 2" in html
    assert "IRA" not in html
    assert "Unnamed account" not in html


def test_header_picker_orders_masked_labels_numerically(monkeypatch):
    """Nickname order would read Account 2 then Account 1. Privacy sorts 1, 2, 10."""
    import re

    from app.privacy import sort_masked_account_choices

    choices = [
        {"tenant_id": "snaptrade:zzz", "label": "Alpha"},
        {"tenant_id": "snaptrade:mmm", "label": "Zulu"},
        {"tenant_id": "snaptrade:ten", "label": "Middle"},
    ]
    by_tid = {
        "snaptrade:mmm": "Account 1",
        "snaptrade:zzz": "Account 2",
        "snaptrade:ten": "Account 10",
    }
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr("app.privacy.viewer_slots", lambda: (by_tid, {}))
    ordered = sort_masked_account_choices(choices)
    html = _render_picker(
        scope_account_choices=ordered,
        selected_tenant_ids=["snaptrade:zzz", "snaptrade:mmm", "snaptrade:ten"],
        tenants_query="snaptrade:zzz,snaptrade:mmm,snaptrade:ten",
    )
    spans = re.findall(r"<span>(Account \d+)</span>", html)
    assert spans == ["Account 1", "Account 2", "Account 10"]
    assert "Alpha" not in html and "Zulu" not in html


def test_context_processor_sorts_the_header_picker(monkeypatch):
    """The live header list is Account N order, not nickname order."""
    from flask_login import login_user

    from app import _inject_feature_flags

    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 42

        def get_id(self):
            return "42"

    rows = [
        {
            "tenant_id": "snaptrade:zzz",
            "display_nickname": "Alpha",
            "account_name": "Schwab Account",
        },
        {
            "tenant_id": "snaptrade:mmm",
            "display_nickname": "Zulu",
            "account_name": "Schwab Account",
        },
    ]
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr("app.models.get_broker_tenants_for_user", lambda user_id: rows)
    monkeypatch.setattr("app.models.list_account_groups", lambda user_id: [])
    monkeypatch.setattr(
        "app.routes._account_rename_urls_for_rows", lambda rows: {},
    )
    with app.test_request_context("/overview"):
        login_user(_Viewer())
        ctx = _inject_feature_flags()
    assert [c["tenant_id"] for c in ctx["scope_account_choices"]] == [
        "snaptrade:mmm",
        "snaptrade:zzz",
    ]
    assert [c["tenant_id"] for c in ctx["visible_account_choices"]] == [
        "snaptrade:mmm",
        "snaptrade:zzz",
    ]


def test_nav_does_not_host_the_account_picker():
    """The picker is a page-header filter, and only when there are 2+ accounts."""
    from pathlib import Path

    templates = Path(app.root_path) / "templates"
    base = (templates / "base.html").read_text()
    nav = base.split("<nav", 1)[1].split("</nav>", 1)[0]
    assert "ht-header-scope" not in base
    assert "header_account_only" not in base
    assert "ht-page-acct" not in nav
    assert "_account_scope_filters.html" not in nav
    assert "--ht-nav-h" in base

    partial = (templates / "_account_scope_filters.html").read_text()
    assert "scope_account_choices|length > 1" in partial
    assert "ht-page-acct" in partial

    # Every surface that filters by account keeps Apply persistence.
    for name in (
        "weekly_review.html",
        "today.html",
        "positions.html",
        "position_detail.html",
        "accounts.html",
        "wealth.html",
        "strategies.html",
        "strategy_fit.html",
        "sectors.html",
        "trader_story.html",
        "earnings_watch.html",
        "insights.html",
        "day_detail.html",
    ):
        text = (templates / name).read_text()
        assert "_account_scope_filters.html" in text, name
        assert "data-ht-persist-tenants" in text, name
        assert "data-ht-preserve-query" in text, name
        assert "scope_account_choices|length > 1" in text, name


def test_symbol_tabstrip_sticks_below_the_nav():
    from pathlib import Path

    templates = Path(app.root_path) / "templates"
    strip = (templates / "_symbol_tabstrip.html").read_text()
    base = (templates / "base.html").read_text()
    assert "top: var(--ht-nav-h, 3.5rem)" in strip
    assert "z-index: 1020" in strip
    assert "background: #0a0e17" in strip
    # The late base rule outranks the strip's own <style> if both set top.
    assert "body .sym-tabstrip" in base
    assert "top: var(--ht-nav-h, 3.5rem)" in base
    assert 'setProperty("--ht-nav-h"' in base
