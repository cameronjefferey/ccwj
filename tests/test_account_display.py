"""Nickname display: tenant_id wins when several Schwab accounts collide."""

import pytest

from app.routes import (
    _disambiguated_tenant_labels,
    _resolve_account_display,
    _unique_account_name_labels,
)


def _rows():
    return [
        {
            "tenant_id": "snaptrade:aaa",
            "account_name": "Schwab Account",
            "display_nickname": "Emmory Investment",
        },
        {
            "tenant_id": "snaptrade:bbb",
            "account_name": "Schwab Account",
            "display_nickname": "Sara IRA",
        },
        {
            "tenant_id": "snaptrade:ccc",
            "account_name": "Alpaca Paper Account",
            "display_nickname": "Testing",
        },
    ]


def test_unique_account_name_labels_drops_colliding_schwab():
    rows = _rows()
    tmap = _disambiguated_tenant_labels(rows)
    umap = _unique_account_name_labels(rows, tmap)
    assert "Schwab Account" not in umap
    assert umap["Alpaca Paper Account"] == "Testing"


def test_unique_account_name_labels_maps_a_single_schwab():
    rows = [_rows()[0]]
    tmap = _disambiguated_tenant_labels(rows)
    umap = _unique_account_name_labels(rows, tmap)
    assert umap == {"Schwab Account": "Emmory Investment"}


def test_resolve_prefers_tenant_id_on_colliding_broker_name():
    rows = _rows()
    tmap = _disambiguated_tenant_labels(rows)
    umap = _unique_account_name_labels(rows, tmap)
    assert _resolve_account_display(
        "Schwab Account", "snaptrade:aaa",
        tenant_labels=tmap, unique_name_labels=umap,
    ) == "Emmory Investment"
    assert _resolve_account_display(
        "Schwab Account", "snaptrade:bbb",
        tenant_labels=tmap, unique_name_labels=umap,
    ) == "Sara IRA"


def test_resolve_without_tenant_id_keeps_colliding_broker_name():
    """Name-only lookup must not pick an arbitrary nickname."""
    rows = _rows()
    tmap = _disambiguated_tenant_labels(rows)
    umap = _unique_account_name_labels(rows, tmap)
    assert _resolve_account_display(
        "Schwab Account", None,
        tenant_labels=tmap, unique_name_labels=umap,
    ) == "Schwab Account"


def test_resolve_unique_name_fallback_without_tenant_id():
    rows = [_rows()[0]]
    tmap = _disambiguated_tenant_labels(rows)
    umap = _unique_account_name_labels(rows, tmap)
    assert _resolve_account_display(
        "Schwab Account", None,
        tenant_labels=tmap, unique_name_labels=umap,
    ) == "Emmory Investment"


def test_resolve_already_a_nickname_passes_through():
    rows = _rows()
    tmap = _disambiguated_tenant_labels(rows)
    umap = _unique_account_name_labels(rows, tmap)
    assert _resolve_account_display(
        "Emmory Investment", None,
        tenant_labels=tmap, unique_name_labels=umap,
    ) == "Emmory Investment"


def test_rename_urls_only_for_generic_snaptrade_names():
    from app import app
    from app.routes import _account_rename_urls_for_rows

    rows = [
        {
            "tenant_id": "snaptrade:aaa", "broker_uuid": "aaa",
            "account_name": "Schwab Account", "display_nickname": "Emmory",
        },
        {
            "tenant_id": "snaptrade:bbb", "broker_uuid": "bbb",
            "account_name": "Schwab Account", "display_nickname": None,
        },
        {
            "tenant_id": "snaptrade:ccc", "broker_uuid": "ccc",
            "account_name": "Robinhood ••••1876",
            "display_nickname": "Robinhood ••••1876",
        },
        {
            "tenant_id": "manual:manual:9:IRA", "broker_uuid": "",
            "account_name": "IRA", "display_nickname": None,
        },
        {
            "tenant_id": "demo:demo-account", "broker_uuid": "",
            "account_name": "Demo Account", "display_nickname": None,
        },
        {
            "tenant_id": "snaptrade:no-uuid-col", "broker_uuid": "",
            "account_name": "Schwab Account", "display_nickname": "",
        },
    ]
    with app.test_request_context():
        out = _account_rename_urls_for_rows(rows)
    assert "snaptrade:aaa" not in out
    assert "manual:manual:9:IRA" not in out
    assert "demo:demo-account" not in out
    assert out["snaptrade:bbb"].endswith("#acct-bbb")
    assert "/snaptrade/accounts" in out["snaptrade:bbb"]
    assert out["snaptrade:ccc"].endswith("#acct-ccc")
    assert out["snaptrade:no-uuid-col"].endswith("#acct-no-uuid-col")


def test_snaptrade_accounts_needing_nickname_filters_generic_only():
    """Post-connect naming step: only rows with no nickname (or a
    nickname identical to the broker label) should show up."""
    from app.routes import _snaptrade_accounts_needing_nickname

    rows = [
        {"snaptrade_account_id": "a1", "account_name": "Schwab Account",
         "display_nickname": None},
        {"snaptrade_account_id": "a2", "account_name": "Schwab Account",
         "display_nickname": "Schwab Account"},
        {"snaptrade_account_id": "a3", "account_name": "Schwab Account",
         "display_nickname": "Retirement"},
        {"snaptrade_account_id": "a4", "account_name": "Robinhood ••••1876",
         "display_nickname": ""},
    ]
    out = _snaptrade_accounts_needing_nickname(rows)
    ids = {r["snaptrade_account_id"] for r in out}
    assert ids == {"a1", "a2", "a4"}


@pytest.mark.parametrize("slug,expected", [
    ("Charles Schwab", True), ("Schwab", True), ("SCHWAB", True),
    ("Robinhood", False), (None, False), ("", False),
])
def test_complete_account_schwab_detection_matches_institution_name(slug, expected):
    """snaptrade_accounts.broker_slug is the raw SnapTrade institution name
    (``Charles Schwab``), not the dbt slug (``schwab``) — the "Complete this
    account" CSV-upload branch on snaptrade_accounts.html must match on a
    case-insensitive substring, not an exact lowercase equality."""
    from app import app

    with app.app_context():
        tmpl = app.jinja_env.from_string(
            "{% set _is_schwab = c.broker_slug and 'schwab' in c.broker_slug|lower %}"
            "{{ 'YES' if _is_schwab else 'NO' }}"
        )
        rendered = tmpl.render(c={"broker_slug": slug})
    assert rendered == ("YES" if expected else "NO")


def test_snaptrade_accounts_needing_nickname_empty_when_all_named():
    from app.routes import _snaptrade_accounts_needing_nickname

    rows = [
        {"snaptrade_account_id": "a1", "account_name": "Schwab Account",
         "display_nickname": "Retirement"},
        {"snaptrade_account_id": "a2", "account_name": "Alpaca Paper Account",
         "display_nickname": "Testing"},
    ]
    assert _snaptrade_accounts_needing_nickname(rows) == []
