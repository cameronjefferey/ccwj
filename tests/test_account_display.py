"""Nickname display: tenant_id wins when several Schwab accounts collide."""

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
