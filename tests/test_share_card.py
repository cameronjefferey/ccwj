"""Share cards are privacy-safe pictures of one closed trade.

Numbers on the card come from the viewer's warehouse row. A query string
full of made-up dollars does not paint a card, and a miss is a 404.
"""

import io

import pandas as pd
from PIL import Image

from app.share_card import (
    DISCLAIMER,
    WORDMARK,
    build_share_card,
    card_for_viewer,
    hindsight_line,
    holding_pnl,
    parse_share_ref,
    render_share_png,
)


def test_hindsight_matches_the_sold_versus_hold_example():
    # delta = closing cash − expiry settlement.
    # realized −3333 and hold +11933 ⇒ delta = −15266.
    realized = -3333
    delta = -15266
    assert holding_pnl(realized, delta) == 11933
    assert hindsight_line(realized, delta) == (
        "Sold for -$3,333 · Holding would have made +$11,933"
    )


def test_card_payload_has_no_identity_or_balance_fields():
    card = build_share_card(
        symbol="jepi",
        strategy="Covered Call",
        open_date="2026-01-02",
        close_date="2026-03-20",
        realized_pnl=-3333,
        early_close_delta=-15266,
        direction="long",
    )
    assert card["symbol"] == "JEPI"
    assert card["strategy"] == "Covered Call"
    assert "Jan" in card["dates"] and "Mar" in card["dates"]
    assert card["realized_label"] == "-$3,333"
    assert card["hindsight"] == "Sold for -$3,333 · Holding would have made +$11,933"
    assert card["wordmark"] == WORDMARK
    assert card["disclaimer"] == DISCLAIMER
    blob = " ".join(str(v) for v in card.values()).lower()
    for leaked in ("schwab", "broker", "roth", "@", "account 1", "cash"):
        assert leaked not in blob
    assert set(card) == {
        "symbol", "strategy", "dates", "realized_pnl", "realized_label",
        "hindsight", "wordmark", "disclaimer",
    }


def test_missing_hindsight_omits_the_comparison():
    card = build_share_card(symbol="XLU", strategy="Buy and Hold", realized_pnl=1822.5)
    assert card["hindsight"] is None
    assert card["realized_label"] == "+$1,822"


def test_png_sizes_and_brand_colors():
    card = build_share_card(
        symbol="JEPI",
        strategy="Covered Call",
        open_date="2026-01-02",
        close_date="2026-03-20",
        realized_pnl=-3333,
        early_close_delta=-15266,
    )
    for layout, size in (("square", (1080, 1080)), ("story", (1080, 1920))):
        raw = render_share_png(card, layout)
        image = Image.open(io.BytesIO(raw))
        assert image.size == size
        assert image.getpixel((10, 4)) == (91, 140, 255)  # accent bar #5b8cff
        assert image.getpixel((10, 40)) == (10, 14, 23)  # desk #0a0e17


_OWNED = ["snaptrade:mine"]
_FORGED = {
    "strategy": "Invented Wheel",
    "open": "1999-01-01",
    "close": "1999-12-31",
    "realized": "999999",
    "direction": "long",
}


def _option_row(**overrides):
    row = {
        "symbol": "JEPI",
        "strategy": "Covered Call",
        "trade_symbol": "JEPI  260320C00055000",
        "tenant_id": "snaptrade:mine",
        "open_date": "2026-01-02",
        "close_date": "2026-03-20",
        "realized_pnl": -3333,
        "direction": "Sold",
        "early_close_vs_expiry_delta": -15266,
    }
    row.update(overrides)
    return row


def _frames_for(monkeypatch, frames):
    """``frames`` is a list of DataFrames, consumed in query order."""
    pending = list(frames)

    def _query(sql, params):
        assert "999999" not in sql
        assert "Invented" not in sql
        if not pending:
            return pd.DataFrame()
        return pending.pop(0)

    monkeypatch.setattr("app.share_card._query_df", _query)


def test_parse_share_ref_ignores_dollars_and_strategy():
    spec = parse_share_ref({
        "ref": "option",
        "symbol": "jepi",
        "trade_symbol": "JEPI  260320C00055000",
        "tenant": "snaptrade:mine",
        **_FORGED,
    })
    assert spec["kind"] == "option"
    assert spec["symbol"] == "JEPI"
    assert "realized" not in spec
    assert "strategy" not in spec
    assert parse_share_ref({"symbol": "JEPI", "realized": "999999", "strategy": "Fake"}) is None


def test_option_card_uses_warehouse_numbers_not_the_query_string(monkeypatch):
    _frames_for(monkeypatch, [pd.DataFrame([_option_row()])])
    card = card_for_viewer({
        "ref": "option",
        "symbol": "JEPI",
        "trade_symbol": "JEPI  260320C00055000",
        "tenant": "snaptrade:mine",
        **_FORGED,
    }, _OWNED)
    assert card["symbol"] == "JEPI"
    assert card["strategy"] == "Covered Call"
    assert card["realized_label"] == "-$3,333"
    assert card["realized_pnl"] == -3333
    assert "Jan" in card["dates"] and "Mar" in card["dates"]
    assert "1999" not in card["dates"]
    assert card["hindsight"] == (
        "Closed for -$3,333 · Holding would have made +$11,933"
    )
    assert "Invented" not in card["strategy"]
    assert "999" not in card["realized_label"]


def test_missing_warehouse_row_is_not_a_card(monkeypatch):
    _frames_for(monkeypatch, [pd.DataFrame([_option_row(tenant_id="snaptrade:someone-else")])])
    assert card_for_viewer({
        "ref": "option",
        "symbol": "JEPI",
        "trade_symbol": "JEPI  260320C00055000",
        "tenant": "snaptrade:mine",
        **_FORGED,
    }, _OWNED) is None
    assert card_for_viewer({
        "ref": "option",
        "symbol": "JEPI",
        "trade_symbol": "JEPI  260320C00055000",
        **_FORGED,
    }, []) is None


def test_open_position_is_not_a_card(monkeypatch):
    summary = pd.DataFrame([{
        "symbol": "JEPI",
        "strategy": "Covered Call",
        "status": "Open",
        "realized_pnl": -3333,
        "first_trade_date": "2026-01-02",
        "last_trade_date": "2026-03-20",
        "tenant_id": "snaptrade:mine",
    }])
    _frames_for(monkeypatch, [summary])
    assert card_for_viewer({
        "ref": "position",
        "symbol": "JEPI",
        **_FORGED,
    }, _OWNED) is None


def test_closed_position_card_sums_warehouse_legs(monkeypatch):
    summary = pd.DataFrame([
        {
            "symbol": "JEPI",
            "strategy": "Covered Call",
            "status": "Closed",
            "realized_pnl": 1,
            "first_trade_date": "2026-01-02",
            "last_trade_date": "2026-03-20",
            "tenant_id": "snaptrade:mine",
        },
        {
            "symbol": "JEPI",
            "strategy": "Buy and Hold",
            "status": "Closed",
            "realized_pnl": 2,
            "first_trade_date": "2025-06-01",
            "last_trade_date": "2026-04-01",
            "tenant_id": "snaptrade:mine",
        },
    ])
    options = pd.DataFrame([_option_row()])
    equity = pd.DataFrame([{
        "symbol": "JEPI",
        "tenant_id": "snaptrade:mine",
        "trade_symbol": "JEPI",
        "session_id": 4,
        "open_date": "2025-06-01",
        "close_date": "2026-04-01",
        "realized_pnl": 1822.5,
        "description": "Equity Sold",
    }])
    _frames_for(monkeypatch, [summary, options, equity])
    card = card_for_viewer({
        "ref": "position",
        "symbol": "JEPI",
        "tenants": "snaptrade:mine,snaptrade:not-mine",
        **_FORGED,
    }, _OWNED)
    assert card["strategy"] == "Closed position"
    assert card["realized_pnl"] == -3333 + 1822.5
    assert card["hindsight"] is None
    assert "2025" in card["dates"] or "Jun" in card["dates"]


def test_equity_leg_reads_the_session_row(monkeypatch):
    equity = pd.DataFrame([{
        "symbol": "XLU",
        "tenant_id": "snaptrade:mine",
        "trade_symbol": "XLU",
        "session_id": 9,
        "open_date": "2024-02-01",
        "close_date": "2026-05-06",
        "realized_pnl": 1822.5,
        "description": "Equity Sold",
    }])
    _frames_for(monkeypatch, [equity])
    card = card_for_viewer({
        "ref": "equity",
        "symbol": "XLU",
        "tenant": "snaptrade:mine",
        "session": "9",
        "close": "2026-05-06",
        **_FORGED,
    }, _OWNED)
    assert card["strategy"] == "Equity Sold"
    assert card["realized_label"] == "+$1,822"
    assert card["hindsight"] is None
    assert "1999" not in card["dates"]


def test_share_route_404s_when_the_lookup_misses(monkeypatch):
    from flask_login import login_user
    from werkzeug.exceptions import NotFound

    from app import app
    from app.share_card import share_card_png

    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7

        def get_id(self):
            return "7"

    monkeypatch.setattr("app.share_card._owned_tenant_ids", lambda user_id: _OWNED)
    _frames_for(monkeypatch, [pd.DataFrame()])
    query = (
        "/share/card.png?layout=square&ref=option&symbol=JEPI"
        "&trade_symbol=JEPI%20%20260320C00055000&tenant=snaptrade:mine"
        "&strategy=Invented%20Wheel&realized=999999&open=1999-01-01"
    )
    with app.test_request_context(query):
        login_user(_Viewer())
        try:
            share_card_png()
        except NotFound:
            pass
        else:
            raise AssertionError("expected 404, not a card")


def test_share_route_png_ignores_forged_numbers(monkeypatch):
    from flask_login import login_user

    from app import app
    from app.share_card import share_card_png

    class _Viewer:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7

        def get_id(self):
            return "7"

    monkeypatch.setattr("app.share_card._owned_tenant_ids", lambda user_id: _OWNED)

    def _once(sql, params):
        return pd.DataFrame([_option_row()])

    monkeypatch.setattr("app.share_card._query_df", _once)

    def _fetch(path):
        with app.test_request_context(path):
            login_user(_Viewer())
            resp = share_card_png()
            resp.direct_passthrough = False
            return resp.status_code, resp.mimetype, resp.get_data()

    base = (
        "/share/card.png?layout=square&ref=option&symbol=JEPI"
        "&trade_symbol=JEPI%20%20260320C00055000&tenant=snaptrade:mine"
    )
    forged = base + "&strategy=Invented%20Wheel&realized=999999&direction=long"
    status_a, mime_a, png_a = _fetch(base)
    status_b, mime_b, png_b = _fetch(forged)
    assert status_a == 200 and status_b == 200
    assert mime_a == "image/png" and mime_b == "image/png"
    assert png_a == png_b
    assert len(png_a) > 1000


def test_anonymous_share_url_is_not_an_image():
    from app import app

    client = app.test_client()
    resp = client.get(
        "/share/card.png?symbol=JEPI&strategy=Fake&realized=999999&ref=position"
    )
    assert resp.status_code in (302, 401)
    assert resp.mimetype != "image/png"
