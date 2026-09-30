"""Share cards are privacy-safe pictures of one closed trade."""

import io

from PIL import Image

from app.share_card import (
    DISCLAIMER,
    WORDMARK,
    build_share_card,
    hindsight_line,
    holding_pnl,
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
