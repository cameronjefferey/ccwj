"""Plain-English glossary tooltips."""

from pathlib import Path

from app.glossary import (
    LEARN_MORE_BASE,
    glossary_entries,
    learn_more_url,
    lookup_term,
    render_term,
    render_term_link,
    render_term_mark,
)

REQUIRED = (
    "strike price",
    "expiration date",
    "premium",
    "contract (100 shares)",
    "expires worthless",
    "buyer/holder",
    "seller/writer",
    "exercise",
    "assignment",
    "long/short",
    "open/close",
    "covered call",
    "called away",
    "in/out/at the money",
    "cost basis",
    "cash-secured put",
    "DTE",
    "rolling",
    "annualized return",
    "spread",
    "max profit/loss",
    "break-even",
    "theta",
    "delta",
    "bid/ask",
    "realized/unrealized",
    "win rate",
    "return on capital",
)

ADVICE = ("you should", "we recommend", "consider ", "try to")


def test_required_labels_resolve():
    missing = [label for label in REQUIRED if lookup_term(label) is None]
    assert missing == []


def test_definitions_are_short_and_not_advice():
    for entry in glossary_entries():
        text = entry["definition"].strip()
        sentences = [s for s in text.replace("!", ".").replace("?", ".").split(".") if s.strip()]
        assert 1 <= len(sentences) <= 2, entry["slug"]
        lowered = text.lower()
        for phrase in ADVICE:
            assert phrase not in lowered, entry["slug"]


def test_learn_more_is_off_until_the_route_exists():
    assert LEARN_MORE_BASE is None
    assert learn_more_url("dte") is None
    html = str(render_term("DTE"))
    assert "ht-term-btn" in html
    assert "ht-term-i" not in html
    assert "aria-label" not in html
    assert 'aria-describedby="ht-term-dte"' in html
    assert ">DTE</button>" in html
    assert "days to expiration" in html.lower()
    assert "Learn more" not in html
    assert 'type="button"' in html


def test_learn_more_hook_points_at_learn_slug(monkeypatch):
    import app.glossary as glossary
    monkeypatch.setattr(glossary, "LEARN_MORE_BASE", "/learn")
    assert glossary.learn_more_url("dte") == "/learn/dte"
    html = str(glossary.render_term("DTE"))
    assert 'href="/learn/dte"' in html
    assert "Learn more" in html


def test_unknown_label_is_plain_text_and_mark_can_stand_beside_a_link():
    assert str(render_term("Symbol")) == "Symbol"
    mark = str(render_term_mark("Win Rate"))
    assert "ht-term-label" not in mark
    assert "ht-term-mark" in mark
    assert "ht-term-i" in mark
    assert 'aria-label="Definition of Win rate"' in mark
    assert "Win rate" in mark or "win rate" in mark.lower()


def test_sort_link_is_the_trigger_and_escapes_the_href():
    html = str(render_term_link("Realized", "/positions?sort=realized&dir=desc"))
    assert 'class="ht-term-btn"' in html
    assert 'href="/positions?sort=realized&amp;dir=desc"' in html
    assert "<button" not in html
    assert "ht-term-i" not in html
    assert "aria-label" not in html
    assert ">Realized</a>" in html
    assert str(render_term_link("Symbol", "/positions?page=1")) == (
        '<a href="/positions?page=1">Symbol</a>'
    )


def test_positions_headers_use_the_word_not_an_icon():
    text = Path("app/templates/positions.html").read_text()
    assert "term_mark(" not in text
    assert "term_link('Realized'" in text
    assert "term_link('Unrealized'" in text
    assert "term_link('Win Rate'" in text
    assert "term_link('Collected'" in text
    assert "{{ term('Unrealized') }}" in text
