"""Overview and Today read as one page: headline, what happened, then the rest."""

from pathlib import Path

ROOT = Path("app/templates")
PAGE = (ROOT / "weekly_review.html").read_text()
BELOW = (ROOT / "_overview_below.html").read_text()
TODAY = (ROOT / "today.html").read_text()
STYLES = (ROOT / "_review_styles.html").read_text()
SINCE = (ROOT / "_since_last_looked.html").read_text()
AFTER = (ROOT / "_after_hours_movers.html").read_text()


def test_overview_story_order_is_headline_then_what_happened_then_accounts():
    happened = PAGE.index('id="ov-happened"')
    accounts = PAGE.index('id="ov-accounts"')
    building = PAGE.index('id="ov-building"')
    below = PAGE.index('{% include "_overview_below.html" %}')
    assert happened < accounts < building < below
    assert '<details class="disclosure" id="ov-building">' in PAGE
    assert 'id="ht-overview-below" class="ov-below-slot"' in PAGE
    assert 'id="ht-overview-below" class="review-section"' not in PAGE
    assert "el.remove()" in PAGE
    assert "border:1px solid #5b8cff" not in PAGE


def test_secondary_overview_sections_are_collapsed_in_order():
    assert BELOW.index('id="ov-radar"') < BELOW.index('id="ov-execution"')
    assert BELOW.index('id="ov-execution"') < BELOW.index('id="ov-daily"')
    assert BELOW.index('id="ov-daily"') < BELOW.index('id="ov-performance"')
    assert BELOW.index('id="ov-performance"') < BELOW.index('id="ov-week"')
    assert 'class="review-section"' not in BELOW
    assert '<details class="disclosure" id="ov-week">' in BELOW


def test_realized_pill_and_collected_premium_stay_literal():
    assert "realized" in PAGE
    assert "net premium" not in PAGE.lower()
    assert "net premium" not in TODAY.lower()
    assert "collected $" in TODAY
    assert "collected $" in BELOW


def test_snapshot_columns_stack_on_a_phone_instead_of_hiding():
    assert 'data-label="Share of book"' in PAGE
    assert 'data-label="1 month"' in PAGE
    assert "snap-col-share { display: none" not in STYLES
    assert "content: attr(data-label)" in STYLES
    assert "box-shadow: none" in STYLES
    section = STYLES.split(".review-section {", 1)[1].split("}", 1)[0]
    assert "box-shadow: none" in section


def test_today_story_order_keeps_since_last_looked_off_overview():
    happened = TODAY.index('id="ov-happened"')
    since = TODAY.index("{% include '_since_last_looked.html' %}")
    opened = TODAY.index('id="ov-open"')
    assert happened < since < opened
    assert TODAY.count('<div class="ov-page">') == 1
    assert TODAY.strip().endswith("{% endblock %}")
    assert "</div>" in TODAY.split('id="ov-open"', 1)[1]
    assert "Since you last looked" in SINCE
    assert "Since you last looked" not in PAGE
    assert '<details class="disclosure since-card">' in SINCE
    assert 'class="ov-block"' in AFTER
    assert 'class="review-section"' not in AFTER
