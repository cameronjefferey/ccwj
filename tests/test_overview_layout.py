"""Overview and Today read as one page: headline, what happened, then the rest."""

from pathlib import Path

ROOT = Path("app/templates")
PAGE = (ROOT / "weekly_review.html").read_text()
BELOW = (ROOT / "_overview_below.html").read_text()
DAILY = (ROOT / "_overview_daily.html").read_text()
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
    assert BELOW.index('id="ov-execution"') < BELOW.index('{% include "_overview_daily.html" %}')
    assert BELOW.index('{% include "_overview_daily.html" %}') < BELOW.index('id="ov-performance"')
    assert BELOW.index('id="ov-performance"') < BELOW.index('id="ov-week"')
    assert 'class="review-section"' not in BELOW
    assert '<details class="disclosure" id="ov-daily">' not in BELOW
    assert '<details class="disclosure" id="ov-week">' in BELOW
    assert "overview_daily_in_fragment" in BELOW


def test_daily_change_is_open_after_accounts():
    accounts = PAGE.index('id="ov-accounts"')
    slot = PAGE.index('id="ht-overview-daily"')
    inline = PAGE.index('{% include "_overview_daily.html" %}')
    building = PAGE.index('id="ov-building"')
    assert accounts < slot < building
    assert accounts < inline < building
    assert '<div class="review-section" id="ov-daily">' in DAILY
    assert '<h2 class="section-header">Daily change</h2>' in DAILY
    assert "<details" not in DAILY
    assert "placeOverviewDaily" in PAGE
    assert "Chart.getChart" in PAGE


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


def test_benchmark_rows_match_account_stack_on_a_phone():
    phone = STYLES.split("@media (max-width: 720px)", 1)[1].split("@media", 1)[0]
    assert '[data-bs-theme="dark"] .snapshot-table .snapshot-table-row.benchmark > div' in phone
    assert "background: transparent" in phone
    assert "text-align: left" in phone
    # (0,3,1) so it beats the desktop (0,2,1) right-align on every row.
    rule = phone.split(
        ".snapshot-table .snapshot-table-row > div:not(:first-child)", 1
    )[1].split("}", 1)[0]
    assert "text-align: left" in rule
    desktop = STYLES.split("@media (max-width: 720px)", 1)[0]
    assert ".snapshot-table-row > div:not(:first-child)" in desktop
    assert "text-align: right" in desktop
    assert ".snapshot-table-row.benchmark > div" in desktop
    assert ".snapshot-table .snapshot-table-row > div:not(:first-child)" not in desktop


def test_headline_figures_stay_one_line_and_shrink_on_a_narrow_screen():
    big = STYLES.split(".ov-big {", 1)[1].split("}", 1)[0]
    assert "clamp(" in big
    figures = STYLES.split(".ov-big, .ov-pct {", 1)[1].split("}", 1)[0]
    assert "white-space: nowrap" in figures
    assert ".ov-sub" not in figures
    sub = STYLES.split(".ov-sub {", 1)[1].split("}", 1)[0]
    assert "white-space: normal" in sub
    assert "overflow-wrap: break-word" in sub
    assert "word-break: normal" in sub
    assert "white-space: nowrap" not in sub
    assert "overflow-wrap: normal" in STYLES
    assert "word-break: normal" in STYLES
    assert ".ov-big { font-size: 1.45rem; overflow-wrap: anywhere" not in STYLES
    assert ".ov-big { font-size: 2.15rem; }" not in STYLES


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
