"""Overview and Today read as one page: headline, performance, what happened, then the rest."""

from pathlib import Path

ROOT = Path("app/templates")
PAGE = (ROOT / "weekly_review.html").read_text()
BELOW = (ROOT / "_overview_below.html").read_text()
DAILY = (ROOT / "_overview_daily.html").read_text()
PERF = (ROOT / "_overview_performance.html").read_text()
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


def test_overview_grades_the_whole_book_not_a_seven_day_window():
    src = Path("app/weekly_review.py").read_text()
    assert "overview_graded_verdicts" in src
    assert "market_today - timedelta(days=6)" not in src


def test_execution_card_lists_every_verdict_and_hides_when_empty():
    start = BELOW.index("EXECUTION REVIEW")
    gate = BELOW[start:BELOW.index('id="ov-execution"')]
    assert "exit_verdicts_landed or exit_verdicts_pending" in gate
    assert "open_option_record" not in gate
    body = BELOW[BELOW.index('id="ov-execution"'):BELOW.index("simple-view-full execution")]
    assert "exit_verdicts_landed[0]" not in body
    assert "for v in exit_verdicts_landed" in body
    assert "exit_verdicts_pending['items']" in body
    assert "exit_verdicts_total.label" in body
    assert "v.closed_label" in body and "v.worth_label" in body
    assert "v.outcome_label" in body
    assert "Next:" not in body


def test_simple_view_tucks_execution_and_hides_performance():
    exec_at = BELOW.index('id="ov-execution"')
    week_at = BELOW.index('id="ov-week"')
    assert BELOW.rfind("{% if not simple_view %}", 0, exec_at) != -1
    assert "simple-view-full execution" in BELOW[exec_at:week_at]
    assert 'id="ov-performance"' not in BELOW
    assert PERF.index("{% if not simple_view %}") < PERF.index('id="ov-performance"')
    assert "simple-view-full performance" in PERF
    slot = PAGE.index('id="ht-overview-performance"')
    assert PAGE.rfind("{% if not simple_view %}", 0, slot) > PAGE.index('class="review-hero"')
    assert slot < PAGE.index('id="ov-show-more"')
    assert PAGE.index("{% if not simple_view %}") < PAGE.index("See your trader profile")


def test_performance_by_account_opens_under_the_hero():
    """First section under the hero, expanded unless a stored collapse says closed."""
    hero = PAGE.index('class="review-hero"')
    slot = PAGE.index('id="ht-overview-performance"')
    inline = PAGE.index('{% include "_overview_performance.html" %}')
    happened = PAGE.index('id="ov-happened"')
    accounts = PAGE.index('id="ov-accounts"')
    assert hero < slot < happened < accounts
    assert hero < inline < happened
    assert '<details class="disclosure" id="ov-performance" open>' in PERF
    assert "Performance by account" in PERF
    assert "How this is computed" in PERF
    assert "ht-ov-performance" in PAGE
    assert 'stored === "0"' in PAGE
    assert "placeOverviewPerformance" in PAGE
    assert "overview_performance_in_fragment" in BELOW
    assert BELOW.index("overview_performance_in_fragment") < BELOW.index('id="ov-week"')
    assert 'id="ov-performance"' not in BELOW
    # Phone: title and chevron share a row; the note wraps underneath.
    # Dividend / G/L % / Annualized stay hideable.
    assert "grid-template-columns: minmax(0, 1fr) auto" in STYLES
    assert '#ov-performance .breakdown-wrap { min-width: 0; max-width: 100%; }' in STYLES
    assert 'data-m="hide">Dividend' in PERF
    assert 'data-m="hide">G/L %' in PERF
    # Subtitle still names week_start. The window math stays in Python.
    # The label includes the month, and the year when that Monday is not
    # in the current year.
    assert "week_start|session_date('dow', today)" in PERF
    assert "week_start.strftime('%a %-d')" not in PERF
    source = Path("app/weekly_review.py").read_text()
    assert '"overview_performance_in_fragment": False' in source
    assert '"overview_performance_in_fragment": True' in source


def test_secondary_overview_sections_are_collapsed_in_order():
    assert BELOW.index('id="ov-radar"') < BELOW.index('id="ov-execution"')
    assert BELOW.index('id="ov-execution"') < BELOW.index('{% include "_overview_daily.html" %}')
    assert BELOW.index('{% include "_overview_daily.html" %}') < BELOW.index('id="ov-week"')
    assert 'class="review-section"' not in BELOW
    assert '<details class="disclosure" id="ov-daily">' not in BELOW
    assert '<details class="disclosure" id="ov-week">' in BELOW
    assert "overview_daily_in_fragment" in BELOW


def test_daily_change_is_open_after_accounts():
    accounts = PAGE.index('id="ov-accounts"')
    slot = PAGE.index('id="ht-overview-daily"')
    inline = PAGE.index('{% include "_overview_daily.html" %}')
    building = PAGE.index('id="ov-building"')
    show_more = PAGE.index('id="ov-show-more"')
    assert accounts < slot < building
    assert accounts < inline < building
    assert slot < show_more
    assert inline < show_more
    assert "Daily change didn't load" in PAGE
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


def test_overview_shows_the_paper_fill_sync_note():
    assert 'class="ov-paper-fills"' in PAGE
    assert "paper_fills" in PAGE
    assert "See {{ fill.symbol }}" in PAGE
    assert ".ov-paper-fills" in STYLES
    source = Path("app/weekly_review.py").read_text()
    assert "paper_fill_notices" in source
    assert "_with_overview_cache_epoch" in source
    assert "DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)" in source
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY date DESC) <= 12" in source
    warmer = Path("app/cache_ops.py").read_text()
    assert "bind_user_query_epoch" in warmer


def test_session_trades_stay_columns_on_a_phone():
    """Fills stay a table. Phone width scrolls the wrap; it does not stack cards."""
    assert 'class="breakdown-wrap"' in PAGE
    assert 'class="tt-table"' in PAGE
    assert "tbody { display: block" not in STYLES
    assert ".ov-page .tt-table tbody" not in STYLES
    assert ".tt-table td:nth-child(3)" not in STYLES
    assert ".tt-table td:nth-child(5)" not in STYLES
    block = STYLES.split("@media (max-width: 640px) {\n        /* A 390px card", 1)[1]
    block = block.split("@media", 1)[0]
    assert "table-layout: auto" in block
    assert "min-width: 100%" in block
    assert "overflow-x: auto" in block
    assert "text-overflow: ellipsis" not in block
    assert ".tt-verb-long { display: inline" in block
    assert ".tt-verb-short { display: none" in block
    assert "td.tt-qty" in block
    assert "position: sticky" not in block
    assert "tt-sym-cell" in PAGE
    assert "tt-qty" in PAGE
    assert "option_symbol" in PAGE
    assert "tt-verb-short" in PAGE
    assert "tt-tag" in PAGE
    assert "is_spread" in PAGE
    assert "tt-sym-cell" in TODAY
    assert "tt-qty" in TODAY
    assert "is_spread" in TODAY


def test_heatmap_dollars_scroll_inside_the_card():
    assert 'class="cal-scroll"' in DAILY
    scroll = STYLES.split(".cal-scroll {", 1)[1].split("}", 1)[0]
    assert "overflow-x: auto" in scroll
    rule = STYLES.split(".cal-day .cal-day-pnl {", 1)[1].split("}", 1)[0]
    assert ".72rem" in rule
    assert ".38rem" not in rule
    assert "ellipsis" not in rule
    assert "text-overflow" not in rule
    phone = STYLES.split("@media (max-width: 480px)", 1)[1].split("@media", 1)[0]
    assert ".68rem" in phone
    assert ".38rem" not in phone
    assert "1fr" not in phone
    assert "width: 100%" not in phone


def test_heatmap_fills_the_card_from_tablet_up():
    """From 900px the weekday columns share the card. Phone sizing stays."""
    base = STYLES.split(".cal-grid-rolling {", 1)[1].split("}", 1)[0]
    assert "width: max-content" in base
    assert "minmax(3.4rem, max-content)" in base
    start = STYLES.index("@media (min-width: 900px)")
    desk = STYLES[start:STYLES.index(".cal-day.today", start)]
    assert "width: 100%" in desk
    assert "repeat(5, minmax(0, 1fr))" in desk
    assert "min-height: 5.75rem" in desk
    assert "font-size: 1.05rem" in desk
    assert "font-size: 1.4rem" in desk
    assert "color-mix" not in desk
    today = STYLES[STYLES.index(".cal-day.today"):STYLES.index(".cal-day.today") + 80]
    assert "outline: 2px solid var(--color-mirror-ink)" in today


def test_today_names_the_dividend_bar_date_when_it_is_not_today():
    assert "Dividends paid {{ today_movers.as_of_label }}" in TODAY
    assert "Dividends paid today" in TODAY
    assert "combined_impact is not none" in PAGE


def test_snapshot_stays_columns_on_a_phone():
    """Account rows stay a table at phone width. Share of book moves under the name."""
    assert 'class="snapshot-scroll"' in PAGE
    assert 'class="acct-share-phone"' in PAGE
    assert ">1W<" not in PAGE
    assert ">1M<" not in PAGE
    assert "1 week" in PAGE and "1 month" in PAGE
    phone = STYLES.split("@media (max-width: 720px)", 1)[1].split("@media", 1)[0]
    assert ".snapshot-table { display: flex" not in phone
    assert "flex-direction: column" not in phone
    assert "content: attr(data-label)" not in phone
    assert "grid-column: 1 / -1" not in phone
    assert "text-align: left" not in phone
    assert ".snap-col-share { display: none" in phone
    assert "position: sticky" in phone
    assert "left: 0" in phone
    assert "overflow-x: auto" in phone
    assert "overflow-x: clip" in phone
    assert "repeat(3, max-content)" in phone
    assert "var(--font-mono)" in phone
    assert ".acct-share-phone" in phone
    assert "box-shadow: none" in STYLES
    section = STYLES.split(".review-section {", 1)[1].split("}", 1)[0]
    assert "box-shadow: none" in section
    desktop = STYLES.split("@media (max-width: 720px)", 1)[0]
    assert "minmax(0, 1.15fr) minmax(6.5rem, .9fr) max-content repeat(3, minmax(4.25rem, .8fr))" in desktop
    assert ".snapshot-table-row > div:not(:first-child)" in desktop
    assert "text-align: right" in desktop
    assert ".snap-col-share { display: none" not in desktop


def test_headline_figures_stay_one_line_and_shrink_on_a_narrow_screen():
    big = STYLES.split(".ov-big {", 1)[1].split("}", 1)[0]
    assert "clamp(" in big
    assert "container-type: inline-size" in STYLES
    sized = STYLES.split(".ov-top .ov-big:not(.ov-invest) {", 1)[1].split("}", 1)[0]
    assert "clamp(1.05rem, 13.5cqi, 2.75rem)" in sized
    assert "2.75rem" in sized
    assert "18cqi" not in sized
    assert "5.2vw" not in sized
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
    val = STYLES.split(".snapshot-table .snap-cell-val {", 1)[1].split("}", 1)[0]
    assert "ellipsis" not in val
    assert "text-overflow" not in val
    assert "white-space: nowrap" in val
    assert "max-content" in STYLES


def test_radar_chip_subtitles_wrap_so_money_is_not_cut_off():
    small = STYLES.split(".ov-ev small {", 1)[1].split("}", 1)[0]
    assert "white-space: normal" in small
    assert "overflow-wrap: normal" in small
    assert "word-break: normal" in small
    assert "white-space: nowrap" not in small
    assert "text-overflow: ellipsis" not in small
    assert "minmax(3.15rem, auto)" in BELOW
    assert "50px)" not in BELOW


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
