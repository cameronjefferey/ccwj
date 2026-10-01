"""Trader Profile reads as one page: headline, Right now, then the rest."""

from pathlib import Path

PAGE = Path("app/templates/trader_story.html").read_text()


def test_story_order_is_headline_then_right_now_then_collapsed_rest():
    hero = PAGE.index('id="ts-hero"')
    now = PAGE.index('id="ts-now"')
    profile = PAGE.index('id="ts-profile"')
    execution = PAGE.index('id="ts-execution"')
    notable = PAGE.index('id="ts-notable"')
    style = PAGE.index('id="ts-style"')
    years = PAGE.index('id="ts-years"')
    assert hero < now < profile < execution < notable < style < years
    assert "<!-- /ts-hero -->" in PAGE
    assert '<details class="disclosure" id="ts-profile">' in PAGE
    assert '<details class="disclosure" id="ts-execution">' in PAGE
    assert '<details class="disclosure" id="ts-notable">' in PAGE
    assert '<details class="disclosure" id="ts-style">' in PAGE
    assert '<details class="disclosure" id="ts-years">' in PAGE
    assert 'class="section-header">Right now</h2>' in PAGE
    assert 'class="section-header">Profile summary</span>' in PAGE
    assert 'class="section-header">Execution review</span>' in PAGE
    assert 'class="section-header">Notable positions</span>' in PAGE
    assert 'class="section-header">Performance by style</span>' in PAGE
    assert 'class="section-header">Year by year</span>' in PAGE
    assert "shadow-sm" not in PAGE
    assert "this_week.watches" in PAGE
    assert "this_week.items" not in PAGE
    assert "On the clock" in PAGE
    assert "Last week" in PAGE
    assert "ts-loop-kicker" in PAGE
    assert "position_detail" in PAGE
    assert "scoped_url('positions')" in PAGE
    assert "<canvas" not in PAGE


def test_collected_means_premium_received():
    assert "term('Collected')" in PAGE
    assert "Premium received." in PAGE
    assert "net premium" not in PAGE.lower()
    assert "premium collected" not in PAGE.lower()
    assert "placed at risk buying options" in PAGE


def test_phone_rows_stack_on_word_boundaries():
    assert 'data-label="Positions"' in PAGE
    assert 'data-label="Profitable"' in PAGE
    assert 'data-label="Total return"' in PAGE
    assert 'data-label="Best position"' in PAGE
    assert "content: attr(data-label)" in PAGE
    assert "overflow-x: clip" in PAGE
    assert "text-overflow: ellipsis" not in PAGE
    assert "overflow-wrap: anywhere" not in PAGE
    assert "word-break: anywhere" not in PAGE
    assert "overflow-wrap: break-word" in PAGE
    assert "word-break: normal" in PAGE


def test_account_picker_stays_in_the_page_header():
    assert "_account_scope_filters.html" in PAGE
    assert "data-ht-persist-tenants" in PAGE
    assert "data-ht-preserve-query" in PAGE
    assert "scope_account_choices|length > 1" in PAGE
    assert PAGE.index("_account_scope_filters.html") < PAGE.index('id="ts-now"')
    assert "url_for('trader_story', scope='all')" in PAGE
