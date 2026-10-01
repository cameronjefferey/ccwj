"""Strategies reads as one page: headline, the cards, then the rest."""

from pathlib import Path

PAGE = Path("app/templates/strategies.html").read_text()
FIT = Path("app/templates/strategy_fit.html").read_text()


def test_strategies_story_order_is_headline_then_cards_then_collapsed_rest():
    hero = PAGE.index('id="strat-hero"')
    listing = PAGE.index('id="strat-list"')
    focus = PAGE.index('id="strategy-focus"')
    breakdown = PAGE.index('id="strat-breakdown"')
    over = PAGE.index('id="strat-over-time"')
    moves = PAGE.index('id="strat-moves"')
    assert hero < listing < focus < breakdown < over < moves
    assert '<details class="disclosure" id="strat-breakdown">' in PAGE
    assert '<details class="disclosure" id="strat-over-time">' in PAGE
    assert '<details class="disclosure" id="strat-moves">' in PAGE
    assert 'class="section-header">Strategies</h2>' in PAGE
    assert 'class="section-header">Breakdown by type</span>' in PAGE
    assert 'class="section-header">Over time</span>' in PAGE
    assert 'class="section-header">What moves this strategy</span>' in PAGE
    assert "shadow-sm" not in PAGE
    assert 'aria-label="Strategies view"' in PAGE
    assert PAGE.index('aria-label="Strategies view"') < hero
    assert "<!-- /strat-hero -->" in PAGE
    assert "position_detail" in PAGE
    assert "population_label" in PAGE
    assert "focus_strategy.num_symbols }} symbol" not in PAGE
    assert "scoped_url('positions'" in PAGE


def test_collected_means_premium_received_and_net_is_labeled_net():
    assert "term('Collected')" in PAGE
    assert "Premium received." in PAGE
    assert "net premium" not in PAGE.lower()
    assert "term('Net Premium')" not in PAGE
    assert "collected minus what was paid" in PAGE
    assert "term('Realized')" in PAGE
    assert "term('Unrealized')" in PAGE


def test_phone_rows_stack_and_the_chart_resizes_when_opened():
    assert 'data-label="Total return"' in PAGE
    assert 'data-label="Realized"' in PAGE
    assert 'data-label="Unrealized"' in PAGE
    assert "content: attr(data-label)" in PAGE
    assert "overflow-x: clip" in PAGE
    assert "text-overflow: ellipsis" not in PAGE
    assert "display: flex !important" in PAGE
    assert "table-layout: fixed" in PAGE
    assert "Chart.getChart" in PAGE
    assert "minmax(min(100%, 280px), 1fr)" in PAGE


def test_account_picker_stays_in_the_page_header():
    assert "_account_scope_filters.html" in PAGE
    assert "data-ht-persist-tenants" in PAGE
    assert "data-ht-preserve-query" in PAGE
    assert "scope_account_choices|length > 1" in PAGE


def test_fit_matrix_is_headline_then_matrix_then_notes():
    hero = FIT.index('id="fit-hero"')
    card = FIT.index('id="fitCard"')
    notes = FIT.index('id="fit-notes"')
    assert hero < card < notes
    assert '<details class="disclosure" id="fit-notes">' in FIT
    assert "Where the result is negative" in FIT
    assert "Where you lose money" not in FIT
    assert "fit-table-wrap" in FIT
    assert "overflow: auto" in FIT
    assert "function renderTotal" in FIT
    assert 'id="fitScrollHint"' in FIT
    assert "<!-- /fit-hero -->" in FIT
    assert 'aria-label="Strategies view"' in FIT
    assert FIT.index('aria-label="Strategies view"') < hero
    assert "_account_scope_filters.html" in FIT
    assert "data-ht-persist-tenants" in FIT
    assert "scope_account_choices|length > 1" in FIT
