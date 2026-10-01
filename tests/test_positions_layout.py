"""Positions list reads as one page: headline, the symbol table, then the rest."""

from pathlib import Path

PAGE = Path("app/templates/positions.html").read_text()


def test_positions_story_order_is_headline_then_table_then_collapsed_rest():
    hero = PAGE.index('class="review-hero pos-hero"')
    listing = PAGE.index('id="pos-list"')
    filters = PAGE.index('id="pos-filters"')
    symbol = PAGE.index('id="symbolTable"')
    strategy = PAGE.index('id="strategy-detail"')
    detail = PAGE.index('id="positionsTable"')
    breakdown = PAGE.index('id="pos-breakdown"')
    assert hero < listing < filters < symbol < strategy < detail < breakdown
    assert '<details class="disclosure" id="strategy-detail">' in PAGE
    assert '<details class="disclosure" id="pos-breakdown">' in PAGE
    assert 'class="section-header">Positions</span>' not in PAGE
    assert 'class="section-header">Positions</h2>' in PAGE
    assert 'class="section-header">By strategy</span>' in PAGE
    assert 'class="section-header">Breakdown</span>' in PAGE
    assert "shadow-sm" not in PAGE
    assert "One row per account and symbol." in PAGE


def test_secondary_sections_start_collapsed_and_keep_search_and_pager():
    assert 'data-ht-search="#symbolTable"' in PAGE
    assert 'data-ht-search="#positionsTable"' in PAGE
    assert 'data-ht-pager="#strategyPager"' in PAGE
    assert 'data-server-sort="1"' in PAGE
    assert 'id="strategyPager"' in PAGE
    assert "No positions match the selected filters." in PAGE
    assert "No results match this search." in PAGE


def test_collected_means_premium_received_and_net_is_labeled_net():
    assert "term_link('Collected'" in PAGE
    assert "term('Collected')" in PAGE
    assert "Premium collected" in PAGE
    assert "Premium received." in PAGE
    assert "net premium" not in PAGE.lower()
    assert "The number on the right is the net." in PAGE
    assert "term_link('Realized'" in PAGE
    assert "term_link('Unrealized'" in PAGE
    assert "{{ term('Unrealized') }}" in PAGE
    assert ">W/L<" in PAGE


def test_symbol_header_sticks_under_the_nav_without_an_inner_scroller():
    assert "body .pos-page #symbolTable thead th" in PAGE
    assert "position: sticky" in PAGE
    assert "top: var(--ht-nav-h, 3.5rem)" in PAGE
    assert "ht-sticky" not in PAGE
    assert "max-height: 75vh" not in PAGE
    assert "overflow: visible" in PAGE


def test_money_can_wrap_and_win_loss_stays_one_value():
    assert "td.pos-money" in PAGE
    assert "white-space: nowrap" not in PAGE.split("td.pos-money", 1)[1].split("}", 1)[0]
    assert PAGE.count('class="pos-wl-pair"') >= 2
    assert 'data-label="W/L"' in PAGE


def test_phone_rows_stack_with_labels_and_desktop_columns_are_not_clipped():
    assert 'data-label="Collected"' in PAGE
    assert 'data-label="Total return"' in PAGE
    assert 'data-label="W/L"' in PAGE
    assert "content: attr(data-label)" in PAGE
    assert "overflow-x: clip" in PAGE
    assert "text-overflow: ellipsis" not in PAGE
    assert "swipe" not in PAGE
    assert "body:has(#positionsTable) .ht-page { max-width: none; }" in PAGE
    assert "@media (min-width: 768px) and (max-width: 1366px)" in PAGE
    assert PAGE.count("table-layout: fixed") >= 2
    assert "display: flex !important" in PAGE


def test_account_picker_stays_in_the_page_header():
    assert "_account_scope_filters.html" in PAGE
    assert "data-ht-persist-tenants" in PAGE
    assert "data-ht-preserve-query" in PAGE
    assert "scope_account_choices|length > 1" in PAGE
    assert 'class="hero-chip"' in PAGE
    assert "<!-- /pos-hero -->" in PAGE
