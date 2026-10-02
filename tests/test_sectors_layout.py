"""Sectors reads as one page: headline, the sector table, then the rest."""

from pathlib import Path

PAGE = Path("app/templates/sectors.html").read_text()


def test_sectors_story_order_is_headline_then_table_then_collapsed_rest():
    hero = PAGE.index('id="sec-hero"')
    listing = PAGE.index('id="sec-list"')
    table = PAGE.index('id="secTable"')
    detail = PAGE.index('id="sec-detail"')
    assert hero < listing < table < detail
    assert '<details class="disclosure" id="sec-detail">' in PAGE
    assert 'class="section-header">Sectors</h2>' in PAGE
    assert 'class="section-header">Inside each sector</span>' in PAGE
    assert "shadow-sm" not in PAGE
    assert "<!-- /sec-hero -->" in PAGE
    assert "position_detail" in PAGE
    assert "scoped_url('positions'" in PAGE
    assert "replace(' ', '-')|lower" in PAGE
    assert "kpis.num_subsectors|default(0)" in PAGE
    assert 'id="sec-detail"' in PAGE
    assert "details.open = true" in PAGE


def test_collected_means_premium_received():
    assert "term('Collected')" in PAGE
    assert "Premium received." in PAGE
    assert "term('Premium')" not in PAGE
    assert "net premium" not in PAGE.lower()
    assert "term('Realized')" in PAGE
    assert "term('Unrealized')" in PAGE


def test_phone_rows_stay_columns_and_desktop_columns_are_not_clipped():
    assert 'data-label="Sector"' in PAGE
    assert 'data-label="Total return"' in PAGE
    assert 'data-label="Realized"' in PAGE
    assert 'data-label="Unrealized"' in PAGE
    assert 'data-label="Symbols"' in PAGE
    assert "content: attr(data-label)" not in PAGE
    assert "thead { display: none" not in PAGE
    assert "overflow-x: clip" in PAGE
    assert "overflow-x: auto" in PAGE
    assert "text-overflow: ellipsis" not in PAGE
    assert "display: table" in PAGE
    assert "display: flex !important" not in PAGE
    assert "table-layout: fixed" in PAGE
    assert 'class="sec-name"' in PAGE
    assert "overflow-wrap: break-word" in PAGE
    assert "word-break: normal" in PAGE
    assert "sec-sub-head" in PAGE
    assert "sec-subs" in PAGE


def test_account_picker_stays_in_the_page_header():
    assert "_account_scope_filters.html" in PAGE
    assert "data-ht-persist-tenants" in PAGE
    assert "data-ht-preserve-query" in PAGE
    assert "scope_account_choices|length > 1" in PAGE
