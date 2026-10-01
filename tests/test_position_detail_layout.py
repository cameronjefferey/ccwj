"""Position detail reads as one page: one gain, one loss, one story order."""

from pathlib import Path

PAGE = Path("app/templates/position_detail.html").read_text()


def test_story_order_is_headline_chart_what_worked_then_details():
    story = PAGE.find('{% include "_story_summary.html" %}')
    chart = PAGE.find('id="position-chart"')
    worked = PAGE.find(">What worked<")
    runs = PAGE.find('{% include "_covered_call_runs.html" %}')
    held = PAGE.find('{% include "_held_to_expiry.html" %}')
    legs = PAGE.rfind("Position Legs")
    by_type = PAGE.find('id="pd-by-type"')
    raw = PAGE.find('id="pd-raw"')
    matrix = PAGE.find('id="pd-matrix"')
    assert story < chart < worked < runs < held < legs
    assert legs - held < 400
    assert legs < by_type < raw < matrix
    assert PAGE.count('{% include "_story_summary.html" %}') == 1
    assert PAGE.count('{% include "_held_to_expiry.html" %}') == 1
    assert PAGE.count('id="pd-matrix"') == 1


def test_gain_and_loss_are_one_pair_and_cards_share_one_style():
    assert "--pd-gain: var(--gain)" in PAGE
    assert "--pd-loss: var(--loss)" in PAGE
    base = Path("app/templates/base.html").read_text()
    assert "--gain: #28c08a" in base
    assert "--loss: #f0556d" in base
    assert "box-shadow: none" in PAGE
    for old in ("#1b7a3d", "#5cb85c", "#e8a838", "#c9302c", "#198754", "rgba(25,135,84"):
        assert old not in PAGE
    assert 'class="wl-win"' in PAGE or 'tone = "wl-win"' in PAGE
    assert 'tone = "wl-loss"' in PAGE
    assert '<details class="pd-fold" id="pd-matrix">' in PAGE


def test_strategy_premium_column_says_collected():
    assert "term('Premium')" not in PAGE
    assert ">Collected</dt>" in PAGE
    assert "total_premium_received" in PAGE
    assert "Premium received on this strategy." in PAGE


def test_wide_tables_stack_on_a_phone():
    assert "table.pd-legs" in PAGE
    assert "table.pd-stack" in PAGE
    assert "pd-legs-scroll" in PAGE
    assert 'class="pd-openable"' in PAGE
    assert 'data-label="Amount"' in PAGE
    assert 'class="pd-extra"' in PAGE
    # The legs table tag stays stable for the cell renderer.
    assert '<table class="table table-sm table-hover align-middle mb-0 pd-legs">' in PAGE
    assert "@media (max-width: 640px)" in PAGE
    assert "overflow-x: clip" in PAGE
