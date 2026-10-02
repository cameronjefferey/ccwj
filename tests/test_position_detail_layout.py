"""Position detail reads as one page: one gain, one loss, one story order."""

from pathlib import Path

PAGE = Path("app/templates/position_detail.html").read_text()


def test_story_order_is_headline_mirror_then_chart_then_legs():
    story = PAGE.find('{% include "_story_summary.html" %}')
    mirror_end = PAGE.find("<!-- MIRROR-END -->")
    legs = PAGE.find('id="pd-legs"')
    chart = PAGE.find('id="position-chart"')
    worked = PAGE.find(">What worked<")
    details = PAGE.find('id="pd-details"')
    by_type = PAGE.find('id="pd-by-type"')
    raw = PAGE.find('id="pd-raw"')
    matrix = PAGE.find('id="pd-matrix"')
    assert story < mirror_end < chart < legs < worked
    assert PAGE.count('id="position-chart"') == 1
    assert PAGE.count('id="pnlChart"') == 1
    assert details < by_type < raw < matrix
    assert '<details class="pd-fold" id="pd-details">' in PAGE
    assert "Breakdown by type and the raw log" in PAGE
    assert PAGE.count('{% include "_story_summary.html" %}') == 1
    assert PAGE.count('{% include "_held_to_expiry.html" %}') == 1
    assert PAGE.count('id="pd-matrix"') == 1
    assert PAGE.count('id="pd-legs"') == 1


def test_gain_and_loss_are_one_pair_and_cards_share_one_style():
    assert "--pd-gain: var(--gain)" in PAGE
    assert "--pd-loss: var(--loss)" in PAGE
    tokens = Path("app/templates/_gain_loss_tokens.html").read_text()
    base = Path("app/templates/base.html").read_text()
    assert "--gain: #28c08a" in tokens
    assert "--loss: #f0556d" in tokens
    assert '{% include "_gain_loss_tokens.html" %}' in base
    assert "box-shadow: none" in PAGE
    for old in ("#1b7a3d", "#5cb85c", "#e8a838", "#c9302c", "#198754", "rgba(25,135,84"):
        assert old not in PAGE
    assert 'class="wl-win"' in PAGE or 'tone = "wl-win"' in PAGE
    assert 'tone = "wl-loss"' in PAGE
    assert '<details class="pd-fold" id="pd-matrix">' in PAGE


def test_hero_scope_chips_sit_in_the_hero_not_a_full_width_bar():
    assert 'class="ov-bar"' not in PAGE
    assert 'class="filter-bar filter-bar-hero"' not in PAGE
    assert 'class="pd-hero-scope"' in PAGE
    assert "data-ht-persist-tenants" in PAGE
    assert "scope_account_choices|length > 1" in PAGE
    hero = PAGE.split('class="review-hero pos-hero"', 1)[1].split("<!-- MIRROR-END -->", 1)[0]
    assert hero.find("pos-hero-id") < hero.find("pos-total") < hero.find("pd-hero-scope")
    assert 'grid-template-areas: "id total" "scope total"' in PAGE
    phone = PAGE.split("@media (max-width: 640px)", 1)[1].split("</style>", 1)[0]
    assert 'grid-template-areas: "id total" "scope scope"' in phone
    assert "minmax(8.75rem, 11.5rem)" in phone
    assert "flex-wrap: nowrap" in phone
    assert "padding: .6rem 1.15rem .65rem" in PAGE
    assert "padding: .45rem .65rem .5rem" in phone


def test_headline_total_uses_cqi_and_does_not_truncate():
    assert 'class="pos-total"' in PAGE
    assert 'class="ov-big' in PAGE
    rule = PAGE.split(".pos-hero .pos-total .ov-big {", 1)[1].split("}", 1)[0]
    assert "clamp(1.05rem, 13.5cqi, 2.75rem)" in rule
    assert "white-space: nowrap" in rule
    assert "ellipsis" not in rule
    assert "text-overflow: clip" in rule
    assert 'class="h2 mb-0' not in PAGE


def test_strategy_premium_column_says_collected():
    assert "term('Premium')" not in PAGE
    assert ">Collected</dt>" in PAGE
    assert "total_premium_received" in PAGE
    assert "Premium received on this strategy." in PAGE


def test_wide_tables_keep_columns_on_a_phone():
    assert "table.pd-legs" in PAGE
    assert "table.pd-stack" in PAGE
    assert "pd-legs-scroll" in PAGE
    assert 'class="pd-openable"' in PAGE
    assert 'data-label="Amount"' in PAGE
    assert 'class="pd-extra"' in PAGE
    assert 'class="pd-more"' in PAGE
    # The legs table tag stays stable for the cell renderer.
    assert '<table class="table table-sm table-hover align-middle mb-0 pd-legs">' in PAGE
    assert "@media (max-width: 640px)" in PAGE
    phone = PAGE.split("@media (max-width: 640px)", 1)[1].split("</style>", 1)[0]
    assert "display: table" in phone
    assert "tbody { display: block" not in phone
    assert "thead { display: none" not in phone
    assert "overflow-x: auto" in phone
    assert "height: 280px" in phone
    assert "height: 520px" in PAGE.split("@media (max-width: 640px)", 1)[0]
