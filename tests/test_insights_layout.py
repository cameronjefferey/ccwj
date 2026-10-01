"""AI Insights reads as one page: headline, the patterns, then the rest."""

from pathlib import Path

PAGE = Path("app/templates/insights.html").read_text()


def test_insights_story_order_is_headline_then_patterns_then_collapsed_rest():
    hero = PAGE.index('id="ins-hero"')
    patterns = PAGE.index('id="ins-patterns"')
    timing = PAGE.index('id="ins-timing"')
    writeup = PAGE.index('id="ins-writeup"')
    ask = PAGE.index('id="ins-ask"')
    assert hero < patterns < timing < writeup < ask
    assert "<!-- /ins-hero -->" in PAGE
    assert '<details class="disclosure" id="ins-timing">' in PAGE
    assert '<details class="disclosure" id="ins-writeup">' in PAGE
    assert '<details class="disclosure" id="ins-ask">' in PAGE
    assert 'class="section-header">Patterns</h2>' in PAGE
    assert 'class="section-header">Exit timing</span>' in PAGE
    assert 'class="section-header">Write-up</span>' in PAGE
    assert 'class="section-header">Ask AI</span>' in PAGE
    assert "shadow-sm" not in PAGE
    assert "position_detail" in PAGE
    assert 'id="coachForm"' in PAGE
    assert 'id="coachInput"' in PAGE
    assert 'id="llmModelSelect"' in PAGE
    assert 'id="genBtn"' in PAGE
    assert 'id="regenBtn"' in PAGE
    assert 'optgroup label="Included"' in PAGE
    assert 'optgroup label="HappyTrader AI"' in PAGE
    assert "js-llm-model-field" in PAGE
    assert "<canvas" not in PAGE


def test_realized_is_the_close_and_given_back_is_not_a_second_result():
    assert "term('Realized')" in PAGE
    assert ">Actual<" not in PAGE
    assert "Given back" in PAGE
    assert "not a second result" in PAGE
    assert "term('Collected')" not in PAGE
    assert "net premium" not in PAGE.lower()
    assert "premium captured" in PAGE
    assert 'class="strat-name"' in PAGE
    assert "{% for s in coaching.signals %}" in PAGE


def test_phone_rows_stack_on_word_boundaries():
    assert 'data-label="Realized"' in PAGE
    assert 'data-label="Given back"' in PAGE
    assert 'data-label="Symbol"' in PAGE
    assert "content: attr(data-label)" in PAGE
    assert "display: flex !important" in PAGE
    assert "table-layout: fixed" in PAGE
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
    assert PAGE.index("_account_scope_filters.html") < PAGE.index('id="ins-patterns"')
    assert "url_for('insights', scope='all')" in PAGE
