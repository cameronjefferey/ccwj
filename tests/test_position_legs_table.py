"""Position Legs table: numbers stay whole, If held is a value or a dash."""

from datetime import datetime
from pathlib import Path

from app import app


def _legs_table_source():
    html = Path("app/templates/position_detail.html").read_text()
    start = html.index(
        '<table class="table table-sm table-hover align-middle mb-0 pd-legs">'
    )
    depth = 0
    i = start
    while i < len(html):
        next_open = html.find("<table", i)
        next_close = html.find("</table>", i)
        if next_open != -1 and (next_close == -1 or next_open <= next_close):
            depth += 1
            i = next_open + len("<table")
            continue
        depth -= 1
        i = next_close + len("</table>")
        if depth == 0:
            return html[start:i]
    raise AssertionError("Position Legs table is not closed")


def _render_legs():
    year = datetime.now().year
    prior = year - 1
    current_positions = [{
        "description": "BE",
        "leg_kind": "Equity",
        "instrument_type": "Equity",
        "account": "Sara Investment",
        "account_display": "Sara Investment",
        "tenant_id": "t1",
        "quantity_display": "200",
        "open_date": f"{year}-04-23",
        "days_held": 160,
        "cost_basis": 47639.20,
        "market_value": 56162.00,
        "unrealized_pnl": 8523.00,
        "unrealized_pnl_pct": 17.9,
        "leg_num": 1,
    }]
    trade_outcomes = [
        {
            "trade_symbol": "BE",
            "strategy": "Covered Call",
            "account": "Sara Investment",
            "account_display": "Sara Investment",
            "tenant_id": "t1",
            "quantity_display": "2",
            "open_date": f"{year}-09-25",
            "close_date": f"{year}-09-25",
            "days_held": 0,
            "cost": 825.32,
            "proceeds": 338.66,
            "pnl": -486.66,
            "return_pct": -14.2,
            "is_winner": False,
            "type": "option",
            "leg_num": 1,
            "close_type": "Closed",
        },
        {
            "trade_symbol": "BE",
            "strategy": "Covered Call",
            "account": "Sara Investment",
            "account_display": "Sara Investment",
            "tenant_id": "t1",
            "quantity_display": "2",
            "open_date": f"{prior}-09-11",
            "close_date": f"{prior}-09-11",
            "days_held": 0,
            "cost": 0,
            "proceeds": 296.66,
            "pnl": 296.66,
            "return_pct": 100.0,
            "is_winner": True,
            "type": "option",
            "leg_num": 1,
            "close_type": "Expired",
            "held_id": "held-9",
            "held_pill": "$40 more if held",
            "pnl_if_held": 336.66,
            "held_better": True,
        },
    ]
    source = (
        "{% set current_positions = current_positions %}"
        "{% set trade_outcomes = trade_outcomes %}"
        + _legs_table_source()
    )
    with app.app_context(), app.test_request_context("/"):
        return app.jinja_env.from_string(source).render(
            current_positions=current_positions,
            trade_outcomes=trade_outcomes,
            symbol="BE",
        )


def test_compact_date_drops_the_current_year():
    year = datetime.now().year
    compact = app.jinja_env.filters["compact_date"]
    human = app.jinja_env.filters["human_date"]
    assert compact(f"{year}-09-25") == "Sep 25"
    assert compact(f"{year}-04-23") == "Apr 23"
    prior = year - 1
    assert compact(f"{prior}-09-11") == f"Sep 11 '{str(prior)[-2:]}"
    assert human(f"{year}-09-25") == f"Sep 25, {year}"
    assert compact("") == ""
    assert compact("not-a-date") == "not-a-date"


def test_legs_table_keeps_numbers_whole_and_dashes_if_held():
    page = Path("app/templates/position_detail.html").read_text()
    assert "table-layout: auto" in page
    assert "table-layout: fixed" not in page
    assert "Value / Proceeds" not in page
    assert 'title="Market value, or proceeds if closed">Value</th>' in page
    assert "min-width: max-content" in page
    assert 'class="pd-num">Return</th>' in page
    # Phones keep the figure columns. Account and days drop earlier.
    legs = page[page.index("mb-0 pd-legs"):]
    header = legs[legs.index("<thead"):legs.index("</thead>")]
    assert 'data-m="hide"' not in header[header.rfind("<th", 0, header.index(">If held<")):]
    assert "pd-opt" in header[header.index(">Account<") - 80:header.index(">Account<")]
    assert "JetBrains Mono" in page

    html = _render_legs()
    year = datetime.now().year
    prior = year - 1
    assert "-14.2%" in html
    assert "100.0%" in html
    assert "17.9%" in html
    assert "$47,639.20" in html
    assert "$56,162.00" in html
    assert "$825.32" in html
    assert "-$486.66" in html
    # Full dates stay on the tooltip; the cell itself is compact.
    assert f'title="Sep 25, {year}">Sep 25</td>' in html
    assert f'title="Apr 23, {year}">Apr 23</td>' in html
    # Jinja escapes the apostrophe; the browser shows Sep 11 '25.
    assert f"Sep 11 &#39;{str(prior)[-2:]}</td>" in html
    assert 'title="Covered Call"' in html
    assert 'title="Sara Investment"' in html
    # Expired / not-yet-gradeable legs are a dash, not an empty cell.
    assert 'title="Not applicable">—</span>' in html
    assert 'title="Not applicable">—</td>' in html
    # Early close with a chart shows the counterfactual dollars, clickable.
    assert 'data-held-open="held-9"' in html
    assert 'title="$40 more if held"' in html
    assert "$336.66" in html
    assert "$40 more if held</button>" not in html
