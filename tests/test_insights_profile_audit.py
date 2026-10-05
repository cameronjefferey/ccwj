"""Insights and Trader Profile figures recomputed from fills and rows.

The warehouse already groups a same-day spread as one win or loss.
These tests pin the page readers that were still averaging or counting
each leg, and the labels that describe the resulting number.
"""

from datetime import date

import pandas as pd

import app.insights as insights
from app.outcome_units import SPREAD_STRATEGIES, option_record_from_fills
from app.position_story import _money
from app.trader_story import _contract_record_fact, compose_novel, build_book


def _fill(day, symbol, action, occ, qty, amount, tenant="snaptrade:real",
         account="Real", price=1.0):
    return {
        "account": account,
        "tenant_id": tenant,
        "symbol": symbol,
        "trade_date": pd.Timestamp(day),
        "action": action,
        "instrument_type": "Call" if occ[-9] == "C" or "C0" in occ else "Put",
        "trade_symbol": occ,
        "quantity": qty,
        "price": price,
        "amount": amount,
    }


def test_exit_timing_headline_weights_by_scored_closes():
    """A 3-close strategy must not count the same as a 200-close one.

    Unweighted mean of 10% and 80% is 45%. Weighted by scored closes:
    (3 * 10 + 200 * 80) / 203 = 79%.
    """
    rows = [
        {"strategy": "Covered Call", "giveback_pct": 10.0, "days_past_peak": 1.0,
         "pnl_given_back": 30.0, "trades": 3, "total_closed": 10},
        {"strategy": "Cash-Secured Put", "giveback_pct": 80.0, "days_past_peak": 9.0,
         "pnl_given_back": 1600.0, "trades": 200, "total_closed": 250},
    ]
    headlines = insights._exit_timing_headlines(rows, 203, 400)
    weighted = (3 * 10 + 200 * 80) / 203
    assert round(headlines["avg_giveback"], 1) == round(weighted, 1)
    assert round(headlines["avg_giveback"]) == 79
    assert round(headlines["avg_days"], 1) == round((3 * 1 + 200 * 9) / 203, 1)
    assert headlines["total_given_back"] == 1630.0
    brief = insights._format_exit_timing_brief(headlines, rows)
    assert "79%" in brief
    assert "45%" not in brief
    assert "Given back:" in brief
    assert "Days after the high:" in brief
    assert "203 of 400" in brief
    assert "DTE WIN RATES" not in brief


def test_discovery_sequence_groups_spreads_and_skips_undecided():
    sql = insights._DISCOVERY_SQL.format(
        tenant_clause="",
        sequence_clause="",
        spread_list=insights._spread_strategy_sql(),
    )
    assert "int_trade_sequence" not in sql
    assert "is_winner IS NOT NULL" not in sql
    assert "PARTITION BY tenant_id" in sql
    assert "LOGICAL_AND(status = 'Closed')" in sql
    assert "ROUND(total_pnl, 2) > 0" in sql
    assert "ROUND(total_pnl, 2) < 0" in sql
    for name in SPREAD_STRATEGIES:
        assert f"'{name}'" in sql


def test_weekly_summary_groups_a_spread_and_picks_the_true_best():
    """ANY_VALUE on the weekly mart could name a $100 leg the best trade.

    The two call-spread legs are one +$60 result. The other account's
    +$500 long call is the best closed result.
    """
    rows = [
        {
            "week_start": date(2026, 9, 28), "tenant_id": "snaptrade:a",
            "account": "A", "user_id": "1", "symbol": "SPX",
            "strategy": "Call Spread", "trade_symbol": "SPXW  260930C07650000",
            "open_date": date(2026, 9, 28), "close_date": date(2026, 9, 30),
            "option_expiry": date(2026, 9, 30), "status": "Closed",
            "total_pnl": 100.0, "trades_opened": 4, "dividends_amount": 12.5,
        },
        {
            "week_start": date(2026, 9, 28), "tenant_id": "snaptrade:a",
            "account": "A", "user_id": "1", "symbol": "SPX",
            "strategy": "Call Spread", "trade_symbol": "SPXW  260930C07655000",
            "open_date": date(2026, 9, 28), "close_date": date(2026, 9, 30),
            "option_expiry": date(2026, 9, 30), "status": "Closed",
            "total_pnl": -40.0, "trades_opened": 4, "dividends_amount": 12.5,
        },
        {
            "week_start": date(2026, 9, 28), "tenant_id": "snaptrade:b",
            "account": "B", "user_id": "1", "symbol": "MU",
            "strategy": "Long Call", "trade_symbol": "MU    261016C00120000",
            "open_date": date(2026, 9, 29), "close_date": date(2026, 9, 30),
            "option_expiry": date(2026, 10, 16), "status": "Closed",
            "total_pnl": 500.0, "trades_opened": 4, "dividends_amount": 12.5,
        },
        {
            "week_start": date(2026, 9, 28), "tenant_id": "snaptrade:b",
            "account": "B", "user_id": "1", "symbol": "QQQ",
            "strategy": "Long Put", "trade_symbol": "QQQ   261016P00400000",
            "open_date": date(2026, 9, 29), "close_date": date(2026, 9, 30),
            "option_expiry": date(2026, 10, 16), "status": "Closed",
            "total_pnl": 0.0, "trades_opened": 4, "dividends_amount": 12.5,
        },
    ]
    summary = insights.summarize_weekly_closes(pd.DataFrame(rows))
    assert summary["winners"] == 2
    assert summary["losers"] == 0
    assert summary["zeros"] == 1
    assert summary["pnl"] == 560.0
    assert summary["best"]["symbol"] == "MU"
    assert summary["best"]["pnl"] == 500.0
    assert summary["worst"]["pnl"] == 60.0
    text = insights.format_weekly_context(summary)
    assert "WEEK STARTING 2026-09-28" in text
    assert "2 closed (2W/0L, 100%)" in text
    assert "1 closed at $0" in text
    assert "Dividends $12.50 are separate" in text
    assert "Best closed result: MU" in text
    assert "$100.00" not in text
    assert "a spread counts as one" in text
    assert "not included" in text


def test_weekly_all_losers_and_large_dollars_stay_literal():
    rows = [{
        "week_start": "2026-09-28", "tenant_id": "t", "account": "A",
        "symbol": "BE", "strategy": "Covered Call", "trade_symbol": "BE1",
        "open_date": "2026-09-01", "option_expiry": "2026-09-30",
        "status": "Closed", "total_pnl": -1234567.89,
        "trades_opened": 1, "dividends_amount": 0,
    }]
    summary = insights.summarize_weekly_closes(pd.DataFrame(rows))
    text = insights.format_weekly_context(summary)
    assert "0%" in text
    assert "1 closed (0W/1L, 0%)" in text
    assert "-$1,234,567.89" in text
    assert "$-1" not in text
    assert "1.2M" not in text
    assert _money(1234567) == "$1,234,567"


def test_prompt_uses_grouped_closes_and_spread_net_credit():
    df = pd.DataFrame([
        {
            "symbol": "SPX", "strategy": "Call Spread", "status": "Closed",
            "total_pnl": 60, "realized_pnl": 60, "unrealized_pnl": 0,
            "total_premium_received": 300, "total_premium_paid": 100,
            "num_individual_trades": 4, "num_winners": 1, "num_losers": 0,
            "win_rate": 1, "avg_pnl_per_trade": 60, "avg_days_in_trade": 2,
            "first_trade_date": "2026-09-28", "last_trade_date": "2026-09-30",
            "total_dividend_income": 0, "total_return": 60,
            "num_trade_groups": 2,
        },
        {
            "symbol": "AAA", "strategy": "Covered Call", "status": "Open",
            "total_pnl": 10, "realized_pnl": 0, "unrealized_pnl": 10,
            "total_premium_received": 50, "total_premium_paid": 0,
            "num_individual_trades": 2, "num_winners": 0, "num_losers": 0,
            "win_rate": 0, "avg_pnl_per_trade": 0, "avg_days_in_trade": 5,
            "first_trade_date": "2026-09-01", "last_trade_date": "2026-09-30",
            "total_dividend_income": 0, "total_return": 10,
            "num_trade_groups": 1,
        },
    ])
    text = insights._build_prompt_data(df)
    assert "Closed results: 1 (1W / 0L)" in text
    assert "Fills: 6" in text
    assert "Trades: 6" not in text
    assert "Net credit: $250.00" in text
    assert "Opens are not included" in text
    assert "includes positions still open" in text


def _occ(root, yymmdd, cp, strike):
    return f"{root}{yymmdd}{cp}{int(round(strike * 1000)):08d}"


def test_option_record_from_fills_groups_and_drops_edge_cases():
    short = _occ("SPXW", "260930", "C", 7650)
    long = _occ("SPXW", "260930", "C", 7655)
    lone = _occ("MU", "261016", "C", 120)
    zero = _occ("QQQ", "261016", "P", 400)
    orphan = _occ("KLAC", "260918", "C", 800)
    partial = _occ("NVO", "260918", "C", 50)
    rolled_old = _occ("AMD", "260918", "C", 150)
    rolled_new = _occ("AMD", "261016", "C", 155)
    assigned = _occ("T", "260918", "P", 20)
    loser = _occ("BE", "260918", "C", 30)
    loser2 = _occ("BE", "260925", "C", 31)
    day = date(2026, 9, 28)

    # Two sequential round-trips on the same strikes stay two verticals
    # because the prices differ. Same-price re-opens fuse into one lot.
    lots = [
        _fill(day, "SPXW", "option_sell_to_open", short, 20, 2000.0, price=1.0),
        _fill(day, "SPXW", "option_buy_to_open", long, 20, -800.0, price=0.4),
        _fill(date(2026, 9, 29), "SPXW", "option_buy_to_close", short, 20, -400.0, price=0.2),
        _fill(date(2026, 9, 29), "SPXW", "option_sell_to_close", long, 20, 200.0, price=0.1),
        _fill(date(2026, 9, 29), "SPXW", "option_sell_to_open", short, 10, 900.0, price=2.0),
        _fill(date(2026, 9, 29), "SPXW", "option_buy_to_open", long, 10, -400.0, price=0.8),
        _fill(date(2026, 9, 30), "SPXW", "option_expired", short, 10, 0.0, price=0.0),
        _fill(date(2026, 9, 30), "SPXW", "option_expired", long, 10, 0.0, price=0.0),
    ]
    condor = [
        _fill(day, "SPX", "option_sell_to_open", _occ("SPX", "261016", "C", 5000), 1, 150),
        _fill(day, "SPX", "option_buy_to_open", _occ("SPX", "261016", "C", 5010), 1, -40),
        _fill(day, "SPX", "option_sell_to_open", _occ("SPX", "261016", "P", 4800), 1, 120),
        _fill(day, "SPX", "option_buy_to_open", _occ("SPX", "261016", "P", 4790), 1, -30),
        _fill(date(2026, 10, 16), "SPX", "option_expired", _occ("SPX", "261016", "C", 5000), 1, 0),
        _fill(date(2026, 10, 16), "SPX", "option_expired", _occ("SPX", "261016", "C", 5010), 1, 0),
        _fill(date(2026, 10, 16), "SPX", "option_expired", _occ("SPX", "261016", "P", 4800), 1, 0),
        _fill(date(2026, 10, 16), "SPX", "option_expired", _occ("SPX", "261016", "P", 4790), 1, 0),
    ]
    other = [
        _fill(day, "MU", "option_buy_to_open", lone, 1, -500),
        _fill(date(2026, 10, 1), "MU", "option_sell_to_close", lone, 1, 800),
        _fill(day, "QQQ", "option_sell_to_open", zero, 1, 100),
        _fill(date(2026, 10, 1), "QQQ", "option_buy_to_close", zero, 1, -100),
        _fill(date(2026, 9, 18), "KLAC", "option_buy_to_close", orphan, 1, 400),
        _fill(day, "NVO", "option_sell_to_open", partial, 2, 200),
        _fill(date(2026, 9, 30), "NVO", "option_buy_to_close", partial, 1, -50),
        _fill(date(2026, 9, 1), "AMD", "option_sell_to_open", rolled_old, 1, 300),
        _fill(day, "AMD", "option_buy_to_close", rolled_old, 1, -80),
        _fill(day, "AMD", "option_sell_to_open", rolled_new, 1, 250),
        _fill(date(2026, 9, 1), "T", "option_sell_to_open", assigned, 1, 500),
        _fill(date(2026, 9, 18), "T", "option_assigned", assigned, 1, -200),
    ]
    fills = lots + condor + other
    record = option_record_from_fills(fills)
    # 2 vertical lots + 1 condor + MU win + AMD closed roll + assignment.
    # QQQ $0, KLAC no-open, NVO partial, and the new AMD open are out.
    assert record["winners"] == 6
    assert record["losers"] == 0
    assert record["zeros"] == 1
    assert record["decided"] == 6

    losers = [
        _fill(day, "BE", "option_buy_to_open", loser, 1, -100),
        _fill(date(2026, 9, 18), "BE", "option_sell_to_close", loser, 1, 20),
        _fill(day, "BE", "option_buy_to_open", loser2, 1, -80),
        _fill(date(2026, 9, 25), "BE", "option_expired", loser2, 1, 0),
    ]
    all_lose = option_record_from_fills(losers)
    assert all_lose == {"winners": 0, "losers": 2, "zeros": 0, "decided": 2}

    one = option_record_from_fills(losers[:2])
    fact = _contract_record_fact(one)
    assert fact["value"] == "0W / 1L"
    assert "One closed option trade" in fact["detail"]
    assert "no opening fill" in fact["detail"]

    assert option_record_from_fills([])["decided"] == 0
    assert _contract_record_fact(option_record_from_fills([])) is None

    paper_win = [
        _fill(day, "AMD", "option_sell_to_open", rolled_old, 1, 999,
              tenant="snaptrade:paper", account="Paper"),
        _fill(date(2026, 9, 18), "AMD", "option_expired", rolled_old, 1, 0,
              tenant="snaptrade:paper", account="Paper"),
        _fill(day, "BE", "option_buy_to_open", loser, 1, -100),
        _fill(date(2026, 9, 18), "BE", "option_sell_to_close", loser, 1, 20),
    ]
    scoped = option_record_from_fills(paper_win, tenant_ids=["snaptrade:real"])
    assert scoped["winners"] == 0
    assert scoped["losers"] == 1


def test_profile_contract_record_comes_from_fills_not_per_leg_fingerprint():
    from tests.test_trader_story import _BOOK_SUMMARY, _BOOK_TRADES, _book
    novel = compose_novel(_book(), _BOOK_TRADES)
    facts = {f["label"]: f for f in novel["profile"]["facts"]}
    assert facts["Closed option trades"]["value"] == "2W / 0L"
    assert "A spread counts as one" in facts["Closed option trades"]["detail"]
    # The story engine still counts per contract. This page does not
    # copy that fingerprint into the record.
    book = build_book(_BOOK_TRADES, None, None, _BOOK_SUMMARY)
    per_leg = sum(e["stats"]["contract_wins"] for e in book.values())
    assert per_leg == 2
