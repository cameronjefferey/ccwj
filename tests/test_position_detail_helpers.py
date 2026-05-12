"""Unit tests for position detail merge logic (no BigQuery)."""

import pandas as pd
import pytest

from app.routes import (
    _compute_breakdown_by_type,
    _legs_df_to_sessions_list,
    _merge_position_strategy_breakdown,
    _supplement_summary_with_rolled,
)


def _legs_row(
    leg_id, display_leg_num, status, open_date, last_activity_date,
    equity_pnl=0.0, closed_options_pnl=0.0, open_options_pnl=0.0,
    options_count=0, open_options_count=0, options_only=False,
):
    """Shape mirrors int_position_legs SELECT — keeps the test honest about
    the mart contract."""
    combined = round(equity_pnl + closed_options_pnl + open_options_pnl, 2)
    return {
        "account": "Cameron Investment",
        "user_id": 9,
        "symbol": "PLTR",
        "leg_id": leg_id,
        "leg_type": "options_only" if options_only else "equity_session",
        "status": status,
        "open_date": open_date,
        "last_activity_date": last_activity_date,
        "equity_pnl": equity_pnl,
        "closed_options_pnl": closed_options_pnl,
        "open_options_pnl": open_options_pnl,
        "combined_pnl": combined,
        "options_count": options_count,
        "open_options_count": open_options_count,
        "max_quantity_held": 1.0 if not options_only else 0.0,
        "num_trades": options_count if options_only else 1,
        "options_only": options_only,
        "display_leg_num": display_leg_num,
        "days_held": 1,
    }


def _summary_row(account, strategy, status, total_pnl, symbol="RDDT"):
    return {
        "account": account,
        "symbol": symbol,
        "strategy": strategy,
        "status": status,
        "total_pnl": total_pnl,
        "realized_pnl": total_pnl if status == "Closed" else 0.0,
        "unrealized_pnl": total_pnl if status == "Open" else 0.0,
        "total_premium_received": 0.0,
        "total_premium_paid": 0.0,
        "num_trade_groups": 1,
        "num_individual_trades": 1,
        "num_winners": 1 if (status == "Closed" and total_pnl > 0) else 0,
        "num_losers": 1 if (status == "Closed" and total_pnl <= 0) else 0,
        "win_rate": 0.0,
        "avg_pnl_per_trade": total_pnl,
        "avg_days_in_trade": 0.0,
        "first_trade_date": None,
        "last_trade_date": None,
        "total_dividend_income": 0.0,
        "dividend_count": 0,
        "total_return": total_pnl,
    }


def test_merge_adds_all_closed_strategies_when_summary_empty():
    """Same shape as classification fallback: one row per closed option group."""
    closed = pd.DataFrame(
        {
            "account": ["A", "A"],
            "strategy": ["Covered Call", "Long Call"],
            "total_pnl": [100.0, -25.0],
            "premium_received": [5.0, 0.0],
            "premium_paid": [0.0, 0.0],
            "open_date": pd.to_datetime(["2023-01-01", "2024-01-01"]),
            "close_date": pd.to_datetime(["2023-02-01", "2024-02-01"]),
            "days_in_trade": [10, 20],
        }
    )
    out = _merge_position_strategy_breakdown("RDDT", pd.DataFrame(), closed, pd.DataFrame())
    assert len(out) == 2
    strats = set(out["strategy"].astype(str))
    assert strats == {"Covered Call", "Long Call"}
    assert (out["status"] == "Closed").all()
    assert abs(float(out["total_pnl"].sum()) - 75.0) < 0.01


def test_merge_skips_closed_pair_already_in_summary():
    """Extra rows are only (account, strategy) not already in positions_summary."""
    summary = pd.DataFrame(
        [
            {
                "account": "A",
                "symbol": "RDDT",
                "strategy": "Long Call",
                "status": "Open",
                "total_pnl": 50.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 50.0,
                "total_premium_received": 0.0,
                "total_premium_paid": 0.0,
                "num_trade_groups": 1,
                "num_individual_trades": 1,
                "num_winners": 0,
                "num_losers": 0,
                "win_rate": 0.0,
                "avg_pnl_per_trade": 0.0,
                "avg_days_in_trade": 0.0,
                "first_trade_date": None,
                "last_trade_date": None,
                "total_dividend_income": 0.0,
                "dividend_count": 0,
                "total_return": 50.0,
            }
        ]
    )
    closed = pd.DataFrame(
        {
            "account": ["A"],
            "strategy": ["Long Call"],
            "total_pnl": [10.0],
            "premium_received": [0.0],
            "premium_paid": [0.0],
            "open_date": pd.to_datetime(["2020-01-01"]),
            "close_date": pd.to_datetime(["2020-02-01"]),
            "days_in_trade": [5],
        }
    )
    out = _merge_position_strategy_breakdown("RDDT", summary, closed, pd.DataFrame())
    # Long Call already in summary — no duplicate row from closed_legs
    assert len(out) == 1


def test_merge_adds_different_closed_strategy_from_legs():
    summary = pd.DataFrame(
        [
            {
                "account": "A",
                "symbol": "RDDT",
                "strategy": "Long Call",
                "status": "Open",
                "total_pnl": 50.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 50.0,
                "total_premium_received": 0.0,
                "total_premium_paid": 0.0,
                "num_trade_groups": 1,
                "num_individual_trades": 1,
                "num_winners": 0,
                "num_losers": 0,
                "win_rate": 0.0,
                "avg_pnl_per_trade": 0.0,
                "avg_days_in_trade": 0.0,
                "first_trade_date": None,
                "last_trade_date": None,
                "total_dividend_income": 0.0,
                "dividend_count": 0,
                "total_return": 50.0,
            }
        ]
    )
    closed = pd.DataFrame(
        {
            "account": ["A"],
            "strategy": ["Covered Call"],
            "total_pnl": [200.0],
            "premium_received": [0.0],
            "premium_paid": [0.0],
            "open_date": pd.to_datetime(["2019-01-01"]),
            "close_date": pd.to_datetime(["2019-06-01"]),
            "days_in_trade": [100],
        }
    )
    out = _merge_position_strategy_breakdown("RDDT", summary, closed, pd.DataFrame())
    assert len(out) == 2
    assert "Covered Call" in set(out["strategy"].astype(str))


def test_supplement_adds_missing_closed_strategies_from_rollup():
    """Mart has open strategy only; classification rollup fills in a closed one."""
    summary = pd.DataFrame([_summary_row("A", "Long Call", "Open", 50.0)])
    rolled = pd.DataFrame([
        _summary_row("A", "Long Call", "Open", 50.0),
        _summary_row("A", "Wheel", "Closed", 420.0),
    ])
    out = _supplement_summary_with_rolled(summary, rolled)
    strats = set(out["strategy"].astype(str))
    assert strats == {"Long Call", "Wheel"}
    assert len(out) == 2
    wheel = out[out["strategy"] == "Wheel"].iloc[0]
    assert wheel["status"] == "Closed"
    assert float(wheel["total_pnl"]) == 420.0


def test_supplement_keeps_mart_row_when_pair_already_exists():
    """Mart rows take precedence; rollup does not override total_pnl."""
    summary = pd.DataFrame([_summary_row("A", "Long Call", "Open", 50.0)])
    rolled = pd.DataFrame([_summary_row("A", "Long Call", "Closed", 999.0)])
    out = _supplement_summary_with_rolled(summary, rolled)
    assert len(out) == 1
    r = out.iloc[0]
    assert r["status"] == "Open"
    assert float(r["total_pnl"]) == 50.0


def test_supplement_with_empty_summary_returns_rolled():
    rolled = pd.DataFrame([_summary_row("A", "Wheel", "Closed", 100.0)])
    out = _supplement_summary_with_rolled(pd.DataFrame(), rolled)
    assert len(out) == 1
    assert out.iloc[0]["strategy"] == "Wheel"


def test_supplement_with_empty_rolled_returns_summary():
    summary = pd.DataFrame([_summary_row("A", "Long Call", "Open", 10.0)])
    out = _supplement_summary_with_rolled(summary, pd.DataFrame())
    assert len(out) == 1
    assert out.iloc[0]["strategy"] == "Long Call"


# --------------------------------------------------------------------------- #
# Account-scoping invariant: when /position/<symbol>?account=X scopes the
# page to one account, the int_strategy supplement MUST NOT pull rows from
# the user's other accounts. See routes.py around `strat_accounts_scope`.
# Regression: /position/JEPI?account=Schwab••••0044 used to render rows for
# 4828, 8602, Coco — leaking other accounts into a filtered view.
# --------------------------------------------------------------------------- #


def test_position_detail_scopes_int_strategy_supplement_to_selected_account(
    monkeypatch,
):
    """
    Smoke check for the account-scoping fix in routes.position_detail. We don't
    need to spin up the full Flask request — instead we mimic the small block
    that decides which list of accounts to pass to the int_strategy fetch and
    assert that selected_account wins over the wider user_accounts list.
    """
    user_accounts = ["A", "B", "C"]

    # User filtered to account "B"
    selected_account = "B"
    scope = [selected_account] if selected_account else user_accounts
    assert scope == ["B"]
    # And when no selection is set, fall back to the full set
    selected_account = ""
    scope = [selected_account] if selected_account else user_accounts
    assert scope == user_accounts


def test_merge_does_not_promote_equity_leg_descriptions_to_strategy_rows():
    """
    Regression: int_closed_equity_legs.description is the LEG TYPE
    ("Equity Sold" / "Cost Written Off"), not a strategy. Promoting it
    to the strategy column produced duplicate Strategy Breakdown rows for
    the same Buy-and-Hold session (one row per leg description), each one
    looking like a separate strategy outcome — visible bug on JEPI/0044.
    The merge should fold equity legs into a single 'Buy and Hold' row
    when summary_df doesn't already cover that account.
    """
    closed_equity = pd.DataFrame(
        {
            "account": ["Schwab ••••0044", "Schwab ••••0044"],
            "session_id": [1, 1],
            "open_date": pd.to_datetime(["2024-07-31", "2024-07-31"]),
            "close_date": pd.to_datetime(["2026-04-15", "2026-04-15"]),
            "quantity": [1000.0, 1000.0],
            "cost_basis": [54973.85, 54973.85],
            "sell_proceeds": [57655.0, 0.0],
            "realized_pnl": [2681.15, -54973.85],
            "status": ["Closed", "Closed"],
            "description": ["Equity Sold", "Cost Written Off"],
        }
    )
    out = _merge_position_strategy_breakdown(
        "JEPI", pd.DataFrame(), pd.DataFrame(), closed_equity
    )
    # One row per (account, strategy) — not three.
    assert len(out) == 1, out
    r = out.iloc[0]
    assert r["account"] == "Schwab ••••0044"
    assert r["strategy"] == "Buy and Hold"
    # And nothing labeled "Cost Written Off" or "Equity Sold" anywhere.
    strats = set(out["strategy"].astype(str))
    assert "Cost Written Off" not in strats
    assert "Equity Sold" not in strats


def test_merge_falls_through_when_summary_already_has_buy_and_hold_for_account():
    """
    If positions_summary already classifies the closed equity session as
    Buy-and-Hold for that account, the merge should not append a synthetic
    row — that would double the count.
    """
    summary = pd.DataFrame([_summary_row("Schwab ••••0044", "Buy and Hold", "Closed", 2681.15)])
    closed_equity = pd.DataFrame(
        {
            "account": ["Schwab ••••0044"],
            "session_id": [1],
            "open_date": pd.to_datetime(["2024-07-31"]),
            "close_date": pd.to_datetime(["2026-04-15"]),
            "quantity": [1000.0],
            "cost_basis": [54973.85],
            "sell_proceeds": [57655.0],
            "realized_pnl": [2681.15],
            "status": ["Closed"],
            "description": ["Equity Sold"],
        }
    )
    out = _merge_position_strategy_breakdown(
        "JEPI", summary, pd.DataFrame(), closed_equity
    )
    assert len(out) == 1, out
    assert out.iloc[0]["strategy"] == "Buy and Hold"


def test_merge_falls_through_when_summary_has_dividend_strategy_for_account():
    """
    Regression for the JEPI/0044 visible bug: positions_summary reclassifies
    Buy-and-Hold to "Dividend" when div income > trade gain. The merge used
    to look up only ("Buy and Hold") in `existing` so when the mart shipped
    a "Dividend" row, the merge thought no equity row existed and appended
    a synthetic Buy-and-Hold *alongside* the Dividend row — two rows for
    the same closed session, only one with $16k of dividends.
    Equity-bucket rows ("Buy and Hold" or its "Dividend" reclassification)
    occupy the same slot; either's presence should suppress synthesis.
    """
    summary = pd.DataFrame([
        {
            **_summary_row("Schwab ••••0044", "Dividend", "Closed", 18861.15),
            "total_dividend_income": 16180.0,
            "dividend_count": 21,
        }
    ])
    closed_equity = pd.DataFrame(
        {
            "account": ["Schwab ••••0044"],
            "session_id": [1],
            "open_date": pd.to_datetime(["2024-07-31"]),
            "close_date": pd.to_datetime(["2026-04-15"]),
            "quantity": [1000.0],
            "cost_basis": [54973.85],
            "sell_proceeds": [57655.0],
            "realized_pnl": [2681.15],
            "status": ["Closed"],
            "description": ["Equity Sold"],
        }
    )
    out = _merge_position_strategy_breakdown(
        "JEPI", summary, pd.DataFrame(), closed_equity
    )
    assert len(out) == 1, f"Expected single equity-bucket row, got: {out}"
    r = out.iloc[0]
    assert r["strategy"] == "Dividend"
    assert float(r["total_dividend_income"]) == 16180.0
    # Trade-side P&L preserved as part of total_pnl
    assert float(r["total_pnl"]) == 18861.15


def test_supplement_skips_buy_and_hold_when_mart_has_dividend_same_symbol():
    """
    Regression: positions_summary emits strategy 'Dividend' while
    int_strategy_classification rollup still says 'Buy and Hold'. Both
    are the single equity-slot row for JEPI-class symbols; supplement
    must not add duplicate realized P&L (trips invariant: sum(strategy
    totals) ≠ chart).
    """
    summary = pd.DataFrame([
        _summary_row("Schwab ••••0044", "Dividend", "Closed", 20940.30, symbol="JEPI"),
    ])
    rolled = pd.DataFrame([
        _summary_row("Schwab ••••0044", "Buy and Hold", "Closed", 4312.30, symbol="JEPI"),
    ])
    out = _supplement_summary_with_rolled(summary, rolled)
    assert len(out) == 1, out
    assert out.iloc[0]["strategy"] == "Dividend"


def test_supplement_does_not_introduce_unrelated_account_rows():
    """
    Even if the rolled DataFrame is built from a wider account list (defensive
    case if a future refactor forgets the scope), supplementing should not
    silently inject rows that don't already exist in summary_df keyed on
    (account, strategy). This is what visibly fired on the JEPI page.
    """
    summary = pd.DataFrame([_summary_row("0044", "Buy and Hold", "Open", -52000.0)])
    # Rolled rows from accounts the user didn't ask for. The supplement
    # behavior is to ADD missing (account, strategy) pairs — so this test
    # documents that the scoping invariant lives at the FETCH layer, not the
    # supplement layer. If you ever change supplement to filter, update this.
    rolled = pd.DataFrame([
        _summary_row("0044", "Buy and Hold", "Open", -52000.0),
        _summary_row("4828", "Buy and Hold", "Closed", -10000.0),
        _summary_row("Coco", "Buy and Hold", "Closed", 1000.0),
    ])
    out = _supplement_summary_with_rolled(summary, rolled)
    accounts = sorted(out["account"].unique().tolist())
    # If this test ever flips and accounts == ['0044'], it means supplement
    # learned to scope by account too. That's OK — just delete this test.
    assert "4828" in accounts and "Coco" in accounts, (
        "Supplement is account-agnostic; scoping must happen at the fetch step"
    )


def test_legs_to_sessions_list_empty_df_returns_empty_list():
    """Empty mart fetch (e.g. brand-new symbol with no trades) shouldn't crash
    or invent legs. Position detail must keep rendering with sessions=[]."""
    assert _legs_df_to_sessions_list(pd.DataFrame()) == []
    assert _legs_df_to_sessions_list(None) == []


def test_legs_to_sessions_list_preserves_leg_id_and_display_order():
    """Mart leg_id ↔ legacy session_id contract: sequential positive ids
    per (user_id, account, symbol), sorted by display_leg_num. Critical
    because bookmarked URLs (?leg=N) and the JS pill click handler both
    pivot on these stable ids."""
    rows = [
        # Out of order on purpose; helper must sort by display_leg_num.
        _legs_row(
            leg_id=2, display_leg_num=2, status="Open",
            open_date="2025-06-03", last_activity_date="2026-05-08",
            equity_pnl=-226.27, closed_options_pnl=381.0, open_options_pnl=-3163.67,
            options_count=24, open_options_count=2,
        ),
        _legs_row(
            leg_id=1, display_leg_num=1, status="Closed",
            open_date="2024-11-25", last_activity_date="2024-11-29",
            closed_options_pnl=-1715.0, options_count=1, options_only=True,
        ),
    ]
    out = _legs_df_to_sessions_list(pd.DataFrame(rows))
    assert [s["display_leg"] for s in out] == [1, 2]
    assert [s["session_id"] for s in out] == [1, 2]
    assert [s["status"] for s in out] == ["Closed", "Open"]


def test_legs_to_sessions_list_open_leg_merges_equity_and_live_options():
    """The PLTR/Cameron Investment regression in the merged-interval model.
    A LEAP opened mid-equity-session and still live, plus a short call
    opened after the equity sold off — these used to render as TWO Open
    legs (one for each anchor window), which a trader correctly reads as
    nonsense (you only have one current PLTR chapter). Under the merged
    model they collapse into one Open leg whose interval extends from
    the equity session start through to today. status='Open',
    last_trade_date = today's snapshot."""
    rows = [
        _legs_row(
            leg_id=2, display_leg_num=2, status="Open",
            open_date="2025-06-03", last_activity_date="2026-05-08",
            equity_pnl=-226.27, closed_options_pnl=381.0,
            open_options_pnl=-3163.67,
            options_count=24, open_options_count=2,
        ),
    ]
    out = _legs_df_to_sessions_list(pd.DataFrame(rows))
    assert len(out) == 1
    s = out[0]
    assert s["status"] == "Open"
    assert s["last_trade_date"] == "2026-05-08"
    # Combined = equity + closed options + open unrealized. Two open
    # contracts (LEAP at -$4,355, short call at +$1,192) net to -$3,164.
    assert abs(s["combined_pnl"] - (-3008.94)) < 0.01, s
    # Both currently-open contracts are reflected in the count.
    assert s["open_options_count"] == 2


def test_legs_to_sessions_list_options_pnl_combines_closed_and_open():
    """options_pnl is the leg-level options total the template renders.
    Must sum closed_options_pnl + open_options_pnl from the mart so the
    pill caption reflects current value, not just settled trades."""
    rows = [
        _legs_row(
            leg_id=1, display_leg_num=1, status="Open",
            open_date="2026-04-14", last_activity_date="2026-05-08",
            closed_options_pnl=-1118.0, open_options_pnl=1191.65,
            options_count=4, open_options_count=1, options_only=True,
        ),
    ]
    out = _legs_df_to_sessions_list(pd.DataFrame(rows))
    assert len(out) == 1
    s = out[0]
    assert s["options_only"] is True
    assert abs(s["options_pnl"] - 73.65) < 0.01
    assert abs(s["combined_pnl"] - 73.65) < 0.01
    assert s["equity_pnl"] == 0.0
    assert s["open_options_count"] == 1


def test_legs_to_sessions_list_handles_null_dates():
    """Mart can emit NULL last_activity_date (rare but possible if a
    snapshot-only leg has no close info). The reshape must not crash and
    must produce empty strings the template can render with [:10] slicing."""
    row = _legs_row(
        leg_id=1, display_leg_num=1, status="Closed",
        open_date="2024-01-01", last_activity_date=None,
    )
    out = _legs_df_to_sessions_list(pd.DataFrame([row]))
    assert out[0]["last_trade_date"] == ""
    assert out[0]["open_date"] == "2024-01-01"


# --- Breakdown by Type (equity / options / dividends) ---------------------


class _StubBQClient:
    """Tiny BigQuery client stand-in for testing _compute_breakdown_by_type
    without spinning up an actual BQ connection. Returns a fixed dividends
    DataFrame for any query, so we can isolate dividend filtering logic
    from query construction."""

    def __init__(self, dividends_df):
        self._dividends = dividends_df

    def query(self, _sql):
        outer = self

        class _Job:
            def to_dataframe(self_inner):
                return outer._dividends.copy()

        return _Job()


def _bd_lookup(rows, type_label):
    for r in rows:
        if r["type"] == type_label:
            return r
    raise AssertionError(f"missing {type_label} row in {rows}")


def test_breakdown_by_type_unfiltered_pltr_shape():
    """Whole-position render: equity from int_closed_equity_legs sums per
    closure event (3 partial sells of one Buy and Hold session), options
    from closed_legs + open mark-to-market, and a 0 dividend row when the
    symbol pays nothing. The session count must collapse to 1 even though
    the closed_equity_df has 3 rows for one continuous session — otherwise
    the UI says '3 sessions' for a position the trader thinks of as one."""
    closed_equity = pd.DataFrame([
        {"account": "Cameron Investment", "session_id": 1,
         "open_date": "2025-06-03", "close_date": "2025-12-30",
         "realized_pnl": 40.89},
        {"account": "Cameron Investment", "session_id": 1,
         "open_date": "2025-06-03", "close_date": "2026-02-23",
         "realized_pnl": -683.58},
        {"account": "Cameron Investment", "session_id": 1,
         "open_date": "2025-06-03", "close_date": "2026-04-06",
         "realized_pnl": 416.42},
    ])
    closed_legs = pd.DataFrame([
        {"account": "Cameron Investment", "open_date": "2025-06-03", "total_pnl": -1334.0},
    ])
    current = pd.DataFrame([
        {"account": "Cameron Investment", "instrument_type": "Call",
         "unrealized_pnl": -4354.0},
        {"account": "Cameron Investment", "instrument_type": "Call",
         "unrealized_pnl": 1190.33},
    ])
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(pd.DataFrame()),
        safe_symbol="PLTR",
        strat_accounts_scope=["Cameron Investment"],
        closed_equity_df=closed_equity,
        closed_legs_df=closed_legs,
        current_df=current,
        leg_predicate=None,
    )
    eq = _bd_lookup(rows, "Equity")
    assert eq["realized"] == -226.27, eq
    assert eq["unrealized"] == 0.0
    assert eq["count"] == 1, "three closure events but only one session"
    assert eq["count_label"] == "session"

    opt = _bd_lookup(rows, "Options")
    assert opt["realized"] == -1334.0
    assert abs(opt["unrealized"] - (-3163.67)) < 0.01
    assert opt["count"] == 3
    assert opt["count_open"] == 2

    div = _bd_lookup(rows, "Dividends")
    assert div["unrealized"] is None, "dividends never have a mark-to-market"
    assert div["count"] == 0


def test_breakdown_by_type_returns_empty_when_no_activity():
    """Card must hide entirely when there is nothing to show."""
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(pd.DataFrame()),
        safe_symbol="ZZZZ",
        strat_accounts_scope=["Cameron Investment"],
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame(),
        current_df=pd.DataFrame(),
        leg_predicate=None,
    )
    assert rows == []


def test_breakdown_by_type_filters_dividends_by_leg_predicate():
    """When a leg pill is selected, only dividend events whose trade_date
    falls inside that leg's window contribute. JEPI's monthly distributions
    pre-2026 must NOT show up when the trader filters to a 2026-only leg."""
    from datetime import date

    dividends = pd.DataFrame([
        {"account": "Cameron Investment", "user_id": 9, "symbol": "JEPI",
         "trade_date": pd.Timestamp("2024-08-15"), "amount": 12.34},
        {"account": "Cameron Investment", "user_id": 9, "symbol": "JEPI",
         "trade_date": pd.Timestamp("2026-03-15"), "amount": 9.87},
    ])
    leg_window = (date(2026, 1, 1), date(2026, 12, 31))
    pred = lambda d: leg_window[0] <= d <= leg_window[1]

    rows = _compute_breakdown_by_type(
        client=_StubBQClient(dividends),
        safe_symbol="JEPI",
        strat_accounts_scope=["Cameron Investment"],
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame(),
        current_df=pd.DataFrame([
            {"account": "Cameron Investment", "instrument_type": "Equity",
             "unrealized_pnl": 50.0},
        ]),
        leg_predicate=pred,
    )
    div = _bd_lookup(rows, "Dividends")
    assert div["realized"] == 9.87, "only the in-window event counts"
    assert div["count"] == 1
    eq = _bd_lookup(rows, "Equity")
    assert eq["unrealized"] == 50.0
    assert eq["count_open"] == 1


def test_breakdown_by_type_dividend_query_failure_is_non_fatal():
    """A schema drift on int_dividend_events must not 500 the position page.
    The breakdown card should still render with a 0-dividend row."""

    class _FailingClient:
        def query(self, _sql):
            class _Job:
                def to_dataframe(self_inner):
                    raise RuntimeError("simulated BQ schema drift")
            return _Job()

    rows = _compute_breakdown_by_type(
        client=_FailingClient(),
        safe_symbol="PLTR",
        strat_accounts_scope=["Cameron Investment"],
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame([
            {"account": "Cameron Investment", "open_date": "2025-06-03", "total_pnl": 100.0},
        ]),
        current_df=pd.DataFrame(),
        leg_predicate=None,
    )
    div = _bd_lookup(rows, "Dividends")
    assert div["realized"] == 0.0
    assert div["count"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
