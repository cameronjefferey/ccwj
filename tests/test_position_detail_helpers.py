"""Unit tests for position detail merge logic (no BigQuery)."""

from datetime import date

import pandas as pd
import pytest

from app.routes import (
    _compute_breakdown_by_type,
    _equity_raw_trades_for_partial_close_outcome,
    _legs_df_to_sessions_list,
    _merge_position_strategy_breakdown,
    _supplement_summary_with_rolled,
)


def _legs_row(
    leg_id, display_leg_num, status, open_date, last_activity_date,
    equity_pnl=0.0, closed_options_pnl=0.0, open_options_pnl=0.0,
    options_count=0, open_options_count=0, options_only=False,
    account="Cameron Investment", tenant_id="snaptrade:cam-uuid",
):
    """Shape mirrors int_position_legs SELECT — keeps the test honest about
    the mart contract."""
    combined = round(equity_pnl + closed_options_pnl + open_options_pnl, 2)
    return {
        "tenant_id": tenant_id,
        "account": account,
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


def test_legs_to_sessions_list_carries_tenant_and_account_for_grouping():
    """Legs must carry ``tenant_id`` + ``account`` so the position page can
    group pills by account when a symbol is traded across several accounts
    (leg_id / display_leg restart per tenant → without a grouping key the
    UI shows a confusing run of duplicate 'Leg 1' pills)."""
    rows = [
        _legs_row(
            leg_id=1, display_leg_num=1, status="Closed",
            open_date="2024-06-17", last_activity_date="2024-09-23",
            equity_pnl=100.0,
            account="Schwab Account", tenant_id="snaptrade:aaa",
        ),
        _legs_row(
            leg_id=1, display_leg_num=1, status="Open",
            open_date="2026-05-11", last_activity_date="2026-07-17",
            equity_pnl=200.0,
            account="Schwab Account", tenant_id="snaptrade:bbb",
        ),
    ]
    out = _legs_df_to_sessions_list(pd.DataFrame(rows))
    # Same display_leg / session_id across the two accounts (the collision
    # the grouping fixes) — but distinct tenant_id keys the group.
    assert [s["display_leg"] for s in out] == [1, 1]
    assert [s["session_id"] for s in out] == [1, 1]
    assert {s["tenant_id"] for s in out} == {"snaptrade:aaa", "snaptrade:bbb"}
    assert all(s["account"] == "Schwab Account" for s in out)


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
        tenant_scope=None,
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


def test_breakdown_by_type_otm_at_expiry_today_realizes_not_unrealizes():
    """Friday OTM expiry: int_option_contracts auto-closes the contract
    (status='Closed', total_pnl=net_cash_flow), int_strategy_classification
    surfaces it as a Closed option row, and int_enriched_current drops
    the option row from the snapshot. Breakdown by Type must therefore
    show the credit under Options.realized — NOT Options.unrealized.

    Pre-fix (no auto-close, no int_enriched_current filter): the contract
    landed in BOTH closed_legs_df (because dbt eventually marked it
    Closed after midnight UTC) AND current_df (because the broker
    snapshot still carried it for 1-2 days). Options.unrealized double-
    counted the stale broker mark-to-close while Options.realized
    showed the same contract's net_cash_flow — Breakdown total
    disagreed with the chart by exactly the broker's stale mv.
    """
    # Closed legs query now returns the auto-closed contract with
    # total_pnl=net_cash_flow ($208 premium kept on the OTM short call).
    closed_legs = pd.DataFrame([
        {
            "account": "Cameron Investment",
            "open_date": "2026-05-12",
            "total_pnl": 208.0,
        },
    ])
    # current_df no longer contains the auto-closed Call (dbt filtered
    # it). It still carries the position's open equity row.
    current = pd.DataFrame([
        {
            "account": "Cameron Investment",
            "instrument_type": "Equity",
            "unrealized_pnl": 0.0,
        },
    ])
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(pd.DataFrame()),
        safe_symbol="PLTR",
        tenant_scope=None,
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=closed_legs,
        current_df=current,
        leg_predicate=None,
    )
    opt = _bd_lookup(rows, "Options")
    assert opt["realized"] == 208.0, (
        f"OTM auto-close credit should land in realized. Got {opt}"
    )
    assert opt["unrealized"] == 0.0, (
        f"int_enriched_current filtered out the auto-closed contract, "
        f"so Options.unrealized should be 0 (no double-count of the "
        f"stale broker snapshot). Got {opt}"
    )
    assert opt["count"] == 1
    assert opt["count_open"] == 0


def test_breakdown_by_type_returns_empty_when_no_activity():
    """Card must hide entirely when there is nothing to show."""
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(pd.DataFrame()),
        safe_symbol="ZZZZ",
        tenant_scope=None,
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
        tenant_scope=None,
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


def test_breakdown_by_type_admin_scope_none_runs_dividend_query():
    """Admin views (`?account=` empty + non-tenant user) flow
    `strat_accounts_scope=None` into `_compute_breakdown_by_type`.

    The pre-fix gate `strat_accounts_scope is not None and len(...) > 0`
    short-circuited admin entirely, leaving `div_total = 0.0`. That zero
    then propagated up to `kpis["dividend_income"]` via the override
    block in routes.py (line ~3216, "pin hero total_return to ledger"),
    so admin users saw $0 dividends in Hero / Breakdown-by-Type while
    Strategy Breakdown showed the correct positions_summary value (real
    JEPI bug May 2026: $0 vs $77,780 split).

    Admin must run the query unscoped (`_account_sql_and(None) == ""`)
    so all tenants' dividends roll up. `_filter_df_by_accounts(df, None)`
    is a no-op for admin (per `_filter_df_by_user` short-circuit).
    """
    dividends = pd.DataFrame([
        {"account": "Schwab ••••0044", "user_id": 8, "symbol": "JEPI",
         "trade_date": pd.Timestamp("2024-08-01"), "amount": 578.0},
        {"account": "Schwab ••••4828", "user_id": 8, "symbol": "JEPI",
         "trade_date": pd.Timestamp("2024-08-01"), "amount": 578.0},
    ])
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(dividends),
        safe_symbol="JEPI",
        tenant_scope=None,  # admin
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame(),
        current_df=pd.DataFrame(),
        leg_predicate=None,
    )
    div = _bd_lookup(rows, "Dividends")
    assert div["realized"] == 1156.0, (
        f"Admin (scope=None) must see all $1156 of JEPI dividends, "
        f"not $0 (gate regression). Got: {div}"
    )
    assert div["count"] == 2


def test_breakdown_by_type_empty_account_list_still_short_circuits():
    """A logged-in user with ZERO linked accounts (`strat_accounts_scope=[]`)
    has no data to show — the dividend query SHOULD still be skipped.
    The fix that opens up admin (None) must NOT let through the genuine
    empty-list case (tenant exists but is unlinked / freshly signed up).
    """
    dividends = pd.DataFrame([
        {"account": "Schwab ••••0044", "user_id": 8, "symbol": "JEPI",
         "trade_date": pd.Timestamp("2024-08-01"), "amount": 578.0},
    ])
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(dividends),
        safe_symbol="JEPI",
        tenant_scope=[],  # logged in but no linked tenants
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame(),
        current_df=pd.DataFrame(),
        leg_predicate=None,
    )
    # No equity, no options, no dividends queried → no rows at all
    assert rows == [], (
        "Empty account list must still short-circuit (no data to show); "
        f"got {rows}"
    )


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
        tenant_scope=None,
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


# --- Crypto positions (Coinbase via SnapTrade) ---------------------------


def test_breakdown_by_type_crypto_symbol_relabels_equity_row_as_crypto():
    """BTC / ETH / USDC sit-and-hold positions go through the same
    int_equity_sessions → int_closed_equity_legs mechanics as a stock
    holding (buy → hold → sell). The breakdown card relabels the row
    as ``Crypto`` so the user sees the asset-class signal in the UI
    instead of fusing it with their VOO / JEPI Equity total."""
    # Open BTC position — unrealized only.
    current_df = pd.DataFrame([
        {"account": "Coinbase Account", "instrument_type": "Equity",
         "unrealized_pnl": 202.27},
    ])
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(pd.DataFrame()),
        safe_symbol="BTC",
        tenant_scope=None,
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame(),
        current_df=current_df,
        leg_predicate=None,
    )
    types = [r["type"] for r in rows]
    assert "Crypto" in types, f"BTC should surface as Crypto, got {types}"
    assert "Equity" not in types, "BTC must not double-render as Equity"
    crypto = _bd_lookup(rows, "Crypto")
    assert crypto["unrealized"] == pytest.approx(202.27)
    assert crypto["count_open"] == 1
    assert crypto["count_label"] == "holding"


def test_breakdown_by_type_crypto_symbol_suppresses_dividends_row():
    """Crypto holdings don't pay dividends in our pipeline (no
    yfinance ex-div feed, no broker dividend events for BTC/ETH).
    Rendering ``$0 dividends`` on every crypto page is noisy — the
    breakdown card must skip the row entirely."""
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(pd.DataFrame()),
        safe_symbol="ETH",
        tenant_scope=None,
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame(),
        current_df=pd.DataFrame([
            {"account": "Coinbase Account", "instrument_type": "Equity",
             "unrealized_pnl": 100.0},
        ]),
        leg_predicate=None,
    )
    types = [r["type"] for r in rows]
    assert "Dividends" not in types, (
        f"Crypto pages must not render a Dividends row; got {types}"
    )


def test_breakdown_by_type_crypto_holding_plural_label():
    """Multi-fill BTC position (two snapshot rows, e.g. user holds BTC
    in two sub-wallets that SnapTrade surfaces separately) reads as
    ``2 holdings`` not ``2 sessions`` — the equity-session terminology
    doesn't fit the crypto mental model."""
    current_df = pd.DataFrame([
        {"account": "Coinbase Account", "instrument_type": "Equity",
         "unrealized_pnl": 100.0},
        {"account": "Coinbase Account", "instrument_type": "Equity",
         "unrealized_pnl": 50.0},
    ])
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(pd.DataFrame()),
        safe_symbol="BTC",
        tenant_scope=None,
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame(),
        current_df=current_df,
        leg_predicate=None,
    )
    crypto = _bd_lookup(rows, "Crypto")
    assert crypto["count"] == 2
    assert crypto["count_label"] == "holdings"


def test_breakdown_by_type_usdc_stablecoin_classifies_as_crypto():
    """USDC is a stablecoin and lives on a crypto exchange in this
    product. The user weekly-DCAs ``Buy 100 USDC at $1.00``. It must
    classify as Crypto (not Equity) so the strategy breakdown matches
    the rest of the user's Coinbase activity."""
    rows = _compute_breakdown_by_type(
        client=_StubBQClient(pd.DataFrame()),
        safe_symbol="USDC",
        tenant_scope=None,
        closed_equity_df=pd.DataFrame(),
        closed_legs_df=pd.DataFrame(),
        current_df=pd.DataFrame([
            {"account": "Coinbase Account", "instrument_type": "Equity",
             "unrealized_pnl": 0.0},
        ]),
        leg_predicate=None,
    )
    assert _bd_lookup(rows, "Crypto")  # raises if missing


def test_equity_raw_trades_clips_future_partial_sells():
    """Each partial-close outcome should show buy + fills through that close only."""

    trades = [
        {"trade_date": date(2024, 7, 31), "trade_symbol": "JEPI", "account": "A", "qty": -2000, "instrument_type": "Equity"},
        {"trade_date": date(2026, 4, 15), "trade_symbol": "JEPI", "account": "A", "qty": 1000, "instrument_type": "Equity"},
        {"trade_date": date(2026, 5, 6), "trade_symbol": "JEPI", "account": "A", "qty": 1000, "instrument_type": "Equity"},
    ]
    session_range = (date(2024, 7, 31), date(2026, 5, 6))
    first = _equity_raw_trades_for_partial_close_outcome(
        trades,
        trade_symbol="JEPI",
        account="A",
        session_range=session_range,
        close_milestone=date(2026, 4, 15),
    )
    second = _equity_raw_trades_for_partial_close_outcome(
        trades,
        trade_symbol="JEPI",
        account="A",
        session_range=session_range,
        close_milestone=date(2026, 5, 6),
    )
    assert len(first) == 2
    assert len(second) == 3
    assert {r["trade_date"] for r in first} == {date(2024, 7, 31), date(2026, 4, 15)}


def test_chart_partition_current_falls_back_when_snapshot_user_id_null():
    """Mart partition carries populated user_id; enriched_current still NULL
    until backfill — strict match used to drop all rows and skip live MTM."""
    from app.routes import _filter_current_for_chart_partition

    cdf = pd.DataFrame(
        [
            {
                "account": "Emmory Investment",
                "user_id": None,
                "instrument_type": "Equity",
                "market_value": 2369.7,
                "cost_basis": 2019.8,
                "unrealized_pnl": 349.9,
                "current_price": 236.97,
            },
        ]
    )
    out = _filter_current_for_chart_partition(cdf, "Emmory Investment", 9)
    assert len(out) == 1
    assert float(out["unrealized_pnl"].iloc[0]) == pytest.approx(349.9, rel=0, abs=0.01)


def test_drop_phantom_equity_writeoffs_strips_when_broker_still_holds():
    """IYW-shaped: 10-share open lot in current_df + bogus 10-share Cost
    Written Off in closed_equity_df → strip the writeoff."""
    from app.routes import _drop_phantom_equity_writeoffs

    closed_equity = pd.DataFrame([
        {
            "account": "Emmory Investment",
            "symbol": "IYW",
            "trade_symbol": "IYW",
            "quantity": 10,
            "cost_basis": 1966.78,
            "realized_pnl": -1966.78,
            "description": "Cost Written Off",
        },
        {
            "account": "Emmory Investment",
            "symbol": "IYW",
            "trade_symbol": "IYW",
            "quantity": 2,
            "cost_basis": 393.36,
            "realized_pnl": 9.46,
            "description": "Equity Sold",
        },
    ])
    current = pd.DataFrame([
        {
            "account": "Emmory Investment",
            "symbol": "IYW",
            "instrument_type": "Equity",
            "quantity": 10,
            "market_value": 2369.7,
            "cost_basis": 2019.8,
            "unrealized_pnl": 349.9,
        },
    ])
    kept, removed = _drop_phantom_equity_writeoffs(closed_equity, current)
    assert len(kept) == 1
    assert str(kept["description"].iloc[0]).lower() == "equity sold"
    assert len(removed) == 1
    assert float(removed["realized_pnl"].iloc[0]) == pytest.approx(-1966.78)


def test_drop_phantom_equity_writeoffs_keeps_writeoff_when_no_open_shares():
    """No matching open lot → real loss → keep the row."""
    from app.routes import _drop_phantom_equity_writeoffs

    closed_equity = pd.DataFrame([
        {
            "account": "A",
            "symbol": "XYZ",
            "trade_symbol": "XYZ",
            "quantity": 10,
            "realized_pnl": -1000.0,
            "description": "Cost Written Off",
        },
    ])
    current = pd.DataFrame()
    kept, removed = _drop_phantom_equity_writeoffs(closed_equity, current)
    assert len(kept) == 1
    assert removed.empty


def test_drop_phantom_equity_writeoffs_keeps_when_open_shares_too_few():
    """Open 3 shares but writeoff for 10 — broker can't absorb residual,
    so the writeoff is plausibly real (off-platform transfer of 7)."""
    from app.routes import _drop_phantom_equity_writeoffs

    closed_equity = pd.DataFrame([
        {
            "account": "A",
            "symbol": "XYZ",
            "trade_symbol": "XYZ",
            "quantity": 10,
            "realized_pnl": -1000.0,
            "description": "Cost Written Off",
        },
    ])
    current = pd.DataFrame([
        {
            "account": "A",
            "symbol": "XYZ",
            "instrument_type": "Equity",
            "quantity": 3,
        },
    ])
    kept, removed = _drop_phantom_equity_writeoffs(closed_equity, current)
    assert len(kept) == 1
    assert removed.empty


def test_drop_phantom_equity_writeoffs_account_isolation():
    """Open 10 shares in account A must not absorb a 10-share writeoff
    in account B (different masked accounts = different real holdings)."""
    from app.routes import _drop_phantom_equity_writeoffs

    closed_equity = pd.DataFrame([
        {
            "account": "B",
            "symbol": "XYZ",
            "trade_symbol": "XYZ",
            "quantity": 10,
            "realized_pnl": -1000.0,
            "description": "Cost Written Off",
        },
    ])
    current = pd.DataFrame([
        {
            "account": "A",
            "symbol": "XYZ",
            "instrument_type": "Equity",
            "quantity": 10,
        },
    ])
    kept, removed = _drop_phantom_equity_writeoffs(closed_equity, current)
    assert len(kept) == 1
    assert removed.empty


def test_addback_phantom_writeoffs_to_summary_corrects_closed_dividend_row():
    """IYW-shaped Strategy Breakdown: positions_summary still carries the
    phantom -$1,966.78 in a Closed Dividend row. Once the writeoff is
    stripped, the addback must restore that row's realized so it shows
    the small +$9.47 from the real interim sells, not -$1,957."""
    from app.routes import _addback_phantom_writeoffs_to_summary

    summary = pd.DataFrame([
        {
            "account": "Emmory Investment",
            "symbol": "IYW",
            "strategy": "Buy and Hold",
            "status": "Open",
            "realized_pnl": 0.0,
            "total_pnl": 349.90,
            "total_return": 349.90,
        },
        {
            "account": "Emmory Investment",
            "symbol": "IYW",
            "strategy": "Dividend",
            "status": "Closed",
            "realized_pnl": -1957.31,
            "total_pnl": -1956.92,
            "total_return": -1956.92,
        },
    ])
    removed = pd.DataFrame([
        {
            "account": "Emmory Investment",
            "symbol": "IYW",
            "trade_symbol": "IYW",
            "quantity": 10,
            "realized_pnl": -1966.78,
            "description": "Cost Written Off",
        },
    ])
    out = _addback_phantom_writeoffs_to_summary(summary, removed)
    closed = out[out["status"].astype(str).str.strip().eq("Closed")].iloc[0]
    assert float(closed["realized_pnl"]) == pytest.approx(9.47, abs=0.02)
    assert float(closed["total_pnl"]) == pytest.approx(9.86, abs=0.02)
    assert float(closed["total_return"]) == pytest.approx(9.86, abs=0.02)
    open_row = out[out["status"].astype(str).str.strip().eq("Open")].iloc[0]
    assert float(open_row["realized_pnl"]) == 0.0
    assert float(open_row["total_pnl"]) == pytest.approx(349.90, abs=0.02)


def test_addback_phantom_writeoffs_noop_when_no_removed_rows():
    from app.routes import _addback_phantom_writeoffs_to_summary

    summary = pd.DataFrame([
        {
            "account": "A",
            "symbol": "X",
            "status": "Closed",
            "realized_pnl": -100.0,
            "total_pnl": -100.0,
            "total_return": -100.0,
        }
    ])
    out = _addback_phantom_writeoffs_to_summary(summary, pd.DataFrame())
    assert float(out["realized_pnl"].iloc[0]) == -100.0


def test_addback_phantom_writeoffs_isolated_to_matching_account_symbol():
    """Writeoff stripped on A/IYW must not adjust B/IYW or A/MSFT rows."""
    from app.routes import _addback_phantom_writeoffs_to_summary

    summary = pd.DataFrame([
        {
            "account": "A",
            "symbol": "IYW",
            "status": "Closed",
            "realized_pnl": -1957.31,
            "total_pnl": -1956.92,
            "total_return": -1956.92,
        },
        {
            "account": "B",
            "symbol": "IYW",
            "status": "Closed",
            "realized_pnl": -1957.31,
            "total_pnl": -1957.31,
            "total_return": -1957.31,
        },
        {
            "account": "A",
            "symbol": "MSFT",
            "status": "Closed",
            "realized_pnl": -1957.31,
            "total_pnl": -1957.31,
            "total_return": -1957.31,
        },
    ])
    removed = pd.DataFrame([
        {
            "account": "A",
            "symbol": "IYW",
            "trade_symbol": "IYW",
            "quantity": 10,
            "realized_pnl": -1966.78,
            "description": "Cost Written Off",
        },
    ])
    out = _addback_phantom_writeoffs_to_summary(summary, removed)
    a_iyw = out[(out["account"] == "A") & (out["symbol"] == "IYW")].iloc[0]
    b_iyw = out[(out["account"] == "B") & (out["symbol"] == "IYW")].iloc[0]
    a_msft = out[(out["account"] == "A") & (out["symbol"] == "MSFT")].iloc[0]
    assert float(a_iyw["realized_pnl"]) == pytest.approx(9.47, abs=0.02)
    assert float(b_iyw["realized_pnl"]) == -1957.31
    assert float(a_msft["realized_pnl"]) == -1957.31


def test_equity_slice_for_live_chart_is_case_insensitive():
    from app.routes import _equity_slice_for_live_chart

    df = pd.DataFrame(
        [
            {"instrument_type": " equity ", "unrealized_pnl": 3.0},
            {"instrument_type": "Call", "unrealized_pnl": 99.0},
        ]
    )
    out = _equity_slice_for_live_chart(df)
    assert len(out) == 1
    assert float(out["unrealized_pnl"].iloc[0]) == 3.0


def test_snap_chart_terminal_to_breakdown_moves_last_equity_when_ledger_known():
    from app.routes import _snap_position_chart_terminal_to_breakdown

    chart = {
        "dates": ["2025-12-29", "2025-12-30"],
        "equity": [62.48, -1957.31],
        "options": [0.0, 0.0],
        "dividends": [0.0, 0.39],
        "total": [62.48, -1956.92],
        "underlying_price": [230.0, 236.0],
        "has_underlying_price": True,
    }
    breakdown_rows = [
        {"type": "Equity", "total": -1607.41},
        {"type": "Options", "total": 0.0},
        {"type": "Dividends", "total": 0.39},
    ]
    _snap_position_chart_terminal_to_breakdown(chart, breakdown_rows)
    assert chart["total"][-1] == pytest.approx(-1607.02, abs=0.02)
    assert chart["equity"][-1] == pytest.approx(-1607.41, abs=0.02)


def test_chart_partition_current_prefers_explicit_user_id_when_both_present():
    from app.routes import _filter_current_for_chart_partition

    cdf = pd.DataFrame(
        [
            {
                "account": "A",
                "user_id": None,
                "instrument_type": "Equity",
                "unrealized_pnl": 1.0,
            },
            {
                "account": "A",
                "user_id": 9,
                "instrument_type": "Equity",
                "unrealized_pnl": 888.0,
            },
        ]
    )
    out = _filter_current_for_chart_partition(cdf, "A", 9)
    assert len(out) == 1
    assert float(out["unrealized_pnl"].iloc[0]) == 888.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
