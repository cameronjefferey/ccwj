"""
"If You Did Nothing" Benchmark — Strategy-driven comparison to buy-and-hold.

For each strategy (Covered Call, Wheel, CSP, etc.), compares your actual P&L
to what you would have made by simply buying and holding the same stocks.
"""
from datetime import date, timedelta
from flask import render_template
from flask_login import login_required, current_user

from app import app
from app.routes import (
    get_bigquery_client,
    _account_sql_and,
    _filter_df_by_accounts,
)
from app.auth import get_accounts_for_user


# Query positions_summary for benchmark
BENCHMARK_POSITIONS_QUERY = """
    SELECT
        account,
        symbol,
        strategy,
        status,
        total_return,
        realized_pnl,
        unrealized_pnl,
        first_trade_date,
        last_trade_date
    FROM `ccwj-dbt.analytics.positions_summary`
    WHERE 1=1 {account_filter}
    ORDER BY strategy, symbol
"""

# Equity cost (capital deployed) per (account, symbol) in a date range
EQUITY_COST_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        SUM(ABS(amount)) AS equity_cost
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE instrument_type = 'Equity'
      AND action = 'equity_buy'
      AND amount < 0
      AND trade_date >= @start_date
      AND trade_date <= @end_date
    GROUP BY 1, 2
"""


def _fetch_price_at_date(symbol, d):
    """Fetch closing price for symbol on date d (or nearest prior trading day)."""
    try:
        import yfinance as yf
        d_str = d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=d_str, end=d_str, auto_adjust=True)
        if not hist.empty:
            return float(hist["Close"].iloc[0])
        # Try a window to get nearest prior day
        start = (d - timedelta(days=14)) if hasattr(d, "day") else d
        hist = ticker.history(start=start, end=d_str, auto_adjust=True)
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return None


def _compute_hold_pnl(positions_df, equity_cost_df):
    """For each position, compute hold P&L = capital * (exit_price - entry_price) / entry_price."""
    import pandas as pd

    symbols = positions_df["symbol"].unique().tolist()
    price_cache = {}  # (symbol, date_key) -> price

    def get_price(sym, d):
        if pd.isna(d):
            return None
        dt = d.date() if hasattr(d, "date") and callable(getattr(d, "date")) else d
        dkey = str(dt)[:10]
        key = (sym, dkey)
        if key not in price_cache:
            price_cache[key] = _fetch_price_at_date(sym, dt)
        return price_cache[key]

    # Build equity cost map: (account, symbol) -> cost
    cost_map = {}
    if not equity_cost_df.empty:
        for _, row in equity_cost_df.iterrows():
            cost_map[(str(row["account"]), str(row["symbol"]))] = float(row["equity_cost"] or 0)

    results = []
    for _, row in positions_df.iterrows():
        acc = str(row["account"])
        sym = str(row["symbol"])
        first_d = row["first_trade_date"]
        last_d = row["last_trade_date"]
        your_pnl = float(row["total_return"] or 0)
        capital = cost_map.get((acc, sym), 0)

        entry_price = get_price(sym, first_d) if first_d else None
        exit_price = get_price(sym, last_d) if last_d else None

        hold_pnl = None
        hold_return_pct = None
        if entry_price and exit_price and entry_price > 0:
            hold_return_pct = (exit_price - entry_price) / entry_price
            if capital > 0:
                hold_pnl = capital * hold_return_pct

        results.append({
            "account": acc,
            "symbol": sym,
            "strategy": row["strategy"],
            "status": row["status"],
            "your_pnl": your_pnl,
            "hold_pnl": hold_pnl,
            "hold_return_pct": hold_return_pct * 100 if hold_return_pct is not None else None,
            "capital": capital,
            "entry_price": entry_price,
            "exit_price": exit_price,
        })

    return results


def _aggregate_by_strategy(position_results):
    """Aggregate position-level results by strategy."""
    from collections import defaultdict
    by_strategy = defaultdict(lambda: {"your_pnl": 0, "hold_pnl": 0, "positions": [], "hold_denom": 0})

    for r in position_results:
        s = r["strategy"]
        by_strategy[s]["your_pnl"] += r["your_pnl"]
        if r["hold_pnl"] is not None:
            by_strategy[s]["hold_pnl"] += r["hold_pnl"]
            by_strategy[s]["hold_denom"] += 1
        by_strategy[s]["positions"].append(r)

    out = []
    for strategy, data in sorted(by_strategy.items()):
        diff = None
        if data["hold_denom"] > 0:
            diff = data["your_pnl"] - data["hold_pnl"]
        out.append({
            "strategy": strategy,
            "your_pnl": round(data["your_pnl"], 2),
            "hold_pnl": round(data["hold_pnl"], 2) if data["hold_denom"] > 0 else None,
            "difference": round(diff, 2) if diff is not None else None,
            "num_positions": len(data["positions"]),
            "positions": data["positions"],
        })
    return out


@app.route("/benchmark")
@login_required
def benchmark():
    """Benchmark page: strategy-driven 'what if you didn't trade options?' comparison."""
    user_accounts = get_accounts_for_user(current_user.id)
    if not user_accounts:
        return render_template(
            "benchmark.html",
            title="If You Did Nothing",
            strategy_data=[],
            error="Upload your trade history to see the benchmark.",
        )

    try:
        client = get_bigquery_client()
        account_filter = _account_sql_and(user_accounts)

        positions_df = client.query(
            BENCHMARK_POSITIONS_QUERY.format(account_filter=account_filter)
        ).to_dataframe()

        if positions_df.empty:
            return render_template(
                "benchmark.html",
                title="If You Did Nothing",
                strategy_data=[],
                error="No positions found. Upload your Schwab trade history to get started.",
            )

        positions_df = _filter_df_by_accounts(positions_df, user_accounts)
        if positions_df.empty:
            return render_template(
                "benchmark.html",
                title="If You Did Nothing",
                strategy_data=[],
                error="No positions in your accounts.",
            )

        # Parse dates
        for col in ["first_trade_date", "last_trade_date"]:
            if col in positions_df.columns:
                positions_df[col] = positions_df[col].apply(
                    lambda x: x.date() if hasattr(x, "date") and callable(getattr(x, "date")) else x
                )

        # Date range for equity cost query
        start_d = positions_df["first_trade_date"].min()
        end_d = positions_df["last_trade_date"].max()
        if start_d is None or end_d is None:
            start_d = date(2020, 1, 1)
            end_d = date.today()

        start_str = str(start_d)[:10] if start_d else "2020-01-01"
        end_str = str(end_d)[:10] if end_d else str(date.today())[:10]

        equity_cost_df = positions_df.iloc[0:0]
        try:
            q = EQUITY_COST_QUERY.replace("@start_date", f"'{start_str}'").replace("@end_date", f"'{end_str}'")
            equity_cost_df = client.query(q).to_dataframe()
            equity_cost_df = _filter_df_by_accounts(equity_cost_df, user_accounts)
        except Exception:
            pass

        # Compute hold P&L per position
        position_results = _compute_hold_pnl(positions_df, equity_cost_df)

        # Aggregate by strategy
        strategy_data = _aggregate_by_strategy(position_results)

        return render_template(
            "benchmark.html",
            title="If You Did Nothing",
            strategy_data=strategy_data,
            error=None,
        )

    except Exception as e:
        return render_template(
            "benchmark.html",
            title="If You Did Nothing",
            strategy_data=[],
            error=f"Could not load benchmark: {str(e)}",
        )
