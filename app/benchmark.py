"""
"If You Did Nothing" Benchmark — Strategy-driven comparison to buy-and-hold.

Reads pre-computed hold P&L from mart_benchmark (dbt), which uses close prices
from the yfinance pipeline (stg_daily_prices). Zero live API calls in this route.
"""
from collections import defaultdict
from flask import render_template
from flask_login import login_required, current_user

from app import app
from app.routes import get_bigquery_client, _account_sql_and, _filter_df_by_accounts
from app.auth import get_accounts_for_user


BENCHMARK_QUERY = """
    SELECT
        account,
        symbol,
        strategy,
        status,
        your_pnl,
        capital,
        entry_price,
        exit_price,
        hold_return_pct,
        hold_pnl
    FROM `ccwj-dbt.analytics.mart_benchmark`
    WHERE 1=1 {account_filter}
    ORDER BY strategy, symbol
"""


def _aggregate_by_strategy(rows):
    """Aggregate position-level results by strategy."""
    by_strategy = defaultdict(lambda: {"your_pnl": 0, "hold_pnl": 0, "positions": [], "hold_denom": 0})

    for r in rows:
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

        df = client.query(
            BENCHMARK_QUERY.format(account_filter=account_filter)
        ).to_dataframe()

        if df.empty:
            return render_template(
                "benchmark.html",
                title="If You Did Nothing",
                strategy_data=[],
                error="No positions found. Upload your Schwab trade history to get started.",
            )

        df = _filter_df_by_accounts(df, user_accounts)
        if df.empty:
            return render_template(
                "benchmark.html",
                title="If You Did Nothing",
                strategy_data=[],
                error="No positions in your accounts.",
            )

        # Convert to list of dicts
        rows = []
        for _, row in df.iterrows():
            hold_pnl = float(row["hold_pnl"]) if row["hold_pnl"] is not None and str(row["hold_pnl"]) != "nan" else None
            rows.append({
                "account": str(row["account"]),
                "symbol": str(row["symbol"]),
                "strategy": str(row["strategy"]),
                "status": str(row["status"]),
                "your_pnl": float(row["your_pnl"] or 0),
                "hold_pnl": hold_pnl,
                "hold_return_pct": float(row["hold_return_pct"]) if row["hold_return_pct"] is not None and str(row["hold_return_pct"]) != "nan" else None,
                "capital": float(row["capital"] or 0),
                "entry_price": float(row["entry_price"]) if row["entry_price"] is not None and str(row["entry_price"]) != "nan" else None,
                "exit_price": float(row["exit_price"]) if row["exit_price"] is not None and str(row["exit_price"]) != "nan" else None,
            })

        strategy_data = _aggregate_by_strategy(rows)

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
