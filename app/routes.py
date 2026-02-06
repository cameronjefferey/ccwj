from flask import render_template, request
from app import app
from app.bigquery_client import get_bigquery_client
import pandas as pd


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html", title="Home")


@app.route("/ping")
def ping():
    return "Flask app is alive"


@app.route("/positions")
def positions():
    client = get_bigquery_client()

    # ------------------------------------------------------------------
    # 1. Load positions_summary from BigQuery
    # ------------------------------------------------------------------
    query = """
        SELECT *
        FROM `ccwj-dbt.analytics.positions_summary`
        ORDER BY account, symbol, strategy
    """
    try:
        df = client.query(query).to_dataframe()
    except Exception as exc:
        return render_template(
            "positions.html",
            error=str(exc),
            rows=[],
            kpis={},
            strategy_chart=[],
            accounts=[],
            strategies=[],
            selected_account="",
            selected_strategy="",
            selected_status="",
        )

    # ------------------------------------------------------------------
    # 2. Clean up types
    # ------------------------------------------------------------------
    numeric_cols = [
        "total_pnl", "realized_pnl", "unrealized_pnl",
        "total_premium_received", "total_premium_paid",
        "num_trade_groups", "num_individual_trades",
        "num_winners", "num_losers", "win_rate",
        "avg_pnl_per_trade", "avg_days_in_trade",
        "total_dividend_income", "dividend_count", "total_return",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in ["first_trade_date", "last_trade_date"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("NaT", "")

    # ------------------------------------------------------------------
    # 3. Filter options (computed before filtering)
    # ------------------------------------------------------------------
    accounts = sorted(df["account"].dropna().unique())
    strategies = sorted(df["strategy"].dropna().unique())

    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")
    selected_status = request.args.get("status", "")

    filtered = df.copy()
    if selected_account:
        filtered = filtered[filtered["account"] == selected_account]
    if selected_strategy:
        filtered = filtered[filtered["strategy"] == selected_strategy]
    if selected_status:
        filtered = filtered[filtered["status"] == selected_status]

    # ------------------------------------------------------------------
    # 4. KPIs
    # ------------------------------------------------------------------
    total_winners = int(filtered["num_winners"].sum())
    total_losers = int(filtered["num_losers"].sum())
    total_closed = total_winners + total_losers

    kpis = {
        "total_return": float(filtered["total_return"].sum()),
        "realized_pnl": float(filtered["realized_pnl"].sum()),
        "unrealized_pnl": float(filtered["unrealized_pnl"].sum()),
        "premium_collected": float(filtered["total_premium_received"].sum()),
        "win_rate": total_winners / total_closed if total_closed else 0,
        "num_positions": len(filtered),
        "total_trades": int(filtered["num_individual_trades"].sum()),
    }

    # ------------------------------------------------------------------
    # 5. Chart data: total P&L by strategy
    # ------------------------------------------------------------------
    strategy_chart = (
        filtered.groupby("strategy")["total_pnl"]
        .sum()
        .sort_values(ascending=True)
        .reset_index()
        .rename(columns={"total_pnl": "pnl"})
        .to_dict(orient="records")
    )

    # ------------------------------------------------------------------
    # 6. Table rows
    # ------------------------------------------------------------------
    rows = filtered.to_dict(orient="records")

    return render_template(
        "positions.html",
        rows=rows,
        kpis=kpis,
        strategy_chart=strategy_chart,
        accounts=accounts,
        strategies=strategies,
        selected_account=selected_account,
        selected_strategy=selected_strategy,
        selected_status=selected_status,
    )
