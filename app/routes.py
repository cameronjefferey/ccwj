from flask import render_template, request
from app import app
from app.bigquery_client import get_bigquery_client
from google.cloud import bigquery
from datetime import datetime, date
import pandas as pd
import json


# ------------------------------------------------------------------
# SQL: date-filtered re-aggregation of positions_summary
# ------------------------------------------------------------------
DATE_FILTERED_QUERY = """
WITH classified AS (
    SELECT *
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE open_date <= @end_date
      AND COALESCE(close_date, CURRENT_DATE()) >= @start_date
),

dividends AS (
    SELECT
        account,
        underlying_symbol AS symbol,
        SUM(amount) AS total_dividend_income,
        COUNT(*) AS dividend_count
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE action = 'dividend'
      AND trade_date >= @start_date
      AND trade_date <= @end_date
    GROUP BY 1, 2
),

strategy_summary AS (
    SELECT
        account,
        symbol,
        strategy,

        CASE
            WHEN COUNTIF(status = 'Open') > 0 AND COUNTIF(status = 'Closed') > 0 THEN 'Mixed'
            WHEN COUNTIF(status = 'Open') > 0 THEN 'Open'
            ELSE 'Closed'
        END AS status,

        SUM(total_pnl) AS total_pnl,
        SUM(CASE WHEN status = 'Closed' THEN total_pnl ELSE 0 END) AS realized_pnl,
        SUM(CASE WHEN status = 'Open'   THEN total_pnl ELSE 0 END) AS unrealized_pnl,

        SUM(premium_received) AS total_premium_received,
        SUM(ABS(premium_paid)) AS total_premium_paid,

        COUNT(*) AS num_trade_groups,
        SUM(num_trades) AS num_individual_trades,
        COUNTIF(is_winner AND status = 'Closed') AS num_winners,
        COUNTIF(NOT is_winner AND status = 'Closed') AS num_losers,

        SAFE_DIVIDE(
            COUNTIF(is_winner AND status = 'Closed'),
            NULLIF(COUNTIF(status = 'Closed'), 0)
        ) AS win_rate,

        SAFE_DIVIDE(
            SUM(CASE WHEN status = 'Closed' THEN total_pnl ELSE 0 END),
            NULLIF(COUNTIF(status = 'Closed'), 0)
        ) AS avg_pnl_per_trade,

        ROUND(AVG(days_in_trade), 1) AS avg_days_in_trade,
        MIN(open_date) AS first_trade_date,
        MAX(COALESCE(close_date, CURRENT_DATE())) AS last_trade_date

    FROM classified
    GROUP BY 1, 2, 3
),

with_dividend_rank AS (
    SELECT
        ss.*,
        ROW_NUMBER() OVER (
            PARTITION BY ss.account, ss.symbol
            ORDER BY
                CASE ss.strategy
                    WHEN 'Wheel'        THEN 1
                    WHEN 'Covered Call'  THEN 2
                    WHEN 'Buy and Hold'  THEN 3
                    ELSE 99
                END
        ) AS dividend_rank
    FROM strategy_summary ss
),

final AS (
    SELECT
        wdr.account,
        wdr.symbol,
        wdr.strategy,
        wdr.status,
        ROUND(wdr.total_pnl, 2) AS total_pnl,
        ROUND(wdr.realized_pnl, 2) AS realized_pnl,
        ROUND(wdr.unrealized_pnl, 2) AS unrealized_pnl,
        ROUND(wdr.total_premium_received, 2) AS total_premium_received,
        ROUND(wdr.total_premium_paid, 2) AS total_premium_paid,
        wdr.num_trade_groups,
        wdr.num_individual_trades,
        wdr.num_winners,
        wdr.num_losers,
        ROUND(wdr.win_rate, 4) AS win_rate,
        ROUND(wdr.avg_pnl_per_trade, 2) AS avg_pnl_per_trade,
        wdr.avg_days_in_trade,
        wdr.first_trade_date,
        wdr.last_trade_date,
        CASE WHEN wdr.dividend_rank = 1
            THEN ROUND(COALESCE(d.total_dividend_income, 0), 2)
            ELSE 0
        END AS total_dividend_income,
        CASE WHEN wdr.dividend_rank = 1
            THEN COALESCE(d.dividend_count, 0)
            ELSE 0
        END AS dividend_count,
        ROUND(
            wdr.total_pnl
            + CASE WHEN wdr.dividend_rank = 1 THEN COALESCE(d.total_dividend_income, 0) ELSE 0 END
        , 2) AS total_return
    FROM with_dividend_rank wdr
    LEFT JOIN dividends d
        ON wdr.account = d.account
        AND wdr.symbol = d.symbol
)

SELECT * FROM final
ORDER BY account, symbol, strategy
"""

# ------------------------------------------------------------------
# Default (no date filter): use the pre-built mart
# ------------------------------------------------------------------
DEFAULT_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.positions_summary`
    ORDER BY account, symbol, strategy
"""

ERROR_DEFAULTS = dict(
    error="",
    rows=[],
    symbol_rows=[],
    kpis={},
    strategy_chart=[],
    accounts=[],
    strategies=[],
    symbols=[],
    selected_account="",
    selected_strategy="",
    selected_status="",
    selected_symbol="",
    selected_start_date="",
    selected_end_date="",
    date_filtered=False,
)


def _parse_date(value):
    """Return a date object if value is a valid YYYY-MM-DD string, else None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


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
    # 1. Read filter params
    # ------------------------------------------------------------------
    selected_account = request.args.get("account", "")
    selected_strategy = request.args.get("strategy", "")
    selected_status = request.args.get("status", "")
    selected_symbol = request.args.get("symbol", "")
    selected_start_date = request.args.get("start_date", "")
    selected_end_date = request.args.get("end_date", "")

    start_date = _parse_date(selected_start_date)
    end_date = _parse_date(selected_end_date)
    date_filtered = start_date is not None or end_date is not None

    # ------------------------------------------------------------------
    # 2. Query BigQuery
    # ------------------------------------------------------------------
    try:
        if date_filtered:
            # Fill open boundaries with wide defaults
            effective_start = start_date or date(2000, 1, 1)
            effective_end = end_date or date.today()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", effective_start),
                    bigquery.ScalarQueryParameter("end_date", "DATE", effective_end),
                ]
            )
            df = client.query(DATE_FILTERED_QUERY, job_config=job_config).to_dataframe()
        else:
            df = client.query(DEFAULT_QUERY).to_dataframe()
    except Exception as exc:
        ctx = dict(ERROR_DEFAULTS)
        ctx["error"] = str(exc)
        return render_template("positions.html", **ctx)

    # ------------------------------------------------------------------
    # 3. Clean up types
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
    # 4. Filter options (computed before client-side filtering)
    # ------------------------------------------------------------------
    accounts = sorted(df["account"].dropna().unique())
    strategies = sorted(df["strategy"].dropna().unique())
    symbols = sorted(df["symbol"].dropna().unique())

    filtered = df.copy()
    if selected_account:
        filtered = filtered[filtered["account"] == selected_account]
    if selected_strategy:
        filtered = filtered[filtered["strategy"] == selected_strategy]
    if selected_status:
        filtered = filtered[filtered["status"] == selected_status]
    if selected_symbol:
        filtered = filtered[filtered["symbol"] == selected_symbol]

    # ------------------------------------------------------------------
    # 5. KPIs
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
    # 6. Chart data: total P&L by strategy
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
    # 7. Symbol-level summary (grouped by account + symbol)
    # ------------------------------------------------------------------
    if not filtered.empty:
        symbol_agg = (
            filtered.groupby(["account", "symbol"])
            .agg(
                total_pnl=("total_pnl", "sum"),
                realized_pnl=("realized_pnl", "sum"),
                unrealized_pnl=("unrealized_pnl", "sum"),
                total_premium_received=("total_premium_received", "sum"),
                total_dividend_income=("total_dividend_income", "sum"),
                total_return=("total_return", "sum"),
                num_individual_trades=("num_individual_trades", "sum"),
                num_winners=("num_winners", "sum"),
                num_losers=("num_losers", "sum"),
                num_strategies=("strategy", "nunique"),
                strategies=("strategy", lambda x: ", ".join(sorted(x.unique()))),
            )
            .reset_index()
        )
        closed = symbol_agg["num_winners"] + symbol_agg["num_losers"]
        symbol_agg["win_rate"] = symbol_agg["num_winners"] / closed.replace(0, pd.NA)
        symbol_agg["win_rate"] = symbol_agg["win_rate"].fillna(0)
        symbol_agg = symbol_agg.sort_values("total_return", ascending=False)
        symbol_rows = symbol_agg.to_dict(orient="records")
    else:
        symbol_rows = []

    # ------------------------------------------------------------------
    # 8. Strategy detail rows
    # ------------------------------------------------------------------
    rows = filtered.to_dict(orient="records")

    return render_template(
        "positions.html",
        rows=rows,
        symbol_rows=symbol_rows,
        kpis=kpis,
        strategy_chart=strategy_chart,
        accounts=accounts,
        strategies=strategies,
        symbols=symbols,
        selected_account=selected_account,
        selected_strategy=selected_strategy,
        selected_status=selected_status,
        selected_symbol=selected_symbol,
        selected_start_date=selected_start_date,
        selected_end_date=selected_end_date,
        date_filtered=date_filtered,
    )


# ======================================================================
# Daily Position Detail  (/symbols)
# ======================================================================

TRADES_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        trade_date,
        action,
        action_raw,
        trade_symbol,
        instrument_type,
        description,
        quantity,
        price,
        fees,
        amount
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE underlying_symbol IS NOT NULL
      AND trade_date IS NOT NULL
    ORDER BY underlying_symbol, trade_date
"""

CURRENT_POSITIONS_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        instrument_type,
        trade_symbol,
        description,
        quantity,
        current_price,
        market_value,
        cost_basis,
        unrealized_pnl,
        unrealized_pnl_pct
    FROM `ccwj-dbt.analytics.stg_current`
"""

STRATEGIES_MAP_QUERY = """
    SELECT account, symbol, strategy
    FROM `ccwj-dbt.analytics.positions_summary`
"""

def _compute_equity_pnl(equity_trades):
    """
    Compute realized P&L for equity trades using average cost method.

    Only SELL events produce P&L.  Buy events simply update the cost basis.
    Returns a list of (trade_date, pnl) tuples.
    """
    if equity_trades.empty:
        return []

    shares_held = 0.0
    total_cost = 0.0
    pnl_events = []

    for _, row in equity_trades.sort_values("trade_date").iterrows():
        action = row["action"]
        qty = abs(float(row["quantity"])) if row["quantity"] else 0
        amount = float(row["amount"])
        trade_date = row["trade_date"]

        if qty == 0:
            continue

        if action == "equity_buy":
            # Cash out → increase position at cost = abs(amount)
            shares_held += qty
            total_cost += abs(amount)

        elif action in ("equity_sell", "equity_sell_short"):
            if shares_held > 0:
                avg_cost = total_cost / shares_held
                shares_sold = min(qty, shares_held)
                cost_basis = avg_cost * shares_sold
                pnl = amount - cost_basis  # amount is positive (proceeds)

                total_cost = max(0.0, total_cost - cost_basis)
                shares_held = max(0.0, shares_held - shares_sold)
            else:
                # No long position (short sale or data gap)
                pnl = amount

            pnl_events.append((trade_date, round(pnl, 2)))

    return pnl_events


def _build_chart_data(group, sym_current):
    """
    Build daily cumulative P&L chart data for one (account, symbol) group.

    - Equity:    P&L computed via average-cost method (only sells generate P&L).
    - Options:   amount IS the P&L (premiums received/paid).
    - Dividends: amount IS the P&L (cash received).
    """
    # Split trades by type
    equity_trades = group[group["instrument_type"] == "Equity"]
    option_trades = group[group["instrument_type"].isin(["Call", "Put"])]
    dividend_trades = group[group["instrument_type"] == "Dividend"]
    other_trades = group[~group["instrument_type"].isin(["Equity", "Call", "Put", "Dividend"])]

    # Equity: realized P&L per sell event
    equity_pnl_events = _compute_equity_pnl(equity_trades)
    equity_by_date = {}
    for d, pnl in equity_pnl_events:
        equity_by_date[d] = equity_by_date.get(d, 0) + pnl

    # Options / dividends / other: daily sum of amounts
    def _daily_sums(df):
        if df.empty:
            return {}
        return df.groupby("trade_date")["amount"].sum().to_dict()

    option_by_date = _daily_sums(option_trades)
    dividend_by_date = _daily_sums(dividend_trades)
    other_by_date = _daily_sums(other_trades)

    # Union of all dates
    all_dates = set()
    all_dates.update(equity_by_date)
    all_dates.update(option_by_date)
    all_dates.update(dividend_by_date)
    all_dates.update(other_by_date)

    if not all_dates:
        return {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}

    sorted_dates = sorted(all_dates)

    # Build cumulative series
    eq_cum = opt_cum = div_cum = oth_cum = 0.0
    equity_series, options_series, dividends_series, total_series = [], [], [], []

    for d in sorted_dates:
        eq_cum += equity_by_date.get(d, 0)
        opt_cum += option_by_date.get(d, 0)
        div_cum += dividend_by_date.get(d, 0)
        oth_cum += other_by_date.get(d, 0)

        equity_series.append(round(eq_cum, 2))
        options_series.append(round(opt_cum, 2))
        dividends_series.append(round(div_cum, 2))
        total_series.append(round(eq_cum + opt_cum + div_cum + oth_cum, 2))

    # Append today with unrealized P&L for open positions
    if not sym_current.empty:
        eq_unreal = float(sym_current.loc[sym_current["instrument_type"] == "Equity", "unrealized_pnl"].sum())
        opt_unreal = float(sym_current.loc[sym_current["instrument_type"].isin(["Call", "Put"]), "unrealized_pnl"].sum())
        unrealized_total = eq_unreal + opt_unreal

        if unrealized_total != 0 and sorted_dates:
            today = date.today()
            if sorted_dates[-1] != today:
                sorted_dates.append(today)
                equity_series.append(round(equity_series[-1] + eq_unreal, 2))
                options_series.append(round(options_series[-1] + opt_unreal, 2))
                dividends_series.append(dividends_series[-1])
                total_series.append(round(total_series[-1] + unrealized_total, 2))
            else:
                equity_series[-1] = round(equity_series[-1] + eq_unreal, 2)
                options_series[-1] = round(options_series[-1] + opt_unreal, 2)
                total_series[-1] = round(total_series[-1] + unrealized_total, 2)

    return {
        "dates": [str(d) for d in sorted_dates],
        "equity": equity_series,
        "options": options_series,
        "dividends": dividends_series,
        "total": total_series,
    }


@app.route("/symbols")
def symbols_detail():
    client = get_bigquery_client()

    try:
        trades_df = client.query(TRADES_QUERY).to_dataframe()
        current_df = client.query(CURRENT_POSITIONS_QUERY).to_dataframe()
        strat_df = client.query(STRATEGIES_MAP_QUERY).to_dataframe()
    except Exception as exc:
        return render_template(
            "symbols.html",
            error=str(exc),
            symbol_data=[],
            chart_data_json="[]",
            accounts=[],
            selected_account="",
        )

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    for col in ["amount", "quantity", "price", "fees"]:
        trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
    trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)
    if "unrealized_pnl_pct" in current_df.columns:
        current_df["unrealized_pnl_pct"] = pd.to_numeric(current_df["unrealized_pnl_pct"], errors="coerce").fillna(0)

    # Strategy map: (account, symbol) → sorted list of strategies
    strat_map = (
        strat_df.groupby(["account", "symbol"])["strategy"]
        .apply(lambda x: sorted(x.unique().tolist()))
        .to_dict()
    )

    # ------------------------------------------------------------------
    # Account filter
    # ------------------------------------------------------------------
    accounts = sorted(trades_df["account"].dropna().unique())
    selected_account = request.args.get("account", "")

    if selected_account:
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]

    # ------------------------------------------------------------------
    # Build per-symbol data
    # ------------------------------------------------------------------
    symbol_data = []
    chart_data_list = []

    for (account, symbol), group in trades_df.groupby(["account", "symbol"]):
        group = group.sort_values("trade_date")

        # Current positions for this symbol
        sym_current = current_df[
            (current_df["account"] == account) & (current_df["symbol"] == symbol)
        ]

        total_realized = round(float(group["amount"].sum()), 2)
        unrealized = round(float(sym_current["unrealized_pnl"].sum()), 2) if not sym_current.empty else 0.0
        total_return = round(total_realized + unrealized, 2)
        num_trades = len(group)
        first_date = str(group["trade_date"].min())
        last_date = str(group["trade_date"].max())
        strategies = strat_map.get((account, symbol), [])

        # Chart data
        chart = _build_chart_data(group, sym_current)
        chart_data_list.append(chart)

        # Trade table rows (convert dates to str for Jinja)
        trades_table = group.copy()
        trades_table["trade_date"] = trades_table["trade_date"].astype(str)
        trades_list = trades_table.to_dict(orient="records")

        # Current positions table
        current_list = sym_current.to_dict(orient="records") if not sym_current.empty else []

        symbol_data.append({
            "account": account,
            "symbol": symbol,
            "total_realized": total_realized,
            "unrealized": unrealized,
            "total_return": total_return,
            "num_trades": num_trades,
            "first_date": first_date,
            "last_date": last_date,
            "strategies": strategies,
            "trades": trades_list,
            "current_positions": current_list,
            "_chart_idx": len(chart_data_list) - 1,
        })

    # Sort by total return descending; rebuild chart list in matching order
    symbol_data.sort(key=lambda x: x["total_return"], reverse=True)
    sorted_charts = [chart_data_list[item["_chart_idx"]] for item in symbol_data]
    for item in symbol_data:
        del item["_chart_idx"]

    return render_template(
        "symbols.html",
        symbol_data=symbol_data,
        chart_data_json=json.dumps(sorted_charts),
        accounts=accounts,
        selected_account=selected_account,
    )


# ======================================================================
# Account Performance  (/accounts)
# ======================================================================

ACCOUNT_BALANCES_QUERY = """
    SELECT account, row_type, market_value, cost_basis,
           unrealized_pnl, unrealized_pnl_pct, percent_of_account
    FROM `ccwj-dbt.analytics.stg_account_balances`
"""

STRATEGY_CLASSIFICATION_QUERY = """
    SELECT account, symbol, strategy, status, open_date, close_date,
           total_pnl, num_trades
    FROM `ccwj-dbt.analytics.int_strategy_classification`
"""

ACCOUNT_POSITIONS_SUMMARY_QUERY = """
    SELECT account, strategy,
           SUM(total_pnl) AS total_pnl,
           SUM(realized_pnl) AS realized_pnl,
           SUM(unrealized_pnl) AS unrealized_pnl,
           SUM(total_premium_received) AS premium_received,
           SUM(total_premium_paid) AS premium_paid,
           SUM(num_individual_trades) AS num_trades,
           SUM(num_winners) AS num_winners,
           SUM(num_losers) AS num_losers,
           SUM(total_dividend_income) AS dividend_income,
           SUM(total_return) AS total_return
    FROM `ccwj-dbt.analytics.positions_summary`
    GROUP BY account, strategy
    ORDER BY account, strategy
"""


def _build_account_summary_chart(trades_df, current_df):
    """
    Build account-level cumulative P&L chart (Equity / Options / Dividends / Total).
    Same logic as _build_chart_data but across ALL symbols in the account.
    """
    equity_trades = trades_df[trades_df["instrument_type"] == "Equity"]
    option_trades = trades_df[trades_df["instrument_type"].isin(["Call", "Put"])]
    dividend_trades = trades_df[trades_df["instrument_type"] == "Dividend"]
    other_trades = trades_df[~trades_df["instrument_type"].isin(["Equity", "Call", "Put", "Dividend"])]

    # Equity: compute P&L per (account, symbol) using avg cost, then merge
    equity_by_date = {}
    if not equity_trades.empty:
        for (_, symbol), grp in equity_trades.groupby(["account", "symbol"]):
            for d, pnl in _compute_equity_pnl(grp):
                equity_by_date[d] = equity_by_date.get(d, 0) + pnl

    def _daily_sums(df):
        if df.empty:
            return {}
        return df.groupby("trade_date")["amount"].sum().to_dict()

    option_by_date = _daily_sums(option_trades)
    dividend_by_date = _daily_sums(dividend_trades)
    other_by_date = _daily_sums(other_trades)

    all_dates = set()
    all_dates.update(equity_by_date)
    all_dates.update(option_by_date)
    all_dates.update(dividend_by_date)
    all_dates.update(other_by_date)

    if not all_dates:
        return {"dates": [], "equity": [], "options": [], "dividends": [], "total": []}

    sorted_dates = sorted(all_dates)
    eq_cum = opt_cum = div_cum = oth_cum = 0.0
    equity_s, options_s, dividends_s, total_s = [], [], [], []

    for d in sorted_dates:
        eq_cum += equity_by_date.get(d, 0)
        opt_cum += option_by_date.get(d, 0)
        div_cum += dividend_by_date.get(d, 0)
        oth_cum += other_by_date.get(d, 0)
        equity_s.append(round(eq_cum, 2))
        options_s.append(round(opt_cum, 2))
        dividends_s.append(round(div_cum, 2))
        total_s.append(round(eq_cum + opt_cum + div_cum + oth_cum, 2))

    # Append unrealized to today
    if not current_df.empty:
        eq_unreal = float(current_df.loc[current_df["instrument_type"] == "Equity", "unrealized_pnl"].sum())
        opt_unreal = float(current_df.loc[current_df["instrument_type"].isin(["Call", "Put"]), "unrealized_pnl"].sum())
        total_unreal = eq_unreal + opt_unreal
        if total_unreal != 0 and sorted_dates:
            today = date.today()
            if sorted_dates[-1] != today:
                sorted_dates.append(today)
                equity_s.append(round(equity_s[-1] + eq_unreal, 2))
                options_s.append(round(options_s[-1] + opt_unreal, 2))
                dividends_s.append(dividends_s[-1])
                total_s.append(round(total_s[-1] + total_unreal, 2))
            else:
                equity_s[-1] = round(equity_s[-1] + eq_unreal, 2)
                options_s[-1] = round(options_s[-1] + opt_unreal, 2)
                total_s[-1] = round(total_s[-1] + total_unreal, 2)

    return {
        "dates": [str(d) for d in sorted_dates],
        "equity": equity_s,
        "options": options_s,
        "dividends": dividends_s,
        "total": total_s,
    }


def _build_strategy_time_chart(strat_df):
    """
    Build cumulative P&L over time per strategy from trade-group data.
    Closed groups → P&L attributed to close_date.
    Open groups   → P&L attributed to today.
    """
    if strat_df.empty:
        return {"dates": [], "series": {}}

    today = date.today()
    rows = []
    for _, r in strat_df.iterrows():
        pnl_date = r["close_date"] if r["status"] == "Closed" and pd.notna(r["close_date"]) else today
        rows.append({"strategy": r["strategy"], "pnl_date": pnl_date, "pnl": float(r["total_pnl"])})

    events = pd.DataFrame(rows)
    events["pnl_date"] = pd.to_datetime(events["pnl_date"]).dt.date

    # Sum P&L per (strategy, date)
    grouped = events.groupby(["strategy", "pnl_date"])["pnl"].sum().reset_index()
    strategies = sorted(grouped["strategy"].unique())
    all_dates = sorted(grouped["pnl_date"].unique())

    series = {}
    for strat in strategies:
        strat_data = grouped[grouped["strategy"] == strat].set_index("pnl_date")["pnl"]
        cum = 0.0
        vals = []
        for d in all_dates:
            cum += float(strat_data.get(d, 0))
            vals.append(round(cum, 2))
        series[strat] = vals

    return {
        "dates": [str(d) for d in all_dates],
        "series": series,
    }


@app.route("/accounts")
def accounts():
    client = get_bigquery_client()

    try:
        balances_df = client.query(ACCOUNT_BALANCES_QUERY).to_dataframe()
        trades_df = client.query(TRADES_QUERY).to_dataframe()
        current_df = client.query(CURRENT_POSITIONS_QUERY).to_dataframe()
        strat_class_df = client.query(STRATEGY_CLASSIFICATION_QUERY).to_dataframe()
        strat_summary_df = client.query(ACCOUNT_POSITIONS_SUMMARY_QUERY).to_dataframe()
    except Exception as exc:
        return render_template(
            "accounts.html",
            error=str(exc),
            kpis={},
            summary_chart_json="{}",
            strategy_chart_json="{}",
            strategy_rows=[],
            accounts=[],
            selected_account="",
        )

    # ------------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------------
    for col in ["market_value", "cost_basis", "unrealized_pnl", "unrealized_pnl_pct", "percent_of_account"]:
        if col in balances_df.columns:
            balances_df[col] = pd.to_numeric(balances_df[col], errors="coerce").fillna(0)

    for col in ["amount", "quantity", "price", "fees"]:
        trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)
    trades_df["trade_date"] = pd.to_datetime(trades_df["trade_date"]).dt.date

    for col in ["unrealized_pnl", "market_value", "quantity", "current_price", "cost_basis"]:
        if col in current_df.columns:
            current_df[col] = pd.to_numeric(current_df[col], errors="coerce").fillna(0)

    for col in ["total_pnl", "num_trades"]:
        if col in strat_class_df.columns:
            strat_class_df[col] = pd.to_numeric(strat_class_df[col], errors="coerce").fillna(0)
    for col in ["open_date", "close_date"]:
        if col in strat_class_df.columns:
            strat_class_df[col] = pd.to_datetime(strat_class_df[col], errors="coerce").dt.date

    num_cols = ["total_pnl", "realized_pnl", "unrealized_pnl", "premium_received",
                "premium_paid", "num_trades", "num_winners", "num_losers",
                "dividend_income", "total_return"]
    for col in num_cols:
        if col in strat_summary_df.columns:
            strat_summary_df[col] = pd.to_numeric(strat_summary_df[col], errors="coerce").fillna(0)

    # ------------------------------------------------------------------
    # Account filter
    # ------------------------------------------------------------------
    all_accounts = sorted(trades_df["account"].dropna().unique())
    selected_account = request.args.get("account", "")

    if selected_account:
        balances_df = balances_df[balances_df["account"] == selected_account]
        trades_df = trades_df[trades_df["account"] == selected_account]
        current_df = current_df[current_df["account"] == selected_account]
        strat_class_df = strat_class_df[strat_class_df["account"] == selected_account]
        strat_summary_df = strat_summary_df[strat_summary_df["account"] == selected_account]

    # ------------------------------------------------------------------
    # KPIs from balances
    # ------------------------------------------------------------------
    cash_rows = balances_df[balances_df["row_type"] == "cash"]
    total_rows = balances_df[balances_df["row_type"] == "account_total"]

    cash_balance = float(cash_rows["market_value"].sum())
    account_value = float(total_rows["market_value"].sum())
    invested_value = account_value - cash_balance
    acct_unrealized = float(total_rows["unrealized_pnl"].sum())
    acct_cost_basis = float(total_rows["cost_basis"].sum())

    # Realized P&L from positions_summary
    realized_pnl = float(strat_summary_df["realized_pnl"].sum())
    total_return = float(strat_summary_df["total_return"].sum())

    kpis = {
        "account_value": account_value,
        "cash_balance": cash_balance,
        "invested_value": invested_value,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": acct_unrealized,
        "total_return": total_return,
    }

    # ------------------------------------------------------------------
    # Chart 1: Cumulative P&L over time (summary)
    # ------------------------------------------------------------------
    summary_chart = _build_account_summary_chart(trades_df, current_df)

    # ------------------------------------------------------------------
    # Chart 2: Strategy P&L over time
    # ------------------------------------------------------------------
    strategy_chart = _build_strategy_time_chart(strat_class_df)

    # ------------------------------------------------------------------
    # Strategy summary table
    # ------------------------------------------------------------------
    if not strat_summary_df.empty:
        strat_summary_df["win_rate"] = strat_summary_df.apply(
            lambda r: r["num_winners"] / (r["num_winners"] + r["num_losers"])
            if (r["num_winners"] + r["num_losers"]) > 0 else 0,
            axis=1,
        )
        strategy_rows = strat_summary_df.to_dict(orient="records")
    else:
        strategy_rows = []

    return render_template(
        "accounts.html",
        kpis=kpis,
        summary_chart_json=json.dumps(summary_chart),
        strategy_chart_json=json.dumps(strategy_chart),
        strategy_rows=strategy_rows,
        accounts=all_accounts,
        selected_account=selected_account,
    )
