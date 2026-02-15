from datetime import date
from flask import render_template, request
from flask_login import login_required, current_user
import pandas as pd

from app import app
from app.bigquery_client import get_bigquery_client
from app.models import get_accounts_for_user, is_admin


# ------------------------------------------------------------------
# Helpers (same pattern as routes.py)
# ------------------------------------------------------------------

def _user_account_list():
    if is_admin(current_user.username):
        return None
    return get_accounts_for_user(current_user.id)


def _account_filter(accounts):
    """Return a SQL AND clause fragment for account filtering."""
    if accounts is None:
        return ""
    if not accounts:
        return "AND 1 = 0"
    quoted = ", ".join(f"'{a.replace(chr(39), chr(39)+chr(39))}'" for a in accounts)
    return f"AND account IN ({quoted})"


def _filter_df(df, accounts, col="account"):
    if accounts is None:
        return df
    if not accounts:
        return df.iloc[0:0]
    return df[df[col].isin(accounts)]


# ------------------------------------------------------------------
# SQL Queries
# ------------------------------------------------------------------

CLOSED_TRADES_QUERY = """
    SELECT
        account,
        symbol,
        trade_symbol,
        strategy,
        trade_group_type,
        status,
        open_date,
        close_date,
        days_in_trade,
        total_pnl,
        num_trades
    FROM `ccwj-dbt.analytics.int_strategy_classification`
    WHERE status = 'Closed'
      {account_filter}
    ORDER BY close_date DESC
"""

DIVIDENDS_QUERY = """
    SELECT
        account,
        underlying_symbol AS symbol,
        trade_date,
        amount,
        description
    FROM `ccwj-dbt.analytics.stg_history`
    WHERE action = 'dividend'
      AND trade_date IS NOT NULL
      {account_filter}
    ORDER BY trade_date DESC
"""


def _classify_gains(df):
    """Add gain_type column: 'Short-Term' or 'Long-Term' based on holding period."""
    df = df.copy()
    df["gain_type"] = df["days_in_trade"].apply(
        lambda d: "Long-Term" if d > 365 else "Short-Term"
    )
    return df


def _detect_wash_sales(df):
    """
    Flag potential wash sales: closed trades at a loss where the same
    symbol was purchased within 30 days before or after the close date.

    Returns a list of dicts with wash sale details.
    """
    if df.empty:
        return []

    # Only look at losses
    losses = df[df["total_pnl"] < 0].copy()
    if losses.empty:
        return []

    # All trades (including opens) for repurchase detection
    all_trades = df.copy()

    wash_sales = []
    for _, loss_row in losses.iterrows():
        symbol = loss_row["symbol"]
        account = loss_row["account"]
        close_dt = loss_row["close_date"]

        if pd.isna(close_dt):
            continue

        # Find trades in the same symbol/account that opened within 30 days
        same_symbol = all_trades[
            (all_trades["symbol"] == symbol)
            & (all_trades["account"] == account)
            & (all_trades.index != loss_row.name)  # exclude self
        ]

        for _, other_row in same_symbol.iterrows():
            open_dt = other_row["open_date"]
            if pd.isna(open_dt):
                continue

            days_diff = (open_dt - close_dt).days
            if -30 <= days_diff <= 30:
                wash_sales.append({
                    "account": account,
                    "symbol": symbol,
                    "loss_close_date": str(close_dt),
                    "loss_pnl": float(loss_row["total_pnl"]),
                    "loss_strategy": loss_row["strategy"],
                    "repurchase_open_date": str(open_dt),
                    "repurchase_strategy": other_row["strategy"],
                    "days_between": days_diff,
                })
                break  # One match is enough to flag

    return wash_sales


# ------------------------------------------------------------------
# Federal tax bracket mapping
# ------------------------------------------------------------------
# Ordinary income brackets and their corresponding long-term capital gains rates
TAX_BRACKETS = [
    {"rate": 10, "label": "10%", "lt_rate": 0},
    {"rate": 12, "label": "12%", "lt_rate": 0},
    {"rate": 22, "label": "22%", "lt_rate": 15},
    {"rate": 24, "label": "24%", "lt_rate": 15},
    {"rate": 32, "label": "32%", "lt_rate": 15},
    {"rate": 35, "label": "35%", "lt_rate": 15},
    {"rate": 37, "label": "37%", "lt_rate": 20},
]

DEFAULT_BRACKET = 22  # Most common bracket for middle-income filers


@app.route("/taxes")
@login_required
def taxes():
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    acct_filter = _account_filter(user_accounts)

    # Available tax years
    current_year = date.today().year
    selected_year = request.args.get("year", str(current_year))
    try:
        selected_year_int = int(selected_year)
    except ValueError:
        selected_year_int = current_year

    # Tax bracket selection
    selected_bracket = request.args.get("bracket", str(DEFAULT_BRACKET))
    try:
        bracket_rate = int(selected_bracket)
    except ValueError:
        bracket_rate = DEFAULT_BRACKET

    # Look up the matching bracket info
    bracket_info = next(
        (b for b in TAX_BRACKETS if b["rate"] == bracket_rate),
        {"rate": DEFAULT_BRACKET, "label": "22%", "lt_rate": 15},
    )

    try:
        # Closed trades
        trades_df = client.query(
            CLOSED_TRADES_QUERY.format(account_filter=acct_filter)
        ).to_dataframe()

        # Dividends
        dividends_df = client.query(
            DIVIDENDS_QUERY.format(account_filter=acct_filter)
        ).to_dataframe()
    except Exception as exc:
        return render_template(
            "taxes.html",
            title="Tax Center",
            error=str(exc),
            summary={},
            gains_rows=[],
            wash_sales=[],
            dividend_rows=[],
            years=[],
            selected_year=selected_year,
            tax_brackets=TAX_BRACKETS,
            selected_bracket=bracket_rate,
            bracket_info=bracket_info,
        )

    # Clean types
    for col in ["total_pnl", "days_in_trade", "num_trades"]:
        if col in trades_df.columns:
            trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").fillna(0)

    for col in ["open_date", "close_date"]:
        if col in trades_df.columns:
            trades_df[col] = pd.to_datetime(trades_df[col], errors="coerce").dt.date

    if "amount" in dividends_df.columns:
        dividends_df["amount"] = pd.to_numeric(dividends_df["amount"], errors="coerce").fillna(0)
    if "trade_date" in dividends_df.columns:
        dividends_df["trade_date"] = pd.to_datetime(dividends_df["trade_date"], errors="coerce").dt.date

    # Filter to user accounts (DataFrame-level)
    trades_df = _filter_df(trades_df, user_accounts)
    dividends_df = _filter_df(dividends_df, user_accounts)

    # Classify gains
    trades_df = _classify_gains(trades_df)

    # Detect wash sales (across all years for accuracy)
    wash_sales = _detect_wash_sales(trades_df)

    # Determine available years
    if not trades_df.empty:
        trades_df["tax_year"] = trades_df["close_date"].apply(
            lambda d: d.year if pd.notna(d) else None
        )
        all_years = sorted(trades_df["tax_year"].dropna().unique(), reverse=True)
    else:
        all_years = []

    if not dividends_df.empty:
        dividends_df["tax_year"] = dividends_df["trade_date"].apply(
            lambda d: d.year if pd.notna(d) else None
        )
        div_years = dividends_df["tax_year"].dropna().unique().tolist()
        all_years = sorted(set(list(all_years) + div_years), reverse=True)

    years = [int(y) for y in all_years]

    # Filter to selected year
    year_trades = trades_df[trades_df["tax_year"] == selected_year_int] if not trades_df.empty else trades_df
    year_dividends = dividends_df[dividends_df["tax_year"] == selected_year_int] if not dividends_df.empty else dividends_df
    year_wash = [w for w in wash_sales if str(selected_year_int) in w["loss_close_date"]]

    # Summary
    st_gains = float(year_trades.loc[
        (year_trades["gain_type"] == "Short-Term") & (year_trades["total_pnl"] > 0), "total_pnl"
    ].sum()) if not year_trades.empty else 0
    st_losses = float(year_trades.loc[
        (year_trades["gain_type"] == "Short-Term") & (year_trades["total_pnl"] < 0), "total_pnl"
    ].sum()) if not year_trades.empty else 0
    lt_gains = float(year_trades.loc[
        (year_trades["gain_type"] == "Long-Term") & (year_trades["total_pnl"] > 0), "total_pnl"
    ].sum()) if not year_trades.empty else 0
    lt_losses = float(year_trades.loc[
        (year_trades["gain_type"] == "Long-Term") & (year_trades["total_pnl"] < 0), "total_pnl"
    ].sum()) if not year_trades.empty else 0

    net_st = st_gains + st_losses
    net_lt = lt_gains + lt_losses
    net_total = net_st + net_lt

    total_dividends = float(year_dividends["amount"].sum()) if not year_dividends.empty else 0
    wash_sale_total = sum(abs(w["loss_pnl"]) for w in year_wash)

    # Estimated tax impact
    st_rate = bracket_info["rate"] / 100
    lt_rate = bracket_info["lt_rate"] / 100

    # Tax only applies to net gains (losses reduce taxable income up to $3,000)
    est_st_tax = max(0, net_st) * st_rate
    est_lt_tax = max(0, net_lt) * lt_rate
    est_div_tax = max(0, total_dividends) * st_rate  # assume ordinary dividends
    est_total_tax = est_st_tax + est_lt_tax + est_div_tax

    # If net losses, show potential deduction (up to $3,000 cap)
    net_loss_deduction = 0
    if net_total < 0:
        net_loss_deduction = min(abs(net_total), 3000)

    summary = {
        "st_gains": st_gains,
        "st_losses": st_losses,
        "net_st": net_st,
        "lt_gains": lt_gains,
        "lt_losses": lt_losses,
        "net_lt": net_lt,
        "net_total": net_total,
        "total_dividends": total_dividends,
        "wash_sale_total": wash_sale_total,
        "num_closed_trades": len(year_trades),
        "est_st_tax": est_st_tax,
        "est_lt_tax": est_lt_tax,
        "est_div_tax": est_div_tax,
        "est_total_tax": est_total_tax,
        "net_loss_deduction": net_loss_deduction,
        "st_rate": bracket_info["rate"],
        "lt_rate": bracket_info["lt_rate"],
    }

    # Convert to display rows
    gains_rows = []
    if not year_trades.empty:
        for _, r in year_trades.iterrows():
            gains_rows.append({
                "account": r["account"],
                "symbol": r["symbol"],
                "strategy": r["strategy"],
                "trade_group_type": r["trade_group_type"],
                "open_date": str(r["open_date"]) if pd.notna(r["open_date"]) else "",
                "close_date": str(r["close_date"]) if pd.notna(r["close_date"]) else "",
                "days_in_trade": int(r["days_in_trade"]),
                "total_pnl": float(r["total_pnl"]),
                "gain_type": r["gain_type"],
            })

    dividend_rows = []
    if not year_dividends.empty:
        div_agg = year_dividends.groupby(["account", "symbol"]).agg(
            total=("amount", "sum"),
            count=("amount", "count"),
            last_date=("trade_date", "max"),
        ).reset_index().sort_values("total", ascending=False)
        dividend_rows = div_agg.to_dict(orient="records")
        for row in dividend_rows:
            row["last_date"] = str(row["last_date"]) if pd.notna(row["last_date"]) else ""

    return render_template(
        "taxes.html",
        title="Tax Center",
        summary=summary,
        gains_rows=gains_rows,
        wash_sales=year_wash,
        dividend_rows=dividend_rows,
        years=years,
        selected_year=selected_year,
        tax_brackets=TAX_BRACKETS,
        selected_bracket=bracket_rate,
        bracket_info=bracket_info,
    )
