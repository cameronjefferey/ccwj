"""Tax Center — reads pre-classified tax lots from dbt (int_tax_lots)."""
from datetime import date
from flask import render_template, request
from flask_login import login_required, current_user
import pandas as pd

from app import app
from app.bigquery_client import get_bigquery_client
from app.models import get_accounts_for_user, is_admin


def _user_account_list():
    if is_admin(current_user.username):
        return None
    return get_accounts_for_user(current_user.id)


def _account_filter(accounts):
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


TAX_LOTS_QUERY = """
    SELECT *
    FROM `ccwj-dbt.analytics.int_tax_lots`
    WHERE 1 = 1
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

TAX_BRACKETS = [
    {"rate": 10, "label": "10%", "lt_rate": 0},
    {"rate": 12, "label": "12%", "lt_rate": 0},
    {"rate": 22, "label": "22%", "lt_rate": 15},
    {"rate": 24, "label": "24%", "lt_rate": 15},
    {"rate": 32, "label": "32%", "lt_rate": 15},
    {"rate": 35, "label": "35%", "lt_rate": 15},
    {"rate": 37, "label": "37%", "lt_rate": 20},
]

DEFAULT_BRACKET = 22


@app.route("/taxes")
@login_required
def taxes():
    client = get_bigquery_client()
    user_accounts = _user_account_list()
    acct_filter = _account_filter(user_accounts)

    current_year = date.today().year
    selected_year = request.args.get("year", str(current_year))
    try:
        selected_year_int = int(selected_year)
    except ValueError:
        selected_year_int = current_year

    selected_bracket = request.args.get("bracket", str(DEFAULT_BRACKET))
    try:
        bracket_rate = int(selected_bracket)
    except ValueError:
        bracket_rate = DEFAULT_BRACKET

    bracket_info = next(
        (b for b in TAX_BRACKETS if b["rate"] == bracket_rate),
        {"rate": DEFAULT_BRACKET, "label": "22%", "lt_rate": 15},
    )

    try:
        lots_df = client.query(
            TAX_LOTS_QUERY.format(account_filter=acct_filter)
        ).to_dataframe()

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

    for col in ["total_pnl", "days_in_trade", "num_trades", "tax_year"]:
        if col in lots_df.columns:
            lots_df[col] = pd.to_numeric(lots_df[col], errors="coerce").fillna(0)

    for col in ["open_date", "close_date"]:
        if col in lots_df.columns:
            lots_df[col] = pd.to_datetime(lots_df[col], errors="coerce").dt.date

    if "amount" in dividends_df.columns:
        dividends_df["amount"] = pd.to_numeric(dividends_df["amount"], errors="coerce").fillna(0)
    if "trade_date" in dividends_df.columns:
        dividends_df["trade_date"] = pd.to_datetime(dividends_df["trade_date"], errors="coerce").dt.date

    lots_df = _filter_df(lots_df, user_accounts)
    dividends_df = _filter_df(dividends_df, user_accounts)

    # Available years
    all_years = sorted(lots_df["tax_year"].dropna().unique(), reverse=True) if not lots_df.empty else []
    if not dividends_df.empty:
        dividends_df["tax_year"] = dividends_df["trade_date"].apply(
            lambda d: d.year if pd.notna(d) else None
        )
        div_years = dividends_df["tax_year"].dropna().unique().tolist()
        all_years = sorted(set(list(all_years) + div_years), reverse=True)
    years = [int(y) for y in all_years]

    # Filter to selected year
    year_lots = lots_df[lots_df["tax_year"] == selected_year_int] if not lots_df.empty else lots_df
    year_dividends = dividends_df[dividends_df["tax_year"] == selected_year_int] if not dividends_df.empty else dividends_df

    # Wash sales from pre-computed flags
    wash_sales = []
    if not year_lots.empty and "is_potential_wash_sale" in year_lots.columns:
        ws_rows = year_lots[year_lots["is_potential_wash_sale"] == True]
        for _, r in ws_rows.iterrows():
            wash_sales.append({
                "account": r["account"],
                "symbol": r["symbol"],
                "loss_close_date": str(r["close_date"]) if pd.notna(r["close_date"]) else "",
                "loss_pnl": float(r["total_pnl"]),
                "loss_strategy": r["strategy"],
                "repurchase_open_date": str(r.get("wash_repurchase_date", "")) if pd.notna(r.get("wash_repurchase_date")) else "",
                "repurchase_strategy": r.get("wash_repurchase_strategy", ""),
                "days_between": int(r.get("wash_days_between", 0)) if pd.notna(r.get("wash_days_between")) else 0,
            })

    # Summary calculations
    st_gains = float(year_lots.loc[
        (year_lots["gain_type"] == "Short-Term") & (year_lots["total_pnl"] > 0), "total_pnl"
    ].sum()) if not year_lots.empty else 0
    st_losses = float(year_lots.loc[
        (year_lots["gain_type"] == "Short-Term") & (year_lots["total_pnl"] < 0), "total_pnl"
    ].sum()) if not year_lots.empty else 0
    lt_gains = float(year_lots.loc[
        (year_lots["gain_type"] == "Long-Term") & (year_lots["total_pnl"] > 0), "total_pnl"
    ].sum()) if not year_lots.empty else 0
    lt_losses = float(year_lots.loc[
        (year_lots["gain_type"] == "Long-Term") & (year_lots["total_pnl"] < 0), "total_pnl"
    ].sum()) if not year_lots.empty else 0

    net_st = st_gains + st_losses
    net_lt = lt_gains + lt_losses
    net_total = net_st + net_lt

    total_dividends = float(year_dividends["amount"].sum()) if not year_dividends.empty else 0
    wash_sale_total = sum(abs(w["loss_pnl"]) for w in wash_sales)

    st_rate = bracket_info["rate"] / 100
    lt_rate = bracket_info["lt_rate"] / 100
    est_st_tax = max(0, net_st) * st_rate
    est_lt_tax = max(0, net_lt) * lt_rate
    est_div_tax = max(0, total_dividends) * st_rate
    est_total_tax = est_st_tax + est_lt_tax + est_div_tax

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
        "num_closed_trades": len(year_lots),
        "est_st_tax": est_st_tax,
        "est_lt_tax": est_lt_tax,
        "est_div_tax": est_div_tax,
        "est_total_tax": est_total_tax,
        "net_loss_deduction": net_loss_deduction,
        "st_rate": bracket_info["rate"],
        "lt_rate": bracket_info["lt_rate"],
    }

    gains_rows = []
    if not year_lots.empty:
        for _, r in year_lots.iterrows():
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
                "is_wash_sale": bool(r.get("is_potential_wash_sale", False)),
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
        wash_sales=wash_sales,
        dividend_rows=dividend_rows,
        years=years,
        selected_year=selected_year,
        tax_brackets=TAX_BRACKETS,
        selected_bracket=bracket_rate,
        bracket_info=bracket_info,
    )
