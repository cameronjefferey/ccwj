"""Live AI-analysis copy for the public demo.

The demo user cannot regenerate insights (writes are blocked), and the
old startup seed was a one-shot essay about covered calls on AAPL / NVDA
/ META dated the moment it was first inserted. Production kept serving
that row forever.

This module writes the analysis from the same portfolio rows the real
coach reads (``positions_summary``). No model call, no hardcoded book.
"""

from __future__ import annotations

from datetime import datetime


_BANNED_UNLESS_PRESENT = (
    "What This Demo Shows",
    "Mirror Score",
    "Upload your own data",
    "Poor Man's Covered Call",
    "PMCC",
)


def _num(v) -> float:
    try:
        if v is None:
            return 0.0
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    if f != f:
        return 0.0
    return f


def _money(n: float, places: int = 0) -> str:
    sign = "-" if n < 0 else ""
    return f"{sign}${abs(n):,.{places}f}"


def _date_label(value) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in ("nat", "none", "nan"):
        return ""
    return text[:10]


def compose_demo_analysis(portfolio_df, coaching_data=None, generated_at=None):
    """Return ``(summary, full_analysis, generated_at)`` for the demo page.

    ``portfolio_df`` is the positions_summary frame (one row per account,
    symbol, strategy). ``coaching_data`` is the dict already built for
    /insights; recent exits are quoted when present. Empty input does not
    fall back to a marketing essay.
    """
    when = generated_at or datetime.now()
    coaching_data = coaching_data or {}
    if portfolio_df is None or getattr(portfolio_df, "empty", True):
        summary = "The demo account has no positions in the warehouse yet."
        body = (
            "## Summary\n\n"
            "There is nothing to analyze until the demo book's positions "
            "are in the warehouse. This note is built from that book, not "
            "from a saved write-up."
        )
        return summary, body, when

    df = portfolio_df.copy()
    for col in (
        "total_return", "realized_pnl", "unrealized_pnl",
        "total_dividend_income", "num_individual_trades",
        "num_winners", "num_losers", "total_premium_received",
        "total_premium_paid",
    ):
        if col in df.columns:
            df[col] = df[col].map(_num)

    symbols = sorted({
        str(s).strip().upper()
        for s in df.get("symbol", [])
        if str(s).strip()
    })
    strategies = []
    if "strategy" in df.columns:
        grouped = df.groupby("strategy", dropna=False)
        for name, part in grouped:
            label = str(name or "Unlabeled").strip() or "Unlabeled"
            strategies.append({
                "name": label,
                "total": _num(part["total_return"].sum()) if "total_return" in part else 0.0,
                "trades": int(part["num_individual_trades"].sum()) if "num_individual_trades" in part else 0,
                "symbols": sorted({
                    str(s).strip().upper()
                    for s in part.get("symbol", [])
                    if str(s).strip()
                }),
            })
        strategies.sort(key=lambda row: (-abs(row["total"]), row["name"]))

    total = _num(df["total_return"].sum()) if "total_return" in df.columns else 0.0
    realized = _num(df["realized_pnl"].sum()) if "realized_pnl" in df.columns else 0.0
    unrealized = _num(df["unrealized_pnl"].sum()) if "unrealized_pnl" in df.columns else 0.0
    dividends = (
        _num(df["total_dividend_income"].sum())
        if "total_dividend_income" in df.columns else 0.0
    )
    winners = int(df["num_winners"].sum()) if "num_winners" in df.columns else 0
    losers = int(df["num_losers"].sum()) if "num_losers" in df.columns else 0
    closed = winners + losers
    trades = int(df["num_individual_trades"].sum()) if "num_individual_trades" in df.columns else 0

    first = ""
    last = ""
    if "first_trade_date" in df.columns:
        dates = [_date_label(v) for v in df["first_trade_date"].tolist()]
        dates = [d for d in dates if d]
        first = min(dates) if dates else ""
    if "last_trade_date" in df.columns:
        dates = [_date_label(v) for v in df["last_trade_date"].tolist()]
        dates = [d for d in dates if d]
        last = max(dates) if dates else ""

    top_names = [s["name"] for s in strategies[:3]]
    style = ", ".join(top_names) if top_names else "the strategies in this book"
    window = ""
    if first and last:
        window = f" from {first} to {last}"
    elif first:
        window = f" since {first}"

    summary = (
        f"The demo account has {len(symbols)} symbol"
        f"{'' if len(symbols) == 1 else 's'}"
        f"{window}, traded mainly as {style}. "
        f"Lifetime P&L on the current book is {_money(total)} "
        f"({_money(realized)} realized, {_money(unrealized)} unrealized)."
    )

    lines = ["## Summary", "", summary, "", "## What the book actually is", ""]
    if strategies:
        lines.append("Strategy totals on this account:")
        lines.append("")
        for row in strategies:
            held = ", ".join(row["symbols"][:8])
            extra = ""
            if len(row["symbols"]) > 8:
                extra = f" and {len(row['symbols']) - 8} more"
            lines.append(
                f"- **{row['name']}** — {_money(row['total'])} "
                f"across {row['trades']} trade{'s' if row['trades'] != 1 else ''}"
                + (f" ({held}{extra})" if held else "")
            )
        lines.append("")
    if symbols:
        preview = ", ".join(symbols[:12])
        more = f" and {len(symbols) - 12} more" if len(symbols) > 12 else ""
        lines.append(f"Symbols in the book: {preview}{more}.")
        lines.append("")
    if closed:
        rate = winners / closed
        lines.append(
            f"Closed groups: {winners} winners and {losers} losers "
            f"({rate:.0%} of {closed}). Fills recorded: {trades}."
        )
        lines.append("")
    if dividends:
        lines.append(f"Dividends attributed on these rows: {_money(dividends, 2)}.")
        lines.append("")

    exits = coaching_data.get("recent_exits") or []
    if exits:
        lines.append("## Recent closes")
        lines.append("")
        for ex in exits[:6]:
            sym = str(ex.get("symbol") or "").strip()
            strat = str(ex.get("strategy") or "").strip()
            pnl = _num(ex.get("actual_pnl"))
            when_closed = _date_label(ex.get("close_date"))
            label = sym or "A contract"
            if strat:
                label = f"{label} ({strat})"
            dated = f" on {when_closed}" if when_closed else ""
            lines.append(f"- {label}{dated}: closed at {_money(pnl)}.")
        lines.append("")

    lines.append(
        "This write-up is rebuilt from the demo account's current "
        "positions and closes. It is not a saved essay."
    )
    body = "\n".join(lines).strip() + "\n"
    return summary, body, when
