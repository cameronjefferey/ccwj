"""Execution review — grading decisions against everything the data knows.

The warehouse doesn't just have the trade log; it has the underlying's
daily closes for every option's expiry date and (accumulating since Aug
2026) daily option marks. That lets us grade each resolved contract
against the record of what happened AFTER the decision:

  * an early buyback of a short option that went on to expire worthless
    paid real money to close risk that never materialized;
  * a roll away from a strike that was never breached was insurance that
    wasn't tested; a roll away from a strike that finished deep in the
    money genuinely sidestepped that settlement;
  * a long option sold months before expiry either banked value the
    market later took back, or left the rest of the move on the table.

All math lives in dbt (`int_option_exit_quality`); this module only
aggregates and phrases. Copy stays neutral evidence — counts and dollars,
"the record shows", never advice or psychological labels (AGENTS.md
pattern-detection rules). Holding to expiry carried risk the trader chose
not to take, and the sentences must not pretend otherwise.

DATA-SUFFICIENCY GATES (the "after X days" promise): the profile card
renders only with >= MIN_GRADED_PROFILE graded contracts; per-symbol
sentences need >= MIN_GRADED_SYMBOL; marks-based (peak-capture) claims
switch on per-contract via ``data_reliable`` and need
>= MIN_MARKED_CONTRACTS qualifying contracts. Surfaces strengthen
automatically as history accrues — no flag day.
"""

from datetime import date, timedelta

import pandas as pd

from app.position_story import _money

# Both queries are tenant-scoped in SQL AND project tenant_id so the
# fail-closed DataFrame filter works (pinned by
# tests/test_tenant_filtered_queries_carry_tenant_id.py).
EXECUTION_REVIEW_QUERY = """
    SELECT
        tenant_id, account, symbol, trade_symbol, option_type, option_strike,
        option_expiry, direction, open_date, close_date, close_type,
        days_held, dte_at_close, contracts,
        premium_received, cost_to_close, proceeds_from_close, realized_pnl,
        underlying_close_at_expiry, intrinsic_at_expiry, expiry_settlement_value,
        expired_worthless, gradeable_early_close, early_close_vs_expiry_delta,
        was_rolled, roll_new_strike, roll_new_expiry, net_roll_credit,
        peak_unrealized_pnl, snapshot_count, snapshot_density, data_reliable,
        pnl_given_back, giveback_pct
    FROM `ccwj-dbt.analytics.int_option_exit_quality`
    WHERE 1=1
    {tenant_filter}
"""

POSITION_EXECUTION_QUERY = """
    SELECT
        tenant_id, account, symbol, trade_symbol, option_type, option_strike,
        option_expiry, direction, open_date, close_date, close_type,
        days_held, dte_at_close, contracts,
        premium_received, cost_to_close, proceeds_from_close, realized_pnl,
        underlying_close_at_expiry, intrinsic_at_expiry, expiry_settlement_value,
        expired_worthless, gradeable_early_close, early_close_vs_expiry_delta,
        was_rolled, roll_new_strike, roll_new_expiry, net_roll_credit,
        peak_unrealized_pnl, snapshot_count, snapshot_density, data_reliable,
        pnl_given_back, giveback_pct
    FROM `ccwj-dbt.analytics.int_option_exit_quality`
    WHERE symbol = '{symbol}'
    {tenant_filter}
"""

# Live open-option record for the Daily Review "right now" block. Reads
# int_option_contracts directly (no new mart needed): premium collected /
# paid is fills-truth, the current mark is the live broker snapshot.
# Tenant-scoped + projects tenant_id (pinned by
# tests/test_tenant_filtered_queries_carry_tenant_id.py).
OPEN_OPTION_RECORD_QUERY = """
    SELECT
        tenant_id, account,
        underlying_symbol AS symbol,
        trade_symbol, option_type, option_strike, option_expiry,
        direction, open_date,
        contracts_sold_to_open, contracts_bought_to_open,
        premium_received, premium_paid,
        current_market_value, current_unrealized_pnl
    FROM `ccwj-dbt.analytics.int_option_contracts`
    WHERE status = 'Open'
      AND option_expiry IS NOT NULL
    {tenant_filter}
"""

MIN_GRADED_PROFILE = 5
MIN_GRADED_SYMBOL = 2
MIN_MARKED_CONTRACTS = 3
# Below this the verdict is rounding noise, not evidence.
MIN_NOTE_DELTA = 20.0
# Rolling self-comparison window (days) and the minimum recent sample.
TREND_WINDOW_DAYS = 90
MIN_TREND_RECENT = 3


def _prep(df):
    """Coerce the numeric/bool columns once; return a clean copy."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    for col in ("early_close_vs_expiry_delta", "realized_pnl", "contracts",
                "intrinsic_at_expiry", "expiry_settlement_value",
                "peak_unrealized_pnl", "dte_at_close", "days_held",
                "premium_received"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ("gradeable_early_close", "expired_worthless", "was_rolled",
                "data_reliable"):
        if col in out.columns:
            # map() instead of fillna().astype(bool): object-dtype columns
            # with None trip pandas' downcasting FutureWarning.
            out[col] = out[col].map(
                lambda v: bool(v) if pd.notna(v) else False)
    for col in ("open_date", "close_date", "option_expiry"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.date
    if "symbol" in out.columns:
        out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
    if "trade_symbol" in out.columns:
        out["trade_symbol"] = out["trade_symbol"].astype(str).str.strip()
    return out


def _contract_label(row):
    """"$200 call (exp Jun 18 '26)" — display handle for one contract."""
    try:
        strike = float(row.get("option_strike"))
        strike_txt = f"${int(strike)}" if strike == int(strike) else f"${strike:g}"
    except (TypeError, ValueError):
        strike_txt = ""
    ot = str(row.get("option_type") or "").upper()
    otype = "call" if ot.startswith("C") else "put" if ot.startswith("P") else "option"
    exp = row.get("option_expiry")
    exp_txt = ""
    try:
        exp_txt = f" (exp {pd.Timestamp(exp).strftime('%b %-d %Y')})"
    except (TypeError, ValueError):
        pass
    return f"{strike_txt} {otype}{exp_txt}".strip()


def _signed(v):
    return ("+" if v >= 0 else "\u2212") + _money(v)


# ── Profile card ─────────────────────────────────────────────────────────

def summarize_execution(df, min_graded=MIN_GRADED_PROFILE, today=None):
    """Fold the exit-quality rows into the Trader Profile card context.

    Returns None when there isn't enough graded evidence yet (the
    sufficiency gate), else a TAKEAWAY-FIRST dict (Aug 2026 readability
    pass — the old shape was paragraphs of prose plus chips repeating
    the same numbers, and users couldn't find the takeaway):

      headline  — the one number that answers "are my exits good?":
                  net of every graded early close vs the expiry outcome.
      findings  — scannable rows {label, value, tone, detail}: one bold
                  number per row, one short muted clause of context.
      examples  — largest verdicts, each provable on its position page.
      pending_note — set while daily-mark coverage is still accumulating.

    ``today`` anchors the rolling self-comparison (recent
    TREND_WINDOW_DAYS of early exits vs the lifetime baseline) — the
    part of this card that MOVES week to week. Defaults to the wall
    clock; tests pin it.
    """
    df = _prep(df)
    if df.empty or "gradeable_early_close" not in df.columns:
        return None
    graded = df[df["gradeable_early_close"]
                & df["early_close_vs_expiry_delta"].notna()]
    if len(graded) < min_graded:
        return None

    net_all = float(graded["early_close_vs_expiry_delta"].sum())
    headline = {
        "value": _signed(net_all),
        "tone": "pos" if net_all >= 0 else "neg",
        "text": ("your early exits vs holding to expiry"),
        "sub": (f"Every early buyback, roll, and long sale — "
                f"{len(graded)} contracts graded against what each one "
                f"actually did at expiry."),
    }

    findings = []

    # 1. Long options sold before expiry — usually the biggest number,
    #    so it leads.
    longs = graded[(graded["direction"] == "Bought") & ~graded["was_rolled"]]
    if len(longs) >= 2:
        n_better = int((longs["early_close_vs_expiry_delta"] > 0).sum())
        net = float(longs["early_close_vs_expiry_delta"].sum())
        findings.append({
            "label": "Long exits",
            "value": _signed(net),
            "tone": "pos" if net >= 0 else "neg",
            "detail": (f"{len(longs)} sales before expiry — the exit beat "
                       f"holding in {n_better} of them."),
        })

    # 2. Early buybacks on short options (rolls handled separately).
    shorts = graded[(graded["direction"] == "Sold") & ~graded["was_rolled"]]
    if len(shorts) >= 2:
        n_worthless = int(shorts["expired_worthless"].sum())
        saved = float(shorts.loc[
            shorts["early_close_vs_expiry_delta"] > 0,
            "early_close_vs_expiry_delta"].sum())
        net = float(shorts["early_close_vs_expiry_delta"].sum())
        detail = (f"{n_worthless} of {len(shorts)} buybacks would have "
                  f"expired worthless anyway.")
        if saved > 1:
            n_saved = int((shorts["early_close_vs_expiry_delta"] > 0).sum())
            detail += (f" Closing dodged {_money(saved)} on the {n_saved} "
                       f"that finished in the money.")
        findings.append({
            "label": "Short buybacks",
            "value": _signed(net),
            "tone": "pos" if net >= 0 else "neg",
            "detail": detail,
        })

    # 3. Roll necessity — the closed leg's own expiry answers whether the
    #    strike ever got run over.
    rolls = graded[graded["was_rolled"]]
    if len(rolls) >= 2:
        untested = int(rolls["expired_worthless"].sum())
        sidestepped = float(rolls.loc[
            rolls["early_close_vs_expiry_delta"] > 0,
            "early_close_vs_expiry_delta"].sum())
        detail = "The old strike expired worthless — insurance, not rescue."
        if sidestepped > 1:
            detail = (f"The rest finished in the money — rolling sidestepped "
                      f"{_money(sidestepped)}.")
        findings.append({
            "label": "Rolls never tested",
            "value": f"{untested} of {len(rolls)}",
            "tone": "neutral",
            "detail": detail,
        })

    # 4. Expiry discipline (no counterfactual needed — it happened).
    if "close_type" in df.columns:
        expired = df[df["close_type"].isin(["Expired", "ExpiredOTM"])
                     & (df["direction"] == "Sold") & (df["realized_pnl"] > 0)]
        if len(expired) >= 2:
            kept = float(expired["realized_pnl"].sum())
            findings.append({
                "label": "Held to expiry",
                "value": f"{_money(kept)} kept",
                "tone": "pos",
                "detail": (f"{len(expired)} short contracts carried all the "
                           f"way to worthless expiry."),
            })

    # 5. Rolling self-comparison — the mirror compares you to you, and
    #    this is the number that moves week to week. Per-contract average
    #    (not total) so a different recent trade count doesn't masquerade
    #    as a behavior change.
    trend = execution_trend(graded, today=today)
    if trend:
        findings.append({
            "label": f"Last {TREND_WINDOW_DAYS} days",
            "value": f"{_signed(trend['recent_avg'])}/exit",
            "tone": "pos" if trend["recent_avg"] >= 0 else "neg",
            "detail": (f"Average early-exit result across "
                       f"{trend['n_recent']} recent exits, vs "
                       f"{_signed(trend['baseline_avg'])}/exit before "
                       f"the window."),
        })

    # 6. Marks record — unlocks as daily-mark coverage accumulates.
    pending_note = None
    marked = df[df["data_reliable"] & (df["peak_unrealized_pnl"] > 50)
                & (df["realized_pnl"] > 0)]
    if len(marked) >= MIN_MARKED_CONTRACTS:
        capture = (marked["realized_pnl"] / marked["peak_unrealized_pnl"]) \
            .clip(0, 1)
        med = float(capture.median()) * 100
        findings.append({
            "label": "Peak capture",
            "value": f"{med:.0f}% median",
            "tone": "neutral",
            "detail": (f"Share of the best exit the daily marks recorded, "
                       f"across {len(marked)} winners."),
        })
    else:
        pending_note = ("Daily option marks are still accumulating — "
                        "peak-capture grading unlocks as coverage builds.")

    # Examples: the largest verdicts, each provable on its position page.
    examples = []
    ex = graded.reindex(
        graded["early_close_vs_expiry_delta"].abs()
        .sort_values(ascending=False).index)
    for _, row in ex.head(3).iterrows():
        delta = float(row["early_close_vs_expiry_delta"])
        label = _contract_label(row)
        if row["was_rolled"]:
            tail = ("the strike was never tested"
                    if row["expired_worthless"]
                    else f"the roll sidestepped {_money(delta)}")
            desc = f"Rolled away from the {label} — {tail}."
        elif row["direction"] == "Sold":
            desc = (f"Bought back the {label}; it expired worthless — "
                    f"{_money(-delta)} given up vs holding."
                    if delta < 0 else
                    f"Bought back the {label} before it finished in the "
                    f"money — {_money(delta)} avoided.")
        else:
            desc = (f"Sold the {label} early; by expiry it was worth "
                    f"{_money(-delta)} more than the exit."
                    if delta < 0 else
                    f"Sold the {label} near the highs — {_money(delta)} "
                    f"better than the expiry outcome.")
        examples.append({"symbol": row["symbol"], "desc": desc,
                         "delta": round(delta, 2)})

    return {
        "n_graded": int(len(graded)),
        "headline": headline,
        "findings": findings,
        "examples": examples,
        "pending_note": pending_note,
    }


# ── Rolling self-comparison ──────────────────────────────────────────────

def execution_trend(graded, today=None, window_days=TREND_WINDOW_DAYS):
    """Recent early-exit record vs the lifetime baseline.

    ``graded`` — already-_prep'd rows with a non-null delta (the caller
    filters). Returns None until there are MIN_TREND_RECENT recent exits
    AND an out-of-window baseline to compare against; else
    {recent_avg, baseline_avg, n_recent} (the profile card phrases it).
    """
    if graded is None or len(graded) == 0 or "close_date" not in graded.columns:
        return None
    today = today or date.today()
    cutoff = today - timedelta(days=window_days)
    recent = graded[graded["close_date"] >= cutoff]
    baseline = graded[graded["close_date"] < cutoff]
    if len(recent) < MIN_TREND_RECENT or len(baseline) < MIN_TREND_RECENT:
        return None
    recent_avg = float(recent["early_close_vs_expiry_delta"].mean())
    baseline_avg = float(baseline["early_close_vs_expiry_delta"].mean())
    return {
        "recent_avg": round(recent_avg, 2),
        "baseline_avg": round(baseline_avg, 2),
        "n_recent": int(len(recent)),
    }


# ── Verdict maturation (Daily Review) ────────────────────────────────────

def _short_label(row):
    """"$200 call" — the compact handle for feed rows where the expiry is
    already carried by an adjacent badge or days-left figure."""
    try:
        strike = float(row.get("option_strike"))
        strike_txt = f"${int(strike)}" if strike == int(strike) else f"${strike:g}"
    except (TypeError, ValueError):
        strike_txt = ""
    ot = str(row.get("option_type") or "").upper()
    otype = "call" if ot.startswith("C") else "put" if ot.startswith("P") else "option"
    return f"{strike_txt} {otype}".strip()


def _verdict_action(row):
    """The SHORT action line for a landed verdict in the Daily Review feed.

    The row already leads with the signed dollar delta in its own column,
    so this line only says WHAT HAPPENED — no numbers repeated (Aug 2026
    readability pass; the full sentence form lives on in
    _verdict_sentence for the email digest, which has no layout to lean
    on)."""
    delta = float(row["early_close_vs_expiry_delta"])
    short = _short_label(row)
    if row["was_rolled"]:
        if row["expired_worthless"]:
            return f"Rolled off the {short} — the old strike was never tested."
        return f"Rolled off the {short} — the old strike finished in the money."
    if str(row["direction"]) == "Sold":
        if delta < 0:
            if row["expired_worthless"]:
                return f"Bought back the {short}; it expired worthless anyway."
            return f"Bought back the {short}; holding would have paid more."
        return f"Bought back the {short} before it finished in the money."
    if delta < 0:
        return f"Sold the {short} early; it was worth more at expiry."
    return f"Sold the {short}; the exit beat the expiry outcome."


def _verdict_sentence(row):
    """One landed verdict as a full sentence — used by the weekly summary
    EMAIL, which renders plain list items with no delta column."""
    delta = float(row["early_close_vs_expiry_delta"])
    label = _contract_label(row)
    closed = row.get("close_date")
    closed_txt = ""
    try:
        closed_txt = f" on {pd.Timestamp(closed).strftime('%b %-d')}"
    except (TypeError, ValueError):
        pass
    if row["was_rolled"]:
        if row["expired_worthless"]:
            return (f"The {label} you rolled away from{closed_txt} expired "
                    f"worthless — the roll was never tested.")
        return (f"The {label} you rolled away from{closed_txt} finished in "
                f"the money — rolling sidestepped {_money(delta)}.")
    if str(row["direction"]) == "Sold":
        if delta < 0:
            if row["expired_worthless"]:
                return (f"The {label} you bought back{closed_txt} expired "
                        f"worthless — that close gave up {_money(delta)} vs "
                        f"holding.")
            return (f"The {label} you bought back{closed_txt} — holding to "
                    f"expiry would have come out {_money(delta)} better.")
        return (f"The {label} you bought back{closed_txt} finished in the "
                f"money — closing early avoided {_money(delta)}.")
    if delta < 0:
        return (f"The {label} you sold{closed_txt} was worth {_money(delta)} "
                f"more at expiry than your exit.")
    return (f"The {label} you sold{closed_txt} — that exit beat the expiry "
            f"outcome by {_money(delta)}.")


def verdicts_landed(df, start, end):
    """Verdicts that MATURED in [start, end] — early closes whose expiry
    date arrived in the window, making the counterfactual knowable. This
    is the Daily Review's "news since you last looked": each item is new
    information on the day it lands, not a re-read of a lifetime total."""
    df = _prep(df)
    if df.empty or "gradeable_early_close" not in df.columns:
        return []
    rows = df[df["gradeable_early_close"]
              & df["early_close_vs_expiry_delta"].notna()
              & df["option_expiry"].notna()
              & (df["option_expiry"] >= start)
              & (df["option_expiry"] <= end)]
    if rows.empty:
        return []
    rows = rows.reindex(
        rows["early_close_vs_expiry_delta"].abs()
        .sort_values(ascending=False).index)
    out = []
    for _, r in rows.iterrows():
        delta = float(r["early_close_vs_expiry_delta"])
        out.append({
            "symbol": r["symbol"],
            "landed": r["option_expiry"].isoformat(),
            "landed_label": pd.Timestamp(r["option_expiry"]).strftime("%a %b %-d"),
            # action → the page feed (delta rendered separately);
            # sentence → the email digest (self-contained prose).
            "action": _verdict_action(r),
            "sentence": _verdict_sentence(r),
            "delta": round(delta, 2),
        })
    return out


def verdicts_pending(df, today):
    """The open loop: early closes whose expiry hasn't arrived yet. Each
    one is a verdict already in the mail — {n, next_date_label, items}."""
    df = _prep(df)
    if df.empty or "close_type" not in df.columns:
        return None
    rows = df[(df["close_type"] == "Closed")
              & df["close_date"].notna()
              & df["option_expiry"].notna()
              & (df["close_date"] < df["option_expiry"])
              & (df["option_expiry"] >= today)]
    if rows.empty:
        return None
    rows = rows.sort_values("option_expiry")
    items = []
    for _, r in rows.iterrows():
        items.append({
            "symbol": r["symbol"],
            "label": _contract_label(r),
            "short_label": _short_label(r),
            "expiry": r["option_expiry"].isoformat(),
            "expiry_label": pd.Timestamp(r["option_expiry"]).strftime("%a %b %-d"),
            "days_away": int((r["option_expiry"] - today).days),
        })
    nxt = items[0]
    return {
        "n": len(items),
        "next_date_label": nxt["expiry_label"],
        "next_symbol": nxt["symbol"],
        "next_label": nxt["label"],
        "next_short_label": nxt["short_label"],
        "items": items,
    }


# ── Live open-option record (Daily Review) ───────────────────────────────

def open_option_record(df, today):
    """The record so far on OPEN contracts — the number a premium seller
    checks daily. Strictly observational: premium captured / mark vs cost
    and days left; never a suggestion.

    Returns {"shorts": [...], "longs": [...]} or None. Only contracts the
    live broker snapshot actually carries (a never-snapshotted open
    contract has no mark to report)."""
    if df is None or df.empty:
        return None
    out = df.copy()
    for col in ("premium_received", "premium_paid", "current_market_value",
                "current_unrealized_pnl", "option_strike",
                "contracts_sold_to_open", "contracts_bought_to_open"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["option_expiry"] = pd.to_datetime(
        out["option_expiry"], errors="coerce").dt.date
    out = out[out["option_expiry"].notna()
              & (out["option_expiry"] >= today)
              & ((out["current_market_value"].abs() > 0.004)
                 | (out["current_unrealized_pnl"].abs() > 0.004))]
    if out.empty:
        return None

    shorts, longs = [], []
    for _, r in out.iterrows():
        days_left = int((r["option_expiry"] - today).days)
        base = {
            "symbol": str(r.get("symbol") or "").upper(),
            "label": _contract_label(r),
            "days_left": days_left,
            "unrealized": round(float(r["current_unrealized_pnl"]), 2),
        }
        if str(r.get("direction")) == "Sold" and r["premium_received"] > 0.004:
            pct = float(r["current_unrealized_pnl"]) / float(r["premium_received"])
            shorts.append({
                **base,
                "premium": round(float(r["premium_received"]), 2),
                "captured_pct": max(-999, min(999, round(pct * 100))),
            })
        elif str(r.get("direction")) == "Bought" and r["premium_paid"] < -0.004:
            paid = -float(r["premium_paid"])
            longs.append({
                **base,
                "paid": round(paid, 2),
                "mark": round(float(r["current_market_value"]), 2),
                "change_pct": max(-999, min(999, round(
                    float(r["current_unrealized_pnl"]) / paid * 100))),
            })
    if not shorts and not longs:
        return None
    shorts.sort(key=lambda x: x["days_left"])
    longs.sort(key=lambda x: x["days_left"])
    return {"shorts": shorts, "longs": longs}


# ── Position review integration ──────────────────────────────────────────

def symbol_execution_sentences(df, min_graded=MIN_GRADED_SYMBOL):
    """0-2 mirror sentences for one symbol's Position review card."""
    df = _prep(df)
    if df.empty or "gradeable_early_close" not in df.columns:
        return []
    graded = df[df["gradeable_early_close"]
                & df["early_close_vs_expiry_delta"].notna()]
    if len(graded) < min_graded:
        return []
    out = []
    net = float(graded["early_close_vs_expiry_delta"].sum())
    worthless = int(graded["expired_worthless"].sum())
    if net < -1:
        verdict = f"closing early gave up {_money(net)} vs holding"
    elif net > 1:
        verdict = f"closing early came out {_money(net)} ahead"
    else:
        verdict = "closing early came out about even"
    out.append(f"Early exits here: {worthless} of {len(graded)} expired "
               f"worthless anyway — {verdict}.")
    rolls = graded[graded["was_rolled"]]
    if len(rolls) >= 2:
        untested = int(rolls["expired_worthless"].sum())
        out.append(f"{untested} of {len(rolls)} rolls were never tested — "
                   f"the original strike expired worthless.")
    return out


def exit_notes(df):
    """{(tenant/account key, trade_symbol): verdict sentence} for the review.

    The same OCC contract can be traded in multiple physical accounts, so
    ``trade_symbol`` alone is not unique.  Keys mirror
    ``position_story._normalize_fills``: broker-stable ``tenant_id`` first,
    account display label only for legacy rows without a tenant id.
    """
    df = _prep(df)
    if df.empty or "gradeable_early_close" not in df.columns:
        return {}
    notes = {}
    graded = df[df["gradeable_early_close"]
                & df["early_close_vs_expiry_delta"].notna()
                & (df["early_close_vs_expiry_delta"].abs() >= MIN_NOTE_DELTA)]
    for _, row in graded.iterrows():
        delta = float(row["early_close_vs_expiry_delta"])
        tsym = row["trade_symbol"]
        if not tsym:
            continue
        raw_tenant_id = row.get("tenant_id")
        tenant_id = (
            ""
            if raw_tenant_id is None or pd.isna(raw_tenant_id)
            else str(raw_tenant_id).strip()
        )
        state_key = tenant_id or str(row.get("account") or "")
        if row["was_rolled"]:
            if row["expired_worthless"]:
                note = ("After the fact: the strike you rolled "
                        "away from expired worthless — the roll was never "
                        "tested.")
            else:
                note = (f"After the fact: the original strike "
                        f"finished in the money — rolling sidestepped "
                        f"{_money(delta)} of settlement value.")
        elif str(row["direction"]) == "Sold":
            if delta < 0:
                note = (f"After the fact: this contract expired "
                        f"worthless — the early close gave up {_money(delta)} "
                        f"vs holding."
                        if row["expired_worthless"] else
                        f"After the fact: holding to expiry would "
                        f"have come out {_money(delta)} better.")
            else:
                note = (f"After the fact: the strike finished in "
                        f"the money — closing early avoided {_money(delta)}.")
        else:
            if delta < 0:
                note = (f"After the fact: by expiry this contract "
                        f"was worth {_money(delta)} more than the exit "
                        f"price.")
            else:
                note = (f"After the fact: this exit beat the "
                        f"expiry outcome by {_money(delta)}.")
        notes[(state_key, tsym)] = note
    return notes
