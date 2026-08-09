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

MIN_GRADED_PROFILE = 5
MIN_GRADED_SYMBOL = 2
MIN_MARKED_CONTRACTS = 3
# Below this the verdict is rounding noise, not evidence.
MIN_NOTE_DELTA = 20.0


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
            out[col] = out[col].fillna(False).astype(bool)
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

def summarize_execution(df, min_graded=MIN_GRADED_PROFILE):
    """Fold the exit-quality rows into the Trader Profile card context.

    Returns None when there isn't enough graded evidence yet (the
    sufficiency gate), else a dict with sentences / chips / examples.
    """
    df = _prep(df)
    if df.empty or "gradeable_early_close" not in df.columns:
        return None
    graded = df[df["gradeable_early_close"]
                & df["early_close_vs_expiry_delta"].notna()]
    if len(graded) < min_graded:
        return None

    sentences = []
    chips = [{"label": "Contracts graded", "value": f"{len(graded)}"}]

    # 1. Early buybacks on short options (rolls handled separately).
    shorts = graded[(graded["direction"] == "Sold") & ~graded["was_rolled"]]
    if len(shorts) >= 2:
        worthless = shorts[shorts["expired_worthless"]]
        cost = float(-shorts.loc[
            shorts["early_close_vs_expiry_delta"] < 0,
            "early_close_vs_expiry_delta"].sum())
        saved = float(shorts.loc[
            shorts["early_close_vs_expiry_delta"] > 0,
            "early_close_vs_expiry_delta"].sum())
        net = float(shorts["early_close_vs_expiry_delta"].sum())
        s = (f"You bought back {len(shorts)} short contracts before expiry; "
             f"{len(worthless)} of them went on to expire worthless.")
        bits = []
        if cost > 1:
            bits.append(f"those early closes gave up {_money(cost)} vs holding")
        if saved > 1:
            n_saved = int((shorts["early_close_vs_expiry_delta"] > 0).sum())
            bits.append(f"{n_saved} finished in the money, and closing early "
                        f"avoided {_money(saved)}")
        if bits:
            s += " Against the expiry record, " + "; ".join(bits) + "."
        sentences.append(s)
        chips.append({"label": "Early buybacks vs expiry",
                      "value": _signed(net)})

    # 2. Roll necessity — the closed leg's own expiry answers whether the
    #    strike ever got run over.
    rolls = graded[graded["was_rolled"]]
    if len(rolls) >= 2:
        untested = int(rolls["expired_worthless"].sum())
        sidestepped = float(rolls.loc[
            rolls["early_close_vs_expiry_delta"] > 0,
            "early_close_vs_expiry_delta"].sum())
        s = (f"Of the {len(rolls)} strikes you rolled away from, {untested} "
             f"went on to expire worthless — those rolls were never tested.")
        if sidestepped > 1:
            s += (f" The rest were: rolling sidestepped {_money(sidestepped)} "
                  f"of settlement value at the original strikes.")
        sentences.append(s)
        chips.append({"label": "Rolls never tested",
                      "value": f"{untested} of {len(rolls)}"})

    # 3. Long options sold before expiry.
    longs = graded[(graded["direction"] == "Bought") & ~graded["was_rolled"]]
    if len(longs) >= 2:
        n_better = int((longs["early_close_vs_expiry_delta"] > 0).sum())
        net = float(longs["early_close_vs_expiry_delta"].sum())
        verdict = ("came out ahead of" if net >= 0 else "gave up value vs")
        sentences.append(
            f"On long options, you sold before expiry {len(longs)} times; "
            f"selling early beat holding to expiry in {n_better} of them. "
            f"Net across all of these, your exits {verdict} the expiry "
            f"outcome by {_money(net)}."
        )
        chips.append({"label": "Long exits vs expiry", "value": _signed(net)})

    # 4. Expiry discipline (no counterfactual needed — it happened).
    if "close_type" in df.columns:
        expired = df[df["close_type"].isin(["Expired", "ExpiredOTM"])
                     & (df["direction"] == "Sold") & (df["realized_pnl"] > 0)]
        if len(expired) >= 2:
            kept = float(expired["realized_pnl"].sum())
            chips.append({"label": "Held to worthless expiry",
                          "value": f"{len(expired)} · {_money(kept)} kept"})

    # 5. Marks record — unlocks as daily-mark coverage accumulates.
    marked = df[df["data_reliable"] & (df["peak_unrealized_pnl"] > 50)
                & (df["realized_pnl"] > 0)]
    if len(marked) >= MIN_MARKED_CONTRACTS:
        capture = (marked["realized_pnl"] / marked["peak_unrealized_pnl"]) \
            .clip(0, 1)
        med = float(capture.median()) * 100
        sentences.append(
            f"On the {len(marked)} winning contracts with daily-mark "
            f"coverage, you captured a median {med:.0f}% of the best exit "
            f"the marks recorded."
        )
        chips.append({"label": "Median peak capture", "value": f"{med:.0f}%"})
    else:
        sentences.append(
            "Daily option marks are still accumulating for your accounts — "
            "peak-capture grading switches on per contract as coverage "
            "becomes reliable."
        )

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
        "sentences": sentences,
        "chips": chips[:6],
        "examples": examples,
    }


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
    lead = (f"{len(graded)} of the contracts you closed early here have a "
            f"known expiry outcome: {worthless} went on to expire worthless")
    if net < -1:
        out.append(f"{lead}, and the early exits gave up {_money(net)} "
                   f"vs holding to expiry.")
    elif net > 1:
        out.append(f"{lead}, and the early exits came out {_money(net)} "
                   f"ahead of the expiry outcome.")
    else:
        out.append(f"{lead}.")
    rolls = graded[graded["was_rolled"]]
    if len(rolls) >= 2:
        untested = int(rolls["expired_worthless"].sum())
        out.append(f"{untested} of the {len(rolls)} strikes you rolled away "
                   f"from were never tested at their original expiry.")
    return out


def exit_notes(df):
    """{trade_symbol: verdict sentence} for the day-by-day review — appended
    to the buyback / sale sentence on the day the contract closed, so the
    story and the grade read as one voice."""
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
        if row["was_rolled"]:
            if row["expired_worthless"]:
                note = ("The record after the fact: the strike you rolled "
                        "away from expired worthless — the roll was never "
                        "tested.")
            else:
                note = (f"The record after the fact: the original strike "
                        f"finished in the money — rolling sidestepped "
                        f"{_money(delta)} of settlement value.")
        elif str(row["direction"]) == "Sold":
            if delta < 0:
                note = (f"The record after the fact: this contract expired "
                        f"worthless — the early close gave up {_money(delta)} "
                        f"vs holding."
                        if row["expired_worthless"] else
                        f"The record after the fact: holding to expiry would "
                        f"have come out {_money(delta)} better.")
            else:
                note = (f"The record after the fact: the strike finished in "
                        f"the money — closing early avoided {_money(delta)}.")
        else:
            if delta < 0:
                note = (f"The record after the fact: by expiry this contract "
                        f"was worth {_money(delta)} more than the exit "
                        f"price.")
            else:
                note = (f"The record after the fact: this exit beat the "
                        f"expiry outcome by {_money(delta)}.")
        notes[tsym] = note
    return notes
