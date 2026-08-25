"""Recurring this-week / last-week loop for the Trader Profile.

The lifetime novel is worth one long read. These two blocks are why
someone opens /story again: what is on the clock this week, and whether
last week looked like them. Questions, not advice — "you usually roll;
this one has 5 days left" is a mirror, not a recommendation.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from app.execution_quality import (
    _cluster_structure_groups,
    _short_label,
    _signed,
    _structure_kind,
    _structure_name,
)
from app.position_story import _money

# Prior completed weeks needed before we call a week "like" / "unlike".
MIN_BASELINE_WEEKS = 4
# Open contracts further out than this stay off the this-week watch.
WATCH_HORIZON_DAYS = 14
# Shorts we usually close around N DTE: flag when days_left is inside
# that window plus a small cushion.
USUAL_WINDOW_PAD = 2
THIS_WEEK_CAP = 8
# Leftover-vs-expiry claim at a DTE needs a real sample (same bar as
# the Execution Review card). Below this we only show live P&L.
MIN_LEFTOVER_SAMPLE = 5
# Match historical early closes to the live contract's days left.
# Wider as the live DTE grows: a 10-day watch looks at ~5–15 DTE exits,
# never at the 2-DTE habit.
LEFTOVER_DTE_PAD = 2
# Ignore leftover % below this — rounding, not a pattern.
MIN_LEFTOVER_PCT = 5
# Sidestep $ needs the same noise floor as day-row verdicts.
MIN_SIDESTEP_DOLLARS = 20.0
# "Hold this structure longer" needs two real samples.
MIN_HOLD_COMPARE = 5
HOLD_LONGER_GAP_DAYS = 3


def today_for_user(tz_name=None):
    """User-profile date so Monday 1am ET isn't still 'last week' in UTC."""
    tz = (tz_name or "America/New_York").strip() or "America/New_York"
    try:
        return datetime.now(ZoneInfo(tz)).date()
    except Exception:
        return date.today()


def week_bounds(today):
    """ISO week (Monday start): this week's Monday, last week's Mon–Sun."""
    this_start = today - timedelta(days=today.weekday())
    last_start = this_start - timedelta(days=7)
    last_end = this_start - timedelta(days=1)
    return this_start, last_start, last_end


def _week_label(start, end=None):
    end = end or (start + timedelta(days=4))  # Friday
    if start.month == end.month:
        return f"{start.strftime('%b %-d')}–{end.strftime('%-d')}"
    return f"{start.strftime('%b %-d')}–{end.strftime('%b %-d')}"


def _as_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        return pd.Timestamp(val).date()
    except Exception:
        return None


def _prep_trades(df):
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["_d"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.date
    out["_sym"] = out["symbol"].astype(str).str.strip().str.upper()
    out["_amt"] = pd.to_numeric(out.get("amount"), errors="coerce").fillna(0.0)
    out["_action"] = out["action"].astype(str)
    return out.dropna(subset=["_d"])


def _in_range(series, start, end):
    return (series >= start) & (series <= end)


def _count_rolls(week_df):
    """Same-day close + open on one tenant+symbol ≈ a roll.

    Matches the Daily Review idea without importing that page's pairing
    (order-independent BTC/STC + STO/BTO, different contract).
    """
    if week_df is None or week_df.empty:
        return 0
    closes = {"option_buy_to_close", "option_sell_to_close"}
    opens = {"option_sell_to_open", "option_buy_to_open"}
    keys = ["_sym", "_d"]
    if "tenant_id" in week_df.columns:
        keys = ["tenant_id"] + keys
    rolls = 0
    for _, g in week_df.groupby(keys, dropna=False):
        acts = set(g["_action"])
        if acts & closes and acts & opens:
            tsyms = g["trade_symbol"].astype(str).nunique()
            if tsyms >= 2:
                rolls += 1
    return rolls


def _week_activity(trades, start, end):
    empty = {
        "fills": 0, "trade_days": 0, "symbols": 0, "premium": 0.0,
        "rolls": 0, "new_symbols": 0, "expired": 0, "buybacks": 0,
    }
    if trades is None or trades.empty:
        return empty
    g = trades[_in_range(trades["_d"], start, end)]
    if g.empty:
        return empty
    first_by_sym = trades.groupby("_sym")["_d"].min()
    new_syms = int((first_by_sym.loc[first_by_sym.index.isin(g["_sym"])]
                    .between(start, end)).sum()) if len(first_by_sym) else 0
    return {
        "fills": int(len(g)),
        "trade_days": int(g["_d"].nunique()),
        "symbols": int(g["_sym"].nunique()),
        "premium": float(g.loc[
            (g["_action"] == "option_sell_to_open") & (g["_amt"] > 0), "_amt"
        ].sum()),
        "rolls": _count_rolls(g),
        "new_symbols": new_syms,
        "expired": int((g["_action"] == "option_expired").sum()),
        "buybacks": int((g["_action"] == "option_buy_to_close").sum()),
    }


def _prior_week_starts(trades, before):
    """Mondays of completed weeks strictly before ``before`` that had fills."""
    if trades is None or trades.empty:
        return []
    prior = trades[trades["_d"] < before]
    if prior.empty:
        return []
    mondays = sorted({
        d - timedelta(days=d.weekday()) for d in prior["_d"].unique()
    })
    return [m for m in mondays if m + timedelta(days=6) < before]


def _median(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    s = pd.Series(vals, dtype="float64")
    return float(s.median())


def _habit(exec_df):
    """roll / expire / mixed / None from the lifetime short-option record."""
    if exec_df is None or exec_df.empty:
        return None, None
    df = exec_df.copy()
    if "direction" in df.columns:
        shorts = df[df["direction"].astype(str) == "Sold"]
    else:
        shorts = df
    if shorts.empty:
        return None, None
    rolled = 0
    expired = 0
    if "was_rolled" in shorts.columns:
        rolled = int(shorts["was_rolled"].fillna(False).astype(bool).sum())
    if "close_type" in shorts.columns:
        expired = int(shorts["close_type"].isin(["Expired", "ExpiredOTM"]).sum())
    dtes = []
    if "dte_at_close" in shorts.columns:
        early = shorts
        if "gradeable_early_close" in shorts.columns:
            early = shorts[shorts["gradeable_early_close"].fillna(False).astype(bool)]
        dtes = pd.to_numeric(early["dte_at_close"], errors="coerce").dropna()
        dtes = [int(x) for x in dtes.tolist() if x >= 0]
    typical_dte = int(round(_median(dtes))) if len(dtes) >= 5 else None
    total = rolled + expired
    if total < 5:
        return None, typical_dte
    if rolled >= 0.6 * total:
        return "roll", typical_dte
    if expired >= 0.6 * total:
        return "expire", typical_dte
    return "mixed", typical_dte


def _unlike(actual, typical, *, burst=3):
    """True when this week is a real break from the median week."""
    if typical is None:
        return False
    if typical < 1 and actual >= burst:
        return True
    if typical >= 3 and actual == 0:
        return True
    if typical >= 2 and actual >= 2 * typical + 0.5:
        return True
    return False


def _open_records(open_df, today):
    if open_df is None or open_df.empty:
        return []
    recs = []
    for _, r in open_df.iterrows():
        exp = _as_date(r.get("option_expiry"))
        if exp is None or exp < today:
            continue
        days = (exp - today).days
        if days > WATCH_HORIZON_DAYS:
            continue
        rec = r.to_dict()
        rec["option_expiry"] = exp
        rec["symbol"] = str(r.get("symbol") or "").strip().upper()
        rec["_days"] = days
        rec["_pnl"] = _as_float(r.get("current_unrealized_pnl"))
        recs.append(rec)
    return recs


def _as_float(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _watch_pnl(members):
    if not any("current_unrealized_pnl" in m for m in members):
        return None
    return round(sum(_as_float(m.get("current_unrealized_pnl"))
                     for m in members), 2)


def _horizon_pad(days):
    """How far a historical close may sit from this watch's days left."""
    if days <= 4:
        return LEFTOVER_DTE_PAD
    if days <= 8:
        return 3
    return 5


def _option_side(val):
    ot = str(val or "").upper()
    if ot.startswith("C"):
        return "C"
    if ot.startswith("P"):
        return "P"
    return ""


def _watch_kind(members):
    """Structure key for leftover matching. Single short call ≠ 2-DTE puts."""
    if len(members) > 1:
        return _structure_kind(members) or "spread"
    m = members[0]
    side = _option_side(m.get("option_type"))
    direc = str(m.get("direction") or "")
    if direc == "Sold" and side == "C":
        return "short_call"
    if direc == "Sold" and side == "P":
        return "short_put"
    if direc == "Bought" and side == "C":
        return "long_call"
    if direc == "Bought" and side == "P":
        return "long_put"
    if direc == "Sold":
        return "short"
    return None


def _exec_kind(row):
    direc = str(row.get("direction") or "")
    side = _option_side(row.get("option_type"))
    if direc == "Sold" and side == "C":
        return "short_call"
    if direc == "Sold" and side == "P":
        return "short_put"
    if direc == "Bought" and side == "C":
        return "long_call"
    if direc == "Bought" and side == "P":
        return "long_put"
    if direc == "Sold":
        return "short"
    return None


def _kind_noun(kind):
    return {
        "short_call": "short call",
        "short_put": "short put",
        "long_call": "long call",
        "long_put": "long put",
    }.get(kind) or "short"


# Warehouse labels from positions_summary / int_strategy_classification.
# Never say "naked" without this evidence — live option rows have no
# share-coverage column.
_NAKED_CALL = "Naked Call"
_COVERED_CALLS = {
    "Covered Call",
    "Partially Covered Call",
    "Poor Man Covered Call",
}
_CASH_SECURED_PUT = "Cash-Secured Put"


def _open_strategy_labels(symbol, strategies_df):
    if strategies_df is None or strategies_df.empty:
        return set()
    if "symbol" not in strategies_df.columns or "strategy" not in strategies_df.columns:
        return set()
    sym = str(symbol or "").strip().upper()
    col = strategies_df["symbol"].astype(str).str.strip().str.upper()
    rows = strategies_df.loc[col == sym, "strategy"]
    return {str(s).strip() for s in rows.tolist() if pd.notna(s) and str(s).strip()}


def _display_noun(kind, symbol, strategies_df):
    """Name this live structure. 'naked call' only when classification
    says so and no covered-call label shares the symbol."""
    labels = _open_strategy_labels(symbol, strategies_df)
    if kind == "short_call":
        naked = _NAKED_CALL in labels
        covered = bool(labels & _COVERED_CALLS)
        if naked and not covered:
            return "naked call"
        if covered and not naked:
            return "covered call"
        return "short call"
    if kind == "short_put":
        if _CASH_SECURED_PUT in labels:
            return "cash-secured put"
        return "short put"
    return _kind_noun(kind)


def _plural_noun(noun):
    if not noun or noun.endswith("s"):
        return noun or "shorts"
    return noun + "s"


def _filter_kind(df, kind):
    if df is None or df.empty or not kind:
        return df
    if kind not in ("short_call", "short_put", "long_call", "long_put"):
        return df
    kinds = df.apply(_exec_kind, axis=1)
    return df[kinds == kind]


def _dte_band(shorts, center, pad=LEFTOVER_DTE_PAD):
    dte = pd.to_numeric(shorts["dte_at_close"], errors="coerce")
    return shorts[(dte >= center - pad) & (dte <= center + pad)]


def _graded_shorts(exec_df):
    if exec_df is None or exec_df.empty:
        return pd.DataFrame()
    if "gradeable_early_close" not in exec_df.columns:
        return pd.DataFrame()
    if "early_close_vs_expiry_delta" not in exec_df.columns:
        return pd.DataFrame()
    df = exec_df.copy()
    if "direction" in df.columns:
        shorts = df[df["direction"].astype(str) == "Sold"]
    else:
        shorts = df
    graded = shorts[shorts["gradeable_early_close"].fillna(False).astype(bool)
                    & shorts["early_close_vs_expiry_delta"].notna()]
    if graded.empty or "dte_at_close" not in graded.columns:
        return pd.DataFrame()
    return graded


def _leftover_from_sample(sample, *, pad, horizon="this_dte"):
    if sample is None or len(sample) < MIN_LEFTOVER_SAMPLE:
        return None
    rolled = False
    if "was_rolled" in sample.columns:
        rolled = float(sample["was_rolled"].fillna(False).astype(bool)
                       .mean()) >= 0.6
    dte_med = int(round(float(pd.to_numeric(
        sample["dte_at_close"], errors="coerce").median())))

    if "expired_worthless" in sample.columns:
        otm = sample[sample["expired_worthless"].fillna(False).astype(bool)]
    else:
        otm = sample.iloc[0:0]
    if (len(otm) >= MIN_LEFTOVER_SAMPLE
            and "premium_received" in otm.columns):
        prem = pd.to_numeric(otm["premium_received"], errors="coerce")
        delta = pd.to_numeric(otm["early_close_vs_expiry_delta"],
                              errors="coerce")
        usable = otm[(prem > 1) & (delta < 0)]
        if len(usable) >= MIN_LEFTOVER_SAMPLE:
            pcts = ((-delta[usable.index] / prem[usable.index])
                    .clip(lower=0, upper=2))
            med_pct = float(pcts.median()) * 100
            if med_pct >= MIN_LEFTOVER_PCT:
                return {
                    "kind": "otm_leftover_pct",
                    "pct": int(round(med_pct)),
                    "n": int(len(usable)),
                    "dte": int(round(float(pd.to_numeric(
                        usable["dte_at_close"], errors="coerce").median()))),
                    "rolled": bool(
                        "was_rolled" in usable.columns
                        and float(usable["was_rolled"].fillna(False)
                                  .astype(bool).mean()) >= 0.6
                    ),
                    "pad": pad,
                    "horizon": horizon,
                }

    if "expired_worthless" in sample.columns:
        itm = sample[~sample["expired_worthless"].fillna(False).astype(bool)
                     & sample["expired_worthless"].notna()]
    else:
        itm = sample.iloc[0:0]
    if len(itm) >= MIN_LEFTOVER_SAMPLE:
        delta = pd.to_numeric(itm["early_close_vs_expiry_delta"],
                              errors="coerce").dropna()
        med = float(delta.median()) if len(delta) else 0.0
        if med >= MIN_SIDESTEP_DOLLARS:
            return {
                "kind": "sidestep",
                "dollars": med,
                "n": int(len(itm)),
                "dte": dte_med,
                "rolled": rolled,
                "pad": pad,
                "horizon": horizon,
            }
    return None


def _leftover_record(exec_df, days, typical_dte=None, kind=None):
    """Leftover vs expiry for THIS structure near THIS days left.

    A 10-day short call reads short-call exits around 10 DTE — not the
    lifetime 2-DTE habit, and not put leftovers. ``typical_dte`` is
    ignored on purpose (kept so older callers don't break).
    """
    del typical_dte
    graded = _graded_shorts(exec_df)
    if graded.empty:
        return None
    scoped = _filter_kind(graded, kind)
    if scoped is None or scoped.empty:
        return None
    pad = _horizon_pad(days)
    sample = _dte_band(scoped, days, pad=pad)
    rec = _leftover_from_sample(sample, pad=pad, horizon="this_dte")
    if rec:
        rec["structure"] = kind
        return rec
    # Far watches only: pool "a week or more left" for this structure.
    # Phrase as week-plus — never as "at 10 DTE" if the median isn't.
    if days >= 8:
        dte = pd.to_numeric(scoped["dte_at_close"], errors="coerce")
        week_plus = scoped[dte >= 7]
        rec = _leftover_from_sample(week_plus, pad=pad, horizon="week_plus")
        if rec:
            rec["structure"] = kind
            return rec
    return None


def _hold_longer(exec_df, kind):
    """This structure's median exit DTE vs the trader's other shorts."""
    if kind not in ("short_call", "short_put"):
        return None
    graded = _graded_shorts(exec_df)
    if graded.empty:
        return None
    this = _filter_kind(graded, kind)
    others = graded if this is None or this.empty else graded.drop(this.index, errors="ignore")
    if this is None or len(this) < MIN_HOLD_COMPARE:
        return None
    if others is None or len(others) < MIN_HOLD_COMPARE:
        return None
    this_dte = _median(pd.to_numeric(this["dte_at_close"], errors="coerce").tolist())
    other_dte = _median(pd.to_numeric(others["dte_at_close"], errors="coerce").tolist())
    if this_dte is None or other_dte is None:
        return None
    if this_dte < other_dte + HOLD_LONGER_GAP_DAYS:
        return None
    return {
        "kind": kind,
        "this_dte": int(round(this_dte)),
        "other_dte": int(round(other_dte)),
    }


def _leftover_dte(record, days=None):
    dte = record["dte"]
    pad = int(record.get("pad") or LEFTOVER_DTE_PAD)
    if (days is not None and record.get("horizon") != "week_plus"
            and abs(int(days) - int(dte)) <= pad):
        return int(days)
    return int(dte)


def _leftover_clause(record, days, noun, *, name_structure=False):
    """Uncapitalized leftover clause so it can glue onto 'hold too long'."""
    dte = _leftover_dte(record, days)
    named = f"on a {noun} " if name_structure else ""
    if record["kind"] == "otm_leftover_pct":
        if record.get("horizon") == "week_plus":
            return (f"an exit {named}with a week or more left "
                    f"instead of expiry typically costs you "
                    f"{record['pct']}% of the credit")
        return (f"an exit {named}at {dte} DTE instead of expiry typically "
                f"costs you {record['pct']}% of the credit")
    if record.get("horizon") == "week_plus":
        return (f"an exit {named}with a week or more left typically "
                f"sidestepped {_money(record['dollars'])} vs expiry")
    return (f"an exit {named}at {dte} DTE typically sidestepped "
            f"{_money(record['dollars'])} vs expiry")


def _leftover_sentence(record, days=None, noun=None):
    noun = noun or _kind_noun(record.get("structure"))
    clause = _leftover_clause(record, days, noun, name_structure=True)
    return clause[0].upper() + clause[1:] + "."


def _hold_longer_sentence(hold, noun=None):
    noun = noun or _kind_noun(hold["kind"])
    return (f"You hold {_plural_noun(noun)} too long "
            f"(median {hold['this_dte']} DTE vs {hold['other_dte']} "
            f"on your other shorts).")


def _pattern_sentence(hold, leftover, days, noun):
    """One punchy claim when both facts exist — the only-here sentence.

    'You hold naked calls too long, and an exit at 10 DTE instead of
    expiry typically costs you 18% of the credit.'
    """
    combine = bool(
        hold and leftover and leftover.get("kind") == "otm_leftover_pct"
    )
    leftover_clause = (
        _leftover_clause(leftover, days, noun, name_structure=not combine)
        if leftover else None
    )
    if combine:
        return (f"You hold {_plural_noun(noun)} too long, and "
                f"{leftover_clause}.")
    if leftover_clause:
        return leftover_clause[0].upper() + leftover_clause[1:] + "."
    if hold:
        return _hold_longer_sentence(hold, noun)
    return None


def _net_credit(members):
    """Net premium on the live structure. Short credit is +, long debit is −."""
    rec = sum(_as_float(m.get("premium_received")) for m in members)
    paid = sum(_as_float(m.get("premium_paid")) for m in members)
    return rec + paid


def _credit_sentence(pnl, credit):
    """This position's mark vs its own credit — never a lifetime leftover."""
    if pnl is None or credit is None or abs(credit) < 1:
        return None
    if credit > 1:
        if pnl >= 0:
            pct = int(round(min(999, max(-999, pnl / credit * 100))))
            return (f"You've captured {pct}% of the {_money(credit)} "
                    f"credit so far.")
        given = -pnl
        extra = given - credit
        if extra >= 1:
            return (f"The mark has given back the {_money(credit)} credit "
                    f"and {_money(extra)} more.")
        return (f"The mark has given back {_money(given)} of the "
                f"{_money(credit)} credit.")
    debit = -credit
    if pnl >= 0:
        return f"The long is {_signed(pnl)} on a {_money(debit)} debit."
    return f"The long is {_signed(pnl)} against a {_money(debit)} debit."


def _distance_sentence(days, typical_dte, in_usual):
    if typical_dte is None:
        return None
    if in_usual:
        return None
    if days > typical_dte + USUAL_WINDOW_PAD:
        gap = days - typical_dte
        return (f"{gap} day{'s' if gap != 1 else ''} before your usual "
                f"{typical_dte} DTE close.")
    return None


def _habit_clause(name, days, habit, typical_dte, in_usual):
    if habit == "roll" and (in_usual or days <= 7):
        return (f"You usually roll. {name} — {days} day"
                f"{'s' if days != 1 else ''} left. Roll it, or let this "
                f"one expire?")
    if habit == "expire" and days <= 7:
        return (f"You usually hold to expiry. {name} — {days} day"
                f"{'s' if days != 1 else ''} left.")
    if in_usual:
        return (f"Your typical early close is around {typical_dte} DTE. "
                f"{name} is inside that window. Close it, or hold?")
    if days == 0:
        return f"{name} expires today."
    if days <= 7:
        return (f"{name} expires in {days} day"
                f"{'s' if days != 1 else ''}.")
    return f"{name} — {days} days left."


def _leftover_applies(leftover, days):
    """Leftover is about this structure near this expiry — not a 2-DTE stamp."""
    if not leftover:
        return False
    if leftover.get("horizon") == "week_plus":
        return int(days) >= 8
    try:
        pad = int(leftover.get("pad") or LEFTOVER_DTE_PAD)
        return abs(int(leftover["dte"]) - int(days)) <= pad
    except (TypeError, ValueError, KeyError):
        return False


def _lead_sentence(pnl, days):
    if pnl is None:
        return None
    lead = f"Currently {_signed(pnl)}"
    if days == 0:
        return lead + " · expires today"
    return lead + f" with {days} day{'s' if days != 1 else ''} left"


def _watch_item(members, habit, typical_dte, leftover=None, hold=None,
                strategies_df=None):
    days = min(int(m["_days"]) for m in members)
    symbol = members[0]["symbol"]
    kind = _watch_kind(members)
    noun = _display_noun(kind, symbol, strategies_df)
    if len(members) > 1:
        name = _structure_name(members)
        structure = _structure_kind(members)
        label = name
    else:
        name = _short_label(members[0])
        structure = None
        label = name
    in_usual = (
        typical_dte is not None and days <= typical_dte + USUAL_WINDOW_PAD
    )
    pnl = _watch_pnl(members)
    credit = _net_credit(members)
    use_leftover = _leftover_applies(leftover, days)
    if not use_leftover:
        leftover = None
    lead = _lead_sentence(pnl, days)
    bits = []
    pattern = _pattern_sentence(hold, leftover, days, noun)
    if pattern:
        bits.append(pattern)
        mark = _credit_sentence(pnl, credit)
        if leftover is None and hold and mark and pnl is not None and pnl < 0:
            bits.append(mark)
    else:
        far = _distance_sentence(days, typical_dte, in_usual)
        mark = _credit_sentence(pnl, credit)
        if far and mark:
            bits.append(f"{far} {mark}")
        elif far:
            bits.append(far)
        elif habit == "expire" and days <= 7:
            bits.append("You usually hold to expiry.")
        elif habit == "roll" and (in_usual or days <= 7):
            bits.append("You usually roll — roll it, or let this one expire?")
        elif in_usual and typical_dte is not None:
            bits.append(f"Your typical early close is around {typical_dte} DTE.")
        elif mark:
            bits.append(mark)
    tail = " ".join(bits) if bits else None
    if lead and tail:
        prompt = f"{lead}. {tail}"
    elif lead:
        prompt = f"{lead}."
    elif tail:
        prompt = tail
    else:
        prompt = _habit_clause(name, days, habit, typical_dte, in_usual)
    return {
        "symbol": symbol,
        "label": label,
        "structure": structure,
        "days_left": days,
        "pnl": pnl,
        "pnl_text": _signed(pnl) if pnl is not None else None,
        "prompt": prompt,
        "in_usual_window": bool(in_usual),
        "leftover": leftover,
    }


def build_this_week(open_df, exec_df=None, today=None, strategies_df=None):
    """Forward-looking watch: expiries in the next 14 days, questioned
    against the trader's own roll / expire habit."""
    today = today or date.today()
    habit, typical_dte = _habit(exec_df)
    recs = _open_records(open_df, today)
    if not recs:
        return {
            "headline": "Nothing on the clock in the next 14 days.",
            "sub": "Open stock positions stay on Positions — this list is the options that force a decision.",
            # `watches` not `items` — see note on the populated return.
            "watches": [],
            "habit": habit,
            "typical_dte": typical_dte,
        }
    items = []
    for members in _cluster_structure_groups(recs):
        days = min(int(m["_days"]) for m in members)
        kind = _watch_kind(members)
        leftover = _leftover_record(exec_df, days, typical_dte, kind=kind)
        hold = _hold_longer(exec_df, kind)
        items.append(_watch_item(
            members, habit, typical_dte, leftover, hold,
            strategies_df=strategies_df))
    items.sort(key=lambda x: (not x["in_usual_window"], x["days_left"], x["symbol"]))
    items = items[:THIS_WEEK_CAP]
    n = len(items)
    usual_n = sum(1 for i in items if i["in_usual_window"])
    if habit == "roll" and usual_n:
        headline = (f"{usual_n} position{'s' if usual_n != 1 else ''} inside "
                    f"your usual roll window.")
    elif usual_n:
        headline = (f"{usual_n} position{'s' if usual_n != 1 else ''} inside "
                    f"your usual close window.")
    else:
        headline = (f"{n} open option{'s' if n != 1 else ''} expiring "
                    f"in the next {WATCH_HORIZON_DAYS} days.")
    sub = "Every early close also removed risk — this is the record, not a recommendation."
    if typical_dte is not None and not any(i.get("leftover") for i in items):
        sub = (f"You typically close shorts around {typical_dte} DTE. " + sub)
    return {
        "headline": headline,
        "sub": sub,
        # Named `watches`, not `items`: Jinja2 prefers attributes over keys,
        # so `this_week.items` would resolve to dict.items() and 500 the page.
        "watches": items,
        "habit": habit,
        "typical_dte": typical_dte,
    }


def build_last_week(trades_df, exec_df=None, today=None):
    """What last ISO week looked like vs the trader's median week."""
    del exec_df  # reserved: last-week graded closes can join later
    today = today or date.today()
    _, last_start, last_end = week_bounds(today)
    trades = _prep_trades(trades_df)
    last = _week_activity(trades, last_start, last_end)
    prior_starts = _prior_week_starts(trades, last_start)
    prior = [_week_activity(trades, s, s + timedelta(days=6))
             for s in prior_starts]
    can_compare = len(prior) >= MIN_BASELINE_WEEKS
    med = {
        "fills": _median([p["fills"] for p in prior]) if can_compare else None,
        "rolls": _median([p["rolls"] for p in prior]) if can_compare else None,
        "premium": _median([p["premium"] for p in prior]) if can_compare else None,
        "expired": _median([p["expired"] for p in prior]) if can_compare else None,
    }

    flags = []
    if can_compare:
        if _unlike(last["fills"], med["fills"]):
            flags.append("fills")
        if _unlike(last["rolls"], med["rolls"], burst=2):
            flags.append("rolls")
        if last["expired"] >= 2 and (med["expired"] or 0) < 1:
            flags.append("expired")
        if last["fills"] == 0 and (med["fills"] or 0) >= 3:
            flags.append("quiet")

    if flags:
        if "quiet" in flags:
            headline = "You sat last week — that's unusual."
        else:
            headline = "Last week didn't look like you."
        tone = "unlike"
    elif can_compare and last["fills"] == 0:
        headline = "A quiet week, which is typical."
        tone = "like"
    elif can_compare:
        headline = "Last week looked like you."
        tone = "like"
    elif last["fills"] or last["expired"]:
        headline = "What you did last week."
        tone = ""
    else:
        headline = "No fills last week."
        tone = ""

    facts = []
    if last["fills"] or can_compare:
        detail = "fills"
        if last["trade_days"]:
            detail += (f" across {last['trade_days']} session"
                       f"{'s' if last['trade_days'] != 1 else ''}")
        if last["symbols"]:
            detail += (f" · {last['symbols']} symbol"
                       f"{'s' if last['symbols'] != 1 else ''}")
        if can_compare and med["fills"] is not None:
            detail += f" · usual week is {med['fills']:.0f}"
        facts.append({
            "label": "Fills",
            "value": str(last["fills"]),
            "tone": "neg" if "fills" in flags or "quiet" in flags else "",
            "detail": detail,
        })
    if last["rolls"] or (can_compare and (med["rolls"] or 0) >= 1):
        detail = "same-day close + open on one symbol"
        if can_compare and med["rolls"] is not None:
            detail += f" · usual week is {med['rolls']:.0f}"
        facts.append({
            "label": "Rolls",
            "value": str(last["rolls"]),
            "tone": "neg" if "rolls" in flags else "",
            "detail": detail,
        })
    if last["premium"] > 1:
        detail = "premium sold to open"
        if can_compare and med["premium"] and med["premium"] > 1:
            detail += f" · usual week is {_money(med['premium'])}"
        facts.append({
            "label": "Premium",
            "value": _money(last["premium"]),
            "tone": "",
            "detail": detail,
        })
    if last["expired"] or "expired" in flags:
        facts.append({
            "label": "Expired",
            "value": str(last["expired"]),
            "tone": "neg" if "expired" in flags else "pos",
            "detail": "short contracts that went to expiry last week",
        })
    if last["new_symbols"]:
        facts.append({
            "label": "New symbols",
            "value": str(last["new_symbols"]),
            "tone": "",
            "detail": "first fill ever on that name",
        })
    if last["buybacks"] and not last["rolls"]:
        facts.append({
            "label": "Buybacks",
            "value": str(last["buybacks"]),
            "tone": "",
            "detail": "short options bought back last week",
        })

    return {
        "headline": headline,
        "tone": tone,
        "label": _week_label(last_start, last_end),
        "facts": facts[:6],
        "unlike": flags,
        "activity": last,
    }


def compose_story_loop(trades_df, open_df, exec_df=None, today=None,
                       tz_name=None, strategies_df=None):
    """Both cards, or None when there is nothing to say yet."""
    today = today or today_for_user(tz_name)
    this_week = build_this_week(
        open_df, exec_df, today=today, strategies_df=strategies_df)
    last_week = build_last_week(trades_df, exec_df, today=today)
    has_watch = bool(this_week["watches"])
    has_last = bool(last_week["facts"] or last_week["activity"]["fills"]
                    or last_week["tone"])
    if not has_watch and not has_last:
        return None
    return {"this_week": this_week, "last_week": last_week}
