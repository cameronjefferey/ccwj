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
        recs.append(rec)
    return recs


def _watch_item(members, habit, typical_dte):
    days = min(int(m["_days"]) for m in members)
    symbol = members[0]["symbol"]
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
    if habit == "roll" and (in_usual or days <= 7):
        prompt = (f"You usually roll. {name} — {days} day"
                  f"{'s' if days != 1 else ''} left. Roll it, or let this "
                  f"one expire?")
    elif habit == "expire" and days <= 7:
        prompt = (f"You usually hold to expiry. {name} — {days} day"
                  f"{'s' if days != 1 else ''} left.")
    elif in_usual:
        prompt = (f"Your typical early close is around {typical_dte} DTE. "
                  f"{name} is inside that window. Close it, or hold?")
    elif days == 0:
        prompt = f"{name} expires today."
    elif days <= 7:
        prompt = (f"{name} expires in {days} day"
                  f"{'s' if days != 1 else ''}.")
    else:
        prompt = f"{name} — {days} days left."
    return {
        "symbol": symbol,
        "label": label,
        "structure": structure,
        "days_left": days,
        "prompt": prompt,
        "in_usual_window": bool(in_usual),
    }


def build_this_week(open_df, exec_df=None, today=None):
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
        items.append(_watch_item(members, habit, typical_dte))
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
    sub = "A question, not a recommendation — holding also keeps the risk."
    if typical_dte is not None:
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
                       tz_name=None):
    """Both cards, or None when there is nothing to say yet."""
    today = today or today_for_user(tz_name)
    this_week = build_this_week(open_df, exec_df, today=today)
    last_week = build_last_week(trades_df, exec_df, today=today)
    has_watch = bool(this_week["watches"])
    has_last = bool(last_week["facts"] or last_week["activity"]["fills"]
                    or last_week["tone"])
    if not has_watch and not has_last:
        return None
    return {"this_week": this_week, "last_week": last_week}
