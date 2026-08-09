"""Position story engine — plain-English narrative of a position.

Turns the raw fill stream into the story a trader would tell a friend:
"opened a wheel", "rolled the calls up and out", "kept the full premium",
"three quiet weeks of theta doing the work". Two outputs feed the
Position Detail page:

  story_items   — chronological narrative rows for the Story card. Two
                  shapes: event days (headline + raw fills underneath)
                  and interludes (what happened BETWEEN trades, computed
                  from the daily-mark chart series — the data no broker
                  dashboard has).
  story_markers — one dot per event day for the chart overlay; tooltip
                  leads with the headline.

Detection is deliberately heuristic and NEVER speculative about intent
beyond what the fills mechanically show (see AGENTS.md pattern-detection
rules: evidence, not psychology). "Cash-secured" for a short put and
"covered" for a short call describe the standard structure implied by
the position state we can see, not the trader's margin arrangement.

Dividends come from int_dividend_events (synthetic pipeline covers
JEPI-class positions where the broker never shipped explicit dividend
rows); interest / fees / transfers are account noise, not position
story, and are skipped.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

import pandas as pd

__all__ = [
    "build_position_story",
    "compose_mirror",
    "parse_occ",
    "_behavior_candidates",
    "_new_stats",
    "_money",
    "_span_text",
]


# OCC option symbol: "RKLB 250117C00037000" -> expiry / type / strike.
# Lazy prefix tolerates underlyings that themselves contain spaces.
_OCC_RE = re.compile(r"^\s*.+?\s+(\d{2})(\d{2})(\d{2})([CP])(\d{8})\s*$")

# Interludes only narrate stretches a human would call "waiting":
# at least ~2.5 weeks of silence AND a move worth talking about.
_INTERLUDE_MIN_DAYS = 16
_INTERLUDE_MIN_MOVE = 250.0

# A "break" is a long silence while FLAT — you closed out and walked away.
# Chapter-break copy instead of P&L narration (there is no position to mark).
_BREAK_MIN_DAYS = 45


def parse_occ(trade_symbol):
    """Parse an OCC option symbol into {expiry, option_type, strike}.

    Returns None for equity rows / unparseable strings.
    """
    m = _OCC_RE.match(str(trade_symbol or ""))
    if not m:
        return None
    yy, mm, dd, cp, strike8 = m.groups()
    try:
        expiry = date(2000 + int(yy), int(mm), int(dd))
    except ValueError:
        return None
    return {
        "expiry": expiry,
        "option_type": "call" if cp == "C" else "put",
        "strike": int(strike8) / 1000.0,
    }


def _num(v):
    try:
        f = float(v)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _money(v, decimals=0):
    """$1,234 (positive magnitude only — sign is carried by the sentence)."""
    return f"${abs(v):,.{decimals}f}"


def _fmt_strike(strike):
    if strike == int(strike):
        return f"${int(strike)}"
    return f"${strike:g}"


def _fmt_expiry(expiry, anchor):
    """"Jan 17" within the same year as the trade, "Jan 17 '26" across years."""
    try:
        if expiry.year == anchor.year:
            return expiry.strftime("%b %-d")
        return expiry.strftime("%b %-d '%y") 
    except (AttributeError, ValueError):
        return str(expiry)


def _plural(n, word):
    n = int(round(abs(n)))
    return f"{n} {word}{'s' if n != 1 else ''}"


def _shares(q):
    return _plural(q, "share")


def _contracts(q):
    return _plural(q, "contract")


# ── Per-fill normalization ───────────────────────────────────────────────

_RAW_VERBS = {
    "equity_buy": ("Bought", "buy"),
    "equity_sell": ("Sold", "sell"),
    "equity_sell_short": ("Sold short", "sell"),
    "option_sell_to_open": ("Sold to open", "sell"),
    "option_buy_to_open": ("Bought to open", "buy"),
    "option_buy_to_close": ("Bought to close", "buy"),
    "option_sell_to_close": ("Sold to close", "sell"),
    "option_expired": ("Expired", "lifecycle"),
    "option_assigned": ("Assigned", "lifecycle"),
    "option_exercised": ("Exercised", "lifecycle"),
    "dividend_reinvest": ("Dividend reinvested", "income"),
}


def _normalize_fills(trades_df):
    """Yield one dict per story-relevant fill, oldest first."""
    if trades_df is None or trades_df.empty or "trade_date" not in trades_df.columns:
        return []
    fills = []
    for _, r in trades_df.iterrows():
        action = str(r.get("action") or "").strip()
        if action not in _RAW_VERBS:
            continue
        d = r.get("trade_date")
        if d is None or pd.isna(d):
            continue
        if hasattr(d, "date") and callable(getattr(d, "date", None)):
            d = d.date()
        tsym = str(r.get("trade_symbol") or "").strip()
        fills.append({
            "date": d,
            "action": action,
            "account": str(r.get("account") or ""),
            "is_option": str(r.get("instrument_type") or "") in ("Call", "Put"),
            "trade_symbol": tsym,
            "occ": parse_occ(tsym),
            "quantity": _num(r.get("quantity")) or 0.0,
            "price": _num(r.get("price")),
            "amount": _num(r.get("amount")) or 0.0,
        })
    fills.sort(key=lambda f: f["date"])
    return fills


def _raw_event(fill):
    """The literal fill line shown under the headline (old story format)."""
    verb, kind = _RAW_VERBS[fill["action"]]
    bits = []
    q = fill["quantity"]
    if q:
        bits.append(f"{abs(q):,.0f}{'×' if fill['is_option'] else ' sh'}")
    if fill["is_option"] and fill["trade_symbol"]:
        bits.append(fill["trade_symbol"])
    if fill["price"]:
        bits.append(f"@ ${fill['price']:,.2f}")
    return {"verb": verb, "kind": kind, "detail": " ".join(bits),
            "amount": fill["amount"]}


def _new_stats():
    """The behavioral fingerprint accumulated while narrating.

    Every counter is recorded by the same branch that writes the story
    sentence, so the mirror (compose_mirror) can never disagree with the
    chapters. Dollars are magnitudes; wins/losses are realized contract
    outcomes the engine could pair up (flat after close / expiry).
    """
    return {
        "rolls": 0, "roll_credit": 0.0,
        "covered_calls": 0, "cc_rent": 0.0,
        "puts_sold": 0, "premium_collected": 0.0,
        "long_opens": 0, "long_risk": 0.0,
        "expired_kept": 0, "expired_premium": 0.0,
        "expired_lost": 0, "expired_lost_premium": 0.0,
        "contract_wins": 0, "contract_win_total": 0.0,
        "contract_losses": 0, "contract_loss_total": 0.0,
        "assignments": 0, "wheels_opened": 0, "wheels_completed": 0,
        "stock_opens": 0, "adds": 0, "trims": 0,
        "dividend_total": 0.0, "drip_shares": 0.0,
        "quiet_gain": 0.0, "quiet_loss": 0.0, "away_breaks": 0,
        "splits": 0, "chapters": 0, "span_days": 0,
    }


# ── The state machine ────────────────────────────────────────────────────

class _AccountState:
    """Running per-account position state as of the day being narrated."""

    def __init__(self):
        self.shares = 0.0
        # occ trade_symbol -> {"net": signed contracts, "cash": running cash}
        self.options = {}
        # Wheel: short-put -> assignment -> covered calls -> called away.
        self.wheel_active = False
        self.wheel_premium = 0.0

    def opt(self, tsym):
        return self.options.setdefault(tsym, {"net": 0.0, "cash": 0.0})


def _day_headline(day_fills, state_by_account, multi_account, stats):
    """Compose the plain-English sentences for one day's fills.

    Mutates per-account state as it consumes fills and accumulates the
    behavioral fingerprint into ``stats`` (the same detection that writes
    a sentence records the maneuver — the mirror never disagrees with the
    story). Returns (sentences, dominant_kind).
    """
    sentences = []
    kinds = []

    # Group per account so share counts / coverage are computed against
    # the right account, then narrate in one merged stream.
    by_account = {}
    for f in day_fills:
        by_account.setdefault(f["account"], []).append(f)

    def _tag(sentence, account):
        """Append the account name inside the final period for multi-account
        positions, so readers know whose ledger the maneuver hit."""
        if not (multi_account and account):
            return sentence
        if sentence.endswith("."):
            return f"{sentence[:-1]} — {account}."
        return f"{sentence} — {account}"

    for account, fills in by_account.items():
        st = state_by_account.setdefault(account, _AccountState())
        sentences_before = len(sentences)

        opt_fills = [f for f in fills if f["is_option"] or f["occ"]]
        eq_fills = [f for f in fills
                    if not (f["is_option"] or f["occ"])
                    and f["action"] != "dividend_reinvest"]

        # DRIPs: brokers ship several fractional fills (often 0-quantity
        # stubs) per reinvestment — aggregate to one sentence per day.
        drip_q = sum(abs(f["quantity"]) for f in fills
                     if f["action"] == "dividend_reinvest")
        if drip_q > 0:
            st.shares += drip_q
            stats["drip_shares"] += drip_q
            if drip_q >= 0.005:
                qtxt = f"{drip_q:,.2f}".rstrip("0").rstrip(".")
                sentences.append(_tag(
                    f"Dividend reinvested into {qtxt} more share"
                    f"{'s' if drip_q >= 1.995 else ''}.", account))
                kinds.append("income")

        # Assignments/short exercises arrive as an option row PLUS a
        # mechanical equity fill AT THE STRIKE. The lifecycle sentence
        # covers both — swallow the share fill (still updating share
        # state) so the story doesn't read "Bought 100 shares. Assigned
        # on the $10 put." twice. Matching is strike-price-based because
        # splits can change the contract deliverable, so quantity math
        # (100 × contracts) isn't reliable across a split boundary.
        lifecycle_strikes = [
            f["occ"]["strike"] for f in opt_fills
            if f["occ"] and f["action"] in ("option_assigned", "option_exercised")
        ]

        def _at_strike(price, strikes):
            if price is None:
                return False
            return any(abs(price - s) <= max(0.005 * s, 0.01) for s in strikes)

        # Same-day equity fill directions at a lifecycle strike, used to
        # infer short-vs-long voice for contracts whose open predates our
        # tracking (or whose OCC symbol was renamed by a split).
        day_ctx = {
            "buy_prices": [f["price"] for f in eq_fills
                           if f["action"] == "equity_buy" and f["price"]],
            "sell_prices": [f["price"] for f in eq_fills
                            if f["action"] in ("equity_sell", "equity_sell_short")
                            and f["price"]],
        }

        # ── Rolls: a close + an open of the same option type, same day,
        # same direction (short stays short / long stays long). The
        # single most common maneuver we can name.
        closes_short = [f for f in opt_fills if f["action"] == "option_buy_to_close"]
        opens_short = [f for f in opt_fills if f["action"] == "option_sell_to_open"]
        closes_long = [f for f in opt_fills if f["action"] == "option_sell_to_close"]
        opens_long = [f for f in opt_fills if f["action"] == "option_buy_to_open"]

        consumed = set()

        def _detect_rolls(closes, opens, short_side):
            for c in closes:
                if id(c) in consumed or not c["occ"]:
                    continue
                for o in opens:
                    if id(o) in consumed or not o["occ"]:
                        continue
                    if o["occ"]["option_type"] != c["occ"]["option_type"]:
                        continue
                    if (o["occ"]["strike"] == c["occ"]["strike"]
                            and o["occ"]["expiry"] == c["occ"]["expiry"]):
                        continue  # same contract both ways isn't a roll
                    consumed.add(id(c))
                    consumed.add(id(o))
                    sentences.append(_tag(_phrase_roll(c, o, short_side), account))
                    kinds.append("sell" if short_side else "buy")
                    stats["rolls"] += 1
                    stats["roll_credit"] += c["amount"] + o["amount"]
                    # The roll's open leg is still premium collected — keep
                    # the gross-credit stat consistent with fills-level
                    # rollups (the /story eras sum STO credits directly).
                    if short_side:
                        stats["premium_collected"] += max(o["amount"], 0.0)
                    # State: apply both fills.
                    for f in (c, o):
                        rec = st.opt(f["trade_symbol"])
                        sign = 1 if f["action"] in ("option_buy_to_open", "option_buy_to_close") else -1
                        rec["net"] += sign * abs(f["quantity"])
                        rec["cash"] += f["amount"]
                    if short_side and st.wheel_active:
                        net_credit = c["amount"] + o["amount"]
                        st.wheel_premium += max(net_credit, 0.0)
                    break

        _detect_rolls(closes_short, opens_short, short_side=True)
        _detect_rolls(closes_long, opens_long, short_side=False)

        # ── Equity fills (before remaining option opens, so same-day
        # "buy shares then sell a call" reads as covered).
        for f in eq_fills:
            q = abs(f["quantity"])
            # Mechanical fill of an assignment/exercise (either direction):
            # the lifecycle sentence tells this part of the story.
            if _at_strike(f["price"], lifecycle_strikes) and f["action"] in (
                    "equity_buy", "equity_sell"):
                st.shares += q if f["action"] == "equity_buy" else -q
                kinds.append("lifecycle")
                continue
            s = _phrase_equity(f, st, stats)
            if s:
                sentences.append(_tag(s, account))
                kinds.append(_RAW_VERBS[f["action"]][1])

        # ── Remaining option fills.
        for f in opt_fills:
            if id(f) in consumed:
                continue
            s = _phrase_option(f, st, day_ctx, stats)
            if s:
                sentences.append(_tag(s, account))
                kinds.append(_RAW_VERBS[f["action"]][1])

        if len(sentences) == sentences_before and fills:
            # Nothing narratable (shouldn't happen) — keep the day silent
            # rather than inventing copy; raw fills still render below.
            pass

    dominant = "buy"
    for k in ("sell", "buy", "lifecycle", "income"):
        if k in kinds:
            dominant = k
            break
    return sentences, dominant


def _phrase_roll(close_fill, open_fill, short_side):
    c_occ, o_occ = close_fill["occ"], open_fill["occ"]
    n = abs(open_fill["quantity"]) or abs(close_fill["quantity"])
    otype = c_occ["option_type"]

    dir_bits = []
    if o_occ["strike"] > c_occ["strike"]:
        dir_bits.append("up")
    elif o_occ["strike"] < c_occ["strike"]:
        dir_bits.append("down")
    if o_occ["expiry"] > c_occ["expiry"]:
        dir_bits.append("out")
    elif o_occ["expiry"] < c_occ["expiry"]:
        dir_bits.append("in")
    direction = " and ".join(dir_bits) if dir_bits else "over"

    net = close_fill["amount"] + open_fill["amount"]
    if net > 0.005:
        cash = f"collecting a net {_money(net)} credit"
    elif net < -0.005:
        cash = f"paying {_money(net)} to reposition"
    else:
        cash = "for even money"

    anchor = close_fill["date"]
    side = "short " if short_side else ""
    return (
        f"Rolled the {side}{_fmt_strike(c_occ['strike'])} {otype}"
        f"{'s' if n > 1 else ''} {direction}: "
        f"{_fmt_strike(c_occ['strike'])} {_fmt_expiry(c_occ['expiry'], anchor)} "
        f"→ {_fmt_strike(o_occ['strike'])} {_fmt_expiry(o_occ['expiry'], anchor)}, {cash}."
    )


def _phrase_equity(f, st, stats=None):
    stats = stats if stats is not None else _new_stats()
    q = abs(f["quantity"])
    p = f["price"]
    at = f" at ${p:,.2f}" if p else ""
    action = f["action"]

    if action == "equity_sell_short":
        st.shares -= q
        return f"Sold short {_shares(q)}{at}."

    if action == "equity_buy":
        before = st.shares
        st.shares += q
        if before <= 0.0001:
            stats["stock_opens"] += 1
            return f"Started the stock position: {_shares(q)}{at} ({_money(f['amount'])})."
        stats["adds"] += 1
        return f"Added {_shares(q)}{at} — now holding {st.shares:,.0f}."

    if action == "equity_sell":
        before = st.shares
        st.shares -= q
        if st.shares < -0.0001 and before > 0:
            # Oversell (usually a call assignment selling more than held).
            return (f"Sold {_shares(q)}{at} — more than you held; "
                    f"now short {abs(st.shares):,.0f}.")
        if st.shares <= 0.0001 and before > 0:
            if st.wheel_active:
                st.wheel_active = False
            return f"Sold the last {_shares(q)}{at} — stock position closed."
        if before > 0:
            stats["trims"] += 1
            return f"Trimmed {_shares(q)}{at} — {st.shares:,.0f} remaining."
        return f"Sold {_shares(q)}{at}."

    return None


def _phrase_option(f, st, day_ctx=None, stats=None):
    day_ctx = day_ctx or {"buy_prices": [], "sell_prices": []}
    stats = stats if stats is not None else _new_stats()

    def _near(price_list, strike):
        return any(abs(p - strike) <= max(0.005 * strike, 0.01)
                   for p in price_list)

    occ = f["occ"]
    n = abs(f["quantity"])
    action = f["action"]
    tsym = f["trade_symbol"]
    rec = st.opt(tsym)

    # No parseable OCC: fall back to generic copy.
    if not occ:
        verb = _RAW_VERBS[action][0].lower()
        return f"{verb.capitalize()} {_contracts(n)} of {tsym or 'options'}."

    strike = _fmt_strike(occ["strike"])
    otype = occ["option_type"]
    exp = _fmt_expiry(occ["expiry"], f["date"])
    amt = f["amount"]

    if action == "option_sell_to_open":
        rec["net"] -= n
        rec["cash"] += amt
        stats["premium_collected"] += max(amt, 0.0)
        if otype == "put":
            stats["puts_sold"] += 1
            wheel_lead = ""
            if not st.wheel_active and st.shares < 100:
                st.wheel_active = True
                st.wheel_premium = 0.0
                stats["wheels_opened"] += 1
                wheel_lead = "Opened a wheel: "
            if st.wheel_active:
                st.wheel_premium += max(amt, 0.0)
            verb = f"{wheel_lead}sold" if wheel_lead else "Sold"
            return (
                f"{verb} {_contracts(n)} of the {strike} put ({exp}), "
                f"collecting {_money(amt)}."
            )
        # Short call: covered if the account holds enough shares.
        covered = st.shares + 0.0001 >= 100 * n
        if st.wheel_active:
            st.wheel_premium += max(amt, 0.0)
        if covered:
            stats["covered_calls"] += 1
            stats["cc_rent"] += max(amt, 0.0)
            return (
                f"Sold {_contracts(n)} of the {strike} covered call ({exp}) "
                f"against your shares, collecting {_money(amt)}."
            )
        return (
            f"Sold {_contracts(n)} of the {strike} call ({exp}), "
            f"collecting {_money(amt)}."
        )

    if action == "option_buy_to_open":
        rec["net"] += n
        rec["cash"] += amt
        stats["long_opens"] += 1
        stats["long_risk"] += abs(min(amt, 0.0))
        stance = "bullish" if otype == "call" else "downside protection" if st.shares > 0 else "bearish"
        return (
            f"Bought {_contracts(n)} of the {strike} {otype} ({exp}) — "
            f"{_money(amt)} at risk ({stance})."
        )

    if action == "option_buy_to_close":
        prior_cash = rec["cash"]
        rec["net"] += n
        rec["cash"] += amt
        if abs(rec["net"]) < 0.0001 and prior_cash > 0:
            pnl = rec["cash"]
            rec["cash"] = 0.0
            if pnl > 0:
                stats["contract_wins"] += 1
                stats["contract_win_total"] += pnl
                outcome = f"locking in {_money(pnl)} of the premium"
            else:
                stats["contract_losses"] += 1
                stats["contract_loss_total"] += abs(pnl)
                outcome = f"a net {_money(pnl)} loss on the contract"
            return f"Bought back the {strike} {otype} ({exp}) for {_money(amt)} — {outcome}."
        return f"Bought back {_contracts(n)} of the {strike} {otype} ({exp}) for {_money(amt)}."

    if action == "option_sell_to_close":
        prior_cash = rec["cash"]
        rec["net"] -= n
        rec["cash"] += amt
        if abs(rec["net"]) < 0.0001:
            pnl = rec["cash"]
            rec["cash"] = 0.0
            if pnl > 0:
                stats["contract_wins"] += 1
                stats["contract_win_total"] += pnl
                outcome = f"a {_money(pnl)} win on the contract"
            else:
                stats["contract_losses"] += 1
                stats["contract_loss_total"] += abs(pnl)
                outcome = f"taking the {_money(pnl)} loss"
            return f"Sold the {strike} {otype}s ({exp}) for {_money(amt)} — {outcome}."
        return f"Sold {_contracts(n)} of the {strike} {otype} ({exp}) for {_money(amt)}."

    if action == "option_expired":
        was_short = rec["net"] < -0.0001 or (abs(rec["net"]) < 0.0001 and rec["cash"] > 0)
        premium = rec["cash"]
        rec["net"] = 0.0
        rec["cash"] = 0.0
        if was_short:
            if premium > 0.005:
                stats["expired_kept"] += 1
                stats["expired_premium"] += premium
                stats["contract_wins"] += 1
                stats["contract_win_total"] += premium
                return (
                    f"The {strike} {otype} expired worthless — "
                    f"you kept the full {_money(premium)} premium."
                )
            return f"The {strike} {otype} expired worthless — premium fully earned."
        if premium < -0.005:
            stats["expired_lost"] += 1
            stats["expired_lost_premium"] += abs(premium)
            stats["contract_losses"] += 1
            stats["contract_loss_total"] += abs(premium)
            return (
                f"The {strike} {otype} expired worthless — "
                f"the {_money(premium)} paid for it was lost."
            )
        return f"The {strike} {otype} expired."

    if action == "option_assigned":
        premium = st.wheel_premium
        rec["net"] = 0.0
        rec["cash"] = 0.0
        stats["assignments"] += 1
        if otype == "put":
            note = ""
            if st.wheel_active and premium > 0.005:
                note = (f" {_money(premium)} of premium already collected "
                        f"lowers your effective cost basis.")
            return f"Assigned on the {strike} put: took delivery of the shares at {strike}.{note}"
        # Short call assignment = shares called away.
        note = ""
        if st.wheel_active:
            st.wheel_active = False
            stats["wheels_completed"] += 1
            if premium > 0.005:
                note = f" Wheel complete: {_money(premium)} of premium collected over the cycle."
        return f"Shares called away at {strike} on the {otype}.{note}"

    if action == "option_exercised":
        was_short = rec["net"] < -0.0001
        was_long = rec["net"] > 0.0001
        rec["net"] = 0.0
        rec["cash"] = 0.0
        if not (was_short or was_long):
            # Untracked contract (opened before our window, or the OCC
            # symbol was renamed by a split): infer the side from the
            # direction of the same-day mechanical share fill.
            if otype == "call":
                was_short = _near(day_ctx["sell_prices"], occ["strike"])
            else:
                was_short = _near(day_ctx["buy_prices"], occ["strike"])
        if was_short:
            stats["assignments"] += 1
            if otype == "call":
                note = ""
                if st.wheel_active:
                    st.wheel_active = False
                    stats["wheels_completed"] += 1
                    if st.wheel_premium > 0.005:
                        note = (f" Wheel complete: {_money(st.wheel_premium)} "
                                f"of premium collected over the cycle.")
                return (f"Your short {strike} call was exercised — "
                        f"shares called away at {strike}.{note}")
            return (f"Your short {strike} put was exercised — "
                    f"took delivery at {strike}.")
        return f"Exercised the {strike} {otype} ({exp}) — converted into stock."

    return None


# ── Interludes: what the daily marks did BETWEEN trades ──────────────────

def _series_at(series, idx):
    """Last non-null value at or before idx."""
    j = idx
    while j >= 0:
        v = series[j] if j < len(series) else None
        if v is not None:
            return float(v)
        j -= 1
    return None


def _build_interlude(d1, d2, chart_data):
    """Narrate the quiet stretch strictly between event days d1 and d2."""
    dates = chart_data.get("dates") or []
    total = chart_data.get("total") or []
    options = chart_data.get("options") or []
    prices = chart_data.get("underlying_price") or []
    if not dates or not total:
        return None

    iso1, iso2 = d1.isoformat(), d2.isoformat()
    # Window: first chart day >= d1 ... last chart day strictly before d2
    # (so the destination day's own trades aren't counted as "waiting").
    i1 = next((i for i, d in enumerate(dates) if d >= iso1), None)
    i2 = None
    for i in range(len(dates) - 1, -1, -1):
        if dates[i] < iso2:
            i2 = i
            break
    if i1 is None or i2 is None or i2 <= i1:
        return None

    t1, t2 = _series_at(total, i1), _series_at(total, i2)
    if t1 is None or t2 is None:
        return None
    delta = t2 - t1
    if abs(delta) < _INTERLUDE_MIN_MOVE:
        return None

    o1, o2 = _series_at(options, i1), _series_at(options, i2)
    opt_delta = (o2 - o1) if (o1 is not None and o2 is not None) else 0.0

    gap_days = (d2 - d1).days
    weeks = max(1, round(gap_days / 7))
    span = f"{weeks} week{'s' if weeks != 1 else ''}"

    price_clause = ""
    p1, p2 = _series_at(prices, i1), _series_at(prices, i2)
    if p1 and p2 and abs(p2 - p1) / max(p1, 0.01) > 0.02:
        price_clause = f" while the stock went ${p1:,.2f} → ${p2:,.2f}"

    options_led = abs(opt_delta) >= 0.6 * abs(delta) and abs(opt_delta) > 1
    if delta > 0:
        if options_led and opt_delta > 0:
            text = (f"Over the next {span}, time decay added {_money(delta)} "
                    f"with no trades placed{price_clause}.")
        else:
            text = (f"A quiet {span}: +{_money(delta)} with no trades placed"
                    f"{price_clause}.")
    else:
        if options_led and opt_delta < 0:
            text = (f"The next {span} went against you — option marks moved "
                    f"{_money(delta)}{price_clause}.")
        else:
            text = (f"Over the next {span} the position gave back "
                    f"{_money(delta)}{price_clause}.")

    return {
        "type": "interlude",
        "date_iso": iso1,
        "end_iso": iso2,
        "text": text,
        "delta": round(delta, 2),
    }


# ── Public builder ───────────────────────────────────────────────────────

def _wrap(text, width=58):
    """Chart.js tooltips take a list of lines — wrap long headlines."""
    words, lines, cur = text.split(), [], ""
    for w in words:
        if cur and len(cur) + 1 + len(w) > width:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}" if cur else w
    if cur:
        lines.append(cur)
    return lines


def _normalize_splits(splits_df):
    """[(date, ratio)] from stg_split_events, oldest first."""
    if splits_df is None or getattr(splits_df, "empty", True):
        return []
    out = []
    for _, r in splits_df.iterrows():
        try:
            d = pd.to_datetime(r.get("split_date")).date()
        except (TypeError, ValueError):
            continue
        ratio = _num(r.get("split_ratio"))
        if ratio and ratio > 0 and abs(ratio - 1.0) > 1e-9:
            out.append((d, ratio))
    out.sort()
    return out


def _phrase_split(symbolless_ratio, before, after):
    r = symbolless_ratio
    if r >= 1:
        rt = f"{r:g}-for-1 split"
    else:
        rt = f"1-for-{1 / r:g} reverse split"
    return (f"A {rt}: your {before:,.0f} shares became {after:,.0f} "
            f"(position value unchanged).")


def build_position_story(trades_df, div_df, chart_data=None, splits_df=None):
    """Return ``(story_items, story_markers)``.

    story_items — chronological, oldest first. Event days:
    ``{type:'day', date_iso, label, kind, headlines:[...], events:[...],
    amount}``; interludes: ``{type:'interlude', date_iso, end_iso, text,
    delta}``.

    story_markers — ``[{d, k, t:[line, ...]}]`` per event day for the
    chart scatter overlay (tooltip lines lead with the headlines).

    ``splits_df`` (stg_split_events; symbol-grain public data) serves two
    jobs: a narrated story beat when shares were held through the split,
    and the share-unit correction that keeps running state honest —
    stg_history quantities are in the units of their fill date (pre-split
    for old fills), so without applying the ratio a later post-split sell
    reads as an oversell (see the stock-splits rule; SCHD 3-for-1 was the
    canonical miss).
    """
    fills = _normalize_fills(trades_df)

    # Cash dividends (synthetic pipeline; see module docstring).
    div_by_day = {}
    if div_df is not None and not div_df.empty and "trade_date" in div_df.columns:
        for _, r in div_df.iterrows():
            try:
                d = pd.to_datetime(r.get("trade_date")).date()
            except (TypeError, ValueError):
                continue
            amt = _num(r.get("amount")) or 0.0
            if abs(amt) >= 0.01:
                div_by_day[d] = div_by_day.get(d, 0.0) + amt

    fills_by_day = {}
    for f in fills:
        fills_by_day.setdefault(f["date"], []).append(f)

    stats = _new_stats()
    if not fills_by_day and not div_by_day:
        return [], [], stats

    first_day = min(set(fills_by_day) | set(div_by_day))
    splits_by_day = {}
    for d, ratio in _normalize_splits(splits_df):
        if d >= first_day:
            splits_by_day.setdefault(d, []).append(ratio)

    all_days = sorted(set(fills_by_day) | set(div_by_day) | set(splits_by_day))

    accounts = {f["account"] for f in fills if f["account"]}
    multi_account = len(accounts) > 1

    def _flat_everywhere():
        for st in state_by_account.values():
            if abs(st.shares) > 0.5:
                return False
            for rec in st.options.values():
                if abs(rec["net"]) > 0.01:
                    return False
        return True

    state_by_account = {}
    story_items, story_markers = [], []
    prev_event_day = None

    for d in all_days:
        # Interlude for the silent stretch before this event day.
        gap = (d - prev_event_day).days if prev_event_day is not None else 0
        if gap >= _INTERLUDE_MIN_DAYS:
            interlude = (_build_interlude(prev_event_day, d, chart_data)
                         if chart_data else None)
            if interlude:
                story_items.append(interlude)
                if interlude["delta"] > 0:
                    stats["quiet_gain"] += interlude["delta"]
                else:
                    stats["quiet_loss"] += abs(interlude["delta"])
            elif gap >= _BREAK_MIN_DAYS and _flat_everywhere():
                stats["away_breaks"] += 1
                months = max(1, round(gap / 30))
                span = (f"{months} month{'s' if months != 1 else ''}"
                        if months >= 2 else f"{gap} days")
                story_items.append({
                    "type": "interlude",
                    "date_iso": prev_event_day.isoformat(),
                    "end_iso": d.isoformat(),
                    "text": (f"No activity for {span} — fully out of the "
                             f"position. Trading resumed "
                             f"{d.strftime('%B %Y')}."),
                    "delta": 0.0,
                })

        # Splits apply BEFORE the day's fills (fills on a split day arrive
        # in post-split units) and narrate only when shares were held.
        split_headlines = []
        for ratio in splits_by_day.get(d, []):
            before = sum(st.shares for st in state_by_account.values())
            for st in state_by_account.values():
                st.shares *= ratio
            after = sum(st.shares for st in state_by_account.values())
            if before > 0.5:
                stats["splits"] += 1
                split_headlines.append(_phrase_split(ratio, before, after))

        day_fills = fills_by_day.get(d, [])
        headlines, dominant = ([], "income") if not day_fills else _day_headline(
            day_fills, state_by_account, multi_account, stats)
        if split_headlines:
            headlines = split_headlines + headlines
            if not day_fills:
                dominant = "lifecycle"

        div_amt = div_by_day.get(d, 0.0)
        if div_amt:
            stats["dividend_total"] += div_amt
            headlines = headlines + [
                f"Collected {_money(div_amt, 2)} in dividends."
            ]

        events = [_raw_event(f) for f in day_fills]
        if div_amt:
            events.append({"verb": "Dividend", "kind": "income",
                           "detail": "", "amount": div_amt})

        if not headlines and not events:
            # A split while flat: state already adjusted, nothing to tell.
            continue

        amount = sum(e["amount"] for e in events)
        iso = d.isoformat()

        story_items.append({
            "type": "day",
            "date_iso": iso,
            "label": d.strftime("%b %-d, %Y"),
            "kind": dominant,
            "headlines": headlines,
            "events": events,
            "amount": round(amount, 2),
        })

        tooltip_lines = []
        for h in headlines:
            tooltip_lines.extend(_wrap(h))
        story_markers.append({"d": iso, "k": dominant, "t": tooltip_lines})
        prev_event_day = d

    day_items = [i for i in story_items if i["type"] == "day"]
    stats["chapters"] = len(day_items)
    if len(day_items) >= 2:
        stats["span_days"] = (
            date.fromisoformat(day_items[-1]["date_iso"])
            - date.fromisoformat(day_items[0]["date_iso"])
        ).days

    return story_items, story_markers, stats


# ── The mirror: "here's you, in this position" ───────────────────────────

def _span_text(days):
    if days < 60:
        return f"{days} days"
    months = round(days / 30)
    if months < 24:
        return f"{months} months"
    return f"{days / 365:.1f} years"


def _behavior_candidates(stats, symbol):
    """(score, sentence) pairs for the behaviors this position's chapters
    can prove, scored by the dollars (or repetition) behind them. Shared
    by the per-position mirror and the cross-position trader novel."""
    candidates = []
    if stats["premium_collected"] > 1:
        clause = (f"You traded {symbol} primarily for income: "
                  f"{_money(stats['premium_collected'])} of option premium collected")
        bits = []
        if stats["covered_calls"]:
            bits.append(f"{stats['covered_calls']} covered call"
                        f"{'s' if stats['covered_calls'] != 1 else ''}")
        if stats["puts_sold"]:
            bits.append(f"{stats['puts_sold']} short put"
                        f"{'s' if stats['puts_sold'] != 1 else ''}")
        if bits:
            clause += f" across {' and '.join(bits)}"
        candidates.append((stats["premium_collected"], clause + "."))
    if stats["long_opens"] and stats["long_risk"] > 1:
        w, l = stats["contract_wins"], stats["contract_losses"]
        clause = (f"You put {_money(stats['long_risk'])} at risk across "
                  f"{stats['long_opens']} long-option purchase"
                  f"{'s' if stats['long_opens'] != 1 else ''}")
        if w + l:
            clause += f"; closed contracts here went {w}W / {l}L"
        candidates.append((stats["long_risk"], clause + "."))
    if stats["rolls"]:
        clause = (f"You rolled rather than closed "
                  f"{stats['rolls']} time{'s' if stats['rolls'] != 1 else ''}")
        if stats["roll_credit"] > 1:
            clause += f", collecting {_money(stats['roll_credit'])} while repositioning"
        elif stats["roll_credit"] < -1:
            clause += f", paying {_money(stats['roll_credit'])} to reposition"
        candidates.append((max(abs(stats["roll_credit"]), 500 * stats["rolls"]),
                           clause + "."))
    if stats["expired_kept"]:
        candidates.append((
            stats["expired_premium"],
            f"{stats['expired_kept']} short contract"
            f"{'s' if stats['expired_kept'] != 1 else ''} here expired "
            f"worthless, keeping the full {_money(stats['expired_premium'])} "
            f"of premium.",
        ))
    if stats["wheels_completed"]:
        candidates.append((
            2000.0 * stats["wheels_completed"],
            f"You turned {stats['wheels_completed']} full wheel cycle"
            f"{'s' if stats['wheels_completed'] != 1 else ''} — "
            f"put premium, assignment, call premium, called away.",
        ))
    quiet_net = stats["quiet_gain"] - stats["quiet_loss"]
    if stats["quiet_gain"] > 1 and quiet_net > 0:
        candidates.append((
            stats["quiet_gain"],
            f"{_money(stats['quiet_gain'])} of the gains accrued during "
            f"stretches with no trades placed.",
        ))
    if stats["dividend_total"] > 1:
        candidates.append((
            stats["dividend_total"],
            f"{symbol} paid {_money(stats['dividend_total'], 2)} in dividends "
            f"over the holding period.",
        ))
    if stats["adds"] >= 3 and stats["adds"] > 2 * max(stats["trims"], 1):
        candidates.append((
            300.0 * stats["adds"],
            f"You built the stock position incrementally — "
            f"{stats['adds']} separate buys.",
        ))

    candidates.sort(key=lambda c: c[0], reverse=True)
    return candidates


def compose_mirror(stats, symbol, book_rank=None, book_size=None):
    """2-4 evidence-only sentences reflecting HOW the trader traded this
    position, plus where it sits in their book. This is the mirror: it
    describes behavior the chapters below can prove, never intent (see
    AGENTS.md pattern-detection rules — no psychological labeling).

    ``book_rank``/``book_size``: 1-based P&L rank among the trader's
    symbols (from the tab-strip rollup), included when the book is big
    enough for rank to mean something.
    """
    if not stats or not stats["chapters"]:
        return []

    sentences = []

    # Shape of the position's history.
    shape = (f"{stats['chapters']} trade day"
             f"{'s' if stats['chapters'] != 1 else ''}")
    if stats["span_days"] >= 14:
        shape += f" across {_span_text(stats['span_days'])}"
    if stats["away_breaks"]:
        shape += (f", including {stats['away_breaks']} period"
                  f"{'s' if stats['away_breaks'] != 1 else ''} fully out of "
                  f"the position")
    sentences.append(f"{symbol}: {shape}.")

    # Keep only the two most load-bearing behaviors so the mirror stays a
    # reflection, not a report.
    sentences.extend(text for _, text in _behavior_candidates(stats, symbol)[:2])

    # Where it sits across the trader's full history.
    if book_rank and book_size and book_size >= 5:
        if book_rank == 1:
            sentences.append(
                f"Across everything you've traded, {symbol} is your best "
                f"position by total P&L ({book_size} symbols)."
            )
        elif book_rank <= max(3, round(book_size * 0.1)):
            sentences.append(
                f"Across everything you've traded, {symbol} ranks "
                f"#{book_rank} of {book_size} symbols by total P&L."
            )
        elif book_rank > book_size - max(3, round(book_size * 0.1)):
            sentences.append(
                f"Across everything you've traded, {symbol} ranks near the "
                f"bottom — #{book_rank} of {book_size} symbols by total P&L."
            )
        else:
            sentences.append(
                f"Across everything you've traded, {symbol} ranks "
                f"#{book_rank} of {book_size} symbols by total P&L."
            )

    return sentences
