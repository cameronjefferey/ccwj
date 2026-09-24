"""Position story engine — plain-English narrative of a position.

Turns the raw fill stream into the story a trader would tell a friend:
"opened an iron condor", "rolled the calls up and out", "kept the full
premium", "three quiet weeks of theta doing the work". Two outputs feed the
Position Detail page:

  story_items   — chronological narrative rows for the Story card. Two
                  shapes: event days (headline + raw fills underneath)
                  and interludes (what happened BETWEEN trades, computed
                  from the daily-mark chart series — the data no broker
                  dashboard has).
  story_markers — one dot per event day for the chart overlay; tooltip
                  leads with the headline.

This is a deterministic fill state machine, not an LLM. It never reads
Strategy Breakdown; it names maneuvers from the fills and the running
share/option state. Defined-risk opens (iron condor, vertical, strangle)
whose legs land within 7 days are grouped from matching fills so a
condor's short put is not mislabeled a wheel — named on the last wing.
Detection is NEVER speculative about intent beyond
what the fills mechanically show (see AGENTS.md pattern-detection rules:
evidence, not psychology). "Cash-secured" for a short put and "covered"
for a short call describe the standard structure implied by the position
state we can see, not the trader's margin arrangement.

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

# Classification pairs verticals / condors within 7 days. The review uses
# the same window so a condor legged in Monday–Wednesday is one structure.
_STRUCTURE_MAX_SPAN_DAYS = 7


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
        raw_tenant_id = r.get("tenant_id")
        tenant_id = (
            ""
            if raw_tenant_id is None or pd.isna(raw_tenant_id)
            else str(raw_tenant_id).strip()
        )
        account = str(r.get("account") or "")
        fills.append({
            "date": d,
            "action": action,
            "account": account,
            # account is a display label and can collide across physical
            # accounts.  State must follow the broker-stable tenant grain or
            # shares/options in two "Schwab Account" tenants fuse into one
            # fictional position.  The account fallback keeps synthetic and
            # legacy callers that do not carry tenant_id working.
            "state_key": tenant_id or account,
            "is_option": str(r.get("instrument_type") or "") in ("Call", "Put"),
            "trade_symbol": tsym,
            "occ": parse_occ(tsym),
            "quantity": _num(r.get("quantity")) or 0.0,
            "price": _num(r.get("price")),
            "amount": _num(r.get("amount")) or 0.0,
        })
    fills.sort(key=lambda f: f["date"])
    for i, f in enumerate(fills):
        f["uid"] = i
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


def _day_headline(day_fills, state_by_account, multi_account, stats,
                  exit_notes=None, label_map=None, structure_plan=None):
    """Compose the plain-English sentences for one day's fills.

    Mutates per-account state as it consumes fills and accumulates the
    behavioral fingerprint into ``stats`` (the same detection that writes
    a sentence records the maneuver — the mirror never disagrees with the
    story). Returns (sentences, dominant_kind).

    ``exit_notes`` — optional {(tenant/account key, trade_symbol): verdict
    sentence} from the execution-review layer (app/execution_quality.py).
    When a contract's completing close (buyback, sale, or the closed leg of
    a roll) narrates, its after-the-fact verdict ("this contract went on to
    expire worthless…") renders on the next line, so the story and the grade
    read as one voice.
    """
    sentences = []
    kinds = []
    exit_notes = exit_notes or {}
    label_map = label_map or {}

    def _note_for(fill):
        trade_symbol = (fill.get("trade_symbol") or "").strip()
        # Physical accounts can hold the same OCC contract.  Match the
        # execution verdict on the same tenant-grained state key used by the
        # story engine so one account's result cannot decorate another's fill.
        tenant_note = exit_notes.get((fill.get("state_key") or "", trade_symbol))
        if tenant_note is not None:
            return tenant_note
        # Compatibility for synthetic/legacy callers that still supply the
        # pre-tenant {trade_symbol: note} shape.
        return exit_notes.get(trade_symbol)

    # Group per physical tenant so share counts / coverage are computed
    # against the right account. Display labels are not unique: one user can
    # legitimately have several tenants all named "Schwab Account".
    by_account = {}
    for f in day_fills:
        by_account.setdefault(f["state_key"], []).append(f)

    def _tag(sentence, account):
        """Append the account name inside the final period for multi-account
        positions, so readers know whose ledger the maneuver hit."""
        if not (multi_account and account):
            return sentence
        if sentence.endswith("."):
            return f"{sentence[:-1]} — {account}."
        return f"{sentence} — {account}"

    for state_key, fills in by_account.items():
        raw_account = fills[0]["account"]
        account = label_map.get(state_key) or raw_account
        st = state_by_account.setdefault(state_key, _AccountState())
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
                    note = _note_for(c)
                    if note:
                        sentences.append(_tag(note, account))
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

        # Defined-risk opens (same day or legged in within 7 days). Named
        # on the LAST open date; earlier days apply state without a
        # directional one-liner so a condor short put is never a wheel.
        planned_uids = _all_planned_uids(structure_plan)
        day_date = fills[0]["date"] if fills else None
        completions = (structure_plan or {}).get((day_date, state_key), [])
        for f in opt_fills:
            if f.get("uid") in planned_uids:
                consumed.add(id(f))
                _apply_option_open_state(f, st, stats)
        for sentence, _uids, kind in completions:
            sentences.append(_tag(sentence, account))
            kinds.append(kind)

        remaining_opens = [
            f for f in opt_fills
            if id(f) not in consumed
            and f["action"] in ("option_sell_to_open", "option_buy_to_open")
        ]
        day_ctx["option_open_count"] = len(remaining_opens)
        day_ctx["has_long_open"] = any(
            f["action"] == "option_buy_to_open" for f in remaining_opens)
        day_ctx["has_short_call"] = any(
            f["action"] == "option_sell_to_open"
            and f.get("occ") and f["occ"]["option_type"] == "call"
            for f in remaining_opens)

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
                # Verdict from the execution-review layer, on the
                # COMPLETING close only (net position back to zero) so a
                # contract closed in pieces gets one verdict, not one per
                # partial fill.
                if f["action"] in ("option_buy_to_close",
                                   "option_sell_to_close"):
                    rec = st.opt(f["trade_symbol"])
                    if abs(rec["net"]) < 0.0001:
                        note = _note_for(f)
                        if note:
                            sentences.append(_tag(note, account))

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


def _one_of_type(fills, otype):
    hits = [f for f in fills if f["occ"]["option_type"] == otype]
    return hits[0] if len(hits) == 1 else None


def _matching_qty_phrase(fills):
    """'10 contracts' when every leg is the same size; else empty."""
    qs = [abs(f["quantity"]) for f in fills]
    if not qs:
        return ""
    rounded = []
    for q in qs:
        r = round(q)
        rounded.append(int(r) if abs(q - r) < 1e-6 else q)
    if len(set(rounded)) != 1:
        return ""
    return _contracts(rounded[0])


def _structure_cash(net):
    if net > 0.005:
        return f"collecting a net {_money(net)} credit"
    if net < -0.005:
        return f"putting {_money(net)} at risk"
    return "even on cash"


def _apply_option_open_state(f, st, stats):
    """Apply an STO/BTO fill to running state without composing copy.

    Used by same-day structure detection so a named condor still updates
    net/cash/premium the way the per-leg phrases would — minus the wheel
    flag a lone short put would have set.
    """
    n = abs(f["quantity"])
    rec = st.opt(f["trade_symbol"])
    amt = f["amount"]
    occ = f.get("occ")
    if f["action"] == "option_sell_to_open":
        rec["net"] -= n
        rec["cash"] += amt
        stats["premium_collected"] += max(amt, 0.0)
        if occ and occ["option_type"] == "put":
            stats["puts_sold"] += 1
    elif f["action"] == "option_buy_to_open":
        rec["net"] += n
        rec["cash"] += amt
        stats["long_opens"] += 1
        stats["long_risk"] += abs(min(amt, 0.0))


def _opened_lead(name, fills, exp):
    qty = _matching_qty_phrase(fills)
    qty_bit = f", {qty}" if qty else ""
    uniq = sorted({f["date"] for f in fills if f.get("date")})
    span_bit = ""
    if len(uniq) >= 2:
        span_bit = f" over {len(uniq)} sessions"
    return f"Opened {name}{span_bit}{qty_bit} expiring {exp}"


def _try_iron(group):
    """Four legs, same expiry: long/short put + long/short call, wings outside."""
    shorts = [f for f in group if f["action"] == "option_sell_to_open"]
    longs = [f for f in group if f["action"] == "option_buy_to_open"]
    if len(group) != 4 or len(shorts) != 2 or len(longs) != 2:
        return None
    sp, sc = _one_of_type(shorts, "put"), _one_of_type(shorts, "call")
    lp, lc = _one_of_type(longs, "put"), _one_of_type(longs, "call")
    if not all((sp, sc, lp, lc)):
        return None
    if not (lp["occ"]["strike"] < sp["occ"]["strike"]
            and lc["occ"]["strike"] > sc["occ"]["strike"]):
        return None
    fly = abs(sp["occ"]["strike"] - sc["occ"]["strike"]) < 0.01
    name = "an iron butterfly" if fly else "an iron condor"
    legs = [sp, sc, lp, lc]
    net = sum(f["amount"] for f in legs)
    anchor = max(f["date"] for f in legs)
    exp = _fmt_expiry(sp["occ"]["expiry"], anchor)
    if fly:
        detail = (
            f"short the {_fmt_strike(sp['occ']['strike'])} straddle, "
            f"long the {_fmt_strike(lp['occ']['strike'])} / "
            f"{_fmt_strike(lc['occ']['strike'])} wings"
        )
    else:
        detail = (
            f"short the {_fmt_strike(sp['occ']['strike'])} put / "
            f"{_fmt_strike(sc['occ']['strike'])} call, "
            f"long the {_fmt_strike(lp['occ']['strike'])} / "
            f"{_fmt_strike(lc['occ']['strike'])} wings"
        )
    sentence = (
        f"{_opened_lead(name, legs, exp)}: {detail}, {_structure_cash(net)}."
    )
    return sentence, legs, "sell" if net >= 0 else "buy"


def _try_vertical(group):
    shorts = [f for f in group if f["action"] == "option_sell_to_open"]
    longs = [f for f in group if f["action"] == "option_buy_to_open"]
    if len(group) != 2 or len(shorts) != 1 or len(longs) != 1:
        return None
    s, lo = shorts[0], longs[0]
    if s["occ"]["option_type"] != lo["occ"]["option_type"]:
        return None
    otype = s["occ"]["option_type"]
    legs = [s, lo]
    net = s["amount"] + lo["amount"]
    exp = _fmt_expiry(s["occ"]["expiry"], s["date"])
    detail = (
        f"short {_fmt_strike(s['occ']['strike'])} / "
        f"long {_fmt_strike(lo['occ']['strike'])}"
    )
    sentence = (
        f"{_opened_lead(f'a {otype} spread', legs, exp)}: {detail}, "
        f"{_structure_cash(net)}."
    )
    return sentence, legs, "sell" if net >= 0 else "buy"


def _try_strangle(group):
    shorts = [f for f in group if f["action"] == "option_sell_to_open"]
    longs = [f for f in group if f["action"] == "option_buy_to_open"]
    if len(group) != 2:
        return None
    if len(shorts) == 2 and not longs:
        sp, sc = _one_of_type(shorts, "put"), _one_of_type(shorts, "call")
        if not (sp and sc):
            return None
        same = abs(sp["occ"]["strike"] - sc["occ"]["strike"]) < 0.01
        name = "a short straddle" if same else "a short strangle"
        legs = [sp, sc]
        net = sp["amount"] + sc["amount"]
        exp = _fmt_expiry(sp["occ"]["expiry"], sp["date"])
        if same:
            detail = (
                f"the {_fmt_strike(sp['occ']['strike'])} put and call"
            )
        else:
            detail = (
                f"{_fmt_strike(sp['occ']['strike'])} put / "
                f"{_fmt_strike(sc['occ']['strike'])} call"
            )
        sentence = (
            f"{_opened_lead(name, legs, exp)}: {detail}, {_structure_cash(net)}."
        )
        return sentence, legs, "sell"
    if len(longs) == 2 and not shorts:
        lp, lc = _one_of_type(longs, "put"), _one_of_type(longs, "call")
        if not (lp and lc):
            return None
        same = abs(lp["occ"]["strike"] - lc["occ"]["strike"]) < 0.01
        name = "a long straddle" if same else "a long strangle"
        legs = [lp, lc]
        net = lp["amount"] + lc["amount"]
        exp = _fmt_expiry(lp["occ"]["expiry"], lp["date"])
        if same:
            detail = (
                f"the {_fmt_strike(lp['occ']['strike'])} put and call"
            )
        else:
            detail = (
                f"{_fmt_strike(lp['occ']['strike'])} put / "
                f"{_fmt_strike(lc['occ']['strike'])} call"
            )
        sentence = (
            f"{_opened_lead(name, legs, exp)}: {detail}, {_structure_cash(net)}."
        )
        return sentence, legs, "buy"
    return None


def _all_planned_uids(structure_plan):
    out = set()
    for hits in (structure_plan or {}).values():
        for _sentence, uids, _kind in hits:
            out.update(uids)
    return out


def _plan_open_structures(all_fills):
    """Name iron condor / vertical / strangle groups whose opens span ≤7 days.

    Returns ``{(last_open_date, state_key): [(sentence, uids, kind), ...]}``
    so the review names the structure when the last wing lands.
    """
    by = {}
    for f in all_fills or []:
        if f["action"] not in ("option_sell_to_open", "option_buy_to_open"):
            continue
        if not f.get("occ"):
            continue
        key = (f["state_key"], f["occ"]["expiry"])
        by.setdefault(key, []).append(f)
    plan = {}
    for _key, group in by.items():
        span = (max(f["date"] for f in group) - min(f["date"] for f in group)).days
        if span > _STRUCTURE_MAX_SPAN_DAYS:
            continue
        hit = _try_iron(group) or _try_vertical(group) or _try_strangle(group)
        if not hit:
            continue
        sentence, struct_fills, kind = hit
        last = max(f["date"] for f in struct_fills)
        sk = struct_fills[0]["state_key"]
        uids = [f["uid"] for f in struct_fills]
        plan.setdefault((last, sk), []).append((sentence, uids, kind))
    return plan


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
            # Track a possible wheel so assignment can name it — but do
            # not say "Opened a wheel" here. A CSP that expires or is
            # bought back never became one; classification only labels
            # Wheel after the put is assigned. Same-day longs / short
            # calls are a defined-risk structure, not a wheel seed.
            can_seed_wheel = (
                not st.wheel_active
                and st.shares < 100
                and not day_ctx.get("has_long_open")
                and not day_ctx.get("has_short_call")
                and day_ctx.get("option_open_count", 1) <= 1
            )
            if can_seed_wheel:
                st.wheel_active = True
                st.wheel_premium = 0.0
            if st.wheel_active:
                st.wheel_premium += max(amt, 0.0)
            return (
                f"Sold {_contracts(n)} of the {strike} put ({exp}), "
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
            wheel_bit = ""
            note = ""
            if st.wheel_active:
                stats["wheels_opened"] += 1
                wheel_bit = ", starting a wheel"
                if premium > 0.005:
                    note = (f" {_money(premium)} of premium already collected "
                            f"lowers your effective cost basis.")
            return (
                f"Assigned on the {strike} put: took delivery of the "
                f"shares at {strike}{wheel_bit}.{note}"
            )
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
            if st.wheel_active:
                stats["wheels_opened"] += 1
                return (f"Your short {strike} put was exercised — "
                        f"took delivery at {strike}, starting a wheel.")
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


# ── Column layout for the day-by-day review ─────────────────────────────
# Headlines stay the source of truth (chart tooltips, mirror, tests). The
# review renders each one as a card in the Options or Shares column. Cash
# is the day's fills, separate from the card. Parsing is presentation-only:
# it must not invent a maneuver the sentence didn't already name.

_MONEY_RE = r"\$[\d,]+(?:\.\d+)?"


def _comma_counts(text):
    """'1500 shares' → '1,500 shares'. Leaves prices ($15.36, $23,040) alone."""
    if not text:
        return text
    return re.sub(
        r"(?<![\d$,.\-])\d{4,}(?!\d)",
        lambda m: f"{int(m.group(0)):,}",
        text,
    )


def _card(lane, title, metric=None, tone=None, pill=None, pill_style=None,
          detail=None, note=None, account=None):
    title = _comma_counts(re.sub(r"\s+", " ", (title or "").strip()).rstrip("."))
    detail = _comma_counts((detail or "").strip().rstrip(".")) or None
    if metric and "$" not in metric and "+" not in metric:
        metric = _comma_counts(metric)
    return {
        "lane": lane,
        "title": title,
        "metric": metric,
        "tone": tone or "",
        "pill": pill,
        "pill_style": pill_style,
        "detail": detail,
        "note": note,
        "account": account,
    }


def _plus(raw):
    """'$1,137' → '+$1,137'. Already-signed values pass through."""
    raw = (raw or "").strip()
    if not raw:
        return raw
    if raw[0] in "+-−":
        return raw
    return f"+{raw}"


def _minus(raw):
    raw = (raw or "").strip().lstrip("+")
    if raw.startswith("-") or raw.startswith("−"):
        return "-$" + raw.lstrip("-−$")
    if raw.startswith("$"):
        return f"-{raw}"
    return f"-${raw}"


def _stance_pill(stance):
    words = (stance or "").strip()
    if not words:
        return None
    return " ".join(w[:1].upper() + w[1:] for w in words.split())


def _strip_account(sentence, labels):
    """Pull a multi-account suffix (' — Cameron Investment.') off a headline.

    The suffix is only stripped when it matches a known display label, so a
    sentence's own em dash ('— $2,137 at risk') stays part of the card.
    """
    for lab in sorted((labels or []), key=len, reverse=True):
        suffix = f" — {lab}."
        if sentence.endswith(suffix):
            body = sentence[: -len(suffix)].rstrip()
            if body and not body.endswith("."):
                body += "."
            return body, lab
    return sentence, None


def _split_extra(sentence):
    """Separate a follow-on bookkeeping sentence ('Wheel complete: …')."""
    parts = sentence.split(". ")
    if len(parts) == 1:
        return sentence, ""
    head = parts[0]
    if not head.endswith("."):
        head += "."
    return head, ". ".join(parts[1:])


def _cash_clause(clause):
    """Turn a maneuver's cash tail into (metric, tone)."""
    clause = (clause or "").strip().rstrip(".")
    m = re.search(rf"collecting a net ({_MONEY_RE}) credit", clause)
    if m:
        return f"{_plus(m.group(1))} net credit", "pos"
    m = re.search(rf"paying ({_MONEY_RE}) to reposition", clause)
    if m:
        return f"{_minus(m.group(1))} to reposition", "neg"
    m = re.search(rf"putting ({_MONEY_RE}) at risk", clause)
    if m:
        return f"{m.group(1)} at risk", ""
    if "even" in clause:
        return "Even money", ""
    return None, ""


def _cards_for_sentence(sentence):
    """Return one or two cards for a single headline, or None to keep walking."""
    head, extra = _split_extra(sentence)

    m = re.match(
        rf"^(Rolled the .+?):\s*(.+?)\s*→\s*(.+?),\s*(.+)\.?$",
        head,
    )
    if m:
        metric, tone = _cash_clause(m.group(4))
        card = _card(
            "options", m.group(1), metric=metric, tone=tone,
            pill=f"{m.group(2).strip()} → {m.group(3).strip()}",
            pill_style="move",
        )
        if extra:
            card["detail"] = extra.rstrip(".")
        return [card]

    m = re.match(r"^(Opened .+?):\s*(.+)$", head)
    if m:
        body = m.group(2).rstrip(".")
        detail = body
        metric, tone = None, ""
        tail = re.search(
            rf",\s*(collecting a net {_MONEY_RE} credit|putting {_MONEY_RE} at risk|even on cash)\.?$",
            body,
        )
        if tail:
            detail = body[: tail.start()].rstrip(" ,")
            metric, tone = _cash_clause(tail.group(1))
        card = _card("options", m.group(1), metric=metric, tone=tone, detail=detail)
        if extra:
            card["detail"] = ((card["detail"] + ". ") if card["detail"] else "") + extra.rstrip(".")
        return [card]

    m = re.match(rf"^(Bought .+?) — ({_MONEY_RE}) at risk \(([^)]+)\)\.?$", head)
    if m:
        return [_card(
            "options", m.group(1), metric=f"{m.group(2)} at risk",
            pill=_stance_pill(m.group(3)), pill_style="stance",
        )]

    m = re.match(rf"^(.+?) — a ({_MONEY_RE}) win on the contract\.?$", head)
    if m:
        return [_card("options", m.group(1), metric=f"{_plus(m.group(2))} on the contract", tone="pos")]

    m = re.match(rf"^(.+?) — taking the ({_MONEY_RE}) loss\.?$", head)
    if m:
        return [_card("options", m.group(1), metric=f"{_minus(m.group(2))} on the contract", tone="neg")]

    m = re.match(rf"^(.+?) — locking in ({_MONEY_RE}) of the premium\.?$", head)
    if m:
        return [_card("options", m.group(1), metric=f"{_plus(m.group(2))} of the premium", tone="pos")]

    m = re.match(rf"^(.+?) — a net ({_MONEY_RE}) loss on the contract\.?$", head)
    if m:
        return [_card("options", m.group(1), metric=f"{_minus(m.group(2))} on the contract", tone="neg")]

    m = re.match(rf"^(Sold .+?),\s*collecting ({_MONEY_RE})\.?$", head)
    if m:
        return [_card("options", m.group(1), metric=f"{_plus(m.group(2))} collected", tone="pos")]

    m = re.match(rf"^(The .+? expired worthless) — you kept the full ({_MONEY_RE}) premium\.?$", head)
    if m:
        return [_card("options", m.group(1), metric=f"{_plus(m.group(2))} premium kept", tone="pos")]

    m = re.match(rf"^(The .+? expired worthless) — the ({_MONEY_RE}) paid for it was lost\.?$", head)
    if m:
        return [_card("options", m.group(1), metric=f"{_minus(m.group(2))} lost", tone="neg")]

    m = re.match(r"^(The .+? expired worthless) — premium fully earned\.?$", head)
    if m:
        return [_card("options", m.group(1), metric="Premium kept", tone="pos")]

    m = re.match(r"^(The .+?) expired\.?$", head)
    if m and "expired" not in m.group(1):
        return [_card("options", f"{m.group(1)} expired")]

    m = re.match(r"^Assigned on the (.+?):\s*(.+)\.?$", head)
    if m:
        rest = m.group(2).rstrip(".")
        opt = _card("options", f"Assigned on the {m.group(1)}")
        if ", starting a wheel" in rest:
            rest = rest.split(", starting a wheel")[0].rstrip(" ,")
            opt["detail"] = "Starting a wheel"
        if extra:
            opt["detail"] = ((opt["detail"] + ". ") if opt["detail"] else "") + extra.rstrip(".")
        share_title = rest[:1].upper() + rest[1:] if rest else "Took delivery of the shares"
        return [opt, _card("shares", share_title)]

    m = re.match(rf"^Shares called away at ({_MONEY_RE}) on the (\w+)\.?$", head)
    if m:
        share = _card("shares", f"Shares called away at {m.group(1)}")
        if extra:
            share["detail"] = extra.rstrip(".")
        return [
            _card("options", f"The short {m.group(1)} {m.group(2)} was assigned"),
            share,
        ]

    m = re.match(
        rf"^Your short ({_MONEY_RE}) (call|put) was exercised — (.+?)\.?$",
        head,
    )
    if m:
        rest = m.group(3).rstrip(".")
        opt = _card("options", f"The short {m.group(1)} {m.group(2)} was exercised")
        if ", starting a wheel" in rest:
            rest = rest.split(", starting a wheel")[0].rstrip(" ,")
            opt["detail"] = "Starting a wheel"
        if extra:
            # Wheel-complete bookkeeping belongs with the shares leaving.
            pass
        share_title = rest[:1].upper() + rest[1:] if rest else "Shares settled"
        share = _card("shares", share_title)
        if extra:
            share["detail"] = extra.rstrip(".")
        return [opt, share]

    m = re.match(r"^(Exercised the .+?) — converted into stock\.?$", head)
    if m:
        return [
            _card("options", m.group(1)),
            _card("shares", "Converted into stock"),
        ]

    m = re.match(rf"^Started the stock position:\s*(.+?)\s+\(({_MONEY_RE})\)\.?$", head)
    if m:
        return [_card("shares", f"Started the position: {m.group(1)}", metric=m.group(2))]

    m = re.match(r"^Added (.+?) — now holding (.+)\.?$", head)
    if m:
        return [_card("shares", f"Added {m.group(1)}", metric=f"Now holding {m.group(2).rstrip('.')}")]

    m = re.match(r"^Trimmed (.+?) — (.+?) remaining\.?$", head)
    if m:
        return [_card("shares", f"Trimmed {m.group(1)}", metric=f"{m.group(2)} remaining")]

    m = re.match(r"^Sold the last (.+?) — stock position closed\.?$", head)
    if m:
        return [_card("shares", f"Sold the last {m.group(1)}", metric="Position closed")]

    m = re.match(r"^Sold (.+?) — more than you held; now short (.+)\.?$", head)
    if m:
        return [_card(
            "shares", f"Sold {m.group(1)}",
            metric=f"Now short {m.group(2).rstrip('.')}",
            detail="More than you held",
        )]

    m = re.match(rf"^Collected ({_MONEY_RE}) in dividends\.?$", head)
    if m:
        return [_card("shares", "Dividend", metric=_plus(m.group(1)), tone="pos")]

    if head.startswith("Dividend reinvested"):
        return [_card("shares", head.rstrip("."))]

    if re.match(r"^A .+split:", head):
        return [_card("shares", head.rstrip("."))]

    if re.match(r"^Sold short [\d,]+ shares?\b", head) or re.match(r"^Sold [\d,.]+ shares?\b", head):
        return [_card("shares", head.rstrip("."))]

    return None


def _fallback_card(sentence, account):
    low = sentence.lower()
    shareish = any(tok in low for tok in (
        " share", "stock position", "dividend", "split", "called away",
        "took delivery", "converted into stock", "sold short",
    ))
    return _card("shares" if shareish else "options", sentence.rstrip("."), account=account)


def _fill_fallback_cards(events):
    """A day the engine kept silent (a structure wing, before it is named).

    Show the fill itself. Do not invent a strategy name — the completing
    day already does that.
    """
    cards = []
    for e in events or []:
        kind = e.get("kind")
        detail = (e.get("detail") or "").strip()
        verb = e.get("verb") or "Fill"
        if kind == "income":
            amt = e.get("amount") or 0
            metric = _plus(f"${abs(amt):,.2f}") if abs(amt) >= 0.01 else None
            cards.append(_card("shares", "Dividend", metric=metric, tone="pos" if metric else ""))
            continue
        lane = "shares" if " sh" in f" {detail}" else "options"
        cards.append(_card(lane, verb, detail=detail or None))
    return cards


def _layout_cash(item):
    events = item.get("events") or []
    fills = [e for e in events if e.get("kind") in ("buy", "sell")]
    income = [
        e for e in events
        if e.get("kind") == "income" and abs(e.get("amount") or 0) >= 0.01
    ]
    amount = item.get("amount") or 0
    if fills:
        return "fills", len(fills)
    if income:
        return "income", len(income)
    if abs(amount) >= 0.01:
        return "cash", 0
    return "none", 0


def _hindsight(sentence):
    body = re.sub(r"^After the fact:\s*", "", sentence, flags=re.I).strip()
    body = body.replace(
        "expired worthless — the roll was never tested",
        "expired worthless, so the roll was never tested",
    )
    if body and not body.endswith("."):
        body += "."
    return f"In hindsight: {body}" if body else None


def _remember(cards, produced, account):
    for c in produced:
        if account and not c.get("account"):
            c["account"] = account
        cards.append(c)


def layout_story_items(items, account_labels=None):
    """Attach option_cards, share_cards, and cash_mode to each event day.

    Mutates ``items`` in place. Headlines are unchanged.
    """
    labels = [a for a in (account_labels or []) if a]
    for item in items or []:
        if item.get("type") != "day":
            continue
        cards = []
        for raw in item.get("headlines") or []:
            sentence, account = _strip_account(raw, labels)
            if sentence.startswith("After the fact:"):
                note = _hindsight(sentence)
                host = cards[-1] if cards else None
                if host is None:
                    host = _card("options", "", account=account)
                    cards.append(host)
                host["note"] = note
                if account and not host.get("account"):
                    host["account"] = account
                continue
            if (
                sentence.startswith("Wheel complete")
                or "premium already collected" in sentence
            ) and cards:
                prev = cards[-1]
                extra = sentence.rstrip(".")
                prev["detail"] = ((prev["detail"] + ". ") if prev["detail"] else "") + extra
                continue
            produced = _cards_for_sentence(sentence)
            if produced:
                _remember(cards, produced, account)
                continue
            cards.append(_fallback_card(sentence, account))
        if not cards:
            cards = _fill_fallback_cards(item.get("events"))
            # A suppressed structure wing is one account; leave account blank
            # unless every fill shared a label we were given. Single-account
            # reviews show the name in the header, not on the card.
        mode, count = _layout_cash(item)
        item["option_cards"] = [c for c in cards if c["lane"] == "options" and (c["title"] or c["note"])]
        item["share_cards"] = [c for c in cards if c["lane"] == "shares" and (c["title"] or c["note"])]
        item["cash_mode"] = mode
        item["fill_count"] = count
        label = item.get("label") or ""
        if "," in label:
            day, year = label.rsplit(",", 1)
            item["label_day"] = day.strip()
            item["label_year"] = year.strip()
        else:
            item["label_day"] = label
            item["label_year"] = ""
        # Delivery of assigned shares has a real cost on the swallowed fill.
        _stamp_delivery_cost(item)
    return items


def _stamp_delivery_cost(item):
    cost = 0.0
    found = False
    for e in item.get("events") or []:
        detail = e.get("detail") or ""
        if e.get("kind") == "buy" and " sh" in f" {detail}":
            amt = e.get("amount") or 0.0
            if abs(amt) >= 0.01:
                cost += amt
                found = True
    if not found:
        return
    shown = f"${abs(cost):,.2f}" if abs(cost - round(cost)) >= 0.001 else f"${abs(cost):,.0f}"
    for card in item.get("share_cards") or []:
        title = (card.get("title") or "").lower()
        if title.startswith("took delivery") and not card.get("metric"):
            card["metric"] = shown


def story_header(stats):
    """Symbol-card kicker: trade-day count, span, and the accounts involved."""
    if not stats or not stats.get("chapters"):
        return None
    span = ""
    if stats.get("span_days", 0) >= 14:
        span = _span_text(stats["span_days"])
    accounts = []
    seen = set()
    for lab in stats.get("accounts") or []:
        lab = (lab or "").strip()
        if lab and lab not in seen:
            seen.add(lab)
            accounts.append(lab)
    return {
        "days": int(stats["chapters"]),
        "span": span,
        "accounts": accounts,
    }


def _iso_day(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "date"):
        try:
            return value.date().isoformat()
        except (TypeError, ValueError):
            pass
    try:
        return pd.to_datetime(value).date().isoformat()
    except (TypeError, ValueError):
        return None


def close_pnl_by_day(frame, date_col, pnl_col):
    """Sum a closed-leg frame's P&L by close date (ISO). Empty when absent."""
    out = {}
    if frame is None or getattr(frame, "empty", True):
        return out
    if date_col not in frame.columns or pnl_col not in frame.columns:
        return out
    for _, row in frame.iterrows():
        iso = _iso_day(row.get(date_col))
        if not iso:
            continue
        try:
            pnl = float(row.get(pnl_col) or 0)
        except (TypeError, ValueError):
            continue
        if pnl != pnl:  # NaN
            continue
        out[iso] = out.get(iso, 0.0) + pnl
    return {k: round(v, 2) for k, v in out.items()}


def _fmt_realized(pnl):
    tone = "pos" if pnl > 0.005 else "neg" if pnl < -0.005 else ""
    sign = "+" if pnl > 0.005 else "-" if pnl < -0.005 else ""
    return f"{sign}${abs(pnl):,.2f} realized", tone


def _option_terminal(card):
    title = (card.get("title") or "").lower()
    return (
        title.startswith("assigned on")
        or title.startswith("the short")
        or title.startswith("exercised")
        or "expired" in title
    )


def _share_terminal(card):
    title = (card.get("title") or "").lower()
    metric = (card.get("metric") or "").lower()
    return (
        title.startswith("shares called away")
        or title.startswith("sold the last")
        or metric == "position closed"
    )


def _stamp_realized(card, pnl):
    if abs(pnl) < 0.01:
        return False
    if card.get("metric") and card.get("tone") in ("pos", "neg"):
        return False
    text, tone = _fmt_realized(pnl)
    if card.get("metric") and not card.get("detail"):
        card["detail"] = card["metric"]
    card["metric"] = text
    card["tone"] = tone
    return True


def _pnl_for_story_day(by_day, iso, used, window_days=5):
    """P&L whose close date is this story day, or the broker line a few days later.

    Option expiry is dated the contract's Friday in the warehouse; Schwab
    often posts the exercise/assignment fill the next session. A card on
    that later day is the same close. Two unlabeled closes in the window
    are left unmatched so a dollar is never pinned on the wrong contract.
    """
    if not iso:
        return None, None
    if iso in by_day and iso not in used:
        return iso, by_day[iso]
    try:
        target = date.fromisoformat(iso)
    except ValueError:
        return None, None
    near = []
    for key, pnl in by_day.items():
        if key in used:
            continue
        try:
            delta = (target - date.fromisoformat(key)).days
        except ValueError:
            continue
        if 0 < delta <= window_days:
            near.append((delta, key, pnl))
    if len(near) != 1:
        return None, None
    _delta, key, pnl = near[0]
    return key, pnl


def attach_realized_pnl(items, option_pnl_by_day=None, equity_pnl_by_day=None):
    """Fill a terminal card's dollar from closed-leg P&L when the sentence has none.

    Assignment and exercise sentences name the maneuver without the ledger
    result. Rolls and outright closes already carry their own figure, and
    those are left alone. A day with two unlabeled terminal cards is left
    alone too — splitting one total across them would mis-attribute it.
    """
    option_pnl_by_day = option_pnl_by_day or {}
    equity_pnl_by_day = equity_pnl_by_day or {}
    used_opt, used_eq = set(), set()
    for item in items or []:
        if item.get("type") != "day":
            continue
        iso = item.get("date_iso")
        opt_cards = [
            c for c in (item.get("option_cards") or [])
            if _option_terminal(c)
            and not (c.get("metric") and c.get("tone") in ("pos", "neg"))
        ]
        if len(opt_cards) == 1:
            key, pnl = _pnl_for_story_day(option_pnl_by_day, iso, used_opt)
            if key is not None and _stamp_realized(opt_cards[0], pnl):
                used_opt.add(key)
        sh_cards = [
            c for c in (item.get("share_cards") or [])
            if _share_terminal(c)
            and not (c.get("metric") and c.get("tone") in ("pos", "neg"))
        ]
        if len(sh_cards) == 1:
            key, pnl = _pnl_for_story_day(equity_pnl_by_day, iso, used_eq)
            if key is not None and _stamp_realized(sh_cards[0], pnl):
                used_eq.add(key)
    return items


def build_position_story(
    trades_df,
    div_df,
    chart_data=None,
    splits_df=None,
    seed_trades_df=None,
    exit_notes=None,
    label_map=None,
):
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

    ``seed_trades_df`` is optional full-history context for a filtered
    story. Fills before the first visible day and intervening splits are
    replayed silently into the per-tenant state machine. Without that seed,
    a leg/date-filtered post-split sell starts from zero shares and invents
    a phantom short even though the pre-split buy exists outside the view.
    Seed activity never contributes chapters or behavioral stats.

    ``exit_notes`` — optional {(tenant/account key, trade_symbol): verdict
    sentence} from the execution-review layer; appended to the completing
    close's headline (see _day_headline). Seed replay never narrates, so it
    never receives the notes.

    ``label_map`` — optional ``{tenant_id: display label}`` (the same map
    Position Detail uses for pills / Strategy Breakdown). Multi-account
    sentences append the nickname, not the colliding SnapTrade base
    label ("Schwab Account").
    """
    fills = _normalize_fills(trades_df)
    seed_fills = _normalize_fills(seed_trades_df)
    structure_plan = _plan_open_structures(fills)

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
    split_events = _normalize_splits(splits_df)
    splits_by_day = {}
    for d, ratio in split_events:
        if d >= first_day:
            splits_by_day.setdefault(d, []).append(ratio)

    all_days = sorted(set(fills_by_day) | set(div_by_day) | set(splits_by_day))

    # Multi-account detection follows the same tenant grain as state. Two
    # physical accounts with the same display label are still two accounts.
    accounts = {f["state_key"] for f in fills if f["state_key"]}
    multi_account = len(accounts) > 1

    state_by_account = {}

    # Reconstruct state at the filtered window boundary in the historical
    # share units that actually existed each day. Splits apply before that
    # day's fills, matching the visible replay below. This is narrative-only
    # state; dollars and warehouse P&L remain untouched.
    seed_by_day = {}
    for f in seed_fills:
        if f["date"] < first_day:
            seed_by_day.setdefault(f["date"], []).append(f)
    seed_splits_by_day = {}
    for d, ratio in split_events:
        if d < first_day:
            seed_splits_by_day.setdefault(d, []).append(ratio)
    seed_stats = _new_stats()
    for d in sorted(set(seed_by_day) | set(seed_splits_by_day)):
        for ratio in seed_splits_by_day.get(d, []):
            for st in state_by_account.values():
                st.shares *= ratio
        day_seed_fills = seed_by_day.get(d, [])
        if day_seed_fills:
            _day_headline(day_seed_fills, state_by_account, False, seed_stats)

    def _flat_everywhere():
        for st in state_by_account.values():
            if abs(st.shares) > 0.5:
                return False
            for rec in st.options.values():
                if abs(rec["net"]) > 0.01:
                    return False
        return True

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
            day_fills, state_by_account, multi_account, stats,
            exit_notes=exit_notes, label_map=label_map,
            structure_plan=structure_plan)
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

    account_labels = []
    seen_labels = set()
    for f in fills:
        lab = ((label_map or {}).get(f["state_key"]) or f.get("account") or "").strip()
        if lab and lab not in seen_labels:
            seen_labels.add(lab)
            account_labels.append(lab)
    stats["accounts"] = account_labels
    layout_story_items(story_items, account_labels)

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
