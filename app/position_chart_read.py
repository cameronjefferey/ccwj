"""Chart read for Position Detail.

Compares the equity line and the options line already drawn on the
cumulative P&L chart, plus where trade-day dots sit. The page shows the
opening sentence to everyone. The rest is blurred until HappyTrader AI
is unlocked. The model may only restate these figures. It does not get
a new warehouse query, and it does not get to recommend a trade.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re

from app.db import execute, fetch_one

_log = logging.getLogger(__name__)

_MIN_POINTS = 12
_MIN_MOVE = 50.0
_DAY_EPS = 1.0
_MIN_DROP = 500.0
_MIN_OPT_GIVEBACK = 200.0
_MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()

_SYSTEM = """You are reading one position for the person who traded it.

You receive that position's trade-day review, in order. Those lines are the only source. Different positions teach different lessons. Read these lines and say what THIS record shows. Do not apply a lesson from some other chart.

Write at most 3 sentences, about 70 words. Address the reader as "you", not "the trader". No title and no bullets. Do not list the premiums or the contracts that expired. Name only the days and figures the lesson uses.
- Sentence 1 is the story of this position.
- One sentence names the comparison the lines support, including which days. When a line gives both what happened and what holding would have produced, use that. Do not merge two different results into one phrase. Do not describe a holding result for a day whose line does not state one.
- The last sentence starts with "The lesson from this chart". Quantify it. Add every "gave up $X vs holding" amount on a line that also says the contract expired worthless, and put that sum in the sentence as what closing cost versus waiting. Do not describe what you were thinking or why you closed.

Use ordinary money words: bought back, paid, expired, kept. Do not write "gave up", "compared to holding", "versus holding", or "early closure".
When a review line says "gave up $X vs holding" and the contract expired worthless, the number to quote is $X, the amount you paid to buy it back. Say you paid that amount, and that the call would have expired worthless. Do not quote the smaller "net loss" from that same line. Do not say that waiting would have cost $X.
Use dollar figures only as they appear in the lines. Do not say approximately. Do not guess why they traded. Do not invent how long the position lasted, a win rate, or a count the lines do not state. Do not tell them what they should do.
"""


def _f(v) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _money(n: float) -> str:
    sign = "-" if n < 0 else ""
    return f"{sign}${abs(n):,.0f}"


def _day_label(iso: str) -> str:
    year, month, day = str(iso)[:10].split("-")
    return f"{_MONTHS[int(month) - 1]} {int(day)}, {year}"


def _worst_equity_drop(eq: list[float]):
    """Peak-to-later-trough with the largest dollar drop. None if the line never falls."""
    peak = eq[0]
    peak_i = 0
    worst = 0.0
    found = None
    for i, v in enumerate(eq):
        if v >= peak:
            peak = v
            peak_i = i
        drop = v - peak
        if drop < worst:
            worst = drop
            found = (peak_i, i, drop)
    return found


def chart_path_facts(chart: dict | None, markers: list | None) -> dict | None:
    """Return measured equity-vs-options facts, or None when the chart
    is too short or only one line moves."""
    if not chart:
        return None
    dates = list(chart.get("dates") or [])
    equity = list(chart.get("equity") or [])
    options = list(chart.get("options") or [])
    n = min(len(dates), len(equity), len(options))
    if n < _MIN_POINTS:
        return None
    eq = [_f(equity[i]) for i in range(n)]
    opt = [_f(options[i]) for i in range(n)]
    if max(eq) - min(eq) < _MIN_MOVE or max(opt) - min(opt) < _MIN_MOVE:
        return None

    up_opt = down_opt = 0.0
    up_days = down_days = 0
    for i in range(1, n):
        de = eq[i] - eq[i - 1]
        do = opt[i] - opt[i - 1]
        if de >= _DAY_EPS:
            up_days += 1
            up_opt += do
        elif de <= -_DAY_EPS:
            down_days += 1
            down_opt += do
    index = {str(dates[i])[:10]: i for i in range(n)}
    trade_up = trade_down = 0
    for m in markers or []:
        if not isinstance(m, dict):
            continue
        i = index.get(str(m.get("d") or "")[:10])
        if i is None or i == 0:
            continue
        j = max(0, i - 5)
        slope = eq[i] - eq[j]
        if slope >= _DAY_EPS:
            trade_up += 1
        elif slope <= -_DAY_EPS:
            trade_down += 1
    trades = trade_up + trade_down

    options_worse_on_declines = (
        up_days >= 3 and down_days >= 3
        and down_opt < -_MIN_MOVE and down_opt < up_opt - _MIN_MOVE
    )
    trades_on_rises = (
        up_days >= 3 and down_days >= 3
        and trades >= 4 and trade_up >= 3 and trade_up >= 2 * max(trade_down, 1)
    )

    window = _worst_equity_drop(eq)
    trades_before_peak = trades_during_drop = 0
    window_option_loss = False
    peak_i = trough_i = None
    if window is not None:
        peak_i, trough_i, equity_drop = window
        opt_during = opt[trough_i] - opt[peak_i]
        for m in markers or []:
            if not isinstance(m, dict):
                continue
            i = index.get(str(m.get("d") or "")[:10])
            if i is None:
                continue
            if i <= peak_i:
                trades_before_peak += 1
            elif i <= trough_i:
                trades_during_drop += 1
        window_option_loss = (
            peak_i >= 3
            and equity_drop <= -_MIN_DROP
            and opt_during <= -_MIN_OPT_GIVEBACK
            and trades_before_peak >= 4
            and trades_before_peak >= 2 * max(trades_during_drop, 1)
        )
    if not options_worse_on_declines and not trades_on_rises and not window_option_loss:
        return None

    facts = {
        "up_days": up_days,
        "down_days": down_days,
        "up_opt": round(up_opt, 2),
        "down_opt": round(down_opt, 2),
        "trade_up": trade_up,
        "trade_down": trade_down,
        "trades": trades,
        "options_worse_on_declines": options_worse_on_declines,
        "trades_on_rises": trades_on_rises,
        "window_option_loss": window_option_loss,
        "peak_label": _day_label(dates[peak_i]) if window_option_loss else "",
        "trough_label": _day_label(dates[trough_i]) if window_option_loss else "",
        "equity_drop": round(eq[trough_i] - eq[peak_i], 2) if window_option_loss else 0,
        "options_at_peak": round(opt[peak_i], 2) if window_option_loss else 0,
        "options_at_trough": round(opt[trough_i], 2) if window_option_loss else 0,
        "options_during_drop": round(opt[trough_i] - opt[peak_i], 2) if window_option_loss else 0,
        "trades_before_peak": trades_before_peak if window_option_loss else 0,
        "trades_during_drop": trades_during_drop if window_option_loss else 0,
    }
    return facts


def chart_read_sentences(facts: dict) -> tuple[str, str]:
    """Opening line, then the rest. Both are evidence, not a suggestion."""
    if facts.get("window_option_loss"):
        before = facts["trades_before_peak"]
        during = facts["trades_during_drop"]
        high = facts["peak_label"]
        low = facts["trough_label"]
        if during == 0:
            where = (
                f"All {before} trade days landed on the way up to the share high "
                f"on {high}. None landed while the shares fell through {low}."
            )
        else:
            where = (
                f"{before} trade days landed on the way up to the share high on {high}, "
                f"and {during} landed while the shares fell through {low}."
            )
        lead = (
            f"{where} Option P&L went from {_money(facts['options_at_peak'])} at that high "
            f"to {_money(facts['options_at_trough'])} at the low."
        )
        rest = (
            f"The trades and the option loss are in different parts of this chart. "
            f"The climb into {high} is where this position was active. The drop through "
            f"{low} is where the option line gave back {_money(abs(facts['options_during_drop']))}, "
            f"and it is the quiet part of the record"
            + (
                "."
                if during == 0
                else f" — {during} trade days against {before} on the way up."
            )
            + " The decline is the stretch this position barely traded."
        )
        return lead, rest
    lead_bits = []
    if facts["options_worse_on_declines"]:
        lead_bits.append(
            f"On days the shares fell, option P&L changed by {_money(facts['down_opt'])}. "
            f"On days the shares rose, it changed by {_money(facts['up_opt'])}."
        )
    if facts["trades_on_rises"]:
        lead_bits.append(
            f"{facts['trade_up']} of {facts['trades']} trade days landed while the shares "
            f"were already moving up, and {facts['trade_down']} while they were moving down."
        )
    lead = " ".join(lead_bits)
    rest = (
        f"That split is across {facts['up_days']} rising sessions and "
        f"{facts['down_days']} falling sessions on this chart. "
        "The option line is the running option result, realized and still open, "
        "not a forecast of what a different timing would have kept."
    )
    return lead, rest


def _ensure_table() -> None:
    execute(
        """
        CREATE TABLE IF NOT EXISTS position_chart_reads (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            symbol      TEXT NOT NULL,
            scope       TEXT NOT NULL DEFAULT '',
            brief_hash  TEXT NOT NULL,
            brief       TEXT NOT NULL,
            lead        TEXT NOT NULL,
            body        TEXT,
            generated_at TIMESTAMPTZ,
            UNIQUE (user_id, symbol, scope, brief_hash)
        )
        """
    )


def _hash(facts: dict) -> str:
    raw = json.dumps(facts, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def store_chart_brief(user_id, symbol: str, scope: str, facts: dict, lead: str) -> str:
    _ensure_table()
    digest = _hash(facts)
    execute(
        """
        INSERT INTO position_chart_reads (user_id, symbol, scope, brief_hash, brief, lead)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id, symbol, scope, brief_hash) DO NOTHING
        """,
        (user_id, symbol.upper(), scope, digest, json.dumps(facts), lead),
    )
    return digest


def load_chart_read(user_id, symbol: str, scope: str, digest: str):
    _ensure_table()
    return fetch_one(
        """
        SELECT lead, body, brief FROM position_chart_reads
        WHERE user_id = %s AND symbol = %s AND scope = %s AND brief_hash = %s
        """,
        (user_id, symbol.upper(), scope, digest),
    )


def review_brief(markers) -> dict | None:
    """The trade-day lines already written for this position. This is what
    the model reads. Nothing here decides the lesson."""
    lines = []
    for m in markers or []:
        if not isinstance(m, dict):
            continue
        text = " ".join(m.get("t") or []).strip()
        if not text:
            continue
        lines.append({"date": str(m.get("d") or "")[:10], "line": text})
    if len(lines) < 4:
        return None
    return {"review_lines": lines[:80], "read": 7}


_PROSE_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2})\b"
)
_MONTH_NUMBER = {
    name: i + 1
    for i, name in enumerate(
        "January February March April May June July August September October November December".split()
    )
}


def _line_supports_holding(line: str) -> bool:
    return "vs holding" in line or "After the fact" in line


def _ungrounded(cleaned: str, lines: list[dict]) -> str | None:
    """A holding claim, or a dollar, that the review lines do not contain."""
    lowered = cleaned.lower()
    if "winning trade" in lowered or "win rate" in lowered:
        return "invented a win rate"
    by_md = {}
    source_amounts = set()
    for row in lines:
        year, month, day = row["date"].split("-")
        by_md[(int(month), int(day))] = row["line"]
        for amount in re.findall(r"\$([0-9,]+(?:\.\d+)?)", row["line"]):
            source_amounts.add(round(float(amount.replace(",", "")), 2))
    for match in _PROSE_DATE_RE.finditer(cleaned):
        month = _MONTH_NUMBER[match.group(1)]
        key = (month, int(match.group(2)))
        sentence_end = cleaned.find(".", match.start())
        sentence = cleaned[max(0, cleaned.rfind(".", 0, match.start()) + 1): sentence_end if sentence_end != -1 else match.start() + 180]
        if len(_PROSE_DATE_RE.findall(sentence)) > 2:
            continue
        if re.search(r"\bheld\b|holding", sentence, re.I):
            line = by_md.get(key, "")
            if not _line_supports_holding(line):
                return f"{match.group(0)} has no holding result in the review"
    for amount in re.findall(r"\$([0-9,]+(?:\.\d+)?)", cleaned):
        value = round(float(amount.replace(",", "")), 2)
        if value not in source_amounts and not _is_small_sum(value, source_amounts):
            return f"${amount} is not in the review"
    for row in lines:
        net = re.search(r"net \$([0-9,]+(?:\.\d+)?) loss", row["line"], re.I)
        gave = re.search(r"gave up \$([0-9,]+(?:\.\d+)?)", row["line"], re.I)
        if not net or not gave:
            continue
        net_amt = "$" + net.group(1)
        paid_amt = "$" + gave.group(1)
        if net_amt in cleaned:
            return f"quote {paid_amt}, the amount paid to buy it back, not the {net_amt} net loss"
    total = _waiting_cost(lines)
    if total and _money(total) not in cleaned.split("The lesson from this chart")[-1] and _money(total) not in cleaned.split("the lesson from this chart")[-1]:
        return f"the lesson must include the total {_money(total)}, the cost of closing instead of waiting"
    return None


def _waiting_cost(lines: list[dict]) -> float:
    """Sum of give-up amounts on lines that say the contract expired worthless."""
    total = 0.0
    found = False
    for row in lines:
        if "expired worthless" not in row["line"]:
            continue
        gave = re.search(r"gave up \$([0-9,]+(?:\.\d+)?)", row["line"], re.I)
        if not gave:
            continue
        total += float(gave.group(1).replace(",", ""))
        found = True
    return round(total, 2) if found else 0.0


def _too_long(cleaned: str) -> str | None:
    sentences = [s for s in re.split(r"(?<=[.!?])\s+", cleaned.strip()) if s.strip()]
    if len(sentences) > 3:
        return "too long; use at most 3 sentences"
    if not cleaned.rstrip().endswith((".", "!", "?")):
        return "cut off; finish the last sentence"
    if len(re.findall(r"\$", cleaned)) > 6:
        return "too many dollar figures; name only the ones the lesson uses"
    if "the lesson from this chart" not in cleaned.lower():
        return 'the last sentence must start with "The lesson from this chart"'
    return _plain_language(cleaned)


def _plain_language(cleaned: str) -> str | None:
    jargon = ("gave up", "compared to holding", "versus holding", "early closure")
    if any(phrase in cleaned.lower() for phrase in jargon):
        return "use bought back, paid, and expired instead of review jargon"
    if re.search(r"\bshould\b", cleaned, re.I):
        return "do not tell them what they should do"
    if "the trader" in cleaned.lower():
        return 'say "you", not "the trader"'
    if re.search(r"\bbeliev", cleaned, re.I):
        return "do not describe what you were thinking"
    return None


def _is_small_sum(target: float, amounts: set[float]) -> bool:
    from itertools import combinations
    cents = [round(n * 100) for n in amounts if 0 < n <= target]
    target_cents = round(target * 100)
    for size in (2, 3, 4):
        for combo in combinations(cents, size):
            if sum(combo) == target_cents:
                return True
    return False


def generate_chart_body(facts: dict) -> str | None:
    from app.llm import call_llm, llm_available

    if not llm_available():
        return None
    lines = facts.get("review_lines") or []
    if not lines:
        return None
    user = "Trade-day review:\n" + json.dumps(lines, indent=2)
    for _attempt in range(3):
        text, err = call_llm(
            _SYSTEM,
            user,
            kind="position.chart_read",
            max_tokens=220,
            temperature=0.2,
            allow_paid=False,
        )
        if err or not text:
            _log.info("chart read skipped: %s", err)
            return None
        cleaned = re.sub(r"(?m)^#+\s+.*\n+", "", text.strip()).strip()
        problem = _ungrounded(cleaned, lines) or _too_long(cleaned)
        if not problem:
            return cleaned or None
        _log.info("chart read redraft: %s", problem)
        user += f"\n\nYour draft was not used: {problem}. Rewrite from the lines only."
        last = cleaned
    kept = [
        sentence.strip()
        for sentence in re.split(r"(?<=\.)\s+", last)
        if sentence.strip()
        and _ungrounded(sentence, lines) is None
        and _plain_language(sentence) is None
    ]
    text = " ".join(kept[:3])
    if not text or _too_long(text):
        return None
    return text
