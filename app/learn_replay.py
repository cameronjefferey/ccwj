"""Historical trade replays for /learn.

Each replay is one YAML file in ``app/learn_replays/``. The file names the
strategy, the legs, a day-by-day price path, and the decision points.
Pages do not read the warehouse. Option marks in
``int_option_marks_daily`` belong to a customer's account, and this
public lesson must not publish them.

``price_source.kind`` is ``illustrative`` until a series is copied from
``stg_daily_prices`` and labeled with that table. Illustrative numbers
are a teaching path, not a quote and not a past trade.

"Premium" is allowed only when the replay collects money (a short
option). A long call or long put says "paid".
"""

from __future__ import annotations

import re
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import yaml

from app.learn_catalog import episode_by_slug
from app.money import fmt_money

_DIR = Path(__file__).resolve().parent / "learn_replays"
_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_CONTRACT = 100
_STRATEGIES = {
    ("long", "call"): "Long call",
    ("long", "put"): "Long put",
    ("short", "call"): "Covered call",
}
_MODES = ("close", "hold", "expire", "assigned", "roll")


def replays():
    return list(_REPLAYS)


def replay_by_slug(slug):
    for replay in _REPLAYS:
        if replay["slug"] == slug:
            return replay
    return None


def replays_for_lesson(slug):
    return [replay for replay in _REPLAYS if replay["lesson_slug"] == slug]


def sitemap_paths():
    return [(f"/learn/replay/{replay['slug']}", "monthly", "0.5") for replay in _REPLAYS]


def _D(value, slug, label):
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise ValueError(f"{slug}: {label} must be a number")
    try:
        number = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{slug}: {label} must be a number") from exc
    return number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _money(number, signed=False):
    return fmt_money(float(number), signed=signed)


def _pct(start, now):
    if start == 0:
        raise ValueError("percent move needs a non-zero start")
    return ((now - start) / start * Decimal(100)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def _pct_phrase(pct):
    amount = f"{abs(pct):f}"
    if "." in amount:
        amount = amount.rstrip("0").rstrip(".")
    direction = "down" if pct < 0 else "up"
    return f"{direction} {amount}%"


def _intrinsic(right, strike, underlying):
    if right == "call":
        raw = underlying - strike
    else:
        raw = strike - underlying
    return max(raw, Decimal("0.00"))


def _require_text(raw, slug, label):
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{slug}: {label} must be a non-empty string")
    return raw.strip()


def _day_map(days, slug):
    found = {}
    for day in days:
        if day["day"] in found:
            raise ValueError(f"{slug}: day {day['day']} is repeated")
        found[day["day"]] = day
    return found


def _option_pnl(side, entry, mark, contracts):
    gross = (mark - entry) * _CONTRACT * contracts
    if side == "short":
        gross = -gross
    return gross.quantize(Decimal("0.01"))


def _share_pnl(cost, mark, quantity):
    return ((mark - cost) * quantity).quantize(Decimal("0.01"))


def _choice_result(replay, decision, choice, by_day):
    """Dollar result of one choice, from the path. Not a second set of numbers."""
    slug = replay["slug"]
    side = replay["leg"]["side"]
    entry = replay["days"][0]["option"]
    contracts = replay["leg"]["contracts"]
    pause = by_day[decision["after_day"]]
    last = replay["days"][-1]
    mode = choice["mode"]
    shares = replay["shares"]

    if mode == "close":
        exit_day = pause
    elif mode == "hold":
        exit_day = by_day[choice["exit_day"]]
        if exit_day["day"] <= pause["day"]:
            raise ValueError(f"{slug}: hold must exit after the pause")
    elif mode in ("expire", "assigned"):
        exit_day = last
        if last["left"] != 0:
            raise ValueError(f"{slug}: {mode} needs the last day to be expiration")
    elif mode == "roll":
        exit_day = last
        if side != "short" or shares is None:
            raise ValueError(f"{slug}: roll is only for a covered call")
        if last["left"] != 0:
            raise ValueError(f"{slug}: roll marks the shares at expiration")
    else:
        raise ValueError(f"{slug}: unknown choice mode {mode}")

    option_pnl = _option_pnl(side, entry, exit_day["option"], contracts)
    detail = []
    assumption = choice.get("assumption")

    if side == "long":
        if mode == "assigned":
            raise ValueError(f"{slug}: a long option is not called away")
        result = option_pnl
        headline = result
    else:
        collected = (entry * _CONTRACT * contracts).quantize(Decimal("0.01"))
        if mode == "close":
            paid = (pause["option"] * _CONTRACT * contracts).quantize(Decimal("0.01"))
            option_pnl = (collected - paid).quantize(Decimal("0.01"))
            share = _share_pnl(shares["cost"], pause["underlying"], shares["quantity"])
            headline = (option_pnl + share).quantize(Decimal("0.01"))
            detail = [
                {"label": "Premium collected", "text": _money(collected), "sign": "flat"},
                {"label": "Paid to close", "text": _money(paid), "sign": "flat"},
                {"label": "Option result", "text": _money(option_pnl, signed=True), "sign": _sign(option_pnl)},
                {"label": "Shares still held", "text": _money(share, signed=True), "sign": _sign(share)},
            ]
        elif mode == "assigned":
            if replay["leg"]["right"] != "call":
                raise ValueError(f"{slug}: assignment here is a short call")
            if last["underlying"] <= replay["leg"]["strike"]:
                raise ValueError(f"{slug}: called away needs the stock above the strike at expiration")
            share = _share_pnl(shares["cost"], replay["leg"]["strike"], shares["quantity"])
            option_pnl = collected
            headline = (option_pnl + share).quantize(Decimal("0.01"))
            given_up = (
                (last["underlying"] - replay["leg"]["strike"]) * shares["quantity"]
            ).quantize(Decimal("0.01"))
            detail = [
                {"label": "Premium collected", "text": _money(collected), "sign": "flat"},
                {"label": "Shares called away", "text": _money(share, signed=True), "sign": _sign(share)},
                {"label": "Rise above the strike, not kept", "text": _money(given_up), "sign": "flat"},
            ]
        elif mode == "roll":
            credit = choice["new_credit"]
            paid = (pause["option"] * _CONTRACT * contracts).quantize(Decimal("0.01"))
            credit_cash = (credit * _CONTRACT * contracts).quantize(Decimal("0.01"))
            option_pnl = (collected - paid + credit_cash).quantize(Decimal("0.01"))
            share = _share_pnl(shares["cost"], last["underlying"], shares["quantity"])
            headline = (option_pnl + share).quantize(Decimal("0.01"))
            detail = [
                {"label": "Premium collected", "text": _money(collected), "sign": "flat"},
                {"label": "Paid to close", "text": _money(paid), "sign": "flat"},
                {"label": "Assumed credit on the new call", "text": _money(credit_cash), "sign": "flat"},
                {"label": "Option result", "text": _money(option_pnl, signed=True), "sign": _sign(option_pnl)},
                {"label": "Shares still held at the end", "text": _money(share, signed=True), "sign": _sign(share)},
            ]
        else:
            raise ValueError(f"{slug}: a covered call choice is close, called away, or roll")
        result = headline

    result_text = _money(result, signed=True)
    if result_text not in choice["explanation"]:
        raise ValueError(
            f"{slug}: choice {choice['id']} explanation must include {result_text}"
        )
    return {
        "id": choice["id"],
        "label": choice["label"],
        "result_label": choice["result_label"],
        "mode": mode,
        "result": float(result),
        "result_text": result_text,
        "sign": _sign(result),
        "explanation": choice["explanation"],
        "assumption": assumption,
        "detail": detail,
    }


def _sign(number):
    if number > 0:
        return "gain"
    if number < 0:
        return "loss"
    return "flat"


def _public_strings(replay, decisions):
    strings = [
        replay["title"],
        replay["summary"],
        replay["price_source"]["label"],
        replay["price_source"]["note"],
        replay["strategy"],
    ]
    strings.extend(replay["recap"])
    for decision in decisions:
        strings.append(decision["prompt"])
        strings.append(decision["question"])
        for choice in decision["choices"]:
            strings.extend([
                choice["label"],
                choice["result_label"],
                choice["explanation"],
                choice.get("assumption") or "",
            ])
            for line in choice["detail"]:
                strings.append(line["label"])
    return strings


def _validate(raw):
    if not isinstance(raw, dict):
        raise ValueError("replay must be an object")
    slug = raw.get("slug")
    if not isinstance(slug, str) or not _SLUG.fullmatch(slug):
        raise ValueError(f"bad replay slug: {slug!r}")
    number = raw.get("number")
    if not isinstance(number, int) or number < 1:
        raise ValueError(f"{slug}: number must be a positive integer")
    title = _require_text(raw.get("title"), slug, "title")
    strategy = _require_text(raw.get("strategy"), slug, "strategy")
    ticker = _require_text(raw.get("ticker"), slug, "ticker")
    if ticker.upper() != ticker or not ticker.isalpha():
        raise ValueError(f"{slug}: ticker must be uppercase letters")
    summary = _require_text(raw.get("summary"), slug, "summary")
    lesson_slug = raw.get("lesson_slug")
    episode = episode_by_slug(lesson_slug) if isinstance(lesson_slug, str) else None
    if episode is None:
        raise ValueError(f"{slug}: lesson_slug must be an Options 101 episode")
    if episode["slug"] == slug:
        raise ValueError(f"{slug}: replay slug collides with an episode")

    source = raw.get("price_source")
    if not isinstance(source, dict):
        raise ValueError(f"{slug}: price_source must be an object")
    kind = source.get("kind")
    label = _require_text(source.get("label"), slug, "price_source.label")
    note = _require_text(source.get("note"), slug, "price_source.note")
    if kind == "illustrative":
        if "illustrative" not in label.lower() or "illustrative" not in note.lower():
            raise ValueError(f"{slug}: illustrative prices must say so in the label and the note")
        if ticker not in note:
            raise ValueError(f"{slug}: the note must name the sample ticker")
    elif kind == "stg_daily_prices":
        if "stg_daily_prices" not in note:
            raise ValueError(f"{slug}: a warehouse path must name stg_daily_prices")
        if "illustrative" in label.lower():
            raise ValueError(f"{slug}: warehouse prices are not labeled illustrative")
    else:
        raise ValueError(f"{slug}: price_source.kind must be illustrative or stg_daily_prices")

    leg = raw.get("leg")
    if not isinstance(leg, dict):
        raise ValueError(f"{slug}: leg must be an object")
    side = leg.get("side")
    right = leg.get("right")
    if (side, right) not in _STRATEGIES or _STRATEGIES[(side, right)] != strategy:
        raise ValueError(f"{slug}: strategy does not match the leg")
    strike = _D(leg.get("strike"), slug, "strike")
    if strike <= 0:
        raise ValueError(f"{slug}: strike must be positive")
    contracts = leg.get("contracts")
    if contracts != 1:
        raise ValueError(f"{slug}: starter replays use one contract")

    shares = None
    if side == "short":
        raw_shares = raw.get("shares")
        if not isinstance(raw_shares, dict):
            raise ValueError(f"{slug}: a covered call needs shares")
        quantity = raw_shares.get("quantity")
        if quantity != _CONTRACT:
            raise ValueError(f"{slug}: one short call covers 100 shares")
        cost = _D(raw_shares.get("cost"), slug, "share cost")
        if cost <= 0:
            raise ValueError(f"{slug}: share cost must be positive")
        shares = {"quantity": quantity, "cost": cost}
    elif raw.get("shares") not in (None, {}):
        raise ValueError(f"{slug}: a long option does not carry a share lot")

    raw_days = raw.get("days")
    if not isinstance(raw_days, list) or len(raw_days) < 2:
        raise ValueError(f"{slug}: days must be a list of at least two sessions")
    days = []
    for index, row in enumerate(raw_days):
        if not isinstance(row, dict):
            raise ValueError(f"{slug}: each day must be an object")
        day_no = row.get("day")
        if not isinstance(day_no, int) or day_no < 1:
            raise ValueError(f"{slug}: day numbers start at 1")
        if index == 0 and day_no != 1:
            raise ValueError(f"{slug}: the first session is day 1")
        if index and day_no <= days[-1]["day"]:
            raise ValueError(f"{slug}: days must increase")
        underlying = _D(row.get("underlying"), slug, "underlying")
        option = _D(row.get("option"), slug, "option")
        left = row.get("left")
        if underlying <= 0 or option < 0:
            raise ValueError(f"{slug}: prices must be non-negative, and the stock positive")
        if not isinstance(left, int) or left < 0:
            raise ValueError(f"{slug}: left must be a non-negative integer")
        if index and left > days[-1]["left"]:
            raise ValueError(f"{slug}: days left cannot increase")
        days.append({
            "day": day_no,
            "underlying": underlying,
            "option": option,
            "left": left,
        })
    if days[-1]["left"] != 0:
        raise ValueError(f"{slug}: the path ends at expiration")
    intrinsic = _intrinsic(right, strike, days[-1]["underlying"])
    if days[-1]["option"] != intrinsic:
        raise ValueError(
            f"{slug}: expiration option value must be intrinsic ({_money(intrinsic)}), "
            f"got {_money(days[-1]['option'])}"
        )
    if shares is not None and days[0]["underlying"] != shares["cost"]:
        raise ValueError(f"{slug}: day 1 stock price is the share cost")

    by_day = _day_map(days, slug)
    raw_decisions = raw.get("decisions")
    if not isinstance(raw_decisions, list) or not raw_decisions:
        raise ValueError(f"{slug}: a replay needs at least one decision")
    decisions = []
    seen_ids = set()
    previous_day = 0
    for raw_decision in raw_decisions:
        if not isinstance(raw_decision, dict):
            raise ValueError(f"{slug}: each decision must be an object")
        decision_id = raw_decision.get("id")
        if not isinstance(decision_id, str) or not _SLUG.fullmatch(decision_id):
            raise ValueError(f"{slug}: bad decision id {decision_id!r}")
        if decision_id in seen_ids:
            raise ValueError(f"{slug}: decision ids must be unique")
        seen_ids.add(decision_id)
        after_day = raw_decision.get("after_day")
        if after_day not in by_day:
            raise ValueError(f"{slug}: decision {decision_id} pauses on a missing day")
        if after_day <= previous_day:
            raise ValueError(f"{slug}: decisions must move forward")
        previous_day = after_day
        prompt = _require_text(raw_decision.get("prompt"), slug, "prompt")
        question = _require_text(raw_decision.get("question"), slug, "question")
        facts = raw_decision.get("facts")
        if not isinstance(facts, dict):
            raise ValueError(f"{slug}: decision {decision_id} needs facts")
        pause = by_day[after_day]
        stock_move = facts.get("stock_move_pct")
        if isinstance(stock_move, bool) or not isinstance(stock_move, (int, float)):
            raise ValueError(f"{slug}: stock_move_pct must be a number")
        stock_pct = _pct(days[0]["underlying"], pause["underlying"])
        if stock_pct != Decimal(str(stock_move)).quantize(Decimal("0.01")):
            raise ValueError(
                f"{slug}: stock move at the pause is {stock_pct}%, facts say {stock_move}"
            )
        phrase = _pct_phrase(stock_pct)
        if re.search(rf"{re.escape(phrase)}(?!\d)", prompt) is None:
            raise ValueError(f"{slug}: prompt must include {phrase!r}")
        if "option_move_pct" in facts:
            option_move = facts["option_move_pct"]
            option_pct = _pct(days[0]["option"], pause["option"])
            if option_pct != Decimal(str(option_move)).quantize(Decimal("0.01")):
                raise ValueError(
                    f"{slug}: option move at the pause is {option_pct}%, facts say {option_move}"
                )
            option_phrase = _pct_phrase(option_pct)
            if re.search(rf"{re.escape(option_phrase)}(?!\d)", prompt) is None:
                raise ValueError(f"{slug}: prompt must include {option_phrase!r}")
        days_left = facts.get("days_left")
        if days_left != pause["left"]:
            raise ValueError(f"{slug}: days_left does not match the pause")
        if re.search(rf"(?<!\d){int(days_left)} day", prompt) is None:
            raise ValueError(f"{slug}: prompt must mention {days_left} day")

        raw_choices = raw_decision.get("choices")
        if not isinstance(raw_choices, list) or not 2 <= len(raw_choices) <= 3:
            raise ValueError(f"{slug}: a decision has two or three choices")
        choices = []
        choice_ids = set()
        built = {
            "slug": slug,
            "leg": {"side": side, "right": right, "strike": strike, "contracts": contracts},
            "shares": shares,
            "days": days,
        }
        decision_stub = {"after_day": after_day}
        for raw_choice in raw_choices:
            if not isinstance(raw_choice, dict):
                raise ValueError(f"{slug}: each choice must be an object")
            choice_id = raw_choice.get("id")
            if not isinstance(choice_id, str) or not _SLUG.fullmatch(choice_id):
                raise ValueError(f"{slug}: bad choice id {choice_id!r}")
            if choice_id in choice_ids:
                raise ValueError(f"{slug}: choice ids must be unique")
            choice_ids.add(choice_id)
            mode = raw_choice.get("mode")
            if mode not in _MODES:
                raise ValueError(f"{slug}: choice mode {mode!r} is not supported")
            label_text = _require_text(raw_choice.get("label"), slug, "choice label")
            result_label = _require_text(
                raw_choice.get("result_label"), slug, "result_label"
            )
            explanation = _require_text(raw_choice.get("explanation"), slug, "explanation")
            prepared = {
                "id": choice_id,
                "label": label_text,
                "result_label": result_label,
                "mode": mode,
                "explanation": explanation,
                "assumption": None,
            }
            if mode == "hold":
                exit_day = raw_choice.get("exit_day")
                if exit_day not in by_day:
                    raise ValueError(f"{slug}: hold exit_day is missing")
                prepared["exit_day"] = exit_day
            if mode == "roll":
                prepared["new_credit"] = _D(raw_choice.get("new_credit"), slug, "new_credit")
                if prepared["new_credit"] <= 0:
                    raise ValueError(f"{slug}: roll credit must be positive")
                assumption = _require_text(raw_choice.get("assumption"), slug, "assumption")
                if "not a quote" not in assumption.lower():
                    raise ValueError(f"{slug}: a roll must say the new price is not a quote")
                prepared["assumption"] = assumption
            elif raw_choice.get("assumption"):
                raise ValueError(f"{slug}: only a roll carries an assumption")
            choices.append(_choice_result(built, decision_stub, prepared, by_day))
        results = [choice["result"] for choice in choices]
        if len(set(results)) != len(results):
            raise ValueError(f"{slug}: choices at {decision_id} must not share a result")
        decisions.append({
            "id": decision_id,
            "after_day": after_day,
            "after_index": next(
                index for index, day in enumerate(days) if day["day"] == after_day
            ),
            "prompt": prompt,
            "question": question,
            "choices": choices,
        })

    recap_raw = raw.get("recap")
    if not isinstance(recap_raw, list) or not recap_raw:
        raise ValueError(f"{slug}: recap must be a list of paragraphs")
    recap = [_require_text(part, slug, "recap") for part in recap_raw]

    paid = (days[0]["option"] * _CONTRACT * contracts).quantize(Decimal("0.01"))
    if side == "long":
        entry_facts = [
            {"label": "Bought", "value": f"1 {right}"},
            {"label": "Strike", "value": _money(strike)},
            {"label": "Paid", "value": _money(paid)},
            {"label": "Sample", "value": ticker},
        ]
    else:
        entry_facts = [
            {"label": "Sold", "value": "1 call"},
            {"label": "Strike", "value": _money(strike)},
            {"label": "Premium collected", "value": _money(paid)},
            {"label": "Share cost", "value": _money(shares["cost"])},
        ]

    view_days = []
    for index, day in enumerate(days):
        option_pnl = _option_pnl(side, days[0]["option"], day["option"], contracts)
        share_pnl = None
        share_text = None
        if shares is not None:
            share_pnl = _share_pnl(shares["cost"], day["underlying"], shares["quantity"])
            share_text = _money(share_pnl, signed=True)
        left = day["left"]
        if left == 0:
            left_text = "Expiration"
        elif left == 1:
            left_text = "1 day left"
        else:
            left_text = f"{left} days left"
        view_days.append({
            "day": day["day"],
            "index": index,
            "underlying": float(day["underlying"]),
            "option": float(day["option"]),
            "pnl": float(option_pnl),
            "share_pnl": None if share_pnl is None else float(share_pnl),
            "underlying_text": _money(day["underlying"]),
            "option_text": _money(day["option"]),
            "pnl_text": _money(option_pnl, signed=True),
            "share_text": share_text,
            "left_text": left_text,
            "day_label": f"{index + 1} of {len(days)}",
        })

    draft = {
        "slug": slug,
        "title": title,
        "summary": summary,
        "strategy": strategy,
        "price_source": {"kind": kind, "label": label, "note": note},
        "recap": recap,
    }
    texts = " ".join(_public_strings(draft, decisions)).lower()
    if side == "long":
        if "premium" in texts:
            raise ValueError(f"{slug}: say paid, not premium, on a long option")
    elif "premium" not in texts:
        raise ValueError(f"{slug}: a covered call names the premium collected")

    return {
        "slug": slug,
        "number": number,
        "title": title,
        "strategy": strategy,
        "ticker": ticker,
        "summary": summary,
        "lesson_slug": episode["slug"],
        "lesson_title": episode["title"],
        "deeper_href": f"/learn/{episode['slug']}",
        "price_source": {"kind": kind, "label": label, "note": note},
        "show_shares": shares is not None,
        "entry_facts": entry_facts,
        "days": view_days,
        "last_index": len(view_days) - 1,
        "decisions": decisions,
        "recap": recap,
        "chart_label": "Option result and stock price, built one day at a time",
    }


def _load(directory=None):
    directory = Path(directory) if directory else _DIR
    paths = sorted(directory.glob("*.yaml"))
    if len(paths) < 3:
        raise ValueError("learn replays need a long call, a long put, and a covered call")
    loaded = [_validate(yaml.safe_load(path.read_text(encoding="utf-8"))) for path in paths]
    slugs = [replay["slug"] for replay in loaded]
    numbers = [replay["number"] for replay in loaded]
    if len(slugs) != len(set(slugs)):
        raise ValueError("replay slugs must be unique")
    if len(numbers) != len(set(numbers)):
        raise ValueError("replay numbers must be unique")
    strategies = [replay["strategy"] for replay in loaded]
    for required in ("Long call", "Long put", "Covered call"):
        if required not in strategies:
            raise ValueError(f"missing starter replay: {required}")
    episode_slugs = {replay["lesson_slug"] for replay in loaded}
    for replay in loaded:
        if replay["slug"] in episode_slugs:
            raise ValueError(f"{replay['slug']}: replay slug collides with an episode")
    loaded.sort(key=lambda replay: replay["number"])
    return tuple(loaded)


_REPLAYS = _load()
