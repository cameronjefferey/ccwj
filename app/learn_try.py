"""Try-it tickets and the end-of-lesson checkpoint.

A Try it link opens /practice with the radios already set. It does not
place an order. The paper ticket can buy one call or one put. Lessons
about selling say so, and still open the long option of that type.

The two questions are a check, not a score. Answering both, or Mark as
done, records the episode. Watching 90% of the video still does too.
"""

from __future__ import annotations

from urllib.parse import urlencode

from app import learn_catalog as catalog
from app import learn_replay

_SIDES = {"call": "call", "put": "put"}
_TENORS = {
    "soon": "the nearest expiration",
    "weekly": "a weekly expiration",
    "multi": "a multi-week expiration",
}


def _ticket(side, tenor, sentence):
    side = _SIDES[side]
    return {
        "symbol": "SPY",
        "side": side,
        "tenor": tenor,
        "distance": "at",
        "button": f"Try a SPY {side}",
        "sentence": sentence,
        "href": practice_href("SPY", side, tenor, "at"),
    }


def practice_href(symbol, side, tenor, distance) -> str:
    return "/practice?" + urlencode({
        "symbol": symbol,
        "side": side,
        "tenor": tenor,
        "distance": distance,
    })


def _buy(side, tenor, extra=""):
    when = _TENORS[tenor]
    sentence = (
        f"Buy one SPY {side} on the paper account, at the price, {when}. "
        "The next page reviews it. Nothing is placed until you confirm."
    )
    if extra:
        sentence = extra + " " + sentence
    return _ticket(side, tenor, sentence)


# One paper trade per lesson. ``also`` is a second ticket on the same card.
_LESSONS = {
    "what-is-an-option": _buy("call", "soon"),
    "calls-and-puts": {
        **_buy("call", "soon"),
        "also": _buy("put", "soon"),
    },
    "strike-expiration-premium": _buy("call", "weekly"),
    "buying-and-selling": _buy("call", "soon"),
    "covered-calls": _buy(
        "call",
        "soon",
        "A covered call is a call sold against shares you hold. This ticket buys a call.",
    ),
    "cash-secured-puts": _buy(
        "put",
        "soon",
        "A cash-secured put is a put you sell. This ticket buys a put.",
    ),
    "the-wheel": _buy(
        "put",
        "soon",
        "The wheel often starts with a short put. This ticket buys a put.",
    ),
    "spreads": _buy(
        "call",
        "soon",
        "A spread is two contracts. This ticket is one long call.",
    ),
    "options-risk": _buy("call", "soon"),
    "reading-a-position": _buy("call", "soon"),
}

_REPLAYS = {
    "long-call": _buy("call", "soon"),
    "long-put": _buy("put", "soon"),
    "covered-call": _buy(
        "call",
        "soon",
        "A covered call is a call sold against shares you hold. This ticket buys a call.",
    ),
}

# Two questions each. The first choice is the one the episode teaches.
# Either answer can be picked. The explanation is the check.
_CHECKS = {
    "what-is-an-option": [
        {
            "prompt": "What do you pay for an option?",
            "choices": [
                ("premium", "A fee, called the premium", "That fee is the premium. It is the cost of the contract."),
                ("shares", "The full price of the shares", "The premium is the fee. You do not pay for the shares unless you use the contract."),
            ],
        },
        {
            "prompt": "Do you have to use the contract?",
            "choices": [
                ("no", "No. You can let it end", "You can use it, sell it, or let it end. The fee is what you spent."),
                ("yes", "Yes. You must buy the shares", "You do not have to use the right. Letting it end is allowed."),
            ],
        },
    ],
    "calls-and-puts": [
        {
            "prompt": "A call is the right to",
            "choices": [
                ("buy", "Buy", "A call is the right to buy the shares."),
                ("sell", "Sell", "That is a put. A call is the right to buy."),
            ],
        },
        {
            "prompt": "A put is the right to",
            "choices": [
                ("sell", "Sell", "A put is the right to sell the shares."),
                ("buy", "Buy", "That is a call. A put is the right to sell."),
            ],
        },
    ],
    "strike-expiration-premium": [
        {
            "prompt": "The locked price is the",
            "choices": [
                ("strike", "Strike", "The strike is the locked price. The premium is the fee."),
                ("premium", "Premium", "The premium is the fee. The strike is the locked price."),
            ],
        },
        {
            "prompt": "One standard equity contract covers",
            "choices": [
                ("hundred", "100 shares", "One contract is 100 shares."),
                ("one", "1 share", "One contract is 100 shares, so the fee is 100 times the price you see."),
            ],
        },
    ],
    "buying-and-selling": [
        {
            "prompt": "The buyer of an option is",
            "choices": [
                ("long", "Long", "The buyer is long. The seller is short."),
                ("short", "Short", "The seller is short. The buyer is long."),
            ],
        },
        {
            "prompt": "Assignment happens to the",
            "choices": [
                ("seller", "Seller", "Exercise is the buyer using the right. Assignment is the seller's side of that."),
                ("buyer", "Buyer", "The buyer exercises. The seller is assigned."),
            ],
        },
    ],
    "covered-calls": [
        {
            "prompt": "A covered call is sold against shares you",
            "choices": [
                ("hold", "Already hold", "Covered means the shares are already in the account."),
                ("lack", "Do not own", "Without the shares, the short call is not covered."),
            ],
        },
        {
            "prompt": "Called away means the shares",
            "choices": [
                ("sold", "Can be sold at the strike", "Assignment sells the shares at the strike."),
                ("kept", "Stay no matter the price", "If the call finishes in the money, the shares can be called away."),
            ],
        },
    ],
    "cash-secured-puts": [
        {
            "prompt": "The cash set aside is there to",
            "choices": [
                ("buy", "Buy the shares if you are assigned", "Assignment buys the shares. The cash is what pays for them."),
                ("sell", "Sell a call", "This contract is a put. The cash is for buying shares if assigned."),
            ],
        },
        {
            "prompt": "The premium you collect",
            "choices": [
                ("lowers", "Lowers the net price if you buy the shares", "The fee you collected comes off the price you pay if assigned."),
                ("extra", "Is a second purchase of the stock", "It is a fee you collected, not another share purchase."),
            ],
        },
    ],
    "the-wheel": [
        {
            "prompt": "The wheel often starts by",
            "choices": [
                ("put", "Selling a put", "A short put can turn into shares. Covered calls come after that."),
                ("spread", "Buying a call spread", "The wheel here is a short put, then covered calls if you take the shares."),
            ],
        },
        {
            "prompt": "Rolling an option means",
            "choices": [
                ("both", "Closing one and opening another", "The close and the new open are the roll."),
                ("delete", "Deleting the shares", "The shares stay. The option contract is the part that changes."),
            ],
        },
    ],
    "spreads": [
        {
            "prompt": "A vertical spread uses two options with different",
            "choices": [
                ("strikes", "Strikes", "Same type, same expiration, two strikes."),
                ("names", "Underlyings", "Both contracts are on the same shares. The strikes differ."),
            ],
        },
        {
            "prompt": "Defined risk means the worst case is",
            "choices": [
                ("known", "Known when you open it", "The two strikes cap the gain and the loss."),
                ("open", "Whatever the stock does later", "A spread's worst case is set by the strikes, not an open-ended move."),
            ],
        },
    ],
    "options-risk": [
        {
            "prompt": "The most a long option can lose is",
            "choices": [
                ("premium", "The premium you paid", "You can lose the fee. You do not owe more than that on a long option."),
                ("company", "The whole company", "A long option's loss stops at the premium."),
            ],
        },
        {
            "prompt": "Theta is how the price changes as",
            "choices": [
                ("time", "Time passes", "Theta is the time piece. Delta is the piece that follows the shares."),
                ("split", "The stock splits", "A split changes the contract's terms. Theta is time passing."),
            ],
        },
    ],
    "reading-a-position": [
        {
            "prompt": "Realized is the result of a position that is",
            "choices": [
                ("closed", "Closed", "Realized is the close. Unrealized is still open."),
                ("open", "Still open", "An open result is unrealized. Realized is the close."),
            ],
        },
        {
            "prompt": "A high win rate can still lose money when",
            "choices": [
                ("size", "The losses are larger than the wins", "The count of wins and the dollars are different."),
                ("closed", "The market is closed", "The market being closed does not change a win rate into a loss."),
            ],
        },
    ],
}


def _public_ticket(raw):
    if not raw:
        return None
    ticket = {
        "symbol": raw["symbol"],
        "side": raw["side"],
        "tenor": raw["tenor"],
        "distance": raw["distance"],
        "button": raw["button"],
        "sentence": raw["sentence"],
        "href": raw["href"],
    }
    also = raw.get("also")
    if also:
        ticket["also"] = {
            "button": also["button"],
            "href": also["href"],
        }
    return ticket


def for_lesson(slug):
    return _public_ticket(_LESSONS.get(slug))


def for_replay(slug):
    return _public_ticket(_REPLAYS.get(slug))


def checks_for_lesson(slug):
    rows = []
    for item in _CHECKS.get(slug) or []:
        rows.append({
            "prompt": item["prompt"],
            "choices": [
                {"id": choice[0], "label": choice[1], "explain": choice[2]}
                for choice in item["choices"]
            ],
        })
    return rows


def deeper_for_lesson(slug):
    """Replay for this lesson, otherwise the next episode, otherwise the series."""
    replays = learn_replay.replays_for_lesson(slug)
    if replays:
        return {"href": f"/learn/replay/{replays[0]['slug']}", "label": "Go deeper"}
    _previous, nxt = catalog.neighbors(slug)
    if nxt:
        return {"href": f"/learn/{nxt['slug']}", "label": "Go deeper"}
    return {"href": "/learn#replays", "label": "Go deeper"}
