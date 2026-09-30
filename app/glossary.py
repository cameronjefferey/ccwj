"""Plain-English glossary for column headers and labels.

Definitions are one or two short sentences. They name what a figure is.
They do not tell the reader what to trade.

``LEARN_MORE_BASE`` is the config hook for a future ``/learn/<slug>``
page. Leave it ``None`` until that route exists — the tooltip omits
the link rather than pointing at a 404.

TODO: set LEARN_MORE_BASE to "/learn" when /learn/<slug> ships, and
add the route that renders one entry per slug.
"""

from html import escape

from markupsafe import Markup

# TODO: point this at "/learn" when the learn pages exist.
LEARN_MORE_BASE = None

# slug -> entry. ``labels`` are the header strings that should pick this
# entry (matched case-insensitively, whitespace collapsed).
_ENTRIES = (
    {
        "slug": "strike-price",
        "title": "Strike price",
        "labels": (
            "strike price", "strike", "strike distance",
        ),
        "definition": (
            "The strike price is the share price named in an option contract. "
            "Strike distance is how far the stock price sits from that strike."
        ),
    },
    {
        "slug": "expiration-date",
        "title": "Expiration date",
        "labels": ("expiration date", "expiration", "expiry"),
        "definition": (
            "The expiration date is the last day an option contract is in force. "
            "After that, the contract is settled or it ends."
        ),
    },
    {
        "slug": "premium",
        "title": "Premium",
        "labels": (
            "premium", "net premium", "premium collected",
        ),
        "definition": (
            "Premium is the price of an option contract. "
            "The buyer pays it, and the seller collects it."
        ),
    },
    {
        "slug": "contract",
        "title": "Contract",
        "labels": ("contract", "contract (100 shares)", "contracts"),
        "definition": (
            "One option contract usually stands for 100 shares of the underlying stock."
        ),
    },
    {
        "slug": "expires-worthless",
        "title": "Expires worthless",
        "labels": ("expires worthless", "expire worthless", "expired worthless"),
        "definition": (
            "An option expires worthless when it ends with no value. "
            "The buyer is not paid, and the seller keeps the premium."
        ),
    },
    {
        "slug": "buyer-holder",
        "title": "Buyer / holder",
        "labels": ("buyer/holder", "buyer / holder", "buyer", "holder"),
        "definition": (
            "The buyer, also called the holder, pays the premium. "
            "That person has the right to exercise the contract."
        ),
    },
    {
        "slug": "seller-writer",
        "title": "Seller / writer",
        "labels": ("seller/writer", "seller / writer", "seller", "writer"),
        "definition": (
            "The seller, also called the writer, collects the premium. "
            "That person takes on the obligation if the contract is exercised."
        ),
    },
    {
        "slug": "exercise",
        "title": "Exercise",
        "labels": ("exercise", "exercised"),
        "definition": (
            "Exercise is the holder using the contract to buy or sell shares at the strike price."
        ),
    },
    {
        "slug": "assignment",
        "title": "Assignment",
        "labels": ("assignment", "assigned"),
        "definition": (
            "Assignment is when a short option is selected to fulfill the contract. "
            "The writer then buys or sells the shares at the strike."
        ),
    },
    {
        "slug": "long-short",
        "title": "Long / short",
        "labels": ("long/short", "long / short", "long", "short"),
        "definition": (
            "Long means the position was bought, so cash was paid to open it. "
            "Short means it was sold, so cash was collected to open it."
        ),
    },
    {
        "slug": "open-close",
        "title": "Open / close",
        "labels": ("open/close", "open / close", "open", "close"),
        "definition": (
            "Open means the position is still on, started by an opening trade. "
            "Close means it has ended, finished by a closing trade."
        ),
    },
    {
        "slug": "covered-call",
        "title": "Covered call",
        "labels": ("covered call",),
        "definition": (
            "A covered call is a call sold while holding at least 100 shares of the same stock."
        ),
    },
    {
        "slug": "called-away",
        "title": "Called away",
        "labels": ("called away",),
        "definition": (
            "Called away means the shares were sold because a short call was assigned."
        ),
    },
    {
        "slug": "moneyness",
        "title": "In / out / at the money",
        "labels": (
            "in/out/at the money",
            "in the money", "out of the money", "at the money",
            "itm", "otm", "atm", "moneyness",
        ),
        "definition": (
            "In the money means the stock is on the side of the strike where the option has intrinsic value. "
            "Out of the money means it does not, and at the money means the stock is about at the strike."
        ),
    },
    {
        "slug": "cost-basis",
        "title": "Cost basis",
        "labels": ("cost basis",),
        "definition": (
            "Cost basis is what was paid for the shares, using the average cost the broker reports."
        ),
    },
    {
        "slug": "cash-secured-put",
        "title": "Cash-secured put",
        "labels": (
            "cash-secured put", "cash secured put", "csp",
        ),
        "definition": (
            "A cash-secured put is a put sold while enough cash is set aside to buy the shares if assigned."
        ),
    },
    {
        "slug": "dte",
        "title": "DTE",
        "labels": ("dte", "days to expiration", "days to expiry"),
        "definition": (
            "DTE means days to expiration. "
            "It is how many days are left until the option's expiration date."
        ),
    },
    {
        "slug": "rolling",
        "title": "Rolling",
        "labels": ("rolling", "roll", "rolled"),
        "definition": (
            "Rolling is closing one option and opening another on the same stock. "
            "The new contract usually has a different strike or expiration."
        ),
    },
    {
        "slug": "annualized-return",
        "title": "Annualized return",
        "labels": ("annualized return", "annualized"),
        "definition": (
            "Annualized return restates a result as if that same pace had lasted a full year. "
            "It compares holds of different lengths."
        ),
    },
    {
        "slug": "spread",
        "title": "Spread",
        "labels": ("spread", "spreads"),
        "definition": (
            "A spread is two or more option contracts on the same stock opened together. "
            "One is usually bought and another sold."
        ),
    },
    {
        "slug": "max-profit-loss",
        "title": "Max profit / loss",
        "labels": (
            "max profit/loss", "max profit / loss",
            "max profit", "max loss", "maximum profit", "maximum loss",
        ),
        "definition": (
            "Max profit is the largest gain this structure can show if held to the outcome that pays the most. "
            "Max loss is the largest loss if held to the outcome that costs the most."
        ),
    },
    {
        "slug": "break-even",
        "title": "Break-even",
        "labels": ("break-even", "break even", "breakeven"),
        "definition": (
            "Break-even is the stock price where the position finishes flat, before fees."
        ),
    },
    {
        "slug": "theta",
        "title": "Theta",
        "labels": ("theta",),
        "definition": (
            "Theta is how much an option's price tends to change as one day passes, with the stock price unchanged."
        ),
    },
    {
        "slug": "delta",
        "title": "Delta",
        "labels": ("delta",),
        "definition": (
            "Delta is how much an option's price tends to change when the stock price moves by one dollar."
        ),
    },
    {
        "slug": "bid-ask",
        "title": "Bid / ask",
        "labels": ("bid/ask", "bid / ask", "bid", "ask"),
        "definition": (
            "The bid is the price buyers are offering, and the ask is the price sellers are asking. "
            "Together they are the quote for the contract."
        ),
    },
    {
        "slug": "realized-unrealized",
        "title": "Realized / unrealized",
        "labels": ("realized/unrealized", "realized / unrealized"),
        "definition": (
            "Realized profit or loss is the result on positions that are already closed. "
            "Unrealized profit or loss is the result on positions still open, using the latest mark."
        ),
    },
    {
        "slug": "realized",
        "title": "Realized",
        "labels": (
            "realized", "realized p&l", "realized pnl", "realized (period)",
        ),
        "definition": (
            "Realized profit or loss is the result on positions that are already closed."
        ),
    },
    {
        "slug": "unrealized",
        "title": "Unrealized",
        "labels": (
            "unrealized", "unrealized p&l", "unrealized pnl",
        ),
        "definition": (
            "Unrealized profit or loss is the result on positions that are still open, using the latest mark."
        ),
    },
    {
        "slug": "win-rate",
        "title": "Win rate",
        "labels": ("win rate",),
        "definition": (
            "Win rate is the share of closed trades that finished with a gain."
        ),
    },
    {
        "slug": "return-on-capital",
        "title": "Return on capital",
        "labels": ("return on capital", "roc"),
        "definition": (
            "Return on capital is the result divided by the money that was tied up in the position."
        ),
    },
)


def _norm_label(value) -> str:
    text = " ".join(str(value or "").replace("\u00a0", " ").split())
    return text.lower()


def glossary_entries():
    """Every glossary entry, in display order."""
    return list(_ENTRIES)


def learn_more_url(slug):
    """``/learn/<slug>`` once ``LEARN_MORE_BASE`` is set, else ``None``.

    TODO: wire this to url_for('learn_term', slug=slug) when that page exists.
    """
    base = (LEARN_MORE_BASE or "").strip().rstrip("/")
    slug = (slug or "").strip().strip("/")
    if not base or not slug:
        return None
    return f"{base}/{slug}"


_BY_LABEL = {}
for _entry in _ENTRIES:
    for _label in _entry["labels"]:
        _BY_LABEL[_norm_label(_label)] = _entry


def lookup_term(label=None, slug=None):
    """Resolve a header string or slug to one entry, or ``None``."""
    if slug:
        want = str(slug).strip().lower()
        for entry in _ENTRIES:
            if entry["slug"] == want:
                return entry
    key = _norm_label(label)
    if not key:
        return None
    found = _BY_LABEL.get(key)
    if found:
        return found
    # "Realized (period)" and similar window suffixes.
    if "(" in key:
        found = _BY_LABEL.get(key.split("(", 1)[0].strip())
        if found:
            return found
    return None


def _next_term_id(slug):
    try:
        from flask import g, has_request_context
        if has_request_context():
            n = int(getattr(g, "_ht_term_n", 0)) + 1
            g._ht_term_n = n
            return f"ht-term-{slug}-{n}"
    except Exception:
        pass
    return f"ht-term-{slug}"


def _pop_html(entry, tip_id):
    definition = escape(entry["definition"])
    more = learn_more_url(entry["slug"])
    more_html = ""
    if more:
        more_html = (
            f'<a class="ht-term-more" href="{escape(more)}">Learn more</a>'
        )
    return (
        f'<span id="{tip_id}" class="ht-term-pop" role="tooltip">'
        f'<span class="ht-term-def">{definition}</span>'
        f'{more_html}'
        '</span>'
    )


def _tip_markup(entry, trigger_html):
    tip_id = _next_term_id(entry["slug"])
    return Markup(
        '<span class="ht-term">'
        f'{trigger_html}'
        f'{_pop_html(entry, tip_id)}'
        '</span>'
    )


def render_term(label, slug=None):
    """The label itself is the definition control (dotted underline).

    No extra icon: Positions columns are fixed-width, and a sibling
    "i" was wide enough to ellipsize the header. Unknown labels render
    as escaped text, so a header can call this unconditionally.
    ``type="button"`` so the control does not submit a filter form.
    The visible word stays the accessible name; the definition is
    ``aria-describedby``.
    """
    entry = lookup_term(label, slug)
    safe = escape(str(label or ""))
    if not entry:
        return Markup(safe)
    tip_id = _next_term_id(entry["slug"])
    trigger = (
        f'<button type="button" class="ht-term-btn" aria-expanded="false" '
        f'aria-controls="{tip_id}" aria-describedby="{tip_id}">'
        f'{safe}</button>'
    )
    return Markup(
        '<span class="ht-term">'
        f'{trigger}'
        f'{_pop_html(entry, tip_id)}'
        '</span>'
    )


def render_term_link(label, href, slug=None):
    """Sortable header: the existing link is the definition control.

    A second icon beside the link ellipsizes fixed-width columns.
    Hover, focus, and a coarse-pointer tap still open the definition;
    a fine-pointer click follows ``href``.
    """
    entry = lookup_term(label, slug)
    safe = escape(str(label or ""))
    url = escape(str(href or ""), quote=True)
    if not entry:
        return Markup(f'<a href="{url}">{safe}</a>')
    tip_id = _next_term_id(entry["slug"])
    trigger = (
        f'<a class="ht-term-btn" href="{url}" aria-expanded="false" '
        f'aria-controls="{tip_id}" aria-describedby="{tip_id}">'
        f'{safe}</a>'
    )
    return Markup(
        '<span class="ht-term">'
        f'{trigger}'
        f'{_pop_html(entry, tip_id)}'
        '</span>'
    )


def render_term_mark(label, slug=None):
    """Small "i" when the visible label cannot be the control.

    Accounts KPI labels are rewritten with ``textContent``, which would
    delete a button wrapped around that text. Those cards are not
    fixed-width columns, so the icon can sit beside the label.
    """
    entry = lookup_term(label, slug)
    if not entry:
        return Markup("")
    tip_id = _next_term_id(entry["slug"])
    title = escape(entry["title"])
    trigger = (
        f'<button type="button" class="ht-term-btn ht-term-mark" '
        f'aria-expanded="false" aria-controls="{tip_id}" '
        f'aria-label="Definition of {title}">'
        '<span class="ht-term-i" aria-hidden="true">i</span>'
        '</button>'
    )
    return Markup(
        '<span class="ht-term">'
        f'{trigger}'
        f'{_pop_html(entry, tip_id)}'
        '</span>'
    )
