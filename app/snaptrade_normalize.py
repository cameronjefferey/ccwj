"""SnapTrade → seed-CSV normalization.

SnapTrade is a brokerage aggregator that returns activities, positions,
and balances in its own JSON shape across ~20 brokers. This module
translates that shape into the three seed contracts the rest of the
pipeline already understands:

* ``HISTORY_SEED_COLUMNS`` (trade_history.csv)
* ``CURRENT_SEED_COLUMNS`` (current_positions.csv)
* ``BALANCE_SEED_COLUMNS`` (account_balances.csv)

Once a DataFrame leaves this module it goes through the same
``merge_and_push_seeds`` boundary as native Schwab sync and manual CSV
upload, so the broker-sync-safety invariants (tenant scope, dedup,
canonical uid, monotonic merge) all apply automatically.

This file contains NO network calls and NO Postgres writes — it is
pure data shaping so it can be unit-tested without touching SnapTrade
or the database.

The bug class to remember while editing this file: SnapTrade fields
named ``price``, ``value``, ``amount`` lie about their semantics the
same way Schwab's ``averagePrice`` did (it was per-share cost basis,
not current price; see broker-sync-safety SKILL 2026-05-11). For
positions, derive ``Price = market_value / quantity`` for equities
rather than trusting any per-share field SnapTrade returns; the
``positions_to_current_df`` enforcement of the ``qty * Price ==
market_value`` invariant matches the dbt regression test
``int_enriched_current_equity_price_consistent.sql``.
"""
from __future__ import annotations

import hashlib
import logging
import math
import re
from datetime import datetime, date
from typing import Iterable, Mapping, Optional, Sequence

import pandas as pd

from app.upload import (
    BALANCE_SEED_COLUMNS,
    CRYPTO_SYMBOLS,
    CURRENT_SEED_COLUMNS,
    HISTORY_SEED_COLUMNS,
    canonicalize_crypto_pair_symbol,
    symbol_defaults_to_crypto,
)

_log = logging.getLogger(__name__)


def _seed_user_id(user_id):
    """Informational seed ``user_id``. Blank for a demo session or non-numeric id.

    ``int("demo-session:…")`` raises. A blank stamp does not invent a
    Postgres id on the raw row.
    """
    from app.demo_guard import numeric_user_id
    uid = numeric_user_id(user_id)
    return "" if uid is None else uid


# ---------------------------------------------------------------------------
# Crypto detection
# ---------------------------------------------------------------------------
# SnapTrade's UniversalSymbol carries a ``type`` block (``code`` +
# ``description``). The canonical crypto code is ``"crypto"``; some
# broker-specific feeds emit ``"CRYPTO"``, ``"Cryptocurrency"``, or
# ``"DIGITAL_ASSET"``. We accept any of those as a positive signal AND
# fall back to a symbol-whitelist match (``app.upload.CRYPTO_SYMBOLS``)
# so the classification is consistent with the dbt ``stg_crypto_symbols``
# whitelist downstream — see ``int_strategy_classification`` for where
# the warehouse decides "Crypto" vs "Buy and Hold".
#
# Why both signals: SnapTrade's per-broker normalization isn't 100%
# uniform — early Coinbase responses (May 2026) shipped crypto rows
# WITHOUT the type code set, so the type-only check would miss them.
# The whitelist guarantees BTC / ETH / USDC etc. get tagged regardless.
_CRYPTO_TYPE_CODES = frozenset({"CRYPTO", "CRYPTOCURRENCY", "DIGITAL_ASSET"})


def _is_crypto(symbol_obj: Mapping) -> bool:
    """Decide whether a SnapTrade position/activity's symbol is crypto.

    Precedence (broker signal wins over the ticker whitelist):
      1. ``symbol.symbol.type.code`` (or ``symbol.type.code`` for flattened
         payloads) matches a known crypto code  → crypto.
      2. The broker gave us an EXPLICIT, non-crypto ``type`` block (e.g.
         common stock ``code='cs'``, ETF ``code='et'``)  → NOT crypto,
         and we do NOT consult the ticker whitelist. The broker's own
         instrument type is authoritative.
      3. No usable ``type`` block at all (e.g. early Coinbase rows that
         omitted it)  → fall back to the curated CRYPTO_SYMBOLS whitelist.

    Why the broker signal must win (2026-07-14): several crypto tickers
    COLLIDE with real equities/ETFs — ``SEI`` is both the Sei token AND
    Solaris Energy Infrastructure (NYSE). A user holding Solaris on Schwab
    (an equity-only broker) had the share tagged ``Cryptocurrency`` purely
    because the ticker was on the whitelist, which then rendered a "Crypto"
    strategy row on an equity + long-call position. SnapTrade's
    UniversalSymbol carries ``type.code='cs'`` for that share, so trusting
    the broker resolves the collision at the source. Other colliding
    tickers (LINK, COMP, UNI, W, …) get the same protection for free.

    Returns False for non-Mapping inputs / unknown shapes — better to
    fall through to "Equity" than mis-classify an equity row as crypto.
    """
    if not isinstance(symbol_obj, Mapping):
        return False
    saw_type_block = False
    for candidate in (
        symbol_obj.get("symbol") if isinstance(symbol_obj.get("symbol"), Mapping) else None,
        symbol_obj,
    ):
        if not isinstance(candidate, Mapping):
            continue
        type_block = candidate.get("type")
        if isinstance(type_block, Mapping):
            code = str(type_block.get("code") or "").strip().upper()
            desc = str(type_block.get("description") or "").strip().upper()
            if code in _CRYPTO_TYPE_CODES or "CRYPTO" in desc:
                return True
            # A non-empty, non-crypto type block is the broker's explicit
            # "this is a normal security" verdict — trust it (step 2).
            if code or desc:
                saw_type_block = True
    if saw_type_block:
        return False
    # No broker type info → curated whitelist fallback (step 3).
    # Ambiguous tickers (SNX = TD SYNNEX, SEI = Solaris Energy, …) fail
    # toward Equity. A missing type on those names used to stamp
    # Cryptocurrency and the warehouse then trusted that stamp.
    underlying = _underlying_from_symbol(symbol_obj).upper()
    return symbol_defaults_to_crypto(underlying)


# ---------------------------------------------------------------------------
# Action vocabulary
# ---------------------------------------------------------------------------
# SnapTrade normalizes per-broker activity types into a small canonical
# vocabulary on the /accounts/{id}/activities response. The keys here
# match SnapTrade's canonical type strings; the values are the
# SCHWAB-CSV-EXPORT-style labels that ``stg_history.sql`` already knows
# how to fold into its internal taxonomy (see the
# ``case lower(trim(action)) ...`` block in stg_history.sql).
#
# IMPORTANT: SnapTrade ships equity AND option BUY/SELL events under
# the same canonical types ``BUY`` / ``SELL``. To distinguish open from
# close on options, ``activities_to_history_df`` consults the
# activity's ``description`` text (broker-original wording like
# "BUY TO CLOSE") AND falls back to "to Open" when description is
# ambiguous. See ``_resolve_option_action`` below.
#
# Anything we deliberately drop (cash transfers, contributions, etc.)
# returns ``None`` and is skipped in ``activities_to_history_df``.
# Unknown types log a warning and are also skipped — better to lose a
# row than to ship "Unknown" into the seed where dbt would silently
# bucket it as ``other``.
SNAPTRADE_ACTIVITY_TO_ACTION: dict[str, Optional[str]] = {
    # Equity & options share BUY / SELL — option open/close is then
    # disambiguated from `description` text (see _resolve_option_action).
    "BUY": "Buy",
    "SELL": "Sell",
    # Option lifecycle events stg_history maps to internal
    # option_expired / option_assigned / option_exercised.
    "OPTIONEXPIRATION": "Expired",
    "OPTIONASSIGNMENT": "Assigned",
    "OPTIONEXERCISE": "Exchange or Exercise",
    # Dividends — fold every flavor into the CSV-export "Cash Dividend"
    # label that stg_history maps to internal ``dividend``.
    "DIVIDEND": "Cash Dividend",
    "SUBSTITUTE_DIVIDEND": "Cash Dividend",  # Payment in lieu. Same income bucket.
    "STOCK_DIVIDEND": "Cash Dividend",
    # REI is a dividend that bought more shares. The dividend row stays a
    # Cash Dividend (so a re-sync dedups against rows already stored that
    # way). When units × price matches the cash amount, a companion Buy
    # is added so the new shares are not left to the opening-balance guess.
    "REI": "Cash Dividend",
    # Interest / fees
    "INTEREST": "Credit Interest",
    "FEE": "ADR Mgmt Fee",
    "TAX": "ADR Mgmt Fee",  # Closest CSV-export bucket; refine later if needed.
    # External cash movements. These are NOT trades — they're the trader
    # adding or removing their own money — but they DO move account value,
    # which distorts "how am I doing" on balance-based surfaces (the /wealth
    # curve, /accounts value, Daily Review deltas). We capture the
    # unambiguous ones as a dedicated ``Cash Transfer`` action so those
    # pages can offer an "exclude deposits & withdrawals" toggle. They map
    # to stg_history ``action = 'cash_transfer'`` / ``instrument_type =
    # 'Cash Event'`` and are inert in EVERY trade/P&L model (all of which
    # filter to Equity/Call/Put/dividend). Sign convention below: DEPOSIT /
    # CONTRIBUTION = cash IN (+), WITHDRAWAL = cash OUT (−).
    "DEPOSIT": "Deposit",
    "WITHDRAWAL": "Withdrawal",
    "CONTRIBUTION": "Deposit",   # e.g. IRA / 401k contribution — cash in.
    # Cash moving between the trader's own accounts. Per-account wealth
    # should net these out (they cancel across the All-accounts view).
    # Previously unmapped → logged as unknown and dropped, so the
    # exclude-transfers toggle never saw them.
    "INTERNAL_CASH_TRANSFER_IN": "Deposit",
    "INTERNAL_CASH_TRANSFER_OUT": "Withdrawal",
    # Still DROPPED — ambiguous direction or asset kind, so mislabeling
    # them as cash flow would corrupt the net-deposits number:
    #   TRANSFER            — often SHARE transfers between accounts, not cash.
    #   JOURNAL             — share journals stay dropped; cash-only journals
    #                         (no ticker, non-zero amount) are promoted at
    #                         read time to Deposit/Withdrawal.
    #   DISTRIBUTION        — resolved at read time, not dropped: a ticker
    #                         is income (Cash Dividend); no ticker is a
    #                         cash withdrawal.
    "TRANSFER": None,
    "JOURNAL": None,
    "DISTRIBUTION": None,
    # Stock splits handled out-of-band by current_position_stock_price.py
    # → stg_split_events; SnapTrade's SPLIT activity row would
    # double-count if we wrote it through.
    "SPLIT": None,
    # Backwards-compat alias keys (some older brokers via SnapTrade
    # ship these strings instead of the canonical ones above). Map to
    # the same labels so a broker quirk doesn't drop the activity.
    "STOCKDIVIDEND": "Cash Dividend",
    "QUALIFIEDDIVIDEND": "Qualified Dividend",
    "MARGININTEREST": "Margin Interest",
    "STOCKSPLIT": None,
    # Raw brokerage strings SnapTrade forwards when it does not fold
    # them into BUY/SELL. Cash-settled index closes (SPXW "as of"
    # Buy to Close / Sell to Close, zero commission) arrived this way
    # and were logged as unknown and dropped.
    "BUY TO OPEN": "Buy to Open",
    "BUY TO CLOSE": "Buy to Close",
    "SELL TO OPEN": "Sell to Open",
    "SELL TO CLOSE": "Sell to Close",
    "BUY_TO_OPEN": "Buy to Open",
    "BUY_TO_CLOSE": "Buy to Close",
    "SELL_TO_OPEN": "Sell to Open",
    "SELL_TO_CLOSE": "Sell to Close",
    "EXPIRED": "Expired",
    "ASSIGNED": "Assigned",
    "EXERCISED": "Exchange or Exercise",
    "EXERCISE": "Exchange or Exercise",
    "ASSIGNMENT": "Assigned",
    "EXPIRATION": "Expired",
    "OPTION_EXPIRATION": "Expired",
    "OPTION_ASSIGNMENT": "Assigned",
    "OPTION_EXERCISE": "Exchange or Exercise",
    "EXCHANGE OR EXERCISE": "Exchange or Exercise",
}


# Index-option cash settlement. Not a share exercise. Side comes from
# option_type, the description, or the cash sign.
_CASH_SETTLEMENT_TYPES = frozenset({
    "CASH_SETTLEMENT",
    "CASHSETTLEMENT",
    "OPTIONCASHSETTLEMENT",
    "OPTION_CASH_SETTLEMENT",
    "INDEX_OPTION_SETTLEMENT",
    "INDEXOPTIONSETTLEMENT",
})

_AS_OF_MDY_RE = re.compile(
    r"\bas of\s+(\d{1,2})/(\d{1,2})/(\d{4})\b", re.IGNORECASE,
)
# Provisional order rows with no commission. Activities replace them.
ESTIMATED_FEE_MARK = "est. fee"
_SCHWAB_OPTION_TEXT_RE = re.compile(
    r"\b([A-Z]{1,6})\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+([\d.]+)\s+([CP])\b",
    re.IGNORECASE,
)


# SnapTrade's activity-level ``option_type`` field carries the EXPLICIT
# open/close action (far more reliable than parsing the broker description).
SNAPTRADE_OPTION_TYPE_TO_ACTION: dict[str, str] = {
    "BUY_TO_OPEN": "Buy to Open",
    "BUY_TO_CLOSE": "Buy to Close",
    "SELL_TO_OPEN": "Sell to Open",
    "SELL_TO_CLOSE": "Sell to Close",
}


# SnapTrade's ``recent_orders`` record-level ``action`` field ALSO carries the
# explicit open/close for options on brokers that support it. The value shape is
# broker-dependent: Schwab-via-SnapTrade ships the underscore form
# (``BUY_OPEN`` / ``SELL_CLOSE`` / …, verified 2026-07-08 against a live Cameron
# Investment condor + FBK STO + PENG BTC); the strict ``ActionStrictWithOptions``
# enum documents the ``_TO_`` form. Accept BOTH so the orders fast-path can carry
# options with ZERO guessing. A broker that ships only bare ``BUY`` / ``SELL``
# for an option (e.g. Alpaca paper) is NOT in this map, so it correctly falls
# through to "defer to the activities feed" rather than guessing open vs close.
SNAPTRADE_ORDER_ACTION_TO_OPTION_ACTION: dict[str, str] = {
    "BUY_OPEN": "Buy to Open",
    "BUY_TO_OPEN": "Buy to Open",
    "BUY_CLOSE": "Buy to Close",
    "BUY_TO_CLOSE": "Buy to Close",
    "SELL_OPEN": "Sell to Open",
    "SELL_TO_OPEN": "Sell to Open",
    "SELL_CLOSE": "Sell to Close",
    "SELL_TO_CLOSE": "Sell to Close",
}


def _resolve_option_action(canonical: str, description: str, option_type: str = "") -> str:
    """Pick the open/close-aware option action.

    SnapTrade collapses every option open/close into BUY/SELL at the
    activity ``type`` level, but ships the EXPLICIT action in the
    activity-level ``option_type`` field (``BUY_TO_OPEN`` /
    ``SELL_TO_CLOSE`` / ...). Prefer that — it's authoritative. Only fall
    back to parsing the broker description when ``option_type`` is empty.

    Why this matters: Schwab option descriptions via SnapTrade are just
    ``"CALL ORACLE CORP $200 EXP 06/18/26"`` with NO open/close hint, so
    description parsing defaulted every closing leg to OPEN. A
    buy-to-open + sell-to-close round trip then never closed: the contract
    stayed Open with phantom premium (ORCL 2026-06: STC 30/50 contracts
    read as STO → a "Naked Call / Open / $126K unrealized" position that
    was actually a fully-closed +$126K realized round trip). See
    broker-sync-safety.
    """
    explicit = SNAPTRADE_OPTION_TYPE_TO_ACTION.get(
        str(option_type or "").strip().upper()
    )
    if explicit:
        return explicit
    desc = (description or "").lower()
    if "to close" in desc or "to_close" in desc or "tc" in desc.split():
        return "Buy to Close" if canonical == "Buy" else "Sell to Close"
    if "to open" in desc or "to_open" in desc or "to" in desc and "open" in desc:
        return "Buy to Open" if canonical == "Buy" else "Sell to Open"
    # Default: open. Documented as a known limitation; revisit when
    # we see a broker whose description never disambiguates.
    return "Buy to Open" if canonical == "Buy" else "Sell to Open"


def _is_finite_number(value) -> bool:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f)


def _safe_float(value, default=0.0) -> float:
    try:
        f = float(value)
        if math.isfinite(f):
            return f
    except (TypeError, ValueError):
        pass
    return default


def _number_missing(value) -> bool:
    """True when a SnapTrade numeric field was omitted or blank.

    An explicit 0 is present. Mutual-fund NAVs often arrive as null
    rather than zero; treating null as zero books the whole cost basis
    as a loss.
    """
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return not _is_finite_number(value)


def _coerce_symbol_obj(symbol) -> dict:
    """SnapTrade documents ``symbol`` / ``universal_symbol`` as an object or null.

    Vanguard-shaped payloads can send the ticker as a bare string. A
    string has no ``.get``; coercing it here keeps one account's sync
    from raising, and keeps the ticker.
    """
    if isinstance(symbol, str):
        ticker = symbol.strip()
        if not ticker:
            return {}
        return {"raw_symbol": ticker, "symbol": ticker, "description": ticker}
    if isinstance(symbol, Mapping):
        return dict(symbol)
    return {}


def _promote_option_text(symbol_obj: Mapping, original) -> dict:
    """A Schwab long-form contract sent as a string is an option, not a ticker.

    ``SPXW 10/01/2026 7650.00 C`` must become an OCC symbol so fee netting
    and dedup still see the same contract. A plain fund ticker does not
    match and stays an equity symbol.
    """
    base = dict(symbol_obj) if isinstance(symbol_obj, Mapping) else {}
    if base.get("option_symbol"):
        return base
    text = original if isinstance(original, str) else str(base.get("raw_symbol") or "")
    osi = _osi_from_broker_text(text)
    if not osi:
        return base
    parsed = _option_symbol_from_text(osi)
    if not parsed:
        return base
    base["option_symbol"] = parsed
    base["raw_symbol"] = osi
    return base


def _attach_option_symbol(symbol_obj: Mapping, option_symbol) -> dict:
    """Fold a top-level option contract into the symbol object.

    Activities and positions sometimes leave ``symbol`` null and put the
    contract on ``option_symbol``. A string there is OCC text, not a
    mapping — parse it instead of calling ``.get`` on the string.
    """
    base = dict(symbol_obj) if isinstance(symbol_obj, Mapping) else {}
    if base.get("option_symbol"):
        return base
    if isinstance(option_symbol, str):
        parsed = _option_symbol_from_text(option_symbol)
        if parsed:
            base["option_symbol"] = parsed
        return base
    if isinstance(option_symbol, Mapping):
        base["option_symbol"] = option_symbol
    return base


def _position_quantity(pos: Mapping) -> float:
    """Share count from ``units`` only.

    SnapTrade's ``fractional_units`` is deprecated. A remainder in that
    field is ignored, including when ``units`` is null. The same number
    in both fields is stored once because only ``units`` is read.
    """
    units_raw = pos.get("units")
    if not _is_finite_number(units_raw):
        return 0.0
    return _safe_float(units_raw, 0.0)


def _is_cash_equivalent(pos: Mapping) -> bool:
    """True when SnapTrade's flag says this holding is already in cash.

    Money-market sweeps set ``cash_equivalent``. An unflagged money-market
    fund is decided separately: it is cash only when the broker cash
    balance already includes it (see ``symbols_already_in_cash``).
    """
    value = pos.get("cash_equivalent")
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value == 1
    return str(value or "").strip().lower() in {"true", "1", "yes"}


_MONEY_MARKET_TYPE_CODES = frozenset({
    "MMF", "MM", "MONEYMARKET", "MONEY_MARKET", "CASH_EQUIVALENT",
})


def _position_symbol_obj(pos: Mapping) -> dict:
    return _promote_option_text(
        _attach_option_symbol(
            _coerce_symbol_obj(pos.get("symbol")),
            pos.get("option_symbol"),
        ),
        pos.get("symbol"),
    )


def _activity_ticker(act: Mapping) -> str:
    """Ticker or OCC symbol on an activity. Blank when the row is cash."""
    symbol_obj = _promote_option_text(
        _attach_option_symbol(
            _coerce_symbol_obj(act.get("symbol")),
            act.get("option_symbol"),
        ),
        act.get("symbol"),
    )
    if _is_option(symbol_obj):
        return snaptrade_symbol_to_osi(symbol_obj).strip().upper()
    return _underlying_from_symbol(symbol_obj).strip().upper()


def _equity_ticker(symbol_obj: Mapping) -> str:
    if _is_option(symbol_obj):
        return ""
    return _underlying_from_symbol(symbol_obj).strip().upper()


def _type_code_and_description(symbol_obj: Mapping) -> tuple[str, str]:
    codes: list[str] = []
    descs: list[str] = []
    for candidate in (
        symbol_obj.get("symbol") if isinstance(symbol_obj.get("symbol"), Mapping) else None,
        symbol_obj,
    ):
        if not isinstance(candidate, Mapping):
            continue
        type_block = candidate.get("type")
        if isinstance(type_block, Mapping):
            codes.append(str(type_block.get("code") or "").strip().upper())
            descs.append(str(type_block.get("description") or "").strip().upper())
        elif isinstance(type_block, str) and type_block.strip():
            codes.append(type_block.strip().upper())
    return " ".join(codes), " ".join(descs)


def _is_money_market_fund(pos: Mapping, symbol_obj: Mapping) -> bool:
    """A settlement / money-market fund, not a market-priced mutual fund.

    The type block wins when SnapTrade names it. Otherwise a stable $1
    NAV (price and, when present, average cost within two cents of $1)
    nominates the position. A fund at a real market price does not.
    Nomination is not enough to drop the holding — cash has to already
    include the dollars.
    """
    if _is_option(symbol_obj) or _is_crypto(symbol_obj):
        return False
    code, desc = _type_code_and_description(symbol_obj)
    compact = re.sub(r"[\s_\-]", "", code)
    if compact in {re.sub(r"[\s_\-]", "", item) for item in _MONEY_MARKET_TYPE_CODES}:
        return True
    if "MONEYMARKET" in compact or "MONEY MARKET" in desc:
        return True
    price = pos.get("price")
    if _number_missing(price) or abs(_safe_float(price, 0.0) - 1.0) > 0.02:
        return False
    avg = pos.get("average_purchase_price")
    if not _number_missing(avg) and abs(_safe_float(avg, 0.0) - 1.0) > 0.02:
        return False
    return True


def _quoted_market_value(pos: Mapping, symbol_obj: Mapping) -> float:
    raw_mv = pos.get("market_value")
    if _number_missing(raw_mv):
        raw_mv = pos.get("equity")
    if not _number_missing(raw_mv):
        return _safe_float(raw_mv, 0.0)
    price = pos.get("price")
    if _number_missing(price):
        return 0.0
    units = _position_quantity(pos)
    mult = 100.0 if _is_option(symbol_obj) else 1.0
    return units * _safe_float(price, 0.0) * mult


def _reported_cash_and_total(balances, account_summary) -> tuple[float, float | None]:
    """Broker cash, and the account total when the summary actually sent one.

    A missing total is None, not zero. An explicit zero stays zero.
    """
    cash = 0.0
    if isinstance(balances, Iterable) and not isinstance(balances, (str, bytes, Mapping)):
        for bal in balances:
            if isinstance(bal, Mapping):
                cash += _safe_float(bal.get("cash"), 0.0)
    if not cash and isinstance(account_summary, Mapping):
        balance_block = account_summary.get("balance")
        nested = balance_block.get("cash") if isinstance(balance_block, Mapping) else None
        cash = _safe_float(nested, _safe_float(account_summary.get("cash"), 0.0))

    if not isinstance(account_summary, Mapping):
        return cash, None
    balance_block = account_summary.get("balance")
    if not isinstance(balance_block, Mapping):
        return cash, None
    total_block = balance_block.get("total")
    if isinstance(total_block, Mapping) and _is_finite_number(total_block.get("amount")):
        return cash, _safe_float(total_block.get("amount"), 0.0)
    if _is_finite_number(total_block):
        return cash, _safe_float(total_block, 0.0)
    return cash, None


def _sweep_symbol_set(symbols: Iterable[str] | None) -> set[str]:
    return {
        str(symbol).strip().upper()
        for symbol in (symbols or ())
        if str(symbol).strip()
    }


def cash_equivalent_symbols(positions: Iterable[Mapping] | None) -> set[str]:
    """Tickers whose SnapTrade position has ``cash_equivalent`` true."""
    symbols: set[str] = set()
    for pos in positions or ():
        if not isinstance(pos, Mapping) or not _is_cash_equivalent(pos):
            continue
        ticker = _equity_ticker(_position_symbol_obj(pos))
        if ticker:
            symbols.add(ticker)
    return symbols


def symbols_already_in_cash(
    positions: Iterable[Mapping] | None,
    *,
    balances=None,
    account_summary=None,
) -> set[str]:
    """Tickers that must not also be equity holdings.

    A ``cash_equivalent`` flag is always in this set. An unflagged
    money-market fund joins it only when cash plus the other holdings
    already match the reported account total — the fund's dollars are
    inside cash, and adding the position would count them twice.
    When the total is missing, or the total only matches if the fund is
    included, the fund stays a holding.
    """
    flagged: set[str] = set()
    candidates: list[tuple[str, float]] = []
    other_mv = 0.0
    for pos in positions or ():
        if not isinstance(pos, Mapping):
            continue
        symbol_obj = _position_symbol_obj(pos)
        ticker = _equity_ticker(symbol_obj)
        mv = _quoted_market_value(pos, symbol_obj)
        if _is_cash_equivalent(pos):
            if ticker:
                flagged.add(ticker)
            continue
        if ticker and _is_money_market_fund(pos, symbol_obj):
            candidates.append((ticker, mv))
            continue
        other_mv += mv

    if not candidates:
        return flagged
    cash, total = _reported_cash_and_total(balances, account_summary)
    if total is None:
        return flagged
    fund_mv = sum(mv for _ticker, mv in candidates)
    without_fund = cash + other_mv
    with_fund = without_fund + fund_mv
    tolerance = max(1.0, 0.002 * abs(total))
    already_inside = (
        abs(without_fund - total) <= tolerance
        and abs(with_fund - total) > tolerance
    )
    if not already_inside:
        return flagged
    return flagged | {ticker for ticker, _mv in candidates if ticker}


def _reinvestment_is_share_purchase(units: float, price: float, amount: float) -> bool:
    """True when a REI/stock-dividend row's units are shares bought.

    SnapTrade's ``units`` on a plain DIVIDEND is often the shares that
    received the cash, not shares purchased. A reinvestment agrees with
    the cash: units × price ≈ |amount|. A share-balance in ``units``
    (price × a large lot, tiny cash) does not, and must not become a buy.
    """
    if units == 0 or price <= 0 or amount == 0:
        return False
    gross = abs(units * price)
    amt = abs(amount)
    return abs(gross - amt) <= max(1.0, 0.02 * amt)


def _same_day_buy_already_present(
    activity_list: Sequence[Mapping],
    sym_str: str,
    trade_date: str,
    units: float,
) -> bool:
    """True when this batch already has a Buy of the same shares.

    Some brokers emit REI and a separate BUY for one reinvestment.
    Adding a companion Buy on top of that BUY would count the shares
    twice. Match on symbol, date, and quantity; price can differ by
    a rounding cent and still be the same lot.
    """
    target_qty = abs(units)
    if not sym_str or not trade_date or target_qty == 0:
        return False
    for other in activity_list:
        if not isinstance(other, Mapping):
            continue
        if str(other.get("type") or "").strip().upper() != "BUY":
            continue
        other_obj = _attach_option_symbol(
            _coerce_symbol_obj(other.get("symbol")),
            other.get("option_symbol"),
        )
        if _is_option(other_obj):
            continue
        if _underlying_from_symbol(other_obj) != sym_str:
            continue
        other_date = _format_date_mdy(
            other.get("trade_date")
            or other.get("settlement_date")
            or other.get("date")
        )
        if other_date != trade_date:
            continue
        other_units = abs(_safe_float(other.get("units"), 0.0))
        if abs(other_units - target_qty) <= max(1e-4, 0.001 * target_qty):
            return True
    return False


def _is_cash_only_journal(act: Mapping) -> bool:
    """Schwab-via-SnapTrade JOURNAL of money, not shares.

    Cash journals have a non-zero amount, no ticker, and no share qty
    (Emmory CSV ``JOURNAL FRM …852`` / $500). Share journals keep a
    symbol and units — those stay dropped so they cannot inflate
    net-deposits.
    """
    if act.get("option_symbol"):
        return False
    symbol_obj = act.get("symbol") if isinstance(act.get("symbol"), Mapping) else {}
    ticker = _underlying_from_symbol(symbol_obj) if symbol_obj else ""
    if str(ticker or "").strip():
        return False
    units = abs(_safe_float(act.get("units"), 0.0))
    amount = abs(_safe_float(act.get("amount"), 0.0))
    return units < 1e-9 and amount > 0.005


def _format_date_mdy(value) -> str:
    """Coerce SnapTrade's various date/datetime shapes into ``MM/DD/YYYY``.

    stg_history parses dates with ``parse_date('%m/%d/%Y', regexp_extract(date,
    r'(\\d{1,2}/\\d{1,2}/\\d{4})'))`` (MDY token anywhere in the cell, matching
    ``_canonicalize_date_mdy``). We emit the bare ``MM/DD/YYYY`` form.
    """
    if value is None or value == "":
        return ""
    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y")
    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")
    s = str(value).strip()
    if not s:
        return ""
    as_of = _as_of_trade_date(s)
    if as_of:
        return as_of
    # ISO 8601 ("2026-05-11" or "2026-05-11T14:30:00Z")
    iso_head = s.split("T", 1)[0].split(" ", 1)[0]
    try:
        d = datetime.strptime(iso_head, "%Y-%m-%d").date()
        return d.strftime("%m/%d/%Y")
    except ValueError:
        pass
    # Already MM/DD/YYYY
    try:
        d = datetime.strptime(iso_head, "%m/%d/%Y").date()
        return d.strftime("%m/%d/%Y")
    except ValueError:
        pass
    return s  # last resort — let downstream see it and fail loudly


# ---------------------------------------------------------------------------
# Option symbol formatting
# ---------------------------------------------------------------------------

def _as_of_trade_date(value) -> str:
    """Trade date from ``10/02/2026 as of 10/01/2026``.

    The first date is when Schwab posted the cash settlement. The date
    after "as of" is the expiry / trade date the fill belongs to.
    A clock suffix (``as of 08:30 PM``) returns empty.
    """
    if value is None:
        return ""
    match = _AS_OF_MDY_RE.search(str(value))
    if not match:
        return ""
    month, day, year = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    try:
        return date(year, month, day).strftime("%m/%d/%Y")
    except ValueError:
        return ""


def _mdy_date(value):
    text = str(value or "").strip()
    if not text:
        return None
    head = text.split("T", 1)[0].split(" ", 1)[0]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(head, fmt).date()
        except ValueError:
            continue
    return None


def _activity_fee(act: Mapping) -> float:
    """Commission on one activity.

    SnapTrade's documented field is ``fee``. Schwab option opens have
    arrived with that field 0 or null while the commission sits on
    ``commission`` / ``fees``, or on a separate ``FEE`` activity that
    shares ``external_reference_id`` (attached by the caller).
    """
    if not isinstance(act, Mapping):
        return 0.0
    for key in ("fee", "fees", "commission"):
        raw = act.get(key)
        if isinstance(raw, Mapping):
            raw = raw.get("amount") or raw.get("fee") or raw.get("commission")
        fee = _safe_float(raw, 0.0)
        if fee:
            return abs(fee)
    return 0.0


def _linked_fee_amounts(activities: Iterable[Mapping]) -> dict[str, float]:
    """FEE activities keyed by the fill they belong to.

    A commission booked as its own activity (type FEE, same
    ``external_reference_id`` as the option fill) is not a standalone
    ADR fee. The fill nets it once and the FEE row is not emitted again.
    """
    totals: dict[str, float] = {}
    for act in activities or ():
        if not isinstance(act, Mapping):
            continue
        if str(act.get("type") or "").strip().upper() != "FEE":
            continue
        ref = str(act.get("external_reference_id") or "").strip()
        if not ref:
            continue
        amount = abs(_safe_float(act.get("amount"), 0.0)) or _activity_fee(act)
        if amount:
            totals[ref] = totals.get(ref, 0.0) + amount
    return totals


def _net_option_cash(amount_signed: float, units: float, price: float, fees: float) -> float:
    """Net a gross option premium by the commission once.

    Statement Amount is already net (STO 10 × 7.77 × 100 − 12.22 =
    7,757.78). An order row is the gross (7,770) with the fee beside
    it. If ``|amount|`` already matches ``qty × price × 100`` minus the
    fee, leave it — subtracting again double-charges the Sep legs.
    """
    if not fees or not units or not price:
        return amount_signed
    gross = abs(units) * abs(price) * 100.0
    if abs(abs(amount_signed) - gross) > 0.05:
        return amount_signed
    fee = abs(fees)
    if amount_signed < 0:
        return round(-(gross + fee), 2)
    return round(gross - fee, 2)


def _osi_from_broker_text(text: str) -> str:
    """OCC symbol from a Schwab symbol cell or a description.

    ``SPXW 10/01/2026 7650.00 C`` and a bare OSI tail both work.
    Cash-settled index activities sometimes ship the contract only as
    text (``option_symbol`` null because the root is not a stock).
    """
    if not text or not isinstance(text, str):
        return ""
    raw = text.strip().upper()
    compact = re.sub(r"\s+", "", raw)
    occ = re.search(r"([A-Z]{1,6})(\d{6}[CP]\d{8})", compact)
    if occ:
        return f"{occ.group(1)[:6]:<6}{occ.group(2)}"
    match = _SCHWAB_OPTION_TEXT_RE.search(raw)
    if not match:
        return ""
    root, expiry_s, strike_s, cp = match.groups()
    expiry_s = expiry_s.strip()
    exp_dt = None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            exp_dt = datetime.strptime(expiry_s, fmt).date()
            break
        except ValueError:
            continue
    if exp_dt is None:
        return ""
    try:
        strike_thou = int(round(float(strike_s) * 1000))
    except (TypeError, ValueError):
        return ""
    return f"{root[:6]:<6}{exp_dt.strftime('%y%m%d')}{cp.upper()}{strike_thou:08d}"


def _resolve_cash_settlement_action(description: str, option_type: str, amount: float) -> str:
    """Side of an index cash settlement that is not a share exercise."""
    explicit = SNAPTRADE_OPTION_TYPE_TO_ACTION.get(str(option_type or "").strip().upper())
    if explicit:
        return explicit
    desc = (description or "").lower()
    if "buy to close" in desc or "bought to close" in desc:
        return "Buy to Close"
    if "sell to close" in desc or "sold to close" in desc:
        return "Sell to Close"
    if "buy to open" in desc or "bought to open" in desc:
        return "Buy to Open"
    if "sell to open" in desc or "sold to open" in desc:
        return "Sell to Open"
    if "expir" in desc:
        return "Expired"
    if "assign" in desc:
        return "Assigned"
    if "exercis" in desc:
        return "Exchange or Exercise"
    if abs(amount) < 0.005:
        return "Expired"
    return "Buy to Close" if amount < 0 else "Sell to Close"


def snaptrade_symbol_to_osi(symbol_obj: Mapping) -> str:
    """Turn a SnapTrade option ``symbol`` object into the compact OSI
    string ``"AAPL  240119C00150000"`` that stg_history / stg_current
    parse.

    SnapTrade returns option metadata as structured fields rather than
    a pre-formatted symbol. We assemble the standard 21-character OCC
    OSI form: ``UNDERLYING(6, space-padded right)`` + ``YYMMDD`` +
    ``C|P`` + ``STRIKE * 1000 (8, zero-padded left)``.

    For non-option symbols, returns the raw ``symbol`` ticker so the
    same callsite can handle equities and options uniformly.
    """
    if not isinstance(symbol_obj, Mapping):
        return ""

    # SnapTrade nests the brokerage symbol under ``symbol`` (which itself
    # has ``raw_symbol``, ``symbol``, ``description``, ...). Some payloads
    # flatten it. Try both.
    inner = symbol_obj.get("symbol") if isinstance(symbol_obj.get("symbol"), Mapping) else symbol_obj
    raw = (
        inner.get("raw_symbol")
        or inner.get("symbol")
        or symbol_obj.get("raw_symbol")
        or symbol_obj.get("ticker")
        or ""
    )
    raw = str(raw).strip().upper()

    option_symbol = symbol_obj.get("option_symbol")
    if not isinstance(option_symbol, Mapping):
        return raw

    # SnapTrade ships ``underlying_symbol`` as a nested object
    # (``{"symbol": "PLTR", "raw_symbol": "PLTR", ...}``), not a bare
    # string. Pull the ticker out before falling back to the OSI ticker
    # or the brokerage raw symbol; ``str(<dict>)`` here used to yield
    # garbage like ``"{'SYMBOL': 'PLTR'..."`` and corrupt the OSI string.
    underlying_raw = option_symbol.get("underlying_symbol")
    if isinstance(underlying_raw, Mapping):
        underlying_raw = (
            underlying_raw.get("raw_symbol")
            or underlying_raw.get("symbol")
            or ""
        )
    underlying = (
        underlying_raw
        or option_symbol.get("ticker")
        or raw
        or ""
    )
    underlying = str(underlying).strip().upper()
    expiry = option_symbol.get("expiration_date") or ""
    strike = option_symbol.get("strike_price")
    option_type = (option_symbol.get("option_type") or "").strip().upper()

    if not underlying or not expiry or strike is None or option_type not in ("CALL", "PUT", "C", "P"):
        return raw or ""

    cp = "C" if option_type.startswith("C") else "P"

    expiry_str = str(expiry).split("T", 1)[0]
    try:
        exp_dt = datetime.strptime(expiry_str, "%Y-%m-%d").date()
    except ValueError:
        try:
            exp_dt = datetime.strptime(expiry_str, "%m/%d/%Y").date()
        except ValueError:
            return raw

    yymmdd = exp_dt.strftime("%y%m%d")
    try:
        strike_thou = int(round(float(strike) * 1000))
    except (TypeError, ValueError):
        return raw
    strike_str = f"{strike_thou:08d}"

    # 6-char left-aligned underlying so the total length is the canonical
    # 21 characters. stg_history.sql's regex is anchored on the OSI tail
    # (``r'(\\d{6}[CP]\\d{8})'``) so leading spaces are tolerated.
    return f"{underlying[:6]:<6}{yymmdd}{cp}{strike_str}"


def _is_option(symbol_obj: Mapping) -> bool:
    if not isinstance(symbol_obj, Mapping):
        return False
    if isinstance(symbol_obj.get("option_symbol"), Mapping):
        return True
    inner = symbol_obj.get("symbol")
    if isinstance(inner, Mapping):
        if isinstance(inner.get("option_symbol"), Mapping):
            return True
    return False


def _crypto_pair_base(symbol: str) -> str:
    """BTC-USD / BTCUSD / BTC/USD → BTC when the base is a known crypto.

    Coinbase activities often carry the pair while the position snapshot
    is the bare ticker, so history never joined the holding (0 trades,
    no review, no raw log). USDC itself is a base and is left alone.
    Option OSI symbols (spaces, or longer than a pair) are not rewritten.
    """
    return canonicalize_crypto_pair_symbol(symbol)


def _underlying_from_symbol(symbol_obj: Mapping) -> str:
    if not isinstance(symbol_obj, Mapping):
        return ""
    option_symbol = symbol_obj.get("option_symbol")
    if isinstance(option_symbol, Mapping):
        u = option_symbol.get("underlying_symbol")
        if isinstance(u, Mapping):
            u = u.get("raw_symbol") or u.get("symbol") or ""
        u = u or option_symbol.get("ticker") or ""
        if u:
            return _crypto_pair_base(str(u).strip().upper())
    inner = symbol_obj.get("symbol") if isinstance(symbol_obj.get("symbol"), Mapping) else symbol_obj
    raw = (
        inner.get("raw_symbol")
        or inner.get("symbol")
        or symbol_obj.get("raw_symbol")
        or symbol_obj.get("ticker")
        or ""
    )
    return _crypto_pair_base(str(raw).strip().upper())


def _description_from_symbol(symbol_obj: Mapping) -> str:
    if not isinstance(symbol_obj, Mapping):
        return ""
    inner = symbol_obj.get("symbol") if isinstance(symbol_obj.get("symbol"), Mapping) else symbol_obj
    return str(inner.get("description") or symbol_obj.get("description") or "").strip()


# ---------------------------------------------------------------------------
# Activities → trade_history rows
# ---------------------------------------------------------------------------

def activities_to_history_df(
    activities: Iterable[Mapping],
    *,
    account_name: str,
    user_id,
    tenant_id: str,
    cash_equivalent_symbols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Build a HISTORY_SEED_COLUMNS DataFrame from a list of SnapTrade
    activity objects.

    ``user_id`` and ``tenant_id`` are stamped on every emitted row —
    this is the broker-sync-safety invariant that ``merge_and_push_seeds``
    enforces at the merge boundary.

    Sign convention: dollar amounts emitted as **negative for cash out
    (buys, fees) and positive for cash in (sells, dividends, interest)**
    so stg_history's ``amount_signed`` CTE doesn't have to re-sign them.
    Quantity is the unsigned magnitude — stg_history doesn't read sign
    from quantity.
    """
    rows: list[dict] = []
    user_id_int = _seed_user_id(user_id)
    tenant_id_str = str(tenant_id).strip()
    sweep_symbols = _sweep_symbol_set(cash_equivalent_symbols)
    activity_list = [act for act in (activities or ()) if isinstance(act, Mapping)]
    linked_fees = _linked_fee_amounts(activity_list)

    for act in activity_list:
        if not isinstance(act, Mapping):
            continue
        atype = str(act.get("type") or "").strip().upper()
        if not atype:
            continue
        broker_description = str(act.get("description") or "").strip()
        option_type = act.get("option_type")
        amount_preview = _safe_float(act.get("amount"), 0.0)
        action_label = SNAPTRADE_ACTIVITY_TO_ACTION.get(atype, "__UNKNOWN__")
        if action_label is None and atype == "JOURNAL" and _is_cash_only_journal(act):
            amt = _safe_float(act.get("amount"), 0.0)
            action_label = "Deposit" if amt >= 0 else "Withdrawal"
        if action_label is None and atype == "DISTRIBUTION":
            # A ticker is fund income. No ticker is cash leaving the account.
            action_label = "Cash Dividend" if _activity_ticker(act) else "Withdrawal"
        if atype in _CASH_SETTLEMENT_TYPES or action_label == "__UNKNOWN__":
            settled = _resolve_cash_settlement_action(
                broker_description, option_type, amount_preview,
            )
            # Unknown types with no option clue stay dropped. A cash
            # settlement, or a raw option verb in the description /
            # option_type, is a real fill (SPXW as-of closes).
            has_option_clue = bool(option_type) or bool(
                _osi_from_broker_text(broker_description)
            ) or atype in _CASH_SETTLEMENT_TYPES
            if action_label == "__UNKNOWN__" and (
                atype in _CASH_SETTLEMENT_TYPES or has_option_clue
            ):
                action_label = settled
            elif action_label == "__UNKNOWN__":
                _log.warning(
                    "snaptrade_normalize: dropping unknown activity type %r",
                    atype,
                )
                continue
        if action_label is None:
            continue

        # SnapTrade (Schwab via SnapTrade) ships OPTION activities with
        # ``symbol: null`` and the structured contract at the ACTIVITY top
        # level (``act["option_symbol"]``), NOT nested under ``symbol``.
        # Without folding it in, ``_is_option`` sees an empty dict, every
        # option is misclassified as equity with an empty Symbol, and
        # stg_history can't recognize it as an option — the entire option
        # lane silently vanished from the warehouse (hundreds of contracts
        # per account). See broker-sync-safety "Bugs we've shipped".
        # A bare-string ticker (Vanguard-shaped) is coerced before that
        # fold so ``.get`` is never called on a str.
        symbol_obj = _promote_option_text(
            _attach_option_symbol(
                _coerce_symbol_obj(act.get("symbol")),
                act.get("option_symbol"),
            ),
            act.get("symbol"),
        )
        is_option = _is_option(symbol_obj)
        if is_option:
            sym_str = snaptrade_symbol_to_osi(symbol_obj)
        else:
            sym_str = _underlying_from_symbol(symbol_obj)
        # Description: prefer the activity-level description (broker-
        # original wording — usually carries "Buy to Close" / etc. for
        # options) over the symbol-level description (just a name).
        description = broker_description or _description_from_symbol(symbol_obj) or sym_str
        if not sym_str:
            sym_str = _osi_from_broker_text(description) or _osi_from_broker_text(
                str(act.get("symbol") or "")
            )
            if sym_str:
                is_option = True

        # Disambiguate option open/close from broker description for
        # BUY/SELL (SnapTrade collapses both into the same canonical
        # type; see _resolve_option_action).
        if is_option and action_label in ("Buy", "Sell"):
            action_label = _resolve_option_action(
                action_label, broker_description, act.get("option_type")
            )

        trade_date = _format_date_mdy(
            act.get("trade_date")
            or act.get("settlement_date")
            or act.get("date")
        )
        # Posted date (settlement) vs trade date. "10/02/2026 as of
        # 10/01/2026" belongs on 10/01 even when trade_date is the post.
        as_of = (
            _as_of_trade_date(act.get("trade_date"))
            or _as_of_trade_date(act.get("date"))
            or _as_of_trade_date(broker_description)
        )
        if as_of:
            trade_date = as_of
        # Do not rewrite Date or Description from the contract expiry.
        # Rows already in the seed are keyed on the broker's posting
        # date. Moving them to the expiry (and appending "as of") makes
        # the next sync insert a twin. stg_history caps the warehouse
        # trade_date at expiry; pages read that column.
        units = _safe_float(act.get("units"), 0.0)
        price = _safe_float(act.get("price"), 0.0)
        fees = _activity_fee(act)
        fee_ref = str(act.get("external_reference_id") or "").strip()
        if not fees and fee_ref and fee_ref in linked_fees:
            fees = linked_fees[fee_ref]
        # A FEE row that was folded into its option fill is not also an
        # ADR management fee.
        if (
            atype == "FEE"
            and fee_ref
            and fee_ref in linked_fees
            and action_label == "ADR Mgmt Fee"
        ):
            continue
        amount = act.get("amount")
        if amount is None or not _is_finite_number(amount):
            # Fallback: derive from units * price; use sign convention
            # below to set direction.
            amount = units * price

        amount_f = _safe_float(amount, 0.0)
        # Direction matrix — explicit so we never trust the broker's
        # signed amount (different brokers via SnapTrade have shipped
        # contradictory conventions; stg_history will re-sign anyway,
        # but emitting the canonical sign keeps the seed data readable
        # and prevents dedup drift from sign-flips between syncs).
        cash_out_actions = {
            "Buy", "Buy to Open", "Buy to Close",
            "Margin Interest", "ADR Mgmt Fee",
            "Withdrawal",
        }
        cash_in_actions = {
            "Sell", "Sell to Open", "Sell to Close",
            "Cash Dividend", "Qualified Dividend", "Credit Interest",
            "Deposit",
        }
        if action_label in cash_out_actions:
            amount_signed = -abs(amount_f)
        elif action_label in cash_in_actions:
            amount_signed = abs(amount_f)
        else:
            amount_signed = amount_f
        if is_option and action_label not in ("Expired", "Assigned", "Exchange or Exercise"):
            amount_signed = _net_option_cash(amount_signed, units, price, fees)

        # A fund already inside the cash balance is not an equity lot.
        # Dividends and interest on it stay. They are income, not shares.
        if (
            not is_option
            and action_label in {"Buy", "Sell", "Sell Short"}
            and sym_str.strip().upper() in sweep_symbols
        ):
            continue

        rows.append({
            "Account": account_name,
            "user_id": user_id_int,
            "tenant_id": tenant_id_str,
            "Date": trade_date,
            "Action": action_label,
            "Symbol": sym_str,
            "Description": description,
            "Quantity": abs(units) if units else "",
            "Price": price if price else "",
            "fees_and_comm": fees if fees else "",
            "Amount": amount_signed,
        })
        # Companion share purchase for a reinvestment. The Cash Dividend
        # row above is unchanged so it dedups with rows already stored
        # from before this split. The Buy is a different Action, so it
        # does not collapse into the dividend. Fees stay on the dividend
        # row only.
        if (
            atype in {"REI", "STOCK_DIVIDEND"}
            and not is_option
            and sym_str
            and _reinvestment_is_share_purchase(units, price, amount_f)
            and sym_str.strip().upper() not in sweep_symbols
            and not _same_day_buy_already_present(
                activity_list, sym_str, trade_date, units,
            )
        ):
            rows.append({
                "Account": account_name,
                "user_id": user_id_int,
                "tenant_id": tenant_id_str,
                "Date": trade_date,
                "Action": "Buy",
                "Symbol": sym_str,
                "Description": description,
                "Quantity": abs(units),
                "Price": price,
                "fees_and_comm": "",
                "Amount": -abs(amount_f),
            })

    df = pd.DataFrame(rows, columns=HISTORY_SEED_COLUMNS)
    return df


# ---------------------------------------------------------------------------
# Orders → history rows (real-time fallback for activity-feed lag)
# ---------------------------------------------------------------------------


def _option_symbol_from_text(symbol: str):
    """OCC or long-form contract text → the nested shape ``snaptrade_symbol_to_osi`` reads."""
    from app.option_formatting import parse_occ
    parsed = parse_occ(symbol)
    if not parsed:
        return None
    try:
        year = 2000 + int(parsed["yy"])
        expiry = f"{year:04d}-{int(parsed['mm']):02d}-{int(parsed['dd']):02d}"
    except (TypeError, ValueError, KeyError):
        return None
    cp = str(parsed.get("cp") or "").upper()
    return {
        "underlying_symbol": {
            "symbol": parsed.get("root") or "",
            "raw_symbol": parsed.get("root") or "",
        },
        "expiration_date": expiry,
        "strike_price": parsed.get("strike"),
        "option_type": "CALL" if cp == "C" else "PUT",
    }


def _split_leg_fee(order: Mapping, leg: Mapping):
    """A parent multi-leg fee belongs once, split across the legs.

    Copying ``order.fee`` onto every leg charges the spread twice.
    A leg that carries its own fee keeps that number, including zero.
    """
    own = leg.get("fee")
    if own not in (None, ""):
        return own
    parent = order.get("fee")
    if parent in (None, ""):
        parent = order.get("fees")
    if parent in (None, ""):
        parent = order.get("commission")
    if parent in (None, ""):
        return None
    parent_f = abs(_safe_float(parent, 0.0))
    if not parent_f:
        return None
    legs = order.get("legs") or []
    n = len(legs) if isinstance(legs, (list, tuple)) and legs else 1
    return round(parent_f / n, 2)


_NO_ESTIMATE_ACTIONS = frozenset({
    "expired", "assigned", "exchange or exercise", "exercised",
    "option_expired", "option_assigned", "option_exercised",
})


def _option_expiry_date(symbol, option_symbol=None):
    """Calendar expiry from a SnapTrade option dict or an OCC symbol."""
    if isinstance(option_symbol, Mapping):
        raw = option_symbol.get("expiration_date") or option_symbol.get("expiration")
        parsed = _mdy_date(raw)
        if parsed:
            return parsed
    from app.option_formatting import parse_occ

    parsed = parse_occ(str(symbol or ""))
    if not parsed:
        return None
    try:
        return date(2000 + int(parsed["yy"]), int(parsed["mm"]), int(parsed["dd"]))
    except (TypeError, ValueError, KeyError):
        return None


def _rejects_estimated_fee(action, symbol, trade_date, option_symbol=None, description=""):
    """Settlement, expiry, and assignment rows are not commission fills.

    A same-day close (expiry == trade date, no settlement wording) still
    gets an estimate. A fill posted after the contract expired does not:
    that is the cash-settlement posting day.
    """
    action_l = str(action or "").strip().lower()
    if action_l in _NO_ESTIMATE_ACTIONS:
        return True
    blob = f"{description or ''} {symbol or ''}".lower()
    if "as of" in blob or "cash settlement" in blob:
        return True
    if re.search(r"\b(expired|assigned|exercised)\b", blob):
        return True
    expiry = _option_expiry_date(symbol, option_symbol)
    posted = _mdy_date(trade_date)
    return bool(expiry and posted and expiry < posted)


def recent_option_fee_rates(history_df) -> dict:
    """Newest positive per-contract fee for each option root.

    SPXW statement opens are $12.22 on 10 contracts → $1.222. Order
    rows arrive with a blank fee; this rate fills them until the
    activity replaces the row. Settlement, expiry, assignment, and
    rows already marked estimated are not a rate source.
    """
    if history_df is None or getattr(history_df, "empty", True):
        return {}
    if "Symbol" not in history_df.columns or "fees_and_comm" not in history_df.columns:
        return {}
    from app.option_formatting import parse_occ

    ranked = []
    for _, row in history_df.iterrows():
        desc = str(row.get("Description") or "")
        if ESTIMATED_FEE_MARK in desc.lower():
            continue
        if _rejects_estimated_fee(
            row.get("Action"), row.get("Symbol"), row.get("Date"), description=desc,
        ):
            continue
        fee = abs(_safe_float(row.get("fees_and_comm"), 0.0))
        qty = abs(_safe_float(row.get("Quantity"), 0.0))
        if fee <= 0 or qty < 1e-9:
            continue
        parsed = parse_occ(str(row.get("Symbol") or ""))
        if not parsed:
            continue
        root = str(parsed.get("root") or "").strip().upper()
        if not root:
            continue
        ranked.append((_mdy_date(row.get("Date")) or date.min, root, fee / qty))
    ranked.sort(key=lambda item: item[0])
    rates = {}
    for _when, root, rate in ranked:
        rates[root] = rate
    return rates


def _leg_as_order(order: Mapping, leg: Mapping, index: int) -> Optional[dict]:
    """One v2 order leg as the flat recent-order shape the history mapper reads."""
    instrument = leg.get("instrument") if isinstance(leg.get("instrument"), Mapping) else {}
    symbol = str(instrument.get("symbol") or leg.get("symbol") or "").strip()
    asset = str(
        instrument.get("asset_type")
        or instrument.get("instrument_type")
        or leg.get("instrument_type")
        or ""
    ).upper()
    option_symbol = leg.get("option_symbol") if isinstance(leg.get("option_symbol"), Mapping) else None
    if option_symbol is None and isinstance(instrument.get("option_symbol"), Mapping):
        option_symbol = instrument.get("option_symbol")
    # v2 legs put the contract on instrument.symbol (OCC text) and leave
    # option_symbol empty. asset_type OPTION used to skip the parse and
    # fall through as an equity row, which the action map then dropped.
    if option_symbol is None:
        description = str(instrument.get("description") or "").strip()
        option_symbol = _option_symbol_from_text(symbol) or _option_symbol_from_text(description)
    looks_option = asset in {"OPTION", "OPTIONS"} or option_symbol is not None
    parent_id = str(order.get("brokerage_order_id") or "")
    leg_id = str(leg.get("leg_id") or index)
    price = leg.get("execution_price")
    if price in (None, "") and len(order.get("legs") or []) == 1:
        price = order.get("execution_price")
    row = {
        "brokerage_order_id": f"{parent_id}:{leg_id}" if parent_id else leg_id,
        "status": leg.get("status") or order.get("status"),
        "action": leg.get("action") or order.get("action"),
        "filled_quantity": leg.get("filled_quantity") if leg.get("filled_quantity") not in (None, "") else leg.get("total_quantity"),
        "total_quantity": leg.get("total_quantity"),
        "execution_price": price,
        "time_executed": order.get("time_executed") or leg.get("time_executed") or order.get("time_updated"),
        "time_updated": order.get("time_updated"),
        "fee": _split_leg_fee(order, leg),
    }
    if looks_option:
        if option_symbol is None:
            return None
        row["option_symbol"] = option_symbol
    elif symbol:
        row["universal_symbol"] = {"raw_symbol": symbol, "symbol": symbol, "description": symbol}
    else:
        return None
    return row


def flatten_order_legs(orders: Iterable[Mapping]) -> list:
    """Explode multi-leg orders into one row per leg.

    A same-day SPXW spread is one executed order with two option legs.
    The flat ``option_symbol`` field is empty on that shape, so the
    history mapper used to drop the whole fill. Legs that already look
    like a single-leg order pass through unchanged.
    """
    flat = []
    for order in orders or ():
        if not isinstance(order, Mapping):
            continue
        legs = order.get("legs")
        if not isinstance(legs, (list, tuple)) or not legs:
            flat.append(order)
            continue
        for index, leg in enumerate(legs):
            if not isinstance(leg, Mapping):
                continue
            row = _leg_as_order(order, leg, index)
            if row is not None:
                flat.append(row)
    return flat


def _order_equity_symbol(order: Mapping) -> tuple[str, str]:
    """Ticker and company name from an equity order.

    SnapTrade documents ``universal_symbol`` as an object or null. A
    Vanguard-shaped payload can send the ticker as a bare string (or omit
    the object). Calling ``.get`` on that string raises AttributeError and
    fails the whole account sync. A string is the ticker; anything else
    that is not a mapping contributes no symbol.
    """
    usym = order.get("universal_symbol")
    if isinstance(usym, str):
        ticker = usym.strip()
        return _crypto_pair_base(ticker), ticker
    if not isinstance(usym, Mapping):
        return "", ""
    raw = usym.get("raw_symbol") or usym.get("symbol") or ""
    if isinstance(raw, Mapping):
        raw = raw.get("raw_symbol") or raw.get("symbol") or ""
    ticker = _crypto_pair_base(str(raw or "").strip())
    desc = usym.get("description")
    if not isinstance(desc, str):
        desc = ""
    return ticker, desc.strip()


def orders_to_history_df(
    orders: Iterable[Mapping],
    *,
    account_name: str,
    user_id,
    tenant_id: str,
    fee_rates: Optional[Mapping] = None,
    cash_equivalent_symbols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Build a HISTORY_SEED_COLUMNS DataFrame from SnapTrade's
    ``get_user_account_recent_orders`` endpoint.

    **Why this exists.** SnapTrade's ``activities`` endpoint (the
    canonical history source) lags hours-to-days behind broker truth.
    SnapTrade's own docs warn about this:
    https://docs.snaptrade.com/demo/get-transactions —
    *"could take a significant amount of time for SnapTrade to
    initially retrieve and index the data"*. Their ``recent_orders``
    endpoint reflects executed orders within seconds. Reading both
    and merging gives users their just-placed trades in the next sync
    instead of waiting for SnapTrade's ingestion cadence.

    Real-world repro: 2026-05-14, camada236 placed BUY 98 NVDA on
    Alpaca paper at 18:03:57 UTC. ``recent_orders`` returned that fill
    less than 2 minutes later; ``activities`` returned 0 hours later.
    The first commit shipped 0 trade rows for an account that clearly
    had trades — a trust-killer for first-time users.

    **Options (open/close-aware, 2026-07-08).** Earlier this path skipped
    ALL option orders on the premise that ``recent_orders`` couldn't
    disambiguate Buy-to-Open vs Sell-to-Close. That premise was only true
    for brokers that ship bare ``BUY`` / ``SELL`` (Alpaca paper). The
    record-level ``action`` field DOES carry the explicit open/close on
    brokers that support it (Schwab-via-SnapTrade ships ``BUY_OPEN`` /
    ``SELL_CLOSE`` / …; the strict enum documents the ``_TO_`` form). So we
    now emit option rows when ``action`` maps through
    ``SNAPTRADE_ORDER_ACTION_TO_OPTION_ACTION`` (zero guessing) and STILL
    defer to the activities feed when a broker ships only bare BUY/SELL for
    an option (open vs close genuinely unknowable from the order alone).
    Option Amount applies the 100x contract multiplier so the seed dollars
    match the activities row (whose ``amount`` is the broker's gross premium);
    the cross-source dedup key omits Amount, so the two collapse on
    (Date, Action, Symbol, Quantity, Price) when activities catches up.

    **Dedup contract.** Description is intentionally minimal
    (``universal_symbol.description`` — the company name) so that when
    activities catches up later with a richer Description text, the
    second-pass cross-source dedup in ``app.upload._dedup_history_rows``
    collapses the two rows by (Date, Action, Symbol, Quantity, Price,
    Amount) and keeps the richer-description (activities) row.

    Sign convention: matches ``activities_to_history_df`` exactly so
    the dedup keys agree (BUY → negative Amount, SELL → positive
    Amount).
    """
    rows: list[dict] = []
    user_id_int = _seed_user_id(user_id)
    tenant_id_str = str(tenant_id).strip()
    sweep_symbols = _sweep_symbol_set(cash_equivalent_symbols)

    for order in flatten_order_legs(orders):
        if not isinstance(order, Mapping):
            continue

        status = str(order.get("status") or "").strip().upper()
        if status != "EXECUTED":
            # PENDING / CANCELLED / REJECTED / EXPIRED — not a trade
            # event. Activities will never report these as fills, so
            # writing them would create phantom rows.
            continue

        action = str(order.get("action") or "").strip().upper()
        option_symbol = order.get("option_symbol")
        # A string here is not a contract object. The old ``bool(...)`` check
        # then called ``.get`` inside the OSI builder's callers and, on the
        # equity branch, ``.get`` on ``universal_symbol``. Skip the row.
        if option_symbol and not isinstance(option_symbol, Mapping):
            continue
        is_option = isinstance(option_symbol, Mapping)
        company = ""

        if is_option:
            # Options: emit ONLY when the action explicitly disambiguates
            # open/close (Schwab ships BUY_OPEN/SELL_CLOSE/…; strict enum
            # uses the _TO_ form). A bare BUY/SELL on an option is NOT in
            # the map → defer to the activities feed rather than guess
            # (guessing open/close wrong corrupts the contract lifecycle —
            # see the ORCL phantom-open incident in broker-sync-safety).
            action_label = SNAPTRADE_ORDER_ACTION_TO_OPTION_ACTION.get(action)
            if not action_label:
                _log.info(
                    "snaptrade_normalize: option order action %r lacks an "
                    "open/close signal — deferring to activities feed",
                    action,
                )
                continue
            # ``recent_orders`` puts the contract in a flat ``option_symbol``
            # dict; snaptrade_symbol_to_osi expects it nested under that key
            # (same shape the activities/positions paths fold in). Verified
            # 2026-07-08 to yield the canonical OSI ("DAL   260717C00095000")
            # that matches the positions snapshot exactly.
            sym_str = snaptrade_symbol_to_osi({"option_symbol": option_symbol})
            if not sym_str:
                continue
        else:
            if action == "BUY":
                action_label = "Buy"
            elif action == "SELL":
                action_label = "Sell"
            else:
                # Unknown action verb. Activities will catch this trade
                # eventually — better to skip than guess.
                ticker, _company = _order_equity_symbol(order)
                _log.warning(
                    "snaptrade_normalize: dropping order with unknown action %r "
                    "for symbol %r — will be picked up via activities later",
                    action,
                    ticker,
                )
                continue

            sym_str, company = _order_equity_symbol(order)
            if not sym_str:
                continue
            if sym_str.strip().upper() in sweep_symbols:
                continue

        # Date: orders carry ISO-8601 UTC ``time_executed``; the
        # activities path uses MDY. Reuse the same formatter so dedup
        # keys agree on date format.
        time_executed = order.get("time_executed") or order.get("time_updated")
        if isinstance(time_executed, str):
            # ``_format_date_mdy`` already accepts ISO datetime
            # strings — it parses by date prefix.
            trade_date = _format_date_mdy(time_executed)
        else:
            trade_date = _format_date_mdy(time_executed)

        # Quantity: prefer filled_quantity (handles partial fills
        # correctly); fall back to total_quantity.
        units = _safe_float(
            order.get("filled_quantity"),
            _safe_float(order.get("total_quantity"), 0.0),
        )
        if not units:
            continue

        price = _safe_float(order.get("execution_price"), 0.0)
        if not price:
            # No fill price — can't construct an Amount. Skip; the
            # activities-side will eventually carry the right amount.
            continue
        order_fees = abs(_safe_float(
            order.get("fee") or order.get("fees") or order.get("commission"),
            0.0,
        ))
        estimated_fee = False
        # Cash settlement, expiry, and assignment are zero-commission.
        # A 0DTE close on the expiry date still estimates (Oct 2 20-lot).
        if (
            is_option
            and not order_fees
            and fee_rates
            and not _rejects_estimated_fee(
                action_label, sym_str, trade_date, option_symbol,
                str(order.get("description") or ""),
            )
        ):
            from app.option_formatting import parse_occ
            parsed = parse_occ(sym_str)
            root = str((parsed or {}).get("root") or "").strip().upper()
            rate = _safe_float((fee_rates or {}).get(root), 0.0)
            if rate > 0 and units:
                order_fees = round(abs(units) * rate, 2)
                estimated_fee = order_fees > 0

        # Options are quoted per-share but the contract is 100 shares, so
        # the dollar Amount needs the 100x multiplier to match the
        # activities row's gross premium (equities are 1x). Amount is NOT
        # part of the cross-source dedup key, but it IS the value that
        # survives downstream until activities catches up, so it must be
        # the real dollars.
        contract_mult = 100.0 if is_option else 1.0
        amount = round(abs(units) * price * contract_mult, 6)
        # Sign by cash direction: any Buy* is cash out (negative); any
        # Sell* is cash in (positive). Covers equity Buy/Sell AND the
        # option Buy/Sell to Open/Close labels.
        if action_label.startswith("Buy"):
            amount_signed = -abs(amount)
        else:  # Sell / Sell to Open / Sell to Close
            amount_signed = abs(amount)
        if is_option and order_fees:
            amount_signed = _net_option_cash(amount_signed, units, price, order_fees)

        # Description: minimal so the cross-source dedup prefers activities'
        # richer broker text. Options have no universal_symbol (the contract
        # lives in option_symbol), so use the OSI string; equities use the
        # company name. See docstring "Dedup contract".
        if is_option:
            description = sym_str
            if estimated_fee:
                description = f"{description} {ESTIMATED_FEE_MARK}"
        else:
            description = (company or sym_str).strip()

        rows.append({
            "Account": account_name,
            "user_id": user_id_int,
            "tenant_id": tenant_id_str,
            "Date": trade_date,
            "Action": action_label,
            "Symbol": sym_str,
            "Description": description,
            "Quantity": abs(units),
            "Price": price,
            # A commission on the order is netted into Amount above.
            # Blank when the feed omits it — the activity row's longer
            # description then wins dedup and replaces the gross.
            "fees_and_comm": order_fees if order_fees else "",
            "Amount": amount_signed,
        })

    return pd.DataFrame(rows, columns=HISTORY_SEED_COLUMNS)


# ---------------------------------------------------------------------------
# Positions → current_positions rows
# ---------------------------------------------------------------------------

# US corporate/treasury par is $1,000. A clean quote of 98 means 98% of
# face, so dollar value is units * price / 100 * 1000 = units * price * 10.
# Applied only when the instrument kind is a bond AND the payload did not
# send market_value / equity. An ETF (type ``et`` / ``etf``) is not a bond.
_BOND_PERCENT_OF_FACE = 10.0

_EQUITY_KIND_COMPACTS = frozenset({
    "CS", "ET", "ETF", "STOCK", "AD", "ADR", "CEF", "PS", "OEF",
    "MUTUALFUND", "EQUITY", "COMMONSTOCK", "PREFERREDSTOCK",
    "PREFERRED", "CLOSEDENDFUND", "CLOSEDEND", "ETFS", "OPENENDFUND",
})
_BOND_KIND_COMPACTS = frozenset({"BOND", "BND", "FIXEDINCOME"})
_CUSIP_RE = re.compile(r"^[0-9A-Z]{9}$")
_FIGI_RE = re.compile(r"^[A-Z0-9]{12}$")


def _kind_compacts(pos: Mapping, symbol_obj: Mapping) -> set[str]:
    """Type-code / instrument-kind tokens. Not the security description."""
    found: set[str] = set()
    code, desc = _type_code_and_description(symbol_obj)
    for part in (code, desc):
        whole = re.sub(r"[\s_\-]", "", part).upper()
        if whole:
            found.add(whole)
        for token in re.split(r"[\s_\-]+", part.strip()):
            compact = re.sub(r"[^A-Z0-9]", "", token.upper())
            if compact:
                found.add(compact)
    if isinstance(pos, Mapping):
        instrument = pos.get("instrument")
        if isinstance(instrument, Mapping):
            kind = str(instrument.get("kind") or "")
            whole = re.sub(r"[\s_\-]", "", kind).upper()
            if whole:
                found.add(whole)
    return found


def _is_equity_like_position(pos: Mapping, symbol_obj: Mapping) -> bool:
    return bool(_kind_compacts(pos, symbol_obj) & _EQUITY_KIND_COMPACTS)


def _is_bond_position(pos: Mapping, symbol_obj: Mapping) -> bool:
    """Individual bond / fixed income. A ticker ETF stays equity-like."""
    if _is_option(symbol_obj) or _is_equity_like_position(pos, symbol_obj):
        return False
    return bool(_kind_compacts(pos, symbol_obj) & _BOND_KIND_COMPACTS)


def _kind_label(kind: str) -> str:
    compact = re.sub(r"[\s_\-]", "", kind).upper()
    known = {
        "FUTURE": "Future",
        "CFD": "CFD",
        "TOKENIZEDASSET": "Tokenized Asset",
        "OTHER": "Other",
        "FUND": "Fund",
        "STRUCT": "Structured",
        "STRUCTUREDPRODUCT": "Structured",
    }
    if compact in known:
        return known[compact]
    words = [w for w in re.split(r"[\s_\-]+", kind.strip()) if w]
    return " ".join(w[:1].upper() + w[1:] for w in words)


def _preserved_instrument_kind(pos: Mapping, symbol_obj: Mapping) -> str:
    """Non-equity kind from positions/all. Classic equity rows stay Equity.

    ``instrument.kind`` is the v2 positions/all shape. A classic position
    has no instrument block, so an unknown type code still falls through
    to Equity — the same label Schwab, Fidelity, and Vanguard rows use
    today. Bonds are labeled separately. Mutual-fund and ETF kinds stay
    Equity.
    """
    if not isinstance(pos, Mapping):
        return ""
    instrument = pos.get("instrument")
    if not isinstance(instrument, Mapping):
        return ""
    kind = str(instrument.get("kind") or "").strip()
    compact = re.sub(r"[\s_\-]", "", kind).upper()
    if not compact:
        return ""
    if compact in _EQUITY_KIND_COMPACTS or compact in _BOND_KIND_COMPACTS:
        return ""
    if compact in {"OPTION", "FUTUREOPTION", "CRYPTO", "CRYPTOCURRENCY"}:
        return ""
    if _is_equity_like_position(pos, symbol_obj) or _is_bond_position(pos, symbol_obj):
        return ""
    return _kind_label(kind)


def _mapping_chain(pos: Mapping, symbol_obj: Mapping):
    if isinstance(pos, Mapping):
        yield pos
        instrument = pos.get("instrument")
        if isinstance(instrument, Mapping):
            yield instrument
            figi_obj = instrument.get("figi_instrument")
            if isinstance(figi_obj, Mapping):
                yield figi_obj
    if isinstance(symbol_obj, Mapping):
        yield symbol_obj
        inner = symbol_obj.get("symbol")
        if isinstance(inner, Mapping):
            yield inner
            figi_obj = inner.get("figi_instrument")
            if isinstance(figi_obj, Mapping):
                yield figi_obj


def _explicit_cusip(pos: Mapping, symbol_obj: Mapping) -> str:
    """CUSIP from a field named cusip. Broker ``local_id`` is not a CUSIP."""
    for mapping in _mapping_chain(pos, symbol_obj):
        raw = mapping.get("cusip")
        if not isinstance(raw, str):
            continue
        cusip = raw.strip().upper()
        if _CUSIP_RE.match(cusip):
            return cusip
    return ""


def _explicit_figi(pos: Mapping, symbol_obj: Mapping) -> str:
    for mapping in _mapping_chain(pos, symbol_obj):
        for key in ("figi_code", "figi"):
            raw = mapping.get(key)
            if not isinstance(raw, str):
                continue
            figi = raw.strip().upper()
            if _FIGI_RE.match(figi):
                return figi
    return ""


def _position_description(pos: Mapping, symbol_obj: Mapping) -> str:
    text = _description_from_symbol(symbol_obj)
    if text:
        return text
    if isinstance(pos, Mapping):
        instrument = pos.get("instrument")
        if isinstance(instrument, Mapping):
            instrument_text = str(instrument.get("description") or "").strip()
            if instrument_text:
                return instrument_text
        return str(pos.get("description") or "").strip()
    return ""


def _stable_description_key(text: str) -> str:
    """Same description → same symbol on every sync. Not a random id."""
    norm = re.sub(r"\s+", " ", str(text or "").strip().upper())
    if not norm:
        return ""
    digest = hashlib.sha256(norm.encode("utf-8")).hexdigest()[:8]
    slug = re.sub(r"[^A-Z0-9]+", "-", norm).strip("-")[:24]
    if slug:
        return f"DESC-{slug}-{digest}"
    return f"DESC-{digest}"


def _fallback_identifier(pos: Mapping, symbol_obj: Mapping) -> str:
    """CUSIP, then FIGI, then a stable description key. No ticker here."""
    cusip = _explicit_cusip(pos, symbol_obj)
    if cusip:
        return cusip
    figi = _explicit_figi(pos, symbol_obj)
    if figi:
        return figi
    return _stable_description_key(_position_description(pos, symbol_obj))


def _normalize_position_shape(pos: Mapping) -> Mapping:
    """Copy a positions/all ``instrument`` onto the classic symbol object.

    A classic holding (symbol or option_symbol already set) is returned
    unchanged so equity and option rows keep today's fields.
    """
    if not isinstance(pos, Mapping):
        return pos
    instrument = pos.get("instrument")
    if not isinstance(instrument, Mapping):
        return pos
    symbol = pos.get("symbol")
    has_symbol = (
        (isinstance(symbol, str) and bool(symbol.strip()))
        or (isinstance(symbol, Mapping) and bool(symbol))
    )
    if has_symbol or pos.get("option_symbol"):
        return pos
    raw = str(instrument.get("raw_symbol") or instrument.get("symbol") or "").strip()
    description = str(instrument.get("description") or pos.get("description") or "").strip()
    kind = str(instrument.get("kind") or "").strip()
    symbol_obj: dict = {}
    if raw:
        symbol_obj["raw_symbol"] = raw
        symbol_obj["symbol"] = raw
    if description:
        symbol_obj["description"] = description
    if kind:
        symbol_obj["type"] = {"code": kind, "description": kind}
    cusip = instrument.get("cusip") if isinstance(instrument.get("cusip"), str) else ""
    if not cusip and isinstance(pos.get("cusip"), str):
        cusip = pos.get("cusip")
    if isinstance(cusip, str) and cusip.strip():
        symbol_obj["cusip"] = cusip.strip()
    figi = instrument.get("figi_code") if isinstance(instrument.get("figi_code"), str) else ""
    figi_obj = instrument.get("figi_instrument")
    if not figi and isinstance(figi_obj, Mapping):
        nested = figi_obj.get("figi_code")
        if isinstance(nested, str):
            figi = nested
    if isinstance(figi, str) and figi.strip():
        symbol_obj["figi_code"] = figi.strip()
    out = dict(pos)
    out["symbol"] = symbol_obj
    kind_compact = re.sub(r"[\s_\-]", "", kind).upper()
    if kind_compact in {"OPTION", "FUTUREOPTION"}:
        # Prefer whichever text parses as OCC so the key matches
        # list_option_holdings. A compact OSI and a spaced OSI are the
        # same contract.
        for candidate in (instrument.get("symbol"), instrument.get("raw_symbol"), raw):
            if not isinstance(candidate, str) or not candidate.strip():
                continue
            parsed = _option_symbol_from_text(candidate)
            if parsed:
                out["option_symbol"] = parsed
                symbol_obj["option_symbol"] = parsed
                break
    return out


def position_holding_key(pos: Mapping) -> str:
    """Identifier shared by the seed Symbol and positions/all dedup.

    Ticker or OCC first, then CUSIP, FIGI, and a stable description key.
    """
    if not isinstance(pos, Mapping):
        return ""
    shaped = _normalize_position_shape(pos)
    symbol_obj = _position_symbol_obj(shaped)
    if _is_option(symbol_obj):
        osi = snaptrade_symbol_to_osi(symbol_obj).strip().upper()
        if osi:
            return osi
    ticker = _underlying_from_symbol(symbol_obj).strip().upper()
    if ticker:
        return ticker
    return _fallback_identifier(shaped, symbol_obj)


def merge_holdings_with_all_positions(holdings, all_positions) -> list:
    """Classic positions and option holdings win. New keys are appended.

    ``GET /accounts/{id}/positions`` does not list bonds and other
    non-equity instruments. Those arrive on ``positions/all``. A row
    whose identifier is already on the classic list is dropped so an
    equity or option is not rewritten.
    """
    merged: list = []
    seen: set[str] = set()
    for pos in holdings or ():
        if not isinstance(pos, Mapping):
            continue
        merged.append(pos)
        key = position_holding_key(pos)
        if key:
            seen.add(key)
    for pos in all_positions or ():
        if not isinstance(pos, Mapping):
            continue
        key = position_holding_key(pos)
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(_normalize_position_shape(pos))
    return merged


def _looks_like_percent_quote(value, price: float) -> bool:
    """A clean bond quote is near par (about 0–200), not a dollar total."""
    if _number_missing(value):
        return False
    magnitude = abs(_safe_float(value, 0.0))
    return magnitude <= max(250.0, abs(price) * 3.0)


def _bond_cost_when_price_is_percent(pos: Mapping, units: float, price: float, existing_cost: float) -> float:
    """Dollar cost when the only quote is percent of face.

    ``average_purchase_price`` or a v2 per-unit ``cost_basis`` in the
    same percent range is scaled like the price. A classic dollar total
    (much larger than the quote) is kept.
    """
    if not units:
        return existing_cost
    avg = pos.get("average_purchase_price")
    if not _number_missing(avg):
        avg_f = _safe_float(avg, 0.0)
        if _looks_like_percent_quote(avg, price):
            return avg_f * units * _BOND_PERCENT_OF_FACE
        return avg_f * units
    raw = pos.get("cost_basis")
    if _number_missing(raw):
        return existing_cost
    raw_f = _safe_float(raw, 0.0)
    if _looks_like_percent_quote(raw, price):
        return raw_f * units * _BOND_PERCENT_OF_FACE
    return raw_f


def positions_to_current_df(
    positions: Iterable[Mapping],
    *,
    account_name: str,
    user_id,
    tenant_id: str,
    already_in_cash: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Build a CURRENT_SEED_COLUMNS DataFrame from SnapTrade positions.

    Critical invariant: for equities we derive ``Price = market_value /
    quantity`` rather than trusting any per-share field SnapTrade
    returns. The dbt regression test
    ``int_enriched_current_equity_price_consistent.sql`` requires
    ``abs(qty * current_price - market_value) <= $0.01`` for every
    equity row, and trusting SnapTrade's ``price`` field has the same
    failure mode as trusting Schwab's ``averagePrice`` did
    (cost-per-share masquerading as current price; see SKILL
    2026-05-11). Options keep SnapTrade's ``price`` which is per-share
    of underlying premium and matches the CSV-export semantic of the
    seed's ``Price`` column for option rows.

    A holding with no ticker is kept. The identifier is the CUSIP, then
    the FIGI, then a stable hash of the description — the same string
    on every sync. Broker ``local_id`` is not an identifier.

    Instrument kind is preserved on ``security_type`` when it is not an
    equity, ETF, mutual fund, option, or crypto. Those stay Equity /
    Option / Cryptocurrency so an existing Schwab, Fidelity, or Vanguard
    share or contract does not change. A bond uses ``market_value`` when
    the payload sends one. When kind is bond and only ``price`` is
    present, that price is percent of $1,000 face.
    """
    rows: list[dict] = []
    user_id_int = _seed_user_id(user_id)
    tenant_id_str = str(tenant_id).strip()
    inside_cash = _sweep_symbol_set(already_in_cash)

    for raw_pos in positions or ():
        if not isinstance(raw_pos, Mapping):
            continue
        pos = _normalize_position_shape(raw_pos)
        if not isinstance(pos, Mapping):
            continue
        # Mirror the activities path: SnapTrade can ship an OPTION holding
        # with ``symbol: null`` and the structured contract at the position
        # top level (``pos["option_symbol"]``). Fold it in so open option
        # legs aren't silently dropped (same root cause as the historical
        # option-activity bug — see broker-sync-safety). A bare-string
        # ticker is coerced the same way activities are.
        symbol_obj = _promote_option_text(
            _attach_option_symbol(
                _coerce_symbol_obj(pos.get("symbol")),
                pos.get("option_symbol"),
            ),
            pos.get("symbol"),
        )
        is_option = _is_option(symbol_obj)
        if is_option:
            sym_str = snaptrade_symbol_to_osi(symbol_obj)
        else:
            sym_str = _underlying_from_symbol(symbol_obj)
        used_alt_id = False
        if not sym_str:
            # No ticker. CUSIP, then FIGI, then a stable description key.
            # A missing identifier still skips the row. ``local_id`` is
            # not consulted.
            sym_str = _fallback_identifier(pos, symbol_obj)
            used_alt_id = bool(sym_str)
        if not sym_str:
            continue

        units = _position_quantity(pos)
        raw_price = pos.get("price")
        price_missing = _number_missing(raw_price)
        price = 0.0 if price_missing else _safe_float(raw_price, 0.0)

        # Option contracts represent 100 shares of the underlying, but the
        # two SnapTrade per-unit fields on ``OptionsPosition`` are in
        # DIFFERENT units (verified against the SDK schema, options_position.py):
        #   * ``price``                 = market price PER SHARE
        #   * ``average_purchase_price``= cost basis  PER CONTRACT
        #     ("divide by shares per contract (usually 100) to get per share")
        # SnapTrade ships NO market_value / cost_basis for option holdings,
        # so both are derived — but the 100x multiplier applies ONLY to the
        # per-share ``price``:
        #   market_value = |units| * price * 100          (per-share → total)
        #   cost_basis   = |units| * average_purchase_price (already per-contract)
        # Applying ×100 to cost_basis too double-counts the multiplier and
        # snapshots the option at 100× its real cost (2026-07-13 SEI bug:
        # 10× $70C real cost $9,206.63 stored as $920,663 → -$913,563 phantom
        # unrealized loss). ``contract_mult`` is therefore used for
        # market_value ONLY; cost_basis below intentionally omits it.
        contract_mult = 100.0 if is_option else 1.0

        # market_value: SnapTrade does NOT ship this at the position
        # level. The actual response keys (Alpaca via SnapTrade, May
        # 2026) are: symbol, price, open_pnl, fractional_units,
        # currency, units, average_purchase_price, cash_equivalent,
        # tax_lots. The earlier code did `pos.get("market_value")`,
        # got None, and stored 0.0 for every position — which both
        # broke the UI ("$0 market value, -100% unrealized P&L") AND
        # tripped the dbt regression `int_enriched_current_equity_price_consistent.sql`
        # because qty * price - 0 != 0. Always derive from the fields
        # that DO exist; keep the get for forward-compat in case a
        # different broker through SnapTrade does ship it.
        raw_mv = pos.get("market_value")
        if _number_missing(raw_mv):
            raw_mv = pos.get("equity")
        mv_missing = _number_missing(raw_mv)
        # Explicit 0 is a real zero (a worthless quote). A missing price
        # AND a missing market value is "no NAV yet": leave market value
        # at 0 so the close-based ladder can fill from daily prices, and
        # do not book unrealized as market_value − cost (that is a
        # phantom loss of the whole cost basis).
        if not mv_missing:
            market_value = _safe_float(raw_mv, 0.0)
        elif not price_missing:
            market_value = units * price * contract_mult
        else:
            market_value = 0.0
        # Bond quote is percent of $1,000 face, and only when the payload
        # did not send a dollar market value. Equity and option math above
        # is left as-is (contract_mult is 100 for options and 1 otherwise).
        bond_from_price = (
            not is_option
            and mv_missing
            and not price_missing
            and _is_bond_position(pos, symbol_obj)
        )
        if bond_from_price:
            market_value = units * price * _BOND_PERCENT_OF_FACE
        quote_missing = price_missing and mv_missing

        # cost_basis: prefer broker-supplied total, else derive from
        # average_purchase_price * units. No `cost_basis` field on Alpaca
        # (or Schwab option holdings) via SnapTrade, so we usually derive.
        # Options: ``average_purchase_price`` is PER CONTRACT (see the
        # contract_mult comment above), so total cost basis = avg * |units|
        # with NO extra ×100. abs(units) yields a POSITIVE magnitude so the
        # short-aware unrealized formula in stg_current
        # (market_value + cost_basis) nets correctly for both long and
        # short legs. Equities: average_purchase_price is per share.
        cost_basis = _safe_float(pos.get("cost_basis"), 0.0)
        if not cost_basis:
            avg_purchase = _safe_float(pos.get("average_purchase_price"), 0.0)
            if avg_purchase and units:
                if is_option:
                    cost_basis = avg_purchase * abs(units)
                else:
                    cost_basis = avg_purchase * units
        if bond_from_price:
            cost_basis = _bond_cost_when_price_is_percent(
                pos, units, price, cost_basis,
            )

        # Per-share price for the seed:
        # - Options: SnapTrade's per-share premium is the seed value.
        # - Equities: derive from market_value / units to satisfy the
        #   `qty * price == market_value` regression invariant
        #   tautologically. Falls back to the broker's `price` field
        #   only when we somehow lack one of those two.
        if is_option:
            seed_price = price
        elif units and market_value:
            seed_price = round(market_value / units, 4)
        else:
            seed_price = price

        # Unrealized P&L: SnapTrade gives `open_pnl` directly when the
        # broker computes it (Alpaca does). Prefer it over our own
        # subtraction so we don't drift from the broker's reported
        # value by floating-point noise. Falls through to derived only
        # when SnapTrade omits open_pnl (some brokers).
        gl_dollar = ""
        gl_percent = ""
        open_pnl_raw = pos.get("open_pnl")
        if open_pnl_raw is not None:
            gl_dollar = round(_safe_float(open_pnl_raw, 0.0), 4)
            if cost_basis:
                gl_percent = round(100.0 * gl_dollar / abs(cost_basis), 4)
        elif cost_basis and not quote_missing:
            # Short options carry a positive cost_basis (premium received)
            # and a negative market_value (cost to buy back), so unrealized
            # P&L is market_value + cost_basis. Longs (and equities) net
            # market_value - cost_basis. Mirrors stg_current's short-aware
            # recompute so the seed value agrees before dbt even runs.
            # Skipped when the quote itself is missing: 0 − cost is not a
            # reported loss.
            if is_option and units < 0:
                gl_dollar = round(market_value + cost_basis, 4)
            else:
                gl_dollar = round(market_value - cost_basis, 4)
            gl_percent = round(100.0 * gl_dollar / abs(cost_basis), 4)

        if is_option:
            security_type = "Option"
        elif _is_cash_equivalent(pos) or sym_str.strip().upper() in inside_cash:
            # Settlement / money-market sweeps. stg_current drops this
            # security_type from holdings. The cash balance already
            # includes the dollars, either because SnapTrade set
            # cash_equivalent or because the account total matches
            # without this fund. A priority-2 cash line can still be
            # read from this row when the balance payload omitted cash.
            security_type = "Cash and Money Market"
        elif _is_crypto(symbol_obj):
            # ``Cryptocurrency`` is the broker-corroborated crypto marker
            # (SnapTrade type.code, or whitelist ONLY when the broker gave
            # no type block — see _is_crypto). ``stg_current`` folds it into
            # ``instrument_type='Equity'`` (crypto has no options and behaves
            # like a spot holding for P&L); the "Crypto" STRATEGY bucket is
            # then assigned downstream in ``int_strategy_classification`` and
            # is gated on THIS security_type so a ticker collision (e.g. SEI =
            # Solaris Energy equity vs the Sei token) can't mislabel an equity.
            security_type = "Cryptocurrency"
        elif _is_bond_position(pos, symbol_obj):
            security_type = "Bond"
        else:
            # Equity, ETF, and mutual fund stay Equity. A positions/all
            # kind that is none of those (future, CFD, …) keeps its name.
            security_type = _preserved_instrument_kind(pos, symbol_obj) or "Equity"
        if used_alt_id:
            description = _position_description(pos, symbol_obj) or sym_str
        else:
            description = _description_from_symbol(symbol_obj) or sym_str

        rows.append({
            "Account": account_name,
            "user_id": user_id_int,
            "tenant_id": tenant_id_str,
            "Symbol": sym_str,
            "Description": description,
            "Quantity": units,
            "Price": seed_price,
            "market_value": market_value,
            "cost_bases": cost_basis,
            "gain_or_loss_dollat": gl_dollar,
            "gain_or_loss_percent": gl_percent,
            "security_type": security_type,
        })

    df = pd.DataFrame(rows)
    for col in CURRENT_SEED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[CURRENT_SEED_COLUMNS]


# ---------------------------------------------------------------------------
# Balances → account_balances rows
# ---------------------------------------------------------------------------

def balances_to_balance_df(
    *,
    account_summary: Mapping,
    balances: Iterable[Mapping],
    positions: Iterable[Mapping],
    account_name: str,
    user_id,
    tenant_id: str,
    already_in_cash: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Two rows for account_balances.csv (cash + account_total).

    ``account_summary`` is SnapTrade's ``/accounts/{id}`` payload (has
    ``balance.total.amount``, ``cash`` etc).
    ``balances`` is the per-currency breakdown.
    ``positions`` is the same iterable passed to
    ``positions_to_current_df`` — used to compute total cost basis when
    SnapTrade doesn't report it on the summary.
    """
    user_id_int = _seed_user_id(user_id)
    tenant_id_str = str(tenant_id).strip()

    cash, reported_total = _reported_cash_and_total(balances, account_summary)
    # Distinguish an explicitly reported $0 account from an absent/malformed
    # summary.  A real zero still needs balance rows: otherwise the seed merge
    # cannot clear a prior non-zero balance, and the every-tenant onboarding
    # readiness check can never observe a legitimately empty account.
    has_explicit_total = reported_total is not None
    total = reported_total if reported_total is not None else 0.0
    if already_in_cash is None:
        inside_cash = symbols_already_in_cash(
            positions, balances=balances, account_summary=account_summary,
        )
    else:
        inside_cash = _sweep_symbol_set(already_in_cash)

    pos_mv = 0.0
    pos_cb = 0.0
    for pos in positions or ():
        if not isinstance(pos, Mapping):
            continue
        # Dollars already in cash must not also be stock market value
        # or stock cost. That double-counts a settlement fund.
        ticker = _equity_ticker(_position_symbol_obj(pos))
        if (ticker and ticker in inside_cash) or _is_cash_equivalent(pos):
            continue
        pos_mv += _safe_float(pos.get("market_value"), 0.0)
        pos_cb += _safe_float(pos.get("cost_basis"), 0.0)

    if total <= 0 and (cash != 0 or pos_mv > 0):
        total = pos_mv + cash

    if total <= 0 and cash == 0 and not pos_mv and not has_explicit_total:
        return pd.DataFrame(columns=BALANCE_SEED_COLUMNS)

    pct_cash = ""
    if total > 0:
        pct_cash = round(100.0 * cash / total, 6)

    unreal: Optional[float] = None
    if total and pos_cb:
        unreal = total - pos_cb
    unreal_pct: Optional[float] = None
    if unreal is not None and pos_cb:
        unreal_pct = round(100.0 * unreal / abs(pos_cb), 6) if pos_cb != 0 else None

    rows = [
        {
            "account": account_name,
            "user_id": user_id_int,
            "tenant_id": tenant_id_str,
            "row_type": "cash",
            "market_value": cash,
            "cost_basis": "",
            "unrealized_pnl": "",
            "unrealized_pnl_pct": "",
            "percent_of_account": pct_cash,
        },
        {
            "account": account_name,
            "user_id": user_id_int,
            "tenant_id": tenant_id_str,
            "row_type": "account_total",
            "market_value": total,
            "cost_basis": pos_cb if pos_cb else "",
            "unrealized_pnl": unreal if unreal is not None else "",
            "unrealized_pnl_pct": unreal_pct if unreal_pct is not None else "",
            "percent_of_account": "",
        },
    ]
    return pd.DataFrame(rows, columns=BALANCE_SEED_COLUMNS)
