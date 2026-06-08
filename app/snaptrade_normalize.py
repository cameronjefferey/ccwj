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

import logging
import math
from datetime import datetime, date
from typing import Iterable, Mapping, Optional, Sequence

import pandas as pd

from app.upload import (
    BALANCE_SEED_COLUMNS,
    CRYPTO_SYMBOLS,
    CURRENT_SEED_COLUMNS,
    HISTORY_SEED_COLUMNS,
)

_log = logging.getLogger(__name__)


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

    Two signals, OR'd:
      1. ``symbol.symbol.type.code`` (or ``symbol.type.code`` for flattened
         payloads) matches a known crypto code.
      2. Underlying ticker is on the curated CRYPTO_SYMBOLS whitelist.

    Returns False for non-Mapping inputs / unknown shapes — better to
    fall through to "Equity" than mis-classify an equity row as crypto.
    """
    if not isinstance(symbol_obj, Mapping):
        return False
    # Type code (preferred — broker-canonical when present)
    for candidate in (
        symbol_obj.get("symbol") if isinstance(symbol_obj.get("symbol"), Mapping) else None,
        symbol_obj,
    ):
        if not isinstance(candidate, Mapping):
            continue
        type_block = candidate.get("type")
        if isinstance(type_block, Mapping):
            code = str(type_block.get("code") or "").strip().upper()
            if code in _CRYPTO_TYPE_CODES:
                return True
            desc = str(type_block.get("description") or "").strip().upper()
            if "CRYPTO" in desc:
                return True
    # Symbol-whitelist fallback
    underlying = _underlying_from_symbol(symbol_obj).upper()
    return underlying in CRYPTO_SYMBOLS


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
    "STOCK_DIVIDEND": "Cash Dividend",
    "REI": "Cash Dividend",  # Dividend reinvestment — treated as a div event for P&L.
    # Interest / fees
    "INTEREST": "Credit Interest",
    "FEE": "ADR Mgmt Fee",
    "TAX": "ADR Mgmt Fee",  # Closest CSV-export bucket; refine later if needed.
    # Cash movements — explicitly DROPPED. They're noise relative to the
    # trading mirror and stg_history doesn't have a row type for them.
    "DEPOSIT": None,
    "WITHDRAWAL": None,
    "TRANSFER": None,
    "JOURNAL": None,
    "CONTRIBUTION": None,
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
}


# SnapTrade's activity-level ``option_type`` field carries the EXPLICIT
# open/close action (far more reliable than parsing the broker description).
SNAPTRADE_OPTION_TYPE_TO_ACTION: dict[str, str] = {
    "BUY_TO_OPEN": "Buy to Open",
    "BUY_TO_CLOSE": "Buy to Close",
    "SELL_TO_OPEN": "Sell to Open",
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


def _format_date_mdy(value) -> str:
    """Coerce SnapTrade's various date/datetime shapes into ``MM/DD/YYYY``.

    stg_history parses dates with ``parse_date('%m/%d/%Y', regexp_extract(date,
    r'(\\d{1,2}/\\d{1,2}/\\d{4})$'))`` so any string with a trailing
    MM/DD/YYYY token works; we emit the bare form so the regex anchor is
    irrelevant.
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
            return str(u).strip().upper()
    inner = symbol_obj.get("symbol") if isinstance(symbol_obj.get("symbol"), Mapping) else symbol_obj
    raw = (
        inner.get("raw_symbol")
        or inner.get("symbol")
        or symbol_obj.get("raw_symbol")
        or symbol_obj.get("ticker")
        or ""
    )
    return str(raw).strip().upper()


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
    user_id_int = int(user_id) if user_id is not None and user_id != "" else ""
    tenant_id_str = str(tenant_id).strip()

    for act in activities or ():
        if not isinstance(act, Mapping):
            continue
        atype = str(act.get("type") or "").strip().upper()
        if not atype:
            continue
        action_label = SNAPTRADE_ACTIVITY_TO_ACTION.get(atype, "__UNKNOWN__")
        if action_label is None:
            continue
        if action_label == "__UNKNOWN__":
            _log.warning("snaptrade_normalize: dropping unknown activity type %r", atype)
            continue

        symbol_obj = act.get("symbol") or {}
        # SnapTrade (Schwab via SnapTrade) ships OPTION activities with
        # ``symbol: null`` and the structured contract at the ACTIVITY top
        # level (``act["option_symbol"]``), NOT nested under ``symbol``.
        # Without folding it in, ``_is_option`` sees an empty dict, every
        # option is misclassified as equity with an empty Symbol, and
        # stg_history can't recognize it as an option — the entire option
        # lane silently vanished from the warehouse (hundreds of contracts
        # per account). See broker-sync-safety "Bugs we've shipped".
        if not isinstance(symbol_obj, Mapping):
            symbol_obj = {}
        if not symbol_obj.get("option_symbol") and isinstance(
            act.get("option_symbol"), Mapping
        ):
            symbol_obj = {**symbol_obj, "option_symbol": act["option_symbol"]}
        is_option = _is_option(symbol_obj)
        if is_option:
            sym_str = snaptrade_symbol_to_osi(symbol_obj)
        else:
            sym_str = _underlying_from_symbol(symbol_obj)
        # Description: prefer the activity-level description (broker-
        # original wording — usually carries "Buy to Close" / etc. for
        # options) over the symbol-level description (just a name).
        broker_description = str(act.get("description") or "").strip()
        description = broker_description or _description_from_symbol(symbol_obj) or sym_str

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
        units = _safe_float(act.get("units"), 0.0)
        price = _safe_float(act.get("price"), 0.0)
        fees = _safe_float(act.get("fee"), 0.0)
        amount = act.get("amount")
        if amount is None or not _is_finite_number(amount):
            # Fallback: derive from units * price; use sign convention
            # below to set direction.
            amount = units * price

        amount_f = _safe_float(amount, 0.0)
        # Direction matrix — explicit so we never trust the broker's
        # signed amount (different brokers via SnapTrade have shipped
        # contradictory conventions; stg_history will re-sign anyway,
        # but emitting the canonical sign keeps the seed CSV readable
        # and prevents dedup drift from sign-flips between syncs).
        cash_out_actions = {
            "Buy", "Buy to Open", "Buy to Close",
            "Margin Interest", "ADR Mgmt Fee",
        }
        cash_in_actions = {
            "Sell", "Sell to Open", "Sell to Close",
            "Cash Dividend", "Qualified Dividend", "Credit Interest",
        }
        if action_label in cash_out_actions:
            amount_signed = -abs(amount_f)
        elif action_label in cash_in_actions:
            amount_signed = abs(amount_f)
        else:
            amount_signed = amount_f

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

    df = pd.DataFrame(rows, columns=HISTORY_SEED_COLUMNS)
    return df


# ---------------------------------------------------------------------------
# Orders → history rows (real-time fallback for activity-feed lag)
# ---------------------------------------------------------------------------


def orders_to_history_df(
    orders: Iterable[Mapping],
    *,
    account_name: str,
    user_id,
    tenant_id: str,
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

    **What this function does NOT handle (intentionally).**
    Options orders. The ``recent_orders`` payload does not carry a
    description string we can use to disambiguate
    Buy-to-Open vs Buy-to-Close (whereas activities do — the broker
    text says "Bought to Open" / "Sold to Close"). Without prior
    position state we'd guess wrong half the time. Defer options-via-
    orders until we either (a) reconstruct held-state per symbol or
    (b) SnapTrade adds an order-side open/close flag.

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
    user_id_int = int(user_id) if user_id is not None and user_id != "" else ""
    tenant_id_str = str(tenant_id).strip()

    for order in orders or ():
        if not isinstance(order, Mapping):
            continue

        status = str(order.get("status") or "").strip().upper()
        if status != "EXECUTED":
            # PENDING / CANCELLED / REJECTED / EXPIRED — not a trade
            # event. Activities will never report these as fills, so
            # writing them would create phantom rows.
            continue

        # Skip options orders (see docstring — Open/Close needs
        # description text we don't have on the orders side).
        option_symbol = order.get("option_symbol")
        if option_symbol:
            continue

        action = str(order.get("action") or "").strip().upper()
        if action == "BUY":
            action_label = "Buy"
        elif action == "SELL":
            action_label = "Sell"
        else:
            # Unknown action verb. Activities will catch this trade
            # eventually — better to skip than guess.
            _log.warning(
                "snaptrade_normalize: dropping order with unknown action %r "
                "for symbol %r — will be picked up via activities later",
                action,
                ((order.get("universal_symbol") or {}).get("raw_symbol")),
            )
            continue

        usym = order.get("universal_symbol") or {}
        sym_str = (usym.get("raw_symbol") or usym.get("symbol") or "").strip()
        if not sym_str:
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

        amount = round(units * price, 6)
        if action_label == "Buy":
            amount_signed = -abs(amount)
        else:  # Sell
            amount_signed = abs(amount)

        # Description from the symbol object. Intentionally minimal so
        # the cross-source dedup prefers activities' richer text. See
        # docstring "Dedup contract".
        description = (usym.get("description") or sym_str).strip()

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
            # Orders endpoint does not surface broker fees / commissions;
            # leave empty so activities (which DOES carry them) wins on
            # the cross-source dedup tie-break by descriptor length and
            # this row is the one that gets dropped if activities arrives.
            "fees_and_comm": "",
            "Amount": amount_signed,
        })

    return pd.DataFrame(rows, columns=HISTORY_SEED_COLUMNS)


# ---------------------------------------------------------------------------
# Positions → current_positions rows
# ---------------------------------------------------------------------------

def positions_to_current_df(
    positions: Iterable[Mapping],
    *,
    account_name: str,
    user_id,
    tenant_id: str,
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
    """
    rows: list[dict] = []
    user_id_int = int(user_id) if user_id is not None and user_id != "" else ""
    tenant_id_str = str(tenant_id).strip()

    for pos in positions or ():
        if not isinstance(pos, Mapping):
            continue
        symbol_obj = pos.get("symbol") or {}
        # Mirror the activities path: SnapTrade can ship an OPTION holding
        # with ``symbol: null`` and the structured contract at the position
        # top level (``pos["option_symbol"]``). Fold it in so open option
        # legs aren't silently dropped (same root cause as the historical
        # option-activity bug — see broker-sync-safety).
        if not isinstance(symbol_obj, Mapping):
            symbol_obj = {}
        if not symbol_obj.get("option_symbol") and isinstance(
            pos.get("option_symbol"), Mapping
        ):
            symbol_obj = {**symbol_obj, "option_symbol": pos["option_symbol"]}
        is_option = _is_option(symbol_obj)
        if is_option:
            sym_str = snaptrade_symbol_to_osi(symbol_obj)
        else:
            sym_str = _underlying_from_symbol(symbol_obj)
        if not sym_str:
            continue

        units = _safe_float(pos.get("units"), 0.0)
        price = _safe_float(pos.get("price"), 0.0)

        # Option contracts are quoted per-share but represent 100 shares
        # of the underlying. SnapTrade's ``list_option_holdings`` ships a
        # per-share ``price`` / ``average_purchase_price`` and NO
        # market_value / cost_basis, so the derived fallbacks below must
        # apply the 100x contract multiplier to land total dollars —
        # which is the convention stg_current reads market_value /
        # cost_basis in (e.g. PLTR 2x80C @ $42 → market_value $8,400).
        # Without it an open option snapshots at 1/100th its real value.
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
        market_value = _safe_float(
            pos.get("market_value"),
            _safe_float(pos.get("equity"), units * price * contract_mult),
        )

        # cost_basis: prefer broker-supplied total, else derive from
        # average_purchase_price * units. Same shape comment — no
        # `cost_basis` field on Alpaca via SnapTrade. Options derive a
        # POSITIVE magnitude (abs units * 100) so the short-aware
        # unrealized formula in stg_current (market_value + cost_basis)
        # nets correctly for both long and short legs.
        cost_basis = _safe_float(pos.get("cost_basis"), 0.0)
        if not cost_basis:
            avg_purchase = _safe_float(pos.get("average_purchase_price"), 0.0)
            if avg_purchase and units:
                if is_option:
                    cost_basis = avg_purchase * abs(units) * contract_mult
                else:
                    cost_basis = avg_purchase * units

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
        elif cost_basis:
            # Short options carry a positive cost_basis (premium received)
            # and a negative market_value (cost to buy back), so unrealized
            # P&L is market_value + cost_basis. Longs (and equities) net
            # market_value - cost_basis. Mirrors stg_current's short-aware
            # recompute so the seed value agrees before dbt even runs.
            if is_option and units < 0:
                gl_dollar = round(market_value + cost_basis, 4)
            else:
                gl_dollar = round(market_value - cost_basis, 4)
            gl_percent = round(100.0 * gl_dollar / abs(cost_basis), 4)

        if is_option:
            security_type = "Option"
        elif _is_crypto(symbol_obj):
            # ``Cryptocurrency`` is the agreed marker that
            # ``stg_current`` parses into ``instrument_type='Crypto'``.
            # Older Coinbase rows in the seed shipped as ``Equity`` and
            # are retro-classified via the symbol whitelist in
            # ``stg_crypto_symbols`` — this marker exists so FUTURE
            # syncs are self-describing without needing the whitelist.
            security_type = "Cryptocurrency"
        else:
            security_type = "Equity"
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
) -> pd.DataFrame:
    """Two rows for account_balances.csv (cash + account_total).

    ``account_summary`` is SnapTrade's ``/accounts/{id}`` payload (has
    ``balance.total.amount``, ``cash`` etc).
    ``balances`` is the per-currency breakdown.
    ``positions`` is the same iterable passed to
    ``positions_to_current_df`` — used to compute total cost basis when
    SnapTrade doesn't report it on the summary.
    """
    user_id_int = int(user_id) if user_id is not None and user_id != "" else ""
    tenant_id_str = str(tenant_id).strip()

    cash = 0.0
    if isinstance(balances, Iterable):
        for bal in balances:
            if not isinstance(bal, Mapping):
                continue
            cash += _safe_float(bal.get("cash"), 0.0)

    if not cash and isinstance(account_summary, Mapping):
        cash = _safe_float(
            (account_summary.get("balance") or {}).get("cash") if isinstance(account_summary.get("balance"), Mapping) else None,
            _safe_float(account_summary.get("cash"), 0.0),
        )

    total = 0.0
    if isinstance(account_summary, Mapping):
        balance_block = account_summary.get("balance")
        if isinstance(balance_block, Mapping):
            total_block = balance_block.get("total")
            if isinstance(total_block, Mapping):
                total = _safe_float(total_block.get("amount"), 0.0)
            elif _is_finite_number(total_block):
                total = _safe_float(total_block, 0.0)

    pos_mv = 0.0
    pos_cb = 0.0
    for pos in positions or ():
        if not isinstance(pos, Mapping):
            continue
        pos_mv += _safe_float(pos.get("market_value"), 0.0)
        pos_cb += _safe_float(pos.get("cost_basis"), 0.0)

    if total <= 0 and (cash != 0 or pos_mv > 0):
        total = pos_mv + cash

    if total <= 0 and cash == 0 and not pos_mv:
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
