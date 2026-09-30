"""Display labels for sector / subsector.

The warehouse stores ``Unknown`` when yfinance has no sector. That string
shows up as a sector card and a fit-matrix column. Map symbols we can
identify (broad ETFs, sector ETFs, crypto) and label the rest
``Unclassified``, which sorts after every named sector.

A real sector from the warehouse is left alone. This is a display remap,
not a dbt change, so the page updates without a warehouse rebuild.
"""

from pathlib import Path

UNCLASSIFIED = "Unclassified"

# Blank / sentinel values. "other" is included so an older label sorts
# with the unclassified bucket.
_SENTINELS = frozenset({
    "", "unknown", "unclassified", "other", "none", "null", "nan", "n/a", "na",
})

# Fallback only — used when the warehouse sector is missing. Values follow
# the labels yfinance already uses for single-name stocks (Technology,
# Financial Services, …) so a mapped ETF sits with those cards.
_ETF_SECTORS = {
    "XLK": ("Technology", "Technology"),
    "VGT": ("Technology", "Technology"),
    "XLF": ("Financial Services", "Financial Services"),
    "XLE": ("Energy", "Energy"),
    "XLV": ("Healthcare", "Healthcare"),
    "XLY": ("Consumer Cyclical", "Consumer Cyclical"),
    "XLP": ("Consumer Defensive", "Consumer Defensive"),
    "XLI": ("Industrials", "Industrials"),
    "XLB": ("Basic Materials", "Basic Materials"),
    "XLRE": ("Real Estate", "Real Estate"),
    "VNQ": ("Real Estate", "Real Estate"),
    "XLU": ("Utilities", "Utilities"),
    "XLC": ("Communication Services", "Communication Services"),
    "SMH": ("Technology", "Semiconductors"),
    "SOXX": ("Technology", "Semiconductors"),
    "QQQ": ("Technology", "Nasdaq-100"),
    "QQQM": ("Technology", "Nasdaq-100"),
    "SPY": ("Broad Market", "Broad Market"),
    "VOO": ("Broad Market", "Broad Market"),
    "IVV": ("Broad Market", "Broad Market"),
    "VTI": ("Broad Market", "Broad Market"),
    "ITOT": ("Broad Market", "Broad Market"),
    "IWM": ("Broad Market", "Small Cap"),
    "DIA": ("Broad Market", "Dow Jones"),
    "JEPI": ("Equity Income", "Equity Income"),
    "JEPQ": ("Equity Income", "Equity Income"),
    "SCHD": ("Equity Income", "Equity Income"),
    "VYM": ("Equity Income", "Equity Income"),
    "TLT": ("Fixed Income", "Fixed Income"),
    "IEF": ("Fixed Income", "Fixed Income"),
    "BND": ("Fixed Income", "Fixed Income"),
    "AGG": ("Fixed Income", "Fixed Income"),
    "GLD": ("Commodities", "Gold"),
    "IAU": ("Commodities", "Gold"),
    "SLV": ("Commodities", "Silver"),
    "IBIT": ("Crypto", "Crypto"),
    "FBTC": ("Crypto", "Crypto"),
    "BITO": ("Crypto", "Crypto"),
}


def _load_crypto_symbols():
    path = Path(__file__).resolve().parents[1] / "dbt" / "seeds" / "crypto_symbols.csv"
    found = set()
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return frozenset()
    for i, line in enumerate(lines):
        if i == 0 or not line.strip():
            continue
        found.add(line.split(",")[0].strip().upper())
    return frozenset(found)


CRYPTO_SYMBOLS = _load_crypto_symbols()


def is_unclassified(value) -> bool:
    """True for blank, Unknown, Unclassified, and the same sentinels."""
    return str(value or "").strip().lower() in _SENTINELS


def canonical_sector_param(value) -> str:
    """Map a ``?sector=`` bookmark of Unknown onto the display label.

    An empty query param stays empty (no filter). Unknown / Unclassified
    / Other all select the unclassified bucket.
    """
    text = str(value or "").strip()
    if not text:
        return ""
    if text.lower() in {"unknown", "unclassified", "other"}:
        return UNCLASSIFIED
    return text


def classify_symbol(symbol, sector, subsector=None):
    """Return ``(sector, subsector)`` for display.

    A populated warehouse sector wins. Missing sectors fall through to
    the ETF map, then the crypto list, then Unclassified.
    """
    sym = str(symbol or "").strip().upper()
    sec_missing = is_unclassified(sector)
    sub_missing = is_unclassified(subsector)
    if not sec_missing:
        sub = UNCLASSIFIED if sub_missing else str(subsector).strip()
        return str(sector).strip(), sub
    mapped = _ETF_SECTORS.get(sym)
    if mapped:
        return mapped
    if sym in CRYPTO_SYMBOLS:
        return "Crypto", "Crypto"
    sub = UNCLASSIFIED if sub_missing else str(subsector).strip()
    return UNCLASSIFIED, sub


def apply_sector_labels(df, symbol_col="symbol", sector_col="sector", subsector_col="subsector"):
    """Relabel sector / subsector columns in place-safe copy. No-op without a symbol column."""
    if df is None or getattr(df, "empty", True):
        return df
    if symbol_col not in getattr(df, "columns", []):
        return df
    if sector_col not in df.columns and subsector_col not in df.columns:
        return df
    out = df.copy()
    symbols = out[symbol_col].tolist()
    sectors = out[sector_col].tolist() if sector_col in out.columns else [None] * len(out)
    subs = out[subsector_col].tolist() if subsector_col in out.columns else [None] * len(out)
    new_sec = []
    new_sub = []
    for sym, sec, sub in zip(symbols, sectors, subs):
        ns, nsub = classify_symbol(sym, sec, sub)
        new_sec.append(ns)
        new_sub.append(nsub)
    if sector_col in out.columns:
        out[sector_col] = new_sec
    if subsector_col in out.columns:
        out[subsector_col] = new_sub
    return out


def sort_unclassified_last(labels):
    """Named sectors keep their incoming order. Unclassified goes last."""
    head = []
    tail = []
    for label in labels:
        if is_unclassified(label):
            tail.append(label)
        else:
            head.append(label)
    return head + tail
