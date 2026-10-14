"""Compact OCC contracts must not become the underlying ticker.

Warehouse run 38499020109 fetched 303 option contracts as equities and
tripped the price coverage guard. The root extraction lives in
dbt/macros/occ_underlying_symbol.sql; this pins the same cases.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MACRO = ROOT / "dbt" / "macros" / "occ_underlying_symbol.sql"
OCC_RE = re.compile(r"^([A-Z][A-Z0-9.-]{0,5})(\d{6}[CP]\d{8})$")


def occ_underlying(symbol: str) -> str:
    """Mirror occ_underlying_symbol(): OCC root, else first space token."""
    compact = str(symbol).upper().replace(" ", "")
    match = OCC_RE.match(compact)
    if match:
        return match.group(1)
    return str(symbol).split(" ")[0]


def test_macro_uses_the_occ_root_pattern():
    text = MACRO.read_text()
    assert r"^[A-Z][A-Z0-9.-]{0,5}\d{6}[CP]\d{8}$" in text
    history = (ROOT / "dbt/models/staging/stg_history.sql").read_text()
    current = (ROOT / "dbt/models/staging/stg_current.sql").read_text()
    assert "occ_underlying_symbol('sym_trim', 'sym_upper')" in history
    assert "occ_underlying_symbol('sym_trim', 'sym_upper')" in current


def test_compact_occ_root_is_the_underlying():
    assert occ_underlying("AAPL240119C00150000") == "AAPL"
    assert occ_underlying("QQQ260226P00600000") == "QQQ"
    assert occ_underlying("GOOG260116C00180000") == "GOOG"
    assert occ_underlying("UVXY1251205C00007000") == "UVXY1"


def test_padded_osi_and_long_form_stay_on_the_root():
    assert occ_underlying("AAPL  240119C00150000") == "AAPL"
    assert occ_underlying("DAL   260717C00093000") == "DAL"
    assert occ_underlying("SPX   261016C07655000") == "SPX"
    assert occ_underlying("AAPL 05/12/2023 175.00 C") == "AAPL"
    assert occ_underlying("BRK.B 01/16/2026 450.00 C") == "BRK.B"
    assert occ_underlying("BRK.B  260116C00450000") == "BRK.B"


def test_plain_tickers_are_unchanged():
    for symbol in (
        "AAPL", "BRK.B", "GLOP-C", "GLOP-PC", "BTC-USD",
        "SCHFDX0", "MFC8070C", "2328",
    ):
        assert occ_underlying(symbol) == symbol
