"""Unit tests for app/option_formatting.py.

The Schwab API ships option contract identifiers in OCC packed form
(e.g. `PLTR 260424C00141000`). Showing this raw to a trader forces them
to mentally parse a YYMMDD date and divide an 8-digit strike by 1000.
The `format_option_symbol` helper turns it into something readable
(`PLTR Apr 24 '26 $141C`) and falls back to the input unchanged when
the input doesn't match the OCC layout (e.g. plain equity tickers).
We pin that contract here so future refactors don't re-break the
display in the Expiring Soon card / Position Detail / Symbols pages.
"""
import pytest

from app.option_formatting import format_option_symbol, parse_occ


class TestParseOcc:
    def test_pltr_call_regression(self):
        # Exact string from the user's screenshot of the Expiring Soon card.
        parsed = parse_occ("PLTR 260424C00141000")
        assert parsed == {
            "root": "PLTR",
            "yy": "26",
            "mm": 4,
            "dd": 24,
            "cp": "C",
            "strike": 141.0,
        }

    def test_schwab_long_form_call(self):
        # Schwab CSV export form — the demo and most users see this directly
        # as the trade_symbol. We must NOT render it raw.
        parsed = parse_occ("PLTR 12/19/2025 95.00 C")
        assert parsed == {
            "root": "PLTR",
            "yy": "25",
            "mm": 12,
            "dd": 19,
            "cp": "C",
            "strike": 95.0,
        }

    def test_schwab_long_form_put_no_decimals(self):
        # Some long-form rows skip the trailing ".00" on the strike.
        parsed = parse_occ("AAPL 04/30/2026 230 P")
        assert parsed == {
            "root": "AAPL",
            "yy": "26",
            "mm": 4,
            "dd": 30,
            "cp": "P",
            "strike": 230.0,
        }

    def test_format_long_form_renders_friendly(self):
        # Round-trip: a Schwab-export long form should render the same
        # readable form an OCC packed string would render.
        assert format_option_symbol("PLTR 12/19/2025 95.00 C") == "PLTR Dec 19 '25 $95C"

    def test_no_space_between_root_and_date(self):
        # Some sources omit the embedded space; we should still parse.
        parsed = parse_occ("AAPL260117C00200000")
        assert parsed["root"] == "AAPL"
        assert parsed["mm"] == 1
        assert parsed["dd"] == 17
        assert parsed["cp"] == "C"
        assert parsed["strike"] == 200.0

    def test_put_with_fractional_strike(self):
        # SPX-style fractional strike: 4252500 thousandths = $4252.50.
        parsed = parse_occ("SPX 260619P04252500")
        assert parsed["cp"] == "P"
        assert parsed["strike"] == pytest.approx(4252.5)

    def test_root_with_dot(self):
        # BRK.B and similar tickers are valid roots.
        parsed = parse_occ("BRK.B 260117C00500000")
        assert parsed is not None
        assert parsed["root"] == "BRK.B"

    def test_plain_equity_ticker_returns_none(self):
        # Falls back to None so callers can render the ticker as-is.
        assert parse_occ("AAPL") is None
        assert parse_occ("PLTR") is None

    def test_empty_input_returns_none(self):
        assert parse_occ(None) is None
        assert parse_occ("") is None
        assert parse_occ("   ") is None

    def test_invalid_month_or_day_returns_none(self):
        # Guards against accidental mis-parse if a non-OCC string happens
        # to look numeric.
        assert parse_occ("FAKE 261301C00100000") is None  # month=13
        assert parse_occ("FAKE 260132C00100000") is None  # day=32


class TestFormatOptionSymbol:
    def test_pltr_call_regression(self):
        # User-reported readability complaint — pin the friendly form.
        assert format_option_symbol("PLTR 260424C00141000") == "PLTR Apr 24 '26 $141C"

    def test_drops_ticker_when_requested(self):
        # Useful when the page already has a Symbol column.
        assert (
            format_option_symbol("PLTR 260424C00141000", with_ticker=False)
            == "Apr 24 '26 $141C"
        )

    def test_put_contract(self):
        assert format_option_symbol("AAPL 260117P00150000") == "AAPL Jan 17 '26 $150P"

    def test_fractional_strike_keeps_two_decimals(self):
        # SPX $4252.50 — the ".50" must survive the format step.
        assert (
            format_option_symbol("SPX 260619P04252500")
            == "SPX Jun 19 '26 $4252.50P"
        )

    def test_plain_equity_ticker_passes_through(self):
        # Used as a Jinja filter on columns that may contain either an
        # OCC option string or a plain equity ticker — must not mangle
        # the latter.
        assert format_option_symbol("AAPL") == "AAPL"

    def test_blank_input_returns_empty_string(self):
        # Used in templates with `|option_symbol` after `if t.trade_symbol`,
        # but be defensive — never raise from a Jinja filter.
        assert format_option_symbol("") == ""
        assert format_option_symbol(None) == ""

    def test_unknown_garbage_passes_through_unchanged(self):
        # We'd rather show "TBD" than a stack trace.
        assert format_option_symbol("TBD") == "TBD"
        assert format_option_symbol("not an option") == "not an option"
