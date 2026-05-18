"""Tests for the Yahoo-symbol candidate translation in the yfinance loader.

Schwab's API normalises a preferred series like ``GLOP/PRC`` down to the
broker form ``GLOP-C``. Yahoo Finance uses ``GLOP-PC`` for the same
security (the ``P`` prefix marks it as a preferred class, vs a
dual-class common share like Berkshire's ``BRK-B``).

When the loader queries Yahoo with the broker form for a preferred-class
ticker the response is empty, which silently zeros out the synthetic
dividend stream in ``int_dividend_events.sql`` (``stg_daily_prices`` has
no rows so ``shares_held × div_per_share`` is 0). The user's
``/position/GLOP-C`` page rendered ``$0.00`` in dividends despite
receiving $2,809.76 in qualified dividends per the Schwab statement.

The fix lives in ``current_position_stock_price._yahoo_symbol_candidates``:
return the broker form first, then a ``<root>-P<class>`` fallback when
the broker form looks like a single-letter class suffix.
"""
from __future__ import annotations

import pytest

from current_position_stock_price import _yahoo_symbol_candidates


class TestPreferredFallback:
    """Symbols matching ``<root>-<single-letter>`` get a ``-P<letter>`` alt."""

    def test_glop_c_falls_back_to_glop_pc(self):
        # The canonical regression case (GLOP/PRC preferred series C):
        # Schwab → "GLOP-C"; Yahoo → "GLOP-PC".
        cands = _yahoo_symbol_candidates("GLOP-C")
        assert cands[0] == "GLOP-C"
        assert "GLOP-PC" in cands

    def test_brk_b_includes_brk_pb_fallback(self):
        # BRK-B works directly on Yahoo so the fallback is never hit at
        # runtime — but the candidate list still includes BRK-PB as a
        # cheap defensive alt. The loader only uses the alt if the
        # primary returns no rows, so producing this list is safe for
        # dual-class common shares too.
        cands = _yahoo_symbol_candidates("BRK-B")
        assert cands[0] == "BRK-B"
        assert "BRK-PB" in cands

    def test_already_preferred_form_does_not_double_prefix(self):
        # If a broker (e.g. SnapTrade) already ships the Yahoo-style
        # preferred form, don't append a second "P".
        cands = _yahoo_symbol_candidates("GLOP-PC")
        assert cands == ["GLOP-PC"]


class TestNonPreferred:
    """Plain tickers and option underlyings yield a single candidate."""

    def test_plain_equity_has_no_alt(self):
        assert _yahoo_symbol_candidates("AAPL") == ["AAPL"]

    def test_etf_has_no_alt(self):
        assert _yahoo_symbol_candidates("JEPI") == ["JEPI"]

    def test_two_char_suffix_has_no_alt(self):
        # The preferred-share pattern is ``<root>-<single letter>``. A
        # two-letter suffix isn't ambiguous (e.g. crypto/forex shapes)
        # so we don't add a fallback.
        assert _yahoo_symbol_candidates("FOO-BB") == ["FOO-BB"]


class TestEdgeCases:
    def test_empty_string_returns_empty(self):
        assert _yahoo_symbol_candidates("") == []

    def test_whitespace_stripped(self):
        assert _yahoo_symbol_candidates("  AAPL  ") == ["AAPL"]

    def test_none_returns_empty(self):
        assert _yahoo_symbol_candidates(None) == []

    def test_non_string_returns_empty(self):
        assert _yahoo_symbol_candidates(123) == []


# ---------------------------------------------------------------------------
# End-to-end fetch with mocked yfinance — exercises the candidate loop
# without touching the network. Pin the "preferred series returns empty
# under broker form → fallback to <root>-P<class> wins" behaviour so a
# refactor doesn't quietly drop the fallback and zero out dividends again.
# ---------------------------------------------------------------------------


class _FakeTicker:
    def __init__(self, hist_df):
        self._hist = hist_df

    def history(self, start, end):
        return self._hist


class TestFetchHistoryForSymbol:
    def test_preferred_fallback_wins_when_broker_form_empty(self, monkeypatch):
        import pandas as pd

        from current_position_stock_price import _fetch_history_for_symbol

        empty = pd.DataFrame()
        non_empty = pd.DataFrame(
            {"Close": [25.0, 25.1], "Dividends": [0.0, 0.6]},
            index=pd.to_datetime(["2024-08-01", "2024-09-09"]),
        )

        calls = []

        def fake_ticker(sym):
            calls.append(sym)
            if sym == "GLOP-C":
                return _FakeTicker(empty)
            if sym == "GLOP-PC":
                return _FakeTicker(non_empty)
            raise AssertionError(f"unexpected symbol {sym!r}")

        monkeypatch.setattr("current_position_stock_price.yf.Ticker", fake_ticker)

        hist, ticker, yahoo_sym = _fetch_history_for_symbol(
            "GLOP-C", "2024-07-31", "2026-05-18"
        )
        assert yahoo_sym == "GLOP-PC", (
            "fallback to Yahoo preferred form must be used when broker form "
            "returns empty history — otherwise synthetic dividends are zero"
        )
        assert hist is not None and not hist.empty
        assert calls == ["GLOP-C", "GLOP-PC"]

    def test_broker_form_wins_when_it_has_data(self, monkeypatch):
        # BRK-B is a dual-class common share that Yahoo serves directly
        # under the broker form. The fallback exists but must not be
        # consulted when the primary already has data.
        import pandas as pd

        from current_position_stock_price import _fetch_history_for_symbol

        non_empty = pd.DataFrame(
            {"Close": [400.0], "Dividends": [0.0]},
            index=pd.to_datetime(["2024-08-01"]),
        )

        calls = []

        def fake_ticker(sym):
            calls.append(sym)
            return _FakeTicker(non_empty)

        monkeypatch.setattr("current_position_stock_price.yf.Ticker", fake_ticker)

        hist, _, yahoo_sym = _fetch_history_for_symbol(
            "BRK-B", "2024-07-31", "2026-05-18"
        )
        assert yahoo_sym == "BRK-B"
        assert calls == ["BRK-B"], "must not query fallback when primary has rows"

    def test_both_empty_returns_none(self, monkeypatch):
        import pandas as pd

        from current_position_stock_price import _fetch_history_for_symbol

        empty = pd.DataFrame()

        def fake_ticker(sym):
            return _FakeTicker(empty)

        monkeypatch.setattr("current_position_stock_price.yf.Ticker", fake_ticker)

        hist, ticker, yahoo_sym = _fetch_history_for_symbol(
            "ZZZZ-Q", "2024-07-31", "2026-05-18"
        )
        assert hist is None
        assert ticker is None
        assert yahoo_sym is None
