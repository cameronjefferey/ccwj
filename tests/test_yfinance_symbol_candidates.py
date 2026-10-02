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

from current_position_stock_price import (
    _load_crypto_symbols,
    _yahoo_symbol_candidates,
)


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


class TestCryptoMapping:
    """Crypto tickers map to ``<SYM>-USD`` EXCLUSIVELY.

    The bare ticker collides with an unrelated equity — ``yf.Ticker("LINK")``
    resolves to Interlink Electronics (~$4.55), not Chainlink
    (``LINK-USD`` ≈ $8.20). Because that bare fetch SUCCEEDS with the wrong
    asset's prices, the candidate list must never include the bare ticker
    for a crypto symbol, or the "first non-empty wins" loop would silently
    record the equity's series.
    """

    _CRYPTO = frozenset({"LINK", "BTC", "ETH"})

    def test_crypto_maps_to_usd_pair_exclusively(self):
        assert _yahoo_symbol_candidates("LINK", self._CRYPTO) == ["LINK-USD"]
        assert _yahoo_symbol_candidates("BTC", self._CRYPTO) == ["BTC-USD"]

    def test_crypto_never_includes_bare_ticker(self):
        cands = _yahoo_symbol_candidates("LINK", self._CRYPTO)
        assert "LINK" not in cands, (
            "bare crypto ticker collides with an equity and must not be a "
            "candidate — Yahoo's LINK is Interlink Electronics, not Chainlink"
        )

    def test_crypto_match_is_case_insensitive(self):
        assert _yahoo_symbol_candidates("link", self._CRYPTO) == ["link-USD"]

    def test_non_crypto_symbol_with_crypto_set_is_unaffected(self):
        assert _yahoo_symbol_candidates("AAPL", self._CRYPTO) == ["AAPL"]
        # Preferred-share fallback still applies for non-crypto symbols.
        cands = _yahoo_symbol_candidates("GLOP-C", self._CRYPTO)
        assert cands[0] == "GLOP-C"
        assert "GLOP-PC" in cands

    def test_default_crypto_set_loaded_from_seed(self):
        # With no explicit set, the whitelist is read from the dbt seed
        # (dbt/seeds/crypto_symbols.csv). LINK/BTC/ETH are canonical members.
        seed = _load_crypto_symbols()
        assert {"LINK", "BTC", "ETH"}.issubset(seed)
        assert _yahoo_symbol_candidates("LINK") == ["LINK-USD"]
        # SNX / SEI / COMP are listed stocks on the bare ticker. Mapping
        # them to SYM-USD marks TD SYNNEX with the Synthetix token.
        assert "SNX" in seed
        assert _yahoo_symbol_candidates("SNX") == ["SNX"]
        assert _yahoo_symbol_candidates("SEI") == ["SEI"]
        assert _yahoo_symbol_candidates("COMP") == ["COMP"]

    def test_load_crypto_symbols_missing_file_returns_empty(self, tmp_path):
        missing = tmp_path / "does_not_exist.csv"
        assert _load_crypto_symbols(str(missing)) == frozenset()


class TestIndexCloses:
    """Cash-settled index options have no Yahoo equity ticker.

    SPXW Oct 1 2026 7650/7655C was booked as a worthless win because the
    loader never wrote an SPX close. ^GSPC (and ^SPX) closed at 7666.45
    that day, above 7655.
    """

    def test_spx_and_spxw_use_the_gspc_index(self):
        assert _yahoo_symbol_candidates("SPX") == ["^GSPC", "^SPX"]
        assert _yahoo_symbol_candidates("SPXW") == ["^GSPC", "^SPX"]
        assert "SPXW" not in _yahoo_symbol_candidates("SPXW")
        assert "SPX" not in _yahoo_symbol_candidates("SPX")

    def test_xsp_uses_the_same_index(self):
        assert _yahoo_symbol_candidates("XSP") == ["^GSPC", "^SPX"]

    def test_other_cash_indexes(self):
        assert _yahoo_symbol_candidates("NDX") == ["^NDX"]
        assert _yahoo_symbol_candidates("NDXP") == ["^NDX"]
        assert _yahoo_symbol_candidates("RUT") == ["^RUT"]
        assert _yahoo_symbol_candidates("RUTW") == ["^RUT"]
        assert _yahoo_symbol_candidates("VIX") == ["^VIX"]

    def test_xsp_close_is_one_tenth_of_gspc(self, monkeypatch):
        import pandas as pd

        from current_position_stock_price import _fetch_history_for_symbol

        gspc = pd.DataFrame(
            {"Close": [7666.45], "Dividends": [0.0]},
            index=pd.to_datetime(["2026-10-01"]),
        )

        def fake_ticker(sym):
            assert sym == "^GSPC"
            return _FakeTicker(gspc)

        monkeypatch.setattr("current_position_stock_price.yf.Ticker", fake_ticker)
        hist, _, yahoo_sym = _fetch_history_for_symbol("XSP", "2026-10-01", "2026-10-02")
        assert yahoo_sym == "^GSPC"
        assert abs(float(hist["Close"].iloc[0]) - 766.645) < 1e-6

    def test_spxw_keeps_the_index_close(self, monkeypatch):
        import pandas as pd

        from current_position_stock_price import _fetch_history_for_symbol

        gspc = pd.DataFrame(
            {"Close": [7666.45], "Dividends": [0.0]},
            index=pd.to_datetime(["2026-10-01"]),
        )
        calls = []

        def fake_ticker(sym):
            calls.append(sym)
            if sym == "^GSPC":
                return _FakeTicker(gspc)
            raise AssertionError(f"bare root must not be fetched, got {sym}")

        monkeypatch.setattr("current_position_stock_price.yf.Ticker", fake_ticker)
        hist, _, yahoo_sym = _fetch_history_for_symbol(
            "SPXW", "2026-09-01", "2026-10-02"
        )
        assert calls == ["^GSPC"]
        assert yahoo_sym == "^GSPC"
        assert float(hist["Close"].iloc[0]) > 7655


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

    def test_crypto_fetches_usd_pair_and_never_bare_ticker(self, monkeypatch):
        # The regression this guards: the bare "LINK" fetch SUCCEEDS (Yahoo
        # returns Interlink Electronics), so the loader must query "LINK-USD"
        # and must never fall through to the bare ticker.
        import pandas as pd

        from current_position_stock_price import _fetch_history_for_symbol

        chainlink = pd.DataFrame(
            {"Close": [8.1, 8.2], "Dividends": [0.0, 0.0]},
            index=pd.to_datetime(["2026-08-04", "2026-08-05"]),
        )

        calls = []

        def fake_ticker(sym):
            calls.append(sym)
            if sym == "LINK-USD":
                return _FakeTicker(chainlink)
            # A bare "LINK" would (wrongly) return the equity — assert we
            # never reach it.
            raise AssertionError(f"crypto loader queried non-USD symbol {sym!r}")

        monkeypatch.setattr("current_position_stock_price.yf.Ticker", fake_ticker)

        hist, _, yahoo_sym = _fetch_history_for_symbol(
            "LINK", "2026-05-14", "2026-08-06", frozenset({"LINK"})
        )
        assert yahoo_sym == "LINK-USD"
        assert calls == ["LINK-USD"], "must query only the crypto USD pair"
        assert hist is not None and not hist.empty
