from scripts import refresh_symbol_metadata as metadata


class FakeFundsData:
    def __init__(self, sector_weightings=None, fund_overview=None):
        self.sector_weightings = sector_weightings or {}
        self.fund_overview = fund_overview or {}


class FakeTicker:
    def __init__(self, info, funds_data=None):
        self.ticker = info.get("symbol", "FAKE")
        self.info = info
        self.funds_data = funds_data


class NoFundTicker:
    ticker = "FAKE"

    def __init__(self, info):
        self.info = info

    @property
    def funds_data(self):
        raise RuntimeError("No Fund data found")


def test_fetch_one_uses_yfinance_fund_sector_weightings_for_etfs():
    ticker = FakeTicker(
        {
            "symbol": "SPY",
            "quoteType": "ETF",
            "longName": "SPDR S&P 500 ETF Trust",
        },
        FakeFundsData(
            sector_weightings={
                "technology": 0.32,
                "healthcare": 0.11,
                "financial_services": 0.14,
            },
            fund_overview={"categoryName": "Large Blend", "legalType": "ETF"},
        ),
    )

    row = metadata._fetch_one("SPY", ticker_factory=lambda _: ticker)

    assert row["sector"] == "Technology"
    assert row["subsector"] == "Large Blend"
    assert row["long_name"] == "SPDR S&P 500 ETF Trust"


def test_fetch_one_preserves_company_sector_before_fund_fallback():
    ticker = FakeTicker(
        {
            "symbol": "AAPL",
            "sector": "Technology",
            "industryDisp": "Consumer Electronics",
            "longName": "Apple Inc.",
        },
        FakeFundsData(
            sector_weightings={"healthcare": 1.0},
            fund_overview={"categoryName": "Health"},
        ),
    )

    row = metadata._fetch_one("AAPL", ticker_factory=lambda _: ticker)

    assert row["sector"] == "Technology"
    assert row["subsector"] == "Consumer Electronics"


def test_fetch_one_keeps_unknown_when_yfinance_has_no_sector_metadata():
    ticker = NoFundTicker(
        {
            "symbol": "DELISTED",
            "longName": "Missing Metadata",
        }
    )

    row = metadata._fetch_one("DELISTED", ticker_factory=lambda _: ticker)

    assert row["sector"] == "Unknown"
    assert row["subsector"] == "Unknown"


def test_dominant_fund_sector_normalizes_yfinance_keys():
    assert (
        metadata._dominant_fund_sector(
            {
                "realestate": {"raw": 0.21},
                "consumer_cyclical": 0.2,
            }
        )
        == "Real Estate"
    )
