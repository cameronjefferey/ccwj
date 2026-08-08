"""Coverage guard for the yfinance price loader.

WHY: yfinance is an unofficial API that fails in bulk when Yahoo changes
something. Pre-guard, every per-symbol failure was swallowed with a
print(), the loader WRITE_TRUNCATE'd `daily_position_performance` with
whatever survived, and a feed outage silently zeroed dividends, charts,
and close-based pricing for every user. The guard refuses to publish a
gutted table and exits non-zero so the workflow run goes red.
"""

from current_position_stock_price import _coverage_guard


def test_healthy_run_does_not_trip():
    tripped, msg = _coverage_guard({"AAPL", "SPY", "QQQ", "JEPI"}, set())
    assert not tripped
    assert "coverage=100.0%" in msg


def test_a_few_delisted_symbols_stay_green():
    ok = {f"S{i}" for i in range(18)} | {"SPY", "QQQ"}
    failed = {"DELISTED1", "DELISTED2"}  # 2/22 ≈ 9% failure
    tripped, _ = _coverage_guard(ok, failed)
    assert not tripped


def test_mass_failure_trips():
    ok = {"SPY", "QQQ"}
    failed = {f"S{i}" for i in range(10)}
    tripped, msg = _coverage_guard(ok, failed)
    assert tripped
    assert "PRICES_GUARD_TRIPPED" in msg


def test_spy_failure_trips_even_with_high_coverage():
    ok = {f"S{i}" for i in range(50)}
    tripped, msg = _coverage_guard(ok, {"SPY"})
    assert tripped
    assert "SPY benchmark fetch failed" in msg


def test_symbol_ok_for_any_tenant_counts_as_ok():
    # Same symbol can fail for one (account, user) pair and succeed for
    # another (different position_open_date windows); success wins.
    tripped, msg = _coverage_guard({"AAPL"}, {"AAPL"})
    assert not tripped
    assert "failed=0" in msg


def test_empty_run_reports_without_tripping():
    # Nothing to fetch (fresh deploy, no positions) — nothing to guard.
    tripped, _ = _coverage_guard(set(), set())
    assert not tripped
