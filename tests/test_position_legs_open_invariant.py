"""One Open chapter per symbol: the leg walk must use a single total order.

Same-day open contracts share (open_date, close_date, source). If the
running-max window and the leg-sequence window break that tie
independently, one contract is flagged as a new leg and another is
summed into the previous leg. Both stay Open.
position_legs_at_most_one_open_per_symbol failed the 2026-10-13
warehouse build (run 38394089932) on that split.
"""

from pathlib import Path

LEGS = Path("dbt/models/intermediate/int_position_legs.sql").read_text()


def _window_order(sql, marker):
    """Return the ORDER BY line of the window that starts at `marker`."""
    start = sql.index(marker)
    over = sql.index("over (", start)
    order = sql.index("order by", over)
    return sql[order:sql.index("\n", order)].strip()


def test_leg_windows_share_one_total_order():
    intervals = LEGS.split("all_intervals as (", 1)[1].split("ordered as (", 1)[0]
    assert "interval_key," in intervals
    walk = LEGS.split("ordered as (", 1)[1]
    assert "interval_key" in _window_order(walk, "row_number()")
    # The date key is not unique for a same-day spread. Both walks follow rn.
    assert _window_order(walk, "max(close_date)") == "order by rn"
    assert _window_order(walk, "sum(is_new_leg)") == "order by rn"


def _open_legs(flag_order, sum_order):
    """Break flags along flag_order, cumulative leg id along sum_order.

    Matching orders are one walk (the fix). Different orders are the
    BigQuery tie split that produced two Open legs.
    """
    prev_max = None
    flags = {}
    for row in flag_order:
        flags[row["key"]] = (
            1 if prev_max is None or row["open"] > prev_max else 0
        )
        prev_max = row["close"] if prev_max is None else max(prev_max, row["close"])
    seq = 0
    open_legs = set()
    for row in sum_order:
        seq += flags[row["key"]]
        if row["is_open"]:
            open_legs.add(seq)
    return open_legs


def test_same_day_open_spread_stays_one_leg_when_the_walk_agrees():
    # January chapter is closed. June call and put opened together and
    # are both still open: same dates, same source, different OSI keys.
    closed = {"key": "eq", "open": "2026-01-02", "close": "2026-01-20", "is_open": False}
    call = {"key": "call", "open": "2026-06-01", "close": "2026-10-13", "is_open": True}
    put = {"key": "put", "open": "2026-06-01", "close": "2026-10-13", "is_open": True}

    # Running max sees the call first (new leg, flag 1). The sequence
    # window sums the put first (flag 0), so the put stays on the January
    # leg and the call opens a second one.
    assert _open_legs(
        flag_order=[closed, call, put],
        sum_order=[closed, put, call],
    ) == {1, 2}

    # One shared order keeps both open contracts on the June leg.
    shared = [closed, call, put]
    assert _open_legs(shared, shared) == {2}
