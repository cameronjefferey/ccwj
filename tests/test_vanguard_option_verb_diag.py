"""Vanguard Buy/Sell profile is select-only, and the categories are pinned.

Synthetic fills stand in for warehouse rows. No account id, ticker, or
dollar amount is printed.
"""
from scripts.audit.check12_gap import _LINE, assert_select_only, emit
from scripts.audit.vanguard_option_verb_diag import (
    ALL_KINDS,
    _blank,
    bucket,
    price_flags,
    queries,
)

# Same order as the ``kind`` case. Earlier rows win.
_CASE_ORDER = (
    "closed_qty_zero",
    "undated",
    "same_day_open",
    "no_verb_before_close",
    "real_open_round_trip",
    "real_open_x100",
    "real_open_x1",
    "real_open_mixed",
    "share_delivery",
    "before_price_other",
    "before_qty_mismatch",
)


def test_every_statement_is_a_select():
    names = []
    verbs = ""
    for name, sql in queries():
        names.append(name)
        assert_select_only(sql)
        low = sql.lower()
        assert "insert " not in low
        assert "update " not in low
        assert "delete " not in low
        if name == "verbs":
            verbs = sql
    assert names == ["population", "verbs", "book"]
    for kind in ALL_KINDS:
        assert f"'{kind}'" in verbs
    case = verbs[verbs.find("end as kind") - 2500:verbs.find("end as kind")]
    pos = 0
    for kind in _CASE_ORDER:
        found = case.find(f"'{kind}'")
        assert found > pos, kind
        pos = found


def test_emit_drops_account_ids(capsys):
    emit("example_rows", 12)
    emit("bad", "snaptrade:0506672b-aaaa-bbbb-cccc-ddddeeeeffff")
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0] == "example_rows=12"
    assert all(_LINE.match(line) for line in out)
    assert all("snaptrade" not in line.lower() for line in out)


def test_per_share_premium_is_the_hundred_multiplier():
    # 1 contract, 1.25 per share, cash 125. Per-share option premium.
    assert price_flags(1, 1.25, 125) == (True, False)
    # Fee inside the per-contract budget still counts as that premium.
    assert price_flags(2, 3.0, 603) == (True, False)
    # Five past the budget is neither identity.
    assert price_flags(1, 1.25, 130) == (False, False)


def test_per_contract_price_is_cash_equals_quantity_times_price():
    # Price is already the contract premium, cash equals price × contracts.
    assert price_flags(1, 125, 125) == (False, True)
    assert price_flags(1, 0, 125) == (False, False)
    assert price_flags(0, 1.25, 125) == (False, False)


def test_share_sized_equity_fill_is_not_an_option_open():
    stats = _blank(
        contracts_closed=1,
        eq_buy_before=100,
        qty_before=100,
        opt_buy_before=0,
    )
    assert bucket(stats) == "share_delivery"


def test_buy_before_close_with_per_share_premium_is_a_real_open():
    stats = _blank(opt_buy_before=1, qty_before=1)
    assert bucket(stats) == "real_open_x100"


def test_sell_before_close_with_per_contract_price_is_a_real_open():
    stats = _blank(eq_sell_before=1, qty_before=1)
    assert bucket(stats) == "real_open_x1"


def test_both_sides_matching_before_the_close_are_a_round_trip():
    stats = _blank(
        n_verb=2,
        opt_sell_before=1,
        opt_buy_before=1,
        qty_before=2,
    )
    assert bucket(stats) == "real_open_round_trip"


def test_verb_only_on_the_close_date_is_not_an_earlier_open():
    stats = _blank(
        n_before=0,
        n_same=1,
        opt_qty_same=1,
        qty_before=0,
        n_proper_close=1,
    )
    assert bucket(stats) == "same_day_open"
    stats["n_proper_close"] = 0
    assert bucket(stats) == "no_verb_before_close"


def test_verb_after_the_close_is_pre_history():
    stats = _blank(
        n_before=0,
        n_same=0,
        n_after=1,
        n_verb_dated=1,
    )
    assert bucket(stats) == "no_verb_before_close"


def test_matching_quantity_without_either_price_identity_stays_unclassified():
    stats = _blank(qty_before=1)
    assert bucket(stats) == "before_price_other"


def test_option_premium_with_the_wrong_quantity_is_not_a_full_open():
    stats = _blank(opt_sell_before=2, qty_before=2, contracts_closed=1)
    assert bucket(stats) == "before_qty_mismatch"
