"""Tracker lots must fold when the only overlap opened before the window.

Warehouse run 38501719134 (d9e8893) failed
``tracker_lot_folds_into_option_strategy`` with 3 rows. Compact OCC
roots started equaling the stock, so a <25-share lot overlapped option
contracts the strategy-tag CTE drops (``opened_before_history``: a close
with no opening fill, no cost basis). The fold had counted only that
filtered set, and the lots stayed Buy and Hold.

The singular test is the warehouse check. This pins the SQL so a later
edit cannot point the fold back at the filtered CTE without CI failing,
and mirrors the label the fold emits.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLASSIFICATION = (
    ROOT / "dbt/models/intermediate/int_strategy_classification.sql"
).read_text()
TRACKER_TEST = (
    ROOT / "dbt/tests/tracker_lot_folds_into_option_strategy.sql"
).read_text()


def _cte(sql: str, name: str) -> str:
    start = sql.index(f"{name} as (")
    rest = sql[start + len(f"{name} as (") :]
    # These CTEs are not nested. A middle CTE closes with "),";
    # the last CTE in the warehouse test closes with ")" before select.
    ends = [i for i in (rest.find("\n),\n"), rest.find("\n)\n")) if i != -1]
    return rest[: min(ends)]


def tracker_fold_label(*, max_quantity_held, overlapping_contracts, dominant_strategy):
    """Mirror the <25-share fold in equity_classified.

    ``overlapping_contracts`` is every int_option_contracts row whose
    open falls in the session, including opened_before_history.
    ``dominant_strategy`` is the tagged option strategy only (pre-history
    closes are not tagged). No overlap stays Buy and Hold. An overlap
    with no tagged strategy uses the SQL fallback, Long Call.
    """
    if max_quantity_held < 25 and len(overlapping_contracts) > 0:
        return dominant_strategy or "Long Call"
    return "Buy and Hold"


def test_fold_counts_contracts_the_strategy_tag_set_drops():
    tagged = _cte(CLASSIFICATION, "option_contracts")
    assert "where not coalesce(opened_before_history, false)" in tagged

    overlap = _cte(CLASSIFICATION, "session_any_option")
    assert "ref('int_option_contracts')" in overlap
    assert "opened_before_history" not in overlap
    assert "option_contracts oc" not in overlap
    assert "count(distinct oc.trade_symbol) as num_option_contracts" in overlap

    fold = CLASSIFICATION.split("coalesce(e.max_quantity_held, 0) < 25", 1)[1]
    fold = fold.split("else 'Buy and Hold'", 1)[0]
    assert "sao.num_option_contracts" in fold
    assert "eos.num_option_contracts" not in fold
    assert "coalesce(dos.dominant_strategy, 'Long Call')" in fold

    # Whitelist Crypto yields to the same unfiltered overlap, so a
    # colliding ticker with only a pre-history option is not Crypto.
    crypto = CLASSIFICATION.split("then 'Crypto'", 1)[0]
    assert "coalesce(sao.num_option_contracts, 0) = 0" in crypto


def test_warehouse_test_overlaps_the_unfiltered_contract_set():
    overlap = _cte(TRACKER_TEST, "overlapping_options")
    assert "ref('int_option_contracts')" in overlap
    assert "opened_before_history" not in overlap
    assert "Buy and Hold" in TRACKER_TEST
    assert "Crypto" in TRACKER_TEST


def test_pre_history_overlap_folds_a_tracker_lot():
    pre_history = [{"opened_before_history": True, "trade_symbol": "AAPL240119C00150000"}]
    assert tracker_fold_label(
        max_quantity_held=1,
        overlapping_contracts=pre_history,
        dominant_strategy=None,
    ) == "Long Call"
    assert tracker_fold_label(
        max_quantity_held=8,
        overlapping_contracts=pre_history,
        dominant_strategy="Cash-Secured Put",
    ) == "Cash-Secured Put"


def test_tracker_without_options_stays_buy_and_hold():
    assert tracker_fold_label(
        max_quantity_held=1,
        overlapping_contracts=[],
        dominant_strategy=None,
    ) == "Buy and Hold"
    assert tracker_fold_label(
        max_quantity_held=24.9,
        overlapping_contracts=[],
        dominant_strategy="Long Call",
    ) == "Buy and Hold"


def test_twenty_five_shares_is_not_a_tracker_fold():
    assert tracker_fold_label(
        max_quantity_held=25,
        overlapping_contracts=[{"opened_before_history": True}],
        dominant_strategy="Long Call",
    ) == "Buy and Hold"
