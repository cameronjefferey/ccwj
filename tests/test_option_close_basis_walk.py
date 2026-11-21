"""Option basis walk stays nestable and matches the unit-test fixtures.

dbt wraps model SQL in parentheses. BigQuery rejects a self-referential
WITH inside that wrapper, which failed the price-dependent warehouse
build (run 44836506035). The walk is unrolled. These checks lock the
arithmetic to the fixture rows in the dbt unit tests.
"""

from pathlib import Path

import yaml
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "dbt/models/intermediate/int_option_contract_daily_pnl.sql"
MACRO = ROOT / "dbt/macros/option_close_basis_walk.sql"
DEPTH = ROOT / "dbt/tests/option_close_basis_walk_within_depth.sql"
PROJECT = ROOT / "dbt/dbt_project.yml"
UNIT_YMLS = [
    ROOT / "dbt/models/intermediate/int_option_contract_daily_pnl.yml",
    ROOT / "dbt/models/intermediate/int_option_contract_daily_pnl_basis.yml",
]


def _strip_comments(sql: str) -> str:
    out = []
    i = 0
    while i < len(sql):
        if sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            i = len(sql) if end < 0 else end + 2
            continue
        if sql.startswith("--", i):
            end = sql.find("\n", i)
            i = len(sql) if end < 0 else end
            continue
        if sql.startswith("{#", i):
            end = sql.find("#}", i + 2)
            i = len(sql) if end < 0 else end + 2
            continue
        out.append(sql[i])
        i += 1
    return "".join(out)


def test_basis_walk_is_not_a_recursive_cte():
    model = _strip_comments(MODEL.read_text()).lower()
    macro = _strip_comments(MACRO.read_text()).lower()
    assert "with recursive" not in model
    assert "recursive" not in macro
    assert "option_close_basis_walk()" in MODEL.read_text()


def test_unrolled_walk_renders_one_step_per_close_date():
    project = yaml.safe_load(PROJECT.read_text())
    steps = int(project["vars"]["option_close_basis_max_steps"])
    assert steps >= 32
    env = Environment()
    env.globals["var"] = lambda name: steps
    env.globals["range"] = range
    rendered = env.from_string(
        MACRO.read_text() + "\n{{ option_close_basis_walk() }}"
    ).render()
    assert "close_basis_step_1 as (" in rendered
    assert f"close_basis_step_{steps} as (" in rendered
    assert f"close_basis_step_{steps + 1}" not in rendered
    assert "from close_basis_walk walk" not in rendered
    assert f"select * from close_basis_step_{steps}" in rendered
    depth = DEPTH.read_text()
    assert "option_close_basis_max_steps" in depth
    assert "int_option_contract_daily_pnl" in depth


def _ratio(closed, qty):
    if not qty:
        return 0.0
    return min(closed / qty, 1.0)


def _seed(event):
    ratio = _ratio(event["closed_qty"], event["opened_qty_to_date"])
    return {
        **event,
        "opened_qty_seen": event["opened_qty_to_date"],
        "opening_cash_seen": event["opening_cash_to_date"],
        "remaining_open_qty": max(event["opened_qty_to_date"] - event["closed_qty"], 0.0),
        "remaining_opening_cash": event["opening_cash_to_date"] * (1.0 - ratio),
        "allocated_opening_cash": event["opening_cash_to_date"] * ratio,
    }


def _step(prev, event):
    qty_pool = max(
        prev["remaining_open_qty"] + event["opened_qty_to_date"] - prev["opened_qty_seen"],
        0.0,
    )
    cash_pool = (
        prev["remaining_opening_cash"]
        + event["opening_cash_to_date"]
        - prev["opening_cash_seen"]
    )
    ratio = _ratio(event["closed_qty"], qty_pool)
    return {
        **event,
        "opened_qty_seen": event["opened_qty_to_date"],
        "opening_cash_seen": event["opening_cash_to_date"],
        "remaining_open_qty": max(qty_pool - event["closed_qty"], 0.0),
        "remaining_opening_cash": cash_pool * (1.0 - ratio),
        "allocated_opening_cash": cash_pool * ratio,
    }


def _close_date(row):
    trade_date = row["trade_date"]
    expiry = row.get("option_expiry") or trade_date
    return trade_date if trade_date <= expiry else expiry


def _realized_increments(contract, history):
    """Same close-state and basis walk as int_option_contract_daily_pnl."""
    key = (
        contract["tenant_id"],
        contract["account"],
        contract["user_id"],
        contract["trade_symbol"],
    )
    rows = [
        row for row in history
        if (row["tenant_id"], row["account"], row["user_id"], row["trade_symbol"]) == key
    ]
    opens = {}
    closes = {}
    for row in rows:
        qty = abs(row["quantity"])
        if row["action"] in ("option_sell_to_open", "option_buy_to_open"):
            bucket = opens.setdefault(row["trade_date"], [0.0, 0.0])
        elif row["action"] in (
            "option_buy_to_close",
            "option_sell_to_close",
            "option_expired",
            "option_assigned",
            "option_exercised",
        ):
            bucket = closes.setdefault(_close_date(row), [0.0, 0.0])
        else:
            continue
        bucket[0] += qty
        bucket[1] += row["amount"]

    close_dates = sorted(closes)
    terminal = contract.get("realized_close_date")
    if terminal and terminal not in closes:
        opened = (contract.get("contracts_sold_to_open") or 0) + (
            contract.get("contracts_bought_to_open") or 0
        )
        residual_qty = max(opened - (contract.get("contracts_closed") or 0), 0)
        closes[terminal] = [float(residual_qty), 0.0]
        close_dates = sorted(closes)
    if len(close_dates) <= 1 or not contract.get("realized_close_date"):
        if contract.get("realized_close_date"):
            return [(contract["realized_close_date"], contract["realized_pnl"])]
        return []

    cumulative = 0.0
    allocatable = []
    for close_date in close_dates:
        cumulative += closes[close_date][0]
        prior_qty = sum(qty for day, (qty, _cash) in opens.items() if day < close_date)
        prior_cash = sum(cash for day, (_qty, cash) in opens.items() if day < close_date)
        same_qty, same_cash = opens.get(close_date, (0.0, 0.0))
        if same_qty > 0 and abs(cumulative - prior_qty) <= 1e-9:
            opened_qty = prior_qty
            opening_cash = prior_cash
        else:
            opened_qty = prior_qty + same_qty
            opening_cash = prior_cash + same_cash
        if opened_qty > 0:
            allocatable.append({
                "date": close_date,
                "closed_qty": closes[close_date][0],
                "closing_cash": closes[close_date][1],
                "opened_qty_to_date": opened_qty,
                "opening_cash_to_date": opening_cash,
                "close_date_count": len(close_dates),
            })
    if not allocatable or allocatable[0]["close_date_count"] <= 1:
        return [(contract["realized_close_date"], contract["realized_pnl"])]

    walked = []
    prev = None
    for event in allocatable:
        prev = _seed(event) if prev is None else _step(prev, event)
        walked.append(prev)
    raw = [row["closing_cash"] + row["allocated_opening_cash"] for row in walked]
    raw_total = sum(raw)
    latest = max(row["date"] for row in walked)
    out = []
    for row, increment in zip(walked, raw):
        if row["date"] == latest:
            increment += contract["realized_pnl"] - raw_total
        out.append((row["date"], increment))
    return out


def test_walk_matches_unit_test_realized_rows():
    for path in UNIT_YMLS:
        doc = yaml.safe_load(path.read_text())
        for spec in doc["unit_tests"]:
            given = {item["input"]: item["rows"] for item in spec["given"]}
            contracts = given["ref('int_option_contracts')"]
            history = given["ref('stg_history')"]
            expected = [
                (row["trade_symbol"], str(row["date"]), row["pnl_today"])
                for row in spec["expect"]["rows"]
                if row["is_realized_close"]
            ]
            actual = []
            for contract in contracts:
                if str(contract.get("tenant_id", "")).startswith("tenant:unused"):
                    continue
                for close_date, pnl in _realized_increments(contract, history):
                    actual.append((contract["trade_symbol"], str(close_date), pnl))
            assert actual == expected, spec["name"]
