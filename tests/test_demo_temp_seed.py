"""Temporary demo book: structure, tenancy, and a rising account.

The warehouse curve is ``int_demo_temp_equity_daily``, added on top of
the bot mirror. This test replays the seed with the same cash-sign
rules as ``stg_history`` and public closes on three dates, so CI can
see the climb without BigQuery.

The off switch is the ``DEMO_TEMP_SEED`` env var (GitHub Actions
variable on the warehouse job). Flag off compiles to the pure mirror.
Steps live in ``dbt/seeds/DEMO_TEMP_SEED.md``.
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HISTORY = ROOT / "dbt" / "seeds" / "demo_temp_history.csv"
CURRENT = ROOT / "dbt" / "seeds" / "demo_temp_current.csv"
REMOVAL = ROOT / "dbt" / "seeds" / "DEMO_TEMP_SEED.md"

# Public closes used to mark shares still held on the sample dates.
# The book buys AAPL and AMD on 2026-08-10, so earlier dates are cash.
CLOSES = {
    "AAPL": (
        (date(2026, 9, 4), 319.97),
        (date(2026, 10, 7), 336.67),
    ),
    "AMD": (
        (date(2026, 9, 4), 477.57),
        (date(2026, 10, 7), 645.86),
    ),
}

OPTION_ORDERS = {
    "sell to open",
    "buy to open",
    "sell to close",
    "buy to close",
}
OPEN_ACTIONS = {"sell to open", "buy to open"}
CLOSE_ACTIONS = {
    "buy to close",
    "sell to close",
    "expired",
    "assigned",
    "exchange or exercise",
}


def _num(raw: str) -> float:
    text = (raw or "").strip().replace(",", "").replace("$", "")
    if not text:
        return 0.0
    return float(text)


def _rows(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _signed_amount(row: dict) -> float:
    """Mirror ``amount_signed`` in ``stg_history`` for this seed's actions."""
    action = row["Action"].strip().lower()
    amount = _num(row["Amount"])
    qty = _num(row["Quantity"])
    price = _num(row["Price"])
    fees = _num(row["fees_and_comm"])
    if (
        action in OPTION_ORDERS
        and abs(fees) > 0.005
        and qty
        and price
        and abs(abs(amount) - abs(qty) * price * 100) <= 0.05
    ):
        gross = abs(qty) * price * 100
        if action in ("sell to open", "sell to close"):
            return gross - abs(fees)
        return -(gross + abs(fees))
    if action in ("buy", "buy to open", "buy to close"):
        return -abs(amount)
    if action in ("sell", "sell to open", "sell to close"):
        return abs(amount)
    return amount


def _underlying(symbol: str) -> str:
    return (symbol or "").split(" ")[0]


def _expiry(symbol: str) -> str:
    parts = (symbol or "").split(" ")
    return parts[1] if len(parts) > 1 else ""


def _strike(symbol: str) -> str:
    parts = (symbol or "").split(" ")
    return parts[2] if len(parts) > 2 else ""


def _close_on(symbol: str, day: date) -> float | None:
    best = None
    for stamped, px in CLOSES.get(symbol, ()):
        if stamped <= day:
            best = px
    return best


def _book_on(history: list[dict], day: date, snap: dict[str, float]) -> float:
    """Cash + share marks + option marks, using 2026-10-07 as 'today'."""
    today = date(2026, 10, 7)
    cash = 0.0
    qty: dict[str, float] = {}
    contracts: dict[str, dict] = {}
    for row in history:
        trade_date = date.fromisoformat(row["Date"])
        if trade_date > day:
            continue
        action = row["Action"].strip().lower()
        symbol = (row["Symbol"] or "").strip()
        cash += _signed_amount(row)
        if action == "buy":
            qty[symbol] = qty.get(symbol, 0.0) + _num(row["Quantity"])
        elif action == "sell":
            qty[symbol] = qty.get(symbol, 0.0) - _num(row["Quantity"])
        if not symbol or _expiry(symbol) == "":
            continue
        slot = contracts.setdefault(
            symbol,
            {"open": None, "close": None, "open_mark": 0.0, "sold": 0.0},
        )
        if action in OPEN_ACTIONS and slot["open"] is None:
            slot["open"] = trade_date
        if action in CLOSE_ACTIONS and slot["close"] is None:
            slot["close"] = trade_date
        signed = _signed_amount(row)
        if action == "sell to open":
            slot["open_mark"] -= signed
            slot["sold"] += _num(row["Quantity"])
        elif action == "buy to open":
            slot["open_mark"] -= signed

    equity = 0.0
    for symbol, held in qty.items():
        if held <= 0:
            continue
        px = _close_on(symbol, day)
        if px is None:
            continue
        equity += held * px

    options = 0.0
    for symbol, slot in contracts.items():
        opened = slot["open"]
        if opened is None or opened > day:
            continue
        closed = slot["close"]
        if closed is not None and day >= closed:
            continue
        if closed is not None and closed <= opened:
            continue
        if closed is not None:
            span = max((closed - opened).days, 1)
            frac = (day - opened).days / span
            options += slot["open_mark"] * (1 - frac)
        else:
            span = max((today - opened).days, 1)
            frac = (day - opened).days / span
            target = snap.get(symbol, 0.0)
            options += slot["open_mark"] + (target - slot["open_mark"]) * frac
    return cash + equity + options


def test_temp_seed_files_and_flag_are_the_only_switch():
    project = (ROOT / "dbt" / "dbt_project.yml").read_text()
    assert "env_var('DEMO_TEMP_SEED'" in project
    workflow = (ROOT / ".github/workflows/bigquery_update.yml").read_text()
    assert "DEMO_TEMP_SEED:" in workflow
    assert "vars.DEMO_TEMP_SEED" in workflow

    removal = REMOVAL.read_text()
    assert removal.startswith("# Temporary demo book\n\n## How to turn it off\n")
    turn_off, _, rest = removal.partition("## What this is")
    assert "DEMO_TEMP_SEED" in turn_off
    assert "`false`" in turn_off
    assert "bigquery_update.yml" in turn_off
    for name in (
        "demo_temp_history.csv",
        "demo_temp_current.csv",
        "DEMO_TEMP_SEED.md",
        "int_demo_temp_equity_daily.sql",
        "demo_temp_equity_climbs.sql",
        "demo_temp_off_equals_mirror.sql",
        "demo_temp_history_keeps_mirror.sql",
        "test_demo_temp_seed.py",
        "demo_temp_seed.yml",
        "demo_temp_seed: false",
    ):
        assert name in rest

    history_sql = (ROOT / "dbt/models/staging/demo/stg_demo_history.sql").read_text()
    assert "ref('demo_temp_history')" in history_sql
    assert "demo:demo-account" in history_sql
    assert "ref('stg_broker_alpaca_history')" in history_sql
    assert "select * from mirror\nunion all\nselect * from seed" in history_sql
    assert "{% else %}\n\nselect * from mirror\n\n{% endif %}" in history_sql

    equity_sql = (ROOT / "dbt/models/intermediate/int_demo_temp_equity_daily.sql").read_text()
    assert "ref('demo_temp_history')" in equity_sql
    assert "ref('stg_history')" not in equity_sql
    assert "enabled=demo_temp_seed_on()" in equity_sql

    mart_sql = (ROOT / "dbt/models/marts/mart_account_equity_daily.sql").read_text()
    assert "ref('int_demo_temp_equity_daily')" in mart_sql
    assert "demo_temp_seed_on()" in mart_sql

    off_test = (ROOT / "dbt/tests/demo_temp_off_equals_mirror.sql").read_text()
    assert "enabled=not demo_temp_seed_on()" in off_test
    assert "except distinct" in off_test
    keeps = (ROOT / "dbt/tests/demo_temp_history_keeps_mirror.sql").read_text()
    assert "enabled=demo_temp_seed_on()" in keeps
    assert "mirror_n + seed_n" in keeps

    # The seed is not a second ingestion path in the app.
    app_hits = [
        p.name
        for p in (ROOT / "app").rglob("*.py")
        if "demo_temp_history" in p.read_text()
    ]
    assert app_hits == []


def test_seed_fill_keys_do_not_collide_with_themselves():
    keys = []
    for row in _rows(HISTORY):
        price = _num(row["Price"])
        keys.append((
            row["Date"],
            row["Action"].strip().lower(),
            (row["Symbol"] or "").strip().upper(),
            row["Quantity"].strip(),
            round(price, 4),
        ))
    assert len(keys) == len(set(keys))


def _parse_manifest(flag: str) -> dict:
    dbt = shutil.which("dbt")
    cmd = [dbt] if dbt else ["python3", "-m", "dbt.cli.main"]
    target = f"/tmp/dbt-demo-seed-{flag}"
    env = os.environ.copy()
    env["DEMO_TEMP_SEED"] = flag
    subprocess.run(
        [
            *cmd,
            "parse",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            target,
        ],
        check=True,
        env=env,
        cwd=ROOT / "dbt",
    )
    return json.loads((Path(target) / "manifest.json").read_text())


def _node(manifest: dict, unique_id: str) -> dict:
    if unique_id in manifest["nodes"]:
        return manifest["nodes"][unique_id]
    disabled = (manifest.get("disabled") or {}).get(unique_id) or []
    assert disabled, unique_id
    return disabled[0]


def _deps(manifest: dict, unique_id: str) -> set[str]:
    return set(_node(manifest, unique_id)["depends_on"]["nodes"])


def test_flag_off_graph_is_the_pure_mirror_and_flag_on_adds_the_seed():
    off = _parse_manifest("false")
    on = _parse_manifest("true")

    history = "model.ccwj.stg_demo_history"
    current = "model.ccwj.stg_demo_current"
    balances = "model.ccwj.stg_demo_balances"
    equity = "model.ccwj.int_demo_temp_equity_daily"
    mart = "model.ccwj.mart_account_equity_daily"
    seed_hist = "seed.ccwj.demo_temp_history"
    seed_cur = "seed.ccwj.demo_temp_current"
    alpaca_hist = "model.ccwj.stg_broker_alpaca_history"

    assert _node(off, equity)["config"]["enabled"] is False
    assert _node(on, equity)["config"]["enabled"] is True
    off_eq = "test.ccwj.demo_temp_off_equals_mirror"
    keeps_id = "test.ccwj.demo_temp_history_keeps_mirror"
    assert _node(off, off_eq)["config"]["enabled"] is True
    assert _node(on, off_eq)["config"]["enabled"] is False
    assert _node(off, keeps_id)["config"]["enabled"] is False
    assert _node(on, keeps_id)["config"]["enabled"] is True

    assert alpaca_hist in _deps(off, history)
    assert seed_hist not in _deps(off, history)
    assert seed_hist in _deps(on, history)
    assert alpaca_hist in _deps(on, history)

    assert seed_cur not in _deps(off, current)
    assert seed_cur in _deps(on, current)
    assert seed_hist not in _deps(off, balances)
    assert seed_cur not in _deps(off, balances)
    assert seed_hist in _deps(on, balances)

    assert equity not in _deps(off, mart)
    assert equity in _deps(on, mart)
    assert "model.ccwj.stg_history" not in _deps(on, equity)
    assert seed_hist in _deps(on, equity)
    assert seed_cur in _deps(on, equity)


def test_seed_is_demo_only_and_groups_as_several_strategies():
    history = _rows(HISTORY)
    assert history
    header = set(history[0])
    assert "tenant_id" not in header
    blob = HISTORY.read_text() + CURRENT.read_text()
    assert "snaptrade:" not in blob
    assert "Schwab" not in blob

    deposits = [r for r in history if r["Action"] == "Deposit"]
    assert len(deposits) == 1
    assert _num(deposits[0]["Amount"]) == 100000

    def opened(action: str, underlying: str):
        return [
            r
            for r in history
            if r["Action"].strip().lower() == action
            and _underlying(r["Symbol"]) == underlying
        ]

    aapl_buys = opened("buy", "AAPL")
    assert sum(_num(r["Quantity"]) for r in aapl_buys) >= 100
    assert opened("buy to open", "AAPL") == []
    first_short = min(
        date.fromisoformat(r["Date"]) for r in opened("sell to open", "AAPL")
    )
    assert date.fromisoformat(aapl_buys[0]["Date"]) <= first_short

    ko_assign = [
        r
        for r in history
        if r["Action"] == "Assigned" and r["Symbol"].endswith(" P")
    ]
    assert ko_assign
    assert any(
        r["Action"] == "Buy" and r["Symbol"] == "KO" and r["Date"] == ko_assign[0]["Date"]
        for r in history
    )
    assert any(
        r["Action"] == "Assigned" and r["Symbol"].endswith(" C") and r["Symbol"].startswith("KO")
        for r in history
    )
    assert any(r["Action"] == "Sell" and r["Symbol"] == "KO" for r in history)
    assert opened("buy", "MSFT") == []
    assert opened("buy", "SPY") == []

    pairs: dict[tuple, set] = {}
    for row in history:
        action = row["Action"].strip().lower()
        if action not in OPEN_ACTIONS:
            continue
        key = (_underlying(row["Symbol"]), _expiry(row["Symbol"]), row["Date"])
        pairs.setdefault(key, set()).add((action, _strike(row["Symbol"])))
    spreads = [
        legs
        for legs in pairs.values()
        if {leg[0] for leg in legs} == OPEN_ACTIONS
        and len({leg[1] for leg in legs}) >= 2
    ]
    assert len(spreads) >= 3

    current = _rows(CURRENT)
    current_symbols = {r["Symbol"] for r in current}
    still_open: set[str] = set()
    closed: set[str] = set()
    share_qty: dict[str, float] = {}
    for row in history:
        symbol = (row["Symbol"] or "").strip()
        action = row["Action"].strip().lower()
        if action == "buy" and symbol:
            share_qty[symbol] = share_qty.get(symbol, 0.0) + _num(row["Quantity"])
        elif action == "sell" and symbol:
            share_qty[symbol] = share_qty.get(symbol, 0.0) - _num(row["Quantity"])
        if action in OPEN_ACTIONS and symbol:
            still_open.add(symbol)
        if action in CLOSE_ACTIONS and symbol:
            closed.add(symbol)
            still_open.discard(symbol)
    held = {symbol for symbol, qty in share_qty.items() if qty > 0}
    # Current positions are the shares still held plus contracts not closed.
    assert still_open | held == current_symbols
    assert "AAPL" in held
    assert closed.isdisjoint(current_symbols)


def test_demo_account_equity_climbs_across_the_seed():
    """The deposit sits until August, then the book climbs into October.

    Share buys and the bulk of the option credits are dated on or after
    2026-08-10, which is after the demo broker connect date (2026-08-08).
    A May snapshot of the old book is the wrong shape for this seed.
    """
    history = _rows(HISTORY)
    snap = {
        r["Symbol"]: _num(r["market_value"])
        for r in _rows(CURRENT)
        if _expiry(r["Symbol"])
    }
    aug = _book_on(history, date(2026, 8, 7), snap)
    sep = _book_on(history, date(2026, 9, 4), snap)
    oct_ = _book_on(history, date(2026, 10, 7), snap)
    assert aug == aug  # not NaN
    # Premium from the small pre-August spreads, before the share buys.
    assert aug > 104_000
    # August premium plus the first leg of the AMD/AAPL mark.
    assert sep > aug + 12_000
    # September's climb is the large one, and October finishes higher.
    assert oct_ > sep + 20_000
    assert oct_ > 160_000
