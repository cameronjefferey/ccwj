"""Empower staging is its own partition of the shared seed tables.

No warehouse credentials and no customer rows. These tests read the
dbt sources as text and check the routing contract:

* known_brokers() includes empower, in the same set the app uses
* the three base staging models and the parity test union the
  empower adapters before the catch-all
* the catch-all models stay one-line broker_*_rows calls
* the empower adapters project the seed columns and do not rewrite
  option actions or invent a cost
"""
from __future__ import annotations

import re
from pathlib import Path

from app.early_broker import MODELED_BROKER_KEYS

ROOT = Path(__file__).resolve().parents[1]


def _text(rel: str) -> str:
    return (ROOT / rel).read_text()


def test_known_brokers_includes_empower_and_matches_the_app():
    macro = _text("dbt/macros/broker_slug_from_account.sql")
    match = re.search(
        r"macro known_brokers\(\)[\s\S]*?return\(\[(.*?)\]\)",
        macro,
    )
    assert match, "known_brokers() return list not found"
    slugs = re.findall(r"'([a-z0-9_]+)'", match.group(1))
    assert slugs == [
        "schwab", "alpaca", "fidelity", "interactive", "vanguard", "empower",
    ]
    assert set(slugs) == MODELED_BROKER_KEYS


def test_base_staging_unions_include_empower_before_the_catch_all():
    expected = {
        "dbt/models/staging/stg_history.sql": "stg_broker_empower_history",
        "dbt/models/staging/stg_current.sql": "stg_broker_empower_current",
        "dbt/models/staging/stg_account_balances.sql": "stg_broker_empower_balances",
        "dbt/tests/broker_split_preserves_all_rows.sql": None,
    }
    for rel, model in expected.items():
        text = _text(rel)
        if model:
            assert f"ref('{model}')" in text
        else:
            for name in (
                "stg_broker_empower_history",
                "stg_broker_empower_current",
                "stg_broker_empower_balances",
            ):
                assert f"ref('{name}')" in text
        empower_at = text.find("empower")
        other_at = text.find("stg_broker_other_")
        assert empower_at != -1 and other_at != -1
        assert empower_at < other_at


def test_catch_all_models_exclude_known_brokers():
    for name in (
        "stg_broker_other_history.sql",
        "stg_broker_other_current.sql",
        "stg_broker_other_balances.sql",
    ):
        text = _text(f"dbt/models/staging/brokers/{name}")
        assert "is_catch_all=true" in text
        assert "empower" not in text.lower()


def test_empower_adapters_project_seed_columns_and_do_not_invent_rows():
    history = _text("dbt/models/staging/brokers/stg_broker_empower_history.sql")
    for col in (
        "Account", "user_id", "tenant_id", "Date", "Action", "Symbol",
        "Description", "Quantity", "Price", "fees_and_comm", "Amount",
    ):
        assert col in history
    assert "broker_history_rows('empower')" in history
    assert "option_buy_to_open" not in history
    assert "option_sell_to_close" not in history

    current = _text("dbt/models/staging/brokers/stg_broker_empower_current.sql")
    for col in (
        "tenant_id", "Symbol", "Quantity", "market_value", "cost_bases",
        "security_type",
    ):
        assert col in current
    assert "broker_current_rows('empower')" in current

    balances = _text("dbt/models/staging/brokers/stg_broker_empower_balances.sql")
    for col in ("tenant_id", "row_type", "market_value", "src_priority"):
        assert col in balances
    assert "broker_balances_rows('empower')" in balances


def test_empower_schema_tests_cover_nulls_actions_and_keys():
    yml = _text("dbt/models/staging/brokers/_stg_broker_empower.yml")
    assert "not_null" in yml
    assert "accepted_values" in yml
    assert "Cash Dividend" in yml
    assert "ADR Mgmt Fee" in yml
    assert "Deposit" in yml
    exclusive = _text("dbt/tests/empower_partition_is_exclusive.sql")
    assert "stg_broker_empower_history" in exclusive
    assert "stg_broker_other_history" in exclusive
    assert "stg_broker_vanguard_history" in exclusive
    symbol = _text("dbt/tests/empower_current_symbol_unique.sql")
    assert "tenant_id" in symbol and "symbol" in symbol
    priority = _text("dbt/tests/empower_balance_priority_unique.sql")
    assert "src_priority = 1" in priority


def test_empower_close_only_rule_does_not_infer_an_open():
    sql = _text("dbt/models/staging/stg_history.sql")
    first_verb = sql.split("vanguard_verb_ranked as (", 1)[1].split(
        "empower_open_guard as (", 1
    )[0]
    assert "empower" not in first_verb.lower()
    assert "= 'vanguard'" in first_verb
    close = sql.split("empower_option_verbs as (", 1)[1].split(
        "fee_guard as (", 1
    )[0]
    assert "= 'empower'" in close
    assert "then 'option_sell_to_close'" in close
    assert "then 'option_buy_to_close'" in close
    assert "option_buy_to_open" not in close
    assert "option_sell_to_open" not in close
    assert "amount_raw" not in close
    assert "first_action" not in close
    assert "from empower_option_verbs c" in sql
