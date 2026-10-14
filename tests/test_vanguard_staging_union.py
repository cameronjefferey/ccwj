"""Vanguard staging is its own partition of the shared seed tables.

No warehouse credentials and no customer rows. These tests read the
dbt sources as text and check the routing contract:

* known_brokers() includes vanguard, in the same set the app uses
* the three base staging models and the parity test union the
  vanguard adapters
* the catch-all models stay one-line broker_*_rows('other') calls,
  so they cannot also select vanguard once the slug is known
* the vanguard adapters project the seed columns explicitly
"""
from __future__ import annotations

import re
from pathlib import Path

from app.early_broker import MODELED_BROKER_KEYS

ROOT = Path(__file__).resolve().parents[1]


def _text(rel: str) -> str:
    return (ROOT / rel).read_text()


def test_known_brokers_includes_vanguard_and_matches_the_app():
    macro = _text("dbt/macros/broker_slug_from_account.sql")
    match = re.search(
        r"macro known_brokers\(\)[\s\S]*?return\(\[(.*?)\]\)",
        macro,
    )
    assert match, "known_brokers() return list not found"
    slugs = re.findall(r"'([a-z0-9_]+)'", match.group(1))
    assert slugs == ["schwab", "alpaca", "fidelity", "interactive", "vanguard"]
    assert set(slugs) == MODELED_BROKER_KEYS


def test_base_staging_unions_include_vanguard_before_the_catch_all():
    expected = {
        "dbt/models/staging/stg_history.sql": "stg_broker_vanguard_history",
        "dbt/models/staging/stg_current.sql": "stg_broker_vanguard_current",
        "dbt/models/staging/stg_account_balances.sql": "stg_broker_vanguard_balances",
        "dbt/tests/broker_split_preserves_all_rows.sql": None,
    }
    for rel, model in expected.items():
        text = _text(rel)
        if model:
            assert f"ref('{model}')" in text
        else:
            for name in (
                "stg_broker_vanguard_history",
                "stg_broker_vanguard_current",
                "stg_broker_vanguard_balances",
            ):
                assert f"ref('{name}')" in text
        # Vanguard is a named broker, listed before the catch-all.
        vanguard_at = text.find("vanguard")
        other_at = text.find("stg_broker_other_")
        assert vanguard_at != -1 and other_at != -1
        assert vanguard_at < other_at


def test_catch_all_models_exclude_known_brokers():
    for name in (
        "stg_broker_other_history.sql",
        "stg_broker_other_current.sql",
        "stg_broker_other_balances.sql",
    ):
        text = _text(f"dbt/models/staging/brokers/{name}")
        assert "is_catch_all=true" in text
        assert "vanguard" not in text.lower()


def test_vanguard_adapters_project_seed_columns():
    history = _text("dbt/models/staging/brokers/stg_broker_vanguard_history.sql")
    for col in (
        "Account", "user_id", "tenant_id", "Date", "Action", "Symbol",
        "Description", "Quantity", "Price", "fees_and_comm", "Amount",
    ):
        assert col in history
    assert "broker_history_rows('vanguard')" in history

    current = _text("dbt/models/staging/brokers/stg_broker_vanguard_current.sql")
    for col in (
        "tenant_id", "Symbol", "Quantity", "market_value", "cost_bases",
        "security_type",
    ):
        assert col in current
    assert "broker_current_rows('vanguard')" in current

    balances = _text("dbt/models/staging/brokers/stg_broker_vanguard_balances.sql")
    for col in ("tenant_id", "row_type", "market_value", "src_priority"):
        assert col in balances
    assert "broker_balances_rows('vanguard')" in balances


def test_vanguard_schema_tests_cover_nulls_actions_and_keys():
    yml = _text("dbt/models/staging/brokers/_stg_broker_vanguard.yml")
    assert "not_null" in yml
    assert "accepted_values" in yml
    assert "Cash Dividend" in yml
    assert "ADR Mgmt Fee" in yml
    exclusive = _text("dbt/tests/vanguard_partition_is_exclusive.sql")
    assert "stg_broker_vanguard_history" in exclusive
    assert "stg_broker_other_history" in exclusive
    assert "vanguard_current_symbol_unique" not in exclusive
    symbol = _text("dbt/tests/vanguard_current_symbol_unique.sql")
    assert "tenant_id" in symbol and "symbol" in symbol
    priority = _text("dbt/tests/vanguard_balance_priority_unique.sql")
    assert "src_priority = 1" in priority
