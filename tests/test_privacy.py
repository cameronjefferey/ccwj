"""Privacy mode masks identity and account balances at the data layer.

Trade-level money formatting is a different function and stays numeric.
"""

from app.money import fmt_money
from app.privacy import (
    PRIVACY_MASK,
    format_privacy_balance,
    format_privacy_signed,
    mask_account_label,
    mask_secret,
    shown_account,
    slots_from_rows,
    sort_masked_account_choices,
)


def test_slots_are_stable_by_tenant_id():
    rows = [
        {"tenant_id": "snaptrade:bbb", "display_nickname": "Sara", "account_name": "Schwab Account"},
        {"tenant_id": "snaptrade:aaa", "display_nickname": "Roth", "account_name": "Schwab Account"},
    ]
    by_tid, by_name = slots_from_rows(rows)
    assert by_tid == {"snaptrade:aaa": "Account 1", "snaptrade:bbb": "Account 2"}
    # Colliding broker labels keep the first slot. tenant_id still disambiguates.
    assert by_name["Roth"] == "Account 1"
    assert by_name["Sara"] == "Account 2"
    assert by_name["Schwab Account"] == "Account 1"


def test_mask_is_a_noop_when_privacy_is_off():
    by_tid = {"snaptrade:aaa": "Account 1"}
    by_name = {"Roth": "Account 1"}
    assert mask_account_label("Roth", "snaptrade:aaa", enabled=False, by_tid=by_tid, by_name=by_name) is None
    assert shown_account("Roth", "snaptrade:aaa") == "Roth"
    assert mask_secret("ada@example.com", enabled=False) == "ada@example.com"
    assert format_privacy_balance(19999, enabled=False) == "$19,999"
    assert format_privacy_signed(-250, enabled=False) == "-$250"


def test_account_numbers_and_balances_mask_when_on():
    by_tid, by_name = slots_from_rows([
        {"tenant_id": "snaptrade:aaa", "display_nickname": "Roth", "account_name": "Schwab Account"},
        {"tenant_id": "snaptrade:bbb", "display_nickname": "Sara", "account_name": "Schwab Account"},
    ])
    assert mask_account_label("Schwab Account", "snaptrade:bbb", enabled=True, by_tid=by_tid, by_name=by_name) == "Account 2"
    assert mask_account_label("Roth", None, enabled=True, by_tid=by_tid, by_name=by_name) == "Account 1"
    assert mask_secret("ada@example.com", enabled=True) == PRIVACY_MASK
    assert mask_secret("Schwab", enabled=True) == PRIVACY_MASK
    assert mask_secret("••••1234", enabled=True) == PRIVACY_MASK
    assert mask_secret("", enabled=True) == ""
    assert format_privacy_balance(482331, enabled=True) == PRIVACY_MASK
    assert format_privacy_balance(None, enabled=True) == "—"
    assert format_privacy_signed(1200, enabled=True) == PRIVACY_MASK


def test_masked_lists_sort_by_account_number_not_nickname():
    choices = [
        {"tenant_id": "snaptrade:m", "label": "Zulu"},
        {"tenant_id": "snaptrade:z", "label": "Alpha"},
        {"tenant_id": "snaptrade:a", "label": "Mid"},
    ]
    by_tid, by_name = slots_from_rows([
        {"tenant_id": "snaptrade:z", "display_nickname": "Alpha"},
        {"tenant_id": "snaptrade:a", "display_nickname": "Mid"},
        {"tenant_id": "snaptrade:m", "display_nickname": "Zulu"},
        {"tenant_id": "snaptrade:ten", "display_nickname": "Ten"},
    ])
    # tenant_id order: a, m, ten, z → Account 1, 2, 3, 4
    assert by_tid["snaptrade:a"] == "Account 1"
    assert by_tid["snaptrade:ten"] == "Account 3"
    ordered = sort_masked_account_choices(
        choices + [{"tenant_id": "snaptrade:ten", "label": "Ten"}],
        enabled=True,
        by_tid=by_tid,
        by_name=by_name,
    )
    assert [c["tenant_id"] for c in ordered] == [
        "snaptrade:a",
        "snaptrade:m",
        "snaptrade:ten",
        "snaptrade:z",
    ]
    # Privacy off keeps the nickname order the picker already built.
    assert sort_masked_account_choices(choices, enabled=False) == choices


def test_trade_money_formatter_is_not_masked():
    assert fmt_money(-3333, decimals=0) != PRIVACY_MASK
    assert "3,333" in fmt_money(-3333, decimals=0)


def test_jinja_account_label_uses_privacy_slots(monkeypatch):
    import os
    os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")
    from app import app

    by_tid = {"snaptrade:aaa": "Account 1"}
    by_name = {"Roth IRA": "Account 1"}
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr("app.privacy.viewer_slots", lambda: (by_tid, by_name))
    with app.test_request_context("/positions"):
        label = app.jinja_env.filters["account_label"]
        hide = app.jinja_env.filters["privacy_hide"]
        balance = app.jinja_env.filters["privacy_balance"]
        assert label("Roth IRA", "snaptrade:aaa") == "Account 1"
        assert hide("trader@example.com") == PRIVACY_MASK
        assert balance(250000) == PRIVACY_MASK
        rendered = app.jinja_env.from_string(
            "{{ name | account_label(tid) }}|{{ 88000 | privacy_balance }}|{{ money(pnl, 0) }}"
        ).render(name="Roth IRA", tid="snaptrade:aaa", pnl=-3333.2)
    assert rendered.startswith("Account 1|••••|")
    assert "3,333" in rendered
    assert "Roth" not in rendered


def test_story_review_masks_account_labels(monkeypatch):
    """Position review chip, card line, and chart tooltip share one rewrite."""
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: True)
    monkeypatch.setattr(
        "app.privacy.shown_account",
        lambda name, tenant_id=None: "Account 1" if tenant_id == "snaptrade:aaa" else "Account 2",
    )
    from app.position_story import _mask_story_account_labels

    items = [{
        "type": "day",
        "headlines": ["Sold the $50 call — Roth IRA."],
        "option_cards": [{"account": "Roth IRA", "title": "Sold", "lane": "options"}],
        "share_cards": [{"account": "Roth IRA", "title": "Trimmed", "lane": "shares"}],
    }]
    markers = [{
        "d": "2026-01-02",
        "k": "sell",
        "t": ["Sold the $50 call — Roth IRA."],
        "tips": [{
            "label": "Sold 1 × $50 call · Jan 16",
            "amt": "+$120",
            "sign": 1,
            "kind": "sell",
            "acct": "Roth IRA",
        }],
    }]
    stats = {"accounts": ["Roth IRA"]}
    _mask_story_account_labels(
        items, markers, stats, {"Roth IRA": "snaptrade:aaa"},
    )
    assert stats["accounts"] == ["Account 1"]
    assert items[0]["option_cards"][0]["account"] == "Account 1"
    assert items[0]["share_cards"][0]["account"] == "Account 1"
    assert "Roth" not in items[0]["headlines"][0]
    assert "Account 1" in markers[0]["t"][0]
    assert "Roth" not in markers[0]["t"][0]
    assert markers[0]["tips"][0]["acct"] == "Account 1"
    assert "Roth" not in markers[0]["tips"][0]["label"]


def test_story_review_keeps_names_when_privacy_is_off(monkeypatch):
    monkeypatch.setattr("app.privacy.privacy_mode_on", lambda: False)
    from app.position_story import _mask_story_account_labels

    items = [{
        "headlines": ["Sold the $50 call — Roth IRA."],
        "option_cards": [{"account": "Roth IRA"}],
        "share_cards": [],
    }]
    markers = [{
        "t": ["Sold the $50 call — Roth IRA."],
        "tips": [{"label": "Sold 1 × $50 call", "acct": "Roth IRA"}],
    }]
    stats = {"accounts": ["Roth IRA"]}
    _mask_story_account_labels(
        items, markers, stats, {"Roth IRA": "snaptrade:aaa"},
    )
    assert stats["accounts"] == ["Roth IRA"]
    assert items[0]["option_cards"][0]["account"] == "Roth IRA"
    assert "Roth IRA" in markers[0]["t"][0]
    assert markers[0]["tips"][0]["acct"] == "Roth IRA"
