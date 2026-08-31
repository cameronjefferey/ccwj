"""Right-side position peek drawer (Today / Overview movers)."""
from datetime import date

import pandas as pd
import pytest

from app import app
from app.position_detail import compose_position_peek


def _summary(**kw):
    row = {
        "tenant_id": "snaptrade:abc",
        "account": "Schwab Account",
        "symbol": "NVDA",
        "strategy": "Covered Call",
        "status": "Open",
        "total_pnl": 1200.0,
        "realized_pnl": 800.0,
        "unrealized_pnl": 300.0,
        "total_dividend_income": 100.0,
        "num_individual_trades": 12,
        "num_winners": 4,
        "num_losers": 1,
        "first_trade_date": date(2025, 1, 6),
        "last_trade_date": date(2026, 8, 28),
        "company_name": "NVIDIA Corporation",
    }
    row.update(kw)
    return row


def _current(**kw):
    row = {
        "tenant_id": "snaptrade:abc",
        "account": "Schwab Account",
        "symbol": "NVDA",
        "instrument_type": "Equity",
        "trade_symbol": "NVDA",
        "quantity": 100.0,
        "current_price": 219.73,
        "market_value": 21973.0,
        "cost_basis": 18000.0,
        "unrealized_pnl": 3973.0,
        "option_expiry": None,
    }
    row.update(kw)
    return row


def test_compose_rolls_summary_and_open_lots():
    summary = pd.DataFrame([
        _summary(),
        _summary(strategy="Buy and Hold", total_pnl=50.0, realized_pnl=50.0,
                 unrealized_pnl=0.0, total_dividend_income=0.0,
                 num_individual_trades=2, num_winners=1, num_losers=0),
    ])
    current = pd.DataFrame([
        _current(),
        _current(instrument_type="Call", trade_symbol="NVDA  260904C00230000",
                 quantity=1.0, current_price=4.10, market_value=-410.0,
                 cost_basis=-500.0, unrealized_pnl=90.0,
                 option_expiry=date(2026, 9, 4)),
    ])
    with app.test_request_context("/"):
        out = compose_position_peek(
            "nvda", summary, current,
            label_map={"snaptrade:abc": "Sara Investment"},
            today=date(2026, 8, 31),
        )
    assert out["symbol"] == "NVDA"
    assert out["company_name"] == "NVIDIA Corporation"
    assert out["status"] == "Open"
    assert out["strategies"] == ["Covered Call", "Buy and Hold"]
    assert out["total_pnl"] == 1250.0
    assert out["realized_pnl"] == 850.0
    assert out["dividends"] == 100.0
    assert out["fills"] == 14
    assert out["win_rate"] == pytest.approx(5 / 6, rel=1e-4)
    assert out["accounts"][0]["account"] == "Sara Investment"
    kinds = {h["kind"] for h in out["holdings"]}
    assert kinds == {"equity", "option"}
    equity = next(h for h in out["holdings"] if h["kind"] == "equity")
    assert equity["qty_label"] == "100 sh"


def test_compose_drops_expired_option_lots():
    current = pd.DataFrame([_current(
        instrument_type="Call", trade_symbol="NVDA  260828C00230000",
        quantity=1.0, option_expiry=date(2026, 8, 28),
    )])
    with app.test_request_context("/"):
        out = compose_position_peek(
            "NVDA", pd.DataFrame([_summary()]), current,
            today=date(2026, 8, 31),
        )
    assert out["holdings"] == []


def test_compose_empty_frames_still_identifies_symbol():
    with app.test_request_context("/"):
        out = compose_position_peek("ASTS", pd.DataFrame(), pd.DataFrame())
    assert out["symbol"] == "ASTS"
    assert out["holdings"] == []
    assert out["total_pnl"] == 0.0


def test_peek_endpoint_requires_login():
    client = app.test_client()
    r = client.get("/api/position/NVDA/peek")
    assert r.status_code in (302, 401, 403)
    if r.status_code == 302:
        assert "/login" in (r.headers.get("Location") or "")
