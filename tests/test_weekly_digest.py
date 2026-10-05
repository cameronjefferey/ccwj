"""Weekly digest: grouped trades, verdict dollars, paper exclusion.

The week of Sep 28, 2026 is Sara's SPXW statement (the same fills the
movers card already nets). Best and worst must be the grouped spreads,
not the fused short leg (+$10,780.90) and long leg (−$8,903.10).

MU and BE verdicts use the official closes on Oct 2, 2026:
MU $1,074.89, BE $289.15.
"""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app.email import send_weekly_summary_email
from app.execution_quality import (
    early_close_delta,
    normalize_option_multiplier,
    verdicts_landed,
)
from app.weekly_digest import (
    digest_tenants_for_user,
    digest_trade_label,
    equity_closed_trades,
    grouped_option_trades,
    scope_digest_tenants,
    summarize_closed_trades,
)
from tests.test_spxw_statement import _history

WEEK = date(2026, 9, 28)
WEEK_END = date(2026, 10, 4)
# Official closes, Oct 2, 2026.
MU_CLOSE = 1074.89
BE_CLOSE = 289.15


def _spxw_trades():
    return grouped_option_trades(_history(), WEEK, WEEK_END)


def test_spxw_week_is_five_grouped_trades_net_of_fees():
    trades = _spxw_trades()
    by_pnl = sorted(round(t["pnl"], 2) for t in trades)
    assert by_pnl == [-3574.44, 437.78, 437.78, 902.24, 975.56]
    assert round(sum(t["pnl"] for t in trades), 2) == -821.08
    labels = {t["label"] for t in trades}
    assert "SPXW 10× 7730/7735C spread (expired) +$975.56" in labels
    assert "SPXW 20× 7730/7735C spread +$902.24" in labels
    assert "SPXW 10× 7650/7655C spread -$3,574.44" in labels
    assert "SPXW 5× 7725/7730C spread (expired) +$437.78" in labels
    assert "SPXW 5× 7640/7635P spread (expired) +$437.78" in labels
    # The fused legs of the 7730/7735 book are not trades.
    blob = " ".join(labels)
    assert "10,780.90" not in blob
    assert "8,903.10" not in blob


def test_counts_and_best_worst_are_grouped_trades():
    summary = summarize_closed_trades(
        _spxw_trades(), dividends=175.65, week_start=WEEK,
    )
    assert summary["trades_closed"] == 5
    assert summary["num_winners"] == 4
    assert summary["num_losers"] == 1
    assert summary["best_label"] == "SPXW 10× 7730/7735C spread (expired) +$975.56"
    assert summary["worst_label"] == "SPXW 10× 7650/7655C spread -$3,574.44"
    assert summary["week_label"] == "week of Sep 28"


def test_net_return_is_realized_trades_and_dividends_stay_separate():
    """Net return matches Overview 'Trades this week' realized, after
    pairing each spread. Dividends are not inside it. An equity session
    closed the same week is part of that realized total."""
    trades = _spxw_trades() + equity_closed_trades(
        [{"symbol": "JEPI", "total_pnl": 100.0, "tenant_id": "snaptrade:sara"}]
    )
    summary = summarize_closed_trades(
        trades, dividends=175.65, week_start=WEEK,
    )
    assert summary["net_return"] == -721.08
    assert summary["total_return"] == summary["net_return"]
    assert summary["dividends"] == 175.65
    assert summary["trades_closed"] == 6
    # 100 + the five SPXW results. Dividends are not in the sum.
    assert summary["net_return"] != round(-721.08 + 175.65, 2)


def test_paper_is_left_out_even_when_it_is_the_only_account():
    rows = [
        {
            "tenant_id": "snaptrade:real",
            "account_name": "Schwab Account",
            "broker_label": "Schwab",
        },
        {
            "tenant_id": "snaptrade:paper",
            "account_name": "Alpaca Paper Account",
            "broker_label": "Alpaca Paper",
        },
    ]
    mixed = scope_digest_tenants(
        ["snaptrade:real", "snaptrade:paper"], rows,
    )
    assert mixed == ["snaptrade:real"]
    paper_only = scope_digest_tenants(["snaptrade:paper"], rows)
    assert paper_only == []
    # No broker row means we cannot prove the account is real.
    assert scope_digest_tenants(["snaptrade:mystery"], rows) == []


def test_paper_lookup_error_returns_no_tenants(monkeypatch):
    def _boom(_user_id):
        raise RuntimeError("broker lookup failed")

    monkeypatch.setattr("app.models.get_tenant_ids_for_user", _boom)
    assert digest_tenants_for_user(9) == []


def test_single_leg_label_does_not_repeat_the_symbol():
    assert digest_trade_label("MU", "1× MU 1000C", 3267) == "MU 1× 1000C +$3,267.00"
    assert "MU 1× MU" not in digest_trade_label("MU", "1× MU 1000C", 1)
    assert digest_trade_label(
        "SPXW", "10× 7730/7735C spread", 975.56, expired=True,
    ) == "SPXW 10× 7730/7735C spread (expired) +$975.56"
    fills = pd.DataFrame([
        {
            "tenant_id": "snaptrade:sara",
            "account": "Sara",
            "trade_date": date(2026, 9, 22),
            "action": "option_buy_to_open",
            "trade_symbol": "MU    261002C01000000",
            "quantity": 1,
            "price": 32.67,
            "fees": 0,
            "amount": -3267.0,
        },
        {
            "tenant_id": "snaptrade:sara",
            "account": "Sara",
            "trade_date": date(2026, 9, 30),
            "action": "option_sell_to_close",
            "trade_symbol": "MU    261002C01000000",
            "quantity": 1,
            "price": 32.67,
            "fees": 0,
            "amount": 3267.0,
        },
    ])
    trades = grouped_option_trades(fills, WEEK, WEEK_END)
    assert len(trades) == 1
    assert trades[0]["label"].startswith("MU 1× 1000C")
    assert "MU 1× MU" not in trades[0]["label"]


def _verdict(**kw):
    base = {
        "tenant_id": "snaptrade:sara",
        "account": "Sara Investment",
        "symbol": "MU",
        "trade_symbol": "MU    261002C01100000",
        "option_type": "C",
        "option_strike": 1100.0,
        "option_expiry": date(2026, 10, 2),
        "direction": "Bought",
        "close_date": date(2026, 9, 23),
        "close_type": "Closed",
        "contracts": 1.0,
        "cost_to_close": 0.0,
        "proceeds_from_close": 0.0,
        "underlying_close_at_expiry": MU_CLOSE,
        "expired_worthless": False,
        "gradeable_early_close": True,
        "early_close_vs_expiry_delta": 24171.0,
        "was_rolled": False,
        "close_price": None,
    }
    base.update(kw)
    return base


def test_double_times_100_cash_is_divided_once():
    """1 × $2.4171 × 100 = $241.71. Cash of $24,171 is that premium × 100 again.

    This is a synthetic bad shape on a made-up symbol. The live MU
    $1100 call is 5 contracts at $48.35, tested below.
    """
    qty, cash = normalize_option_multiplier(1, 2.4171, 24171.0)
    assert qty == 1
    assert cash == 241.71
    delta = early_close_delta(_verdict(
        symbol="XYZ",
        trade_symbol="XYZ   261002C01100000",
        proceeds_from_close=24171.0,
        close_price=2.4171,
        contracts=1,
    ))
    assert delta == 241.71


def test_mu_1100_five_contracts_at_48_35_stays_24171():
    """5 × $48.35 × 100 = $24,175. The fill is $24,171.17 after a few dollars of fees.

    Bought Sep 22 for $23,828.31, sold Sep 23, expired worthless
    (MU closed at $1,074.89, under $1,100). The ×100 guard must leave
    this dollar alone.
    """
    qty, cash = normalize_option_multiplier(5, 48.35, 24171.17)
    assert qty == 5
    assert cash == 24171.17
    delta = early_close_delta(_verdict(
        contracts=5,
        proceeds_from_close=24171.17,
        close_price=48.35,
        early_close_vs_expiry_delta=24171.17,
    ))
    assert delta == 24171.17


def test_ten_contracts_at_24_dollars_stay_24171():
    """10 × $24.171 × 100 = $24,171 is a real trade, not a double ×100."""
    qty, cash = normalize_option_multiplier(10, 24.171, 24171.0)
    assert qty == 10
    assert cash == 24171.0
    delta = early_close_delta(_verdict(
        contracts=10, proceeds_from_close=24171.0, close_price=24.171,
        early_close_vs_expiry_delta=24171.0,
    ))
    assert delta == 24171.0


def test_share_count_does_not_scale_intrinsic_by_100_again():
    """Quantity 100 with cash = price × 100 is one contract, not 100.

    Intrinsic on the $1000 call is $74.89 a share. One contract settles
    for $7,489. Treating 100 as the contract count would settle for
    $748,900.
    """
    qty, cash = normalize_option_multiplier(100, 32.67, 3267.0)
    assert qty == 1
    assert cash == 3267.0
    delta = early_close_delta(_verdict(
        option_strike=1000.0,
        trade_symbol="MU    261002C01000000",
        contracts=100,
        proceeds_from_close=3267.0,
        close_price=32.67,
        close_date=date(2026, 10, 1),
        early_close_vs_expiry_delta=-422200.0,
    ))
    # Sold for $3,267. Expiry value of one contract is $7,489.
    # Holding would have been $4,222 better.
    assert delta == -4222.0


def test_mu_1000_and_be_match_the_expiry_close():
    """$1000 call: close $1,074.89, so $74.89 of intrinsic × 100 = $7,489.
    Exit cash $3,267. The $4,222 gap is that difference, ×100 once.

    BE $300 call: close $289.15, so it expired worthless. Buying it
    back for $290 gave up $290. That dollar was already right.
    """
    mu = early_close_delta(_verdict(
        option_strike=1000.0,
        trade_symbol="MU    261002C01000000",
        proceeds_from_close=3267.0,
        close_price=32.67,
        close_date=date(2026, 10, 1),
        early_close_vs_expiry_delta=-4222.0,
    ))
    assert mu == -4222.0
    be = early_close_delta(_verdict(
        symbol="BE",
        trade_symbol="BE    261002C00300000",
        option_strike=300.0,
        direction="Sold",
        close_date=date(2026, 9, 28),
        contracts=1,
        cost_to_close=-290.0,
        proceeds_from_close=0.0,
        close_price=2.90,
        underlying_close_at_expiry=BE_CLOSE,
        early_close_vs_expiry_delta=-290.0,
    ))
    assert be == -290.0


def test_verdicts_phrase_the_audited_dollars_and_group_a_spread():
    rows = [
        _verdict(
            contracts=5,
            proceeds_from_close=24171.17,
            close_price=48.35,
            early_close_vs_expiry_delta=24171.17,
        ),
        _verdict(
            option_strike=1000.0,
            trade_symbol="MU    261002C01000000",
            proceeds_from_close=3267.0,
            close_price=32.67,
            close_date=date(2026, 10, 1),
            early_close_vs_expiry_delta=-4222.0,
        ),
        _verdict(
            symbol="BE",
            trade_symbol="BE    261002C00300000",
            option_strike=300.0,
            direction="Sold",
            close_date=date(2026, 9, 28),
            cost_to_close=-290.0,
            proceeds_from_close=0.0,
            close_price=2.90,
            underlying_close_at_expiry=BE_CLOSE,
            early_close_vs_expiry_delta=-290.0,
        ),
        # A real short + long on this expiry is one verdict. The two
        # long MU calls above stay their own trades.
        _verdict(
            symbol="MU",
            trade_symbol="MU    261002C01250000",
            option_strike=1250.0,
            direction="Sold",
            close_date=date(2026, 9, 30),
            cost_to_close=-100.0,
            proceeds_from_close=0.0,
            close_price=1.0,
            early_close_vs_expiry_delta=-10000.0,
        ),
        _verdict(
            symbol="MU",
            trade_symbol="MU    261002C01260000",
            option_strike=1260.0,
            direction="Bought",
            close_date=date(2026, 9, 30),
            cost_to_close=0.0,
            proceeds_from_close=40.0,
            close_price=0.40,
            early_close_vs_expiry_delta=4000.0,
        ),
    ]
    landed = verdicts_landed(pd.DataFrame(rows), WEEK, WEEK_END)
    by_key = {}
    for row in landed:
        by_key.setdefault(row["structure"] or row["symbol"], []).append(row)

    texts = " ".join(v["sentence"] for v in landed)
    assert "10,780.90" not in texts
    # 5 × $48.35, sold for $24,171.17, expired worthless.
    sold = next(v for v in landed if "1100" in v["sentence"] or "$1100" in v["sentence"])
    assert sold["delta"] == 24171.17
    assert "expired worthless" in sold["sentence"]
    assert "beat holding by $24,171.17" in sold["sentence"]
    assert "$241.71" not in sold["sentence"]

    held = next(v for v in landed if "$1000" in v["sentence"])
    assert held["delta"] == -4222.0
    assert "would have been $4,222 better" in held["sentence"]

    be = next(v for v in landed if v["symbol"] == "BE")
    assert be["delta"] == -290.0
    assert "bought back" in be["sentence"].lower() or "You bought back" in be["sentence"]
    assert "expired worthless" in be["sentence"]
    assert "gave up $290" in be["sentence"]

    spread = next(v for v in landed if v["structure"] == "Call Spread")
    assert spread["delta"] == -60.0
    assert "You closed the $1250 / $1260 call spread" in spread["sentence"]
    assert "both legs" in spread["sentence"]
    # Two legs, one verdict.
    assert sum(1 for v in landed if v["structure"] == "Call Spread") == 1


def test_rendered_weekly_email_uses_grouped_labels(monkeypatch, tmp_path):
    summary = summarize_closed_trades(
        _spxw_trades(), dividends=175.65, week_start=WEEK,
    )
    summary["verdicts"] = verdicts_landed(pd.DataFrame([
        _verdict(
            contracts=5,
            proceeds_from_close=24171.17,
            close_price=48.35,
            early_close_vs_expiry_delta=24171.17,
        ),
        _verdict(
            option_strike=1000.0,
            trade_symbol="MU    261002C01000000",
            proceeds_from_close=3267.0,
            close_price=32.67,
            close_date=date(2026, 10, 1),
            early_close_vs_expiry_delta=-4222.0,
        ),
        _verdict(
            symbol="BE",
            trade_symbol="BE    261002C00300000",
            option_strike=300.0,
            direction="Sold",
            close_date=date(2026, 9, 28),
            cost_to_close=-290.0,
            close_price=2.90,
            underlying_close_at_expiry=BE_CLOSE,
            early_close_vs_expiry_delta=-290.0,
        ),
    ]), WEEK, WEEK_END)
    captured = {}

    def _capture(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("app.email.send_email", _capture)
    send_weekly_summary_email(
        to="testingcameron@example.com",
        username="testingcameron",
        summary=summary,
        dashboard_url="https://happytrader.me/overview",
        unsubscribe_url="https://happytrader.me/email/unsubscribe/preview",
    )
    html = captured["html_body"]
    assert "Your week: week of Sep 28" in html
    assert "-$821.08" in html
    assert "$175.65" in html
    assert "5 (4W / 1L)" in html
    assert "SPXW 10× 7730/7735C spread (expired) +$975.56" in html
    assert "SPXW 10× 7650/7655C spread -$3,574.44" in html
    assert "$10,780.90" not in html
    assert "$8,903.10" not in html
    assert "beat holding by $24,171.17" in html
    assert "would have been $4,222 better" in html
    assert "gave up $290" in html
    assert "$241.71" not in html
    out = Path("/tmp/weekly_digest_preview.html")
    out.write_text(html, encoding="utf-8")
    preview = tmp_path / "weekly_digest_preview.html"
    preview.write_text(html, encoding="utf-8")


def test_preview_week_pins_sql_and_cron_sql_stays():
    from app.email_digests_cli import (
        _VERDICTS_SQL,
        _WEEK_EQUITY_SQL,
        _WEEK_OPTION_FILLS_SQL,
        _sql_for_week,
        _verdicts_sql_for_week,
    )

    assert _sql_for_week(_WEEK_OPTION_FILLS_SQL, None) == _WEEK_OPTION_FILLS_SQL
    for sql in (_WEEK_OPTION_FILLS_SQL, _WEEK_EQUITY_SQL):
        pinned = _sql_for_week(sql, WEEK)
        assert "SELECT @week_start AS week_start" in pinned
        assert "MAX(week_start)" not in pinned
    assert _verdicts_sql_for_week(None) == _VERDICTS_SQL
    week_sql = _verdicts_sql_for_week(WEEK)
    assert "DATE_ADD(@week_start, INTERVAL 6 DAY)" in week_sql
    assert "DATE_SUB(CURRENT_DATE()" not in week_sql


def test_preview_html_does_not_send(monkeypatch):
    from app.email_digests_cli import render_weekly_digest_html

    def _no(**_kwargs):
        raise AssertionError("sent")

    monkeypatch.setattr("app.email.send_email", _no)
    quiet = render_weekly_digest_html(None, None, 1, [], "ada", WEEK)
    assert "Paper accounts are left out" in quiet
    assert "ada" in quiet
    monkeypatch.setattr(
        "app.email_digests_cli._build_weekly_summary",
        lambda *_a, **_k: {
            "week_label": "week of Sep 28",
            "week_start": WEEK,
            "total_return": -821.08,
            "trades_closed": 1,
            "num_winners": 0,
            "num_losers": 1,
            "best_label": None,
            "worst_label": "MU 1× 1000C -$1.00",
            "verdicts": [],
            "dividends": 0,
        },
    )
    html = render_weekly_digest_html(
        object(), object(), 1, ["snaptrade:real"], "ada", WEEK,
    )
    assert "week of Sep 28" in html
    assert "-$821.08" in html


def test_weekly_digest_html_escapes_user_controlled_fields():
    from app.email import build_weekly_summary_email

    payload = '<script>window.adminSessionStolen = true</script>'
    _subject, _body, html = build_weekly_summary_email(
        username=payload,
        summary={
            "week_label": payload,
            "total_return": 1,
            "trades_closed": 1,
            "num_winners": 1,
            "num_losers": 0,
            "best_label": payload,
            "verdicts": [{"symbol": payload, "sentence": payload}],
        },
        dashboard_url='https://happytrader.me/overview" onmouseover="alert(1)',
        unsubscribe_url='https://happytrader.me/unsubscribe" onmouseover="alert(1)',
    )

    assert payload not in html
    assert "&lt;script&gt;window.adminSessionStolen = true&lt;/script&gt;" in html
    assert 'onmouseover="alert(1)' not in html
    assert "onmouseover=&quot;alert(1)" in html


def test_admin_digest_preview_renders_without_sending(monkeypatch):
    from app import app
    from app.models import User

    class _User:
        is_authenticated = True
        is_active = True
        is_anonymous = False
        id = 7
        username = "cameron"

        def get_id(self):
            return "7"

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: _User()))
    monkeypatch.setenv("ADMIN_USERS", "cameron")
    sent = []
    monkeypatch.setattr("app.email.send_email", lambda **k: sent.append(k))
    seen = {}

    def _render(_client, _bigquery, user_id, tenant_ids, username, week_start):
        seen.update(
            user_id=user_id,
            tenant_ids=list(tenant_ids),
            username=username,
            week=week_start,
        )
        return "<html>digest preview</html>"

    monkeypatch.setattr("app.email_digests_cli.render_weekly_digest_html", _render)
    monkeypatch.setattr("app.bigquery_client.get_bigquery_client", lambda: object())
    monkeypatch.setattr(
        "app.weekly_digest.digest_tenants_for_user",
        lambda _user_id: ["snaptrade:real"],
    )
    target = _User()
    target.username = "testingcameron"
    target.id = 3
    monkeypatch.setattr(
        User,
        "get_by_username",
        staticmethod(lambda name: target if name == "testingcameron" else None),
    )
    client = app.test_client()
    anon = client.get("/admin/digest-preview?user=testingcameron&week=2026-10-02")
    assert anon.status_code in (302, 303, 401)

    with client.session_transaction() as sess:
        sess["_user_id"] = "7"
        sess["_fresh"] = True

    class _NotAdmin(_User):
        username = "alice"

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: _NotAdmin()))
    hidden = client.get("/admin/digest-preview?user=testingcameron&week=2026-10-02")
    assert hidden.status_code == 404

    monkeypatch.setattr(User, "get_by_id", staticmethod(lambda uid: _User()))
    bad = client.get("/admin/digest-preview?user=testingcameron&week=not-a-date")
    assert bad.status_code == 400
    missing = client.get("/admin/digest-preview?user=nobody&week=2026-10-02")
    assert missing.status_code == 404
    ok = client.get("/admin/digest-preview?user=testingcameron&week=2026-10-02")
    assert ok.status_code == 200
    assert b"digest preview" in ok.data
    assert ok.headers["Content-Security-Policy"].startswith("default-src 'none'")
    assert "form-action 'none'" in ok.headers["Content-Security-Policy"]
    assert seen["week"] == date(2026, 9, 28)
    assert seen["username"] == "testingcameron"
    assert seen["tenant_ids"] == ["snaptrade:real"]
    assert sent == []
