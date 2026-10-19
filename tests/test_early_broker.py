"""First 10 users of every SnapTrade brokerage slug."""

from datetime import datetime, timezone

from app.early_broker import (
    EARLY_BROKER_USER_CAP,
    MODELED_BROKER_KEYS,
    backfill_early_broker_cohort,
    broker_key,
    count_users_for_broker_key,
    display_name,
    is_modeled_broker,
    maybe_stamp_early_broker_cohort,
    notice_from_accounts,
    trial_days,
)


def test_broker_key_normalizes_institution_names():
    assert broker_key("Robinhood") == "robinhood"
    assert broker_key("Charles Schwab") == "schwab"
    assert broker_key("SCHWAB") == "schwab"
    assert broker_key("Interactive Brokers") == "interactive"
    assert broker_key("IBKR") == "interactive"
    assert broker_key("Alpaca Paper Account") == "alpaca"
    assert broker_key("") == ""
    assert broker_key(None) == ""


def test_modeled_set_matches_dbt_known_brokers():
    assert MODELED_BROKER_KEYS == {
        "schwab", "alpaca", "fidelity", "interactive", "vanguard",
    }
    assert is_modeled_broker("Fidelity")
    assert is_modeled_broker("Charles Schwab")
    assert is_modeled_broker("Vanguard")
    assert is_modeled_broker("Vanguard Account")
    assert not is_modeled_broker("Robinhood")
    assert not is_modeled_broker("Wealthsimple")


def test_display_name_prefers_friendly_label():
    assert display_name("robinhood") == "Robinhood"
    assert display_name("IBKR") == "Interactive Brokers"
    assert display_name("Some New Broker") == "Some New Broker"


def test_notice_none_without_cohort_flag():
    rows = [{"broker_slug": "Robinhood", "early_broker_cohort": False}]
    assert notice_from_accounts(rows) is None
    assert notice_from_accounts([]) is None
    assert notice_from_accounts(None) is None


def test_notice_includes_promo_when_configured(monkeypatch):
    monkeypatch.setenv("EARLY_BROKER_TRIAL_DAYS", "180")
    rows = [{"broker_slug": "Robinhood", "early_broker_cohort": True}]
    note = notice_from_accounts(rows, promo="EARLYMIRROR")
    assert note["broker_key"] == "robinhood"
    assert note["broker_name"] == "Robinhood"
    assert note["promo_code"] == "EARLYMIRROR"
    assert note["trial_days"] == 180
    assert note["trial_months"] == 6
    assert notice_from_accounts(rows, promo="")["promo_code"] is None


def test_count_users_is_distinct_and_key_scoped():
    rows = [
        {"user_id": 1, "broker_slug": "Robinhood"},
        {"user_id": 1, "broker_slug": "Robinhood"},  # second account, same user
        {"user_id": 2, "broker_slug": "robinhood"},
        {"user_id": 3, "broker_slug": "Vanguard"},
        {"user_id": 4, "broker_slug": "Charles Schwab"},
    ]
    assert count_users_for_broker_key("robinhood", rows) == 2
    assert count_users_for_broker_key("vanguard", rows) == 1
    assert count_users_for_broker_key("schwab", rows) == 1


def test_each_broker_slug_has_its_own_first_ten(monkeypatch):
    """Eleven Schwab users do not spend Fidelity's cap. Past the cap we
    do not write, so a grant already stored is left alone.
    """
    writes = []
    schwab = [{"user_id": i, "broker_slug": "Charles Schwab"} for i in range(1, 12)]
    mixed = schwab + [{"user_id": 40, "broker_slug": "Fidelity"}]
    monkeypatch.setattr("app.db.fetch_all", lambda *a, **k: mixed)
    monkeypatch.setattr(
        "app.db.execute",
        lambda sql, params=None: writes.append((sql, params)),
    )
    assert maybe_stamp_early_broker_cohort(11, "acc-schwab", "Schwab") is False
    assert writes == []
    assert maybe_stamp_early_broker_cohort(40, "acc-fidelity", "Fidelity") is True
    assert "FALSE" not in writes[0][0].upper()
    assert writes[0][1] == (40, "acc-fidelity")


def test_stamp_within_cap(monkeypatch):
    writes = []
    existing = [{"user_id": i, "broker_slug": "Robinhood"} for i in range(1, 10)]
    existing.append({"user_id": 10, "broker_slug": "Robinhood"})  # current user already upserted
    monkeypatch.setattr("app.db.fetch_all", lambda *a, **k: existing)
    monkeypatch.setattr(
        "app.db.execute",
        lambda sql, params=None: writes.append((sql, params)),
    )
    assert maybe_stamp_early_broker_cohort(10, "acc-10", "Robinhood") is True
    assert writes
    assert writes[0][1] == (10, "acc-10")


def test_stamp_past_cap(monkeypatch):
    writes = []
    existing = [{"user_id": i, "broker_slug": "Robinhood"} for i in range(1, 12)]
    monkeypatch.setattr("app.db.fetch_all", lambda *a, **k: existing)
    monkeypatch.setattr(
        "app.db.execute",
        lambda sql, params=None: writes.append((sql, params)),
    )
    assert maybe_stamp_early_broker_cohort(11, "acc-11", "Robinhood") is False
    assert writes == []
    assert EARLY_BROKER_USER_CAP == 10


def test_backfill_stamps_first_ten_of_every_slug_and_keeps_grants(monkeypatch):
    writes = []
    sqls = []
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(1, 15):
        rows.append({
            "user_id": i,
            "snaptrade_account_id": f"rh-{i}",
            "broker_slug": "Robinhood",
            "created_at": t0.replace(day=min(i, 28)),
            # Already granted, and past this slug's first 10. Must stay.
            "early_broker_cohort": i in (1, 14),
        })
    rows.append({
        "user_id": 99,
        "snaptrade_account_id": "schwab-99",
        "broker_slug": "Charles Schwab",
        "created_at": t0,
        "early_broker_cohort": False,
    })
    for offset, uid in enumerate(range(100, 111)):
        rows.append({
            "user_id": uid,
            "snaptrade_account_id": f"schwab-{uid}",
            "broker_slug": "Schwab",
            "created_at": t0.replace(day=offset + 2),
            # 12th Schwab user (uid 110) already has a grant.
            "early_broker_cohort": uid == 110,
        })
    monkeypatch.setattr("app.db.fetch_all", lambda *a, **k: rows)
    monkeypatch.setattr(
        "app.db.execute",
        lambda sql, params=None: (sqls.append(sql), writes.append(params)),
    )
    n = backfill_early_broker_cohort()
    stamped_users = {p[0] for p in writes}
    # Robinhood 2-10 (1 already granted). Schwab 99 plus 100-108.
    assert n == 19
    assert stamped_users == set(range(2, 11)) | {99} | set(range(100, 109))
    assert 1 not in stamped_users
    assert 14 not in stamped_users
    assert 109 not in stamped_users
    assert 110 not in stamped_users
    assert sqls
    assert all("TRUE" in sql and "FALSE" not in sql.upper() for sql in sqls)


def test_note_template_renders_promo_and_stays_hidden_for_js():
    from app import app

    with app.test_request_context("/get-started"):
        html = app.jinja_env.get_template("_early_broker_note.html").render(
            early_broker={
                "broker_key": "robinhood",
                "broker_name": "Robinhood",
                "promo_code": "EARLYMIRROR",
                "trial_days": 180,
                "trial_months": 6,
                "offer": "If you subscribe, 6 months of Pro is included before the first charge.",
            },
        )
    assert "If you subscribe, 6 months of Pro is included before the first charge." in html
    assert "first 10 people to connect Robinhood" in html
    assert "calibrate" not in html.lower()
    assert "handful" not in html.lower()
    assert "EARLYMIRROR" not in html
    assert "Have a CSV" not in html
    assert "SnapTrade" not in html
    assert 'data-broker="robinhood"' in html
    assert "hidden" in html
    assert "ht-early-broker-note-dismissed:" in html


def test_note_template_omits_promo_chip_when_unset():
    from app import app

    with app.test_request_context("/get-started"):
        html = app.jinja_env.get_template("_early_broker_note.html").render(
            early_broker={
                "broker_key": "vanguard",
                "broker_name": "Vanguard",
                "promo_code": None,
                "trial_days": 180,
                "trial_months": 6,
                "offer": "If you subscribe, 6 months of Pro is included before the first charge.",
            },
        )
    assert "early Vanguard user" not in html
    assert "6 months of Pro, free." not in html
    assert "If you subscribe, 6 months of Pro is included before the first charge." in html
    assert "first 10 people to connect Vanguard" in html
    assert "calibrate" not in html.lower()
    assert 'id="ht-early-promo"' not in html


def test_notice_names_the_newest_connected_early_broker(monkeypatch):
    """An older early-cohort brokerage must not stay on the banner after
    a later one is connected. Names come from each row's stored
    broker_slug. Fixtures are made up.
    """
    monkeypatch.setenv("EARLY_BROKER_TRIAL_DAYS", "180")
    older = datetime(2026, 3, 2, tzinfo=timezone.utc)
    newer = datetime(2026, 10, 2, tzinfo=timezone.utc)
    note = notice_from_accounts([
        {
            "broker_slug": "Coinbase",
            "early_broker_cohort": True,
            "created_at": older,
        },
        {
            "broker_slug": "Charles Schwab",
            "early_broker_cohort": False,
            "created_at": newer,
        },
        {
            "broker_slug": "Empower",
            "early_broker_cohort": True,
            "created_at": newer,
        },
    ])
    assert note["broker_key"] == "empower"
    assert note["broker_name"] == "Empower"
    assert "Coinbase" not in note["broker_name"]
    assert "6 months" in note["offer"]


def test_empower_and_schwab_both_stamp_under_the_cap(monkeypatch):
    """Every slug is eligible, including a modeled brokerage and Empower."""
    assert broker_key("Empower") == "empower"
    assert display_name("Empower") == "Empower"
    assert not is_modeled_broker("Empower")
    assert is_modeled_broker("Charles Schwab")
    writes = []
    monkeypatch.setattr("app.db.fetch_all", lambda *a, **k: [])
    monkeypatch.setattr(
        "app.db.execute",
        lambda sql, params=None: writes.append(params),
    )
    assert maybe_stamp_early_broker_cohort(4, "acct-empower", "Empower") is True
    assert maybe_stamp_early_broker_cohort(4, "acct-schwab", "Charles Schwab") is True
    assert writes == [(4, "acct-empower"), (4, "acct-schwab")]


def test_trial_days_default_is_six_months(monkeypatch):
    monkeypatch.delenv("EARLY_BROKER_TRIAL_DAYS", raising=False)
    assert trial_days() == 180


def test_trial_days_zero_disables(monkeypatch):
    monkeypatch.setenv("EARLY_BROKER_TRIAL_DAYS", "0")
    assert trial_days() == 0
