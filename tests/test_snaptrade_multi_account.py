"""SnapTrade multi-account orchestration — unit tests.

Pins the per-row sync wrapper, bulk loop, first-sync lookback policy,
and the broken-connection flag handling. NO network calls (the
SnapTrade SDK is mocked); NO Postgres (model helpers are
monkey-patched).
"""
from __future__ import annotations

import os

# Tests import the app module which calls init_db on import unless
# this env var is set. Set it BEFORE any app.* import.
os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

import pytest

from app import models as _models
from app import snaptrade as _snap
from app.snaptrade import _bulk_sync_lookback_days


# ---------------------------------------------------------------------------
# _stable_account_name — the warehouse tenancy label
# ---------------------------------------------------------------------------


def test_stable_account_name_uses_last_four_when_available():
    name = _snap._stable_account_name("FIDELITY", "X12345678")
    assert name == "Fidelity ••••5678"


# ---------------------------------------------------------------------------
# _group_accounts_by_connection — broker-connection grouping for the
# accounts page (reconnect is a connection-level action, not per-account).
# ---------------------------------------------------------------------------


def test_group_accounts_groups_by_authorization_id():
    rows = [
        {"snaptrade_account_id": "a1", "broker_slug": "schwab",
         "brokerage_authorization_id": "auth-X", "connection_broken_at": None},
        {"snaptrade_account_id": "a2", "broker_slug": "schwab",
         "brokerage_authorization_id": "auth-X", "connection_broken_at": "2026-06-23"},
        {"snaptrade_account_id": "b1", "broker_slug": "fidelity",
         "brokerage_authorization_id": "auth-Y", "connection_broken_at": None},
    ]
    groups = _snap._group_accounts_by_connection(rows)
    assert len(groups) == 2
    schwab = next(g for g in groups if g["broker_slug"] == "schwab")
    assert len(schwab["accounts"]) == 2
    assert schwab["authorization_id"] == "auth-X"
    # Any broken account in the group marks the whole connection broken.
    assert schwab["needs_reconnect"] is True
    fidelity = next(g for g in groups if g["broker_slug"] == "fidelity")
    assert fidelity["needs_reconnect"] is False
    assert fidelity["broker_label"] == "Fidelity"


def test_group_accounts_falls_back_to_broker_slug_without_auth_id():
    # No cached auth id yet → accounts still collapse under their broker
    # rather than each rendering a standalone reconnect prompt.
    rows = [
        {"snaptrade_account_id": "a1", "broker_slug": "schwab",
         "brokerage_authorization_id": None, "connection_broken_at": "2026-06-23"},
        {"snaptrade_account_id": "a2", "broker_slug": "schwab",
         "brokerage_authorization_id": "", "connection_broken_at": "2026-06-23"},
    ]
    groups = _snap._group_accounts_by_connection(rows)
    assert len(groups) == 1
    assert groups[0]["broker_slug"] == "schwab"
    assert len(groups[0]["accounts"]) == 2
    assert groups[0]["needs_reconnect"] is True
    # No auth id anywhere → reconnect falls back to the generic portal.
    assert groups[0]["authorization_id"] is None


def test_group_accounts_picks_first_known_auth_id_in_group():
    rows = [
        {"snaptrade_account_id": "a1", "broker_slug": "schwab",
         "brokerage_authorization_id": "", "connection_broken_at": None},
        {"snaptrade_account_id": "a2", "broker_slug": "schwab",
         "brokerage_authorization_id": "auth-Z", "connection_broken_at": None},
    ]
    # a1 has no auth id so it keys on slug; a2 keys on auth-Z. They are
    # genuinely different keys, so two groups — but each carries a usable
    # reconnect target (slug fallback / real auth id respectively).
    groups = _snap._group_accounts_by_connection(rows)
    auth_ids = {g["authorization_id"] for g in groups}
    assert "auth-Z" in auth_ids


def test_stable_account_name_falls_back_when_no_digits():
    name = _snap._stable_account_name("VANGUARD", "ABCD")
    assert name == "Vanguard Account"


def test_stable_account_name_handles_empty_broker():
    name = _snap._stable_account_name("", "12345678")
    assert name == "Broker ••••5678"


# ---------------------------------------------------------------------------
# _bulk_sync_lookback_days — re-used from Schwab; assert SnapTrade
# inherits the SAME first-sync-vs-routine semantics so both connectors'
# bulk loops behave identically per row.
# ---------------------------------------------------------------------------


def test_bulk_lookback_first_sync_pending_picks_full_history():
    assert _bulk_sync_lookback_days(
        first_done=False, force_full_history=False, routine_days=60, full_days=1825,
    ) == 1825


def test_bulk_lookback_first_sync_done_picks_routine():
    assert _bulk_sync_lookback_days(
        first_done=True, force_full_history=False, routine_days=60, full_days=1825,
    ) == 60


def test_bulk_lookback_force_full_overrides_first_done():
    assert _bulk_sync_lookback_days(
        first_done=True, force_full_history=True, routine_days=60, full_days=1825,
    ) == 1825


# ---------------------------------------------------------------------------
# _sync_one_connection — never raises; failures land structured
# ---------------------------------------------------------------------------


@pytest.fixture
def _patched_models(monkeypatch):
    """Stub every Postgres helper _sync_one_connection touches so the
    test can run without a DB."""
    record = {
        "first_sync_marked": [],
        "broken_marked": [],
        "broken_cleared": [],
        "sync_attempts": [],
        "holdings_synced": [],
    }

    monkeypatch.setattr(_snap, "mark_snaptrade_first_sync_completed",
                        lambda u, a: record["first_sync_marked"].append((u, a)))
    monkeypatch.setattr(_snap, "mark_snaptrade_connection_broken",
                        lambda u, a: record["broken_marked"].append((u, a)))
    monkeypatch.setattr(_snap, "clear_snaptrade_connection_broken",
                        lambda u, a: record["broken_cleared"].append((u, a)))

    def _record_attempt(u, a, *, error=None):
        record["sync_attempts"].append((u, a, error))
    monkeypatch.setattr(_snap, "record_snaptrade_sync_attempt", _record_attempt)

    monkeypatch.setattr(_snap, "record_snaptrade_holdings_sync",
                        lambda u, a, when: record["holdings_synced"].append((u, a, when)))

    return record


def test_sync_one_returns_session_expired_when_no_snap_user(monkeypatch, _patched_models):
    """If the user has no snaptrade_connections row (or the SDK can't
    init), the wrapper must return a structured failure rather than
    raise. The bulk loop relies on this so one bad row doesn't kill
    the whole sync."""
    monkeypatch.setattr(_snap, "get_snaptrade_user", lambda u: None)
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: None)

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60,
    )
    assert res["ok"] is False
    assert res["error"] == "session_expired"
    assert _patched_models["sync_attempts"] == [(9, "abc", "session_expired")]


def test_sync_one_marks_connection_broken_on_auth_error(monkeypatch, _patched_models):
    """When the SDK raises an auth-shaped error (broker grant revoked),
    flip ``connection_broken_at`` so the banner surfaces and the cron
    stops retrying silently. Mirrors Schwab's
    ``mark_schwab_refresh_token_invalid``."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())

    def _boom(*args, **kwargs):
        raise _snap._SnapTradeAuthError(
            "get_user_account_positions", RuntimeError("401 Unauthorized"),
        )

    monkeypatch.setattr(_snap, "_run_sync", _boom)

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60,
    )
    assert res["ok"] is False
    assert res["error"] == "connection_broken"
    assert _patched_models["broken_marked"] == [(9, "abc")]


def test_sync_one_clears_broken_flag_and_marks_first_sync_on_success(monkeypatch, _patched_models):
    """Successful sync flips both flags exactly once: clear any stale
    broken-connection state AND mark first_sync_completed so the next
    cron picks the routine lookback."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_run_sync", lambda *a, **k: {
        "history_rows": 12,
        "current_rows": 5,
        "lookback_days": 1825,
        "github_pushed": True,
        "github_error": None,
        "github_head_sha": "deadbeef",
        "github_seed_push_skipped": False,
        "github_skip_reason": None,
    })

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=1825,
    )
    assert res["ok"] is True
    assert res["history_rows"] == 12
    assert res["current_rows"] == 5
    assert res["github_head_sha"] == "deadbeef"
    assert _patched_models["first_sync_marked"] == [(9, "abc")]
    assert _patched_models["broken_cleared"] == [(9, "abc")]
    assert _patched_models["sync_attempts"] == [(9, "abc", None)]


def test_sync_one_unknown_error_records_string_truncated(monkeypatch, _patched_models):
    """Unexpected exceptions don't crash the loop and don't flip the
    broken flag (broker side may be fine; the bug is on us). The
    failure is recorded with a truncated string so the UI can show
    something."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_run_sync",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("X" * 1000)))

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60,
    )
    assert res["ok"] is False
    assert res["error"] == "unknown"
    assert _patched_models["broken_marked"] == []  # NOT auth-shaped
    assert len(_patched_models["sync_attempts"]) == 1
    err = _patched_models["sync_attempts"][0][2]
    assert err is not None and len(err) <= 500


# ---------------------------------------------------------------------------
# force_refresh — "Sync now" must repoll the broker, not just re-read cache.
# ---------------------------------------------------------------------------


def _ok_run_sync(extra=None):
    base = {
        "history_rows": 3, "current_rows": 2, "lookback_days": 60,
        "github_pushed": True, "github_error": None, "github_head_sha": "sha1",
        "github_seed_push_skipped": False, "github_skip_reason": None,
        "github_no_changes": False,
    }
    if extra:
        base.update(extra)
    return base


def test_sync_one_force_refresh_calls_broker_repoll(monkeypatch, _patched_models):
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_run_sync", lambda *a, **k: _ok_run_sync())
    monkeypatch.setattr(_snap, "SNAPTRADE_FORCE_REFRESH_SETTLE_SECONDS", 0)

    calls = []
    monkeypatch.setattr(
        _snap, "_force_refresh_brokerage",
        lambda u, a, **k: calls.append((u, a)) or (True, "ok", None),
    )

    res = _snap._sync_one_connection(
        9, {"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60, force_refresh=True,
    )
    assert res["ok"] is True
    assert calls == [(9, "abc")], "force_refresh must trigger a broker repoll"


def test_sync_one_default_does_not_force_refresh(monkeypatch, _patched_models):
    """The daily cron path (force_refresh defaults False) must NOT incur the
    billed refresh call."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_run_sync", lambda *a, **k: _ok_run_sync())

    calls = []
    monkeypatch.setattr(
        _snap, "_force_refresh_brokerage",
        lambda u, a, **k: calls.append((u, a)) or (True, "ok", None),
    )

    _snap._sync_one_connection(
        9, {"snaptrade_account_id": "abc", "account_name": "X"}, lookback_days=60,
    )
    assert calls == [], "routine sync must not force (billed) refresh"


def test_sync_one_force_refresh_failure_is_non_fatal(monkeypatch, _patched_models):
    """A throttled/failed refresh must not abort the sync — we fall through to
    the normal read."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_run_sync", lambda *a, **k: _ok_run_sync())
    monkeypatch.setattr(_snap, "SNAPTRADE_FORCE_REFRESH_SETTLE_SECONDS", 0)
    monkeypatch.setattr(
        _snap, "_force_refresh_brokerage",
        lambda u, a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    res = _snap._sync_one_connection(
        9, {"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60, force_refresh=True,
    )
    assert res["ok"] is True, "refresh blowup must not fail the sync"


def test_sync_one_propagates_no_changes(monkeypatch, _patched_models):
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(
        _snap, "_run_sync",
        lambda *a, **k: _ok_run_sync({"github_pushed": False, "github_head_sha": None,
                                      "github_no_changes": True}),
    )
    res = _snap._sync_one_connection(
        9, {"snaptrade_account_id": "abc", "account_name": "X"}, lookback_days=60,
    )
    assert res["ok"] is True
    assert res["github_no_changes"] is True
    assert res["github_pushed"] is False


# ---------------------------------------------------------------------------
# _brokerage_authorization_disabled — authoritative "serving stale cache"
# detection. A disabled SnapTrade connection keeps returning the last
# cached positions/balances (so the fetch helpers succeed) while the row
# silently freezes. The only reliable signal is the brokerage
# authorization's own ``disabled`` flag (June 2026: user_id=9 Schwab
# accounts frozen on a June 8 snapshot, connection_status still 'active').
# ---------------------------------------------------------------------------


class _FakeAuthsClient:
    """Minimal SnapTrade SDK stub exposing just the connection-metadata
    surface ``_brokerage_authorization_disabled`` touches."""

    def __init__(self, authorizations, *, raise_on_list=False):
        self._authorizations = authorizations
        self._raise_on_list = raise_on_list

        class _Connections:
            def list_brokerage_authorizations(_self, *, user_id, user_secret):
                if raise_on_list:
                    raise RuntimeError("boom")
                return {"authorizations": authorizations}

        self.connections = _Connections()


_SNAP = {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"}


def test_authorization_disabled_true_when_flag_set():
    client = _FakeAuthsClient([
        {"id": "auth-1", "disabled": True},
        {"id": "auth-2", "disabled": False},
    ])
    acc_row = {"snaptrade_account_id": "abc", "brokerage_authorization_id": "auth-1"}
    assert _snap._brokerage_authorization_disabled(
        client, _SNAP, acc_row, user_id=9,
    ) is True


def test_authorization_disabled_false_when_enabled():
    client = _FakeAuthsClient([{"id": "auth-1", "disabled": False}])
    acc_row = {"snaptrade_account_id": "abc", "brokerage_authorization_id": "auth-1"}
    assert _snap._brokerage_authorization_disabled(
        client, _SNAP, acc_row, user_id=9,
    ) is False


def test_authorization_disabled_none_on_api_error():
    """A transient metadata error must return None ("change nothing") so a
    blip never false-flags a healthy connection as broken."""
    client = _FakeAuthsClient([], raise_on_list=True)
    acc_row = {"snaptrade_account_id": "abc", "brokerage_authorization_id": "auth-1"}
    assert _snap._brokerage_authorization_disabled(
        client, _SNAP, acc_row, user_id=9,
    ) is None


def test_authorization_disabled_none_when_auth_id_unresolvable():
    """No cached auth id AND no way to resolve one → None, never a flag."""
    client = _FakeAuthsClient([{"id": "auth-1", "disabled": True}])
    acc_row = {"snaptrade_account_id": "abc"}  # no brokerage_authorization_id
    # account_information missing on the stub → get_user_account_details
    # raises AttributeError, swallowed → None.
    assert _snap._brokerage_authorization_disabled(
        client, _SNAP, acc_row, user_id=9,
    ) is None


def test_authorization_disabled_none_when_no_matching_auth():
    """Auth id present but SnapTrade returns a different set → None."""
    client = _FakeAuthsClient([{"id": "other-auth", "disabled": True}])
    acc_row = {"snaptrade_account_id": "abc", "brokerage_authorization_id": "auth-1"}
    assert _snap._brokerage_authorization_disabled(
        client, _SNAP, acc_row, user_id=9,
    ) is None


# ---------------------------------------------------------------------------
# Holdings-freshness backstop — the "enabled-but-stalled" case the disabled
# flag misses. SnapTrade keeps returning 200 from a cached snapshot while its
# own sync_status.holdings.last_successful_sync stops advancing (June 2026:
# user_id=9 holdings frozen on a June-12 cache for 7 days, no banner). The
# authoritative freshness signal is that per-account timestamp.
# ---------------------------------------------------------------------------

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def test_parse_iso_datetime_handles_z_suffix_and_date_prefix():
    assert _snap._parse_iso_datetime("2026-06-12T20:06:11.123Z") == datetime(2026, 6, 12, 20, 6, 11, 123000)
    assert _snap._parse_iso_datetime("2026-06-12") == datetime(2026, 6, 12)
    # Already-parsed datetime is normalized to naive.
    assert _snap._parse_iso_datetime(datetime(2026, 6, 12, tzinfo=timezone.utc)) == datetime(2026, 6, 12)
    assert _snap._parse_iso_datetime(None) is None
    assert _snap._parse_iso_datetime("") is None
    assert _snap._parse_iso_datetime("not-a-date") is None


def test_holdings_last_successful_sync_extracts_nested_timestamp():
    summary = {"sync_status": {"holdings": {"last_successful_sync": "2026-06-12T20:06:11Z"}}}
    assert _snap._holdings_last_successful_sync(summary) == date(2026, 6, 12)


def test_holdings_last_successful_sync_none_when_signal_absent():
    # None-is-safe: any missing layer must yield None so a healthy connection
    # without the field is never false-flagged.
    assert _snap._holdings_last_successful_sync({}) is None
    assert _snap._holdings_last_successful_sync({"sync_status": {}}) is None
    assert _snap._holdings_last_successful_sync({"sync_status": {"holdings": {}}}) is None
    assert _snap._holdings_last_successful_sync(None) is None
    assert _snap._holdings_last_successful_sync({"sync_status": {"holdings": {"last_successful_sync": None}}}) is None


def test_holdings_stale_days_counts_days_since_last_sync():
    summary = {"sync_status": {"holdings": {"last_successful_sync": "2026-06-12T20:06:11Z"}}}
    assert _snap._holdings_stale_days(summary, today=date(2026, 6, 19)) == 7
    assert _snap._holdings_stale_days(summary, today=date(2026, 6, 12)) == 0


def test_holdings_stale_days_none_when_signal_missing_or_future():
    assert _snap._holdings_stale_days({}, today=date(2026, 6, 19)) is None
    future = {"sync_status": {"holdings": {"last_successful_sync": "2026-06-25T00:00:00Z"}}}
    assert _snap._holdings_stale_days(future, today=date(2026, 6, 19)) is None


# ---------------------------------------------------------------------------
# Honest "broker data as of" — persist SnapTrade's OWN holdings sync timestamp
# (NOT our cron's cache-read time) so the UI can publish real freshness.
# ---------------------------------------------------------------------------


def test_holdings_last_successful_sync_dt_keeps_full_timestamp():
    """The persisted value keeps time-of-day (the date-only variant powers the
    staleness backstop; the dt variant powers the freshness badge)."""
    summary = {"sync_status": {"holdings": {"last_successful_sync": "2026-06-12T20:06:11Z"}}}
    assert _snap._holdings_last_successful_sync_dt(summary) == datetime(2026, 6, 12, 20, 6, 11)
    # Date-only reduction stays consistent with the dt variant.
    assert _snap._holdings_last_successful_sync(summary) == date(2026, 6, 12)


def test_holdings_last_successful_sync_dt_none_when_signal_absent():
    assert _snap._holdings_last_successful_sync_dt({}) is None
    assert _snap._holdings_last_successful_sync_dt({"sync_status": {"holdings": {}}}) is None
    assert _snap._holdings_last_successful_sync_dt(None) is None


def test_sync_one_persists_holdings_last_successful_sync(monkeypatch, _patched_models):
    """A successful sync stamps the broker's honest 'data as of' timestamp
    that _run_sync surfaced from account_summary."""
    when = datetime(2026, 6, 19, 20, 6, 0)
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_run_sync",
                        lambda *a, **k: _ok_run_sync({"holdings_last_successful_sync": when}))

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60,
    )
    assert res["ok"] is True
    assert _patched_models["holdings_synced"] == [(9, "abc", when)]


def test_broker_data_freshness_uses_oldest_across_accounts(monkeypatch):
    """The always-on freshness strip must show the OLDEST timestamp across a
    user's accounts (the weakest link) so it never overstates how current the
    data is — and report whole days stale."""
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        {"holdings_last_successful_sync": datetime(2026, 6, 19, 20, 6, 0)},
        {"holdings_last_successful_sync": datetime(2026, 6, 12, 13, 0, 0)},  # oldest
        {"holdings_last_successful_sync": None},  # ignored
    ])
    as_of, stale_days = _snap.broker_data_freshness(99, today=date(2026, 6, 22))
    assert as_of == date(2026, 6, 12)
    assert stale_days == 10


def test_broker_data_freshness_none_when_no_timestamps(monkeypatch):
    """Cold start / unauthorized creds → no badge, never a fake date."""
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        {"holdings_last_successful_sync": None},
    ])
    assert _snap.broker_data_freshness(99, today=date(2026, 6, 22)) == (None, None)
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [])
    assert _snap.broker_data_freshness(99) == (None, None)


_ET = ZoneInfo("America/New_York")


def _et(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=_ET)


def _acct(acct_id, stamp, last_sync_at=None):
    return {
        "snaptrade_account_id": acct_id,
        "holdings_last_successful_sync": stamp,
        "last_sync_at": last_sync_at,
    }


def test_after_hours_scope_falls_back_to_last_sync_at(monkeypatch):
    """Some brokers never report holdings_last_successful_sync; fall back to our
    cache-read time (last_sync_at) so those accounts still qualify when we read
    SnapTrade's (real-time, post-close) cache after the bell."""
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        _acct("uuid-a", None, last_sync_at=_et(2026, 7, 7, 16, 30)),
        _acct("uuid-b", None, last_sync_at=_et(2026, 7, 7, 10, 0)),  # pre-close
    ])
    assert _snap.post_close_broker_tenant_ids(99, now=_et(2026, 7, 7, 17, 10)) == {
        "snaptrade:uuid-a",
    }


def test_after_hours_scope_includes_all_post_close_accounts(monkeypatch):
    """Every account synced at/after today's 4pm ET close is in scope, keyed by
    tenant_id (``snaptrade:<uuid>``)."""
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        _acct("uuid-a", _et(2026, 7, 7, 16, 5)),
        _acct("uuid-b", _et(2026, 7, 7, 17, 30)),
    ])
    assert _snap.post_close_broker_tenant_ids(99, now=_et(2026, 7, 7, 17, 10)) == {
        "snaptrade:uuid-a", "snaptrade:uuid-b",
    }


def test_after_hours_scope_drops_only_the_stale_account(monkeypatch):
    """A single mid-session (pre-close) account is DROPPED — not the whole
    section. The BE 2026-07-07 case: a stale account synced ~$295 mid-session
    would have shown a bogus +$25.88/sh gain, but a healthy post-close sibling
    still renders. This is the fix for 'nothing shows after hours'."""
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        _acct("uuid-fresh", _et(2026, 7, 7, 16, 5)),
        _acct("uuid-stale", _et(2026, 7, 7, 10, 30)),  # pre-close, e.g. broken conn
    ])
    assert _snap.post_close_broker_tenant_ids(99, now=_et(2026, 7, 7, 17, 10)) == {
        "snaptrade:uuid-fresh",
    }


def test_after_hours_scope_empty_before_the_close(monkeypatch):
    """During the open session (and pre-market) there is no settled close to
    compare against, so the scope is empty regardless of sync times."""
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        _acct("uuid-a", _et(2026, 7, 7, 13, 0)),
    ])
    assert _snap.post_close_broker_tenant_ids(99, now=_et(2026, 7, 7, 13, 5)) == set()


def test_after_hours_scope_empty_on_weekend(monkeypatch):
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        _acct("uuid-a", _et(2026, 7, 4, 18, 0)),
    ])
    # Saturday 2026-07-11 evening.
    assert _snap.post_close_broker_tenant_ids(99, now=_et(2026, 7, 11, 18, 0)) == set()


def test_after_hours_scope_empty_when_no_timestamps(monkeypatch):
    """Cold start / missing timestamp → never show (never fabricate drift)."""
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        _acct("uuid-a", None),
    ])
    assert _snap.post_close_broker_tenant_ids(99, now=_et(2026, 7, 7, 17, 10)) == set()
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [])
    assert _snap.post_close_broker_tenant_ids(99, now=_et(2026, 7, 7, 17, 10)) == set()


def test_after_hours_scope_handles_utc_naive_timestamps(monkeypatch):
    """Postgres TIMESTAMPTZ usually returns tz-aware datetimes, but if a naive
    one slips through we assume UTC. 21:00 UTC on 2026-07-07 = 17:00 ET, which
    is post-close."""
    monkeypatch.setattr(_snap, "get_snaptrade_accounts", lambda u: [
        _acct("uuid-a", datetime(2026, 7, 7, 21, 0)),  # naive UTC
    ])
    assert _snap.post_close_broker_tenant_ids(99, now=_et(2026, 7, 7, 17, 10)) == {
        "snaptrade:uuid-a",
    }


def test_sync_one_passes_none_holdings_sync_through(monkeypatch, _patched_models):
    """When SnapTrade omits the signal, _sync_one_connection still calls the
    persist helper with None (which the helper treats as a no-op) — so a
    missing timestamp never crashes and never clobbers a prior good value."""
    monkeypatch.setattr(_snap, "get_snaptrade_user",
                        lambda u: {"snaptrade_user_id": "snap-u", "snaptrade_secret": "s"})
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    # _ok_run_sync has no holdings key → result.get(...) returns None.
    monkeypatch.setattr(_snap, "_run_sync", lambda *a, **k: _ok_run_sync())

    res = _snap._sync_one_connection(
        user_id=9,
        acc_row={"snaptrade_account_id": "abc", "account_name": "X"},
        lookback_days=60,
    )
    assert res["ok"] is True
    assert _patched_models["holdings_synced"] == [(9, "abc", None)]


def _patch_run_sync_fetches(monkeypatch, *, account_summary):
    """Stub every external fetch in ``_run_sync`` so the only behavior under
    test is the holdings-freshness backstop. Disabled gate returns False so it
    doesn't pre-empt the staleness check."""
    monkeypatch.setattr(_snap, "_ensure_snaptrade_tenant_id", lambda **k: "snaptrade:t")
    monkeypatch.setattr(_snap, "_brokerage_authorization_disabled", lambda *a, **k: False)
    monkeypatch.setattr(_snap, "_fetch_activities", lambda *a, **k: [])
    monkeypatch.setattr(_snap, "_fetch_recent_orders", lambda *a, **k: [])
    monkeypatch.setattr(_snap, "_fetch_positions", lambda *a, **k: [])
    monkeypatch.setattr(_snap, "_fetch_option_holdings", lambda *a, **k: [])
    monkeypatch.setattr(_snap, "_fetch_balances", lambda *a, **k: [])
    monkeypatch.setattr(_snap, "_fetch_account_summary", lambda *a, **k: account_summary)


def test_run_sync_raises_on_stale_holdings(monkeypatch):
    """End-to-end wiring: a stale ``sync_status.holdings.last_successful_sync``
    escalates to the same _SnapTradeAuthError reconnect path as a disabled
    connection — even though every fetch 'succeeded'."""
    stale = (date.today() - timedelta(days=10)).strftime("%Y-%m-%dT12:00:00Z")
    _patch_run_sync_fetches(
        monkeypatch,
        account_summary={"sync_status": {"holdings": {"last_successful_sync": stale}}},
    )
    monkeypatch.setattr(_snap, "SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS", 4)

    acc_row = {"snaptrade_account_id": "abc", "account_name": "Schwab Account"}
    with pytest.raises(_snap._SnapTradeAuthError) as ei:
        _snap._run_sync(9, object(), snap=_SNAP, acc_row=acc_row, lookback_days=60)
    assert "stale" in ei.value.endpoint


def test_run_sync_does_not_flag_fresh_holdings(monkeypatch):
    """A connection SnapTrade refreshed today must NOT trip the backstop — it
    proceeds past the staleness gate into normalize/push (which we stub out by
    failing on the next external call to keep the test scoped)."""
    fresh = date.today().strftime("%Y-%m-%dT12:00:00Z")
    _patch_run_sync_fetches(
        monkeypatch,
        account_summary={"sync_status": {"holdings": {"last_successful_sync": fresh}}},
    )
    monkeypatch.setattr(_snap, "SNAPTRADE_HOLDINGS_STALE_AFTER_DAYS", 4)

    # Force normalize to raise a SENTINEL so we can prove control flow got PAST
    # the staleness gate without building a full seed-push harness.
    sentinel = RuntimeError("reached normalize")
    monkeypatch.setattr(
        _snap, "activities_to_history_df",
        lambda *a, **k: (_ for _ in ()).throw(sentinel),
    )
    acc_row = {"snaptrade_account_id": "abc", "account_name": "Schwab Account"}
    with pytest.raises(RuntimeError) as ei:
        _snap._run_sync(9, object(), snap=_SNAP, acc_row=acc_row, lookback_days=60)
    assert ei.value is sentinel  # NOT a _SnapTradeAuthError[stale]


# ---------------------------------------------------------------------------
# _connection_attention — proactive "X days" alert classification
# ---------------------------------------------------------------------------


def test_connection_attention_healthy_returns_none():
    row = {
        "snaptrade_account_id": "abc", "account_name": "X", "broker_slug": "schwab",
        "connection_broken_at": None,
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    assert _snap._connection_attention(row, today=date(2026, 6, 19)) is None


def test_connection_attention_broken_reports_stale_days():
    row = {
        "snaptrade_account_id": "abc", "account_name": "Sara Investment",
        "display_nickname": None, "broker_slug": "schwab",
        "connection_broken_at": datetime(2026, 6, 8, 12, 0, tzinfo=timezone.utc),
    }
    att = _snap._connection_attention(row, today=date(2026, 6, 19))
    assert att is not None
    assert att["kind"] == "stale"
    assert att["stale_days"] == 11
    assert att["expires_in_days"] is None
    assert att["account_name"] == "Sara Investment"


def test_connection_attention_broken_today_is_zero_days_not_negative():
    row = {
        "snaptrade_account_id": "abc", "account_name": "X", "broker_slug": "schwab",
        "connection_broken_at": datetime(2026, 6, 19, 23, 0, tzinfo=timezone.utc),
    }
    att = _snap._connection_attention(row, today=date(2026, 6, 19))
    assert att["kind"] == "stale" and att["stale_days"] == 0


def test_connection_attention_expiring_uses_heuristic_lifetime(monkeypatch):
    """When a broker has an operator-verified token lifetime, count down to
    expiry within the warn window."""
    monkeypatch.setitem(
        _snap.SNAPTRADE_BROKER_CONNECTION_LIFETIME_DAYS, "demobroker", 90,
    )
    # created 2026-03-25 + 90d = 2026-06-23; today 2026-06-19 -> 4 days out.
    row = {
        "snaptrade_account_id": "abc", "account_name": "X", "broker_slug": "demobroker",
        "connection_broken_at": None,
        "created_at": datetime(2026, 3, 25, tzinfo=timezone.utc),
    }
    att = _snap._connection_attention(row, today=date(2026, 6, 19))
    assert att is not None
    assert att["kind"] == "expiring"
    assert att["expires_in_days"] == 4
    assert att["stale_days"] is None


def test_connection_attention_expiring_silent_outside_warn_window(monkeypatch):
    monkeypatch.setitem(
        _snap.SNAPTRADE_BROKER_CONNECTION_LIFETIME_DAYS, "demobroker", 90,
    )
    # created today + 90d expiry is far away -> no alert.
    row = {
        "snaptrade_account_id": "abc", "account_name": "X", "broker_slug": "demobroker",
        "connection_broken_at": None,
        "created_at": datetime(2026, 6, 19, tzinfo=timezone.utc),
    }
    assert _snap._connection_attention(row, today=date(2026, 6, 19)) is None


def test_connection_attention_default_lifetime_map_is_empty():
    """Guard the trust rule: no broker ships a guessed forward countdown by
    default — the map must stay empty until a lifetime is operator-verified."""
    assert _snap.SNAPTRADE_BROKER_CONNECTION_LIFETIME_DAYS == {}


def test_connection_reminder_week_index_is_weekly_after_week_zero():
    """The cron dedupe key uses ``stale_days // 7``; week 0 is owned by the
    one-time connection_dropped email, so reminders start at week 1 (day 7)
    and advance one band per 7 days."""
    assert 6 // 7 == 0    # day 6 -> still week 0 (no reminder)
    assert 7 // 7 == 1    # day 7 -> first weekly reminder
    assert 13 // 7 == 1   # day 13 -> same band, deduped
    assert 14 // 7 == 2   # day 14 -> next reminder


# ---------------------------------------------------------------------------
# _looks_like_auth_error — string-shape heuristic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("msg,expected", [
    ("401 Unauthorized", True),
    ("403 Forbidden", True),
    ("Authorization disabled by user", True),
    ("Authorization expired — please reconnect", True),
    ("AUTH REVOKED at the broker", True),
    ("Reconnect required", True),
    ("500 Internal Server Error", False),
    ("Network timeout", False),
    ("Account not found", False),
])
def test_looks_like_auth_error_pattern_matches(msg, expected):
    """Heuristic only — ANY new auth-shaped string SnapTrade ships
    needs to land in this list so the cron flips the broken flag and
    stops silently retrying."""
    assert _snap._looks_like_auth_error(RuntimeError(msg)) is expected


def test_fetch_recent_orders_swallows_403_when_activities_already_succeeded():
    """Fidelity-via-SnapTrade ships a bare ``(403)`` on the orders
    endpoint for brand-new connections that legitimately have no recent
    orders (or whose account class doesn't expose the order stream).
    The orders endpoint is the real-time fallback to activities — by the
    time ``_fetch_recent_orders`` is called, ``_fetch_activities`` has
    already proven the auth is valid. Escalating a 403 here was
    misclassifying the connection as broken and locking users out of
    their fresh Fidelity link (production bug, 2026-05-15).

    The non-fatal contract: any exception from this endpoint logs and
    returns ``[]``; only ``_fetch_activities`` / ``_fetch_positions`` /
    ``_fetch_balances`` may raise ``_SnapTradeAuthError``."""

    class _ApiException(Exception):
        def __str__(self):
            return "(403)"

    class _Boom:
        class account_information:
            @staticmethod
            def get_user_account_recent_orders(**_):
                raise _ApiException()

    result = _snap._fetch_recent_orders(_Boom, "u", "s", "acc")
    assert result == []


def test_fetch_positions_still_raises_auth_error_on_403():
    """Counter-test to the orders-endpoint relaxation: positions IS one
    of the authoritative auth surfaces (alongside activities/balances/
    details). A 403 there must still escalate so a genuinely-revoked
    grant gets caught and flagged."""

    class _ApiException(Exception):
        def __str__(self):
            return "(403)"

    class _Boom:
        class account_information:
            @staticmethod
            def get_user_account_positions(**_):
                raise _ApiException()

    with pytest.raises(_snap._SnapTradeAuthError) as exc_info:
        _snap._fetch_positions(_Boom, "u", "s", "acc")
    assert exc_info.value.endpoint == "get_user_account_positions"


# ---------------------------------------------------------------------------
# Postgres helpers — pure SQL spies (no DB)
# ---------------------------------------------------------------------------


class _ExecuteSpy:
    def __init__(self):
        self.calls = []

    def __call__(self, sql, params=None):
        self.calls.append((sql, params))


def test_save_snaptrade_user_upserts_with_clear_semantics(monkeypatch):
    """``save_snaptrade_user`` must be a pure UPSERT (idempotent on
    re-register). Tests pin the SQL shape so a refactor that changes
    ON CONFLICT semantics can't quietly change re-register behaviour."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    _models.save_snaptrade_user(7, "snap-user-id", "snap-secret")

    assert len(spy.calls) == 1
    sql, params = spy.calls[0]
    assert "INSERT INTO snaptrade_connections" in sql
    assert "ON CONFLICT (user_id) DO UPDATE" in sql
    assert params == (7, "snap-user-id", "snap-secret")


def test_upsert_snaptrade_account_resets_broken_flag(monkeypatch):
    """A re-link of a previously-broken account (user reconnected
    through SnapTrade) must clear ``connection_broken_at`` so the
    banner dismisses on the next page load. Same ergonomic as
    ``save_schwab_connection`` clearing ``refresh_token_invalid_at``."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    _models.upsert_snaptrade_account(
        7, "abc",
        broker_slug="FIDELITY",
        account_number_masked="****1234",
        account_name="Fidelity ••••1234",
    )

    sql, _ = spy.calls[0]
    assert "connection_broken_at  = NULL" in sql or "connection_broken_at = NULL" in sql.replace("  ", " ")


def test_get_snaptrade_accounts_short_circuits_for_none_user(monkeypatch):
    """Anonymous request must NOT query the DB."""
    called = {"n": 0}

    def _spy(sql, params):
        called["n"] += 1
        return []

    monkeypatch.setattr(_models, "fetch_all", _spy)
    assert _models.get_snaptrade_accounts(None) == []
    assert called["n"] == 0


def test_get_expired_snaptrade_accounts_swallows_db_errors(monkeypatch):
    """Banner inject context-processor must not blow up the page on a
    transient DB hiccup. Same defensiveness as the Schwab
    equivalent."""
    def _boom(sql, params):
        raise RuntimeError("db down")

    monkeypatch.setattr(_models, "fetch_all", _boom)
    assert _models.get_expired_snaptrade_accounts(7) == []


def test_get_snaptrade_account_nicknames_falls_back_to_account_name(monkeypatch):
    monkeypatch.setattr(
        _models, "fetch_all",
        lambda sql, params: [
            {"account_name": "Fidelity ••••1234", "display_nickname": None},
            {"account_name": "Vanguard ••••5678", "display_nickname": "  "},
            {"account_name": "Robinhood Account", "display_nickname": "Trading"},
        ],
    )
    out = _models.get_snaptrade_account_nicknames(7)
    assert out == {
        "Fidelity ••••1234": "Fidelity ••••1234",
        "Vanguard ••••5678": "Vanguard ••••5678",
        "Robinhood Account": "Trading",
    }


def test_get_snaptrade_account_nicknames_drops_ambiguous_colliding_names(monkeypatch):
    """Five physical accounts can all be named "Schwab Account" (SnapTrade
    generic label) with five DIFFERENT nicknames. A {name: nick} map would
    keep an arbitrary winner and relabel every surface rendering the raw
    warehouse label with the WRONG nickname. Ambiguous names must be
    dropped so the raw label passes through instead of lying."""
    monkeypatch.setattr(
        _models, "fetch_all",
        lambda sql, params: [
            {"account_name": "Schwab Account", "display_nickname": "Emmory"},
            {"account_name": "Schwab Account", "display_nickname": "Sara 401k"},
            {"account_name": "Schwab Account", "display_nickname": "Cameron Investment"},
            {"account_name": "Alpaca Paper Account", "display_nickname": "Testing"},
        ],
    )
    out = _models.get_snaptrade_account_nicknames(7)
    assert "Schwab Account" not in out
    assert out == {"Alpaca Paper Account": "Testing"}


def test_get_snaptrade_account_nicknames_keeps_consistent_duplicates(monkeypatch):
    """Two rows with the same name AND the same nickname are not ambiguous."""
    monkeypatch.setattr(
        _models, "fetch_all",
        lambda sql, params: [
            {"account_name": "Schwab Account", "display_nickname": "Family"},
            {"account_name": "Schwab Account", "display_nickname": "Family"},
        ],
    )
    assert _models.get_snaptrade_account_nicknames(7) == {"Schwab Account": "Family"}


def test_account_label_map_drops_ambiguous_colliding_names(monkeypatch):
    """routes._account_label_map mirrors the collision guard: colliding
    account_name with divergent nicknames must not map (the df.map()
    consumer would stamp one arbitrary nickname onto all 5 accounts)."""
    import app.routes as _routes

    # _account_label_map does `from app.models import get_broker_tenants_for_user`
    # at call time — patch the source module.
    monkeypatch.setattr(
        _models, "get_broker_tenants_for_user",
        lambda uid: [
            {"account_name": "Schwab Account", "display_nickname": "Emmory"},
            {"account_name": "Schwab Account", "display_nickname": "Sara 401k"},
            {"account_name": "IBKR ••••7930", "display_nickname": "Margin"},
        ],
    )
    out = _routes._account_label_map(7)
    assert "Schwab Account" not in out
    assert out == {"IBKR ••••7930": "Margin"}


def test_record_sync_attempt_updates_last_at_and_error(monkeypatch):
    """Tests the SQL contract ``record_snaptrade_sync_attempt`` writes:
    must SET both ``last_sync_at`` (always) and ``last_sync_error``
    (NULL on success, message on failure)."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    _models.record_snaptrade_sync_attempt(7, "abc", error=None)
    _models.record_snaptrade_sync_attempt(7, "abc", error="boom")

    assert len(spy.calls) == 2
    for sql, _ in spy.calls:
        assert "SET last_sync_at = NOW()" in sql
        assert "last_sync_error = %s" in sql
    assert spy.calls[0][1] == (None, 7, "abc")
    assert spy.calls[1][1] == ("boom", 7, "abc")


# ---------------------------------------------------------------------------
# Force-refresh throttle — must NOT call SnapTrade's billed endpoint
# when the user pressed Refresh-from-broker recently. SnapTrade charges
# per call so a rogue caller pinning the button could rack up our bill
# in seconds without this gate.
# ---------------------------------------------------------------------------


def test_force_refresh_throttles_when_last_refresh_recent(monkeypatch):
    """Pressing Refresh-from-broker within the throttle window MUST NOT
    call SnapTrade's billed ``refresh_brokerage_authorization`` endpoint.
    The route should return ``ok=False`` with a user-friendly message
    that mentions the wait time, and the SDK must NOT be called at all.
    """
    from datetime import datetime, timezone, timedelta

    # Simulate "user just pressed it 30 seconds ago" — well inside any
    # reasonable throttle window.
    fake_acc = {
        "snaptrade_account_id": "acc-id-1",
        "first_sync_completed": True,
        "brokerage_authorization_id": "auth-1",
        "last_force_refresh_at": datetime.now(timezone.utc) - timedelta(seconds=30),
    }
    monkeypatch.setattr(_snap, "_snaptrade_config", lambda: ("cid", "ckey", "redir"))
    monkeypatch.setattr(_snap, "get_snaptrade_user", lambda uid: {
        "snaptrade_user_id": "u", "snaptrade_secret": "s",
    })
    monkeypatch.setattr(_snap, "get_snaptrade_account", lambda uid, aid: fake_acc)

    sdk_call_count = {"refresh": 0, "details": 0}

    class _MockClient:
        class connections:
            @staticmethod
            def refresh_brokerage_authorization(**kwargs):
                sdk_call_count["refresh"] += 1
                return {"detail": "Connection scheduled for refresh"}

        class account_information:
            @staticmethod
            def get_user_account_details(**kwargs):
                sdk_call_count["details"] += 1
                return type("R", (), {"body": {"brokerage_authorization": "auth-1"}})()

    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: _MockClient())

    ok, message, throttle_remaining = _snap._force_refresh_brokerage(
        user_id=7, snaptrade_account_id="acc-id-1", throttle_seconds=600,
    )

    assert ok is False, "Throttled refresh must return ok=False"
    assert sdk_call_count["refresh"] == 0, \
        "Throttle must NOT call SnapTrade's billed refresh endpoint"
    assert sdk_call_count["details"] == 0, \
        "Throttle must short-circuit BEFORE any SnapTrade call (no auth-id lookup either)"
    assert throttle_remaining is not None and 500 <= throttle_remaining <= 600, \
        f"Should report ~570s remaining, got {throttle_remaining}"
    assert "minute" in message.lower(), \
        f"User-facing message should mention wait time; got: {message!r}"


def test_force_refresh_calls_sdk_when_throttle_window_elapsed(monkeypatch):
    """Once the throttle window has fully elapsed, the next press
    SHOULD call SnapTrade and stamp ``last_force_refresh_at = NOW()``
    so the next-next press is throttled fresh.
    """
    from datetime import datetime, timezone, timedelta

    fake_acc = {
        "snaptrade_account_id": "acc-id-2",
        "first_sync_completed": True,
        "brokerage_authorization_id": "auth-2",
        "last_force_refresh_at": datetime.now(timezone.utc) - timedelta(hours=1),
    }
    monkeypatch.setattr(_snap, "_snaptrade_config", lambda: ("cid", "ckey", "redir"))
    monkeypatch.setattr(_snap, "get_snaptrade_user", lambda uid: {
        "snaptrade_user_id": "u", "snaptrade_secret": "s",
    })
    monkeypatch.setattr(_snap, "get_snaptrade_account", lambda uid, aid: fake_acc)

    sdk_call_count = {"refresh": 0, "details": 0}
    stamp_calls = []

    class _MockClient:
        class connections:
            @staticmethod
            def refresh_brokerage_authorization(**kwargs):
                sdk_call_count["refresh"] += 1
                assert kwargs.get("authorization_id") == "auth-2"
                return {"detail": "Connection scheduled for refresh"}

        class account_information:
            @staticmethod
            def get_user_account_details(**kwargs):
                sdk_call_count["details"] += 1
                return type("R", (), {"body": {"brokerage_authorization": "auth-2"}})()

    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: _MockClient())
    monkeypatch.setattr(
        _snap, "stamp_snaptrade_force_refresh_attempt",
        lambda uid, aid: stamp_calls.append((uid, aid)),
    )

    ok, message, _ = _snap._force_refresh_brokerage(
        user_id=7, snaptrade_account_id="acc-id-2", throttle_seconds=600,
    )

    assert ok is True, f"Refresh past throttle should succeed; got: {message!r}"
    assert sdk_call_count["refresh"] == 1, "Refresh endpoint should be called exactly once"
    assert sdk_call_count["details"] == 0, \
        "Cached brokerage_authorization_id must be re-used (no extra detail call)"
    assert stamp_calls == [(7, "acc-id-2")], \
        "Must stamp last_force_refresh_at on success for the next throttle window"


def test_force_refresh_caches_authorization_id_on_first_call(monkeypatch):
    """If the brokerage_authorization_id is not yet cached on the row,
    the first refresh fetches it via ``get_user_account_details`` and
    persists it to Postgres so subsequent refreshes skip the lookup."""
    fake_acc = {
        "snaptrade_account_id": "acc-id-3",
        "first_sync_completed": True,
        "brokerage_authorization_id": None,  # not cached yet
        "last_force_refresh_at": None,        # never refreshed
    }
    monkeypatch.setattr(_snap, "_snaptrade_config", lambda: ("cid", "ckey", "redir"))
    monkeypatch.setattr(_snap, "get_snaptrade_user", lambda uid: {
        "snaptrade_user_id": "u", "snaptrade_secret": "s",
    })
    monkeypatch.setattr(_snap, "get_snaptrade_account", lambda uid, aid: fake_acc)

    set_calls = []
    monkeypatch.setattr(
        _snap, "set_snaptrade_brokerage_authorization_id",
        lambda uid, aid, auth: set_calls.append((uid, aid, auth)),
    )
    monkeypatch.setattr(_snap, "stamp_snaptrade_force_refresh_attempt", lambda uid, aid: None)

    refresh_kwargs = {}

    class _MockClient:
        class connections:
            @staticmethod
            def refresh_brokerage_authorization(**kwargs):
                refresh_kwargs.update(kwargs)
                return {"detail": "Connection scheduled for refresh"}

        class account_information:
            @staticmethod
            def get_user_account_details(**kwargs):
                return type("R", (), {"body": {
                    "brokerage_authorization": "newly-discovered-auth-uuid",
                }})()

    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: _MockClient())

    ok, _msg, _ = _snap._force_refresh_brokerage(
        user_id=7, snaptrade_account_id="acc-id-3",
    )
    assert ok is True
    # Auth ID must be (a) used in the refresh call, (b) cached for next time.
    assert refresh_kwargs.get("authorization_id") == "newly-discovered-auth-uuid"
    assert set_calls == [(7, "acc-id-3", "newly-discovered-auth-uuid")], \
        "Must cache the discovered authorization_id so subsequent presses skip the lookup"


def test_force_refresh_returns_friendly_error_on_unconfigured_server(monkeypatch):
    """If SnapTrade env vars aren't set, the route should NOT raise —
    it should return a user-friendly message. Mirrors how /snaptrade/sync
    behaves on the same condition."""
    monkeypatch.setattr(_snap, "_snaptrade_config", lambda: None)
    ok, message, _ = _snap._force_refresh_brokerage(user_id=7, snaptrade_account_id="x")
    assert ok is False
    assert "not configured" in message.lower()


def test_stamp_force_refresh_attempt_writes_correct_sql(monkeypatch):
    """Pin the SQL ``stamp_snaptrade_force_refresh_attempt`` writes —
    must SET ``last_force_refresh_at = NOW()`` and ``updated_at = NOW()``
    so the throttle gate downstream reads the right column."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    _models.stamp_snaptrade_force_refresh_attempt(7, "acc-1")

    assert len(spy.calls) == 1
    sql, params = spy.calls[0]
    assert "SET last_force_refresh_at = NOW()" in sql
    assert "updated_at = NOW()" in sql
    assert params == (7, "acc-1")


def test_set_brokerage_authorization_id_writes_correct_sql(monkeypatch):
    """Pin the SQL contract for the auth-id cache writer."""
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    ok = _models.set_snaptrade_brokerage_authorization_id(7, "acc-1", "auth-uuid-xyz")
    assert ok is True
    assert len(spy.calls) == 1
    sql, params = spy.calls[0]
    assert "SET brokerage_authorization_id = %s" in sql
    assert "updated_at = NOW()" in sql
    assert params == ("auth-uuid-xyz", 7, "acc-1")


def test_set_brokerage_authorization_id_short_circuits_on_empty_input():
    """Empty/None auth ids must NOT issue a write — those would NULL
    out a previously-cached value and force a re-lookup on every
    subsequent refresh, defeating the whole point of caching."""
    assert _models.set_snaptrade_brokerage_authorization_id(7, "acc-1", None) is False
    assert _models.set_snaptrade_brokerage_authorization_id(7, "acc-1", "") is False
    assert _models.set_snaptrade_brokerage_authorization_id(7, "acc-1", "   ") is False
