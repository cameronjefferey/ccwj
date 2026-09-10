"""Tests for the SnapTrade backstop cron CLI (``app.snaptrade_sync_cli``).

The cron syncs every account with ``defer_push=True`` (fetch + normalize, no
commit) and then pushes ONE batched commit for all of them — replacing the old
one-commit-per-account fan-out that produced ~14 workflow runs a night. These
pin: each account is synced deferred, exactly one batched push happens, broken
connections are skipped (not batched) and notified, and the exit code contract
holds.
"""
import pandas as pd
import pytest

import app.snaptrade_sync_cli as cli
from app import models as _models
from app import snaptrade as _snap
from app import upload as _upload


def _row(uid, acct_id, name, first_done=True):
    return {
        "user_id": uid,
        "snaptrade_account_id": acct_id,
        "account_name": name,
        "first_sync_completed": first_done,
        "broker_slug": "schwab",
    }


def _frames(name, uid, tenant, *, skip_history=False):
    # Tiny real DataFrames — the CLI's commit-message builder calls len() on
    # them; the real merge semantics are unit-tested in test_upload_merge.py.
    return {
        "account_name": name,
        "tenant_id": tenant,
        "history_df": None if skip_history else pd.DataFrame([{"Symbol": "AAPL"}]),
        "current_df": pd.DataFrame([{"Symbol": "AAPL"}]),
        "balances_df": None,
        "skip_history": skip_history,
        "user_id": uid,
    }


def _ok(
    name,
    uid,
    tenant,
    *,
    hist=3,
    cur=5,
    skip_history=False,
    transactions_ready=True,
):
    return {
        "ok": True, "error": None,
        "history_rows": hist, "current_rows": cur,
        "transactions_initial_sync_completed": transactions_ready,
        "deferred": True,
        "frames": _frames(name, uid, tenant, skip_history=skip_history),
    }


@pytest.fixture
def _wire(monkeypatch):
    """Wire up the CLI's external deps; return a dict for per-test overrides."""
    monkeypatch.setattr(_models, "init_db", lambda: None)
    monkeypatch.setattr(_snap, "snaptrade_enabled", lambda: True)
    monkeypatch.setattr(_snap, "_get_snaptrade_client", lambda: object())
    monkeypatch.setattr(_snap, "_routine_lookback_days", lambda: 60)
    monkeypatch.setattr(_snap, "SNAPTRADE_FULL_HISTORY_LOOKBACK_DAYS", 3650, raising=False)
    monkeypatch.setattr(_snap, "_bulk_sync_lookback_days",
                        lambda first_done, **k: 60 if first_done else 3650)
    monkeypatch.setattr(_upload, "_upload_github_config_ok", lambda: (True, None))
    monkeypatch.setattr(cli, "_notify_connection_dropped", lambda *a, **k: None)

    calls = {"batch": [], "synced": [], "first_sync_marked": []}
    monkeypatch.setattr(
        _snap,
        "mark_snaptrade_first_sync_completed",
        lambda user_id, account_id: calls["first_sync_marked"].append(
            (user_id, account_id)
        ),
    )

    def _fake_batch(entries, *, commit_message):
        calls["batch"].append({"entries": list(entries), "message": commit_message})
        return True, None, "sha123", False, len(entries)

    monkeypatch.setattr(_upload, "merge_and_push_seeds_batch", _fake_batch)
    return calls


def test_cron_syncs_deferred_and_pushes_one_batch(_wire, monkeypatch):
    rows = [
        _row(9, "a1", "Schwab Account"),
        _row(9, "a2", "Schwab Account"),
        _row(18, "a3", "Alpaca Paper Account"),
    ]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)

    results = {
        "a1": _ok("Schwab Account", 9, "snaptrade:t-a1"),
        "a2": _ok("Schwab Account", 9, "snaptrade:t-a2"),
        "a3": _ok("Alpaca Paper Account", 18, "snaptrade:t-a3", skip_history=True),
    }
    seen_defer = []
    seen_skip = []

    def _fake_sync(user_id, row, *, lookback_days, defer_push=False, skip_activities=False, history_only=False):
        seen_defer.append(defer_push)
        seen_skip.append(skip_activities)
        return results[row["snaptrade_account_id"]]

    monkeypatch.setattr(_snap, "_sync_one_connection", _fake_sync)

    rc = cli.main()
    assert rc == 0
    # Every account synced in deferred mode.
    assert seen_defer == [True, True, True]
    # Plain backstop run does NOT skip activities (reads the T+1 feed).
    assert seen_skip == [False, False, False]
    # Exactly ONE batched push, carrying all three accounts.
    assert len(_wire["batch"]) == 1
    assert len(_wire["batch"][0]["entries"]) == 3


def test_cron_skips_broken_connection_from_batch(_wire, monkeypatch):
    rows = [_row(9, "a1", "Schwab Account"), _row(9, "a2", "Schwab Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)

    def _fake_sync(user_id, row, *, lookback_days, defer_push=False, skip_activities=False, history_only=False):
        if row["snaptrade_account_id"] == "a2":
            return {"ok": False, "error": "connection_broken"}
        return _ok("Schwab Account", 9, "snaptrade:t-a1")

    monkeypatch.setattr(_snap, "_sync_one_connection", _fake_sync)

    rc = cli.main()
    assert rc == 0
    # Only the healthy account is in the batch.
    assert len(_wire["batch"]) == 1
    assert len(_wire["batch"][0]["entries"]) == 1


def test_cron_all_failed_returns_1_and_no_push(_wire, monkeypatch):
    rows = [_row(9, "a1", "Schwab Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(
        _snap, "_sync_one_connection",
        lambda *a, **k: {"ok": False, "error": "session_expired"},
    )
    rc = cli.main()
    assert rc == 1
    assert _wire["batch"] == []


def test_cron_no_accounts_returns_0(_wire, monkeypatch):
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: [])
    assert cli.main() == 0
    assert _wire["batch"] == []


# ---------------------------------------------------------------------------
# Market-close force-refresh pass (--force-refresh)
# ---------------------------------------------------------------------------

def test_force_refresh_enabled_parses_flag_and_env(monkeypatch):
    assert cli._force_refresh_enabled([]) is False
    assert cli._force_refresh_enabled(["--force-refresh"]) is True
    monkeypatch.setenv("SNAPTRADE_CRON_FORCE_REFRESH", "1")
    assert cli._force_refresh_enabled([]) is True
    monkeypatch.setenv("SNAPTRADE_CRON_FORCE_REFRESH", "0")
    assert cli._force_refresh_enabled([]) is False


def test_default_run_does_not_force_refresh(_wire, monkeypatch):
    """The plain 23:00 backstop must NOT call the billed force-refresh API."""
    rows = [_row(9, "a1", "Schwab Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(
        _snap, "_sync_one_connection",
        lambda *a, **k: _ok("Schwab Account", 9, "snaptrade:t-a1"),
    )

    refreshed = []
    monkeypatch.setattr(
        _snap, "_force_refresh_brokerage",
        lambda *a, **k: refreshed.append(a) or (True, "ok", None),
    )
    # No flag, no env → force_refresh stays off.
    monkeypatch.setattr(cli, "_force_refresh_enabled", lambda *a, **k: False)

    assert cli.main() == 0
    assert refreshed == []  # never billed a refresh
    assert len(_wire["batch"]) == 1


def test_force_refresh_repolls_every_account_then_syncs(_wire, monkeypatch):
    """--force-refresh fires one repoll per account UP FRONT, then reads +
    pushes one batch. Settle sleep is zeroed so the test doesn't wait 90s."""
    rows = [
        _row(9, "a1", "Schwab Account"),
        _row(18, "a2", "Alpaca Paper Account"),
    ]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(cli, "_force_refresh_enabled", lambda *a, **k: True)
    monkeypatch.setattr(_snap, "SNAPTRADE_CRON_FORCE_REFRESH_SETTLE_SECONDS", 0, raising=False)

    order = []

    def _fake_refresh(user_id, acct_id, **k):
        order.append(("refresh", acct_id))
        return (True, "Asked your broker to send fresh data.", None)

    def _fake_sync(user_id, row, *, lookback_days, defer_push=False, skip_activities=False, history_only=False):
        order.append(("sync", row["snaptrade_account_id"]))
        return _ok(row["account_name"], user_id, f"snaptrade:t-{row['snaptrade_account_id']}")

    monkeypatch.setattr(_snap, "_force_refresh_brokerage", _fake_refresh)
    monkeypatch.setattr(_snap, "_sync_one_connection", _fake_sync)

    rc = cli.main()
    assert rc == 0
    # BOTH refreshes fire BEFORE any read (single settle window in between).
    assert order == [
        ("refresh", "a1"), ("refresh", "a2"),
        ("sync", "a1"), ("sync", "a2"),
    ]
    # Still exactly one batched push.
    assert len(_wire["batch"]) == 1
    assert len(_wire["batch"][0]["entries"]) == 2
    # Commit message distinguishes this pass from the nightly backstop.
    assert "force-refresh" in _wire["batch"][0]["message"]


def test_force_refresh_survives_a_refresh_error(_wire, monkeypatch):
    """A raised/failed refresh is non-fatal — the sync still runs for all."""
    rows = [_row(9, "a1", "Schwab Account"), _row(9, "a2", "Schwab Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(cli, "_force_refresh_enabled", lambda *a, **k: True)
    monkeypatch.setattr(_snap, "SNAPTRADE_CRON_FORCE_REFRESH_SETTLE_SECONDS", 0, raising=False)

    def _fake_refresh(user_id, acct_id, **k):
        if acct_id == "a1":
            raise RuntimeError("boom")
        return (True, "ok", None)

    monkeypatch.setattr(_snap, "_force_refresh_brokerage", _fake_refresh)
    monkeypatch.setattr(
        _snap, "_sync_one_connection",
        lambda user_id, row, **k: _ok(row["account_name"], user_id, "snaptrade:t"),
    )

    rc = cli.main()
    assert rc == 0
    # Both accounts still synced + pushed despite the a1 refresh raising.
    assert len(_wire["batch"][0]["entries"]) == 2


# ---------------------------------------------------------------------------
# Intraday real-time-orders poll (--intraday)
# ---------------------------------------------------------------------------

def test_intraday_enabled_parses_flag_and_env(monkeypatch):
    assert cli._intraday_enabled([]) is False
    assert cli._intraday_enabled(["--intraday"]) is True
    monkeypatch.setenv("SNAPTRADE_CRON_INTRADAY", "1")
    assert cli._intraday_enabled([]) is True
    monkeypatch.setenv("SNAPTRADE_CRON_INTRADAY", "0")
    assert cli._intraday_enabled([]) is False


def test_intraday_skips_activities_and_never_force_refreshes(_wire, monkeypatch):
    """--intraday syncs every account with skip_activities=True (real-time
    orders only) and NEVER calls the billed force-refresh, even if the
    force-refresh env happens to be set (intraday takes precedence)."""
    rows = [_row(9, "a1", "Schwab Account"), _row(18, "a2", "Alpaca Paper Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(cli, "_intraday_enabled", lambda *a, **k: True)
    # Even with force-refresh "on", intraday must suppress it.
    monkeypatch.setattr(cli, "_force_refresh_enabled", lambda *a, **k: True)

    refreshed = []
    monkeypatch.setattr(
        _snap, "_force_refresh_brokerage",
        lambda *a, **k: refreshed.append(a) or (True, "ok", None),
    )

    seen_skip = []

    def _fake_sync(user_id, row, *, lookback_days, defer_push=False, skip_activities=False, history_only=False):
        seen_skip.append(skip_activities)
        return _ok(row["account_name"], user_id, f"snaptrade:t-{row['snaptrade_account_id']}")

    monkeypatch.setattr(_snap, "_sync_one_connection", _fake_sync)

    rc = cli.main()
    assert rc == 0
    # Every account read with skip_activities=True.
    assert seen_skip == [True, True]
    # Never touched the billed refresh endpoint.
    assert refreshed == []
    # One batched push, commit message tagged "intraday".
    assert len(_wire["batch"]) == 1
    assert "intraday" in _wire["batch"][0]["message"]


def test_intraday_first_sync_fetches_full_data_and_marks_after_batch(_wire, monkeypatch):
    """Orders-only mode must not complete a new account's first sync.

    The first run needs activities + snapshots, and the Postgres flag may flip
    only after the deferred seed batch is durable.
    """
    rows = [_row(14, "new-account", "Fidelity Account", first_done=False)]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(cli, "_intraday_enabled", lambda *a, **k: True)

    seen = []

    def _fake_sync(
        user_id, row, *, lookback_days, defer_push=False,
        skip_activities=False, history_only=False,
    ):
        seen.append((lookback_days, defer_push, skip_activities, history_only))
        return _ok(row["account_name"], user_id, "snaptrade:new")

    monkeypatch.setattr(_snap, "_sync_one_connection", _fake_sync)

    assert cli.main() == 0
    assert seen == [(3650, True, False, False)]
    assert _wire["first_sync_marked"] == [(14, "new-account")]


@pytest.mark.parametrize(("hist", "skip_history"), [(0, True), (3, False)])
def test_incomplete_transaction_sync_stays_pending_after_batch(
    _wire, monkeypatch, hist, skip_history,
):
    """Neither a snapshot nor recent orders prove the archive is complete."""
    rows = [_row(14, "new-account", "Fidelity Account", first_done=False)]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(
        _snap,
        "_sync_one_connection",
        lambda user_id, row, **kwargs: _ok(
            row["account_name"],
            user_id,
            "snaptrade:new",
            hist=hist,
            cur=5,
            skip_history=skip_history,
            transactions_ready=False,
        ),
    )

    assert cli.main() == 0
    assert len(_wire["batch"]) == 1
    assert _wire["first_sync_marked"] == []


# ---------------------------------------------------------------------------
# "connection_broken_pending" — the debounced disabled-flag signal (see
# app.snaptrade._sync_one_connection). Must NOT be treated as a hard error
# or trigger a reconnect email; a run where every account is merely pending
# must not fail the cron.
# ---------------------------------------------------------------------------

def test_pending_disabled_flag_is_not_notified_and_not_a_hard_error(_wire, monkeypatch):
    rows = [_row(9, "a1", "Schwab Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(
        _snap, "_sync_one_connection",
        lambda *a, **k: {"ok": False, "error": "connection_broken_pending"},
    )
    notified = []
    monkeypatch.setattr(cli, "_notify_connection_dropped",
                        lambda *a, **k: notified.append(a))

    rc = cli.main()
    # succeeded == 0 but pending > 0 -> not treated as a system-wide outage.
    assert rc == 0
    assert notified == []
    assert _wire["batch"] == []


def test_all_accounts_pending_does_not_fail_cron(_wire, monkeypatch):
    rows = [_row(9, "a1", "Schwab Account"), _row(9, "a2", "Schwab Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(
        _snap, "_sync_one_connection",
        lambda *a, **k: {"ok": False, "error": "connection_broken_pending"},
    )
    assert cli.main() == 0


@pytest.mark.parametrize("hard_error", ["session_expired", "connection_broken"])
def test_pending_account_does_not_mask_hard_failures(
    _wire, monkeypatch, hard_error,
):
    """One flapping connection must not make unrelated sync failures green."""
    rows = [_row(9, "pending", "Schwab Account"), _row(18, "failed", "Fidelity Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)

    def _sync(_user_id, row, **_kwargs):
        if row["snaptrade_account_id"] == "pending":
            return {"ok": False, "error": "connection_broken_pending"}
        return {"ok": False, "error": hard_error}

    monkeypatch.setattr(_snap, "_sync_one_connection", _sync)

    assert cli.main() == 1
    assert _wire["batch"] == []


def test_failed_batch_leaves_first_sync_pending(_wire, monkeypatch):
    rows = [_row(14, "new-account", "Fidelity Account", first_done=False)]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)
    monkeypatch.setattr(
        _snap,
        "_sync_one_connection",
        lambda user_id, row, **kwargs: _ok(
            row["account_name"], user_id, "snaptrade:new",
        ),
    )
    monkeypatch.setattr(
        _upload,
        "merge_and_push_seeds_batch",
        lambda *args, **kwargs: (False, "GitHub unavailable", None, False, 0),
    )

    assert cli.main() == 0
    assert _wire["first_sync_marked"] == []


# ---------------------------------------------------------------------------
# _notify_connection_dropped — connection-level email dedupe. One SnapTrade
# authorization commonly backs several tenant rows (e.g. one Schwab login ->
# 6 accounts); without the ``seen`` dedupe a single real break fired one
# near-identical "reconnect" email PER SIBLING ACCOUNT in the same run (real
# case: user_id=9 testingcameron, 6 emails per break episode, 2026-09-04).
# ---------------------------------------------------------------------------

class _FakeUser:
    id = 9
    username = "testingcameron"
    email = "user@example.com"


def _wire_notify(monkeypatch, *, accounts, broken_at):
    """Wire the Postgres/email deps ``_notify_connection_dropped`` touches.

    ``accounts`` maps snaptrade_account_id -> brokerage_authorization_id.
    ``broken_at`` is returned for every account (same break instant, as a
    single poll marking several sibling rows would produce).
    """
    from app import email as _email
    from app import models as _m

    sent = []
    monkeypatch.setattr(_m.User, "get_by_id", staticmethod(lambda uid: _FakeUser()))
    monkeypatch.setattr(
        _m, "get_snaptrade_account",
        lambda uid, aid: {
            "brokerage_authorization_id": accounts.get(aid),
            "connection_broken_at": broken_at,
            "broker_slug": "schwab",
            "display_nickname": aid,
            "account_name": aid,
        },
    )
    monkeypatch.setattr(_m, "record_email_send", lambda *a, **k: True)
    monkeypatch.setattr(_email, "app_base_url", lambda: "https://app.example.com")
    monkeypatch.setattr(
        _email, "send_connection_dropped_email",
        lambda **kwargs: sent.append(kwargs),
    )
    return sent


def test_notify_connection_dropped_collapses_siblings_under_one_authorization(monkeypatch):
    import datetime

    broken_at = datetime.datetime(2026, 9, 4, 14, 31, tzinfo=datetime.timezone.utc)
    accounts = {"a1": "auth-X", "a2": "auth-X", "a3": "auth-X"}
    sent = _wire_notify(monkeypatch, accounts=accounts, broken_at=broken_at)

    seen = set()
    for aid in ("a1", "a2", "a3"):
        cli._notify_connection_dropped(
            9, aid, {"brokerage_authorization_id": accounts[aid], "broker_slug": "schwab"},
            seen=seen,
        )
    # Same authorization, same break instant -> ONE email, not three.
    assert len(sent) == 1


def test_notify_connection_dropped_notifies_each_distinct_authorization(monkeypatch):
    import datetime

    broken_at = datetime.datetime(2026, 9, 4, 14, 31, tzinfo=datetime.timezone.utc)
    accounts = {"a1": "auth-X", "b1": "auth-Y"}
    sent = _wire_notify(monkeypatch, accounts=accounts, broken_at=broken_at)

    seen = set()
    for aid in ("a1", "b1"):
        cli._notify_connection_dropped(
            9, aid, {"brokerage_authorization_id": accounts[aid], "broker_slug": "schwab"},
            seen=seen,
        )
    # Different authorizations -> both notify.
    assert len(sent) == 2


def test_notify_connection_dropped_without_seen_kwarg_still_sends(monkeypatch):
    """Callers that don't pass ``seen`` (e.g. any future caller) keep the
    original per-account behavior — the dedupe is opt-in via the kwarg."""
    import datetime

    broken_at = datetime.datetime(2026, 9, 4, 14, 31, tzinfo=datetime.timezone.utc)
    sent = _wire_notify(monkeypatch, accounts={"a1": "auth-X"}, broken_at=broken_at)

    cli._notify_connection_dropped(9, "a1", {"brokerage_authorization_id": "auth-X"})
    assert len(sent) == 1
