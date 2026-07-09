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


def _ok(name, uid, tenant, *, hist=3, cur=5, skip_history=False):
    return {
        "ok": True, "error": None,
        "history_rows": hist, "current_rows": cur,
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

    calls = {"batch": [], "synced": []}

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

    def _fake_sync(user_id, row, *, lookback_days, defer_push=False):
        seen_defer.append(defer_push)
        return results[row["snaptrade_account_id"]]

    monkeypatch.setattr(_snap, "_sync_one_connection", _fake_sync)

    rc = cli.main()
    assert rc == 0
    # Every account synced in deferred mode.
    assert seen_defer == [True, True, True]
    # Exactly ONE batched push, carrying all three accounts.
    assert len(_wire["batch"]) == 1
    assert len(_wire["batch"][0]["entries"]) == 3


def test_cron_skips_broken_connection_from_batch(_wire, monkeypatch):
    rows = [_row(9, "a1", "Schwab Account"), _row(9, "a2", "Schwab Account")]
    monkeypatch.setattr(_models, "list_all_snaptrade_accounts", lambda: rows)

    def _fake_sync(user_id, row, *, lookback_days, defer_push=False):
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

    def _fake_sync(user_id, row, *, lookback_days, defer_push=False):
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
