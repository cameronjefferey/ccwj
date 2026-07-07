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
