"""Ops Telegram alerts — skip when unset; send when configured."""

import io
import json
from urllib.error import URLError

from scripts import ops_telegram as ot


def test_compose_failure_includes_run_url():
    text = ot.compose_message(
        name="Update Daily Position Performance",
        conclusion="failure",
        event="workflow_dispatch",
        branch="master",
        url="https://github.com/cameronjefferey/ccwj/actions/runs/1",
    )
    assert text.startswith("HappyTrader — warehouse rebuild failed.")
    assert "https://github.com/cameronjefferey/ccwj/actions/runs/1" in text
    assert "Failed run:" in text
    assert "workflow_dispatch" not in text
    assert "branch:" not in text
    assert "looking at it" not in text


def test_compose_failure_mentions_agent_without_dumping_its_url():
    text = ot.compose_message(
        name="Update Daily Position Performance",
        conclusion="failure",
        event="workflow_dispatch",
        branch="master",
        url="https://github.com/cameronjefferey/ccwj/actions/runs/1",
        agent_url="https://cursor.com/agents/bc-abc",
    )
    assert "A Cursor agent is looking at it." in text
    assert "cursor.com/agents/bc-abc" not in text


def test_compose_failure_retries_exhausted():
    text = ot.compose_message(
        name="Update Daily Position Performance",
        conclusion="failure",
        event="workflow_dispatch",
        branch="master",
        url="https://github.com/cameronjefferey/ccwj/actions/runs/1",
        hotfix_skip="retries_exhausted",
    )
    assert "already tried twice" in text
    assert "needs a person" in text


def test_compose_failure_hotfix_in_flight():
    text = ot.compose_message(
        name="Update Daily Position Performance",
        conclusion="failure",
        event="workflow_dispatch",
        branch="master",
        url="https://github.com/cameronjefferey/ccwj/actions/runs/1",
        agent_url="https://cursor.com/agents/bc-live",
        hotfix_skip="hotfix_in_flight",
    )
    assert "already working on it" in text
    assert "looking at it" not in text
    assert "cursor.com/agents/bc-live" not in text


def test_compose_hotfix_started():
    text = ot.compose_hotfix_started(
        name="Update Daily Position Performance",
        agent_url="https://cursor.com/agents/bc-abc",
        url="https://github.com/cameronjefferey/ccwj/actions/runs/1",
    )
    assert text.startswith("HappyTrader — a Cursor agent is working on the warehouse rebuild.")
    assert "Watch it:" in text
    assert "https://cursor.com/agents/bc-abc" in text
    assert "actions/runs/1" not in text
    assert "FAILED" not in text


def test_compose_hotfix_merged():
    text = ot.compose_hotfix_merged(
        name="[cursor-hotfix] Collapse CSV vs SnapTrade date-padding history dupes",
        pr_url="https://github.com/cameronjefferey/ccwj/pull/65",
        branch="cursor/hotfix-dupes",
    )
    assert text.startswith("HappyTrader — the hotfix is live.")
    assert "Collapse CSV vs SnapTrade date-padding history dupes" in text
    assert "[cursor-hotfix]" not in text
    assert "See the change:" in text
    assert "pull/65" in text
    assert "cursor/hotfix-dupes" not in text
    assert "FAILED" not in text


def test_is_cursor_hotfix_branch():
    assert ot.is_cursor_hotfix_branch("cursor/hotfix-update-daily-position-performance-dde6")
    assert ot.is_cursor_hotfix_branch("refs/heads/cursor/foo")
    assert not ot.is_cursor_hotfix_branch("master")
    assert not ot.is_cursor_hotfix_branch("fix/cursor-typo")


def test_compose_message_kind_started_and_merged():
    started = ot.compose_message(
        name="Warehouse",
        conclusion="failure",
        event="workflow_dispatch",
        branch="master",
        url="https://example/run",
        agent_url="https://cursor.com/agents/bc-x",
        kind="started",
    )
    assert started.startswith("HappyTrader — a Cursor agent is working on the warehouse.")
    merged = ot.compose_message(
        name="the pr",
        conclusion="failure",
        event="pull_request",
        branch="cursor/x",
        url="https://example/pull/1",
        kind="merged",
    )
    assert merged.startswith("HappyTrader — the hotfix is live.")


def test_compose_test_ping_is_short():
    text = ot.compose_message(
        name="Ops alert",
        conclusion="test",
        event="workflow_dispatch",
        branch="master",
        url="https://example",
    )
    assert "test ping" in text.lower()
    assert "FAILED" not in text


def test_main_skips_without_secrets(monkeypatch, capsys):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    assert ot.main() == 0
    assert "skipped" in capsys.readouterr().out


def test_main_sends_when_configured(monkeypatch, capsys):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("ALERT_NAME", "Warehouse reconcile audit")
    monkeypatch.setenv("ALERT_CONCLUSION", "failure")
    monkeypatch.setenv("ALERT_EVENT", "schedule")
    monkeypatch.setenv("ALERT_BRANCH", "master")
    monkeypatch.setenv("ALERT_URL", "https://example/run")

    captured = {}

    class _Resp:
        def read(self):
            return json.dumps({"ok": True}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=20):
        captured["url"] = req.full_url
        captured["body"] = req.data.decode()
        return _Resp()

    monkeypatch.setattr(ot.urllib.request, "urlopen", fake_urlopen)
    assert ot.main() == 0
    assert "sent" in capsys.readouterr().out
    assert captured["url"].endswith("/bottok/sendMessage")
    assert "chat_id=123" in captured["body"]
    from urllib.parse import unquote_plus
    body = unquote_plus(captured["body"])
    assert "warehouse audit failed" in body


def test_main_sends_hotfix_started(monkeypatch, capsys):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("ALERT_KIND", "started")
    monkeypatch.setenv("ALERT_NAME", "Update Daily Position Performance")
    monkeypatch.setenv("ALERT_AGENT_URL", "https://cursor.com/agents/bc-abc")
    monkeypatch.setenv("ALERT_URL", "https://example/run")

    captured = {}

    class _Resp:
        def read(self):
            return json.dumps({"ok": True}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=20):
        captured["body"] = req.data.decode()
        return _Resp()

    monkeypatch.setattr(ot.urllib.request, "urlopen", fake_urlopen)
    assert ot.main() == 0
    from urllib.parse import unquote_plus
    body = unquote_plus(captured["body"])
    assert "a Cursor agent is working on the warehouse rebuild" in body
    assert "sent" in capsys.readouterr().out


def test_main_nonzero_on_network_error(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")

    def boom(*a, **k):
        raise URLError("down")

    monkeypatch.setattr(ot.urllib.request, "urlopen", boom)
    monkeypatch.setattr(ot.sys, "stderr", io.StringIO())
    assert ot.main() == 1
