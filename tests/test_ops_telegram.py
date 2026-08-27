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
    assert "FAILED" in text
    assert "Update Daily Position Performance" in text
    assert "https://github.com/cameronjefferey/ccwj/actions/runs/1" in text
    assert "workflow_dispatch" in text


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
    assert "Warehouse+reconcile" in captured["body"] or "Warehouse reconcile" in captured["body"]


def test_main_nonzero_on_network_error(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")

    def boom(*a, **k):
        raise URLError("down")

    monkeypatch.setattr(ot.urllib.request, "urlopen", boom)
    monkeypatch.setattr(ot.sys, "stderr", io.StringIO())
    assert ot.main() == 1
