"""Cursor cloud-agent hotfix launcher — skip when unset; POST when configured."""

import io
import json
from urllib.error import HTTPError, URLError

from scripts import ops_cursor_hotfix as och


def test_agent_id_is_stable_for_the_same_run():
    a = och.agent_id_for_run("33132317666")
    b = och.agent_id_for_run("33132317666")
    assert a == b
    assert a.startswith("bc-")
    assert a != och.agent_id_for_run("1")
    assert och.agent_id_for_failure(sha="abc", run_id="1") == och.agent_id_for_failure(
        sha="abc", run_id="999"
    )


def test_compose_prompt_includes_run_and_guardrails():
    text = och.compose_prompt(
        name="Update Daily Position Performance",
        event="workflow_dispatch",
        branch="master",
        url="https://github.com/cameronjefferey/ccwj/actions/runs/33132317666",
        sha="c1315dc",
        repo="cameronjefferey/ccwj",
    )
    assert "33132317666" in text
    assert "workflow_dispatch" in text
    assert "snapshot_guard" in text
    assert "_dedup_history_rows" in text
    assert "[cursor-hotfix]" in text
    assert "attempt 1 of 2" in text
    assert "Do NOT weaken" in text


def test_compose_launch_body_opens_a_pr_off_master():
    body = och.compose_launch_body(
        name="Update Daily Position Performance",
        event="workflow_dispatch",
        branch="master",
        url="https://github.com/cameronjefferey/ccwj/actions/runs/1",
        sha="abc",
        repo="cameronjefferey/ccwj",
        repo_url="https://github.com/cameronjefferey/ccwj",
        run_id="1",
        attempt=2,
    )
    assert body["autoCreatePR"] is True
    assert body["workOnCurrentBranch"] is False
    assert body["skipReviewerRequest"] is True
    assert body["repos"][0]["startingRef"] == "master"
    assert body["agentId"] == och.agent_id_for_failure(sha="abc", run_id="1")
    assert "Hotfix:" in body["name"]
    assert "attempt 2 of 2" in body["prompt"]["text"]


def test_count_consecutive_hotfix_commits(monkeypatch):
    def fake_check_output(cmd, **kwargs):
        return (
            "fix dupes [cursor-hotfix] 3313\n"
            "fix dupes again [cursor-hotfix] 3314\n"
            "Merge pull request #64\n"
        )

    monkeypatch.setattr(och.subprocess, "check_output", fake_check_output)
    assert och.count_consecutive_hotfix_commits("abc") == 2


def test_count_consecutive_stops_at_human_commit(monkeypatch):
    def fake_check_output(cmd, **kwargs):
        return "human fix\nfix [cursor-hotfix]\n"

    monkeypatch.setattr(och.subprocess, "check_output", fake_check_output)
    assert och.count_consecutive_hotfix_commits("HEAD") == 0


def test_should_skip_cursor_branch_and_repeat_sha():
    assert och.should_skip_loop(branch="cursor/fix-dbt", sha="aaa", skip_sha="")
    assert och.should_skip_loop(branch="master", sha="abc", skip_sha="abc")
    assert och.should_skip_loop(branch="master", sha="abc", skip_sha="zzz") is None
    assert (
        och.should_skip_loop(
            branch="master", sha="abc", skip_sha="", consecutive_hotfixes=2
        )
        == och.SKIP_RETRIES_EXHAUSTED
    )
    assert (
        och.should_skip_loop(
            branch="master", sha="abc", skip_sha="", consecutive_hotfixes=1
        )
        is None
    )


def test_main_skips_without_key(monkeypatch, capsys):
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    assert och.main() == 0
    assert "skipped" in capsys.readouterr().out


def test_main_skips_cursor_branch(monkeypatch, capsys):
    monkeypatch.setenv("CURSOR_API_KEY", "key")
    monkeypatch.setenv("ALERT_BRANCH", "cursor/hotfix-dupes")
    monkeypatch.setattr(och, "count_consecutive_hotfix_commits", lambda sha="": 0)
    assert och.main() == 0
    assert "cursor agent branch" in capsys.readouterr().out


def test_main_skips_after_two_hotfix_commits(monkeypatch, capsys, tmp_path):
    out = tmp_path / "github_output"
    monkeypatch.setenv("CURSOR_API_KEY", "key")
    monkeypatch.setenv("ALERT_BRANCH", "master")
    monkeypatch.setenv("ALERT_SHA", "deadbeef")
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setattr(och, "count_consecutive_hotfix_commits", lambda sha="": 2)
    launched = {"n": 0}

    def fake_launch(body, *, api_key, timeout=30):
        launched["n"] += 1
        return {"status": 201, "data": {}}

    monkeypatch.setattr(och, "launch_agent", fake_launch)
    assert och.main() == 0
    assert launched["n"] == 0
    assert "retries_exhausted" in capsys.readouterr().out
    assert "skip_reason=retries_exhausted" in out.read_text()


def test_main_launches_and_writes_output(monkeypatch, capsys, tmp_path):
    out = tmp_path / "github_output"
    monkeypatch.setenv("CURSOR_API_KEY", "key")
    monkeypatch.setenv("ALERT_NAME", "Update Daily Position Performance")
    monkeypatch.setenv("ALERT_CONCLUSION", "failure")
    monkeypatch.setenv("ALERT_EVENT", "workflow_dispatch")
    monkeypatch.setenv("ALERT_BRANCH", "master")
    monkeypatch.setenv("ALERT_URL", "https://github.com/cameronjefferey/ccwj/actions/runs/1")
    monkeypatch.setenv("ALERT_SHA", "c1315dc")
    monkeypatch.setenv("ALERT_RUN_ID", "1")
    monkeypatch.setenv("GITHUB_REPOSITORY", "cameronjefferey/ccwj")
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setattr(och, "count_consecutive_hotfix_commits", lambda sha="": 0)

    captured = {}

    def fake_launch(body, *, api_key, timeout=30):
        captured["body"] = body
        captured["api_key"] = api_key
        return {
            "status": 201,
            "data": {
                "agent": {
                    "id": body["agentId"],
                    "url": f"https://cursor.com/agents/{body['agentId']}",
                }
            },
        }

    monkeypatch.setattr(och, "launch_agent", fake_launch)
    assert och.main() == 0
    printed = capsys.readouterr().out
    assert "launched" in printed
    assert captured["api_key"] == "key"
    assert captured["body"]["autoCreatePR"] is True
    assert "actions/runs/1" in captured["body"]["prompt"]["text"]
    written = out.read_text()
    assert written.startswith("agent_url=https://cursor.com/agents/")


def test_main_treats_409_as_already_launched_without_started_ping(
    monkeypatch, capsys, tmp_path
):
    out = tmp_path / "github_output"
    monkeypatch.setenv("CURSOR_API_KEY", "key")
    monkeypatch.setenv("ALERT_RUN_ID", "99")
    monkeypatch.setenv("ALERT_BRANCH", "master")
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setattr(och, "count_consecutive_hotfix_commits", lambda sha="": 0)

    def fake_launch(body, *, api_key, timeout=30):
        return {"status": 409, "data": {"error": "agent_id_conflict"}}

    monkeypatch.setattr(och, "launch_agent", fake_launch)
    assert och.main() == 0
    assert "already launched" in capsys.readouterr().out
    assert not out.exists()


def test_main_nonzero_on_api_error(monkeypatch):
    monkeypatch.setenv("CURSOR_API_KEY", "key")
    monkeypatch.setenv("ALERT_BRANCH", "master")
    monkeypatch.setattr(och, "count_consecutive_hotfix_commits", lambda sha="": 0)

    def fake_launch(body, *, api_key, timeout=30):
        return {"status": 401, "data": {"message": "unauthorized"}}

    monkeypatch.setattr(och, "launch_agent", fake_launch)
    monkeypatch.setattr(och.sys, "stderr", io.StringIO())
    assert och.main() == 1


def test_main_nonzero_on_network_error(monkeypatch):
    monkeypatch.setenv("CURSOR_API_KEY", "key")
    monkeypatch.setenv("ALERT_BRANCH", "master")
    monkeypatch.setattr(och, "count_consecutive_hotfix_commits", lambda sha="": 0)

    def boom(*a, **k):
        raise URLError("down")

    monkeypatch.setattr(och, "launch_agent", boom)
    monkeypatch.setattr(och.sys, "stderr", io.StringIO())
    assert och.main() == 1


def test_launch_agent_posts_basic_auth(monkeypatch):
    captured = {}

    class _Resp:
        status = 201

        def read(self):
            return json.dumps({"agent": {"id": "bc-x", "url": "https://cursor.com/agents/bc-x"}}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=30):
        captured["url"] = req.full_url
        captured["auth"] = req.headers.get("Authorization") or req.get_header("Authorization")
        captured["body"] = json.loads(req.data.decode())
        return _Resp()

    monkeypatch.setattr(och.urllib.request, "urlopen", fake_urlopen)
    result = och.launch_agent({"prompt": {"text": "hi"}}, api_key="secret-key")
    assert result["status"] == 201
    assert captured["url"] == och.CURSOR_AGENTS_API
    assert captured["auth"].startswith("Basic ")
    assert captured["body"]["prompt"]["text"] == "hi"


def test_launch_agent_maps_http_error(monkeypatch):
    class _FP:
        def read(self):
            return json.dumps({"error": "agent_id_conflict"}).encode()

        def close(self):
            return None

    def boom(req, timeout=30):
        raise HTTPError(req.full_url, 409, "Conflict", None, _FP())

    monkeypatch.setattr(och.urllib.request, "urlopen", boom)
    result = och.launch_agent({"prompt": {"text": "hi"}}, api_key="k")
    assert result["status"] == 409
    assert result["data"]["error"] == "agent_id_conflict"
