"""Upload/sync processing page: warehouse poll must not leave the user stuck."""

import re
from pathlib import Path

from app.upload import _pick_dispatch_run


def test_pick_dispatch_run_prefers_inflight_over_cancelled_predecessor():
    cancelled = {
        "status": "completed",
        "conclusion": "cancelled",
        "html_url": "https://example/old",
    }
    live = {
        "status": "in_progress",
        "conclusion": None,
        "html_url": "https://example/live",
    }
    # Newest-first, as GitHub returns.
    assert _pick_dispatch_run([live, cancelled]) is live
    assert _pick_dispatch_run([cancelled, live]) is live


def test_pick_dispatch_run_prefers_success_over_earlier_failure():
    failed = {"status": "completed", "conclusion": "failure"}
    ok = {"status": "completed", "conclusion": "success"}
    assert _pick_dispatch_run([ok, failed]) is ok
    assert _pick_dispatch_run([failed]) is failed
    assert _pick_dispatch_run([]) is None


def test_seed_tenant_id_regex_allows_manual_account_names_with_spaces():
    sql = Path("dbt/tests/every_seed_row_has_tenant_id.sql").read_text()
    match = re.search(r"regexp_contains\(tenant_id, r'([^']+)'\)", sql)
    assert match, "every_seed_row_has_tenant_id.sql must pin a tenant_id regex"
    pat = re.compile(match.group(1))
    assert pat.search("manual:manual:Emmory Investment")
    assert pat.search("snaptrade:8c597f1a-f8de-4a14-a3cc-cce4e3697470")
    assert pat.search("demo:demo-account")
    assert not pat.search("not-a-tenant")
    assert not pat.search("manual:")
    assert not pat.search("")


def test_poll_failure_stops_spinner_and_redirects():
    js = Path("app/templates/includes/_github_actions_poll.html").read_text()
    assert 'd.state === "failure"' in js
    assert "stopSpinner()" in js
    assert "This update didn’t finish" in js
    assert "leaveSoon(4000)" in js
    html = Path("app/templates/upload_processing.html").read_text()
    assert 'id="pipelineHeading"' in html
    assert 'id="pipelineIcon"' in html
