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


def test_connect_processing_copy_and_overview_ready_poll():
    html = Path("app/templates/sync_processing.html").read_text()
    assert "We connected your brokerage" in html
    assert "We're pulling your trade history now" in html
    assert "api_sync_overview_ready" in html
    assert "you don't need to hit Sync again" in html
    # The live "Building Overview — usually 15-40 minutes" ticker below
    # (see the connecting-scoped poll script) owns the timing estimate —
    # the static confirmation copy above must not repeat it (Sep 2026
    # duplicative-language fix).
    assert "Then we build Overview" not in html


def test_onboarding_questions_are_optional():
    """Every survey question can be skipped. Next must not block on a
    blank section, and a short free-text answer must not be rejected."""
    html = Path("app/templates/sync_processing.html").read_text()
    assert 'class="ob-question" data-required' not in html
    assert "data-min-len=" not in html
    src = Path("app/marketing.py").read_text()
    assert "_ONBOARDING_REQUIRED_KEYS" not in src
    assert "still missing" not in src


def test_onboarding_validation_scrolls_and_names_the_offending_question():
    """A field error used to render in the shared banner after every
    question in the section, including the email toggle, so it looked
    like the wrong control had failed. The message now lives inside the
    question that failed."""
    html = Path("app/templates/sync_processing.html").read_text()
    assert "function showFieldError(q, msg)" in html
    assert 'note.className = "ob-q-error"' in html
    assert "q.appendChild(note)" in html
    assert "function focusMissing(q)" in html
    assert "Needs at least " in html
    assert "highlighted above" not in html
    assert "That last one needs at least" not in html


def test_dev_onboarding_preview_is_debug_only():
    src = Path("app/marketing.py").read_text()
    start = src.index("def dev_onboarding_preview")
    body = src[start:src.index("\n@app.route", start)]
    assert "if not app.debug:" in body
    assert "abort(404)" in body
    assert "onboarding_preview=True" in body


def test_onboarding_holds_redirect_until_save_or_skip():
    """The survey must stay put until Save or Skip — not until first
    focus, and not because Overview already has rows from older accounts."""
    html = Path("app/templates/sync_processing.html").read_text()
    assert "window.__onboardingHoldRedirect = true;" in html
    assert "window.__onboardingHoldRedirect = false;" in html
    assert 'form.addEventListener("focusin", hold' not in html
    poll = Path("app/templates/includes/_github_actions_poll.html").read_text()
    assert "if (!window.__onboardingHoldRedirect)" in poll


def test_poll_gives_up_with_email_copy_instead_of_silent_redirect():
    js = Path("app/templates/includes/_github_actions_poll.html").read_text()
    assert "n < 300" in js
    assert "we'll email you when the numbers are ready" in js
    else_block = js.split("{% else %}")[-1]
    assert "setTimeout" not in else_block
    assert "leaveSoon" not in else_block
