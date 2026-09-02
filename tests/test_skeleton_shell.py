"""Instant-shell skeleton behavior (app/skeleton.py).

The four BigQuery-heavy pages serve a shimmer shell to genuine browser
navigations and the real render to everything else. These tests pin the
gating contract on a dummy decorated route so they run without BigQuery:

  - Sec-Fetch-Mode: navigate           -> shell
  - X-HT-Full: 1                       -> full page (shell's own fetch)
  - ?_full=1                           -> full page (JS-error fallback)
  - warm per-endpoint cookie           -> full page (no shell round-trip)
  - no Sec-Fetch-Mode header at all    -> full page (test clients, curl,
                                          monitors, old browsers)

A fast (<4s) 200 full render must set the warm cookie so the next
navigation skips the shell while the query cache is hot. A Redis
sentinel (warmer or prior fast render) also skips the shell.
"""

from app import app
from app.skeleton import skeleton_page


@app.route("/_test/skeleton-page")
@skeleton_page
def _skeleton_test_page():
    return "FULL_PAGE_BODY"


def _client():
    return app.test_client()


def test_browser_navigation_gets_shell():
    r = _client().get(
        "/_test/skeleton-page", headers={"Sec-Fetch-Mode": "navigate"}
    )
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "FULL_PAGE_BODY" not in body
    assert "sk-shimmer" in body
    # The shell must re-request with the full-render header.
    assert "X-HT-Full" in body


def test_full_header_gets_real_page_and_warm_cookie():
    r = _client().get(
        "/_test/skeleton-page",
        headers={"Sec-Fetch-Mode": "navigate", "X-HT-Full": "1"},
    )
    assert r.get_data(as_text=True) == "FULL_PAGE_BODY"
    cookie = r.headers.get("Set-Cookie", "")
    assert "ht_fast__skeleton_test_page" in cookie


def test_full_query_param_fallback_gets_real_page():
    r = _client().get(
        "/_test/skeleton-page?_full=1", headers={"Sec-Fetch-Mode": "navigate"}
    )
    assert r.get_data(as_text=True) == "FULL_PAGE_BODY"


def test_warm_cookie_skips_shell():
    c = _client()
    c.set_cookie("ht_fast__skeleton_test_page", "1")
    r = c.get("/_test/skeleton-page", headers={"Sec-Fetch-Mode": "navigate"})
    assert r.get_data(as_text=True) == "FULL_PAGE_BODY"


def test_warm_sentinel_skips_shell(monkeypatch):
    monkeypatch.setattr("app.skeleton._redis_says_warm", lambda: True)
    r = _client().get(
        "/_test/skeleton-page", headers={"Sec-Fetch-Mode": "navigate"}
    )
    assert r.get_data(as_text=True) == "FULL_PAGE_BODY"


def test_fast_render_threshold_covers_warm_pages():
    from app.skeleton import _FAST_RENDER_SECONDS, _WARM_COOKIE_MAX_AGE
    assert _FAST_RENDER_SECONDS >= 4.0
    assert _WARM_COOKIE_MAX_AGE >= 86400


def test_non_navigation_clients_get_full_page():
    # No Sec-Fetch-Mode header: test clients, curl, uptime monitors.
    r = _client().get("/_test/skeleton-page")
    assert r.get_data(as_text=True) == "FULL_PAGE_BODY"
