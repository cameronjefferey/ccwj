"""Anti-cloning surfaces (Sep 2026): AI-crawler robots.txt disallows,
ToS acceptable-use language, and the footer copyright notice.

None of these stop a human from screenshotting a public marketing page
and pasting it into an AI chat -- nothing server-side can. They stop
compliant AI crawlers' own automated fetches (robots.txt), put an
explicit contractual prohibition on record (ToS), and put an explicit
copyright claim on every page (footer). See AGENTS.md discussion in the
Sep 2026 production-readiness pass for the full reasoning.
"""

from app import app
from app.marketing import _AI_CRAWLER_USER_AGENTS


def _client():
    return app.test_client()


def test_robots_txt_disallows_known_ai_crawlers():
    resp = _client().get("/robots.txt")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    # Every catalog entry gets its own explicit block.
    for ua in _AI_CRAWLER_USER_AGENTS:
        assert f"User-agent: {ua}\nDisallow: /" in body
    # The generic wildcard block (real search engines, sitemap) is untouched.
    assert "User-agent: *" in body
    assert "Sitemap:" in body
    assert "Disallow: /positions" in body


def test_ai_crawler_catalog_is_nonempty_and_covers_the_big_three():
    # Guard against an empty/typo'd tuple silently no-oping the whole block.
    assert len(_AI_CRAWLER_USER_AGENTS) >= 5
    assert "GPTBot" in _AI_CRAWLER_USER_AGENTS
    assert "ClaudeBot" in _AI_CRAWLER_USER_AGENTS
    assert "CCBot" in _AI_CRAWLER_USER_AGENTS


def test_terms_page_prohibits_ai_training_use():
    resp = _client().get("/terms")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "AI system" in body or "AI tool" in body
    assert "reproduce or clone" in body


def test_footer_copyright_notice_present_and_dated():
    from app import _current_year

    resp = _client().get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert f"&copy; {_current_year()} HappyTrader" in body
    assert "All rights reserved" in body
