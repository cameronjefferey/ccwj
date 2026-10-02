"""Trade replays on /learn. Prices are illustrative and labeled as such."""

import re

from app import learn_progress
from app import learn_replay


def _client():
    from app import app
    return app.test_client()


def _html(path):
    response = _client().get(path)
    assert response.status_code == 200, path
    return response.get_data(as_text=True)


def _article(path):
    html = _html(path)
    match = re.search(r"<article\b.*</article>", html, re.S)
    assert match, path
    return match.group(0)


def test_starter_replays_match_the_lesson_order():
    rows = learn_replay.replays()
    assert [replay["slug"] for replay in rows] == [
        "long-call",
        "long-put",
        "covered-call",
    ]
    assert [replay["strategy"] for replay in rows] == [
        "Long call",
        "Long put",
        "Covered call",
    ]
    assert [replay["lesson_slug"] for replay in rows] == [
        "calls-and-puts",
        "calls-and-puts",
        "covered-calls",
    ]
    assert {replay["price_source"]["kind"] for replay in rows} == {"illustrative"}
    for replay in rows:
        assert "Illustrative" in replay["price_source"]["label"]
        assert replay["ticker"] in replay["price_source"]["note"]
        assert "not a market quote" in replay["price_source"]["note"]


def test_choice_results_come_from_the_path():
    call = learn_replay.replay_by_slug("long-call")["decisions"][0]
    call_choices = {choice["id"]: choice for choice in call["choices"]}
    assert "down 6%" in call["prompt"]
    assert "down 40%" in call["prompt"]
    assert "10 days" in call["prompt"]
    assert call_choices["close"]["result_text"] == "-$160.00"
    assert call_choices["hold"]["result_text"] == "+$410.00"
    assert call_choices["expire"]["result_text"] == "+$300.00"
    assert call_choices["close"]["detail"] == []

    put = learn_replay.replay_by_slug("long-put")["decisions"][0]
    put_choices = {choice["id"]: choice for choice in put["choices"]}
    assert "down 6%" in put["prompt"]
    assert "up 60%" in put["prompt"]
    assert "8 days" in put["prompt"]
    assert put_choices["close"]["result_text"] == "+$150.00"
    assert put_choices["hold"]["result_text"] == "+$370.00"
    assert put_choices["expire"]["result_text"] == "-$170.00"

    covered = learn_replay.replay_by_slug("covered-call")["decisions"][0]
    covered_choices = {choice["id"]: choice for choice in covered["choices"]}
    assert "up 6%" in covered["prompt"]
    assert "premium" in covered["prompt"]
    assert covered_choices["close"]["result_text"] == "+$260.00"
    assert covered_choices["assigned"]["result_text"] == "+$340.00"
    assert covered_choices["roll"]["result_text"] == "+$760.00"
    assert "not a quote" in covered_choices["roll"]["assumption"]
    labels = [line["label"] for line in covered_choices["close"]["detail"]]
    assert labels[0] == "Premium collected"
    assert "Paid" not in labels[0]


def test_replay_pages_render_the_decision_and_the_checkpoint():
    html = _html("/learn/replay/long-call")
    assert "<title>A long call · Replay - HappyTrader</title>" in html
    assert html.count('class="replay-card"') == 1
    assert "Close early, hold, or let it expire?" in html
    assert html.count('class="replay-choice"') == 3
    assert 'class="replay-choices"' in html
    assert "-$160.00" in html
    assert "+$410.00" in html
    assert "+$300.00" in html
    assert "Did that make sense?" in html
    assert ">Go deeper</a>" in html
    assert 'href="/learn/calls-and-puts"' in html
    assert "For learning only · not investment advice." in html
    assert "Illustrative prices" in html
    assert "not a market quote" in html
    assert "Not a listed ticker" in html
    assert "/static/js/learn-replay.js" in html
    article = _article("/learn/replay/long-call")
    assert "#28c08a" not in article
    assert "#f0556d" not in article
    assert "premium" not in article.lower()
    assert "premium" not in _article("/learn/replay/long-put").lower()
    assert "Paid" in _article("/learn/replay/long-call")

    covered = _article("/learn/replay/covered-call")
    assert "Premium collected" in covered
    assert "+$140.00" not in covered
    assert "$140.00" in covered
    assert "+$260.00" in covered
    assert "+$340.00" in covered
    assert "+$760.00" in covered
    assert "not a quote" in covered
    assert 'href="/learn/covered-calls"' in covered


def test_replay_routes_and_links():
    missing = _client().get("/learn/replay/not-a-replay")
    assert missing.status_code == 404
    index = _client().get("/learn/replay", follow_redirects=False)
    assert index.status_code == 302
    assert index.headers["Location"].endswith("/learn#replays")

    page = _html("/learn")
    assert 'id="replays"' in page
    assert 'href="#replays"' in page
    assert 'href="/learn/replay/long-call"' in page
    assert 'href="/learn/replay/long-put"' in page
    assert 'href="/learn/replay/covered-call"' in page
    assert page.count('class="learn-replay"') == 3
    assert page.count('class="learn-replay-check"') == 3

    calls = _html("/learn/calls-and-puts")
    assert 'href="/learn/replay/long-call"' in calls
    assert 'href="/learn/replay/long-put"' in calls
    assert 'href="/learn/replay/covered-call"' in _html("/learn/covered-calls")
    assert "/learn/replay/" not in _html("/learn/what-is-an-option")

    sitemap = _client().get("/sitemap.xml").get_data(as_text=True)
    for slug in ("long-call", "long-put", "covered-call"):
        assert f"/learn/replay/{slug}" in sitemap


def test_step_query_only_accepts_decision_and_recap():
    assert 'data-start="decision"' in _html("/learn/replay/long-put?step=decision")
    assert 'data-start="recap"' in _html("/learn/replay/covered-call?step=recap")
    assert 'data-start=""' in _html("/learn/replay/long-call?step=skip")


def test_progress_stores_replay_slugs_without_dropping_episodes():
    clean = learn_progress.sanitize({
        "replays": ["long-call", "nope", "long-call", "covered-call"],
        "done": ["what-is-an-option"],
    })
    assert clean["replays"] == ["long-call", "covered-call"]
    assert clean["done"] == ["what-is-an-option"]
    assert learn_progress.sanitize({"replays": ["calls-and-puts"]})["replays"] == []

    merged = learn_progress.merge(
        {"updated": 1, "replays": ["long-call"], "done": [], "last": None},
        {"updated": 2, "replays": ["covered-call"], "done": ["calls-and-puts"], "last": None},
    )
    assert merged["replays"] == ["long-call", "covered-call"]
    assert merged["done"] == ["calls-and-puts"]


def test_saving_a_replay_keeps_the_episode_resume(monkeypatch):
    stored = {
        "updated": 4,
        "last": {
            "slug": "what-is-an-option",
            "t": 8,
            "title": "What is an option?",
            "number": 1,
        },
        "done": ["what-is-an-option"],
        "replays": [],
    }

    def fake_load(_user_id):
        return learn_progress.sanitize(stored)

    monkeypatch.setattr(learn_progress, "load_for_user", fake_load)
    monkeypatch.setattr(learn_progress, "execute", lambda *_args, **_kwargs: None)
    result = learn_progress.save_for_user(7, {"replays": ["long-put"]})
    assert result["replays"] == ["long-put"]
    assert result["done"] == ["what-is-an-option"]
    assert result["last"]["slug"] == "what-is-an-option"


def test_replay_only_progress_still_syncs():
    js = open("app/static/js/learn-progress.js", encoding="utf-8").read()
    guard = "if (!progress.last && !(progress.done || []).length && !(progress.replays || []).length) return;"
    assert guard in js


def test_replay_stylesheet_uses_gain_loss_tokens():
    css = open("app/templates/learn/_styles.html", encoding="utf-8").read()
    assert "#28c08a" not in css
    assert "#f0556d" not in css
    assert "var(--gain)" in css
    assert "var(--loss)" in css
    assert "box-shadow: none" in css
    assert "repeat(3, minmax(0, 1fr))" in css
    assert "white-space: nowrap" in css
