"""Public Options 101 pages (/learn). No login and no warehouse reads.

Replays live at /learn/replay/<slug>. Their prices are in YAML, not
BigQuery, so this module still does not query the warehouse.
"""

import logging

from flask import abort, jsonify, redirect, render_template, request, url_for
from flask_login import current_user
from flask_wtf.csrf import CSRFError

from app import app
from app import learn_catalog as catalog
from app import learn_progress
from app import learn_replay
from app import learn_try
from app.extensions import limiter

logger = logging.getLogger(__name__)


@app.errorhandler(CSRFError)
def _learn_progress_csrf(err):
    """A missing token on the progress save should be JSON, not an HTML 400.

    The browser posts localStorage here. An HTML error page looks like a
    blocked request and the checkmarks never leave this browser.
    """
    if request.path == "/learn/progress":
        return jsonify(error="Refresh the page, then try again."), 400
    return err.get_response()

_FALLBACK_THUMB = "learn/options-101.png"


def _absolute(url):
    if url.startswith(("https://", "http://")):
        return url
    if url.startswith("/"):
        return request.url_root.rstrip("/") + url
    return url_for("static", filename=url, _external=True)


def _thumb(episode, external=False):
    """Resolved thumbnail, or None when the card should use the CSS placeholder."""
    raw = catalog.thumbnail_source(episode)
    if not raw:
        return None
    if raw.startswith("https://"):
        resolved = raw
    else:
        resolved = url_for("static", filename=raw)
    return _absolute(resolved) if external else resolved


def _view_short(short):
    youtube_id = short.get("youtube_id")
    watchable = catalog.valid_youtube_id(youtube_id)
    return {
        "title": short["title"],
        "watchable": watchable,
        "embed_src": catalog.embed_url(youtube_id) if watchable else None,
        "thumb": (
            f"https://i.ytimg.com/vi/{youtube_id}/maxresdefault.jpg" if watchable else None
        ),
    }


def _view_episode(episode):
    watchable = catalog.is_watchable(episode)
    return {
        "slug": episode["slug"],
        "number": episode["number"],
        "number_label": f"{episode['number']:02d}",
        "title": episode["title"],
        "summary": episode["summary"],
        "duration": episode["duration"],
        "published": episode["published"],
        "watchable": watchable,
        "vocab": episode["vocab"],
        "chapters": [
            {
                "t": chapter["t"],
                "label": chapter["label"],
                "clock": catalog.format_clock(chapter["t"]),
            }
            for chapter in episode["chapters"]
        ],
        "recap_paragraphs": catalog.recap_paragraphs(episode["recap"]),
        "shorts": [_view_short(short) for short in episode["shorts"]],
        "embed_src": catalog.embed_url(episode["youtube_id"]) if watchable else None,
        "thumb": _thumb(episode) if watchable else None,
    }


def _video_ld(episode, page_url, thumb_url):
    if not catalog.is_watchable(episode):
        return None
    payload = {
        "@context": "https://schema.org",
        "@type": "VideoObject",
        "name": episode["title"],
        "description": episode["summary"],
        "embedUrl": catalog.embed_url(episode["youtube_id"]),
        "thumbnailUrl": thumb_url,
        "url": page_url,
    }
    duration = catalog.duration_iso(episode.get("duration"))
    if duration:
        payload["duration"] = duration
    return payload


def _fallback_thumb():
    return url_for("static", filename=_FALLBACK_THUMB, _external=True)


@app.route("/learn/")
@app.route("/learn")
def learn_index():
    series = catalog.series()
    rows = [_view_episode(episode) for episode in catalog.episodes()]
    first = next((row for row in rows if row["published"]), rows[0] if rows else None)
    return render_template(
        "learn/index.html",
        title="Options 101",
        meta_description=series["description"],
        canonical=url_for("learn_index", _external=True),
        og_image=_fallback_thumb(),
        og_alt=series["title"],
        series=series,
        episodes=rows,
        replays=learn_replay.replays(),
        shorts=[_view_short(short) for short in catalog.series_shorts()],
        first_episode=first,
    )


def _can_sync_progress():
    if not current_user.is_authenticated:
        return False
    return getattr(current_user, "username", None) != "demo"


@app.route("/learn/progress", methods=["GET", "POST"])
@limiter.limit("120 per minute", methods=["GET"])
@limiter.limit("120 per minute", methods=["POST"])
def learn_progress_api():
    """Resume point for the signed-in account. Logged-out and demo stay local."""
    if not _can_sync_progress():
        if request.method == "POST":
            return ("", 204)
        return jsonify(learn_progress.empty())
    if request.content_length and request.content_length > 8000:
        return ("", 204)
    try:
        if request.method == "GET":
            return jsonify(learn_progress.load_for_user(current_user.id))
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            body = {}
        return jsonify(learn_progress.save_for_user(current_user.id, body))
    except Exception:
        logger.warning("learn progress unavailable", exc_info=True)
        if request.method == "POST":
            return ("", 204)
        return jsonify(learn_progress.empty())


@app.route("/learn/replay")
@app.route("/learn/replay/")
def learn_replay_index():
    return redirect(url_for("learn_index") + "#replays")


@app.route("/learn/replay/<slug>")
def learn_replay_page(slug):
    replay = learn_replay.replay_by_slug(slug)
    if replay is None:
        abort(404)
    step = request.args.get("step") or ""
    if step not in ("decision", "recap"):
        step = ""
    series = catalog.series()
    page_url = url_for("learn_replay_page", slug=slug, _external=True)
    return render_template(
        "learn/replay.html",
        title=f"{replay['title']} · Replay",
        meta_description=(
            f"{replay['summary']} Illustrative prices, for learning only."
        ),
        canonical=page_url,
        og_image=_fallback_thumb(),
        og_alt=replay["title"],
        series=series,
        replay=replay,
        start_step=step,
        try_it=learn_try.for_replay(slug),
    )


@app.route("/learn/<slug>")
def learn_episode(slug):
    episode = catalog.episode_by_slug(slug)
    if episode is None:
        abort(404)
    series = catalog.series()
    page_url = url_for("learn_episode", slug=slug, _external=True)
    thumb = _thumb(episode, external=True) if catalog.is_watchable(episode) else None
    og_image = thumb or _fallback_thumb()
    previous, nxt = (
        catalog.neighbors(slug) if episode["published"] else (None, None)
    )
    return render_template(
        "learn/episode.html",
        title=f"{episode['title']} · Options 101",
        meta_description=(
            f"{episode['summary']} Episode {episode['number']} of Options 101."
        ),
        canonical=page_url,
        og_image=og_image,
        og_alt=episode["title"],
        series=series,
        episode=_view_episode(episode),
        previous=_view_episode(previous) if previous else None,
        next_episode=_view_episode(nxt) if nxt else None,
        lesson_replays=learn_replay.replays_for_lesson(slug),
        try_it=learn_try.for_lesson(slug) if episode["published"] else None,
        checks=learn_try.checks_for_lesson(slug) if episode["published"] else [],
        deeper=learn_try.deeper_for_lesson(slug) if episode["published"] else None,
        video_ld=_video_ld(episode, page_url, og_image),
        robots="noindex" if not episode["published"] else None,
    )
