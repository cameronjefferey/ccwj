"""Options 101 catalog for /learn.

Everything the pages render comes from ``app/learn_episodes.json``.
``data/`` is gitignored Schwab sync output, and ``*.json`` is ignored
repo-wide, so this file is the explicit exception.
Restart the app after editing the file (it is read once at import).

Add a YouTube video
    Set ``youtube_id`` to the 11-character id in the watch URL
    (``https://www.youtube.com/watch?v=THIS``). A null id renders as Coming
    soon: no embed, and the series grid does not link the card. Episodes
    1–10 are published and already carry public ids.

    Shorts use the same ``youtube_id`` field. Null stays a Coming soon tile.
    A real id becomes a vertical lite embed. The series playlist is
    ``series.playlist_url``.

Add an episode
    Append an object with the same keys and a new ``slug`` (lowercase words
    separated by hyphens). ``number`` is the series order. Set ``published``
    to true when the page should be linked from the grid (once it has a
    youtube_id), listed in the sitemap, and indexed. ``published: false``
    still has a URL, but it is noindex and absent from the sitemap.

    ``thumbnail`` is optional. Null plus a youtube_id uses the YouTube
    thumbnail. A ``https://`` URL is used as-is. Any other string is a
    path under ``app/static/`` (for example ``learn/custom.png``).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_PATH = Path(__file__).resolve().parent / "learn_episodes.json"
_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_YOUTUBE_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
_EPISODE_KEYS = (
    "slug",
    "number",
    "title",
    "summary",
    "youtube_id",
    "duration",
    "thumbnail",
    "vocab",
    "chapters",
    "recap",
    "shorts",
    "published",
)
_SERIES_KEYS = (
    "title",
    "subhead",
    "description",
    "cta_heading",
    "cta_button",
    "cta_note",
    "disclaimer",
    "playlist_url",
)

_NOCOOKIE_EMBED = "https://www.youtube-nocookie.com/embed/{youtube_id}"
_EMBED_QUERY = "rel=0&modestbranding=1&enablejsapi=1&playsinline=1"


def valid_youtube_id(value) -> bool:
    return isinstance(value, str) and bool(_YOUTUBE_ID.fullmatch(value))


def embed_url(youtube_id, start=None):
    """Player URL on youtube-nocookie.com, or None when the id is missing."""
    if not valid_youtube_id(youtube_id):
        return None
    url = _NOCOOKIE_EMBED.format(youtube_id=youtube_id) + "?" + _EMBED_QUERY
    if start:
        url += f"&start={int(start)}"
    return url


def is_watchable(episode) -> bool:
    """Live only when the episode is published and a real YouTube id is set."""
    return bool(episode.get("published") and valid_youtube_id(episode.get("youtube_id")))


def format_clock(seconds) -> str:
    seconds = int(seconds)
    return f"{seconds // 60}:{seconds % 60:02d}"


def duration_iso(duration):
    """Schema.org duration from ``6 min`` or ``8:20``. None when unknown."""
    if not isinstance(duration, str):
        return None
    text = duration.strip().lower()
    minutes = re.fullmatch(r"(\d+)\s*min", text)
    if minutes:
        return f"PT{int(minutes.group(1))}M"
    clock = re.fullmatch(r"(\d+):(\d{2})", text)
    if clock:
        return f"PT{int(clock.group(1))}M{int(clock.group(2))}S"
    return None


def recap_paragraphs(recap):
    if not isinstance(recap, str):
        return []
    return [part.strip() for part in re.split(r"\n\s*\n", recap) if part.strip()]


def thumbnail_source(episode):
    """https URL, a path under app/static, or None for a CSS placeholder."""
    thumb = episode.get("thumbnail")
    if isinstance(thumb, str) and thumb:
        return thumb
    youtube_id = episode.get("youtube_id")
    if valid_youtube_id(youtube_id):
        return f"https://i.ytimg.com/vi/{youtube_id}/maxresdefault.jpg"
    return None


def series():
    return _SERIES


def episodes():
    return _EPISODES


def episode_by_slug(slug):
    for episode in _EPISODES:
        if episode["slug"] == slug:
            return episode
    return None


def published_episodes():
    return [episode for episode in _EPISODES if episode["published"]]


def neighbors(slug):
    """Previous and next published episode, or None at each end."""
    rows = published_episodes()
    for index, episode in enumerate(rows):
        if episode["slug"] == slug:
            previous = rows[index - 1] if index else None
            nxt = rows[index + 1] if index + 1 < len(rows) else None
            return previous, nxt
    return None, None


def series_shorts():
    """Shorts from published episodes, in episode order."""
    shorts = []
    for episode in published_episodes():
        for short in episode["shorts"]:
            shorts.append({"episode_slug": episode["slug"], **short})
    return shorts


def sitemap_paths():
    """(path, changefreq, priority) for the public index and published episodes."""
    rows = [("/learn", "weekly", "0.8")]
    for episode in published_episodes():
        rows.append((f"/learn/{episode['slug']}", "monthly", "0.6"))
    return rows


def _optional_id(value, slug, label):
    if value is None or value == "":
        return None
    if not valid_youtube_id(value):
        raise ValueError(
            f"{slug}: {label} must be an 11-character YouTube id or null, got {value!r}"
        )
    return value


def _optional_thumb(value, slug):
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"{slug}: thumbnail must be a string or null")
    if value.startswith("https://"):
        return value
    if value.startswith("/") or value.startswith("http://") or ".." in value:
        raise ValueError(f"{slug}: thumbnail must be an https URL or a static path")
    return value


def _validate_episode(raw):
    if not isinstance(raw, dict):
        raise ValueError("each episode must be an object")
    missing = [key for key in _EPISODE_KEYS if key not in raw]
    if missing:
        raise ValueError(f"episode missing {', '.join(missing)}")
    slug = raw["slug"]
    if not isinstance(slug, str) or not _SLUG.fullmatch(slug):
        raise ValueError(f"bad slug: {slug!r}")
    if not isinstance(raw["number"], int) or raw["number"] < 1:
        raise ValueError(f"{slug}: number must be a positive integer")
    for key in ("title", "summary"):
        if not isinstance(raw[key], str) or not raw[key].strip():
            raise ValueError(f"{slug}: {key} must be a non-empty string")
    if raw["duration"] is not None and (
        not isinstance(raw["duration"], str) or not raw["duration"].strip()
    ):
        raise ValueError(f"{slug}: duration must be a string or null")
    if not isinstance(raw["published"], bool):
        raise ValueError(f"{slug}: published must be true or false")
    if not isinstance(raw["recap"], str):
        raise ValueError(f"{slug}: recap must be a string")
    if not isinstance(raw["vocab"], list) or not all(
        isinstance(word, str) and word.strip() for word in raw["vocab"]
    ):
        raise ValueError(f"{slug}: vocab must be a list of strings")
    chapters = []
    if not isinstance(raw["chapters"], list):
        raise ValueError(f"{slug}: chapters must be a list")
    for chapter in raw["chapters"]:
        if not isinstance(chapter, dict) or set(chapter) != {"t", "label"}:
            raise ValueError(f"{slug}: each chapter needs t and label")
        if not isinstance(chapter["t"], int) or chapter["t"] < 0:
            raise ValueError(f"{slug}: chapter t must be a non-negative integer")
        if not isinstance(chapter["label"], str) or not chapter["label"].strip():
            raise ValueError(f"{slug}: chapter label must be a non-empty string")
        chapters.append({"t": chapter["t"], "label": chapter["label"].strip()})
    shorts = []
    if not isinstance(raw["shorts"], list):
        raise ValueError(f"{slug}: shorts must be a list")
    for short in raw["shorts"]:
        if not isinstance(short, dict) or set(short) != {"youtube_id", "title"}:
            raise ValueError(f"{slug}: each short needs youtube_id and title")
        if not isinstance(short["title"], str) or not short["title"].strip():
            raise ValueError(f"{slug}: short title must be a non-empty string")
        shorts.append({
            "youtube_id": _optional_id(short["youtube_id"], slug, "short youtube_id"),
            "title": short["title"].strip(),
        })
    return {
        "slug": slug,
        "number": raw["number"],
        "title": raw["title"].strip(),
        "summary": raw["summary"].strip(),
        "youtube_id": _optional_id(raw["youtube_id"], slug, "youtube_id"),
        "duration": raw["duration"].strip() if isinstance(raw["duration"], str) else None,
        "thumbnail": _optional_thumb(raw["thumbnail"], slug),
        "vocab": [word.strip() for word in raw["vocab"]],
        "chapters": chapters,
        "recap": raw["recap"].strip(),
        "shorts": shorts,
        "published": raw["published"],
    }


def _load(path=None):
    path = Path(path) if path else _PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("learn catalog must be an object")
    series_raw = payload.get("series")
    if not isinstance(series_raw, dict):
        raise ValueError("learn catalog needs a series object")
    missing = [key for key in _SERIES_KEYS if not str(series_raw.get(key) or "").strip()]
    if missing:
        raise ValueError(f"series missing {', '.join(missing)}")
    series_out = {key: str(series_raw[key]).strip() for key in _SERIES_KEYS}
    if not series_out["playlist_url"].startswith("https://"):
        raise ValueError("series playlist_url must be an https URL")
    raw_episodes = payload.get("episodes")
    if not isinstance(raw_episodes, list) or not raw_episodes:
        raise ValueError("learn catalog needs at least one episode")
    loaded = [_validate_episode(raw) for raw in raw_episodes]
    slugs = [episode["slug"] for episode in loaded]
    numbers = [episode["number"] for episode in loaded]
    if len(slugs) != len(set(slugs)):
        raise ValueError("episode slugs must be unique")
    if len(numbers) != len(set(numbers)):
        raise ValueError("episode numbers must be unique")
    loaded.sort(key=lambda episode: episode["number"])
    return series_out, tuple(loaded)


_SERIES, _EPISODES = _load()
