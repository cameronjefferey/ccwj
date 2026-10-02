"""Where someone left off in Options 101, and which replays they finished.

The browser keeps this in localStorage so it works logged out. Signed-in
accounts (except the shared demo user) also store one small JSON blob so
the same spot is there on the next device. ``done`` is episodes. ``replays``
is replay slugs. A finished replay is a check on /learn, not a score.
No points, streaks, or badges.
"""

from __future__ import annotations

import json
import logging
import time

from app import learn_catalog as catalog
from app import learn_replay
from app.db import execute, fetch_one

logger = logging.getLogger(__name__)

_MAX_T = 8 * 60 * 60
_MAX_UPDATED = 9_000_000_000_000


def empty():
    return {"updated": 0, "last": None, "done": [], "replays": []}


def sanitize(payload):
    """Keep only known episodes. Titles come from the catalog, not the client."""
    by_slug = {episode["slug"]: episode for episode in catalog.episodes()}
    if not isinstance(payload, dict):
        return empty()

    done = []
    for slug in payload.get("done") or []:
        if slug in by_slug and slug not in done:
            done.append(slug)
        if len(done) >= 50:
            break

    replay_slugs = {replay["slug"] for replay in learn_replay.replays()}
    replays = []
    for slug in payload.get("replays") or []:
        if slug in replay_slugs and slug not in replays:
            replays.append(slug)
        if len(replays) >= 50:
            break

    last = None
    raw_last = payload.get("last")
    if isinstance(raw_last, dict) and raw_last.get("slug") in by_slug:
        episode = by_slug[raw_last["slug"]]
        try:
            seconds = int(raw_last.get("t") or 0)
        except (TypeError, ValueError):
            seconds = 0
        seconds = max(0, min(seconds, _MAX_T))
        last = {
            "slug": episode["slug"],
            "t": seconds,
            "title": episode["title"],
            "number": episode["number"],
        }

    try:
        updated = int(payload.get("updated") or 0)
    except (TypeError, ValueError):
        updated = 0
    updated = max(0, min(updated, _MAX_UPDATED))
    if (last or done or replays) and updated == 0:
        updated = int(time.time() * 1000)
    return {"updated": updated, "last": last, "done": done, "replays": replays}


def merge(left, right):
    """Union of finished episodes. The newer timestamp wins the resume point."""
    done = []
    for slug in list(left.get("done") or []) + list(right.get("done") or []):
        if slug not in done:
            done.append(slug)
    replays = []
    for slug in list(left.get("replays") or []) + list(right.get("replays") or []):
        if slug not in replays:
            replays.append(slug)
    left_updated = left.get("updated") or 0
    right_updated = right.get("updated") or 0
    last = left.get("last") if left_updated >= right_updated else right.get("last")
    if last is None:
        last = left.get("last") or right.get("last")
    return {
        "updated": max(left_updated, right_updated),
        "last": last,
        "done": done,
        "replays": replays,
    }


def _parse_row(row):
    if not row:
        return empty()
    progress = row.get("progress")
    if isinstance(progress, str):
        progress = json.loads(progress)
    return sanitize(progress)


def load_for_user(user_id):
    row = fetch_one(
        "SELECT progress FROM learn_progress WHERE user_id = %s",
        (user_id,),
    )
    return _parse_row(row)


def save_for_user(user_id, payload):
    incoming = sanitize(payload)
    if not incoming["last"] and not incoming["done"] and not incoming["replays"]:
        return load_for_user(user_id)
    merged = merge(incoming, load_for_user(user_id))
    execute(
        """
        INSERT INTO learn_progress (user_id, progress)
        VALUES (%s, %s::jsonb)
        ON CONFLICT (user_id) DO UPDATE
        SET progress = EXCLUDED.progress,
            updated_at = NOW()
        """,
        (user_id, json.dumps(merged)),
    )
    return merged
