"""Standalone fantasy-draft availability poll. Not part of HappyTrader."""

from __future__ import annotations

import calendar
import hashlib
import hmac
import json
import os
import re
import threading
from datetime import date, datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, redirect, render_template, request

POLL_START = date.fromisoformat(os.environ.get("DRAFT_START", "2026-08-15"))
POLL_END = date.fromisoformat(os.environ.get("DRAFT_END", "2026-09-08"))
KICKOFF = date.fromisoformat(os.environ.get("DRAFT_KICKOFF", "2026-09-09"))
LABOR_DAY = date(2026, 9, 7)
TITLE = os.environ.get("DRAFT_TITLE", "Draft Night")
TIME_LABEL = os.environ.get("DRAFT_TIME", "4:00 PM Pacific")
TIME_NOTE = os.environ.get(
    "DRAFT_TIME_NOTE", "7:00 PM Eastern · 6:00 PM Central · 5:00 PM Mountain"
)
# Point the old free service at the paid always-on URL.
CANONICAL_URL = os.environ.get("DRAFT_CANONICAL_URL", "").rstrip("/")
MASH_SECONDS = int(os.environ.get("DRAFT_MASH_SECONDS", "8"))
MASH_MAX = 250

_LOCK = threading.Lock()
_NAME_RE = re.compile(r"[^\w\s.'\-]", re.UNICODE)

app = Flask(__name__)
app.url_map.strict_slashes = False


def _poll_path() -> Path:
    # On Render: POLL_PATH=/var/data/poll.json (persistent disk). Local default is a file next to this app.
    return Path(os.environ.get("POLL_PATH", Path(__file__).parent / "poll.json"))


def _empty() -> dict:
    return {"people": [], "mash_revealed": False}


def _host_pin() -> str:
    return os.environ.get("DRAFT_HOST_PIN", "")


def _check_pin(raw) -> bool:
    pin = _host_pin()
    if not pin:
        return False
    got = hashlib.sha256(str(raw or "").encode()).digest()
    want = hashlib.sha256(pin.encode()).digest()
    return hmac.compare_digest(got, want)


def _has_mashed(person: dict) -> bool:
    return bool(person.get("mash_finished_at")) or person.get("mash_count") is not None


def _public_person(person: dict, revealed: bool) -> dict:
    out = {
        "name": person.get("name"),
        "dates": list(person.get("dates") or []),
        "mashed": _has_mashed(person),
    }
    if revealed and _has_mashed(person):
        out["mash_count"] = int(person.get("mash_count") or 0)
        out["mash_finished_at"] = person.get("mash_finished_at")
    return out


def _draft_order(people: list) -> list[dict]:
    mashed = [p for p in people if _has_mashed(p)]

    def sort_key(person: dict):
        count = int(person.get("mash_count") or 0)
        finished = str(person.get("mash_finished_at") or "")
        return (-count, finished, str(person.get("name") or "").casefold())

    ranked = []
    for i, person in enumerate(sorted(mashed, key=sort_key), start=1):
        ranked.append(
            {
                "pick": i,
                "name": person["name"],
                "mash_count": int(person.get("mash_count") or 0),
            }
        )
    return ranked


def _load() -> dict:
    path = _poll_path()
    if not path.exists():
        return _empty()
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return _empty()
    if not isinstance(data, dict) or not isinstance(data.get("people"), list):
        return _empty()
    data.setdefault("mash_revealed", False)
    return data


def _save(data: dict) -> None:
    path = _poll_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def _clean_name(raw: str | None) -> str:
    name = " ".join((raw or "").split())
    name = _NAME_RE.sub("", name).strip()
    if len(name) > 32:
        name = name[:32].rstrip()
    if len(name) < 1:
        raise ValueError("name required")
    return name


def _in_range(d: date) -> bool:
    return POLL_START <= d <= POLL_END


def _parse_day(raw: str) -> date:
    d = date.fromisoformat(raw)
    if not _in_range(d):
        raise ValueError("date out of range")
    return d


def _find_person(people: list, name: str) -> dict | None:
    key = name.casefold()
    for person in people:
        if str(person.get("name", "")).casefold() == key:
            return person
    return None


def _merge_people(existing: list, incoming: list) -> list:
    merged = {str(p.get("name", "")).casefold(): dict(p) for p in existing if p.get("name")}
    for raw in incoming:
        try:
            name = _clean_name(raw.get("name") if isinstance(raw, dict) else None)
            dates = _valid_dates((raw or {}).get("dates") if isinstance(raw, dict) else [])
        except ValueError:
            continue
        key = name.casefold()
        prev = merged.get(key)
        if prev is None:
            merged[key] = {"name": name, "dates": dates}
        else:
            prev["name"] = name
            prev["dates"] = sorted(set(prev.get("dates") or []) | set(dates))
            # Keep mash fields on the existing row; restore never overwrites scores.
    people = list(merged.values())
    people.sort(key=lambda p: p["name"].casefold())
    return people


def _valid_dates(raw_dates) -> list[str]:
    if raw_dates is None:
        return []
    if not isinstance(raw_dates, list):
        raise ValueError("dates must be a list")
    out = []
    seen = set()
    for item in raw_dates:
        d = _parse_day(str(item))
        iso = d.isoformat()
        if iso not in seen:
            seen.add(iso)
            out.append(iso)
    return sorted(out)


def _month_grid(year: int, month: int) -> list[list[dict]]:
    cal = calendar.Calendar(firstweekday=6)  # Sunday
    weeks = []
    for week in cal.monthdatescalendar(year, month):
        row = []
        for d in week:
            row.append(
                {
                    "date": d.isoformat(),
                    "day": d.day,
                    "in_month": d.month == month,
                    "enabled": d.month == month and _in_range(d),
                    "is_weekend": d.weekday() >= 5,
                    "is_kickoff": d == KICKOFF,
                    "is_labor_day": d == LABOR_DAY,
                    "is_today": d == datetime.now(timezone.utc).date(),
                }
            )
        weeks.append(row)
    return weeks


def _best_dates(people: list) -> list[dict]:
    counts: dict[str, list[str]] = {}
    for person in people:
        for iso in person.get("dates") or []:
            counts.setdefault(iso, []).append(person["name"])

    def sort_key(iso: str):
        d = date.fromisoformat(iso)
        # More people first; weekends (Sat, Sun, Fri) beat weekdays on ties.
        weekend_rank = {5: 2, 6: 3, 4: 1}.get(d.weekday(), 0)
        return (-len(counts[iso]), -weekend_rank, iso)

    ranked = []
    for iso in sorted(counts, key=sort_key):
        d = date.fromisoformat(iso)
        ranked.append(
            {
                "date": iso,
                "label": f"{d.strftime('%a, %b')} {d.day}",
                "count": len(counts[iso]),
                "names": counts[iso],
            }
        )
    return ranked[:3]


def _payload() -> dict:
    data = _load()
    people = data["people"]
    months = []
    cursor = date(POLL_START.year, POLL_START.month, 1)
    last = date(POLL_END.year, POLL_END.month, 1)
    while cursor <= last:
        months.append(
            {
                "year": cursor.year,
                "month": cursor.month,
                "label": cursor.strftime("%B %Y"),
                "weeks": _month_grid(cursor.year, cursor.month),
            }
        )
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)

    by_date: dict[str, list[str]] = {}
    for person in people:
        for iso in person.get("dates") or []:
            by_date.setdefault(iso, []).append(person["name"])

    revealed = bool(data.get("mash_revealed"))
    public_people = [_public_person(p, revealed) for p in people]
    mashed_count = sum(1 for p in public_people if p["mashed"])
    order = _draft_order(people) if revealed else []
    if revealed:
        pick_by_name = {row["name"].casefold(): row["pick"] for row in order}
        for person in public_people:
            pick = pick_by_name.get(str(person["name"]).casefold())
            if pick:
                person["pick"] = pick

    return {
        "title": TITLE,
        "time_label": TIME_LABEL,
        "time_note": TIME_NOTE,
        "start": POLL_START.isoformat(),
        "end": POLL_END.isoformat(),
        "kickoff": KICKOFF.isoformat(),
        "kickoff_label": f"{KICKOFF.strftime('%a, %b')} {KICKOFF.day}",
        "people": public_people,
        "months": months,
        "by_date": by_date,
        "best": _best_dates(people),
        "respondent_count": len(people),
        "mash": {
            "seconds": MASH_SECONDS,
            "revealed": revealed,
            "mashed_count": mashed_count,
            "total": len(people),
        },
        "draft_order": order,
    }


@app.after_request
def _noindex(resp):
    resp.headers["X-Robots-Tag"] = "noindex, nofollow"
    return resp


@app.get("/")
def index():
    if CANONICAL_URL:
        return redirect(CANONICAL_URL, code=302)
    return render_template("index.html", title=TITLE, time_label=TIME_LABEL)


@app.get("/api/poll")
def api_poll():
    with _LOCK:
        return jsonify(_payload())


@app.post("/api/poll")
@app.post("/api/availability")
def api_availability():
    body = request.get_json(silent=True) or {}
    try:
        name = _clean_name(body.get("name"))
        dates = _valid_dates(body.get("dates"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    with _LOCK:
        data = _load()
        people = data["people"]
        person = _find_person(people, name)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if person is None:
            person = {"name": name, "dates": dates, "updated_at": now}
            people.append(person)
        else:
            person["name"] = name
            person["dates"] = dates
            person["updated_at"] = now
        people.sort(key=lambda p: p["name"].casefold())
        _save(data)
        return jsonify(_payload())


@app.post("/api/restore")
def api_restore():
    body = request.get_json(silent=True) or {}
    incoming = body.get("people")
    if not isinstance(incoming, list):
        return jsonify({"error": "people must be a list"}), 400
    with _LOCK:
        data = _load()
        data["people"] = _merge_people(data["people"], incoming)
        _save(data)
        return jsonify(_payload())


@app.post("/api/host")
def api_host():
    body = request.get_json(silent=True) or {}
    if not _check_pin(body.get("pin")):
        return jsonify({"error": "wrong pin"}), 403
    with _LOCK:
        data = _load()
        mashed = []
        for person in data["people"]:
            if not _has_mashed(person):
                continue
            mashed.append(
                {
                    "name": person.get("name"),
                    "mash_count": int(person.get("mash_count") or 0),
                    "mash_finished_at": person.get("mash_finished_at"),
                }
            )
        mashed.sort(key=lambda row: (-row["mash_count"], str(row["mash_finished_at"] or "")))
        return jsonify(
            {
                "ok": True,
                "revealed": bool(data.get("mash_revealed")),
                "mashed": mashed,
            }
        )


@app.post("/api/mash")
def api_mash():
    body = request.get_json(silent=True) or {}
    try:
        name = _clean_name(body.get("name"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        count = int(body.get("count"))
    except (TypeError, ValueError):
        return jsonify({"error": "count required"}), 400
    if count < 1 or count > MASH_MAX:
        return jsonify({"error": "count out of range"}), 400

    with _LOCK:
        data = _load()
        if data.get("mash_revealed"):
            return jsonify({"error": "order already revealed"}), 409
        person = _find_person(data["people"], name)
        if person is None:
            return jsonify({"error": "name not on the list"}), 404
        if _has_mashed(person):
            return jsonify({"error": "already mashed"}), 409
        person["mash_count"] = count
        person["mash_finished_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        _save(data)
        return jsonify(_payload())


@app.post("/api/reset")
def api_reset():
    body = request.get_json(silent=True) or {}
    if not _check_pin(body.get("pin")):
        return jsonify({"error": "wrong pin"}), 403
    with _LOCK:
        data = _load()
        cleaned = []
        for person in data["people"]:
            name = person.get("name")
            if not name:
                continue
            cleaned.append({"name": name, "dates": []})
        cleaned.sort(key=lambda p: str(p["name"]).casefold())
        _save({"people": cleaned, "mash_revealed": False})
        return jsonify(_payload())


@app.post("/api/reveal")
def api_reveal():
    body = request.get_json(silent=True) or {}
    if not _check_pin(body.get("pin")):
        return jsonify({"error": "wrong pin"}), 403
    with _LOCK:
        data = _load()
        data["mash_revealed"] = True
        _save(data)
        return jsonify(_payload())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=os.environ.get("FLASK_DEBUG") == "1")
