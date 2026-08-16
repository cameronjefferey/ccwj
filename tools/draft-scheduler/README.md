# Draft Night

A one-page calendar poll for picking a fantasy football draft. Every slot is **4:00 PM Pacific** (7:00 PM Eastern). Friends open **one URL**, tap their name, and tap the nights they can make it. The server remembers.

This is a standalone app. It is not part of HappyTrader and is not served from the main site.

**Live:** https://draft-night-live.onrender.com

## How it works for the group

1. Open the link.
2. Add names (host) or tap your name.
3. Tap every night you can do 4:00 PM Pacific.

That's it. No second link, no order, no packing dates into the URL.

## Why it's on a paid Render instance

Free web services sleep after ~15 minutes and wipe the local file on recycle. Paid **Starter** (~$7/mo) stays awake. A **1GB disk** at `/var/data` is what actually keeps `poll.json` across deploys and restarts. You do not need a database; 30 days of names and nights is a tiny JSON file.

Delete the `draft-night-live` service after the draft to stop billing. The older free service `draft-night` (`draft-night-grvg.onrender.com`) can be deleted too — do not share that one.

## Run it locally

From this folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open http://127.0.0.1:5050.

## Deploy

```text
Root directory: tools/draft-scheduler
Build:          pip install -r requirements.txt
Start:          gunicorn -b 0.0.0.0:$PORT app:app
Instance:       Starter
Disk:           1GB at /var/data
POLL_PATH:      /var/data/poll.json
```

Or apply `render.yaml` as a Blueprint. Do not merge this into `app/render.yaml`.

## What people see

- August + September calendars (Sat Aug 15 – Tue Sep 8, 2026)
- Tap a date to mark yourself free; tap again to undo
- Green heat = more people free that night
- Lime outline = nights you picked
- **Game** on Wed Sep 9 (first game) — not pickable
- Remove a name from the people list
- A “best so far” pick at the top, weekends winning ties

## Optional env

| Variable | Default |
|---|---|
| `DRAFT_TITLE` | `Draft Night` |
| `DRAFT_TIME` | `4:00 PM Pacific` |
| `DRAFT_TIME_NOTE` | `7:00 PM Eastern · 6:00 PM Central · 5:00 PM Mountain` |
| `DRAFT_START` / `DRAFT_END` | `2026-08-15` / `2026-09-08` |
| `DRAFT_KICKOFF` | `2026-09-09` |
| `POLL_PATH` | `./poll.json` locally; `/var/data/poll.json` on Render |
| `PORT` | `5050` locally, Render sets this |

## Tests

```bash
python -m unittest test_app.py
```
