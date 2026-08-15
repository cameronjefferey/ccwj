# Draft Night

A one-page calendar poll for picking a fantasy football draft. Every slot is **4:00 PM Pacific** (7:00 PM Eastern). Friends open the link, type their name, and tap the nights they can make it. The page ranks the best dates as people respond.

This is a standalone app. It is not part of HappyTrader and is not served from the main site.

## Run it tonight (fastest)

From this folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open http://127.0.0.1:5050. To send a link to the group chat, put a public URL in front of it — Cloudflare Quick Tunnel needs no account:

```bash
npx --yes cloudflared tunnel --url http://127.0.0.1:5050
```

That prints a `https://….trycloudflare.com` URL. Paste that in the chat.

## Deploy for the week (Render free)

This is a **separate** web service from HappyTrader. Free instances sleep after
15 minutes idle (first open can take ~30–50s) and wipe the vote file on
redeploy — fine for a one-week poll.

```text
Root directory: tools/draft-scheduler
Build:          pip install -r requirements.txt
Start:          gunicorn -b 0.0.0.0:$PORT app:app
Instance:       Free
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
| `POLL_PATH` | `./poll.json` |
| `PORT` | `5050` locally, Render sets this |

## Tests

```bash
python -m unittest test_app.py
```
