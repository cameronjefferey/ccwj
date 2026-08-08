"""Render app pages to static HTML as a logged-in dev user (mobile QA aid).

Uses the Flask test client with a forced flask-login session so pages render
with REAL dev-warehouse data (BQ_DATASET=analytics_dev via .env), then saves
the HTML into /tmp/ht_pages/ next to a `static` symlink so a plain
`python -m http.server` serves them with working CSS/JS. Screenshot with
headless Chrome at a phone viewport to QA responsive layout.

Usage:
    .venv/bin/python scripts/dev_render_pages.py --user testingcameron1 \
        /daily-review /positions "/position/JEPI"
"""

import argparse
import os
import pathlib
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

OUT_DIR = pathlib.Path("/tmp/ht_pages")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="testingcameron1")
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args()

    from app import app
    from app.db import fetch_one

    row = fetch_one("select id from users where username = %s", (args.user,))
    if not row:
        sys.exit(f"no local user named {args.user!r}")
    user_id = row["id"]

    OUT_DIR.mkdir(exist_ok=True)
    static_link = OUT_DIR / "static"
    if not static_link.exists():
        static_link.symlink_to(pathlib.Path(app.static_folder).resolve())

    client = app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = str(user_id)
        sess["_fresh"] = True

    for path in args.paths:
        # X-HT-Full skips the instant-shell skeleton (app/skeleton.py) so we
        # capture the real rendered page, not the shimmer placeholder.
        resp = client.get(path, follow_redirects=True, headers={"X-HT-Full": "1"})
        name = path.strip("/").replace("/", "_") or "index"
        out = OUT_DIR / f"{name}.html"
        out.write_bytes(resp.data)
        print(f"{path} -> {resp.status_code} {len(resp.data):,}B -> {out}")


if __name__ == "__main__":
    main()
