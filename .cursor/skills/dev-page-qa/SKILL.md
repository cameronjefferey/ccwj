---
name: dev-page-qa
description: >-
  Render any HappyTrader page as a logged-in dev user with real warehouse data
  and screenshot it with headless Chrome. Use when verifying UI changes,
  doing visual QA on templates, checking that a page shows expected data
  (dollar amounts, sections, cards), or QA-ing mobile layouts. Applies to any
  change under app/templates/ or a page module in app/.
---

# Dev Page QA — render with real data, then screenshot

Never call template work done from code reading alone. Render the page with
real dev-warehouse data and grep/screenshot the output. (Per
`data-pipeline-fixes.mdc`: if you can't grep the expected number out of the
rendered HTML, you are not done.)

## 1. Render a page

```bash
.venv/bin/python scripts/dev_render_pages.py --user USERNAME /daily-review /positions
# -> /tmp/ht_pages/daily-review.html, /tmp/ht_pages/positions.html (logged-in, full render)
```

- Omit `--user` for the default dev user. Choose a user whose data exercises
  the feature (see step 2).
- Reads the dev warehouse (`analytics_dev` via `.env`). If a table is missing
  there, build it first — see the `warehouse-validate` skill.
- First grep the HTML for the expected content before screenshotting:

```bash
rg -n "Execution Review|\\$721" /tmp/ht_pages/daily-review.html
```

## 2. Pick a user with the right data

Warehouse rows are stamped with PROD user ids; local Postgres has its own ids.
Map through `broker_tenants` (tenant_id is stable across environments):

```bash
.venv/bin/python - <<'EOF'
from app import app
from app.db import fetch_all
with app.app_context():
    for r in fetch_all("""SELECT bt.user_id, u.username, bt.tenant_id, bt.account_name
                          FROM broker_tenants bt LEFT JOIN users u ON u.id = bt.user_id"""):
        print(r)
EOF
```

Find which tenant has the data in BigQuery (bigquery MCP or a query), then
render as the local username that owns that tenant.

## 3. Screenshot

```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless \
  --disable-gpu --hide-scrollbars \
  --screenshot=/tmp/ht_pages/page.png --window-size=1280,2400 \
  "file:///tmp/ht_pages/daily-review.html"
```

- Read the PNG back to actually look at it; embed it in your reply.
- Long pages: increase window height, or isolate one section into a wrapper
  HTML file (copy the page `<head>` + the section div) and screenshot that.

## 4. Mobile caveat (real phone width)

Headless Chrome enforces a ~500px minimum viewport — `--window-size=390,...`
silently renders a 500px layout cropped to 390. To QA true phone width, wrap
the page in a 390px iframe and screenshot the wrapper:

```html
<body style="margin:0"><iframe src="daily-review.html"
  style="width:390px;height:2400px;border:0"></iframe></body>
```

## Checklist before declaring UI work done

- [ ] Rendered the affected page(s) with real data (not just tests)
- [ ] Grep'd rendered HTML for the expected values/sections
- [ ] Screenshot reviewed at desktop width; mobile iframe if layout changed
- [ ] If the page has modes/branches (open vs closed, admin vs user, empty
      state), rendered the branch you changed
