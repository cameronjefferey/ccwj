# Local test environment

How to test changes locally against **real production data** before pushing to
prod — including logging in as your own prod-scoped self.

## TL;DR — one command

```bash
./scripts/dev.sh            # start the app at http://localhost:5000 (hot-reload)
./scripts/dev.sh --refresh  # also rebuild analytics_dev from prod first
./scripts/dev.sh --link     # also mirror your prod tenants into local Postgres
```

`scripts/dev.sh` is the single entry point. The daily loop is just
`./scripts/dev.sh` — edits to templates/routes/Python hot-reload instantly,
so you see them locally exactly as they'll look in prod. Add `--refresh` only
when you change **dbt models** or want fresher prod data; `--link` is a
one-time step to "become" your prod self.

First-time setup (once):

```bash
# In .env, set your read-only prod connection + prod username:
#   PROD_DATABASE_URL=postgresql://USER:PASS@HOST:PORT/DBNAME
#   DEV_PROD_USERNAME=<your-prod-username>
./scripts/dev.sh --refresh --link    # mirror data + tenants, then start
flask reset-password --username <your-prod-username>   # set a known local pw
```

Then every day after: just `./scripts/dev.sh`.

`dev.sh` refuses to start unless `BQ_DATASET=analytics_dev`, so you can never
accidentally run the local app against the prod warehouse.

## Under the hood

`dev.sh` orchestrates the pieces below; you can also run them directly.

## Why this is set up the way it is

Local dev is **environment-separated** from production so dev writes can never
corrupt prod (numeric `user_id`s collide across the two Postgres databases — an
admin purge by `user_id` once deleted the other environment's rows). Two knobs
in `.env` enforce the split:

| Knob | Local value | Effect |
| --- | --- | --- |
| `BQ_DATASET` | `analytics_dev` | Every app query's hardcoded `ccwj-dbt.analytics.` ref is rewritten to `analytics_dev` at the `get_bigquery_client()` chokepoint. Local reads never touch prod's warehouse. |
| `GITHUB_BRANCH` | `dev-seeds` | Seed CSV reads/writes go to the `dev-seeds` branch. CI builds prod from master/main only, so local syncs never rebuild prod. |

Production leaves **both unset**.

## The data is already mirrored — `scripts/dev-refresh.sh`

`analytics_dev` is a **full mirror** for testing: it builds from
`origin/master` seeds (every prod tenant, including yours) **merged** with your
local environment's own syncs from `origin/dev-seeds`
(`scripts/merge_dev_seeds.py`; local tenants win), using **your working tree's
dbt code**. So after one run, your prod trade history — and every other prod
user's — is sitting in `analytics_dev`, and your model changes are testable
against it before they ship.

`dev-refresh.sh` targets `analytics_dev` only. Plain `dbt build` from `dbt/`
and `refresh.sh` target **prod** `analytics` — use those only for intentional
prod builds.

## Seeing a scoped single-user view: the tenant mapping

The warehouse holds everyone's data, but the Flask UI scopes a logged-in user
to the rows whose **`tenant_id`** is in *their* local Postgres `broker_tenants`
table (`get_tenant_ids_for_user`). Local Postgres is a different database from
prod, so prod users (and their tenant rows) don't exist locally. That's the one
gap, and you have two ways to close it:

### Option A — Admin all-data view (zero setup)

Log in locally as an admin (`ADMIN_USERS=cameron3,happycameron`). Admins
**bypass** tenant scoping (`_user_tenant_list` returns `None`), so you instantly
see *all* of `analytics_dev`, including your own data.

- **Good for:** "does my change render against real data."
- **Not good for:** testing per-user scoping, empty states, or the exact
  single-user experience — you see the union of everyone.

### Option B — Be your own prod-scoped self (recommended)

Mirror your prod `broker_tenants` rows into local Postgres under a local user,
preserving `tenant_id` verbatim (the env-stable warehouse join key):

```bash
PROD_DATABASE_URL=postgresql://USER:PASS@HOST:PORT/DB \
  python scripts/dev-link-prod-tenants.py \
    --prod-username <your-prod-username> \
    --create-local-user        # omit if the local user already exists
```

Then `flask reset-password --username <you>` to set a known local password and
log in. You now see **exactly your prod-scoped slice**, behaving like prod.

The script:
- opens prod **read-only** and never writes to it;
- copies only `tenant_id` + display columns, **never** the numeric `user_id`
  (remapped to your local user's id — ids aren't env-stable);
- keeps `user_accounts` labels in sync for legacy surfaces;
- supports `--dry-run`, `--include-inactive`, and `--local-username`.

**Password-free alternative (`--rows-file`).** The prod DB password is **not**
retrievable through the Render MCP (it exposes read-only query access, not the
credential). If you can't set `PROD_DATABASE_URL`, export the tenant rows
another way (e.g. via the Render MCP `query_render_postgres` against
`broker_tenants`) into a JSON array and load them without a prod connection:

```bash
python scripts/dev-link-prod-tenants.py \
  --rows-file /tmp/tenants.json --local-username <you> --create-local-user
```

After linking, the warehouse rows for those tenant_ids are typically already in
`analytics_dev` (they come from prod's `origin/master` seeds). Run
`./scripts/dev-refresh.sh` if you want to (re)build the mirror. Note:
`scripts/merge_dev_seeds.py` keeps a mirrored prod tenant's master data on
refresh — it only replaces master rows for local tenants that have a fresher
copy on the `dev-seeds` branch — so refreshing won't blank your mirrored view.

## Impersonation

Once a prod-mirrored user exists locally (Option B), admins can switch into it
via the `/admin/users` **Impersonate** button (or `/admin/impersonate/<username>`).
Impersonation operates on **local Postgres users only** — it can't reach a prod
user that doesn't exist in your local DB, which is exactly what Option B
creates.

## Gotcha: don't re-link via SnapTrade to get prod data

`SNAPTRADE_USER_NAMESPACE=local` namespaces local SnapTrade userIds, so
re-linking an account through the local Connect Portal mints **new**
`tenant_id`s (different broker UUID). Those won't match the prod seed rows —
you'd see freshly-synced local data, not your prod history. To see prod
history, **copy the existing tenant_ids** (Option B); don't re-sync.

## Validating dbt changes before pushing

```bash
cd dbt && ../.venv/bin/dbt parse && ../.venv/bin/dbt build   # against prod analytics — careful
# or, for the dev mirror:
./scripts/dev-refresh.sh                                      # builds analytics_dev only
```

`dbt parse` is fast/offline. Dev builds MUST pass `--profiles-dir ~/.dbt
--target dev` (handled by `dev-refresh.sh`) — the repo's `dbt/profiles.yml`
otherwise wins and silently targets prod `analytics`.
