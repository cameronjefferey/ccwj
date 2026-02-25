# Data Isolation Review — Authentication & User Scoping

**Principle:** Every query must scope by `user_id = current_user_id`. No exceptions.

This is the #1 thing that sinks small SaaS. Cross-tenant data leaks are catastrophic.

---

## Integration Tests

Run the data isolation test suite:

```bash
pytest tests/test_data_isolation.py -v
```

Tests verify:

- **Journal:** User B cannot view, edit, or delete User A's journal entries
- **Insights:** User B cannot access User A's cached AI insight
- **Mirror Score:** User B cannot see User A's cached mirror score

Adding similar tests for new data types is mandatory.

---

## Manual Review Checklist

Use this checklist when adding or modifying data access code. Review quarterly.

### SQLite (app/models.py)

| Table | Query Type | Scoping | Status |
|-------|------------|---------|--------|
| users | get_by_id, get_by_username | By id/username (auth) | ✅ |
| user_accounts | get_accounts_for_user | WHERE user_id = ? | ✅ |
| user_accounts | add_account_for_user | INSERT with user_id | ✅ |
| user_accounts | remove_account_for_user | WHERE user_id = ? AND account_name = ? | ✅ |
| uploads | get_uploads_for_user | WHERE user_id = ? | ✅ |
| uploads | record_upload | INSERT with user_id | ✅ |
| insights | get_insight_for_user | WHERE user_id = ? | ✅ |
| insights | save_insight | DELETE/INSERT with user_id | ✅ |
| weekly_mirror_scores | get_mirror_score_for_user | WHERE user_id = ? | ✅ |
| weekly_mirror_scores | save_mirror_score | INSERT with user_id | ✅ |
| journal_entries | create_journal_entry | INSERT with user_id | ✅ |
| journal_entries | get_journal_entry | WHERE id = ? AND user_id = ? | ✅ |
| journal_entries | update_journal_entry | WHERE id = ? AND user_id = ? | ✅ |
| journal_entries | delete_journal_entry | WHERE id = ? AND user_id = ? | ✅ |
| journal_entries | list_journal_entries | WHERE e.user_id = ? | ✅ |
| journal_tags | (via journal_entry_id) | journal_entry owned by user | ✅ |
| schwab_connections | get/update/remove | WHERE user_id = ? | ✅ |

### BigQuery (account-based isolation)

User data in BigQuery is partitioned by `account`. Users link accounts via `user_accounts`. Flow:

1. `user_accounts = get_accounts_for_user(current_user.id)` — never from request
2. `_account_sql_and(user_accounts)` or `_filter_df_by_accounts(df, user_accounts)` — apply filter

| Module | Data Source | Scoping | Status |
|--------|-------------|---------|--------|
| routes | positions_summary, trades, etc. | _user_account_list() → account filter | ✅ |
| mirror_score | BQ trades, strategies | get_accounts_for_user + _account_sql_and | ✅ |
| benchmark | BQ positions | get_accounts_for_user + _filter_df_by_accounts | ✅ |
| weekly_review | BQ trades, journal | _user_account_list() + list_journal_entries(current_user.id) | ✅ |
| taxes | BQ trades, dividends | _user_account_list() + filter | ✅ |
| insights | BQ positions | get_accounts_for_user when not admin | ✅ |

### Routes — caller must pass current_user.id

| Route | Data | Passes current_user.id? | Status |
|-------|------|-------------------------|--------|
| /journal | list_journal_entries | ✅ | ✅ |
| /journal/new | create_journal_entry | ✅ | ✅ |
| /journal/<id> | get_journal_entry, update_journal_entry | ✅ | ✅ |
| /journal/<id>/delete | delete_journal_entry | ✅ | ✅ |
| /journal/export | list_journal_entries | ✅ | ✅ |
| /insights | get_insight_for_user, save_insight | ✅ | ✅ |
| /mirror-score | get_mirror_score_for_user, compute_mirror_score | ✅ | ✅ |
| /benchmark | get_accounts_for_user | ✅ | ✅ |
| /upload | add/remove_account, record_upload | ✅ | ✅ |
| /settings | get_accounts_for_user, etc. | ✅ | ✅ |
| /schwab/* | get_schwab_connection, add_account | ✅ | ✅ |

### Admin Bypass

When `is_admin(current_user.username)` is true (username in `ADMIN_USERS` env):

- `_user_account_list()` returns `None` → no account filter
- Admin sees all data across all users

**Risk:** If `ADMIN_USERS` is misconfigured or an admin account is compromised, full data access. Use sparingly; consider removing for production or restricting to specific support workflows.

---

## Scripts & Batch Jobs

| Script | Behavior | Acceptable? |
|--------|----------|-------------|
| scripts/compute_mirror_scores.py | Iterates all users, computes per user | ✅ Batch job, no request context |
| app/schwab_sync_cli.py | Iterates all schwab_connections | ✅ CLI for cron, syncs each user's own data |

---

## New Code Checklist

When adding a new table, route, or query:

1. [ ] Does the table have a `user_id` column (or equivalent tenant key)?
2. [ ] Does every SELECT/UPDATE/DELETE include `WHERE user_id = ?` with current_user.id?
3. [ ] Is `user_id` taken from `current_user.id` (never from request params)?
4. [ ] For BigQuery: is account filter derived from `get_accounts_for_user(current_user.id)`?
5. [ ] Add an integration test that User B cannot access User A's data.

---

## Last Review

- **Date:** 2025-02-13
- **Reviewer:** (fill in)
- **Next:** Quarterly or after major feature work
