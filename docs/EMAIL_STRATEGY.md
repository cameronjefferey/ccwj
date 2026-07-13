# Email Strategy

How HappyTrader sends email, why we chose the provider we did, what each email
is for, and how a user opts out. Implementation lives in `app/email.py`
(dispatch + templates), `app/snaptrade_sync_cli.py` (reconnect notice), and
`app/email_digests_cli.py` (lifecycle digests).

## Provider: Resend

We send both **transactional** mail (password reset, broker-reconnect) and
**product-marketing / lifecycle** mail (weekly summary, weekly preview,
re-engagement). [Resend](https://resend.com) is the best single-vendor fit:

- **Drops into the existing layer.** `app/email.py` has a swappable backend.
  Resend exposes SMTP, so password reset can ship with **zero code** by setting
  `EMAIL_BACKEND=smtp` + Resend's SMTP creds. The `resend` HTTP backend adds
  HTML + `List-Unsubscribe` support for the richer lifecycle sends.
- **Developer-first + lifecycle in one tool.** Clean HTTP API, HTML email, and
  Broadcasts/Audiences if we later want no-code campaigns — no second product.
- **Cost.** Generous free tier for closed beta; cheap paid tiers.

Tradeoffs we considered: **Postmark** has the best transactional deliverability
but weak campaign tooling; **Loops** is great for no-code lifecycle but more
marketing-team-oriented and less proven for transactional. The `app/email.py`
abstraction means we can split later (e.g. Postmark transactional + Loops
lifecycle) with only config changes.

## Backends (`EMAIL_BACKEND`)

| Value | Use |
|-------|-----|
| `log` (default) | Dev / closed beta — writes the email to the app logger (grep `EMAIL_OUTBOX`). No creds. |
| `smtp` | Any SMTP provider (incl. Resend SMTP). Set `EMAIL_SMTP_*` + `EMAIL_FROM`. |
| `resend` | Resend HTTP API. Set `RESEND_API_KEY` + `EMAIL_FROM`. |

`APP_BASE_URL` (default `https://happytrader.me`) builds links for mail sent
outside a request context (the digest crons and the sync CLI). See
`.env.example` for the full var list.

**Production uses `smtp`, not `resend` (June 2026).** Render's outbound traffic
to the Cloudflare-fronted `api.resend.com` is refused with `HTTP 403 Forbidden`
(the identical request — same key, domain, and User-Agent — succeeds from a
laptop, so it's the egress path, not the credentials). The Resend SMTP endpoint
(`smtp.resend.com:587`) uses the same API key as the password and is not behind
that WAF, so the `smtp` backend is the working transactional path on Render. The
prod `ccwj` web service is configured with `EMAIL_BACKEND=smtp` +
`EMAIL_SMTP_HOST=smtp.resend.com` + `EMAIL_SMTP_USER=resend` +
`EMAIL_SMTP_PASSWORD=<resend api key>`. Don't switch prod back to the `resend`
HTTP backend without re-confirming Render egress is no longer 403'd.

## Email taxonomy

### Transactional (always sent, no opt-out)

| Email | Trigger | Code |
|-------|---------|------|
| Password reset | `/forgot-password` | `send_password_reset_email` (`app/auth.py`) |
| Welcome + verify email | Signup | `send_welcome_verify_email` (`_send_welcome_verification` in `app/auth.py`) |
| Email verification (resend) | `/resend-verification` or the base.html banner | `send_welcome_verify_email` |
| Your data is ready | First successful broker sync (rows landed) | `send_data_ready_email` (fired from `_sync_one_connection` in `app/snaptrade.py`, dedupe `data_ready:<user_id>`) |
| Broker connection dropped | A sync flips `connection_broken_at` NULL→set (week 0) | `send_connection_dropped_email` (fired from `app/snaptrade_sync_cli.py`, dedupe `connection_dropped:<account>:<broken_at_iso>`) |
| Still disconnected (recurring) | Connection still broken ≥7 days, then weekly | `send_connection_reminder_email` (fired from `run_connection_reminder` in `app/email_digests_cli.py`, dedupe `connection_reminder:<account>:<broken_at_iso>:w<week_index>`) |

The connection-dropped email is the practical version of "your token is about
to expire, please renew" — fired once the moment a sync detects the break.
The **still-disconnected reminder** is the recurring follow-up: a daily cron
(`connection_reminder`) re-nudges anyone who hasn't reconnected, at most once
per 7-day band (week 0 is owned by the one-time dropped email). Both carry a
day count so the cost of inaction is concrete, and both are mirrored by the
in-app banner (`_inject_snaptrade_reauth_needed` → `_connection_attention`,
which surfaces "stopped syncing X days ago" / "expires in X days").

**Email verification.** Signup mints a single-use token
(`mint_email_verification_token`, `email_verification_tokens` table, 7-day TTL)
and sends the welcome+verify email. `/verify-email/<token>` consumes it and
stamps `users.email_verified_at`. Unverified signed-in users see a
"confirm your email" banner (base.html, `email_unverified` from
`email_needs_verification`) with a resend button. Verification is **not**
required to use the app — it protects deliverability and account recovery.

### Lifecycle / product-marketing (opt-out)

| Email | Cadence | Opt-in column | Code |
|-------|---------|---------------|------|
| Weekly summary | Sat ~8am PDT | `user_profiles.digest_email` | `run_weekly_summary` (`app/email_digests_cli.py`) |
| Weekly preview | Sun ~4pm PDT | `user_profiles.weekly_preview_email` | `run_weekly_preview` |
| Re-engagement | Daily ~9am PDT, windowed (14–45d dormant) | `user_profiles.product_update_email` | `run_reengagement` |

Opt-in flags are edited at **Profile → Preferences**. `product_update_email`
defaults **on** (lifecycle nudge); the two digests default **off** (explicit
opt-in). Content sources: weekly summary reads `mart_weekly_summary`; weekly
preview reads `int_enriched_current` + `stg_earnings_calendar` + the ex-div
cadence heuristic (mirrors `weekly_review.py`).

### Maintenance: the digests are a hidden schema consumer

The digest crons run **outside dbt** and query marts / intermediate models with
**inlined SQL** (the `_*_SQL` constants in `app/email_digests_cli.py`). A dbt
column rename, mart re-grain, or data-definition change silently breaks a digest:
the sub-query raises `400 Unrecognized name: <col>`, `_build_weekly_preview`
**catches it and the section just disappears from the email** — no failing test,
nothing in the UI. So **any schema/data change must update the email SQL
(`app/email_digests_cli.py`) and the matching template (`send_*_email` /
`_wrap_html` in `app/email.py`) in the same change.** Grep `email_digests_cli.py`
for the model/column before declaring a schema change done. See AGENTS.md
"Email digests read the warehouse directly — keep SQL + templates in sync."

Known columns the digests depend on today (non-exhaustive): weekly summary —
`mart_weekly_summary`(`account, week_start, total_return, total_pnl,
dividends_amount, trades_closed, num_winners, num_losers, best_symbol, best_pnl,
worst_symbol, worst_pnl, user_id, tenant_id`); weekly preview —
`int_enriched_current`(`underlying_symbol, instrument_type, option_strike,
option_expiry, quantity, user_id, tenant_id`), `stg_earnings_calendar`(`symbol,
next_earnings_date`), `stg_daily_prices`(`symbol, date, dividend`).

Regression note (Jul 2026): the preview expirations query selected `strike`, but
`int_enriched_current` exposes it as `option_strike` — the whole "Options
expiring" section vanished from every preview with no error surfaced. Fixed via
`option_strike AS strike`. To preview a template fully populated without sending,
monkeypatch `app.email.send_email` to capture `html_body`, call the `send_*_email`
helper with sample data, and rasterize the HTML (e.g. headless Chrome
`--screenshot`).

## Opt-out / unsubscribe

- Every lifecycle email carries a `List-Unsubscribe` header (+ RFC 8058
  `List-Unsubscribe-Post: List-Unsubscribe=One-Click`) and an in-body footer
  link to `/email/unsubscribe/<token>`.
- The route (`email_unsubscribe` in `app/auth.py`, CSRF-exempt for the
  provider's one-click POST) flips **all** lifecycle opt-ins to false via
  `unsubscribe_user_by_token`. Transactional mail is unaffected.
- The token is a stable per-user capability minted by
  `get_or_create_email_unsubscribe_token` and stored on
  `user_profiles.email_unsubscribe_token`.

## Idempotency

Every app-initiated send records a row in the `email_sends` table keyed by
`(kind, dedupe_key)` via `INSERT ... ON CONFLICT DO NOTHING`. `record_email_send`
returns `True` only on the first insert, so a daily cron (or a retried run)
never double-sends. Dedupe keys:

- `connection_dropped`: `"<snaptrade_account_id>:<broken_at_iso>"` (re-breaks
  after a reconnect notify again).
- `connection_reminder`: `"<snaptrade_account_id>:<broken_at_iso>:w<week_index>"`
  where `week_index = stale_days // 7` — one reminder per 7-day band per break
  episode; a daily cron self-heals without double-sending.
- `weekly_summary` / `weekly_preview`: `"<user_id>:<week_start>"`.
- `reengagement`: `"<user_id>:<last_visit_date>"` (one nudge per dormancy
  episode).

## Deliverability suppression (bounces + complaints)

Resend posts delivery events to `POST /webhooks/resend` (`app/webhooks.py`,
CSRF-exempt, Svix signature verified with `RESEND_WEBHOOK_SECRET`). On
`email.bounced` (hard) we add the recipient to the `email_suppressions` table
with reason `hard_bounce`; on `email.complained` we add `complaint`.

`send_email` checks the list before every send (`_is_suppressed`):

- `hard_bounce` / `invalid` / `manual` → block **all** mail (undeliverable or
  the person asked to stop).
- `complaint` → block **lifecycle** mail only; critical transactional (password
  reset) still goes out.

The check fails **open** (sends) if the lookup errors, so a DB hiccup never
silently drops a password reset. Operators can manage entries with
`add_email_suppression` / `remove_email_suppression`. Configure the webhook in
Resend → Webhooks pointing at `<APP_BASE_URL>/webhooks/resend` and paste its
signing secret into `RESEND_WEBHOOK_SECRET`.

## DNS setup (prerequisite for real sending)

Before flipping off `log`, verify a sending domain in Resend and add the DNS
records it generates (typically a subdomain like `mail.happytrader.me`):

1. **SPF** — TXT authorizing Resend to send for the domain.
2. **DKIM** — CNAME/TXT records Resend provides for message signing.
3. **DMARC** — a `_dmarc` TXT policy (start `p=none` to monitor, then tighten).

Set `EMAIL_FROM` to an address on the verified domain (e.g.
`HappyTrader <noreply@mail.happytrader.me>`). Until DNS verifies, deliverability
will be poor and some providers will reject the mail outright.

## Why the "X days" countdown is staleness-based, not a true expiry

SnapTrade exposes **no** uniform "expires in N days" field — the brokerage
authorization object carries only `created_date` / `updated_date` /
`disabled` / `disabled_date` (verified against `snaptrade_client` 11.x), and
`meta` is broker-specific + deprecated. So a literal forward countdown is only
honest for a broker whose re-auth cadence we've **operator-verified**.

Two layers, both showing a real day count:

1. **Staleness (default, always on).** A connection is detected as broken **at
   sync time** — either an auth-shaped error or the authoritative
   `brokerage_authorization.disabled` flag (see
   `broker-sync-safety` SKILL.md 2026-06-19) — which sets
   `connection_broken_at`. The banner then shows "stopped syncing X days ago"
   and the `connection_reminder` cron emails weekly. `X` = days since the
   break, which is accurate and never a false alarm.

2. **Heuristic forward countdown (opt-in per broker).**
   `SNAPTRADE_BROKER_CONNECTION_LIFETIME_DAYS` in `app/snaptrade.py` maps a
   `broker_slug` → token lifetime (days). When populated, a not-yet-broken
   connection within `SNAPTRADE_CONNECTION_WARN_WINDOW_DAYS` of
   `created_at + lifetime` shows "expires in X days." **The map is empty by
   default** — never guess a lifetime, because a wrong "expires in 3 days!"
   that never comes true burns trust. Fill it in only with verified numbers.

`_connection_attention` in `app/snaptrade.py` is the single classifier behind
both the in-app banner and the reminder email; broken ("stale") always wins
over a heuristic countdown.
