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

## Email taxonomy

### Transactional (always sent, no opt-out)

| Email | Trigger | Code |
|-------|---------|------|
| Password reset | `/forgot-password` | `send_password_reset_email` (`app/auth.py`) |
| Welcome + verify email | Signup | `send_welcome_verify_email` (`_send_welcome_verification` in `app/auth.py`) |
| Email verification (resend) | `/resend-verification` or the base.html banner | `send_welcome_verify_email` |
| Your data is ready | First successful broker sync (rows landed) | `send_data_ready_email` (fired from `_sync_one_connection` in `app/snaptrade.py`, dedupe `data_ready:<user_id>`) |
| Broker connection dropped | A sync flips `connection_broken_at` NULL→set | `send_connection_dropped_email` (fired from `app/snaptrade_sync_cli.py`) |

The connection-dropped email is the practical version of "your token is about
to expire, please renew." See the note below on why it's reactive.

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

## Why broker expiry is reactive, not a countdown

SnapTrade connections don't expose a uniform "expires in N days" we can store —
a connection is detected as broken **at sync time** when the broker returns a
401/403 (`mark_snaptrade_connection_broken` sets `connection_broken_at`). So we
notify reactively ("your connection dropped, reconnect") rather than on a
pre-expiry countdown.

**Stretch (not yet built):** investigate SnapTrade's `brokerage_authorization`
status/expiry fields for a true proactive pre-expiry warning. This would let us
email "your <broker> authorization renews in N days" before the sync fails.
It likely only works for the subset of brokers that expose an expiry, so it
would supplement — not replace — the reactive notice.
