"""
Application data model. Backed by Postgres (see ``app.db``).

Schema is created on app startup via ``init_db()``. All queries go through
``app.db.{fetch_all,fetch_one,execute,execute_returning}`` which use a
shared connection pool.
"""
import hashlib
import json
import logging
import os
import secrets

from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

from app.db import execute, execute_returning, fetch_all, fetch_one, get_conn

_log = logging.getLogger(__name__)


def trade_fingerprint(user_id, account, symbol, trade_symbol, open_date, close_date, strategy):
    """
    Stable id for a logical trade row in Weekly Review (matches mart grain).
    Used for community publish / unpublish without exposing raw brokerage ids.
    """
    parts = [
        str(user_id),
        str(account or ""),
        str(symbol or ""),
        str(trade_symbol or ""),
        str(open_date or ""),
        str(close_date or ""),
        str(strategy or ""),
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def init_db():
    """Create tables if they don't exist. Safe to call on every startup."""
    statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id            SERIAL PRIMARY KEY,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            email         TEXT
        )
        """,
        # NB: the unique index on lower(email) is created inside
        # _migrate_users_email_column() *after* the ALTER TABLE that
        # backfills the column on legacy schemas. CREATE TABLE IF NOT
        # EXISTS does not add new columns to a pre-existing table, so
        # putting the index in this list crashed startup on the prod DB
        # (UndefinedColumn: column "email" does not exist).
        """
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            token_hash  TEXT NOT NULL UNIQUE,
            expires_at  TIMESTAMPTZ NOT NULL,
            used_at     TIMESTAMPTZ,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            requester_ip TEXT
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_password_reset_user_active
        ON password_reset_tokens (user_id) WHERE used_at IS NULL
        """,
        # Email-verification tokens (same single-use shape as password
        # reset). Confirms a signup address is real before we rely on it
        # for deliverability / recovery.
        """
        CREATE TABLE IF NOT EXISTS email_verification_tokens (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            token_hash  TEXT NOT NULL UNIQUE,
            expires_at  TIMESTAMPTZ NOT NULL,
            used_at     TIMESTAMPTZ,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_email_verify_user_active
        ON email_verification_tokens (user_id) WHERE used_at IS NULL
        """,
        """
        CREATE TABLE IF NOT EXISTS user_accounts (
            user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            account_name TEXT NOT NULL,
            PRIMARY KEY (user_id, account_name)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS uploads (
            id            SERIAL PRIMARY KEY,
            user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            account_name  TEXT NOT NULL,
            history_rows  INTEGER NOT NULL DEFAULT 0,
            current_rows  INTEGER NOT NULL DEFAULT 0,
            uploaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS insights (
            id            SERIAL PRIMARY KEY,
            user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            summary       TEXT NOT NULL,
            full_analysis TEXT NOT NULL,
            generated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS strategy_fit_insights (
            id             SERIAL PRIMARY KEY,
            user_id        INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            account_filter TEXT NOT NULL DEFAULT '',
            summary        TEXT NOT NULL,
            full_analysis  TEXT NOT NULL,
            brief_text     TEXT,
            generated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS pro_waitlist (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER REFERENCES users(id) ON DELETE SET NULL,
            email       TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # LEGACY (v1 direct Schwab): kept for idempotent init_db until Phase 6
        # cutover drops the table — see scripts/admin/v2_cutover_reset.py.
        """
        CREATE TABLE IF NOT EXISTS schwab_connections (
            id                            SERIAL PRIMARY KEY,
            user_id                       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            account_hash                  TEXT NOT NULL,
            account_number                TEXT NOT NULL,
            account_name                  TEXT,
            token_json                    TEXT NOT NULL,
            created_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            schwab_first_sync_completed   BOOLEAN NOT NULL DEFAULT FALSE,
            UNIQUE (user_id, account_number)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS weekly_mirror_scores (
            id                    SERIAL PRIMARY KEY,
            user_id               INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            week_start_date       DATE NOT NULL,
            discipline_score      REAL NOT NULL,
            intent_score          REAL NOT NULL,
            risk_alignment_score  REAL NOT NULL,
            consistency_score     REAL NOT NULL,
            mirror_score          REAL NOT NULL,
            confidence_level      TEXT NOT NULL,
            diagnostic_sentence   TEXT,
            generated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (user_id, week_start_date)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id                         INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            display_name                    TEXT,
            headline                        TEXT,
            bio                             TEXT,
            accent                          TEXT NOT NULL DEFAULT 'violet',
            timezone                        TEXT NOT NULL DEFAULT 'America/New_York',
            week_starts_monday              BOOLEAN NOT NULL DEFAULT TRUE,
            default_route                   TEXT NOT NULL DEFAULT 'weekly_review',
            digest_email                    BOOLEAN NOT NULL DEFAULT FALSE,
            weekly_preview_email            BOOLEAN NOT NULL DEFAULT FALSE,
            product_update_email            BOOLEAN NOT NULL DEFAULT TRUE,
            email_unsubscribe_token         TEXT,
            compact_tables                  BOOLEAN NOT NULL DEFAULT FALSE,
            show_account_names_on_published BOOLEAN NOT NULL DEFAULT FALSE,
            profile_visibility              TEXT NOT NULL DEFAULT 'private',
            created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_follows (
            follower_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            following_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (follower_id, following_id),
            CHECK (follower_id <> following_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS community_published_trades (
            id                SERIAL PRIMARY KEY,
            user_id           INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            trade_fingerprint TEXT NOT NULL,
            account_name      TEXT NOT NULL,
            symbol            TEXT NOT NULL,
            strategy          TEXT NOT NULL,
            trade_symbol      TEXT NOT NULL DEFAULT '',
            open_date         TEXT NOT NULL,
            close_date        TEXT NOT NULL DEFAULT '',
            status            TEXT NOT NULL,
            display_pnl       REAL,
            caption           TEXT,
            published_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (user_id, trade_fingerprint)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS community_posts (
            id                  SERIAL PRIMARY KEY,
            user_id             INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            body                TEXT NOT NULL,
            symbol              TEXT,
            strategy            TEXT,
            attached_fingerprint TEXT,
            attachment_kind     TEXT,
            attachment_json     TEXT,
            visibility          TEXT NOT NULL DEFAULT 'followers',
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_community_posts_author_created
        ON community_posts (user_id, created_at DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_community_posts_visibility_created
        ON community_posts (visibility, created_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS user_review_visits (
            user_id        INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            last_visit_at  TIMESTAMPTZ NOT NULL,
            prev_visit_at  TIMESTAMPTZ
        )
        """,
        # Idempotency log for outbound email. Every send the app initiates
        # (connection-dropped notice, weekly digest, re-engagement) records a
        # row keyed by (kind, dedupe_key). The UNIQUE constraint + INSERT ...
        # ON CONFLICT DO NOTHING is the "send exactly once" guard so a cron
        # that runs daily doesn't re-notify on every pass.
        """
        CREATE TABLE IF NOT EXISTS email_sends (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
            kind        TEXT NOT NULL,
            dedupe_key  TEXT NOT NULL,
            to_email    TEXT,
            sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (kind, dedupe_key)
        )
        """,
        # Deliverability suppression list. A row here means "do not email
        # this address" — populated by the Resend webhook (hard bounces,
        # spam complaints) and optionally by hand. ``reason`` drives how
        # strictly we suppress: hard_bounce/invalid/manual block ALL mail
        # (the address is undeliverable or a person asked to stop);
        # complaint blocks only lifecycle/marketing mail (we may still send
        # critical transactional like a password reset). Email is stored
        # normalized (lower/trim) and is the primary key.
        """
        CREATE TABLE IF NOT EXISTS email_suppressions (
            email       TEXT PRIMARY KEY,
            reason      TEXT NOT NULL,
            detail      TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # Login attempt log — feeds the per-username lockout. We keep a
        # row per failed attempt rather than a single counter so we can
        # (a) auto-clear after the cooldown without scheduled work
        # (b) audit what happened during a brute-force probe.
        # Successful logins prune the attacker's prior failures.
        """
        CREATE TABLE IF NOT EXISTS login_attempts (
            id            SERIAL PRIMARY KEY,
            username_lc   TEXT NOT NULL,
            success       BOOLEAN NOT NULL,
            ip_address    TEXT,
            user_agent    TEXT,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_login_attempts_username_recent
        ON login_attempts (username_lc, created_at DESC)
        """,
        # Feedback inbox — anything testers want to flag (broken numbers,
        # ugly mobile, "this is confusing"). Admin reads the table from
        # /admin/feedback and acts on it. ON DELETE SET NULL so removing
        # a user does not erase their bug reports.
        """
        CREATE TABLE IF NOT EXISTS feedback (
            id           SERIAL PRIMARY KEY,
            user_id      INTEGER REFERENCES users(id) ON DELETE SET NULL,
            username     TEXT,
            body         TEXT NOT NULL,
            page_path    TEXT,
            user_agent   TEXT,
            ip_address   TEXT,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at  TIMESTAMPTZ
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_feedback_unresolved_created
        ON feedback (created_at DESC) WHERE resolved_at IS NULL
        """,
        # SnapTrade aggregator: one row per HappyTrader user. Stores the
        # SnapTrade-issued userId/userSecret pair we need to call every
        # SnapTrade endpoint on the user's behalf. Mirrors the per-grant
        # nature of OAuth — one SnapTrade user can carry many linked
        # broker accounts (Fidelity + Vanguard + Robinhood + ...).
        # See plan: SnapTrade multi-broker integration.
        """
        CREATE TABLE IF NOT EXISTS snaptrade_connections (
            user_id            INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            snaptrade_user_id  TEXT NOT NULL UNIQUE,
            snaptrade_secret   TEXT NOT NULL,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # SnapTrade per-account rows. Mirrors schwab_connections grain
        # (one row per linked broker account). ``account_name`` is the
        # warehouse-facing tenant label that flows into seed.Account —
        # follow ``docs/USER_ID_TENANCY.md`` rules: never rename it
        # after first sync. ``broker_slug`` is SnapTrade's canonical
        # broker identifier ('SCHWAB', 'FIDELITY', 'VANGUARD', ...) so
        # the UI can show broker logos / branding without an extra
        # API round-trip. ``connection_broken_at`` mirrors
        # ``refresh_token_invalid_at`` from schwab_connections.
        """
        CREATE TABLE IF NOT EXISTS snaptrade_accounts (
            id                          SERIAL PRIMARY KEY,
            user_id                     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            snaptrade_account_id        TEXT NOT NULL,
            broker_slug                 TEXT NOT NULL,
            account_number_masked       TEXT,
            account_name                TEXT NOT NULL,
            display_nickname            TEXT,
            first_sync_completed        BOOLEAN NOT NULL DEFAULT FALSE,
            last_sync_at                TIMESTAMPTZ,
            holdings_last_successful_sync TIMESTAMPTZ,
            last_sync_error             TEXT,
            connection_broken_at        TIMESTAMPTZ,
            created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (user_id, snaptrade_account_id)
        )
        """,
        # Broker account tenancy table — see docs/BROKER_ACCOUNT_ID_MIGRATION.md.
        #
        # One row per (user, physical broker account). The SERIAL ``id``
        # is the stable, immutable tenant key that gets stamped into
        # every BigQuery seed row. After the staged migration completes,
        # the warehouse joins on this id, not on the user-typeable
        # ``account_name`` string.
        #
        # ``broker_slug`` ∈ {'schwab', 'snaptrade', 'manual', 'demo'}.
        # ``broker_external_id`` is the broker-provided immutable handle:
        #   - schwab    → schwab_connections.account_hash
        #   - snaptrade → snaptrade_accounts.snaptrade_account_id
        #   - manual    → synthetic UUID generated at first upload
        #   - demo      → hard-coded constant per demo seed
        # The pair (broker_slug, broker_external_id) is globally unique
        # — two different users linking the same physical Schwab account
        # each get their own broker_accounts row (different user_id) but
        # the same broker_external_id, which is the right model because
        # their warehouse rows are different tenants.
        """
        CREATE TABLE IF NOT EXISTS broker_accounts (
            id                   SERIAL PRIMARY KEY,
            user_id              INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            broker_slug          TEXT NOT NULL,
            broker_external_id   TEXT NOT NULL,
            account_name         TEXT NOT NULL,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (user_id, broker_slug, broker_external_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_broker_accounts_user
        ON broker_accounts (user_id)
        """,
        # broker_tenants — v2 tenancy table (see docs/V2_TENANT_KEY_DESIGN.md).
        #
        # Replaces broker_accounts (Postgres SERIAL collisions on resets),
        # schwab_connections (direct Schwab is being retired), and
        # snaptrade_accounts (collapsed into this table).
        #
        # The tenant_id column is the warehouse join key:
        #     "<broker_slug>:<broker_uuid>"
        # e.g. "snaptrade:bed78305-a764-4c4d-b4c7-fe59e391f661".
        #
        # broker_uuid comes verbatim from the broker (SnapTrade
        # AccountSimple.id). It is NEVER minted, transformed, hashed,
        # or re-cased in transit. That property is what makes tenant_id
        # collision-proof across Postgres resets, dataset re-creates,
        # and user-id renumbers.
        #
        # The (broker_slug, broker_uuid) UNIQUE constraint enforces the
        # "one physical broker account → one tenant_id" invariant. The
        # (user_id, snaptrade_connection_id, broker_uuid) UNIQUE
        # enforces per-user-per-account uniqueness so two users sharing
        # a brokerage login get two distinct tenant_ids (correct: they
        # should see different filtered slices).
        """
        CREATE TABLE IF NOT EXISTS broker_tenants (
            tenant_id                 TEXT PRIMARY KEY,
            user_id                   INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            broker_slug               TEXT NOT NULL,
            broker_uuid               TEXT NOT NULL,
            account_name              TEXT NOT NULL,
            account_mask              TEXT,
            broker_label              TEXT,
            snaptrade_connection_id   TEXT,
            connection_status         TEXT NOT NULL DEFAULT 'active',
            connection_broken_at      TIMESTAMPTZ,
            first_sync_completed      BOOLEAN NOT NULL DEFAULT FALSE,
            display_nickname          TEXT,
            created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (broker_slug, broker_uuid)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_broker_tenants_user
        ON broker_tenants (user_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_broker_tenants_status
        ON broker_tenants (connection_status)
        WHERE connection_status != 'active'
        """,
        # Onboarding survey — captured during the first SnapTrade sync wait
        # on /sync/processing as a multi-section wizard. One row per user
        # (PK on user_id); resubmitting overwrites. ``answers`` is a JSONB
        # blob so the form can grow/shrink/rename questions without a
        # schema migration — the only contract is "user_id → JSON object
        # of answers". Routes validate required keys; admin views render
        # the blob. Stored for now, not yet used to personalize copy.
        """
        CREATE TABLE IF NOT EXISTS onboarding_responses (
            user_id      INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            answers      JSONB NOT NULL,
            submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        # SnapTrade sync observation log — APPEND-ONLY (mirrors the
        # login_attempts pattern). One row per (account, sync run). Unlike
        # snaptrade_accounts.holdings_last_successful_sync, which only keeps
        # the LATEST value, this keeps the full history so we can measure
        # "how many minutes after the 4pm ET close does each broker's
        # holdings_last_successful_sync actually advance" and retime the
        # sync cron precisely (see CLOSE-BASED REPORTING plan, Phase 3/4).
        #   cron_run_at                  = when THIS sync run executed (our clock)
        #   holdings_last_successful_sync = SnapTrade's authoritative "broker
        #                                   data as of" at run time
        #   last_sync_at                 = when we last read SnapTrade's cache
        #   ok                           = whether the run succeeded
        """
        CREATE TABLE IF NOT EXISTS snaptrade_sync_observations (
            id                            SERIAL PRIMARY KEY,
            user_id                       INTEGER REFERENCES users(id) ON DELETE CASCADE,
            snaptrade_account_id          TEXT,
            broker_slug                   TEXT,
            cron_run_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            holdings_last_successful_sync TIMESTAMPTZ,
            last_sync_at                  TIMESTAMPTZ,
            ok                            BOOLEAN NOT NULL DEFAULT TRUE
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_snaptrade_sync_obs_account_recent
        ON snaptrade_sync_observations (snaptrade_account_id, cron_run_at DESC)
        """,
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    _migrate_schwab_first_sync_column()
    _migrate_schwab_display_nickname_column()
    _migrate_schwab_refresh_token_invalid_at_column()
    _migrate_community_posts_strategy_column()
    _migrate_account_name_unique_index()
    _migrate_users_email_column()
    _migrate_snaptrade_force_refresh_columns()
    _migrate_snaptrade_holdings_sync_column()
    _migrate_broker_account_id_columns()
    _migrate_onboarding_responses_v2()
    _migrate_user_profiles_email_prefs()
    _migrate_users_email_verified_column()
    _migrate_users_preferred_llm_model_column()
    _backfill_broker_tenant_nicknames_from_snaptrade_accounts()


def _backfill_broker_tenant_nicknames_from_snaptrade_accounts():
    """One-shot idempotent backfill: copy ``display_nickname`` from
    ``snaptrade_accounts`` (legacy table, where the existing UI writes)
    over to ``broker_tenants`` (v2 table, where Daily Review and other
    pages now read from).

    Only fills NULL targets — a nickname already on broker_tenants is
    treated as the more authoritative value and never overwritten by
    this backfill. Joins on
    ``broker_tenants.broker_uuid = snaptrade_accounts.snaptrade_account_id``
    since SnapTrade's account UUID is the natural key for both.

    Safe to call on every startup: matches existing
    ``_migrate_*`` helpers' idempotency contract. Once the
    ``update_snaptrade_account_nickname`` dual-write path has been in
    production for one full release cycle, this backfill can be
    removed.
    """
    try:
        execute(
            "UPDATE broker_tenants bt "
            "SET display_nickname = sa.display_nickname, updated_at = NOW() "
            "FROM snaptrade_accounts sa "
            "WHERE bt.broker_slug = 'snaptrade' "
            "  AND bt.broker_uuid = sa.snaptrade_account_id "
            "  AND bt.user_id = sa.user_id "
            "  AND bt.display_nickname IS NULL "
            "  AND sa.display_nickname IS NOT NULL "
            "  AND length(trim(sa.display_nickname)) > 0"
        )
    except Exception as exc:
        _log.warning("broker_tenants nickname backfill skipped: %s", exc)


def _migrate_users_email_verified_column():
    """Idempotent: add ``email_verified_at`` to ``users`` so we can track
    which signup emails have been confirmed. NULL = unverified (legacy rows
    and brand-new signups until they click the link)."""
    try:
        execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified_at TIMESTAMPTZ")
    except Exception as exc:
        _log.warning("users.email_verified_at migration skipped: %s", exc)


def _migrate_users_preferred_llm_model_column():
    """Idempotent: add ``preferred_llm_model`` to ``users`` so each user can
    pick which AI model narrates their insights (see app/llm.py catalog).
    NULL = no explicit choice → app falls back to llm.default_model_key()."""
    try:
        execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS preferred_llm_model TEXT")
    except Exception as exc:
        _log.warning("users.preferred_llm_model migration skipped: %s", exc)


def _migrate_user_profiles_email_prefs():
    """Idempotent: add the granular email-preference columns + unsubscribe
    token to ``user_profiles`` on databases created before the email
    strategy shipped. ``CREATE TABLE IF NOT EXISTS`` does not add columns
    to a pre-existing table, so legacy rows need the explicit ALTER.

    - ``digest_email``           — weekly *summary* opt-in (existing column).
    - ``weekly_preview_email``   — weekly *preview* (look-ahead) opt-in.
    - ``product_update_email``   — product updates + re-engagement nudges.
    - ``email_unsubscribe_token``— one-click unsubscribe token (List-Unsubscribe).
    """
    for ddl in (
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS "
        "weekly_preview_email BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS "
        "product_update_email BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS "
        "email_unsubscribe_token TEXT",
    ):
        try:
            execute(ddl)
        except Exception as exc:
            _log.warning("user_profiles email-prefs migration skipped: %s", exc)


def _migrate_schwab_first_sync_column():
    """Idempotent: add schwab_first_sync_completed for per-user routine vs full-history sync UX."""
    try:
        execute(
            "ALTER TABLE schwab_connections "
            "ADD COLUMN IF NOT EXISTS schwab_first_sync_completed BOOLEAN NOT NULL DEFAULT FALSE"
        )
    except Exception as e:
        _log.warning("schwab_connections migration skipped: %s", e)


def _migrate_schwab_refresh_token_invalid_at_column():
    """Idempotent: add ``refresh_token_invalid_at`` so we can flag a
    connection whose refresh token Schwab has rejected (e.g. user
    hasn't synced in >7 days, OAuth grant revoked). Templates surface
    a "Reconnect Schwab" banner whenever this is non-NULL — without
    it, a user just sees stale data with no signal that anything is
    wrong, since cron failures are invisible to the trader.
    """
    try:
        execute(
            "ALTER TABLE schwab_connections "
            "ADD COLUMN IF NOT EXISTS refresh_token_invalid_at TIMESTAMPTZ"
        )
    except Exception as e:
        _log.warning(
            "schwab_connections refresh_token_invalid_at migration skipped: %s", e,
        )


def _migrate_broker_account_id_columns():
    """Idempotent: add nullable ``broker_account_id`` FK columns to the
    connection tables. See ``docs/BROKER_ACCOUNT_ID_MIGRATION.md``.

    Nullable on purpose for Stage 0 — existing rows survive without one
    until the operator runs the backfill (Stage 1). New rows created by
    ``schwab_callback`` / ``_register_account`` (SnapTrade) /
    ``upload_csv`` populate the column from day one.
    """
    for table in ("schwab_connections", "snaptrade_accounts", "uploads"):
        try:
            execute(
                f"ALTER TABLE {table} "
                "ADD COLUMN IF NOT EXISTS broker_account_id INTEGER "
                "REFERENCES broker_accounts(id) ON DELETE SET NULL"
            )
        except Exception as exc:
            _log.warning(
                "%s.broker_account_id migration skipped: %s", table, exc,
            )


def _migrate_onboarding_responses_v2():
    """Idempotent: upgrade ``onboarding_responses`` from the v1 two-column
    shape (``primary_reason TEXT NOT NULL, other_text TEXT``) to the v2
    shape (``answers JSONB NOT NULL``). Safe to run repeatedly:

    - On a fresh DB the v2 ``CREATE TABLE`` already shipped the new
      shape; ``DROP NOT NULL`` is a no-op (the columns don't exist) and
      ``ADD COLUMN IF NOT EXISTS`` short-circuits.
    - On a dev DB that ran v1: relax the NOT NULL on ``primary_reason``
      so the new wizard form can write rows with only ``answers``
      populated, and add the ``answers`` JSONB column.

    The v1 columns are intentionally left in place — there are unlikely
    to be any prod rows under v1 (this code shipped in the same change),
    and dropping a column on a live table is a separate decision.
    """
    try:
        execute(
            "ALTER TABLE onboarding_responses "
            "ALTER COLUMN primary_reason DROP NOT NULL"
        )
    except Exception as e:
        _log.debug("onboarding_responses.primary_reason DROP NOT NULL skipped: %s", e)
    try:
        execute(
            "ALTER TABLE onboarding_responses "
            "ADD COLUMN IF NOT EXISTS answers JSONB"
        )
    except Exception as e:
        _log.warning("onboarding_responses.answers migration skipped: %s", e)


def _migrate_snaptrade_force_refresh_columns():
    """Idempotent: add columns supporting the user-initiated
    "Refresh from broker" button on /snaptrade/accounts.

    - ``brokerage_authorization_id``: SnapTrade's per-connection
      auth UUID. Cached so the refresh button doesn't spend an extra
      ``get_user_account_details`` round-trip every press. Looked up
      lazily on first refresh and stamped here for re-use.
    - ``last_force_refresh_at``: throttle anchor. ``refresh_brokerage_
      authorization`` is BILLED PER CALL by SnapTrade — without a
      throttle a misbehaving (or impatient) user could spam it and
      run up our SnapTrade bill in a few seconds.
    """
    try:
        execute(
            "ALTER TABLE snaptrade_accounts "
            "ADD COLUMN IF NOT EXISTS brokerage_authorization_id TEXT"
        )
    except Exception as e:
        _log.warning("snaptrade_accounts brokerage_authorization_id migration skipped: %s", e)
    try:
        execute(
            "ALTER TABLE snaptrade_accounts "
            "ADD COLUMN IF NOT EXISTS last_force_refresh_at TIMESTAMPTZ"
        )
    except Exception as e:
        _log.warning("snaptrade_accounts last_force_refresh_at migration skipped: %s", e)


def _migrate_snaptrade_holdings_sync_column():
    """Idempotent: add ``holdings_last_successful_sync`` — the honest
    "broker data as of" timestamp.

    Distinct from ``last_sync_at`` (= when OUR cron last read SnapTrade's
    cache): this is SnapTrade's own ``sync_status.holdings.last_successful_sync``,
    i.e. when SnapTrade last pulled fresh holdings FROM the broker. The two
    clocks diverge exactly when a connection stalls — ``last_sync_at`` keeps
    advancing (we read the cache nightly) while the underlying broker data is
    frozen (June 2026: user_id=9 Schwab frozen 7 days while every sync
    "succeeded"). We already fetch this value for the holdings-freshness
    backstop; persisting it lets the UI publish an honest freshness badge.
    """
    try:
        execute(
            "ALTER TABLE snaptrade_accounts "
            "ADD COLUMN IF NOT EXISTS holdings_last_successful_sync TIMESTAMPTZ"
        )
    except Exception as e:
        _log.warning(
            "snaptrade_accounts holdings_last_successful_sync migration skipped: %s", e,
        )


def _migrate_schwab_display_nickname_column():
    """
    Idempotent: add display_nickname column. This is a UI-only label that lets
    a trader distinguish multiple Schwab accounts (e.g. "Roth IRA",
    "Joint brokerage") on the front end *without* changing account_name —
    account_name is the BigQuery tenancy key and renaming it would orphan
    every existing row in the warehouse.
    """
    try:
        execute(
            "ALTER TABLE schwab_connections ADD COLUMN IF NOT EXISTS display_nickname TEXT"
        )
    except Exception as e:
        _log.warning("schwab_connections display_nickname migration skipped: %s", e)


def _migrate_community_posts_strategy_column():
    """Idempotent: add strategy tag column to community_posts so traders can tag
    posts with a strategy (Covered Call, Wheel, PMCC, etc) in addition to a symbol."""
    try:
        execute("ALTER TABLE community_posts ADD COLUMN IF NOT EXISTS strategy TEXT")
    except Exception as e:
        _log.warning("community_posts strategy migration skipped: %s", e)
    try:
        execute("ALTER TABLE community_posts ADD COLUMN IF NOT EXISTS attachment_kind TEXT")
        execute("ALTER TABLE community_posts ADD COLUMN IF NOT EXISTS attachment_json TEXT")
    except Exception as e:
        _log.warning("community_posts attachment migration skipped: %s", e)


def _migrate_users_email_column():
    """Idempotent: add users.email column on databases created before
    self-serve password recovery existed, then create the partial unique
    index on lower(email).

    Order matters: the index references the email column, so on a legacy
    DB where CREATE TABLE IF NOT EXISTS users was a no-op (table already
    existed without email) the column has to be added first. This used to
    live in init_db()'s statements list and crashed boot with
    UndefinedColumn: column "email" does not exist.
    """
    try:
        execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT")
    except Exception as exc:
        _log.warning("users.email migration skipped: %s", exc)
        return  # no email column ⇒ no index to create

    # Email is optional for legacy rows but unique when present, so two
    # users can never share a recovery address. Index uses lower(email)
    # so 'Foo@bar.com' and 'foo@bar.com' collide on signup.
    try:
        execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS uniq_users_email_lower
               ON users (lower(email)) WHERE email IS NOT NULL"""
        )
    except Exception as exc:
        _log.warning(
            "uniq_users_email_lower index not created (likely duplicate "
            "emails on legacy rows): %s. Resolve with: SELECT lower(email), "
            "count(*) FROM users WHERE email IS NOT NULL GROUP BY 1 HAVING "
            "count(*) > 1;",
            exc,
        )


def _migrate_account_name_unique_index():
    """
    Drop the legacy global-unique index on user_accounts.account_name.

    Earlier in the app's life this index enforced "one user per
    normalized account label" because ``account_name`` was the only
    BigQuery scoping key — sharing a label leaked rows across tenants.
    We've since switched to ``user_id`` as the row-level tenant key on
    every BQ read (see docs/USER_ID_TENANCY.md and the
    bigquery-tenant-isolation rule), so two users (e.g. a parent and
    a child) can legitimately share a label like "Schwab Account".
    This migration removes the global-unique index from any DB where
    it actually got installed; its absence on most prod DBs (the
    install commonly failed during open-beta because pre-existing
    duplicates blocked it) is also fine. The per-user uniqueness is
    still enforced by the table's ``(user_id, account_name)``
    primary/unique key combined with ``ON CONFLICT (user_id,
    account_name) DO NOTHING`` in ``add_account_for_user``.
    """
    try:
        execute("DROP INDEX IF EXISTS uniq_user_accounts_global_account_name")
    except Exception as exc:
        _log.warning(
            "Could not drop legacy uniq_user_accounts_global_account_name "
            "index: %s. The app no longer relies on it; you can drop it "
            "manually if it still exists.",
            exc,
        )


class User(UserMixin):
    """Simple user model backed by Postgres."""

    def __init__(self, id, username, password_hash, email=None):
        self.id = id
        self.username = username
        self.password_hash = password_hash
        self.email = email

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    @staticmethod
    def _from_row(row):
        if not row:
            return None
        return User(
            id=row["id"],
            username=row["username"],
            password_hash=row["password_hash"],
            email=row.get("email") if isinstance(row, dict) else None,
        )

    @staticmethod
    def get_by_id(user_id):
        return User._from_row(
            fetch_one(
                "SELECT id, username, password_hash, email FROM users WHERE id = %s",
                (user_id,),
            )
        )

    @staticmethod
    def get_by_username(username):
        return User._from_row(
            fetch_one(
                "SELECT id, username, password_hash, email FROM users WHERE username = %s",
                (username,),
            )
        )

    @staticmethod
    def get_by_email(email):
        """Lookup is case-insensitive (mirrors uniq_users_email_lower).
        Returns None when no row matches; treat that as 'no account' but do
        not echo it back to the requester in /forgot-password to avoid
        leaking which addresses are signed up."""
        if not email:
            return None
        return User._from_row(
            fetch_one(
                "SELECT id, username, password_hash, email FROM users "
                "WHERE lower(email) = lower(%s) LIMIT 1",
                (email,),
            )
        )

    @staticmethod
    def create(username, password, email=None):
        password_hash = generate_password_hash(password)
        clean_email = (email or "").strip() or None
        execute(
            "INSERT INTO users (username, password_hash, email) VALUES (%s, %s, %s)",
            (username, password_hash, clean_email),
        )

    @staticmethod
    def update_password(user_id, new_password):
        execute(
            "UPDATE users SET password_hash = %s WHERE id = %s",
            (generate_password_hash(new_password), user_id),
        )

    @staticmethod
    def update_email(user_id, email):
        clean = (email or "").strip() or None
        execute("UPDATE users SET email = %s WHERE id = %s", (clean, user_id))


def delete_user(user_id):
    """Permanently delete a user and cascade-clean every related Postgres row.

    Every user-scoped table is declared with ``ON DELETE CASCADE`` on
    ``users(id)``, so a single ``DELETE FROM users`` removes
    user_profiles, user_accounts, schwab_connections, weekly_mirror_scores,
    insights, strategy_fit_insights, uploads, password_reset_tokens,
    user_review_visits, user_follows (both directions), community_posts,
    and community_published_trades in the same transaction.

    Two tables retain the row on purpose:
      * ``feedback``     — ON DELETE SET NULL so bug reports outlive the user.
      * ``pro_waitlist`` — ON DELETE SET NULL so waitlist entries survive.
      * ``login_attempts`` has no FK (keyed by ``username_lc``); rows persist
        as audit trail but become unreachable from any user lookup.

    This does NOT touch BigQuery. The warehouse is rebuilt from
    ``dbt/seeds/*.csv`` on every CI run, so a BQ ``DELETE`` would be
    undone the next dbt build. Use
    ``app.upload.purge_user_id_from_seeds`` first when you want the
    warehouse cleaned alongside the Postgres delete.

    Returns True on success, False if the DELETE raised.
    """
    try:
        execute("DELETE FROM users WHERE id = %s", (user_id,))
        return True
    except Exception as exc:
        _log.warning("delete_user(%s) failed: %s", user_id, exc)
        return False


# ------------------------------------------------------------------
# User <-> Account association
# ------------------------------------------------------------------

def get_accounts_for_user(user_id):
    rows = fetch_all(
        "SELECT account_name FROM user_accounts WHERE user_id = %s ORDER BY account_name",
        (user_id,),
    )
    return [r["account_name"] for r in rows]


def add_account_for_user(user_id, account_name):
    """Link an account label to a user.

    Idempotent for the same ``(user_id, account_name)`` via the
    table's primary/unique key on that pair. Two **different** users
    can both claim the same label (e.g. a parent and a child both
    calling their account "Schwab Account") — tenant isolation is
    enforced at the row level by ``user_id`` everywhere downstream
    (every BQ query passes through ``_account_sql_and`` and every
    DataFrame through ``_filter_df_by_accounts``). See
    ``docs/USER_ID_TENANCY.md``.
    """
    execute(
        "INSERT INTO user_accounts (user_id, account_name) VALUES (%s, %s) "
        "ON CONFLICT (user_id, account_name) DO NOTHING",
        (user_id, account_name),
    )


class AccountClaimedError(Exception):
    """Deprecated.

    Used to be raised when a label was already linked to a different
    user, back when ``account_name`` was the BigQuery tenancy key. With
    ``user_id`` as the tenant key, two users can share a label safely,
    so this is never raised anymore. The class is kept so existing
    ``except AccountClaimedError`` blocks in ``app/upload.py`` and
    ``app/schwab.py`` continue to compile; they're now harmless dead
    branches that can be removed in a follow-up cleanup.
    """

    def __init__(self, account_name: str, owner_user_id: int):
        super().__init__(
            f"Account label {account_name!r} is already linked to another user "
            f"(owner_user_id={owner_user_id})."
        )
        self.account_name = account_name
        self.owner_user_id = owner_user_id


def account_is_claimed_by_other(user_id, account_name):  # noqa: ARG001
    """Deprecated — always returns False.

    Sharing account labels across users is allowed now that ``user_id``
    is the tenant key on every BQ read. Kept as a callable so existing
    pre-flight checks in ``upload.py`` / ``schwab.py`` keep compiling
    and become no-ops.
    """
    return False


def find_cross_tenant_account_conflicts(account_names):  # noqa: ARG001
    """Deprecated — always returns an empty set.

    See ``account_is_claimed_by_other`` and ``docs/USER_ID_TENANCY.md``.
    """
    return set()


def remove_account_for_user(user_id, account_name):
    execute(
        "DELETE FROM user_accounts WHERE user_id = %s AND account_name = %s",
        (user_id, account_name),
    )


# ------------------------------------------------------------------
# Broker account tenancy
# (see docs/BROKER_ACCOUNT_ID_MIGRATION.md)
# ------------------------------------------------------------------

MANUAL_BROKER_SLUG = "manual"
DEMO_BROKER_SLUG = "demo"
SCHWAB_BROKER_SLUG = "schwab"
SNAPTRADE_BROKER_SLUG = "snaptrade"


def get_or_create_broker_account(
    user_id, broker_slug, broker_external_id, account_name,
):
    """Idempotent upsert for ``broker_accounts``. Returns the row id.

    The natural key is ``(user_id, broker_slug, broker_external_id)`` —
    a single physical broker account belonging to a single HappyTrader
    user. Two users linking the same physical Schwab account each get
    their own row (different ``user_id``) but the same
    ``broker_external_id``.

    ``account_name`` is captured on FIRST insert and refreshed on
    re-link only if the new label is non-empty AND different. We never
    let an empty label overwrite a previously-captured one (broker
    APIs occasionally ship empty nicknames mid-session).
    """
    if user_id is None:
        raise ValueError("user_id is required")
    slug = (broker_slug or "").strip().lower()
    if not slug:
        raise ValueError("broker_slug is required")
    ext = (broker_external_id or "").strip()
    if not ext:
        raise ValueError("broker_external_id is required")
    label = (account_name or "").strip()
    if not label:
        raise ValueError("account_name is required")

    row = fetch_one(
        "SELECT id, account_name FROM broker_accounts "
        "WHERE user_id = %s AND broker_slug = %s AND broker_external_id = %s",
        (int(user_id), slug, ext),
    )
    if row:
        if label and label != (row.get("account_name") or ""):
            execute(
                "UPDATE broker_accounts SET account_name = %s, "
                "updated_at = NOW() WHERE id = %s",
                (label, row["id"]),
            )
        return int(row["id"])

    new_row = execute_returning(
        "INSERT INTO broker_accounts "
        "(user_id, broker_slug, broker_external_id, account_name) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (user_id, broker_slug, broker_external_id) "
        "DO UPDATE SET updated_at = NOW() "
        "RETURNING id",
        (int(user_id), slug, ext, label),
    )
    return int(new_row["id"])


def get_broker_account_by_id(broker_account_id):
    """Lookup a single broker_accounts row by primary key."""
    if broker_account_id is None:
        return None
    return fetch_one(
        "SELECT id, user_id, broker_slug, broker_external_id, account_name, "
        "created_at, updated_at FROM broker_accounts WHERE id = %s",
        (int(broker_account_id),),
    )


def get_broker_account_ids_for_user(user_id):
    """Return the list of ``broker_accounts.id`` owned by this user.

    Stage 3 read path: the Flask filter will use this to scope every
    BigQuery query to the user's broker_account_ids. Until then it's
    only used in tests / diagnostics.
    """
    if user_id is None:
        return []
    rows = fetch_all(
        "SELECT id FROM broker_accounts WHERE user_id = %s ORDER BY id",
        (int(user_id),),
    )
    return [int(r["id"]) for r in rows]


def get_broker_accounts_for_user(user_id):
    """Return full broker_accounts rows for this user (display use)."""
    if user_id is None:
        return []
    return fetch_all(
        "SELECT id, user_id, broker_slug, broker_external_id, account_name, "
        "created_at, updated_at FROM broker_accounts "
        "WHERE user_id = %s ORDER BY id",
        (int(user_id),),
    )


# ------------------------------------------------------------------
# Broker tenants (v2 tenancy — see docs/V2_TENANT_KEY_DESIGN.md)
#
# tenant_id := "<broker_slug>:<broker_uuid>"  — the warehouse join key.
# broker_uuid is the broker-stable identifier (SnapTrade UUID, etc).
# Never minted by us, never transformed in transit. The structural
# guarantee that defeats every drift / collision bug the v1
# broker_account_id SERIAL produced.
# ------------------------------------------------------------------


def build_tenant_id(broker_slug, broker_uuid):
    """Compose the warehouse tenant_id from (broker_slug, broker_uuid).

    Format spec (locked in docs/V2_TENANT_KEY_DESIGN.md):
        tenant_id := "<broker_slug>:<broker_uuid>"

    Raises ``ValueError`` for empty / invalid inputs — fail fast so
    a None broker_uuid never silently becomes ``"snaptrade:None"``.
    """
    slug = (broker_slug or "").strip().lower()
    if not slug:
        raise ValueError("broker_slug is required")
    uuid_part = (broker_uuid or "").strip()
    if not uuid_part:
        raise ValueError("broker_uuid is required")
    return f"{slug}:{uuid_part}"


def get_or_create_broker_tenant(
    user_id,
    broker_slug,
    broker_uuid,
    account_name,
    account_mask=None,
    broker_label=None,
    snaptrade_connection_id=None,
):
    """Idempotent upsert for ``broker_tenants``. Returns ``tenant_id``.

    Natural key is ``(broker_slug, broker_uuid)`` — globally unique
    across all users, all brokers, all time. The same physical broker
    account always resolves to the same tenant_id, even after a
    Postgres drop+recreate (because broker_uuid is broker-issued, not
    ours).

    ``account_name``/``account_mask``/``broker_label`` are captured on
    first insert and refreshed only if a non-empty new value differs
    from what's already stored — broker APIs occasionally ship empty
    nicknames mid-session and we don't want them to overwrite the
    real label.
    """
    if user_id is None:
        raise ValueError("user_id is required")
    tenant_id = build_tenant_id(broker_slug, broker_uuid)
    slug = (broker_slug or "").strip().lower()
    uuid_part = (broker_uuid or "").strip()
    label = (account_name or "").strip()
    if not label:
        raise ValueError("account_name is required")
    mask = (account_mask or "").strip() or None
    broker_lbl = (broker_label or "").strip() or None
    snap_conn = (snaptrade_connection_id or "").strip() or None

    row = fetch_one(
        "SELECT tenant_id, account_name, account_mask, broker_label, "
        "snaptrade_connection_id FROM broker_tenants WHERE tenant_id = %s",
        (tenant_id,),
    )
    if row:
        updates = []
        params = []
        if label and label != (row.get("account_name") or ""):
            updates.append("account_name = %s")
            params.append(label)
        if mask and mask != (row.get("account_mask") or ""):
            updates.append("account_mask = %s")
            params.append(mask)
        if broker_lbl and broker_lbl != (row.get("broker_label") or ""):
            updates.append("broker_label = %s")
            params.append(broker_lbl)
        if snap_conn and snap_conn != (row.get("snaptrade_connection_id") or ""):
            updates.append("snaptrade_connection_id = %s")
            params.append(snap_conn)
        if updates:
            updates.append("updated_at = NOW()")
            params.append(tenant_id)
            execute(
                "UPDATE broker_tenants SET "
                + ", ".join(updates)
                + " WHERE tenant_id = %s",
                tuple(params),
            )
        return tenant_id

    execute(
        "INSERT INTO broker_tenants "
        "(tenant_id, user_id, broker_slug, broker_uuid, account_name, "
        " account_mask, broker_label, snaptrade_connection_id) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (tenant_id) DO UPDATE SET updated_at = NOW()",
        (
            tenant_id, int(user_id), slug, uuid_part, label,
            mask, broker_lbl, snap_conn,
        ),
    )
    return tenant_id


def update_broker_tenant_display_nickname(user_id, tenant_id, nickname):
    """Set or clear ``broker_tenants.display_nickname`` for one tenant.

    UI-only label that shadows ``account_name`` everywhere we render
    account labels to the user (Daily Review snap table, /positions,
    /position/<symbol>, /strategies, /wealth, etc). Never writes
    ``account_name`` — that stays as the warehouse tenancy display key
    and matches what the mart's ``account`` column carries.

    Bounded to ``_MAX_SNAPTRADE_NICKNAME_LEN`` to mirror the legacy
    ``snaptrade_accounts.display_nickname`` cap. Pass empty/None to
    clear; the column is nullable.

    Scoped by ``user_id`` so one tenant cannot have their nickname
    overwritten by an admin or another user calling this directly.
    """
    if user_id is None or not tenant_id:
        return False
    label = (nickname or "").strip()
    if len(label) > _MAX_SNAPTRADE_NICKNAME_LEN:
        label = label[:_MAX_SNAPTRADE_NICKNAME_LEN]
    value = label or None
    try:
        execute(
            "UPDATE broker_tenants SET display_nickname = %s, updated_at = NOW() "
            "WHERE tenant_id = %s AND user_id = %s",
            (value, str(tenant_id), int(user_id)),
        )
        return True
    except Exception as exc:
        _log.warning("update_broker_tenant_display_nickname failed: %s", exc)
        return False


def get_broker_tenant(tenant_id):
    """Full row by tenant_id, or None."""
    if not tenant_id:
        return None
    return fetch_one(
        "SELECT tenant_id, user_id, broker_slug, broker_uuid, "
        "account_name, account_mask, broker_label, "
        "snaptrade_connection_id, connection_status, "
        "connection_broken_at, first_sync_completed, display_nickname, "
        "created_at, updated_at FROM broker_tenants WHERE tenant_id = %s",
        (str(tenant_id),),
    )


def get_tenant_ids_for_user(user_id):
    """Active tenant_ids the user can see.

    Excludes rows with ``connection_status != 'active'`` — disabled
    connections still exist as rows so we can fire a "Reconnect"
    banner, but their warehouse data is filtered out at the SQL
    boundary until the user re-auths.

    Returns ``[]`` for None / unknown user / no connections.
    """
    if user_id is None:
        return []
    rows = fetch_all(
        "SELECT tenant_id FROM broker_tenants "
        "WHERE user_id = %s AND connection_status = 'active' "
        "ORDER BY created_at",
        (int(user_id),),
    )
    return [r["tenant_id"] for r in rows]


def get_broker_tenants_for_user(user_id, include_inactive=False):
    """Full broker_tenants rows for this user (Settings → Account display).

    ``include_inactive=True`` also returns rows whose connection has
    been marked disabled, so the UI can show "Reconnect" CTAs for them.
    """
    if user_id is None:
        return []
    sql = (
        "SELECT tenant_id, user_id, broker_slug, broker_uuid, "
        "account_name, account_mask, broker_label, "
        "snaptrade_connection_id, connection_status, "
        "connection_broken_at, first_sync_completed, display_nickname, "
        "created_at, updated_at FROM broker_tenants "
        "WHERE user_id = %s"
    )
    if not include_inactive:
        sql += " AND connection_status = 'active'"
    sql += " ORDER BY created_at"
    return fetch_all(sql, (int(user_id),))


def mark_tenant_connection_broken(tenant_id):
    """Flag a broker tenant as needing re-auth.

    Idempotent on ``connection_broken_at`` — preserves the FIRST
    detection timestamp so the banner doesn't reset every cron run.
    """
    if not tenant_id:
        return
    try:
        execute(
            "UPDATE broker_tenants SET "
            "connection_status = 'disabled', "
            "connection_broken_at = COALESCE(connection_broken_at, NOW()), "
            "updated_at = NOW() "
            "WHERE tenant_id = %s",
            (str(tenant_id),),
        )
    except Exception as exc:
        _log.warning("mark_tenant_connection_broken failed: %s", exc)


def clear_tenant_connection_broken(tenant_id):
    """Clear the broken-connection flag after successful re-auth."""
    if not tenant_id:
        return
    execute(
        "UPDATE broker_tenants SET "
        "connection_status = 'active', "
        "connection_broken_at = NULL, "
        "updated_at = NOW() "
        "WHERE tenant_id = %s",
        (str(tenant_id),),
    )


def mark_tenant_first_sync_completed(tenant_id):
    """Set ``first_sync_completed = TRUE`` after the initial pull."""
    if not tenant_id:
        return
    execute(
        "UPDATE broker_tenants SET first_sync_completed = TRUE, "
        "updated_at = NOW() WHERE tenant_id = %s AND first_sync_completed = FALSE",
        (str(tenant_id),),
    )


def get_broken_broker_tenants(user_id):
    """Return tenant rows needing re-auth (for the in-app banner).

    Mirrors ``get_expired_schwab_connections`` shape so the v1 banner
    template can render the same ``display_nickname or account_name``
    label.
    """
    if user_id is None:
        return []
    try:
        return fetch_all(
            "SELECT tenant_id, account_name, display_nickname, broker_label, "
            "connection_broken_at FROM broker_tenants "
            "WHERE user_id = %s AND connection_status != 'active' "
            "ORDER BY created_at",
            (int(user_id),),
        )
    except Exception as exc:
        _log.warning("get_broken_broker_tenants failed: %s", exc)
        return []


# ------------------------------------------------------------------
# Uploads
# ------------------------------------------------------------------

def record_upload(user_id, account_name, history_rows, current_rows):
    execute(
        "INSERT INTO uploads (user_id, account_name, history_rows, current_rows) "
        "VALUES (%s, %s, %s, %s)",
        (user_id, account_name, history_rows, current_rows),
    )


def get_uploads_for_user(user_id, limit=10):
    return fetch_all(
        "SELECT account_name, history_rows, current_rows, uploaded_at "
        "FROM uploads WHERE user_id = %s ORDER BY uploaded_at DESC LIMIT %s",
        (user_id, limit),
    )


def count_uploads_for_user(user_id):
    """Total number of CSV uploads recorded for this user."""
    row = fetch_one(
        "SELECT COUNT(*) AS n FROM uploads WHERE user_id = %s",
        (user_id,),
    )
    return int(row["n"]) if row else 0


# ------------------------------------------------------------------
# Pro tier waitlist
# ------------------------------------------------------------------

def add_pro_waitlist_entry(user_id=None, email=None):
    """Add a logged-in user (or anonymous email) to the Pro waitlist.

    Idempotent: if the same user_id or email already exists, no-op.
    """
    if user_id is not None:
        existing = fetch_one(
            "SELECT id FROM pro_waitlist WHERE user_id = %s LIMIT 1",
            (user_id,),
        )
        if existing:
            return
        execute(
            "INSERT INTO pro_waitlist (user_id, email) VALUES (%s, %s)",
            (user_id, email),
        )
        return

    if email:
        existing = fetch_one(
            "SELECT id FROM pro_waitlist WHERE email = %s LIMIT 1",
            (email,),
        )
        if existing:
            return
        execute(
            "INSERT INTO pro_waitlist (user_id, email) VALUES (NULL, %s)",
            (email,),
        )


def is_user_on_pro_waitlist(user_id):
    if user_id is None:
        return False
    row = fetch_one(
        "SELECT 1 FROM pro_waitlist WHERE user_id = %s LIMIT 1",
        (user_id,),
    )
    return bool(row)


# ------------------------------------------------------------------
# Insights (AI analysis cache)
# ------------------------------------------------------------------

def get_user_llm_model(user_id):
    """Return the user's chosen LLM catalog key, or None if unset.

    The value is validated against the live allowlist at use-time by
    app.llm.resolve_model_key, so a stale/disabled key here degrades to the
    default model rather than erroring."""
    try:
        row = fetch_one("SELECT preferred_llm_model FROM users WHERE id = %s", (user_id,))
    except Exception:
        return None
    if not row:
        return None
    return row.get("preferred_llm_model")


def set_user_llm_model(user_id, model_key):
    """Persist the user's chosen LLM catalog key (or NULL to clear)."""
    execute(
        "UPDATE users SET preferred_llm_model = %s WHERE id = %s",
        (model_key or None, user_id),
    )


def save_insight(user_id, summary, full_analysis):
    """Save (or replace) the cached AI insight for a user."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM insights WHERE user_id = %s", (user_id,))
            cur.execute(
                "INSERT INTO insights (user_id, summary, full_analysis) "
                "VALUES (%s, %s, %s)",
                (user_id, summary, full_analysis),
            )


def get_insight_for_user(user_id):
    return fetch_one(
        "SELECT summary, full_analysis, generated_at FROM insights "
        "WHERE user_id = %s ORDER BY generated_at DESC LIMIT 1",
        (user_id,),
    )


def save_strategy_fit_insight(user_id, account_filter, summary, full_analysis, brief_text):
    """Save (or replace) a cached strategy-fit insight for (user, account scope).

    `account_filter` lets us cache different views separately (e.g. "All" vs
    a specific account) so toggling the account dropdown doesn't show stale
    narration."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM strategy_fit_insights "
                "WHERE user_id = %s AND account_filter = %s",
                (user_id, account_filter or ""),
            )
            cur.execute(
                "INSERT INTO strategy_fit_insights "
                "(user_id, account_filter, summary, full_analysis, brief_text) "
                "VALUES (%s, %s, %s, %s, %s)",
                (user_id, account_filter or "", summary, full_analysis, brief_text),
            )


def get_strategy_fit_insight_for_user(user_id, account_filter=""):
    return fetch_one(
        "SELECT summary, full_analysis, brief_text, generated_at "
        "FROM strategy_fit_insights "
        "WHERE user_id = %s AND account_filter = %s "
        "ORDER BY generated_at DESC LIMIT 1",
        (user_id, account_filter or ""),
    )


# ------------------------------------------------------------------
# Mirror Score (behavioral diagnostic)
# ------------------------------------------------------------------

def save_mirror_score(
    user_id, week_start_date,
    discipline_score, intent_score, risk_alignment_score, consistency_score,
    mirror_score, confidence_level, diagnostic_sentence=None,
):
    """Save or replace weekly mirror score for a user."""
    execute(
        """INSERT INTO weekly_mirror_scores
           (user_id, week_start_date, discipline_score, intent_score, risk_alignment_score,
            consistency_score, mirror_score, confidence_level, diagnostic_sentence)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (user_id, week_start_date) DO UPDATE SET
               discipline_score     = EXCLUDED.discipline_score,
               intent_score         = EXCLUDED.intent_score,
               risk_alignment_score = EXCLUDED.risk_alignment_score,
               consistency_score    = EXCLUDED.consistency_score,
               mirror_score         = EXCLUDED.mirror_score,
               confidence_level     = EXCLUDED.confidence_level,
               diagnostic_sentence  = EXCLUDED.diagnostic_sentence,
               generated_at         = NOW()""",
        (
            user_id, week_start_date,
            discipline_score, intent_score, risk_alignment_score, consistency_score,
            mirror_score, confidence_level, diagnostic_sentence,
        ),
    )


def get_mirror_score_for_user(user_id, week_start_date=None):
    """Return mirror score for user. If week_start_date is None, return latest."""
    if week_start_date:
        return fetch_one(
            """SELECT week_start_date, discipline_score, intent_score, risk_alignment_score,
                      consistency_score, mirror_score, confidence_level, diagnostic_sentence,
                      generated_at
               FROM weekly_mirror_scores
               WHERE user_id = %s AND week_start_date = %s""",
            (user_id, week_start_date),
        )
    return fetch_one(
        """SELECT week_start_date, discipline_score, intent_score, risk_alignment_score,
                  consistency_score, mirror_score, confidence_level, diagnostic_sentence,
                  generated_at
           FROM weekly_mirror_scores
           WHERE user_id = %s
           ORDER BY week_start_date DESC LIMIT 1""",
        (user_id,),
    )


def get_mirror_score_history(user_id, limit=8):
    """Return the most recent N mirror scores for trend display (oldest -> newest)."""
    rows = fetch_all(
        """SELECT week_start_date, mirror_score, discipline_score, intent_score,
                  risk_alignment_score, consistency_score, confidence_level
           FROM weekly_mirror_scores
           WHERE user_id = %s
           ORDER BY week_start_date DESC LIMIT %s""",
        (user_id, limit),
    )
    return list(reversed(rows))


# ------------------------------------------------------------------
# SnapTrade aggregator connections
# ------------------------------------------------------------------
#
# Direct Schwab (`schwab_connections` + app/schwab.py) was removed in v2.
# The legacy table DDL remains in init_db for existing deployments until
# Phase 6 cutover drops it — see scripts/admin/v2_cutover_reset.py.
#
# SnapTrade is an OAuth-style brokerage aggregator that gives us a single
# integration covering ~20 brokers (Schwab, Fidelity, Vanguard, Robinhood,
# IBKR, Tradier, ...). Per HappyTrader user we store ONE
# ``snaptrade_connections`` row (the SnapTrade userId/userSecret pair) and
# MANY ``snaptrade_accounts`` rows (one per linked brokerage account, same
# grain as ``schwab_connections``).
#
# Tenancy: ``account_name`` is the warehouse-facing label that flows into
# ``seed.Account``. NEVER rename it after first sync — that detaches every
# committed seed row from the user. ``display_nickname`` is UI-only and
# safe to change.

_MAX_SNAPTRADE_NICKNAME_LEN = 80


def save_snaptrade_user(user_id, snaptrade_user_id, snaptrade_secret):
    """
    Store (or refresh) the SnapTrade user credentials for a HappyTrader
    user. Idempotent: re-running with new credentials updates the row.

    Mirrors ``save_schwab_connection``'s "always upsert" semantic so the
    SnapTrade /connect route can blindly call this on every flow.
    """
    execute(
        """INSERT INTO snaptrade_connections
           (user_id, snaptrade_user_id, snaptrade_secret, updated_at)
           VALUES (%s, %s, %s, NOW())
           ON CONFLICT (user_id) DO UPDATE SET
               snaptrade_user_id = EXCLUDED.snaptrade_user_id,
               snaptrade_secret  = EXCLUDED.snaptrade_secret,
               updated_at        = NOW()""",
        (user_id, snaptrade_user_id, snaptrade_secret),
    )


def get_snaptrade_user(user_id):
    """Return the SnapTrade userId/userSecret row, or None."""
    if user_id is None:
        return None
    return fetch_one(
        "SELECT snaptrade_user_id, snaptrade_secret, created_at, updated_at "
        "FROM snaptrade_connections WHERE user_id = %s",
        (user_id,),
    )


def get_user_id_by_snaptrade_user_id(snaptrade_user_id):
    """Reverse lookup: SnapTrade's ``userId`` → our HappyTrader ``user_id``.

    Used by the SnapTrade webhook handler — webhook payloads carry SnapTrade's
    own userId, and we store that verbatim (namespaced when dev/staging share a
    clientId) in ``snaptrade_connections.snaptrade_user_id``. Returns the int
    user_id or None.
    """
    if not snaptrade_user_id:
        return None
    row = fetch_one(
        "SELECT user_id FROM snaptrade_connections WHERE snaptrade_user_id = %s",
        (snaptrade_user_id,),
    )
    return row["user_id"] if row else None


def remove_snaptrade_user(user_id):
    """Drop the SnapTrade user record. ON DELETE CASCADE removes account rows."""
    execute(
        "DELETE FROM snaptrade_connections WHERE user_id = %s",
        (user_id,),
    )


def upsert_snaptrade_account(
    user_id,
    snaptrade_account_id,
    *,
    broker_slug,
    account_number_masked,
    account_name,
):
    """Insert or update a SnapTrade-managed broker account.

    ``account_name`` is the warehouse tenancy label and is overwritten on
    every upsert — SnapTrade can change the label between runs (e.g. user
    renames an account at the broker), but renaming after first sync
    orphans seed rows. Callers (the /snaptrade/callback handler) are
    expected to pass a stable label derived from broker + masked number.
    """
    execute(
        """INSERT INTO snaptrade_accounts
           (user_id, snaptrade_account_id, broker_slug, account_number_masked,
            account_name, updated_at)
           VALUES (%s, %s, %s, %s, %s, NOW())
           ON CONFLICT (user_id, snaptrade_account_id) DO UPDATE SET
               broker_slug           = EXCLUDED.broker_slug,
               account_number_masked = EXCLUDED.account_number_masked,
               account_name          = EXCLUDED.account_name,
               connection_broken_at  = NULL,
               updated_at            = NOW()""",
        (
            user_id,
            snaptrade_account_id,
            broker_slug,
            account_number_masked,
            account_name,
        ),
    )


def get_snaptrade_accounts(user_id):
    """Lightweight metadata for every SnapTrade account the user owns.

    Mirrors ``get_schwab_connections`` shape — every column the multi-
    account UI and bulk sync loop need is SELECTed here, including
    ``first_sync_completed`` (drives full-history-vs-routine lookback
    on the first sync per row, same pattern as Schwab).
    """
    if user_id is None:
        return []
    return fetch_all(
        "SELECT id, snaptrade_account_id, broker_slug, account_number_masked, "
        "account_name, display_nickname, first_sync_completed, last_sync_at, "
        "holdings_last_successful_sync, "
        "last_sync_error, connection_broken_at, brokerage_authorization_id, "
        "last_force_refresh_at, created_at "
        "FROM snaptrade_accounts WHERE user_id = %s "
        "ORDER BY created_at",
        (user_id,),
    )


def get_snaptrade_account(user_id, snaptrade_account_id):
    """Return one SnapTrade account row, or None."""
    return fetch_one(
        "SELECT id, snaptrade_account_id, broker_slug, account_number_masked, "
        "account_name, display_nickname, first_sync_completed, last_sync_at, "
        "holdings_last_successful_sync, "
        "last_sync_error, connection_broken_at, brokerage_authorization_id, "
        "last_force_refresh_at "
        "FROM snaptrade_accounts WHERE user_id = %s AND snaptrade_account_id = %s",
        (user_id, snaptrade_account_id),
    )


def set_snaptrade_brokerage_authorization_id(user_id, snaptrade_account_id, authorization_id):
    """Cache SnapTrade's per-connection auth UUID on the account row.

    Looked up via ``account_information.get_user_account_details`` on the
    first force-refresh and re-used on every subsequent press so the
    button doesn't burn an extra API call to re-discover it. Idempotent
    (UPDATE is a no-op when the value matches).
    """
    auth_id = (authorization_id or "").strip() or None
    if not auth_id:
        return False
    try:
        execute(
            "UPDATE snaptrade_accounts "
            "SET brokerage_authorization_id = %s, updated_at = NOW() "
            "WHERE user_id = %s AND snaptrade_account_id = %s",
            (auth_id, user_id, snaptrade_account_id),
        )
        return True
    except Exception as exc:
        _log.warning("set_snaptrade_brokerage_authorization_id failed: %s", exc)
        return False


def stamp_snaptrade_force_refresh_attempt(user_id, snaptrade_account_id):
    """Stamp ``last_force_refresh_at = NOW()`` for throttle accounting.

    Called AFTER a successful ``refresh_brokerage_authorization`` SDK
    call so the next press is rate-limited. Stamping on attempt-success
    (not on attempt-start) means a 5xx from SnapTrade doesn't burn the
    user's throttle window.
    """
    try:
        execute(
            "UPDATE snaptrade_accounts "
            "SET last_force_refresh_at = NOW(), updated_at = NOW() "
            "WHERE user_id = %s AND snaptrade_account_id = %s",
            (user_id, snaptrade_account_id),
        )
        return True
    except Exception as exc:
        _log.warning("stamp_snaptrade_force_refresh_attempt failed: %s", exc)
        return False


def mark_snaptrade_first_sync_completed(user_id, snaptrade_account_id):
    """Flip the per-row first-sync flag after a successful pull. Same
    semantic as ``mark_schwab_first_sync_completed`` — newly added
    accounts default to full-history on their first sync; subsequent
    syncs use the routine lookback window."""
    execute(
        "UPDATE snaptrade_accounts SET first_sync_completed = TRUE, updated_at = NOW() "
        "WHERE user_id = %s AND snaptrade_account_id = %s",
        (user_id, snaptrade_account_id),
    )


def update_snaptrade_account_nickname(user_id, snaptrade_account_id, nickname):
    """UI-only label, never writes to ``account_name`` (tenancy key).

    Dual-writes to ``broker_tenants.display_nickname`` for the matching
    tenant_id so v2 read paths (``_account_label_map``,
    ``_tenant_display_label``) pick up the user-chosen label
    immediately. ``snaptrade_accounts`` is the legacy table; once it's
    fully retired the dual write can collapse to broker_tenants only.
    """
    label = (nickname or "").strip()
    if len(label) > _MAX_SNAPTRADE_NICKNAME_LEN:
        label = label[:_MAX_SNAPTRADE_NICKNAME_LEN]
    value = label or None
    try:
        execute(
            "UPDATE snaptrade_accounts SET display_nickname = %s, updated_at = NOW() "
            "WHERE user_id = %s AND snaptrade_account_id = %s",
            (value, user_id, snaptrade_account_id),
        )
    except Exception as exc:
        _log.warning("update_snaptrade_account_nickname failed: %s", exc)
        return False
    # Dual write to broker_tenants — v2 read paths use this column.
    try:
        tenant_id = build_tenant_id(SNAPTRADE_BROKER_SLUG, snaptrade_account_id)
        update_broker_tenant_display_nickname(user_id, tenant_id, label)
    except Exception as exc:
        _log.warning(
            "update_snaptrade_account_nickname dual-write to broker_tenants failed: %s",
            exc,
        )
    return True


def record_snaptrade_sync_attempt(user_id, snaptrade_account_id, *, error=None):
    """Stamp ``last_sync_at`` and (optionally) ``last_sync_error`` for the
    given account. Pass ``error=None`` on success to clear any prior
    error message; pass a string to record a failure for the UI."""
    execute(
        "UPDATE snaptrade_accounts "
        "SET last_sync_at = NOW(), last_sync_error = %s, updated_at = NOW() "
        "WHERE user_id = %s AND snaptrade_account_id = %s",
        (error, user_id, snaptrade_account_id),
    )


def record_snaptrade_holdings_sync(user_id, snaptrade_account_id, when):
    """Persist SnapTrade's own ``holdings.last_successful_sync`` — the
    honest "broker data as of" timestamp surfaced in the UI.

    ``when`` is a ``datetime``/``date`` (or None). None is a no-op so a
    missing/unparseable signal never clobbers a previously-good value (mirrors
    the None-is-safe philosophy of the freshness backstop). Best-effort: a
    failure here must never break an otherwise-successful sync."""
    if when is None:
        return False
    try:
        execute(
            "UPDATE snaptrade_accounts "
            "SET holdings_last_successful_sync = %s, updated_at = NOW() "
            "WHERE user_id = %s AND snaptrade_account_id = %s",
            (when, user_id, snaptrade_account_id),
        )
        return True
    except Exception as exc:
        _log.warning("record_snaptrade_holdings_sync failed: %s", exc)
        return False


def record_snaptrade_sync_observation(
    user_id,
    snaptrade_account_id,
    *,
    broker_slug=None,
    holdings_last_successful_sync=None,
    last_sync_at=None,
    ok=True,
):
    """Append ONE row to the append-only ``snaptrade_sync_observations`` log
    per sync run (see CLOSE-BASED REPORTING plan, Phase 3).

    Unlike ``record_snaptrade_holdings_sync`` (which keeps only the latest
    value on ``snaptrade_accounts``), this preserves the FULL history so we
    can measure how late after the 4pm ET close SnapTrade's
    ``holdings_last_successful_sync`` actually advances for each broker, and
    retime the cron precisely. Best-effort: a failure here must never break
    an otherwise-successful sync."""
    try:
        execute(
            "INSERT INTO snaptrade_sync_observations "
            "(user_id, snaptrade_account_id, broker_slug, cron_run_at, "
            " holdings_last_successful_sync, last_sync_at, ok) "
            "VALUES (%s, %s, %s, NOW(), %s, %s, %s)",
            (
                user_id,
                snaptrade_account_id,
                broker_slug,
                holdings_last_successful_sync,
                last_sync_at,
                bool(ok),
            ),
        )
        return True
    except Exception as exc:
        _log.warning("record_snaptrade_sync_observation failed: %s", exc)
        return False


def mark_snaptrade_connection_broken(user_id, snaptrade_account_id):
    """Flag a SnapTrade account as needing reconnection (broker grant
    revoked, broker side error, etc). Idempotent on the timestamp so
    the banner does not reset on every cron run.

    Returns ``True`` when this call is the NULL -> set *transition* (the
    connection was previously healthy and just broke) so callers can fire
    a one-time "reconnect" email; ``False`` when it was already flagged or
    on error. The email_sends log is the durable "send once" guard; this
    boolean is a cheap early-out for the common already-broken case.
    """
    try:
        prior = fetch_one(
            "SELECT connection_broken_at FROM snaptrade_accounts "
            "WHERE user_id = %s AND snaptrade_account_id = %s",
            (user_id, snaptrade_account_id),
        )
        was_broken = bool(prior and prior.get("connection_broken_at"))
        execute(
            "UPDATE snaptrade_accounts "
            "SET connection_broken_at = COALESCE(connection_broken_at, NOW()), "
            "    updated_at = NOW() "
            "WHERE user_id = %s AND snaptrade_account_id = %s",
            (user_id, snaptrade_account_id),
        )
        return not was_broken
    except Exception as exc:
        _log.warning("mark_snaptrade_connection_broken failed: %s", exc)
        return False


def clear_snaptrade_connection_broken(user_id, snaptrade_account_id):
    """Clear the broken-connection flag after a successful sync."""
    try:
        execute(
            "UPDATE snaptrade_accounts SET connection_broken_at = NULL, updated_at = NOW() "
            "WHERE user_id = %s AND snaptrade_account_id = %s",
            (user_id, snaptrade_account_id),
        )
    except Exception as exc:
        _log.warning("clear_snaptrade_connection_broken failed: %s", exc)


def get_expired_snaptrade_accounts(user_id):
    """Rows with ``connection_broken_at`` set — drives the SnapTrade
    "Reconnect" banner. Shape mirrors ``get_expired_schwab_connections``
    so banner code can render both lists with the same template.
    """
    if user_id is None:
        return []
    try:
        return fetch_all(
            "SELECT snaptrade_account_id, broker_slug, account_name, display_nickname, "
            "connection_broken_at "
            "FROM snaptrade_accounts "
            "WHERE user_id = %s AND connection_broken_at IS NOT NULL "
            "ORDER BY created_at",
            (user_id,),
        )
    except Exception as exc:
        _log.warning("get_expired_snaptrade_accounts failed: %s", exc)
        return []


def remove_snaptrade_account(user_id, snaptrade_account_id):
    """Disconnect one SnapTrade-managed account. The Postgres user's
    SnapTrade userId/userSecret in ``snaptrade_connections`` is
    preserved so they can re-add accounts without re-registering."""
    execute(
        "DELETE FROM snaptrade_accounts WHERE user_id = %s AND snaptrade_account_id = %s",
        (user_id, snaptrade_account_id),
    )


def list_all_snaptrade_accounts():
    """Iterate every (user_id, snaptrade_account_id) pair across all
    users. Used by the cron CLI (``app.snaptrade_sync_cli``) — same
    pattern as ``get_all_schwab_connections`` for the Schwab cron.
    Returns the same column set as ``get_snaptrade_accounts`` plus
    ``user_id`` so the CLI knows whose secret to fetch."""
    return fetch_all(
        "SELECT user_id, id, snaptrade_account_id, broker_slug, "
        "account_number_masked, account_name, display_nickname, "
        "first_sync_completed, connection_broken_at "
        "FROM snaptrade_accounts ORDER BY user_id, created_at",
    )


def list_broken_snaptrade_connections():
    """Every SnapTrade account whose ``connection_broken_at`` is set, joined
    to its owner's email/username/unsubscribe token. Drives the recurring
    "still disconnected" reminder cron (``connection_reminder`` in
    ``app/email_digests_cli.py``). Postgres-only — no BigQuery needed.

    Excludes the demo user and rows with no deliverable email. Transactional
    (account-health), so NOT gated by a lifecycle opt-out column."""
    try:
        return fetch_all(
            "SELECT u.id AS user_id, u.username, u.email, "
            "p.email_unsubscribe_token, "
            "a.snaptrade_account_id, a.broker_slug, a.account_name, "
            "a.display_nickname, a.connection_broken_at "
            "FROM snaptrade_accounts a "
            "JOIN users u ON u.id = a.user_id "
            "LEFT JOIN user_profiles p ON p.user_id = u.id "
            "WHERE a.connection_broken_at IS NOT NULL "
            "AND u.email IS NOT NULL AND length(trim(u.email)) > 0 "
            "AND lower(u.username) <> 'demo' "
            "ORDER BY u.id, a.created_at",
        )
    except Exception as exc:
        _log.warning("list_broken_snaptrade_connections failed: %s", exc)
        return []


def get_snaptrade_account_nicknames(user_id):
    """``{account_name: display_label}`` for SnapTrade accounts. Mirrors
    ``get_account_nicknames`` (Schwab) so the request-scoped label
    filter can union the two."""
    if user_id is None:
        return {}
    try:
        rows = fetch_all(
            "SELECT account_name, display_nickname "
            "FROM snaptrade_accounts WHERE user_id = %s",
            (user_id,),
        )
    except Exception as exc:
        _log.warning("get_snaptrade_account_nicknames failed: %s", exc)
        return {}
    # COLLISION GUARD: account_name is NOT unique — SnapTrade returns the
    # generic "{Broker} Account" for several physical accounts (one user had
    # 5 "Schwab Account" rows with 5 different nicknames). A plain
    # {name: nick} dict would keep an arbitrary winner and every surface
    # rendering the raw warehouse label would show the WRONG nickname for
    # 4 of the 5 accounts. When a name maps to more than one distinct
    # nickname, the mapping is ambiguous — drop it and let the raw label
    # pass through. Tenant-addressed surfaces use _tenant_label_map_for_user
    # (keyed by tenant_id) and are unaffected.
    out = {}
    ambiguous = set()
    for r in rows:
        name = (r.get("account_name") or "").strip()
        if not name:
            continue
        nick = (r.get("display_nickname") or "").strip() or name
        if name in out and out[name] != nick:
            ambiguous.add(name)
        else:
            out[name] = nick
    for name in ambiguous:
        out.pop(name, None)
    return out


# ------------------------------------------------------------------
# Profiles & community (Postgres app tables)
# ------------------------------------------------------------------

_PROFILE_COLUMNS = (
    "user_id, display_name, headline, bio, accent, timezone, week_starts_monday, "
    "default_route, digest_email, weekly_preview_email, product_update_email, "
    "email_unsubscribe_token, compact_tables, show_account_names_on_published, "
    "profile_visibility, created_at, updated_at"
)


def _default_profile_row(user_id):
    """Safe defaults when user_profiles is missing or unreadable (e.g. prod before migration)."""
    return {
        "user_id": user_id,
        "display_name": None,
        "headline": None,
        "bio": None,
        "accent": "violet",
        "timezone": "America/New_York",
        "week_starts_monday": True,
        "default_route": "weekly_review",
        "digest_email": False,
        "weekly_preview_email": False,
        "product_update_email": True,
        "email_unsubscribe_token": None,
        "compact_tables": False,
        "show_account_names_on_published": False,
        "profile_visibility": "private",
        "created_at": None,
        "updated_at": None,
    }


def ensure_user_profile(user_id):
    """Create a default profile row if missing."""
    try:
        execute(
            """INSERT INTO user_profiles (user_id) VALUES (%s)
               ON CONFLICT (user_id) DO NOTHING""",
            (user_id,),
        )
    except Exception as exc:
        _log.warning(
            "ensure_user_profile failed (table missing or permissions? deploy init_db / migrations): %s",
            exc,
        )


def get_user_profile(user_id):
    """
    Return profile row dict. Never raises: if user_profiles is missing on a
    stale database, returns defaults so login and Weekly Review still work.
    """
    try:
        ensure_user_profile(user_id)
        row = fetch_one(
            f"SELECT {_PROFILE_COLUMNS} FROM user_profiles WHERE user_id = %s",
            (user_id,),
        )
        if row:
            return row
    except Exception as exc:
        _log.warning("get_user_profile failed (using defaults): %s", exc)
    return _default_profile_row(user_id)


def update_user_profile(user_id, **fields):
    """
    Whitelisted profile updates. Unknown keys are ignored.
    profile_visibility: private | followers | public
    Returns True if a write ran, False if nothing to do or DB error.
    """
    allowed = {
        "display_name",
        "headline",
        "bio",
        "accent",
        "timezone",
        "week_starts_monday",
        "default_route",
        "digest_email",
        "weekly_preview_email",
        "product_update_email",
        "compact_tables",
        "show_account_names_on_published",
        "profile_visibility",
    }
    sets = []
    values = []
    for key, val in fields.items():
        if key not in allowed:
            continue
        sets.append(f"{key} = %s")
        values.append(val)
    if not sets:
        return True
    sets.append("updated_at = NOW()")
    values.append(user_id)
    try:
        ensure_user_profile(user_id)
        execute(f"UPDATE user_profiles SET {', '.join(sets)} WHERE user_id = %s", tuple(values))
        return True
    except Exception as exc:
        _log.warning("update_user_profile failed: %s", exc)
        return False


# ------------------------------------------------------------------
# Email: idempotency log, opt-in recipients, unsubscribe tokens
#
# Email kinds (the ``kind`` column on email_sends and the opt-in column
# that gates each lifecycle send):
#   connection_dropped  → transactional (no opt-out)
#   weekly_summary      → user_profiles.digest_email
#   weekly_preview      → user_profiles.weekly_preview_email
#   reengagement        → user_profiles.product_update_email
# ------------------------------------------------------------------

# Maps a lifecycle email kind to the user_profiles boolean that gates it.
_EMAIL_OPT_IN_COLUMN = {
    "weekly_summary": "digest_email",
    "weekly_preview": "weekly_preview_email",
    "reengagement": "product_update_email",
}


def record_email_send(kind, dedupe_key, *, user_id=None, to_email=None):
    """Append-once log row. Returns ``True`` if THIS call inserted the row
    (caller should send the email) and ``False`` if a row for
    ``(kind, dedupe_key)`` already existed (skip — already sent) or on a
    DB error (skip — never risk spamming on a broken log).

    The dedupe_key should encode whatever makes the send unique: e.g.
    ``"<snaptrade_account_id>:<broken_at_iso>"`` for a reconnect notice or
    ``"<account>:<week_start>"`` for a weekly digest.
    """
    try:
        row = execute_returning(
            "INSERT INTO email_sends (user_id, kind, dedupe_key, to_email) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (kind, dedupe_key) DO NOTHING "
            "RETURNING id",
            (user_id, kind, dedupe_key, to_email),
        )
        return row is not None
    except Exception as exc:
        _log.warning("record_email_send(%s) failed: %s", kind, exc)
        return False


# Higher number = stronger block. A later weaker event must not downgrade a
# stronger one (a complaint after a hard bounce keeps the hard bounce).
_SUPPRESSION_SEVERITY = {"complaint": 1, "manual": 2, "invalid": 3, "hard_bounce": 3}


def _normalize_email(email):
    return (email or "").strip().lower()


def add_email_suppression(email, reason, *, detail=None):
    """Add/refresh a suppression row. ``reason`` is one of complaint,
    manual, invalid, hard_bounce. Never downgrades a stronger existing
    block. Returns True on success."""
    em = _normalize_email(email)
    if not em:
        return False
    reason = (reason or "manual").strip().lower()
    try:
        existing = fetch_one("SELECT reason FROM email_suppressions WHERE email = %s", (em,))
        if existing:
            old = (existing.get("reason") or "").lower()
            if _SUPPRESSION_SEVERITY.get(reason, 0) < _SUPPRESSION_SEVERITY.get(old, 0):
                execute("UPDATE email_suppressions SET updated_at = NOW() WHERE email = %s", (em,))
                return True
        execute(
            "INSERT INTO email_suppressions (email, reason, detail) VALUES (%s, %s, %s) "
            "ON CONFLICT (email) DO UPDATE SET reason = EXCLUDED.reason, "
            "detail = EXCLUDED.detail, updated_at = NOW()",
            (em, reason, detail),
        )
        return True
    except Exception as exc:
        _log.warning("add_email_suppression failed: %s", exc)
        return False


def get_email_suppression(email):
    """Return the suppression reason for an address, or None if not
    suppressed. Cheap enough to call before every send."""
    em = _normalize_email(email)
    if not em:
        return None
    try:
        row = fetch_one("SELECT reason FROM email_suppressions WHERE email = %s", (em,))
        return (row or {}).get("reason")
    except Exception as exc:
        _log.warning("get_email_suppression failed: %s", exc)
        return None


def remove_email_suppression(email):
    """Remove a suppression (e.g. operator re-enabling a fixed address)."""
    em = _normalize_email(email)
    if not em:
        return False
    try:
        execute("DELETE FROM email_suppressions WHERE email = %s", (em,))
        return True
    except Exception as exc:
        _log.warning("remove_email_suppression failed: %s", exc)
        return False


def get_or_create_email_unsubscribe_token(user_id):
    """Return the user's stable one-click unsubscribe token, minting one
    on first use. Used to build the List-Unsubscribe link for lifecycle
    email. Returns None on error (caller can still send without a footer
    link, but should prefer to skip)."""
    if user_id is None:
        return None
    try:
        ensure_user_profile(user_id)
        row = fetch_one(
            "SELECT email_unsubscribe_token FROM user_profiles WHERE user_id = %s",
            (user_id,),
        )
        existing = (row or {}).get("email_unsubscribe_token")
        if existing:
            return existing
        token = secrets.token_urlsafe(32)
        execute(
            "UPDATE user_profiles SET email_unsubscribe_token = %s, updated_at = NOW() "
            "WHERE user_id = %s AND email_unsubscribe_token IS NULL",
            (token, user_id),
        )
        # Re-read in case a concurrent request won the race.
        row = fetch_one(
            "SELECT email_unsubscribe_token FROM user_profiles WHERE user_id = %s",
            (user_id,),
        )
        return (row or {}).get("email_unsubscribe_token") or token
    except Exception as exc:
        _log.warning("get_or_create_email_unsubscribe_token failed: %s", exc)
        return None


def unsubscribe_user_by_token(token):
    """One-click unsubscribe from ALL lifecycle email. Idempotent. Returns
    the username on success (for a friendly confirmation page) or None if
    the token doesn't match anyone."""
    if not token:
        return None
    try:
        row = execute_returning(
            "UPDATE user_profiles SET digest_email = FALSE, "
            "weekly_preview_email = FALSE, product_update_email = FALSE, "
            "updated_at = NOW() WHERE email_unsubscribe_token = %s "
            "RETURNING user_id",
            (token,),
        )
        if not row:
            return None
        uid = row.get("user_id")
        urow = fetch_one("SELECT username FROM users WHERE id = %s", (uid,))
        return (urow or {}).get("username") or "your account"
    except Exception as exc:
        _log.warning("unsubscribe_user_by_token failed: %s", exc)
        return None


def list_email_recipients_for_kind(kind):
    """Users opted into a lifecycle email ``kind`` who have a deliverable
    address. Returns dicts with: user_id, username, email,
    email_unsubscribe_token, timezone.

    Excludes the shared demo account. Raises on an unknown kind so a typo
    in a cron never silently blasts everyone.
    """
    col = _EMAIL_OPT_IN_COLUMN.get(kind)
    if col is None:
        raise ValueError(f"Unknown lifecycle email kind: {kind!r}")
    rows = fetch_all(
        f"SELECT u.id AS user_id, u.username, u.email, "
        f"p.email_unsubscribe_token, p.timezone "
        f"FROM users u JOIN user_profiles p ON p.user_id = u.id "
        f"WHERE p.{col} = TRUE "
        f"AND u.email IS NOT NULL AND length(trim(u.email)) > 0 "
        f"AND lower(u.username) <> 'demo' "
        f"ORDER BY u.id",
    )
    return rows or []


def list_dormant_email_recipients(min_days_away, max_days_away):
    """Users who last opened the app between ``min_days_away`` and
    ``max_days_away`` days ago (a window, so a daily cron doesn't re-nudge
    the same person every day) and are opted into product-update email.

    Uses ``user_review_visits.last_visit_at`` (set by ``bump_review_visit``
    on every Daily Review load) as the activity signal. Returns the same
    shape as ``list_email_recipients_for_kind`` plus ``days_away``.
    """
    rows = fetch_all(
        "SELECT u.id AS user_id, u.username, u.email, "
        "p.email_unsubscribe_token, p.timezone, v.last_visit_at, "
        "EXTRACT(DAY FROM (NOW() - v.last_visit_at))::int AS days_away "
        "FROM users u "
        "JOIN user_profiles p ON p.user_id = u.id "
        "JOIN user_review_visits v ON v.user_id = u.id "
        "WHERE p.product_update_email = TRUE "
        "AND u.email IS NOT NULL AND length(trim(u.email)) > 0 "
        "AND lower(u.username) <> 'demo' "
        "AND v.last_visit_at <= NOW() - (%s || ' days')::interval "
        "AND v.last_visit_at >  NOW() - (%s || ' days')::interval "
        "ORDER BY u.id",
        (int(min_days_away), int(max_days_away)),
    )
    return rows or []


# ------------------------------------------------------------------
# Review visit anchors  ("Since you last looked")
#
# We track two timestamps per user:
#   prev_visit_at  → the visit BEFORE the current one (the one we diff against)
#   last_visit_at  → the most recent visit (becomes prev on the next "real" visit)
#
# A "real" visit is one separated from last_visit_at by at least
# REVIEW_VISIT_PROMOTE_GAP — otherwise rapid reloads would clobber the prev
# anchor and make the diff strip useless.
# ------------------------------------------------------------------
from datetime import timedelta as _timedelta

REVIEW_VISIT_PROMOTE_GAP = _timedelta(minutes=30)


def get_review_visit(user_id):
    """Return {'last_visit_at': dt, 'prev_visit_at': dt} or None if never visited."""
    try:
        row = fetch_one(
            "SELECT last_visit_at, prev_visit_at FROM user_review_visits WHERE user_id = %s",
            (user_id,),
        )
        return row
    except Exception as exc:
        _log.warning("get_review_visit failed: %s", exc)
        return None


def bump_review_visit(user_id, now):
    """
    Record a weekly-review visit. Debounced: if the prior last_visit_at is
    within REVIEW_VISIT_PROMOTE_GAP, last_visit_at is NOT moved — that way a
    burst of reloads doesn't reset the "since you last looked" anchor and
    flatten the diff to nothing.

    On a non-debounced visit, prior last_visit_at is rotated to prev_visit_at.

    Returns the row state BEFORE the bump, so the route can use
    prior['last_visit_at'] as the anchor to diff against.
    """
    prior = get_review_visit(user_id)
    try:
        if prior is None:
            execute(
                "INSERT INTO user_review_visits (user_id, last_visit_at, prev_visit_at) "
                "VALUES (%s, %s, NULL) "
                "ON CONFLICT (user_id) DO UPDATE SET last_visit_at = EXCLUDED.last_visit_at",
                (user_id, now),
            )
        else:
            last = prior.get("last_visit_at")
            if last is None or (now - last) >= REVIEW_VISIT_PROMOTE_GAP:
                execute(
                    "UPDATE user_review_visits "
                    "SET prev_visit_at = last_visit_at, last_visit_at = %s "
                    "WHERE user_id = %s",
                    (now, user_id),
                )
            # else: debounced reload, leave last_visit_at alone
    except Exception as exc:
        _log.warning("bump_review_visit failed: %s", exc)
    return prior


def get_user_by_username(username):
    return fetch_one("SELECT id, username FROM users WHERE lower(username) = lower(%s)", (username,))


def follow_user(follower_id, following_id):
    if follower_id == following_id:
        return False
    try:
        execute(
            """INSERT INTO user_follows (follower_id, following_id) VALUES (%s, %s)
               ON CONFLICT DO NOTHING""",
            (follower_id, following_id),
        )
        return True
    except Exception as exc:
        _log.warning("follow_user failed: %s", exc)
        return False


def unfollow_user(follower_id, following_id):
    try:
        execute(
            "DELETE FROM user_follows WHERE follower_id = %s AND following_id = %s",
            (follower_id, following_id),
        )
    except Exception as exc:
        _log.warning("unfollow_user failed: %s", exc)


def is_following(follower_id, following_id):
    try:
        row = fetch_one(
            "SELECT 1 FROM user_follows WHERE follower_id = %s AND following_id = %s",
            (follower_id, following_id),
        )
        return row is not None
    except Exception as exc:
        _log.warning("is_following failed: %s", exc)
        return False


def follow_counts(user_id):
    try:
        followers = fetch_one(
            "SELECT COUNT(*) AS c FROM user_follows WHERE following_id = %s", (user_id,)
        )
        following = fetch_one(
            "SELECT COUNT(*) AS c FROM user_follows WHERE follower_id = %s", (user_id,)
        )
        return int(followers["c"] or 0), int(following["c"] or 0)
    except Exception as exc:
        _log.warning("follow_counts failed: %s", exc)
        return 0, 0


def list_following_ids(follower_id):
    try:
        rows = fetch_all(
            "SELECT following_id FROM user_follows WHERE follower_id = %s ORDER BY created_at DESC",
            (follower_id,),
        )
        return [int(r["following_id"]) for r in rows]
    except Exception as exc:
        _log.warning("list_following_ids failed: %s", exc)
        return []


def get_published_trade_fingerprints(user_id):
    try:
        rows = fetch_all(
            "SELECT trade_fingerprint FROM community_published_trades WHERE user_id = %s",
            (user_id,),
        )
        return {r["trade_fingerprint"] for r in rows}
    except Exception as exc:
        _log.warning("get_published_trade_fingerprints failed: %s", exc)
        return set()


def count_published_trades(user_id):
    try:
        row = fetch_one(
            "SELECT COUNT(*) AS c FROM community_published_trades WHERE user_id = %s",
            (user_id,),
        )
        return int(row["c"] or 0) if row else 0
    except Exception as exc:
        _log.warning("count_published_trades failed: %s", exc)
        return 0


def publish_community_trade(
    user_id,
    fingerprint,
    account_name,
    symbol,
    strategy,
    trade_symbol,
    open_date,
    close_date,
    status,
    display_pnl,
    caption=None,
):
    """Insert or refresh a published trade snapshot for the community feed."""
    try:
        execute(
            """INSERT INTO community_published_trades
               (user_id, trade_fingerprint, account_name, symbol, strategy, trade_symbol,
                open_date, close_date, status, display_pnl, caption)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (user_id, trade_fingerprint) DO UPDATE SET
                 account_name = EXCLUDED.account_name,
                 symbol = EXCLUDED.symbol,
                 strategy = EXCLUDED.strategy,
                 trade_symbol = EXCLUDED.trade_symbol,
                 open_date = EXCLUDED.open_date,
                 close_date = EXCLUDED.close_date,
                 status = EXCLUDED.status,
                 display_pnl = EXCLUDED.display_pnl,
                 caption = EXCLUDED.caption,
                 published_at = NOW()""",
            (
                user_id,
                fingerprint,
                account_name,
                symbol,
                strategy,
                trade_symbol or "",
                open_date,
                close_date or "",
                status,
                display_pnl,
                caption,
            ),
        )
        return True
    except Exception as exc:
        _log.warning("publish_community_trade failed: %s", exc)
        return False


def unpublish_community_trade(user_id, fingerprint):
    try:
        execute(
            "DELETE FROM community_published_trades WHERE user_id = %s AND trade_fingerprint = %s",
            (user_id, fingerprint),
        )
        return True
    except Exception as exc:
        _log.warning("unpublish_community_trade failed: %s", exc)
        return False


def community_feed_for_follower(viewer_id, limit=50):
    """Recent published trades from people the viewer follows."""
    try:
        return fetch_all(
            """SELECT t.id, t.user_id, t.symbol, t.strategy, t.trade_symbol, t.open_date, t.close_date,
                      t.status, t.display_pnl, t.caption, t.published_at, t.account_name,
                      u.username,
                      COALESCE(NULLIF(TRIM(p.display_name), ''), u.username) AS author_display
               FROM community_published_trades t
               JOIN user_follows f ON f.following_id = t.user_id AND f.follower_id = %s
               JOIN users u ON u.id = t.user_id
               LEFT JOIN user_profiles p ON p.user_id = t.user_id
               ORDER BY t.published_at DESC
               LIMIT %s""",
            (viewer_id, limit),
        )
    except Exception as exc:
        _log.warning("community_feed_for_follower failed: %s", exc)
        return []


def list_public_published_trades(target_user_id, limit=100):
    try:
        return fetch_all(
            """SELECT trade_fingerprint, symbol, strategy, trade_symbol, open_date, close_date,
                      status, display_pnl, caption, published_at, account_name
               FROM community_published_trades
               WHERE user_id = %s
               ORDER BY published_at DESC
               LIMIT %s""",
            (target_user_id, limit),
        )
    except Exception as exc:
        _log.warning("list_public_published_trades failed: %s", exc)
        return []


# ------------------------------------------------------------------
# Community posts (blog-style feed, optionally tied to a symbol)
# ------------------------------------------------------------------

_MAX_POST_BODY_LEN = 4000
_MAX_POST_SYMBOL_LEN = 32
_MAX_POST_STRATEGY_LEN = 64
_MAX_ATTACHMENT_JSON_LEN = 4000
_ALLOWED_POST_VISIBILITY = frozenset({"private", "followers", "public"})
_ALLOWED_ATTACHMENT_KINDS = frozenset({"leg", "strategy", "transaction"})


def create_post(
    user_id,
    body,
    symbol=None,
    strategy=None,
    visibility="followers",
    attached_fingerprint=None,
    attachment_kind=None,
    attachment_json=None,
):
    """
    Insert a new community post. Caller is responsible for having confirmed the
    author identity (flask-login). Returns the new row id, or None on failure.
    """
    clean_body = (body or "").strip()
    if not clean_body:
        return None
    if len(clean_body) > _MAX_POST_BODY_LEN:
        clean_body = clean_body[:_MAX_POST_BODY_LEN]
    clean_symbol = (symbol or "").strip().upper() or None
    if clean_symbol and len(clean_symbol) > _MAX_POST_SYMBOL_LEN:
        clean_symbol = clean_symbol[:_MAX_POST_SYMBOL_LEN]
    clean_strategy = (strategy or "").strip() or None
    if clean_strategy and len(clean_strategy) > _MAX_POST_STRATEGY_LEN:
        clean_strategy = clean_strategy[:_MAX_POST_STRATEGY_LEN]
    vis = (visibility or "followers").strip().lower()
    if vis not in _ALLOWED_POST_VISIBILITY:
        vis = "followers"
    af = (attached_fingerprint or "").strip() or None

    ak = (attachment_kind or "").strip().lower() or None
    if ak not in _ALLOWED_ATTACHMENT_KINDS:
        ak = None
    aj = (attachment_json or "").strip() or None
    if aj and len(aj) > _MAX_ATTACHMENT_JSON_LEN:
        aj = None
    if aj:
        # Defense in depth: only accept strict JSON objects.
        import json as _json
        try:
            parsed = _json.loads(aj)
            if not isinstance(parsed, dict):
                aj = None
        except Exception:
            aj = None
    if not ak:
        aj = None
    try:
        row = execute_returning(
            """INSERT INTO community_posts
               (user_id, body, symbol, strategy, attached_fingerprint,
                attachment_kind, attachment_json, visibility)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING id""",
            (user_id, clean_body, clean_symbol, clean_strategy, af, ak, aj, vis),
        )
        return int(row["id"]) if row else None
    except Exception as exc:
        _log.warning("create_post failed: %s", exc)
        return None


def delete_post(user_id, post_id):
    try:
        execute(
            "DELETE FROM community_posts WHERE id = %s AND user_id = %s",
            (post_id, user_id),
        )
        return True
    except Exception as exc:
        _log.warning("delete_post failed: %s", exc)
        return False


def update_post_visibility(user_id, post_id, visibility):
    vis = (visibility or "").strip().lower()
    if vis not in _ALLOWED_POST_VISIBILITY:
        return False
    try:
        execute(
            "UPDATE community_posts SET visibility = %s, updated_at = NOW() "
            "WHERE id = %s AND user_id = %s",
            (vis, post_id, user_id),
        )
        return True
    except Exception as exc:
        _log.warning("update_post_visibility failed: %s", exc)
        return False


_POST_SELECT_BASE = """
    SELECT p.id, p.user_id, p.body, p.symbol, p.strategy, p.attached_fingerprint,
           p.attachment_kind, p.attachment_json,
           p.visibility, p.created_at, p.updated_at,
           u.username,
           COALESCE(NULLIF(TRIM(pr.display_name), ''), u.username) AS author_display,
           pr.headline AS author_headline,
           t.strategy    AS trade_strategy,
           t.trade_symbol AS trade_symbol,
           t.status      AS trade_status,
           t.display_pnl AS trade_display_pnl,
           t.open_date   AS trade_open_date,
           t.close_date  AS trade_close_date
    FROM community_posts p
    JOIN users u ON u.id = p.user_id
    LEFT JOIN user_profiles pr ON pr.user_id = p.user_id
    LEFT JOIN community_published_trades t
        ON t.user_id = p.user_id AND t.trade_fingerprint = p.attached_fingerprint
"""


def list_posts_by_user(author_id, viewer_id, limit=60):
    """
    Return posts by ``author_id`` that ``viewer_id`` is allowed to see.

    - Private: only the author.
    - Followers: author + anyone following the author.
    - Public: everyone.
    """
    try:
        if author_id == viewer_id:
            sql = (
                _POST_SELECT_BASE
                + " WHERE p.user_id = %s ORDER BY p.created_at DESC LIMIT %s"
            )
            return fetch_all(sql, (author_id, limit))
        sql = (
            _POST_SELECT_BASE
            + """ WHERE p.user_id = %s
                   AND (
                        p.visibility = 'public'
                        OR (p.visibility = 'followers' AND EXISTS (
                             SELECT 1 FROM user_follows f
                             WHERE f.follower_id = %s AND f.following_id = p.user_id
                        ))
                   )
                   ORDER BY p.created_at DESC
                   LIMIT %s"""
        )
        return fetch_all(sql, (author_id, viewer_id, limit))
    except Exception as exc:
        _log.warning("list_posts_by_user failed: %s", exc)
        return []


def community_feed(viewer_id, limit=60):
    """
    Main feed: posts from people viewer follows (visibility followers or public)
    plus viewer's own posts, newest first.
    """
    try:
        sql = (
            _POST_SELECT_BASE
            + """ WHERE (
                    p.user_id = %s
                    OR (
                        p.user_id IN (
                            SELECT following_id FROM user_follows WHERE follower_id = %s
                        )
                        AND p.visibility IN ('followers', 'public')
                    )
                 )
                 ORDER BY p.created_at DESC
                 LIMIT %s"""
        )
        return fetch_all(sql, (viewer_id, viewer_id, limit))
    except Exception as exc:
        _log.warning("community_feed failed: %s", exc)
        return []


def discover_recent_public_posts(viewer_id, limit=30):
    """Public posts from users the viewer does not already follow (for discovery)."""
    try:
        sql = (
            _POST_SELECT_BASE
            + """ WHERE p.visibility = 'public'
                   AND p.user_id <> %s
                   AND p.user_id NOT IN (
                        SELECT following_id FROM user_follows WHERE follower_id = %s
                   )
                 ORDER BY p.created_at DESC
                 LIMIT %s"""
        )
        return fetch_all(sql, (viewer_id, viewer_id, limit))
    except Exception as exc:
        _log.warning("discover_recent_public_posts failed: %s", exc)
        return []


def get_post(post_id):
    try:
        return fetch_one(
            _POST_SELECT_BASE + " WHERE p.id = %s",
            (post_id,),
        )
    except Exception as exc:
        _log.warning("get_post failed: %s", exc)
        return None


def decode_post_attachments(posts):
    """Parse attachment_json into an 'attachment' dict on each post for templates."""
    if not posts:
        return posts
    import json as _json
    for p in posts:
        if not isinstance(p, dict):
            continue
        kind = p.get("attachment_kind")
        raw = p.get("attachment_json")
        if not kind or not raw:
            p["attachment"] = None
            continue
        try:
            data = _json.loads(raw)
            if isinstance(data, dict):
                data.setdefault("kind", kind)
                p["attachment"] = data
            else:
                p["attachment"] = None
        except Exception:
            p["attachment"] = None
    return posts


def discover_public_traders(limit=24):
    """Users who allow public or follower discovery (not private-only)."""
    try:
        return fetch_all(
            """SELECT u.id, u.username,
                      COALESCE(NULLIF(TRIM(p.display_name), ''), u.username) AS display_name,
                      p.headline, p.profile_visibility
               FROM users u
               JOIN user_profiles p ON p.user_id = u.id
               WHERE p.profile_visibility IN ('public', 'followers')
               ORDER BY u.username
               LIMIT %s""",
            (limit,),
        )
    except Exception as exc:
        _log.warning("discover_public_traders failed: %s", exc)
        return []


def _ilike_substring_param(q: str) -> str:
    """Build a %...% pattern for ILIKE; escape backslash, %, and _ in user input."""
    q = (q or "").strip()[:200]
    for a, b in (("\\", "\\\\"), ("%", r"\%"), ("_", r"\_")):
        q = q.replace(a, b)
    return f"%{q}%"


def search_discoverable_traders(exclude_user_id, q, limit=40):
    """
    Substring search (username, display name, headline, bio) among users
    with profile visibility public or followers. Excludes exclude_user_id.
    Returns [] if q is shorter than 2 characters after strip.
    """
    raw = (q or "").strip()
    if len(raw) < 2:
        return []
    pat = _ilike_substring_param(raw)
    try:
        return fetch_all(
            """SELECT u.id, u.username,
                      COALESCE(NULLIF(TRIM(p.display_name), ''), u.username) AS display_name,
                      p.headline, p.profile_visibility
               FROM users u
               JOIN user_profiles p ON p.user_id = u.id
               WHERE p.profile_visibility IN ('public', 'followers')
                 AND u.id != %s
                 AND (
                      u.username ILIKE %s ESCAPE E'\\'
                   OR TRIM(COALESCE(p.display_name, '')) ILIKE %s ESCAPE E'\\'
                   OR TRIM(COALESCE(p.headline, '')) ILIKE %s ESCAPE E'\\'
                   OR TRIM(COALESCE(p.bio, '')) ILIKE %s ESCAPE E'\\'
                 )
               ORDER BY u.username
               LIMIT %s""",
            (exclude_user_id, pat, pat, pat, pat, limit),
        )
    except Exception as exc:
        _log.warning("search_discoverable_traders failed: %s", exc)
        return []


# ------------------------------------------------------------------
# Admin / bootstrap helpers
# ------------------------------------------------------------------

def is_admin(username):
    """Check if a username is in the ADMIN_USERS environment variable."""
    admin_env = os.environ.get("ADMIN_USERS", "")
    if not admin_env:
        return False
    admins = {u.strip().lower() for u in admin_env.split(",") if u.strip()}
    return username.lower() in admins


def seed_users_from_env():
    """
    Auto-create users from the HAPPYTRADER_USERS environment variable.

    Format:  username:password,username2:password2

    Existing users are skipped (not overwritten). Intended only for bootstrap;
    once you have real persistence, prefer the ``flask create-user`` CLI and
    remove HAPPYTRADER_USERS so plaintext passwords don't sit in env vars.
    """
    users_env = os.environ.get("HAPPYTRADER_USERS", "")
    if not users_env:
        return

    for entry in users_env.split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        username, password = entry.split(":", 1)
        username = username.strip()
        password = password.strip()
        if not username or not password:
            continue
        if User.get_by_username(username) is None:
            User.create(username, password)


DEMO_ACCOUNT = "Demo Account"


def ensure_demo_user():
    """
    Create the demo user and link to the Demo Account if not already set up.
    Demo credentials: demo / demo123
    """
    demo = User.get_by_username("demo")
    if demo is None:
        User.create("demo", "demo123")
        demo = User.get_by_username("demo")
    if demo:
        remove_account_for_user(demo.id, "Testing Account")  # migrate from old demo setup
        # Sharing labels across users is allowed (see
        # docs/USER_ID_TENANCY.md), so this just upserts.
        add_account_for_user(demo.id, DEMO_ACCOUNT)
        ensure_user_profile(demo.id)
        _ensure_demo_insight(demo.id)
        _seed_demo_mirror_scores(demo.id)


def _ensure_demo_insight(demo_user_id):
    """Seed a pre-generated insight for the demo user so it's ready on first visit."""
    if get_insight_for_user(demo_user_id):
        return  # already has one
    summary = (
        "Years of consistent options trading across Covered Calls, CSPs, Wheels, and PMCC. "
        "Account growth, strong win rates, and disciplined execution. Your Mirror Score trend "
        "shows real progress—this is what a mature, intentional options trader looks like."
    )
    full_analysis = """## Summary

You've built a track record over multiple years: diversified options strategies, steady premium income, and clear improvement in discipline and alignment with your plan. Your data shows wins and losses, assignments and expirations, and a portfolio that has grown while you've refined your approach.

## Trading Style Overview

You trade like someone who's been at this for years. Covered Calls and Cash-Secured Puts on quality names (AAPL, NVDA, META, GOOGL, COST, SPY). You run the Wheel when assignment makes sense, and you've added Poor Man's Covered Call (PMCC) on names like PLTR. You mix income with occasional directional plays (long calls/puts) and keep position sizing in the picture.

## What's Working

- **Strategy variety** — CSPs, Covered Calls, Wheels, PMCC, and selective directional trades. You're not stuck in one playbook.
- **Mirror Score trend** — Your discipline and intent scores have trended up over time. That's the kind of progress that separates long-term traders from one-off gamblers.
- **Premium and assignments** — You collect premium, take assignment when it fits the plan, and close or roll with intention.

## What This Demo Shows

This profile is built to show what the platform looks like when it's full: weekly review with real numbers, Mirror Score history, strategy breakdowns, and AI Insights. Every section is populated so you can see the full experience.

## Next Steps for You

1. **Upload your own data** — Replace this demo with your real accounts and watch your own trends.
2. **Use AI Insights** — Ask questions about your trades; the AI reads only your data and surfaces patterns, not advice.
3. **Track over time** — The more you upload, the more accurate your snapshots and Mirror Score become."""
    save_insight(demo_user_id, summary, full_analysis)


def _seed_demo_mirror_scores(demo_user_id):
    """Seed demo user with many weeks of Mirror Score history (improving trend)."""
    if get_mirror_score_history(demo_user_id, limit=1):
        return  # already has scores
    from datetime import datetime, timedelta
    start = datetime(2024, 6, 3).date()
    weeks = []
    for i in range(24):
        week_start = start + timedelta(weeks=i)
        weeks.append(week_start.strftime("%Y-%m-%d"))
    # Scores improve over time: 62 -> 88
    for i, ws in enumerate(weeks):
        t = i / max(len(weeks) - 1, 1)
        mirror = round(62 + 26 * t + (i % 3) * 0.5, 1)
        discipline = round(60 + 25 * t, 1)
        intent_ = round(65 + 20 * t, 1)
        risk_ = round(64 + 22 * t, 1)
        consistency_ = round(58 + 28 * t, 1)
        level = "High" if mirror >= 80 else "Medium" if mirror >= 70 else "Building"
        sentence = (
            "Strong alignment with plan; sizing and execution consistent."
            if mirror >= 78 else
            "Good week; keep tracking and sizing positions."
        )
        save_mirror_score(
            demo_user_id, ws,
            discipline, intent_, risk_, consistency_, mirror,
            level, sentence,
        )


# ------------------------------------------------------------------
# Password reset tokens (self-serve recovery)
# ------------------------------------------------------------------
#
# Flow:
# 1. /forgot-password POST → mint a token (raw 32-byte URL-safe string).
#    Email it as ?token=<raw>; persist only sha256(raw). A DB leak does
#    not let an attacker forge reset URLs.
# 2. User clicks /reset-password/<raw> → look up by sha256(raw); validate
#    not used and not expired (1-hour default).
# 3. POST sets the new password and marks token used. Token is
#    single-use; revisiting the same link returns "expired".
#
# Rate limits live on the routes (auth.py).

import hashlib as _hashlib
import secrets as _secrets
from datetime import datetime as _dt, timedelta as _td, timezone as _tz


PASSWORD_RESET_TOKEN_TTL = _td(hours=1)


def _hash_reset_token(raw_token: str) -> str:
    """sha256 hex of the URL-safe raw token. Stored in DB."""
    return _hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def mint_password_reset_token(user_id: int, requester_ip: str | None = None) -> str:
    """Create a single-use reset token and return the raw value to email
    to the user. Existing unused tokens for the same user are invalidated
    so an old email link can't override the most recent request."""
    raw = _secrets.token_urlsafe(32)
    token_hash = _hash_reset_token(raw)
    expires_at = _dt.now(_tz.utc) + PASSWORD_RESET_TOKEN_TTL
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Revoke any older live tokens for this user. The path uses
            # the partial index idx_password_reset_user_active.
            cur.execute(
                "UPDATE password_reset_tokens SET used_at = NOW() "
                "WHERE user_id = %s AND used_at IS NULL",
                (user_id,),
            )
            cur.execute(
                """INSERT INTO password_reset_tokens
                   (user_id, token_hash, expires_at, requester_ip)
                   VALUES (%s, %s, %s, %s)""",
                (user_id, token_hash, expires_at, requester_ip),
            )
    return raw


def consume_password_reset_token(raw_token: str) -> int | None:
    """Validate and atomically consume a token. Returns the owning
    user_id on success, or None if the token is unknown, expired, or
    already used. Single transaction so two clients with the same link
    can't both succeed."""
    if not raw_token:
        return None
    token_hash = _hash_reset_token(raw_token)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, user_id, expires_at, used_at
                   FROM password_reset_tokens
                   WHERE token_hash = %s
                   FOR UPDATE""",
                (token_hash,),
            )
            row = cur.fetchone()
            if not row:
                return None
            if row["used_at"] is not None:
                return None
            expires_at = row["expires_at"]
            now = _dt.now(_tz.utc)
            # psycopg may give a naive or aware datetime; compare as aware.
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=_tz.utc)
            if now > expires_at:
                return None
            cur.execute(
                "UPDATE password_reset_tokens SET used_at = NOW() WHERE id = %s",
                (row["id"],),
            )
            return int(row["user_id"])


# ------------------------------------------------------------------
# Email verification (single-use token; mirrors password reset)
# ------------------------------------------------------------------

EMAIL_VERIFICATION_TOKEN_TTL = _td(days=7)


def mint_email_verification_token(user_id: int) -> str:
    """Create a single-use email-verification token, returning the raw
    value to email. Invalidates older unused tokens for the user."""
    raw = _secrets.token_urlsafe(32)
    token_hash = _hash_reset_token(raw)
    expires_at = _dt.now(_tz.utc) + EMAIL_VERIFICATION_TOKEN_TTL
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE email_verification_tokens SET used_at = NOW() "
                "WHERE user_id = %s AND used_at IS NULL",
                (user_id,),
            )
            cur.execute(
                """INSERT INTO email_verification_tokens
                   (user_id, token_hash, expires_at)
                   VALUES (%s, %s, %s)""",
                (user_id, token_hash, expires_at),
            )
    return raw


def consume_email_verification_token(raw_token: str) -> int | None:
    """Validate + consume a verification token and stamp the user's
    ``email_verified_at``. Returns the user_id on success, else None.
    Single transaction so concurrent clicks can't double-process."""
    if not raw_token:
        return None
    token_hash = _hash_reset_token(raw_token)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, user_id, expires_at, used_at
                   FROM email_verification_tokens
                   WHERE token_hash = %s
                   FOR UPDATE""",
                (token_hash,),
            )
            row = cur.fetchone()
            if not row or row["used_at"] is not None:
                return None
            expires_at = row["expires_at"]
            now = _dt.now(_tz.utc)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=_tz.utc)
            if now > expires_at:
                return None
            cur.execute(
                "UPDATE email_verification_tokens SET used_at = NOW() WHERE id = %s",
                (row["id"],),
            )
            cur.execute(
                "UPDATE users SET email_verified_at = COALESCE(email_verified_at, NOW()) "
                "WHERE id = %s",
                (row["user_id"],),
            )
            return int(row["user_id"])


def mark_email_verified(user_id: int) -> None:
    """Force-mark a user's email verified (admin / CLI use)."""
    try:
        execute(
            "UPDATE users SET email_verified_at = COALESCE(email_verified_at, NOW()) "
            "WHERE id = %s",
            (user_id,),
        )
    except Exception as exc:
        _log.warning("mark_email_verified failed: %s", exc)


def email_needs_verification(user_id: int) -> bool:
    """True when the user has an email on file that hasn't been confirmed.
    Drives the 'please verify your email' banner + the resend route. Never
    raises (returns False) so a stale DB can't break page render."""
    if user_id is None:
        return False
    try:
        row = fetch_one(
            "SELECT email, email_verified_at FROM users WHERE id = %s",
            (user_id,),
        )
    except Exception as exc:
        _log.warning("email_needs_verification failed: %s", exc)
        return False
    if not row:
        return False
    has_email = bool((row.get("email") or "").strip())
    return has_email and row.get("email_verified_at") is None


# ------------------------------------------------------------------
# Feedback inbox (footer Send-Feedback button)
# ------------------------------------------------------------------


_MAX_FEEDBACK_LEN = 4000


def save_feedback(
    *,
    user_id: int | None,
    username: str | None,
    body: str,
    page_path: str | None,
    user_agent: str | None,
    ip_address: str | None,
) -> int | None:
    """Persist a feedback message. Returns the new row id, or None on
    failure (DB unavailable, body empty, etc.). The body is truncated to
    4 KB so a runaway form post can't blow up the table."""
    clean_body = (body or "").strip()
    if not clean_body:
        return None
    if len(clean_body) > _MAX_FEEDBACK_LEN:
        clean_body = clean_body[:_MAX_FEEDBACK_LEN]
    try:
        row = execute_returning(
            """INSERT INTO feedback
               (user_id, username, body, page_path, user_agent, ip_address)
               VALUES (%s, %s, %s, %s, %s, %s)
               RETURNING id""",
            (
                user_id,
                (username or None),
                clean_body,
                (page_path or None),
                (user_agent or None),
                (ip_address or None),
            ),
        )
        return int(row["id"]) if row else None
    except Exception as exc:
        _log.warning("save_feedback failed: %s", exc)
        return None


def list_feedback(*, only_unresolved: bool = False, limit: int = 100) -> list[dict]:
    """Newest-first list for /admin/feedback."""
    where = "WHERE resolved_at IS NULL " if only_unresolved else ""
    try:
        return fetch_all(
            f"""SELECT id, user_id, username, body, page_path, user_agent,
                       ip_address, created_at, resolved_at
                FROM feedback
                {where}
                ORDER BY created_at DESC
                LIMIT %s""",
            (limit,),
        )
    except Exception as exc:
        _log.warning("list_feedback failed: %s", exc)
        return []


def mark_feedback_resolved(feedback_id: int, resolved: bool = True) -> bool:
    """Toggle resolved_at on a single row. Returns True on success."""
    try:
        if resolved:
            execute(
                "UPDATE feedback SET resolved_at = NOW() WHERE id = %s",
                (feedback_id,),
            )
        else:
            execute(
                "UPDATE feedback SET resolved_at = NULL WHERE id = %s",
                (feedback_id,),
            )
        return True
    except Exception as exc:
        _log.warning("mark_feedback_resolved failed: %s", exc)
        return False


# ------------------------------------------------------------------
# Onboarding survey (multi-section wizard during first sync wait)
# ------------------------------------------------------------------
#
# Captured during the first SnapTrade sync on /sync/processing as a
# multi-section wizard (~13 questions, 3-5 min to complete — matches the
# dbt build window). Storage is a single JSONB blob per user so the form
# can grow / shrink / rename questions without a schema migration. The
# only contract is "user_id → JSON object". Required-key validation
# lives in the route layer (``app/routes.py:submit_onboarding_why_here``).
#
# Stored only for now; not yet used to personalize copy. Long-term these
# answers anchor the mirror back at the trader's stated goal — see
# AGENTS.md "Daily Review" / Mirror Score notes.

# Defense-in-depth size cap on the serialized blob. The route layer
# already truncates individual free-text fields; this is the backstop
# against a malicious client that bypasses field-level limits.
_MAX_ONBOARDING_BLOB_BYTES = 16384  # 16 KB


def save_onboarding_response(*, user_id: int, answers: dict) -> bool:
    """Upsert one onboarding response per user.

    ``answers`` is the full per-user blob (dict). Caller (route layer)
    is responsible for validating required keys and trimming free-text
    inputs. Returns True on success.
    """
    if not user_id or not isinstance(answers, dict) or not answers:
        return False
    try:
        blob = json.dumps(answers, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        _log.warning("save_onboarding_response: non-serializable answers: %s", exc)
        return False
    if len(blob.encode("utf-8")) > _MAX_ONBOARDING_BLOB_BYTES:
        _log.warning("save_onboarding_response: blob exceeds %s bytes", _MAX_ONBOARDING_BLOB_BYTES)
        return False
    try:
        execute(
            """INSERT INTO onboarding_responses (user_id, answers, submitted_at)
               VALUES (%s, %s::jsonb, NOW())
               ON CONFLICT (user_id) DO UPDATE SET
                   answers      = EXCLUDED.answers,
                   submitted_at = NOW()""",
            (user_id, blob),
        )
        return True
    except Exception as exc:
        _log.warning("save_onboarding_response failed: %s", exc)
        return False


def get_onboarding_response(user_id: int) -> dict | None:
    """Return the saved onboarding row for ``user_id`` or None.

    The returned dict has keys: ``user_id``, ``answers`` (already
    decoded by psycopg's JSONB adapter), ``submitted_at``.
    """
    if not user_id:
        return None
    try:
        return fetch_one(
            """SELECT user_id, answers, submitted_at
                 FROM onboarding_responses
                WHERE user_id = %s""",
            (user_id,),
        )
    except Exception as exc:
        _log.warning("get_onboarding_response failed: %s", exc)
        return None


# ------------------------------------------------------------------
# Login lockout
# ------------------------------------------------------------------
#
# Why per-username, not per-IP?
#   The /login endpoint already has an IP-keyed flask-limiter cap. That
#   stops a script from hammering a single host. It does NOT stop an
#   attacker who cycles IPs (proxy network, residential VPN) from
#   guessing one user's password forever. Per-username gives the right
#   defense: 5 wrong tries → 15 min cooldown for that name regardless
#   of where the requests come from.
#
# Why a row-per-attempt instead of a counter?
#   - The lockout window slides naturally: we just count failures in
#     the last N minutes; old rows are ignored, no cron needed.
#   - On successful login we delete the username's failure rows, so
#     legitimate users with a typo never accumulate a hole.
#   - The audit table doubles as evidence during incident review and
#     can feed the admin Feedback view later.

LOGIN_FAILURE_LIMIT = 5
LOGIN_LOCKOUT_MINUTES = 15


def _login_username_key(username: str) -> str:
    return (username or "").strip().lower()


def record_login_attempt(
    username: str,
    success: bool,
    *,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> None:
    """Persist one login attempt. On success, also prune that username's
    prior failure rows so the next typo doesn't trigger lockout."""
    key = _login_username_key(username)
    if not key:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO login_attempts
                       (username_lc, success, ip_address, user_agent)
                       VALUES (%s, %s, %s, %s)""",
                    (key, success, ip_address, (user_agent or "")[:512] or None),
                )
                if success:
                    cur.execute(
                        "DELETE FROM login_attempts "
                        "WHERE username_lc = %s AND success = FALSE",
                        (key,),
                    )
    except Exception as exc:
        _log.warning("record_login_attempt failed: %s", exc)


def login_lockout_remaining_seconds(username: str) -> int:
    """Return cooldown seconds left for *username*, or 0 if not locked.

    A username is locked when it has at least LOGIN_FAILURE_LIMIT failed
    attempts within the most recent LOGIN_LOCKOUT_MINUTES window. The
    'remaining' value is the time until the OLDEST failure inside that
    window slides out, which is when the count drops back below the
    limit. We return 0 (no lock) for unknown DB errors so a transient
    Postgres hiccup never permanently locks anyone out.
    """
    key = _login_username_key(username)
    if not key:
        return 0
    try:
        rows = fetch_all(
            """SELECT created_at FROM login_attempts
               WHERE username_lc = %s
                 AND success = FALSE
                 AND created_at > NOW() - (%s || ' minutes')::interval
               ORDER BY created_at DESC
               LIMIT %s""",
            (key, str(LOGIN_LOCKOUT_MINUTES), LOGIN_FAILURE_LIMIT),
        )
    except Exception as exc:
        _log.warning("login_lockout_remaining_seconds failed: %s", exc)
        return 0
    if len(rows) < LOGIN_FAILURE_LIMIT:
        return 0
    oldest = rows[-1]["created_at"]
    if oldest is None:
        return 0
    if oldest.tzinfo is None:
        from datetime import timezone as _tz_local
        oldest = oldest.replace(tzinfo=_tz_local.utc)
    from datetime import datetime as _dt_local, timezone as _tz_local2
    deadline = oldest + _td(minutes=LOGIN_LOCKOUT_MINUTES)
    delta = (deadline - _dt_local.now(_tz_local2.utc)).total_seconds()
    return max(0, int(delta))


def peek_password_reset_token(raw_token: str) -> int | None:
    """Read-only check used by the GET form: is the token still valid?
    Does NOT consume the token; the consume happens on POST."""
    if not raw_token:
        return None
    token_hash = _hash_reset_token(raw_token)
    row = fetch_one(
        """SELECT user_id, expires_at, used_at
           FROM password_reset_tokens
           WHERE token_hash = %s LIMIT 1""",
        (token_hash,),
    )
    if not row or row["used_at"] is not None:
        return None
    expires_at = row["expires_at"]
    now = _dt.now(_tz.utc)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=_tz.utc)
    if now > expires_at:
        return None
    return int(row["user_id"])
