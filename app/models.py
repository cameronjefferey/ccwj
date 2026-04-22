"""
Application data model. Backed by Postgres (see ``app.db``).

Schema is created on app startup via ``init_db()``. All queries go through
``app.db.{fetch_all,fetch_one,execute,execute_returning}`` which use a
shared connection pool.
"""
import hashlib
import logging
import os

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
            password_hash TEXT NOT NULL
        )
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
    ]
    with get_conn() as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
    _migrate_schwab_first_sync_column()


def _migrate_schwab_first_sync_column():
    """Idempotent: add schwab_first_sync_completed for per-user routine vs full-history sync UX."""
    try:
        execute(
            "ALTER TABLE schwab_connections "
            "ADD COLUMN IF NOT EXISTS schwab_first_sync_completed BOOLEAN NOT NULL DEFAULT FALSE"
        )
    except Exception as e:
        _log.warning("schwab_connections migration skipped: %s", e)


class User(UserMixin):
    """Simple user model backed by Postgres."""

    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    @staticmethod
    def _from_row(row):
        if not row:
            return None
        return User(id=row["id"], username=row["username"], password_hash=row["password_hash"])

    @staticmethod
    def get_by_id(user_id):
        return User._from_row(
            fetch_one("SELECT id, username, password_hash FROM users WHERE id = %s", (user_id,))
        )

    @staticmethod
    def get_by_username(username):
        return User._from_row(
            fetch_one(
                "SELECT id, username, password_hash FROM users WHERE username = %s",
                (username,),
            )
        )

    @staticmethod
    def create(username, password):
        password_hash = generate_password_hash(password)
        execute(
            "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
            (username, password_hash),
        )

    @staticmethod
    def update_password(user_id, new_password):
        execute(
            "UPDATE users SET password_hash = %s WHERE id = %s",
            (generate_password_hash(new_password), user_id),
        )


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
    execute(
        "INSERT INTO user_accounts (user_id, account_name) VALUES (%s, %s) "
        "ON CONFLICT (user_id, account_name) DO NOTHING",
        (user_id, account_name),
    )


def remove_account_for_user(user_id, account_name):
    execute(
        "DELETE FROM user_accounts WHERE user_id = %s AND account_name = %s",
        (user_id, account_name),
    )


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


# ------------------------------------------------------------------
# Insights (AI analysis cache)
# ------------------------------------------------------------------

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
# Schwab API connections
# ------------------------------------------------------------------

def save_schwab_connection(user_id, account_hash, account_number, account_name, token_json):
    """Save or update a Schwab connection."""
    execute(
        """INSERT INTO schwab_connections
           (user_id, account_hash, account_number, account_name, token_json, updated_at)
           VALUES (%s, %s, %s, %s, %s, NOW())
           ON CONFLICT (user_id, account_number) DO UPDATE SET
               account_hash = EXCLUDED.account_hash,
               account_name = EXCLUDED.account_name,
               token_json   = EXCLUDED.token_json,
               updated_at   = NOW()""",
        (user_id, account_hash, account_number, account_name or account_number, token_json),
    )


def update_schwab_token(user_id, account_number, token_json):
    """Update the stored token for a Schwab connection (e.g. after refresh)."""
    execute(
        "UPDATE schwab_connections SET token_json = %s, updated_at = NOW() "
        "WHERE user_id = %s AND account_number = %s",
        (token_json, user_id, account_number),
    )


def update_schwab_account_hash(user_id, account_number, account_hash):
    """Update account hash when Schwab rotates hashValue (avoids 401 on trader API)."""
    execute(
        "UPDATE schwab_connections SET account_hash = %s, updated_at = NOW() "
        "WHERE user_id = %s AND account_number = %s",
        (account_hash, user_id, account_number),
    )


def get_schwab_connections(user_id):
    return fetch_all(
        "SELECT account_number, account_name, created_at FROM schwab_connections "
        "WHERE user_id = %s",
        (user_id,),
    )


def get_schwab_connection(user_id, account_number=None):
    """Return a user's Schwab connection (token_json + account_hash).
    If account_number is None, returns the first connection."""
    if account_number:
        return fetch_one(
            "SELECT account_hash, account_number, account_name, token_json, "
            "schwab_first_sync_completed "
            "FROM schwab_connections WHERE user_id = %s AND account_number = %s",
            (user_id, account_number),
        )
    return fetch_one(
        "SELECT account_hash, account_number, account_name, token_json, "
        "schwab_first_sync_completed "
        "FROM schwab_connections WHERE user_id = %s LIMIT 1",
        (user_id,),
    )


def mark_schwab_first_sync_completed(user_id):
    """After a successful manual sync, stop defaulting the account tab to full-history."""
    execute(
        "UPDATE schwab_connections SET schwab_first_sync_completed = TRUE "
        "WHERE user_id = %s",
        (user_id,),
    )


def remove_schwab_connection(user_id, account_number):
    execute(
        "DELETE FROM schwab_connections WHERE user_id = %s AND account_number = %s",
        (user_id, account_number),
    )


# ------------------------------------------------------------------
# Profiles & community (Postgres app tables)
# ------------------------------------------------------------------

_PROFILE_COLUMNS = (
    "user_id, display_name, headline, bio, accent, timezone, week_starts_monday, "
    "default_route, digest_email, compact_tables, show_account_names_on_published, "
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

This profile is built to show what the platform looks like when it's full: weekly review with real numbers, Mirror Score history, strategy breakdowns, and AI coaching. Every section is populated so you can see the full experience.

## Next Steps for You

1. **Upload your own data** — Replace this demo with your real accounts and watch your own trends.
2. **Use the Coach** — Ask questions about your trades; the AI uses only your data.
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
