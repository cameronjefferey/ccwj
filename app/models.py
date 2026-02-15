import sqlite3
import os
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'instance', 'happytrader.db')


def _get_db():
    """Return a connection to the SQLite database."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create the users and user_accounts tables if they don't exist."""
    conn = _get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_accounts (
            user_id INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            PRIMARY KEY (user_id, account_name),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            account_name TEXT NOT NULL,
            history_rows INTEGER NOT NULL DEFAULT 0,
            current_rows INTEGER NOT NULL DEFAULT 0,
            uploaded_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            summary TEXT NOT NULL,
            full_analysis TEXT NOT NULL,
            generated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS journal_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            account TEXT NOT NULL,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            trade_open_date TEXT NOT NULL,
            trade_close_date TEXT,
            trade_symbol TEXT,
            thesis TEXT,
            notes TEXT,
            reflection TEXT,
            confidence INTEGER CHECK (confidence >= 1 AND confidence <= 10),
            mood TEXT,
            sleep_quality INTEGER CHECK (sleep_quality IS NULL OR (sleep_quality >= 1 AND sleep_quality <= 10)),
            entry_time TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS journal_tags (
            journal_entry_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            PRIMARY KEY (journal_entry_id, tag),
            FOREIGN KEY (journal_entry_id) REFERENCES journal_entries(id) ON DELETE CASCADE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schwab_connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            account_hash TEXT NOT NULL,
            account_number TEXT NOT NULL,
            account_name TEXT,
            token_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(user_id, account_number),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()
    conn.close()


class User(UserMixin):
    """Simple user model backed by SQLite."""

    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    @staticmethod
    def get_by_id(user_id):
        conn = _get_db()
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        conn.close()
        if row:
            return User(id=row["id"], username=row["username"], password_hash=row["password_hash"])
        return None

    @staticmethod
    def get_by_username(username):
        conn = _get_db()
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        conn.close()
        if row:
            return User(id=row["id"], username=row["username"], password_hash=row["password_hash"])
        return None

    @staticmethod
    def create(username, password):
        conn = _get_db()
        password_hash = generate_password_hash(password)
        conn.execute(
            "INSERT INTO users (username, password_hash) VALUES (?, ?)",
            (username, password_hash),
        )
        conn.commit()
        conn.close()

    @staticmethod
    def update_password(user_id, new_password):
        """Update the password hash for the given user."""
        conn = _get_db()
        new_hash = generate_password_hash(new_password)
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (new_hash, user_id),
        )
        conn.commit()
        conn.close()


# ------------------------------------------------------------------
# User ↔ Account association
# ------------------------------------------------------------------

def get_accounts_for_user(user_id):
    """Return a sorted list of account names linked to the given user."""
    conn = _get_db()
    rows = conn.execute(
        "SELECT account_name FROM user_accounts WHERE user_id = ? ORDER BY account_name",
        (user_id,),
    ).fetchall()
    conn.close()
    return [r["account_name"] for r in rows]


def add_account_for_user(user_id, account_name):
    """Link an account to a user (no-op if already linked)."""
    conn = _get_db()
    conn.execute(
        "INSERT OR IGNORE INTO user_accounts (user_id, account_name) VALUES (?, ?)",
        (user_id, account_name),
    )
    conn.commit()
    conn.close()


def record_upload(user_id, account_name, history_rows, current_rows):
    """Record a successful upload."""
    conn = _get_db()
    conn.execute(
        "INSERT INTO uploads (user_id, account_name, history_rows, current_rows) VALUES (?, ?, ?, ?)",
        (user_id, account_name, history_rows, current_rows),
    )
    conn.commit()
    conn.close()


def get_uploads_for_user(user_id, limit=10):
    """Return the most recent uploads for a user."""
    conn = _get_db()
    rows = conn.execute(
        "SELECT account_name, history_rows, current_rows, uploaded_at "
        "FROM uploads WHERE user_id = ? ORDER BY uploaded_at DESC LIMIT ?",
        (user_id, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ------------------------------------------------------------------
# Insights (AI analysis cache)
# ------------------------------------------------------------------

def save_insight(user_id, summary, full_analysis):
    """Save (or replace) the cached AI insight for a user."""
    conn = _get_db()
    # Keep only the latest insight per user
    conn.execute("DELETE FROM insights WHERE user_id = ?", (user_id,))
    conn.execute(
        "INSERT INTO insights (user_id, summary, full_analysis) VALUES (?, ?, ?)",
        (user_id, summary, full_analysis),
    )
    conn.commit()
    conn.close()


def get_insight_for_user(user_id):
    """Return the most recent insight for a user, or None."""
    conn = _get_db()
    row = conn.execute(
        "SELECT summary, full_analysis, generated_at FROM insights "
        "WHERE user_id = ? ORDER BY generated_at DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def remove_account_for_user(user_id, account_name):
    """Unlink an account from a user."""
    conn = _get_db()
    conn.execute(
        "DELETE FROM user_accounts WHERE user_id = ? AND account_name = ?",
        (user_id, account_name),
    )
    conn.commit()
    conn.close()


# ------------------------------------------------------------------
# Trade Journal
# ------------------------------------------------------------------

JOURNAL_TAG_OPTIONS = [
    "fomo", "earnings_play", "boredom_trade", "revenge_trade", "high_conviction",
    "scaling_in", "scaling_out", "hedge", "thesis_break", "roll", "assignment_plan",
]
JOURNAL_MOOD_OPTIONS = [
    "calm", "anxious", "euphoric", "frustrated", "neutral", "focused", "tired", "confident",
]


def create_journal_entry(user_id, account, symbol, strategy, trade_open_date, **kwargs):
    """Create a journal entry. Returns the new entry id."""
    conn = _get_db()
    conn.execute(
        """INSERT INTO journal_entries (
            user_id, account, symbol, strategy, trade_open_date,
            trade_close_date, trade_symbol, thesis, notes, reflection,
            confidence, mood, sleep_quality, entry_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            user_id, account, symbol, strategy, trade_open_date,
            kwargs.get("trade_close_date"),
            kwargs.get("trade_symbol"),
            kwargs.get("thesis") or "",
            kwargs.get("notes") or "",
            kwargs.get("reflection") or "",
            kwargs.get("confidence"),
            kwargs.get("mood"),
            kwargs.get("sleep_quality"),
            kwargs.get("entry_time"),
        ),
    )
    entry_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for tag in kwargs.get("tags") or []:
        if tag and str(tag).strip():
            conn.execute(
                "INSERT OR IGNORE INTO journal_tags (journal_entry_id, tag) VALUES (?, ?)",
                (entry_id, str(tag).strip().lower()),
            )
    conn.commit()
    conn.close()
    return entry_id


def update_journal_entry(entry_id, user_id, **kwargs):
    """Update a journal entry. Returns True if updated."""
    conn = _get_db()
    row = conn.execute(
        "SELECT id FROM journal_entries WHERE id = ? AND user_id = ?",
        (entry_id, user_id),
    ).fetchone()
    if not row:
        conn.close()
        return False

    conn.execute(
        """UPDATE journal_entries SET
            trade_close_date = COALESCE(?, trade_close_date),
            trade_symbol = COALESCE(?, trade_symbol),
            thesis = COALESCE(?, thesis),
            notes = COALESCE(?, notes),
            reflection = COALESCE(?, reflection),
            confidence = ?,
            mood = ?,
            sleep_quality = ?,
            entry_time = COALESCE(?, entry_time),
            updated_at = datetime('now')
        WHERE id = ?""",
        (
            kwargs.get("trade_close_date"),
            kwargs.get("trade_symbol"),
            kwargs.get("thesis"),
            kwargs.get("notes"),
            kwargs.get("reflection"),
            kwargs.get("confidence"),
            kwargs.get("mood"),
            kwargs.get("sleep_quality"),
            kwargs.get("entry_time"),
            entry_id,
        ),
    )

    if "tags" in kwargs:
        conn.execute("DELETE FROM journal_tags WHERE journal_entry_id = ?", (entry_id,))
        for tag in kwargs.get("tags") or []:
            if tag and str(tag).strip():
                conn.execute(
                    "INSERT INTO journal_tags (journal_entry_id, tag) VALUES (?, ?)",
                    (entry_id, str(tag).strip().lower()),
                )

    conn.commit()
    conn.close()
    return True


def get_journal_entry(entry_id, user_id):
    """Return a journal entry with tags, or None."""
    conn = _get_db()
    row = conn.execute(
        "SELECT * FROM journal_entries WHERE id = ? AND user_id = ?",
        (entry_id, user_id),
    ).fetchone()
    if not row:
        conn.close()
        return None
    tags = [r[0] for r in conn.execute(
        "SELECT tag FROM journal_tags WHERE journal_entry_id = ?", (entry_id,)
    ).fetchall()]
    conn.close()
    d = dict(row)
    d["tags"] = tags
    return d


def list_journal_entries(user_id, symbol=None, strategy=None, tag=None, start_date=None, end_date=None, limit=100):
    """List journal entries for a user, optionally filtered by symbol, strategy, tag, date range."""
    conn = _get_db()
    q = """
        SELECT e.*, GROUP_CONCAT(t.tag) as tags_csv
        FROM journal_entries e
        LEFT JOIN journal_tags t ON e.id = t.journal_entry_id
        WHERE e.user_id = ?
    """
    params = [user_id]
    if symbol:
        q += " AND e.symbol = ?"
        params.append(symbol)
    if strategy:
        q += " AND e.strategy = ?"
        params.append(strategy)
    if tag:
        q += " AND EXISTS (SELECT 1 FROM journal_tags t2 WHERE t2.journal_entry_id = e.id AND t2.tag = ?)"
        params.append(tag)
    if start_date:
        q += " AND e.trade_open_date >= ?"
        params.append(str(start_date))
    if end_date:
        q += " AND e.trade_open_date <= ?"
        params.append(str(end_date))
    q += " GROUP BY e.id ORDER BY e.trade_open_date DESC, e.created_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(q, params).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["tags"] = [t.strip() for t in (d.get("tags_csv") or "").split(",") if t.strip()]
        del d["tags_csv"]
        result.append(d)
    return result


def delete_journal_entry(entry_id, user_id):
    """Delete a journal entry. Returns True if deleted."""
    conn = _get_db()
    cur = conn.execute(
        "DELETE FROM journal_entries WHERE id = ? AND user_id = ?",
        (entry_id, user_id),
    )
    conn.commit()
    conn.close()
    return cur.rowcount > 0


# ------------------------------------------------------------------
# Schwab API connections
# ------------------------------------------------------------------


def save_schwab_connection(user_id, account_hash, account_number, account_name, token_json):
    """Save or update a Schwab connection. Links account to user."""
    import json
    conn = _get_db()
    conn.execute(
        """INSERT INTO schwab_connections
           (user_id, account_hash, account_number, account_name, token_json, updated_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(user_id, account_number) DO UPDATE SET
           account_hash = excluded.account_hash,
           account_name = excluded.account_name,
           token_json = excluded.token_json,
           updated_at = datetime('now')""",
        (user_id, account_hash, account_number, account_name or account_number, token_json),
    )
    conn.commit()
    conn.close()


def update_schwab_token(user_id, account_number, token_json):
    """Update the stored token for a Schwab connection (e.g. after refresh)."""
    conn = _get_db()
    conn.execute(
        "UPDATE schwab_connections SET token_json = ?, updated_at = datetime('now') "
        "WHERE user_id = ? AND account_number = ?",
        (token_json, user_id, account_number),
    )
    conn.commit()
    conn.close()


def get_schwab_connections(user_id):
    """Return list of Schwab connections for a user."""
    conn = _get_db()
    rows = conn.execute(
        "SELECT account_number, account_name, created_at FROM schwab_connections WHERE user_id = ?",
        (user_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_schwab_connection(user_id, account_number=None):
    """Return token_json and account_hash for a user's Schwab connection.
    If account_number is None, returns the first connection."""
    conn = _get_db()
    if account_number:
        row = conn.execute(
            "SELECT account_hash, account_number, account_name, token_json FROM schwab_connections "
            "WHERE user_id = ? AND account_number = ?",
            (user_id, account_number),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT account_hash, account_number, account_name, token_json FROM schwab_connections "
            "WHERE user_id = ? LIMIT 1",
            (user_id,),
        ).fetchone()
    conn.close()
    return dict(row) if row else None


def remove_schwab_connection(user_id, account_number):
    """Remove a Schwab connection."""
    conn = _get_db()
    conn.execute(
        "DELETE FROM schwab_connections WHERE user_id = ? AND account_number = ?",
        (user_id, account_number),
    )
    conn.commit()
    conn.close()


def is_admin(username):
    """Check if a username is in the ADMIN_USERS environment variable."""
    admin_env = os.environ.get("ADMIN_USERS", "")
    if not admin_env:
        return False
    admins = {u.strip().lower() for u in admin_env.split(",") if u.strip()}
    return username.lower() in admins


# ------------------------------------------------------------------
# Bootstrap helpers
# ------------------------------------------------------------------

def seed_users_from_env():
    """
    Auto-create users from the HAPPYTRADER_USERS environment variable.

    Format:  username:password,username2:password2
    Example: cameron:mypassword,sara:herpassword

    Existing users are skipped (not overwritten).
    This runs on every app startup so users survive Render's ephemeral filesystem.
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
    Uses data from dbt seeds (demo_history.csv, demo_current.csv) — run dbt seed + dbt build.
    """
    demo = User.get_by_username("demo")
    if demo is None:
        User.create("demo", "demo123")
        demo = User.get_by_username("demo")
    if demo:
        remove_account_for_user(demo.id, "Testing Account")  # migrate from old demo setup
        add_account_for_user(demo.id, DEMO_ACCOUNT)
        _ensure_demo_insight(demo.id)


def _ensure_demo_insight(demo_user_id):
    """Seed a pre-generated insight for the demo user so it's ready on first visit."""
    if get_insight_for_user(demo_user_id):
        return  # already has one
    summary = (
        "You're running a diversified options portfolio with Covered Calls, Cash-Secured Puts, "
        "and Wheels on popular names like AAPL, NVDA, META, and GOOGL. Strong premium collection "
        "with room to improve win rate on a few positions."
    )
    full_analysis = """## Summary

You're running a solid, diversified options portfolio with Covered Calls, Cash-Secured Puts, and Wheels on popular names like AAPL, NVDA, META, and GOOGL. Premium collection is strong, but a few positions could use tighter management.

## Trading Style Overview

You favor income strategies: Covered Calls, Cash-Secured Puts, and the Wheel. Holdings span tech (AAPL, NVDA, META, GOOGL), consumer (COST), and ETFs (SPY). You hold positions for weeks to months and collect premium consistently. Assignment and expiration are both part of your normal flow.

## What's Working

- **Premium collection** — You're collecting steady premium from short calls and puts across several symbols.
- **Diversification** — Multiple strategies and sectors reduce single-position risk.
- **Wheel execution** — Your META wheel (put assignment → covered call) shows good discipline.

## What Needs Attention

- **Win rate on some symbols** — A few positions have seen more rollovers or buybacks than ideal.
- **Open SPY put** — Monitor the short put; consider rolling or closing if it moves against you.
- **Position sizing** — Ensure no single position dominates the portfolio.

## Actionable Suggestions

1. **Review open options weekly** — Check theta decay and assignment risk on your short GOOGL call and SPY put.
2. **Track cost basis on assigned shares** — When puts assign, log your effective cost for better tax reporting.
3. **Consider adding more symbols** — Spreading premium across more names can smooth returns and reduce concentration risk."""
    save_insight(demo_user_id, summary, full_analysis)
