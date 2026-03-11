import sqlite3
import os
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

DB_PATH = os.environ.get(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(__file__), '..', 'instance', 'happytrader.db'),
)


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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_mirror_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            week_start_date TEXT NOT NULL,
            discipline_score REAL NOT NULL,
            intent_score REAL NOT NULL,
            risk_alignment_score REAL NOT NULL,
            consistency_score REAL NOT NULL,
            mirror_score REAL NOT NULL,
            confidence_level TEXT NOT NULL,
            diagnostic_sentence TEXT,
            generated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(user_id, week_start_date),
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


# ------------------------------------------------------------------
# Mirror Score (behavioral diagnostic)
# ------------------------------------------------------------------

def save_mirror_score(
    user_id, week_start_date,
    discipline_score, intent_score, risk_alignment_score, consistency_score,
    mirror_score, confidence_level, diagnostic_sentence=None,
):
    """Save or replace weekly mirror score for a user."""
    conn = _get_db()
    conn.execute(
        """INSERT OR REPLACE INTO weekly_mirror_scores
           (user_id, week_start_date, discipline_score, intent_score, risk_alignment_score,
            consistency_score, mirror_score, confidence_level, diagnostic_sentence)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            user_id, week_start_date,
            discipline_score, intent_score, risk_alignment_score, consistency_score,
            mirror_score, confidence_level, diagnostic_sentence,
        ),
    )
    conn.commit()
    conn.close()


def get_mirror_score_for_user(user_id, week_start_date=None):
    """Return mirror score for user. If week_start_date is None, return latest."""
    conn = _get_db()
    if week_start_date:
        row = conn.execute(
            """SELECT week_start_date, discipline_score, intent_score, risk_alignment_score,
                      consistency_score, mirror_score, confidence_level, diagnostic_sentence, generated_at
               FROM weekly_mirror_scores WHERE user_id = ? AND week_start_date = ?""",
            (user_id, week_start_date),
        ).fetchone()
    else:
        row = conn.execute(
            """SELECT week_start_date, discipline_score, intent_score, risk_alignment_score,
                      consistency_score, mirror_score, confidence_level, diagnostic_sentence, generated_at
               FROM weekly_mirror_scores WHERE user_id = ? ORDER BY week_start_date DESC LIMIT 1""",
            (user_id,),
        ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_mirror_score_history(user_id, limit=8):
    """Return the most recent N mirror scores for trend display."""
    conn = _get_db()
    rows = conn.execute(
        """SELECT week_start_date, mirror_score, discipline_score, intent_score,
                  risk_alignment_score, consistency_score, confidence_level
           FROM weekly_mirror_scores WHERE user_id = ?
           ORDER BY week_start_date DESC LIMIT ?""",
        (user_id, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def get_journal_stats(user_id):
    """Return journal activity stats for dashboard nudges."""
    conn = _get_db()
    total = conn.execute(
        "SELECT COUNT(*) FROM journal_entries WHERE user_id = ?", (user_id,)
    ).fetchone()[0]
    latest = conn.execute(
        "SELECT created_at FROM journal_entries WHERE user_id = ? ORDER BY created_at DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    conn.close()
    return {
        "total_entries": total,
        "last_entry_at": latest[0] if latest else None,
    }


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
        _seed_demo_journal(demo.id)
        _seed_demo_mirror_scores(demo.id)


def _ensure_demo_insight(demo_user_id):
    """Seed a pre-generated insight for the demo user so it's ready on first visit."""
    if get_insight_for_user(demo_user_id):
        return  # already has one
    summary = (
        "Years of consistent options trading across Covered Calls, CSPs, Wheels, and PMCC. "
        "Account growth, strong win rates, and disciplined journaling. Your Mirror Score trend "
        "shows real progress—this is what a mature, intentional options trader looks like."
    )
    full_analysis = """## Summary

You've built a track record over multiple years: diversified options strategies, steady premium income, and clear improvement in discipline and alignment with your plan. Your data shows wins and losses, assignments and expirations, and a portfolio that has grown while you've refined your approach.

## Trading Style Overview

You trade like someone who's been at this for years. Covered Calls and Cash-Secured Puts on quality names (AAPL, NVDA, META, GOOGL, COST, SPY). You run the Wheel when assignment makes sense, and you've added Poor Man's Covered Call (PMCC) on names like PLTR. You mix income with occasional directional plays (long calls/puts) and keep position sizing and journaling in the picture.

## What's Working

- **Strategy variety** — CSPs, Covered Calls, Wheels, PMCC, and selective directional trades. You're not stuck in one playbook.
- **Consistent journaling** — Entries across symbols and strategies; mood and tags show you're reflecting on what works.
- **Mirror Score trend** — Your discipline and intent scores have trended up over time. That's the kind of progress that separates long-term traders from one-off gamblers.
- **Premium and assignments** — You collect premium, take assignment when it fits the plan, and close or roll with intention.

## What This Demo Shows

This profile is built to show what the platform looks like when it's full: weekly review with real numbers, journal entries that tie to trades, Mirror Score history, strategy breakdowns, and trade-by-kind insights. Every section is populated so you can see the full experience.

## Next Steps for You

1. **Upload your own data** — Replace this demo with your real accounts and watch your own trends.
2. **Use the Coach** — Ask questions about your trades; the AI uses only your data.
3. **Track over time** — The more you upload and journal, the more accurate your snapshots and Mirror Score become."""
    save_insight(demo_user_id, summary, full_analysis)


def _seed_demo_journal(demo_user_id):
    """Seed demo user with many journal entries so Journal and Weekly Review feel full."""
    if list_journal_entries(demo_user_id, limit=1):
        return  # already has entries
    account = DEMO_ACCOUNT
    entries = [
        ("AAPL", "Covered Call", "2022-02-14", "Selling OTM calls on core holding.", "calm", ["income", "plan"]),
        ("MSFT", "Cash-Secured Put", "2022-04-01", "Wanted to own at 290; put expired worthless.", "focused", ["csp", "win"]),
        ("NVDA", "Covered Call", "2022-06-10", "Assigned on NVDA; took profit and redeployed.", "satisfied", ["assignment", "win"]),
        ("META", "Covered Call", "2022-09-12", "Collecting premium while holding long.", "neutral", ["theta"]),
        ("GOOGL", "Wheel", "2023-02-17", "Put assigned; sold covered call. Wheel in motion.", "disciplined", ["wheel", "plan"]),
        ("TSLA", "Covered Call", "2023-04-10", "High premium on TSLA; assigned and closed.", "good", ["income"]),
        ("COST", "Cash-Secured Put", "2023-06-01", "Quality name; put expired. Would sell again.", "calm", ["csp"]),
        ("AMD", "Cash-Secured Put", "2023-09-01", "Two contracts; both expired OTM.", "focused", ["csp", "win"]),
        ("QQQ", "Long Call", "2023-10-01", "Bullish bet; closed for solid gain.", "excited", ["directional", "win"]),
        ("PLTR", "Covered Call", "2024-03-01", "Selling calls on PLTR; managed well.", "calm", ["income"]),
        ("META", "Wheel", "2024-05-17", "Assigned on puts; sold CC. Classic wheel.", "disciplined", ["wheel"]),
        ("NVDA", "Cash-Secured Put", "2024-07-01", "Put expired. Strong premium.", "satisfied", ["csp", "win"]),
        ("SPY", "Long Put", "2024-08-01", "Hedge; closed for small profit.", "neutral", ["hedge"]),
        ("AAPL", "Covered Call", "2024-09-01", "Another round of CCs; expired worthless.", "good", ["income", "win"]),
        ("AMZN", "Cash-Secured Put", "2024-11-01", "Put expired. Adding to income.", "focused", ["csp"]),
        ("MSFT", "Cash-Secured Put", "2025-01-06", "Sold put; expired. Clean.", "calm", ["csp", "win"]),
        ("GOOGL", "Cash-Secured Put", "2025-02-01", "Expired OTM. Happy with the premium.", "satisfied", ["csp"]),
        ("AMD", "Long Call", "2025-03-01", "Took profit on the call; nice win.", "excited", ["directional", "win"]),
        ("JPM", "Cash-Secured Put", "2025-04-01", "Bank name; put expired.", "neutral", ["csp"]),
        ("NVDA", "Cash-Secured Put", "2025-07-01", "Assigned; now holding 100 shares. Plan was to own.", "disciplined", ["csp", "assignment"]),
        ("TSLA", "Covered Call", "2025-07-15", "Sold call; assigned. Closed position for gain.", "good", ["income", "assignment"]),
        ("META", "Wheel", "2025-09-22", "Put assigned; sold CC. Running the wheel.", "focused", ["wheel", "plan"]),
        ("QQQ", "Spread", "2025-09-01", "Debit spread; closed for profit. Good risk/reward.", "satisfied", ["spread", "win"]),
        ("PLTR", "Poor Man Covered Call", "2025-09-10", "LEAPS + short call. PMCC working as intended.", "calm", ["pmcc", "income"]),
        ("GOOGL", "Covered Call", "2025-11-01", "Sold call on GOOGL; expired.", "good", ["income", "win"]),
        ("PLTR", "Poor Man Covered Call", "2025-11-05", "Rolling short call; managing delta.", "focused", ["pmcc"]),
        ("AAPL", "Covered Call", "2025-12-05", "Year-end CC; expired OTM.", "satisfied", ["income"]),
        ("SPY", "Cash-Secured Put", "2025-12-01", "Selling put on SPY; small position.", "neutral", ["csp"]),
    ]
    for symbol, strategy, open_date, thesis, mood, tags in entries:
        create_journal_entry(
            demo_user_id, account, symbol, strategy, open_date,
            thesis=thesis, mood=mood, tags=tags, confidence=7,
        )


def _seed_demo_mirror_scores(demo_user_id):
    """Seed demo user with many weeks of Mirror Score history (improving trend)."""
    if get_mirror_score_history(demo_user_id, limit=1):
        return  # already has scores
    # 24 weeks of improving scores (Mondays from 2024-06 through 2026-02)
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
        sentence = "Strong alignment with plan; journaling and sizing consistent." if mirror >= 78 else "Good week; keep tracking and sizing positions."
        save_mirror_score(
            demo_user_id, ws,
            discipline, intent_, risk_, consistency_, mirror,
            level, sentence,
        )
