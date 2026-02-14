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


def remove_account_for_user(user_id, account_name):
    """Unlink an account from a user."""
    conn = _get_db()
    conn.execute(
        "DELETE FROM user_accounts WHERE user_id = ? AND account_name = ?",
        (user_id, account_name),
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
