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
    """Create the users table if it doesn't exist."""
    conn = _get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
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
