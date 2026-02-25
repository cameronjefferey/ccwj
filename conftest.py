"""
Pytest configuration. Must set env vars BEFORE app is imported.
"""
import os
import tempfile

import pytest

# Set before any app imports
os.environ.setdefault("SECRET_KEY", "test-secret-key-do-not-use-in-production")
_db_path = os.path.join(tempfile.gettempdir(), f"happytrader_pytest_{os.getpid()}.db")
os.environ["DATABASE_PATH"] = _db_path


@pytest.fixture(scope="session")
def app():
    """Application fixture. Import here so env vars are set first."""
    from app import app as flask_app
    return flask_app


@pytest.fixture(scope="session")
def client(app):
    return app.test_client()


@pytest.fixture
def db_conn():
    """Fresh DB connection for test setup. Uses same DB_PATH as app."""
    from app.models import _get_db
    return _get_db()
