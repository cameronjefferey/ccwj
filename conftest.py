"""
Pytest configuration. Must set env vars BEFORE app is imported.

Tests run against a real Postgres database. Set ``TEST_DATABASE_URL`` in your
environment to a throwaway database (it is wiped at the start of each session).
If unset, DB-dependent tests are skipped so the rest of the suite still runs.

Example:
    createdb happytrader_test
    TEST_DATABASE_URL=postgresql://localhost/happytrader_test pytest
"""
import os

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-do-not-use-in-production")

_test_db_url = os.environ.get("TEST_DATABASE_URL")
if _test_db_url:
    os.environ["DATABASE_URL"] = _test_db_url


def _have_test_db() -> bool:
    return bool(os.environ.get("TEST_DATABASE_URL"))


@pytest.fixture(scope="session")
def app():
    """Application fixture. Import here so env vars are set first."""
    if not _have_test_db():
        pytest.skip("TEST_DATABASE_URL not set; skipping DB-dependent tests")
    from app import app as flask_app
    flask_app.config["WTF_CSRF_ENABLED"] = False
    flask_app.config["RATELIMIT_ENABLED"] = False
    return flask_app


@pytest.fixture(scope="session")
def client(app):
    return app.test_client()


@pytest.fixture
def db_conn():
    """Yield a Postgres connection from the pool. Caller is responsible for any
    cleanup of test rows it creates."""
    if not _have_test_db():
        pytest.skip("TEST_DATABASE_URL not set; skipping DB-dependent tests")
    from app.db import get_conn
    with get_conn() as conn:
        yield conn
