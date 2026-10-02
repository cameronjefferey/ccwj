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
    # A shell that imported the app with the skip flag must not keep init_db
    # off once the suite has a real database.
    os.environ.pop("HAPPYTRADER_SKIP_DB_INIT", None)
else:
    # Without a real Postgres, unit-test files that just import small helpers
    # from app.* must still load. Tell app/__init__.py to skip init_db().
    os.environ["HAPPYTRADER_SKIP_DB_INIT"] = "1"


def _have_test_db() -> bool:
    return bool(os.environ.get("TEST_DATABASE_URL"))


@pytest.fixture(autouse=True)
def _quiet_rate_limits_and_demo_caps():
    """The suite shares 127.0.0.1. A process-wide 300/hour limit would fail
    unrelated tests. Tests that opt in set ``RATELIMIT_ENABLED`` themselves.
    """
    from app import app as flask_app
    from app.demo_guard import reset_demo_caps

    previous = flask_app.config.get("RATELIMIT_ENABLED")
    flask_app.config["RATELIMIT_ENABLED"] = False
    os.environ["DEMO_CAPS_MEMORY"] = "1"
    reset_demo_caps()
    yield
    flask_app.config["RATELIMIT_ENABLED"] = False if previous is None else previous
    os.environ.pop("DEMO_CAPS_MEMORY", None)
    reset_demo_caps()


@pytest.fixture(scope="session")
def app():
    """Application fixture. Import here so env vars are set first."""
    if not _have_test_db():
        pytest.skip("TEST_DATABASE_URL not set; skipping DB-dependent tests")
    from app import app as flask_app
    flask_app.config["WTF_CSRF_ENABLED"] = False
    flask_app.config["RATELIMIT_ENABLED"] = False
    flask_app.config["SESSION_IDLE_TIMEOUT_MINUTES"] = 0
    flask_app.config["INSIGHTS_ENABLED"] = True
    flask_app.config["COMMUNITY_ENABLED"] = True
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
