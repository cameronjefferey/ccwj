"""Rate-limiter store: Redis fallback, then memory."""

from app.extensions import _rate_limit_storage_uri


def test_rate_limit_storage_explicit_wins(monkeypatch):
    monkeypatch.setenv("RATELIMIT_STORAGE_URI", "redis://limit:6379/1")
    monkeypatch.setenv("QUERY_CACHE_REDIS_URL", "redis://shared:6379/0")
    assert _rate_limit_storage_uri() == "redis://limit:6379/1"


def test_rate_limit_storage_falls_back_to_query_cache_redis(monkeypatch):
    monkeypatch.delenv("RATELIMIT_STORAGE_URI", raising=False)
    monkeypatch.setenv("QUERY_CACHE_REDIS_URL", "redis://shared:6379/0")
    assert _rate_limit_storage_uri() == "redis://shared:6379/0"


def test_rate_limit_storage_memory_without_redis(monkeypatch):
    monkeypatch.delenv("RATELIMIT_STORAGE_URI", raising=False)
    monkeypatch.delenv("QUERY_CACHE_REDIS_URL", raising=False)
    assert _rate_limit_storage_uri() == "memory://"
