"""Tests for post-login redirect validation."""

from app.utils import safe_internal_next


def test_safe_internal_next_accepts_path_and_query():
    assert safe_internal_next("/position/RDDT") == "/position/RDDT"
    assert safe_internal_next("/position/RDDT?account=x") == "/position/RDDT?account=x"


def test_safe_internal_next_rejects_external():
    assert safe_internal_next("https://evil.com/") is None
    assert safe_internal_next("//evil.com/path") is None
    assert safe_internal_next("http://x") is None


def test_safe_internal_next_rejects_non_path():
    assert safe_internal_next("") is None
    assert safe_internal_next("relative") is None
    assert safe_internal_next(None) is None


def test_safe_internal_next_rejects_null_bytes():
    assert safe_internal_next("/a\x00b") is None
