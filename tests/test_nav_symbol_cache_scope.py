"""Privacy regression tests for the quick-switcher browser cache."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_quick_switcher_cache_is_scoped_to_authenticated_user():
    template = (ROOT / "app/templates/base.html").read_text()
    script = (ROOT / "app/static/js/nav.js").read_text()

    assert 'data-cache-scope="{{ current_user.get_id() }}"' in template
    assert '"ht-nav-symbols:" + cacheScope' in script
    assert "sessionStorage.getItem(SYMBOL_CACHE_KEY)" in script
    assert "sessionStorage.setItem(" in script
    assert "SYMBOL_CACHE_KEY," in script


def test_quick_switcher_never_reads_legacy_unscoped_cache():
    script = (ROOT / "app/static/js/nav.js").read_text()

    assert 'sessionStorage.getItem("ht-nav-symbols")' not in script
    assert 'sessionStorage.removeItem("ht-nav-symbols")' in script
