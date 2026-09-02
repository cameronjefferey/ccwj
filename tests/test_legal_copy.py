"""Terms and Privacy must describe the product that actually ships."""

from pathlib import Path

_TEMPLATES = Path(__file__).resolve().parents[1] / "app" / "templates"


def test_privacy_names_real_vendors_not_native_schwab_oauth():
    src = (_TEMPLATES / "privacy.html").read_text()
    assert "SnapTrade" in src
    assert "Stripe" in src
    assert "Resend" in src
    assert "Claude" in src
    assert "Gemini" in src
    assert "Sentry" in src
    assert "native Schwab" not in src
    assert "SCHWAB_APP_KEY" not in src


def test_terms_do_not_promise_an_export_button_or_unpaid_beta():
    src = (_TEMPLATES / "terms.html").read_text()
    lower = src.lower()
    assert "you can export" not in lower
    assert "export anytime" not in lower
    assert "closed beta" not in lower
    assert "Stripe" in src
    assert "delete" in lower
