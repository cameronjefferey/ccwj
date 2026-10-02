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


def test_reddit_pixel_copy_matches_where_it_loads():
    privacy = (_TEMPLATES / "privacy.html").read_text()
    base = (_TEMPLATES / "base.html").read_text()
    assert "reached from an ad" not in privacy
    assert "reached from ads" not in base
    assert "Home, pricing, signup, Learn, FAQ, and ad landing pages load the Reddit pixel." in base
    assert "Login does not." in base
    assert "Do Not Track or Global Privacy Control" in base
    assert "The Reddit pixel loads on the home page, pricing, signup, Learn, the FAQ, and the ad landing pages." in privacy
    assert "It does not load on the login page." in privacy
    assert "Do Not Track or Global Privacy Control" in privacy


def test_phone_landing_keeps_the_sticky_signup_clear_of_the_cookie_notice():
    css = (_TEMPLATES / "_landing_styles.html").read_text()
    phone = css.split("@media (max-width: 640px)", 1)[1]
    assert "html:not(.ht-cookie-ok) body.ht-campaign:has(#ht-cookie-notice) .ht-cookie-notice" in phone
    assert "bottom: calc(4.75rem + 0.35rem + env(safe-area-inset-bottom, 0px));" in phone
    assert "padding-bottom: calc(4.75rem + 10rem + env(safe-area-inset-bottom, 0px));" in phone
    assert "body.ht-campaign .ht-feedback-fab { display: none; }" in phone


def test_faq_row_does_not_spill_past_a_phone():
    faq = (_TEMPLATES / "faq.html").read_text()
    assert 'class="row justify-content-center g-0"' in faq


def test_terms_do_not_promise_an_export_button_or_unpaid_beta():
    src = (_TEMPLATES / "terms.html").read_text()
    lower = src.lower()
    assert "you can export" not in lower
    assert "export anytime" not in lower
    assert "closed beta" not in lower
    assert "Stripe" in src
    assert "delete" in lower
