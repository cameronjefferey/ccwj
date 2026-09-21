"""Profile, pricing, and Checkout copy from the Sep 2026 settings audit.

One billing story: 30-day reverse trial for new accounts, grandfathered
beta with no expiry, and a separate early-broker Checkout trial that
every subscribe surface quotes the same way.
"""

from datetime import datetime, timezone

from app.billing import apply_checkout_branding
from app.early_broker import (
    notice_from_accounts,
    subscribe_offer_for_user,
    subscribe_offer_sentence,
)
from app.plan import pricing_story
from app.profile_page import (
    normalize_timezone,
    profile_header_counts,
    timezone_choices,
)
from app import friendly_timestamp, history_window_label


def test_header_counts_live_accounts_and_distinct_brokers():
    rows = [
        {"broker_slug": "Schwab"},
        {"broker_slug": "Charles Schwab"},
        {"broker_slug": "SCHWAB"},
        {"broker_slug": "Coinbase"},
        {"broker_slug": "snaptrade"},
        {"broker_slug": ""},
    ]
    accounts, brokers = profile_header_counts(rows, ["phantom", "phantom-2"])
    assert accounts == 6
    assert brokers == 2


def test_header_counts_fall_back_when_no_snaptrade_rows():
    accounts, brokers = profile_header_counts([], ["CSV only", "Other"])
    assert accounts == 2
    assert brokers == 0


def test_friendly_timestamp_uses_profile_zone():
    raw = "2026-09-21 18:16:48.146577+00:00"
    assert "2026-09-21 18:16" not in friendly_timestamp(raw, "America/New_York")
    text = friendly_timestamp(raw, "America/New_York")
    assert text.startswith("Sep 21, 2026,")
    assert "EDT" in text
    utc = friendly_timestamp(datetime(2026, 9, 21, 18, 16, tzinfo=timezone.utc))
    assert "UTC" in utc
    assert "+00:00" not in utc


def test_history_window_says_years():
    assert history_window_label(1825) == "5 years"
    assert history_window_label(365) == "1 year"
    assert history_window_label(60) == "60 days"


def test_subscribe_offer_sentence_matches_checkout_days():
    assert subscribe_offer_sentence(180) == (
        "If you subscribe, 6 months of Pro is included before the first charge."
    )
    assert subscribe_offer_sentence(0) is None
    assert subscribe_offer_sentence(None) is None


def test_subscribe_offer_hidden_after_a_prior_subscription(monkeypatch):
    monkeypatch.setattr(
        "app.early_broker.pro_trial_days_for_user", lambda uid: 180,
    )
    assert subscribe_offer_for_user(7, prior_subscription_status="canceled") is None
    assert "6 months" in subscribe_offer_for_user(7)


def test_notice_carries_the_same_offer(monkeypatch):
    monkeypatch.setenv("EARLY_BROKER_TRIAL_DAYS", "180")
    note = notice_from_accounts(
        [{"broker_slug": "Coinbase", "early_broker_cohort": True}],
    )
    assert note["offer"] == subscribe_offer_sentence(180)
    assert "calibrate" not in note["offer"]


def test_pricing_story_for_beta_does_not_quote_day_30():
    story = pricing_story({
        "badge": "Your plan",
        "note": "Grandfathered beta access — thanks for being an early user.",
    })
    assert "no expiry" in story["title"].lower() or "no expiry" in story["title"]
    assert "30" not in story["title"]
    assert "30" not in story["freeze_bullet"]
    assert story["show_freeze_explainer"] is False


def test_pricing_story_logged_out_is_the_30_day_trial():
    story = pricing_story(None)
    assert "30 days" in story["title"]
    assert story["show_freeze_explainer"] is True


def test_checkout_branding_names_happytrader_and_the_trial():
    kwargs = apply_checkout_branding(
        {
            "mode": "subscription",
            "success_url": "https://happytrader.me/billing/success?session_id={CHECKOUT_SESSION_ID}",
            "cancel_url": "https://happytrader.me/pricing",
            "subscription_data": {"metadata": {"user_id": "7"}, "trial_period_days": 180},
        },
        trial_days=180,
    )
    assert kwargs["branding_settings"]["display_name"] == "HappyTrader"
    assert "earningsfollower" not in str(kwargs).lower()
    assert "6 months of Pro is included before the first charge" in (
        kwargs["custom_text"]["submit"]["message"]
    )
    assert kwargs["subscription_data"]["description"] == "HappyTrader Pro"
    assert kwargs["subscription_data"]["trial_period_days"] == 180
    assert kwargs["success_url"].startswith("https://happytrader.me/")
    assert kwargs["cancel_url"].startswith("https://happytrader.me/")


def test_ai_checkout_branding_does_not_say_pro_trial():
    kwargs = apply_checkout_branding(
        {"subscription_data": {"metadata": {"user_id": "7"}}},
        description="HappyTrader AI",
    )
    assert kwargs["subscription_data"]["description"] == "HappyTrader AI"
    assert "trial" not in kwargs["custom_text"]["submit"]["message"].lower()
    assert "billing portal" in kwargs["custom_text"]["submit"]["message"]


def test_timezone_select_keeps_a_stored_zone_and_rejects_junk():
    assert normalize_timezone("America/Chicago") == "America/Chicago"
    assert normalize_timezone("Not/AZone") is None
    assert normalize_timezone("") == "America/New_York"
    choices = timezone_choices("Pacific/Auckland")
    assert choices[0] == "Pacific/Auckland"
    assert "America/New_York" in choices


def test_compact_table_rule_overrides_default_padding():
    from pathlib import Path
    css = Path("app/templates/base.html").read_text()
    default = css.find("body .table:not(.table-sm) > :not(caption) > * > *")
    compact = css.find("body.ht-compact .table > :not(caption) > * > *")
    assert default != -1 and compact != -1
    assert compact > default


def test_pricing_page_for_beta_does_not_quote_day_30():
    from app import app
    from app.plan import pricing_story

    trial_card = {
        "badge": "Your plan",
        "note": "Grandfathered beta access — thanks for being an early user.",
        "cta_label": None,
        "cta_endpoint": None,
    }
    with app.test_request_context("/pricing"):
        html = app.jinja_env.get_template("pricing.html").render(
            pricing_story=pricing_story(trial_card),
            trial_card=trial_card,
            current_user=_User(),
            subscription=None,
            subscribe_offer="If you subscribe, 6 months of Pro is included before the first charge.",
            price_monthly="19.99",
            price_annual="199.99",
            price_annual_equiv="16.67",
            price_ai="9.99",
            ai_billing_enabled=True,
            signup_enabled=True,
            signup_invite_required=False,
            csrf_token=lambda: "tok",
        )
    assert ">Beta</h3>" in html
    assert "Grandfathered beta" in html
    assert "free, no expiry" in html
    assert "After day 30" not in html
    assert "Full-access trial" not in html
    assert "6 months of Pro is included before the first charge" in html
    assert "no billing portal" in html or "aren't subscribed yet" in html
    assert "Gemini 2.5 Pro" in html


def test_billing_tab_copy_for_beta_names_portal_only_after_subscribe():
    from app import app

    with app.test_request_context("/profile?tab=billing"):
        html = app.jinja_env.get_template("profile.html").render(
            tab="billing",
            plan_state="beta",
            subscription=None,
            subscribe_offer="If you subscribe, 6 months of Pro is included before the first charge.",
            profile_row={"display_name": "testingcameron", "timezone": "America/New_York"},
            current_user=_User(),
            accounts=[],
            account_count=7,
            broker_count=2,
            recent_uploads=[],
            snaptrade_enabled=True,
            snaptrade_accounts=[],
            billing_enabled=True,
            price_monthly="19.99",
            price_annual="199.99",
            price_annual_equiv="16.67",
            price_ai="9.99",
            ai_billing_enabled=True,
            insights_enabled=True,
            is_demo_user=False,
            email_unverified=False,
            csrf_token=lambda: "tok",
        )
    assert "Beta" in html and "no expiry" in html
    assert "6 months of Pro is included before the first charge" in html
    assert "no billing portal to open" in html
    assert "free for 30 days" not in html
    assert "Gemini 2.5 Pro" in html
    assert "Gemini Pro," not in html


class _User:
    is_authenticated = True
    username = "testingcameron"
    email = "cameronjsmith23@gmail.com"

    def get_id(self):
        return "1"
