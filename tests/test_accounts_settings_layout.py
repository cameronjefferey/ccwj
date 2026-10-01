"""Connected accounts and Settings read as one story each."""

from pathlib import Path

ACCOUNTS = Path("app/templates/snaptrade_accounts.html").read_text()
NAMES = Path("app/templates/name_accounts.html").read_text()
SETTINGS = Path("app/templates/profile.html").read_text()


def test_accounts_story_is_connect_then_list_then_sync_notes():
    hero = ACCOUNTS.index('id="acct-hero"')
    listing = ACCOUNTS.index('id="acct-list"')
    notes = ACCOUNTS.index('id="acct-sync-notes"')
    assert hero < listing < notes
    assert "<!-- /acct-hero -->" in ACCOUNTS
    assert '<details class="disclosure" id="acct-sync-notes">' in ACCOUNTS
    assert 'class="section-header">Connected accounts</h2>' in ACCOUNTS
    assert 'class="section-header">How sync works</span>' in ACCOUNTS
    assert 'class="section-header">Complete this account</span>' in ACCOUNTS
    assert "shadow-sm" not in ACCOUNTS
    assert 'class="btn btn-primary btn-lg"' in ACCOUNTS
    assert "Connect an account" in ACCOUNTS
    assert ACCOUNTS.index("Connect an account") < ACCOUNTS.index("Connected accounts")
    assert "Add a broker or another account" in ACCOUNTS
    assert 'name="nickname"' in ACCOUNTS
    assert 'name="full_history_again"' in ACCOUNTS
    assert 'id="snaptrade_mgr_sync_all_full"' in ACCOUNTS
    assert "url_for('snaptrade_sync')" in ACCOUNTS
    assert "url_for('snaptrade_disconnect')" in ACCOUNTS
    assert "url_for('snaptrade_account_nickname')" in ACCOUNTS
    assert "url_for('snaptrade_connect'" in ACCOUNTS


def test_rename_step_keeps_the_save_button_and_field_names():
    hero = NAMES.index('id="name-hero"')
    listing = NAMES.index('id="name-list"')
    assert hero < listing
    assert "<!-- /name-hero -->" in NAMES
    assert 'class="section-header">Nicknames</h2>' in NAMES
    assert 'name="nickname_{{ c.snaptrade_account_id }}"' in NAMES
    assert "Save and continue" in NAMES
    assert 'class="btn btn-primary btn-lg"' in NAMES
    assert "Skip for now" in NAMES
    assert "shadow-sm" not in NAMES
    assert "box-shadow" not in NAMES


def test_settings_story_is_headline_then_the_tab_action_then_disclosures():
    hero = SETTINGS.index('id="settings-hero"')
    tabs = SETTINGS.index('aria-label="Settings"')
    connections = SETTINGS.index('id="settings-connections"')
    notes = SETTINGS.index('id="settings-sync-notes"')
    actions = SETTINGS.index('id="settings-actions"')
    assert hero < tabs < connections < notes < actions
    assert "<!-- /settings-hero -->" in SETTINGS
    assert '<details class="disclosure" id="settings-sync-notes">' in SETTINGS
    assert '<details class="disclosure" id="settings-actions">' in SETTINGS
    assert '<details class="disclosure" id="account-groups"' in SETTINGS
    assert '<details class="disclosure" id="settings-danger">' in SETTINGS
    assert '<details class="disclosure" id="settings-behavior">' in SETTINGS
    assert 'class="section-header">Connected accounts</h2>' in SETTINGS
    assert 'class="section-header">Your plan</h2>' in SETTINGS
    assert 'id="snaptrade-sync"' in SETTINGS
    assert 'id="group-composer"' in SETTINGS
    assert 'name="group_name"' in SETTINGS
    assert 'name="tenant_id"' in SETTINGS
    assert 'name="display_name"' in SETTINGS
    assert 'name="current_password"' in SETTINGS
    assert 'name="email"' in SETTINGS
    assert 'name="period" value="monthly"' in SETTINGS
    assert 'name="period" value="annual"' in SETTINGS
    assert "url_for('billing_checkout')" in SETTINGS
    assert "url_for('billing_portal')" in SETTINGS
    assert "url_for('billing_checkout_ai')" in SETTINGS
    assert "url_for('delete_account')" in SETTINGS
    assert "30-day free trial, no credit card" in SETTINGS
    assert "no sign-up" not in SETTINGS.lower()
    assert "no signup" not in SETTINGS.lower()
    assert "shadow-sm" not in SETTINGS
    assert 'class="btn btn-primary btn-lg"' in SETTINGS


def test_phone_copy_wraps_on_word_boundaries():
    for page in (ACCOUNTS, NAMES, SETTINGS):
        assert "overflow-x: clip" in page
        assert "overflow-wrap: break-word" in page
        assert "word-break: normal" in page
        assert "overflow-wrap: anywhere" not in page
        assert "word-break: anywhere" not in page
        assert "text-overflow: ellipsis" not in page


def test_account_picker_is_not_on_these_pages():
    for page in (ACCOUNTS, NAMES, SETTINGS):
        assert "_account_scope_filters.html" not in page
        assert "data-ht-persist-tenants" not in page
