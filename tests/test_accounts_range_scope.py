"""Regression tests for preserving /accounts scope during range switches."""

from app.routes import _accounts_scope_query


def test_accounts_range_scope_preserves_direct_tenant_deep_link():
    assert _accounts_scope_query(
        {"tenant": "snaptrade:account-123"}
    ) == "tenant=snaptrade%3Aaccount-123"


def test_accounts_range_scope_preserves_multi_tenant_selection():
    assert _accounts_scope_query(
        {"tenants": "snaptrade:first,snaptrade:second"}
    ) == "tenants=snaptrade%3Afirst%2Csnaptrade%3Asecond"


def test_accounts_range_scope_matches_tenant_resolution_precedence():
    assert _accounts_scope_query({
        "tenant": "snaptrade:physical-account",
        "tenants": "snaptrade:first,snaptrade:second",
        "account": "Shared Account",
    }) == "tenant=snaptrade%3Aphysical-account"


def test_accounts_range_scope_falls_back_to_legacy_account_label():
    assert _accounts_scope_query(
        {"account": "Schwab Account (\u2022\u20226342)"}
    ) == "account=Schwab+Account+%28%E2%80%A2%E2%80%A26342%29"
