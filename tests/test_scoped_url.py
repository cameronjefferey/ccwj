"""In-page links must keep ?tenants= / ?groups= (and not reintroduce
colliding account= labels). See routes.scoped_url."""

from html import unescape
from urllib.parse import parse_qs, urlparse

from app import app
from app.routes import _scope_keep_kwargs, _scope_query_string, scoped_url


TENANT_A = "snaptrade:tenant-a"
TENANT_B = "snaptrade:tenant-b"
TENANTS = f"{TENANT_A},{TENANT_B}"


def test_scope_keep_prefers_tenants_over_account_label():
    keep = _scope_keep_kwargs({
        "tenants": TENANTS,
        "account": "Schwab Account",
        "groups": "3",
    })
    assert keep["tenants"] == TENANTS
    assert keep["groups"] == "3"
    assert "account" not in keep


def test_scope_keep_legacy_account_when_no_tenant():
    keep = _scope_keep_kwargs({"account": "Cameron Investment"})
    assert keep == {"account": "Cameron Investment"}


def test_scope_keep_round_trips_tenant_and_tenants():
    keep = _scope_keep_kwargs({"tenant": TENANT_A, "tenants": TENANTS})
    assert keep["tenant"] == TENANT_A
    assert keep["tenants"] == TENANTS


def test_scoped_url_appends_current_picker():
    with app.test_request_context(f"/positions?tenants={TENANTS}&groups=2"):
        url = scoped_url("positions", status="Open")
    q = parse_qs(urlparse(url).query)
    assert q["tenants"] == [TENANTS]
    assert q["groups"] == ["2"]
    assert q["status"] == ["Open"]
    assert "account" not in q


def test_scoped_url_drops_colliding_account_when_tenant_present():
    with app.test_request_context(f"/accounts?tenants={TENANTS}"):
        url = scoped_url(
            "strategies",
            strategy="Covered Call",
            tenant=TENANT_A,
            tenants=None,
            account="Schwab Account",
        )
    q = parse_qs(urlparse(url).query)
    assert q["tenant"] == [TENANT_A]
    assert q["strategy"] == ["Covered Call"]
    assert "tenants" not in q
    assert "account" not in q


def test_scope_query_string_for_cmdk():
    qs = _scope_query_string({"tenants": TENANTS, "groups": "1"})
    parsed = parse_qs(qs)
    assert parsed["tenants"] == [TENANTS]
    assert parsed["groups"] == ["1"]


def test_positions_open_pill_keeps_tenants():
    with app.test_request_context(f"/positions?tenants={TENANTS}"):
        html = unescape(app.jinja_env.from_string(
            "{{ scoped_url('positions', status=['Open']) }}"
        ).render())
    q = parse_qs(urlparse(html).query)
    assert q["tenants"] == [TENANTS]
    assert q["status"] == ["Open"]


def test_trader_story_has_filter_bar():
    from pathlib import Path
    text = (Path(app.root_path) / "templates" / "trader_story.html").read_text()
    assert "_account_scope_filters.html" in text
    assert "url_for('trader_story')" in text  # Reset still clears


def test_logo_and_cmdk_keep_scope():
    from pathlib import Path
    base = (Path(app.root_path) / "templates" / "base.html").read_text()
    nav = (Path(app.root_path) / "static" / "js" / "nav.js").read_text()
    assert "scoped_url('weekly_review')" in base
    assert "data-scope-qs=" in base
    assert "withScope(" in nav
    with app.test_request_context(f"/overview?tenants={TENANTS}"):
        href = unescape(
            app.jinja_env.from_string("{{ scoped_url('weekly_review') }}").render()
        )
    assert parse_qs(urlparse(href).query)["tenants"] == [TENANTS]
