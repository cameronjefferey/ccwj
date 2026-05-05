"""
Tests for the multi-account Schwab management surface:
  - account discovery from the API
  - default labels when Schwab returns no nickname
  - display nickname trimming / truncation (front-end only label)

These unit tests deliberately avoid touching Flask routes or Postgres so they
run against the in-process module without DB fixtures. The route tests live
in higher-level integration runs that ship with the deploy script.
"""
from app import models as _models
from app.schwab import (
    _schwab_default_account_label,
    _schwab_fetch_remote_accounts,
)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _FakeClient:
    """Mimics just enough of schwab-py's Client surface for discovery."""

    def __init__(self, account_numbers_payload, accounts_payload):
        self._account_numbers_payload = account_numbers_payload
        self._accounts_payload = accounts_payload

    @property
    def session(self):
        class _Sess:
            token = None
        return _Sess()

    def get_account_numbers(self):
        return _FakeResponse(self._account_numbers_payload)

    def get_accounts(self, *args, **kwargs):
        return _FakeResponse(self._accounts_payload)


def test_default_account_label_uses_last_four_digits():
    assert _schwab_default_account_label("123456789") == "Schwab ••••6789"


def test_default_account_label_falls_back_when_not_numeric():
    assert _schwab_default_account_label("ABCDE") == "Schwab ABCDE"


def test_default_account_label_handles_empty_input():
    # Empty input is unlikely (we only call this with API-provided numbers)
    # but the helper must not crash; falling through to "Schwab " is fine.
    assert _schwab_default_account_label("") == "Schwab "


def test_fetch_remote_accounts_enriches_with_nickname_from_accounts_endpoint():
    client = _FakeClient(
        account_numbers_payload=[
            {"accountNumber": "11111", "hashValue": "HASH_A"},
            {"accountNumber": "22222", "hashValue": "HASH_B"},
        ],
        accounts_payload=[
            {
                "securitiesAccount": {
                    "accountNumber": "11111",
                    "nickname": "Roth IRA",
                }
            },
            {
                "securitiesAccount": {
                    "accountNumber": "22222",
                }
            },
        ],
    )
    out = _schwab_fetch_remote_accounts(client)
    assert [a["account_number"] for a in out] == ["11111", "22222"]
    assert out[0]["account_hash"] == "HASH_A"
    assert out[0]["account_name"] == "Roth IRA"
    # No nickname in /accounts → name stays None so the caller can decide
    # whether to substitute the default ••••<last4> label.
    assert out[1]["account_name"] is None


def test_fetch_remote_accounts_skips_accounts_without_number():
    client = _FakeClient(
        account_numbers_payload=[
            {"hashValue": "HASH_X"},
            {"accountNumber": "33333", "hashValue": "HASH_Y"},
        ],
        accounts_payload=[],
    )
    out = _schwab_fetch_remote_accounts(client)
    assert len(out) == 1
    assert out[0]["account_number"] == "33333"


def test_fetch_remote_accounts_dedupes_repeated_numbers():
    client = _FakeClient(
        account_numbers_payload=[
            {"accountNumber": "44444", "hashValue": "HASH_1"},
            {"accountNumber": "44444", "hashValue": "HASH_2"},
        ],
        accounts_payload=[],
    )
    out = _schwab_fetch_remote_accounts(client)
    assert len(out) == 1
    # First-seen wins; downstream relies on the matching hash from the same row.
    assert out[0]["account_hash"] == "HASH_1"


def test_fetch_remote_accounts_returns_empty_when_api_payload_unexpected():
    client = _FakeClient(
        account_numbers_payload={"unexpected": "shape"},
        accounts_payload=[],
    )
    assert _schwab_fetch_remote_accounts(client) == []


def test_fetch_remote_accounts_tolerates_failed_enrichment():
    class _FlakyEnrichClient(_FakeClient):
        def get_accounts(self, *args, **kwargs):
            raise RuntimeError("enrichment 500")

    client = _FlakyEnrichClient(
        account_numbers_payload=[
            {"accountNumber": "55555", "hashValue": "HASH_55"},
        ],
        accounts_payload=None,
    )
    # Discovery still succeeds with just the bare numbers payload.
    out = _schwab_fetch_remote_accounts(client)
    assert len(out) == 1
    assert out[0]["account_number"] == "55555"
    assert out[0]["account_name"] is None


# ---------------------------------------------------------------------------
# Display nickname (UI-only label) — verifies update_schwab_connection_nickname
# trims, truncates, and clears correctly *without* touching account_name. We
# fake out the Postgres helper so this stays a unit test.
# ---------------------------------------------------------------------------

class _ExecuteSpy:
    def __init__(self):
        self.calls = []

    def __call__(self, sql, params):
        self.calls.append((sql, params))


def test_update_schwab_nickname_trims_and_writes_value(monkeypatch):
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    ok = _models.update_schwab_connection_nickname(7, "11111", "  Roth IRA  ")
    assert ok is True
    assert len(spy.calls) == 1
    sql, params = spy.calls[0]
    assert "display_nickname" in sql
    # Critical: account_name (the BigQuery tenancy key) is NOT touched.
    assert "account_name" not in sql
    assert params == ("Roth IRA", 7, "11111")


def test_update_schwab_nickname_clears_when_blank(monkeypatch):
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    ok = _models.update_schwab_connection_nickname(7, "11111", "   ")
    assert ok is True
    sql, params = spy.calls[0]
    # Empty-after-trim collapses to NULL so the UI falls back to account_name.
    assert params == (None, 7, "11111")


def test_update_schwab_nickname_truncates_overlong_input(monkeypatch):
    spy = _ExecuteSpy()
    monkeypatch.setattr(_models, "execute", spy)

    overlong = "x" * 200
    _models.update_schwab_connection_nickname(7, "11111", overlong)
    _, params = spy.calls[0]
    assert len(params[0]) == 80
    assert params[0] == "x" * 80


def test_update_schwab_nickname_returns_false_on_db_error(monkeypatch):
    def _boom(sql, params):
        raise RuntimeError("db down")

    monkeypatch.setattr(_models, "execute", _boom)
    assert _models.update_schwab_connection_nickname(7, "11111", "Roth IRA") is False


# ---------------------------------------------------------------------------
# get_account_nicknames — feeds the global `account_label` Jinja filter so
# the user-set Schwab nickname propagates to every surface that renders an
# account label (positions hero, account dropdowns, profile badges, etc.).
# Renaming `account_name` is unsafe (BigQuery tenancy key); the dict
# returned here MUST be keyed on the raw account_name so callers can swap
# in the display label without changing what gets queried.
# ---------------------------------------------------------------------------


def test_account_nicknames_uses_nickname_when_present(monkeypatch):
    monkeypatch.setattr(
        _models, "fetch_all",
        lambda sql, params: [
            {"account_name": "Schwab Account", "display_nickname": "Investment"},
            {"account_name": "Schwab \u20229437", "display_nickname": "401k"},
        ],
    )
    out = _models.get_account_nicknames(7)
    assert out == {"Schwab Account": "Investment", "Schwab \u20229437": "401k"}


def test_account_nicknames_falls_back_to_account_name(monkeypatch):
    monkeypatch.setattr(
        _models, "fetch_all",
        lambda sql, params: [
            {"account_name": "Schwab Account", "display_nickname": None},
            {"account_name": "Manual Upload", "display_nickname": "   "},
        ],
    )
    out = _models.get_account_nicknames(7)
    # Null AND whitespace-only nicknames both fall back to the raw label so
    # the UI never renders a blank pill.
    assert out == {"Schwab Account": "Schwab Account", "Manual Upload": "Manual Upload"}


def test_account_nicknames_skips_blank_account_names(monkeypatch):
    monkeypatch.setattr(
        _models, "fetch_all",
        lambda sql, params: [
            {"account_name": "", "display_nickname": "Ghost"},
            {"account_name": None, "display_nickname": "Phantom"},
            {"account_name": "Real", "display_nickname": "Nice"},
        ],
    )
    # Defensive: the BQ key would be unusable so we don't index by an empty
    # string — that would hand any blank label the same nickname.
    assert _models.get_account_nicknames(7) == {"Real": "Nice"}


def test_account_nicknames_returns_empty_for_none_user(monkeypatch):
    # Anonymous request short-circuits before hitting the DB.
    called = {"n": 0}

    def _spy(sql, params):
        called["n"] += 1
        return []

    monkeypatch.setattr(_models, "fetch_all", _spy)
    assert _models.get_account_nicknames(None) == {}
    assert called["n"] == 0


def test_account_nicknames_swallows_db_errors(monkeypatch):
    def _boom(sql, params):
        raise RuntimeError("db down")

    monkeypatch.setattr(_models, "fetch_all", _boom)
    # A transient DB hiccup must not blank account labels app-wide; the
    # filter falls back to raw account_name when the dict is empty.
    assert _models.get_account_nicknames(7) == {}
