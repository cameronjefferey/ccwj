"""
Tests for the multi-account Schwab management surface:
  - account discovery from the API
  - default labels when Schwab returns no nickname

These unit tests deliberately avoid touching Flask routes or Postgres so they
run against the in-process module without DB fixtures. The route tests live
in higher-level integration runs that ship with the deploy script.
"""
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
