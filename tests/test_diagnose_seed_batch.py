"""The seed-batch diagnostic must not echo tenant data in its errors."""
import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "diagnose_seed_batch",
    Path(__file__).resolve().parents[1] / "scripts" / "diagnose_seed_batch.py",
)
_diag = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_diag)


def test_public_exception_drops_tenant_facts():
    raw = (
        "boom snaptrade:11111111-2222-3333-4444-555555555555 "
        "user@example.com $1,234.50 "
        + ("Z" * 40)
    )
    text = _diag._public_exception(RuntimeError(raw), allowed_columns=["Amount"])
    assert text.startswith("RuntimeError:")
    assert "11111111" not in text
    assert "user@example.com" not in text
    assert "1,234.50" not in text
    assert "Z" * 40 not in text
    assert "snaptrade:…" in text or "snaptrade:" not in text


def test_public_exception_keeps_seed_column_names():
    exc = RuntimeError('Error converting Pandas column with name: "Amount"')
    text = _diag._public_exception(exc, allowed_columns=["Amount", "Symbol"])
    assert "Amount" in text
    assert "Symbol" not in text or "Amount" in text
