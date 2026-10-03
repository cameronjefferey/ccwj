"""Reproduce the BE expiry-cap collision from warehouse run 37090194526.

The two seed rows are one buy-to-close posted twice. Mar 6 2026 is the
Friday expiry of BE 260306C00165000. The cap only rewrites a close whose
raw date is after that expiry, so the later row is not a new execution.
Both warehouse descriptions are ``CALL BLOOM ENERGY CORP $165 EXP 03/06/26``.
Gross premium is 10 × 0.042 × 100 = $42.00; the amounts −42.92 and −42.12
differ by the fee embedded in the cash ($0.92 vs $0.12). The failure log
does not say which amount sat on which raw date, so the survivor is
whichever row was already on the expiry.
"""
from datetime import date

from app.upload import _option_expiry_from_symbol

_CLOSE = "option_buy_to_close"
_DESC = "CALL BLOOM ENERGY CORP $165 EXP 03/06/26"
_SYMBOL = "BE    260306C00165000"


def _cap(raw, expiry, as_of, action):
    if action != _CLOSE:
        return raw
    candidate = as_of or raw
    if expiry is not None and candidate > expiry:
        return expiry
    return candidate


def _same_grain(left, right):
    return (
        left["tenant"] == right["tenant"]
        and left["action"] == right["action"]
        and left["symbol"] == right["symbol"]
        and left["qty"] == right["qty"]
        and round(left["price"], 4) == round(right["price"], 4)
        and left["capped"] == right["capped"]
    )


def collapse_capped_fills(rows):
    """stg_history late_posting, on the two-row grain the warehouse saw."""
    enriched = []
    for row in rows:
        enriched.append({
            **row,
            "capped": _cap(row["raw"], row.get("expiry"), row.get("as_of"), row["action"]),
        })
    drop = set()
    for i, late in enumerate(enriched):
        if late["raw"] <= late["capped"]:
            continue
        for anchor in enriched:
            if anchor["raw"] > anchor["capped"]:
                continue
            if not _same_grain(late, anchor):
                continue
            same_desc = late["description"] == anchor["description"]
            as_of_match = late.get("as_of") is not None and late["as_of"] == late["capped"]
            if same_desc or as_of_match:
                drop.add(i)
                break
    return [row for i, row in enumerate(enriched) if i not in drop]


def _be(raw, amount, description=_DESC, as_of=None, expiry=None):
    return {
        "tenant": "snaptrade:016576b8-dd40-40f8-8966-b6d908489cb2",
        "raw": raw,
        "expiry": expiry or date(2026, 3, 6),
        "as_of": as_of,
        "action": _CLOSE,
        "symbol": _SYMBOL,
        "qty": 10,
        "price": 0.042,
        "amount": amount,
        "description": description,
    }


def test_occ_symbol_is_the_march_6_expiry():
    assert _option_expiry_from_symbol(_SYMBOL) == date(2026, 3, 6)
    assert round(10 * 0.042 * 100, 2) == 42.00


def test_be_late_posting_collapses_onto_the_expiry_row():
    """Identical descriptions. The row already dated Mar 6 is the fill."""
    expiry = date(2026, 3, 6)
    monday = date(2026, 3, 9)
    for on_expiry, late in ((-42.12, -42.92), (-42.92, -42.12)):
        out = collapse_capped_fills([
            _be(expiry, on_expiry),
            _be(monday, late),
        ])
        assert len(out) == 1
        assert out[0]["raw"] == expiry
        assert out[0]["amount"] == on_expiry
        assert out[0]["capped"] == expiry


def test_two_real_closes_before_expiry_both_stay():
    later_expiry = date(2026, 3, 20)
    out = collapse_capped_fills([
        _be(date(2026, 3, 4), -42.12, description="early", expiry=later_expiry),
        _be(date(2026, 3, 6), -42.92, description="later", expiry=later_expiry),
    ])
    assert len(out) == 2
    assert {row["raw"] for row in out} == {date(2026, 3, 4), date(2026, 3, 6)}


def test_late_row_with_a_different_description_stays():
    """A second fill posted after expiry is not the same description."""
    out = collapse_capped_fills([
        _be(date(2026, 3, 6), -42.12),
        _be(date(2026, 3, 9), -80.00, description="second lot"),
    ])
    assert len(out) == 2


def test_as_of_late_row_collapses_onto_the_named_day():
    out = collapse_capped_fills([
        _be(date(2026, 3, 6), -42.12, expiry=date(2026, 3, 20)),
        _be(
            date(2026, 3, 9), -42.92,
            description=_DESC + " as of 03/06/2026",
            as_of=date(2026, 3, 6),
            expiry=date(2026, 3, 20),
        ),
    ])
    assert len(out) == 1
    assert out[0]["raw"] == date(2026, 3, 6)
    assert out[0]["amount"] == -42.12
