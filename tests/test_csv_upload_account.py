"""CSV upload must attach to the picked tenant, not mint a parallel account.

Picking a Positions nickname (e.g. "Emmory Investment") used to POST that
string and call get_or_create_broker_tenant(manual, manual:<label>), which
created ``manual:manual:Emmory Investment`` beside the SnapTrade tenant
nicknamed Emmory. The picker is now tenant-id valued; a label only
resolves when it uniquely matches an owned tenant.
"""
from pathlib import Path

import pandas as pd

from app.upload import (
    _csv_upload_account_choices,
    _normalize_account_seed_frames,
    _resolve_csv_upload_target,
)


_EMMORY_SNAPTRADE = {
    "tenant_id": "snaptrade:emmory-uuid",
    "account_name": "Schwab Account",
    "display_nickname": "Emmory",
    "account_mask": "••••1111",
}
_EMMORY_INVESTMENT_SNAPTRADE = {
    "tenant_id": "snaptrade:emmory-inv-uuid",
    "account_name": "Schwab Account",
    "display_nickname": "Emmory Investment",
    "account_mask": "••••2222",
}
_CAMERON_401K = {
    "tenant_id": "snaptrade:cameron-401k",
    "account_name": "Schwab Account",
    "display_nickname": "Cameron 401k",
    "account_mask": "••••3333",
}

_HOUSEHOLD = [_EMMORY_SNAPTRADE, _EMMORY_INVESTMENT_SNAPTRADE, _CAMERON_401K]


def test_picker_lists_nicknames_with_tenant_id_values():
    choices = _csv_upload_account_choices(_HOUSEHOLD)
    labels = [c["label"] for c in choices]
    assert "Emmory" in labels
    assert "Emmory Investment" in labels
    by_label = {c["label"]: c["tenant_id"] for c in choices}
    assert by_label["Emmory"] == "snaptrade:emmory-uuid"
    assert by_label["Emmory Investment"] == "snaptrade:emmory-inv-uuid"
    assert "Schwab Account" not in labels


def test_picking_existing_tenant_id_does_not_mint_manual():
    created = []

    def _create(user_id, name):
        created.append((user_id, name))
        return f"manual:manual:{name}"

    name, tid, err = _resolve_csv_upload_target(
        9, "snaptrade:emmory-inv-uuid", "",
        tenants=_HOUSEHOLD, create_manual=_create,
    )
    assert err is None
    assert tid == "snaptrade:emmory-inv-uuid"
    assert name == "Schwab Account"
    assert created == []


def test_legacy_nickname_post_attaches_to_snaptrade_tenant():
    """Old picker posted the visible nickname as the form value."""
    created = []

    name, tid, err = _resolve_csv_upload_target(
        9, "Emmory Investment", "",
        tenants=_HOUSEHOLD,
        create_manual=lambda uid, n: created.append(n) or f"manual:manual:{n}",
    )
    assert err is None
    assert tid == "snaptrade:emmory-inv-uuid"
    assert name == "Schwab Account"
    assert created == []


def test_create_new_with_existing_nickname_attaches_instead_of_duplicating():
    created = []

    name, tid, err = _resolve_csv_upload_target(
        9, "__new__", "Emmory Investment",
        tenants=_HOUSEHOLD,
        create_manual=lambda uid, n: created.append(n) or f"manual:manual:{n}",
    )
    assert err is None
    assert tid == "snaptrade:emmory-inv-uuid"
    assert created == []
    assert name == "Schwab Account"


def test_create_new_with_novel_name_mints_manual_tenant():
    created = []

    def _create(user_id, name):
        created.append(name)
        return f"manual:manual:{name}"

    name, tid, err = _resolve_csv_upload_target(
        9, "__new__", "Keeley IRA",
        tenants=_HOUSEHOLD, create_manual=_create,
    )
    assert err is None
    assert name == "Keeley IRA"
    assert tid == "manual:manual:Keeley IRA"
    assert created == ["Keeley IRA"]


def test_unknown_label_is_rejected_not_minted():
    created = []

    name, tid, err = _resolve_csv_upload_target(
        9, "Emmory Investment", "",
        tenants=[_CAMERON_401K],
        create_manual=lambda uid, n: created.append(n) or f"manual:manual:{n}",
    )
    assert name is None and tid is None
    assert err and "not in your linked accounts" in err
    assert created == []


def test_ambiguous_schwab_account_label_is_rejected():
    """Several SnapTrade tenants share account_name 'Schwab Account'."""
    created = []

    name, tid, err = _resolve_csv_upload_target(
        9, "Schwab Account", "",
        tenants=_HOUSEHOLD,
        create_manual=lambda uid, n: created.append(n) or f"manual:manual:{n}",
    )
    assert name is None and tid is None
    assert err
    assert created == []


def test_csv_account_column_cannot_override_selected_tenant():
    """Schwab Positions often has Account='Emmory' even when the form
    selected Emmory Investment / the SnapTrade tenant."""
    history = pd.DataFrame({
        "Account": ["Emmory"] * 3,
        "Date": ["2024-01-01"] * 3,
        "Action": ["Buy"] * 3,
        "Symbol": ["AAPL", "MSFT", "IYW"],
        "Description": ["x"] * 3,
        "Quantity": [1, 1, 1],
        "Price": [1, 1, 1],
        "Amount": [-1, -1, -1],
    })
    specs, _, _ = _normalize_account_seed_frames(
        "Schwab Account", history, None,
        user_id_int=9,
        tenant_id_str="snaptrade:emmory-inv-uuid",
        skip_history=False, balances_df=None,
    )
    hist = specs[0][1]
    assert list(hist["Account"].unique()) == ["Schwab Account"]
    assert list(hist["tenant_id"].unique()) == ["snaptrade:emmory-inv-uuid"]
    assert "Emmory" not in set(hist["Account"])


def test_upload_template_posts_tenant_id_not_nickname():
    html = Path("app/templates/upload.html").read_text()
    picker = html.split('id="accountSelect"', 1)[1].split("</select>", 1)[0]
    assert "account_choices" in picker
    assert "choice.tenant_id" in picker
    assert "choice.label" in picker
    assert "account_label" not in picker
    assert "same account you see on Positions" in html
