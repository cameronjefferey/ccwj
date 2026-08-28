"""Account groups: labels on Settings → Accounts & data, additive ?groups= filter."""
import os

os.environ.setdefault("HAPPYTRADER_SKIP_DB_INIT", "1")

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.models import (
    _norm_account_group_name,
    create_account_group,
    delete_account_group,
    list_account_groups,
    rename_account_group,
    set_account_group_members,
    tenant_ids_for_groups,
)
from app.routes import (
    _picker_tenant_ids,
    _requested_csv_values,
    _requested_group_ids,
    _scope_filter_options,
    _tenants_for_scope,
)
from app.accounts_page import _accounts_scope_query


OWNED = [
    {"tenant_id": "snaptrade:aaa", "account_name": "Schwab Account",
     "display_nickname": "Cameron 401k"},
    {"tenant_id": "snaptrade:bbb", "account_name": "Schwab Account",
     "display_nickname": "Sara IRA"},
    {"tenant_id": "snaptrade:ccc", "account_name": "Schwab Account",
     "display_nickname": "Kids Roth"},
]


def test_norm_group_name_trims_and_rejects_empty():
    assert _norm_account_group_name("  kids  ") == "kids"
    with pytest.raises(ValueError):
        _norm_account_group_name("   ")
    with pytest.raises(ValueError):
        _norm_account_group_name("x" * 41)


def test_requested_group_ids_comma_and_repeated():
    assert _requested_group_ids({"groups": "3,1,3"}) == [3, 1]
    assert _requested_group_ids({"groups": ["2", "5"]}) == [2, 5]
    assert _requested_group_ids({}) == []
    assert _requested_group_ids({"groups": "nope"}) == []


_CHOICES = [
    {"tenant_id": "snaptrade:aaa", "label": "Cameron 401k"},
    {"tenant_id": "snaptrade:bbb", "label": "Sara IRA"},
    {"tenant_id": "snaptrade:ccc", "label": "Kids Roth"},
]
_GROUPS = [
    {"id": 1, "name": "Kids", "tenant_ids": ["snaptrade:ccc"]},
    {"id": 2, "name": "Retirement", "tenant_ids": ["snaptrade:aaa"]},
    {"id": 3, "name": "Crypto", "tenant_ids": ["snaptrade:bbb"]},
]


def test_account_selection_does_not_hide_groups():
    """Picking an account must not trap the Groups menu on that account's groups."""
    groups, accts = _scope_filter_options(_GROUPS, [], ["snaptrade:bbb"], _CHOICES)
    assert [g["name"] for g in groups] == ["Kids", "Retirement", "Crypto"]
    assert [a["label"] for a in accts] == ["Cameron 401k", "Sara IRA", "Kids Roth"]


def test_account_dropdown_filters_to_selected_groups():
    groups, accts = _scope_filter_options(_GROUPS, [1, 2], [], _CHOICES)
    assert [g["name"] for g in groups] == ["Kids", "Retirement", "Crypto"]
    assert [a["label"] for a in accts] == ["Cameron 401k", "Kids Roth"]


def test_group_filter_still_lists_every_member_account():
    """One selected account inside the group set does not hide sibling members."""
    groups, accts = _scope_filter_options(
        _GROUPS, [1, 2], ["snaptrade:aaa"], _CHOICES,
    )
    assert [g["name"] for g in groups] == ["Kids", "Retirement", "Crypto"]
    assert [a["label"] for a in accts] == ["Cameron 401k", "Kids Roth"]


def test_stale_account_outside_group_stays_visible():
    groups, accts = _scope_filter_options(
        _GROUPS, [1], ["snaptrade:bbb"], _CHOICES,
    )
    assert [g["name"] for g in groups] == ["Kids", "Retirement", "Crypto"]
    assert [a["label"] for a in accts] == ["Sara IRA", "Kids Roth"]


def _resolve(query_string, admin=False, owned=OWNED, selected_account="", group_map=None):
    from app import app
    import app.routes as routes

    group_map = group_map or {}

    def _fake_groups(user_id, group_ids):
        matched = [g for g in group_ids if g in group_map]
        tids = []
        seen = set()
        for gid in matched:
            for tid in group_map[gid]:
                if tid not in seen:
                    seen.add(tid)
                    tids.append(tid)
        return matched, tids

    user = SimpleNamespace(id=9, username="cam", is_authenticated=True)
    with app.test_request_context("/overview" + query_string):
        with patch.object(routes, "current_user", user), \
             patch.object(routes, "is_admin", lambda u: admin), \
             patch.object(routes, "get_broker_tenants_for_user", lambda uid: owned), \
             patch("app.models.tenant_ids_for_groups", _fake_groups), \
             patch("app.models.get_tenant_ids_for_user",
                   lambda uid: [r["tenant_id"] for r in owned]):
            return _tenants_for_scope(selected_account)


def test_groups_union_is_additive():
    scope = _resolve(
        "?groups=1,2",
        group_map={1: ["snaptrade:aaa"], 2: ["snaptrade:ccc", "snaptrade:aaa"]},
    )
    assert scope == ["snaptrade:aaa", "snaptrade:ccc"]


def test_groups_intersect_selected_account():
    """Account picker + group filter AND together (group does not widen)."""
    scope = _resolve(
        "?groups=1",
        selected_account="Sara IRA",
        group_map={1: ["snaptrade:aaa", "snaptrade:bbb"]},
    )
    assert scope == ["snaptrade:bbb"]


def test_groups_empty_membership_is_fail_closed():
    scope = _resolve("?groups=1", group_map={1: []})
    assert scope == []


def test_unknown_groups_are_ignored():
    scope = _resolve("?groups=99")
    assert scope == ["snaptrade:aaa", "snaptrade:bbb", "snaptrade:ccc"]


def test_groups_never_add_unowned_tenants():
    """tenant_ids_for_groups already drops unowned; scope must not re-widen."""
    scope = _resolve(
        "?groups=1",
        group_map={1: ["snaptrade:aaa", "snaptrade:HACK"]},
    )
    assert "snaptrade:HACK" not in scope
    assert "snaptrade:aaa" in scope


def test_accounts_scope_query_appends_groups():
    assert _accounts_scope_query(
        {"account": "Cameron 401k", "groups": "1,2"}
    ) == "account=Cameron+401k&groups=1%2C2"
    assert _accounts_scope_query({"tenant": "snaptrade:aaa"}) == "tenant=snaptrade%3Aaaa"
    assert _accounts_scope_query(
        {"tenants": "snaptrade:aaa,snaptrade:bbb", "groups": "1"}
    ) == "tenants=snaptrade%3Aaaa%2Csnaptrade%3Abbb&groups=1"


def test_picker_tenant_ids_from_tenants_param():
    labels = {r["tenant_id"]: r["display_nickname"] for r in OWNED}
    assert _picker_tenant_ids(
        {"tenants": "snaptrade:bbb,snaptrade:HACK"}, OWNED, labels,
    ) == ["snaptrade:bbb"]
    assert _picker_tenant_ids({"account": "Sara IRA"}, OWNED, labels) == ["snaptrade:bbb"]
    assert _picker_tenant_ids({}, OWNED, labels) == []


def test_requested_csv_values_comma_and_repeated():
    assert _requested_csv_values({"tenants": "a,b,a"}, "tenants") == ["a", "b"]
    assert _requested_csv_values({"tenants": ["x", "y"]}, "tenants") == ["x", "y"]


class _FakeGroupDB:
    def __init__(self):
        self.groups = []  # {id, user_id, name}
        self.members = []  # {group_id, tenant_id}
        self._next = 1
        self.owned_tenants = {
            9: ["snaptrade:aaa", "snaptrade:bbb", "snaptrade:ccc"],
            2: ["snaptrade:zzz"],
        }

    def fetch_one(self, sql, params=()):
        rows = self.fetch_all(sql, params)
        return rows[0] if rows else None

    def fetch_all(self, sql, params=()):
        s = " ".join(sql.split())
        params = tuple(params or ())
        if "FROM account_groups g" in s and "ARRAY_AGG" in s:
            uid = int(params[0])
            out = []
            for g in self.groups:
                if g["user_id"] != uid:
                    continue
                tids = [m["tenant_id"] for m in self.members if m["group_id"] == g["id"]]
                out.append({"id": g["id"], "name": g["name"], "tenant_ids": tids})
            out.sort(key=lambda r: (r["name"].lower(), r["id"]))
            return out
        if "FROM account_groups g" in s and "LEFT JOIN account_group_members" in s:
            uid = int(params[0])
            ids = {int(x) for x in params[1:]}
            rows = []
            for g in self.groups:
                if g["user_id"] != uid or g["id"] not in ids:
                    continue
                mems = [m for m in self.members if m["group_id"] == g["id"]]
                if mems:
                    for m in mems:
                        rows.append({"id": g["id"], "tenant_id": m["tenant_id"]})
                else:
                    rows.append({"id": g["id"], "tenant_id": None})
            return rows
        if s.startswith("SELECT id FROM account_groups WHERE user_id") and "LOWER(name)" in s:
            uid = int(params[0])
            name = params[1]
            exclude = int(params[2]) if len(params) > 2 else None
            for g in self.groups:
                if g["user_id"] == uid and g["name"].lower() == name.lower():
                    if exclude is not None and g["id"] == exclude:
                        continue
                    return [{"id": g["id"]}]
            return []
        if s.startswith("SELECT id FROM account_groups WHERE id"):
            gid, uid = int(params[0]), int(params[1])
            for g in self.groups:
                if g["id"] == gid and g["user_id"] == uid:
                    return [{"id": g["id"]}]
            return []
        if s.startswith("SELECT id, name FROM account_groups WHERE id"):
            gid, uid = int(params[0]), int(params[1])
            for g in self.groups:
                if g["id"] == gid and g["user_id"] == uid:
                    return [{"id": g["id"], "name": g["name"]}]
            return []
        return []

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        params = tuple(params or ())
        if s.startswith("UPDATE account_groups SET name"):
            name, gid, uid = params[0], int(params[1]), int(params[2])
            for g in self.groups:
                if g["id"] == gid and g["user_id"] == uid:
                    g["name"] = name
        elif s.startswith("DELETE FROM account_groups"):
            gid, uid = int(params[0]), int(params[1])
            self.groups = [g for g in self.groups if not (g["id"] == gid and g["user_id"] == uid)]
            self.members = [m for m in self.members if m["group_id"] != gid]
        elif s.startswith("DELETE FROM account_group_members"):
            gid = int(params[0])
            self.members = [m for m in self.members if m["group_id"] != gid]
        elif s.startswith("INSERT INTO account_group_members"):
            gid, tid = int(params[0]), params[1]
            if not any(m["group_id"] == gid and m["tenant_id"] == tid for m in self.members):
                self.members.append({"group_id": gid, "tenant_id": tid})

    def execute_returning(self, sql, params=()):
        s = " ".join(sql.split())
        params = tuple(params or ())
        if s.startswith("INSERT INTO account_groups"):
            uid, name = int(params[0]), params[1]
            row = {"id": self._next, "user_id": uid, "name": name}
            self._next += 1
            self.groups.append(row)
            return {"id": row["id"], "name": name}
        return None


@pytest.fixture
def group_db(monkeypatch):
    db = _FakeGroupDB()
    import app.models as models
    monkeypatch.setattr(models, "fetch_all", db.fetch_all)
    monkeypatch.setattr(models, "fetch_one", db.fetch_one)
    monkeypatch.setattr(models, "execute", db.execute)
    monkeypatch.setattr(models, "execute_returning", db.execute_returning)
    monkeypatch.setattr(
        models, "get_tenant_ids_for_user",
        lambda uid: list(db.owned_tenants.get(int(uid), [])),
    )
    return db


def test_create_list_rename_delete_isolated(group_db):
    a = create_account_group(9, "Kids")
    create_account_group(2, "Kids")  # other user, same name is fine
    mine = list_account_groups(9)
    theirs = list_account_groups(2)
    assert [g["name"] for g in mine] == ["Kids"]
    assert [g["name"] for g in theirs] == ["Kids"]
    assert mine[0]["id"] != theirs[0]["id"]

    rename_account_group(9, a["id"], "kids")
    assert list_account_groups(9)[0]["name"] == "kids"

    with pytest.raises(ValueError):
        create_account_group(9, "KIDS")

    assert delete_account_group(2, a["id"]) is False  # not theirs
    assert delete_account_group(9, a["id"]) is True
    assert list_account_groups(9) == []


def test_membership_many_to_many_and_drops_unowned(group_db):
    kids = create_account_group(9, "kids")
    k401 = create_account_group(9, "401ks")
    set_account_group_members(9, kids["id"], ["snaptrade:ccc", "snaptrade:HACK"])
    set_account_group_members(9, k401["id"], ["snaptrade:aaa", "snaptrade:ccc"])
    listed = {g["name"]: g["tenant_ids"] for g in list_account_groups(9)}
    assert listed["kids"] == ["snaptrade:ccc"]
    assert set(listed["401ks"]) == {"snaptrade:aaa", "snaptrade:ccc"}

    matched, tids = tenant_ids_for_groups(9, [kids["id"], k401["id"]])
    assert set(matched) == {kids["id"], k401["id"]}
    assert set(tids) == {"snaptrade:aaa", "snaptrade:ccc"}

    matched, tids = tenant_ids_for_groups(2, [kids["id"]])
    assert matched == []
    assert tids == []
