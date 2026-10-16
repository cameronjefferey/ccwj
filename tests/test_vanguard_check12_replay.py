"""The CHECK 12 replay is a select, and it does not print identities."""
from scripts.audit.vanguard_check12_replay import (
    _LINE,
    _mismatch_failed,
    assert_select_only,
    emit,
    queries,
)


def test_replay_is_one_select():
    names = []
    sql = ""
    for name, statement in queries():
        names.append(name)
        sql = statement
        assert_select_only(statement)
        low = statement.lower()
        assert "insert " not in low
        assert "update " not in low
        assert "delete " not in low
    assert names == ["replay"]
    assert "order by trade_date nulls last, row_seq nulls last" in sql
    assert "option_buy_to_open" in sql
    assert "option_sell_to_open" in sql
    assert "mismatch_after" in sql


def test_a_zero_mismatch_is_success():
    assert _mismatch_failed(0) is False
    assert _mismatch_failed(None) is True
    assert _mismatch_failed(-1) is True


def test_emit_drops_account_ids(capsys):
    emit("example_rows", 12)
    emit("bad", "snaptrade:0506672b-aaaa-bbbb-cccc-ddddeeeeffff")
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0] == "example_rows=12"
    assert all(_LINE.match(line) for line in out)
    assert all("snaptrade" not in line.lower() for line in out)
