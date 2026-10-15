"""The CHECK 12 warehouse profile is select-only and cannot print identities."""
from scripts.audit.check12_gap import _LINE, assert_select_only, emit, queries


def test_every_statement_is_a_select():
    names = []
    for name, sql in queries():
        names.append(name)
        assert_select_only(sql)
        assert "{{" not in sql
        assert "{0,5}" in sql or name in {"tables", "check12", "simulate"}
    assert names == [
        "tables",
        "spellings",
        "check12",
        "extras",
        "siblings",
        "raw_opens",
        "simulate",
        "fills",
        "raw_actions",
    ]


def test_emit_drops_account_ids(capsys):
    emit("example_rows", 12)
    emit("bad", "snaptrade:0506672b-aaaa-bbbb-cccc-ddddeeeeffff")
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0] == "example_rows=12"
    assert all(_LINE.match(line) for line in out)
    assert all("snaptrade" not in line.lower() for line in out)
    assert any(line == "line_dropped=1" for line in out)
