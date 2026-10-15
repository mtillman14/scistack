"""Data inventory: which schema-key combinations have no ``_schema`` row?

``.claude/plan-data-inventory.md``; semantics in docs/claude/data-inventory.md.
The rules under test:

* the universe is the product of each chosen key's values, taken from
  ``_schema`` rows at EVERY level;
* a deeper row counts for its projection onto the chosen keys, and a row NULL
  at a chosen key fills nothing;
* ``excluded`` (owner ``scidb.exclusions.exclusion_status``) wins over
  ``present``;
* the two views and the CSV layout come from one owner (``inventory_table``).
"""

import csv
import json

import pytest

import scifor as _scifor
from scidb import (
    BaseVariable,
    configure_database,
    exclude_schema,
    include_schema,
    schema_order,
)
from scidb.inventory import (
    EXCLUDED,
    MISSING,
    PRESENT,
    inventory_table,
    schema_inventory,
    write_inventory_csv,
)

SCHEMA = ["subject", "session", "trial"]


class InvTrial(BaseVariable):
    pass


class InvSubject(BaseVariable):
    pass


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    # The declared level order is read from the project at cwd.
    monkeypatch.chdir(tmp_path)
    schema_order.clear_cache()
    _scifor.set_schema([])
    yield
    _scifor.set_schema([])
    schema_order.clear_cache()


def populate():
    """S01/BL/{1,2}, S01/POST/1, S02/BL/1 at trial level; S03 only at subject
    level (session and trial NULL)."""
    InvTrial.save(1.0, subject="S01", session="BL", trial="1")
    InvTrial.save(2.0, subject="S01", session="BL", trial="2")
    InvTrial.save(3.0, subject="S01", session="POST", trial="1")
    InvTrial.save(4.0, subject="S02", session="BL", trial="1")
    InvSubject.save(5.0, subject="S03")


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "inv.duckdb", SCHEMA)
    populate()
    yield database
    database.close()


def statuses(inv) -> dict[tuple, str]:
    n = len(inv.keys)
    return {tuple(r[:n]): r[n] for r in inv.rows}


class TestStatuses:
    def test_values_come_from_rows_at_every_level(self, db):
        inv = schema_inventory(db, ["subject", "session"])
        # S03 only exists at subject level, but it is still a subject.
        assert inv.levels["subject"] == ["S01", "S02", "S03"]
        assert inv.levels["session"] == ["BL", "POST"]
        assert inv.product_size == 6

    def test_deeper_rows_count_once_the_extra_key_is_dropped(self, db):
        inv = schema_inventory(db, ["subject", "session"])
        s = statuses(inv)
        assert s[("S01", "BL")] == PRESENT
        assert s[("S01", "POST")] == PRESENT
        assert s[("S02", "BL")] == PRESENT
        assert s[("S02", "POST")] == MISSING

    def test_rows_with_a_null_chosen_key_are_ignored(self, db):
        s = statuses(schema_inventory(db, ["subject", "session"]))
        # S03's only row has session NULL: it fills no (subject, session) cell.
        assert s[("S03", "BL")] == MISSING
        assert s[("S03", "POST")] == MISSING
        # …but at the subject key alone it is present.
        assert statuses(schema_inventory(db, ["subject"]))[("S03",)] == PRESENT

    def test_every_key_by_default(self, db):
        inv = schema_inventory(db)
        assert inv.keys == SCHEMA
        assert inv.product_size == 3 * 2 * 2
        assert inv.counts == {PRESENT: 4, MISSING: 8, EXCLUDED: 0}
        assert len(inv.rows) == 12

    def test_keys_are_put_in_schema_order(self, db):
        inv = schema_inventory(db, ["trial", "subject"])
        assert inv.keys == ["subject", "trial"]

    def test_rows_follow_product_order(self, db):
        inv = schema_inventory(db, ["subject", "session"])
        assert [r[:2] for r in inv.rows] == [
            ("S01", "BL"), ("S01", "POST"),
            ("S02", "BL"), ("S02", "POST"),
            ("S03", "BL"), ("S03", "POST"),
        ]


class TestExclusions:
    def test_wildcard_exclusion(self, db):
        exclude_schema("bad subject", db=db, subject="S02")
        s = statuses(schema_inventory(db, ["subject", "session"]))
        assert s[("S02", "BL")] == EXCLUDED
        assert s[("S02", "POST")] == EXCLUDED
        assert s[("S01", "BL")] == PRESENT

    def test_include_overrides_a_wildcard(self, db):
        exclude_schema("bad subject", db=db, subject="S02")
        # include_schema needs an exclusion on that exact keyset first; the
        # specific include row then beats the subject-wide wildcard.
        exclude_schema("bad session", db=db, subject="S02", session="BL")
        include_schema("BL was fine", db=db, subject="S02", session="BL")
        s = statuses(schema_inventory(db, ["subject", "session"]))
        assert s[("S02", "POST")] == EXCLUDED
        assert s[("S02", "BL")] == PRESENT

    def test_deeper_exclusion_does_not_exclude_the_coarser_combination(self, db):
        exclude_schema("one bad trial", db=db, subject="S01", session="BL", trial="1")
        s = statuses(schema_inventory(db, ["subject", "session"]))
        assert s[("S01", "BL")] == PRESENT
        full = statuses(schema_inventory(db))
        assert full[("S01", "BL", "1")] == EXCLUDED
        assert full[("S01", "BL", "2")] == PRESENT

    def test_excluded_wins_over_present(self, db):
        exclude_schema("bad session", db=db, subject="S01", session="BL")
        inv = schema_inventory(db, ["subject", "session"])
        assert statuses(inv)[("S01", "BL")] == EXCLUDED
        assert inv.counts == {PRESENT: 2, MISSING: 3, EXCLUDED: 1}

    def test_excluded_missing_combination_reads_excluded(self, db):
        exclude_schema("never collected", db=db, subject="S02", session="POST")
        s = statuses(schema_inventory(db, ["subject", "session"]))
        assert s[("S02", "POST")] == EXCLUDED


class TestValues:
    def test_declared_level_order_is_kept(self, tmp_path):
        (tmp_path / "scistack.toml").write_text(
            'modules = []\n\n[schema_keys]\nsession = ["POST", "BL"]\n',
            encoding="utf-8",
        )
        database = configure_database(tmp_path / "inv.duckdb", SCHEMA)
        try:
            populate()
            inv = schema_inventory(database, ["subject", "session"])
            assert inv.levels["session"] == ["POST", "BL"]
            assert [r[:2] for r in inv.rows][:2] == [("S01", "POST"), ("S01", "BL")]
        finally:
            database.close()

    def test_zero_padded_values_are_kept_as_stored(self, tmp_path):
        database = configure_database(tmp_path / "inv.duckdb", SCHEMA)
        try:
            InvTrial.save(1.0, subject="01", session="BL", trial="01")
            InvTrial.save(2.0, subject="02", session="BL", trial="02")
            inv = schema_inventory(database, ["subject", "trial"])
            assert inv.levels == {"subject": ["01", "02"], "trial": ["01", "02"]}
            s = statuses(inv)
            assert s[("01", "01")] == PRESENT
            assert s[("01", "02")] == MISSING
        finally:
            database.close()

    def test_empty_database(self, tmp_path):
        database = configure_database(tmp_path / "empty.duckdb", SCHEMA)
        try:
            inv = schema_inventory(database)
            assert inv.product_size == 0
            assert inv.rows == []
            assert inv.counts == {PRESENT: 0, MISSING: 0, EXCLUDED: 0}
        finally:
            database.close()


class TestValidation:
    def test_size_guard_raises_before_building(self, db):
        with pytest.raises(ValueError, match="12 combinations") as err:
            schema_inventory(db, max_combinations=11)
        # The message names each key's value count and the way out.
        assert "'subject': 3" in str(err.value)
        assert "Uncheck a key" in str(err.value)

    def test_size_guard_allows_the_exact_limit(self, db):
        assert schema_inventory(db, max_combinations=12).product_size == 12

    def test_unknown_key_raises(self, db):
        with pytest.raises(ValueError, match="Unknown schema key"):
            schema_inventory(db, ["subject", "visit"])

    def test_empty_key_list_raises(self, db):
        with pytest.raises(ValueError, match="at least one schema key"):
            schema_inventory(db, [])


class TestTableAndCsv:
    def test_missing_only_view_has_no_status_column(self, db):
        inv = schema_inventory(db, ["subject", "session"])
        columns, rows = inventory_table(inv, missing_only=True)
        assert columns == ["subject", "session"]
        assert rows == [("S02", "POST"), ("S03", "BL"), ("S03", "POST")]

    def test_all_view_has_status(self, db):
        inv = schema_inventory(db, ["subject", "session"])
        columns, rows = inventory_table(inv, missing_only=False)
        assert columns == ["subject", "session", "status"]
        assert len(rows) == 6
        assert rows[0] == ("S01", "BL", PRESENT)

    def test_csv_missing_only(self, db, tmp_path):
        inv = schema_inventory(db, ["subject", "session"])
        out = tmp_path / "out" / "missing.csv"
        assert write_inventory_csv(inv, out, missing_only=True) == 3
        with out.open(newline="", encoding="utf-8") as fh:
            got = list(csv.reader(fh))
        assert got == [
            ["subject", "session"],
            ["S02", "POST"],
            ["S03", "BL"],
            ["S03", "POST"],
        ]

    def test_csv_all(self, db, tmp_path):
        exclude_schema("bad", db=db, subject="S02")
        inv = schema_inventory(db, ["subject", "session"])
        out = tmp_path / "all.csv"
        assert write_inventory_csv(inv, out, missing_only=False) == 6
        with out.open(newline="", encoding="utf-8") as fh:
            got = list(csv.reader(fh))
        assert got[0] == ["subject", "session", "status"]
        assert ["S02", "BL", EXCLUDED] in got
        assert ["S03", "POST", MISSING] in got
        assert ["S01", "POST", PRESENT] in got

    def test_to_dict_truncates_but_keeps_counts(self, db):
        inv = schema_inventory(db)
        d = inv.to_dict(missing_only=False, limit=5)
        assert len(d["rows"]) == 5
        assert d["total_rows"] == 12
        assert d["truncated"] is True
        assert d["counts"] == {PRESENT: 4, MISSING: 8, EXCLUDED: 0}
        json.dumps(d)  # JSON-serializable as is


class TestCli:
    @pytest.fixture
    def db_path(self, tmp_path):
        path = tmp_path / "cli.duckdb"
        database = configure_database(path, SCHEMA)
        populate()
        exclude_schema("bad", db=database, subject="S02", session="BL")
        database.close()
        return path

    def test_json_missing_only_by_default(self, db_path, capsys):
        from scidb.inspect.cli import main

        rc = main(["--db", str(db_path), "inventory", "--keys", "subject,session", "--json"])
        out = capsys.readouterr().out
        assert rc == 0, out
        payload = json.loads(out)
        assert payload["columns"] == ["subject", "session"]
        assert payload["rows"] == [["S02", "POST"], ["S03", "BL"], ["S03", "POST"]]
        assert payload["counts"] == {PRESENT: 2, MISSING: 3, EXCLUDED: 1}

    def test_all_json(self, db_path, capsys):
        from scidb.inspect.cli import main

        rc = main(["--db", str(db_path), "inventory", "--keys", "subject,session", "--all", "--json"])
        payload = json.loads(capsys.readouterr().out)
        assert rc == 0
        assert payload["columns"] == ["subject", "session", "status"]
        assert ["S02", "BL", EXCLUDED] in payload["rows"]

    def test_human_and_csv(self, db_path, tmp_path, capsys):
        from scidb.inspect.cli import main

        out_csv = tmp_path / "inv.csv"
        rc = main(["--db", str(db_path), "inventory", "--keys", "subject,session", "--all", "--csv", str(out_csv)])
        captured = capsys.readouterr()
        assert rc == 0, captured.err
        assert "2 present, 3 missing, 1 excluded" in captured.out
        assert "status" in captured.out
        with out_csv.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.reader(fh))
        assert rows[0] == ["subject", "session", "status"]
        assert len(rows) == 7

    def test_unknown_key_is_an_error(self, db_path, capsys):
        from scidb.inspect.cli import main

        assert main(["--db", str(db_path), "inventory", "--keys", "visit"]) == 1
        assert "Unknown schema key" in capsys.readouterr().err
