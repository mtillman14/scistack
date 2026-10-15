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
    ALL_LEVELS,
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
        columns, rows = inventory_table(inv, missing_only=True, collapse=False)
        assert columns == ["subject", "session"]
        assert rows == [("S02", "POST"), ("S03", "BL"), ("S03", "POST")]

    def test_all_view_has_status(self, db):
        inv = schema_inventory(db, ["subject", "session"])
        columns, rows = inventory_table(inv, missing_only=False, collapse=False)
        assert columns == ["subject", "session", "status"]
        assert len(rows) == 6
        assert rows[0] == ("S01", "BL", PRESENT)

    def test_csv_missing_only(self, db, tmp_path):
        inv = schema_inventory(db, ["subject", "session"])
        out = tmp_path / "out" / "missing.csv"
        assert write_inventory_csv(inv, out, missing_only=True, collapse=False) == 3
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
        assert write_inventory_csv(inv, out, missing_only=False, collapse=False) == 6
        with out.open(newline="", encoding="utf-8") as fh:
            got = list(csv.reader(fh))
        assert got[0] == ["subject", "session", "status"]
        assert ["S02", "BL", EXCLUDED] in got
        assert ["S03", "POST", MISSING] in got
        assert ["S01", "POST", PRESENT] in got

    def test_to_dict_truncates_but_keeps_counts(self, db):
        inv = schema_inventory(db)
        d = inv.to_dict(missing_only=False, limit=5, collapse=False)
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

        rc = main(["--db", str(db_path), "inventory", "--keys", "subject,session", "--expand", "--json"])
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
        rc = main(["--db", str(db_path), "inventory", "--keys", "subject,session", "--all", "--expand", "--csv", str(out_csv)])
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


# ---------------------------------------------------------------------------
# Within keys (.claude/plan-inventory-within.md)
# ---------------------------------------------------------------------------

ARM_SCHEMA = ["group", "subject", "session", "minute"]
#: Subject order puts ``group`` second: rows are still grouped by it.
ARM_SCHEMA_NONADJACENT = ["subject", "group", "session", "minute"]


class ArmMinute(BaseVariable):
    pass


class ArmSubject(BaseVariable):
    pass


def populate_arms():
    """Two arms with different sessions; each subject belongs to one arm.

    Traditional: T01, T02 × {BL, POST} × minutes {1, 2} — all present.
    CBrace:      C01, C02 × {BL, FIT, POST} × minutes {1, 2} — except
    C02/FIT/minute 2, the one real gap.
    """
    arms = {
        "Traditional": (["T01", "T02"], ["BL", "POST"]),
        "CBrace": (["C01", "C02"], ["BL", "FIT", "POST"]),
    }
    for group, (subjects, sessions) in arms.items():
        for subject in subjects:
            for session in sessions:
                for minute in ("1", "2"):
                    if (subject, session, minute) == ("C02", "FIT", "2"):
                        continue
                    ArmMinute.save(
                        1.0, group=group, subject=subject,
                        session=session, minute=minute,
                    )


def by_values(inv) -> dict[frozenset, str]:
    """Status keyed by {column: value}, independent of column order."""
    n = len(inv.columns)
    return {frozenset(zip(inv.columns, r[:n])): r[n] for r in inv.rows}


def at(**values) -> frozenset:
    return frozenset(values.items())


@pytest.fixture
def arm_db(tmp_path):
    database = configure_database(tmp_path / "arms.duckdb", ARM_SCHEMA)
    populate_arms()
    yield database
    database.close()


class TestWithin:
    def test_without_within_the_arms_cross_into_false_gaps(self, arm_db):
        # Why Within exists: crossing group reports CBrace subjects missing
        # in Traditional, and Traditional subjects missing at FIT.
        inv = schema_inventory(arm_db)
        s = by_values(inv)
        assert inv.product_size == 2 * 4 * 3 * 2
        assert s[at(group="Traditional", subject="C01", session="BL", minute="1")] == MISSING
        assert s[at(group="Traditional", subject="T01", session="FIT", minute="1")] == MISSING
        assert inv.counts == {PRESENT: 19, MISSING: 29, EXCLUDED: 0}

    def test_within_group_reports_only_the_real_gap(self, arm_db):
        inv = schema_inventory(arm_db, within=["group"])
        assert inv.within == ["group"]
        assert inv.keys == ["subject", "session", "minute"]
        assert inv.columns == ARM_SCHEMA
        assert inv.product_size == 8 + 12
        assert inv.counts == {PRESENT: 19, MISSING: 1, EXCLUDED: 0}
        _, rows = inventory_table(inv, missing_only=True)
        assert rows == [("CBrace", "C02", "FIT", "2")]

    def test_per_stratum_values_and_counts(self, arm_db):
        inv = schema_inventory(arm_db, within=["group"])
        # Strata in the group key's global order; values keep the global order.
        assert [s.values for s in inv.strata] == [
            {"group": "CBrace"}, {"group": "Traditional"},
        ]
        cb, trad = inv.strata
        assert cb.levels == {
            "subject": ["C01", "C02"],
            "session": ["BL", "FIT", "POST"],
            "minute": ["1", "2"],
        }
        assert trad.levels["session"] == ["BL", "POST"]
        assert trad.levels["subject"] == ["T01", "T02"]
        assert (cb.product_size, trad.product_size) == (12, 8)
        assert cb.counts == {PRESENT: 11, MISSING: 1, EXCLUDED: 0}
        assert trad.counts == {PRESENT: 8, MISSING: 0, EXCLUDED: 0}
        # levels stays the global per-column list.
        assert inv.levels["session"] == ["BL", "FIT", "POST"]
        assert inv.levels["group"] == ["CBrace", "Traditional"]

    def test_rows_are_grouped_by_stratum(self, arm_db):
        inv = schema_inventory(arm_db, within=["group"])
        groups = [r[0] for r in inv.rows]
        assert groups == ["CBrace"] * 12 + ["Traditional"] * 8
        assert inv.rows[0][:4] == ("CBrace", "C01", "BL", "1")

    def test_keys_none_with_within_crosses_every_other_key(self, arm_db):
        explicit = schema_inventory(
            arm_db, ["subject", "session", "minute"], within=["group"]
        )
        default = schema_inventory(arm_db, within=["group"])
        assert default.keys == explicit.keys
        assert default.rows == explicit.rows

    def test_within_with_fewer_cross_keys(self, arm_db):
        inv = schema_inventory(arm_db, ["session"], within=["group"])
        assert inv.columns == ["group", "session"]
        assert inv.product_size == 3 + 2
        assert inv.counts[MISSING] == 0

    def test_no_within_has_no_strata(self, arm_db):
        inv = schema_inventory(arm_db, ["subject"])
        assert inv.within == []
        assert inv.strata == []
        assert inv.columns == ["subject"]


class TestWithinNonAdjacent:
    @pytest.fixture
    def db2(self, tmp_path):
        database = configure_database(tmp_path / "arms2.duckdb", ARM_SCHEMA_NONADJACENT)
        populate_arms()
        yield database
        database.close()

    def test_same_statuses_and_columns_in_schema_order(self, arm_db, db2):
        a = schema_inventory(arm_db, within=["group"])
        b = schema_inventory(db2, within=["group"])
        assert b.columns == ARM_SCHEMA_NONADJACENT
        assert by_values(a) == by_values(b)
        columns, rows = inventory_table(b, missing_only=True)
        assert columns == ["subject", "group", "session", "minute"]
        assert rows == [("C02", "CBrace", "FIT", "2")]
        # Grouped by stratum even though group is the second column.
        assert [r[1] for r in b.rows] == ["CBrace"] * 12 + ["Traditional"] * 8

    def test_row_null_at_the_within_key_adds_nothing(self, db2):
        # X99 exists only at subject level: group is NULL, so no stratum.
        ArmSubject.save(1.0, subject="X99")
        inv = schema_inventory(db2, within=["group"])
        assert all("X99" not in s.levels["subject"] for s in inv.strata)
        assert inv.product_size == 20
        assert inv.counts == {PRESENT: 19, MISSING: 1, EXCLUDED: 0}
        # Without Within it is a subject like any other.
        assert "X99" in schema_inventory(db2).levels["subject"]


class TestWithinExclusions:
    def test_group_exclusion_marks_the_whole_stratum(self, arm_db):
        exclude_schema("arm dropped", db=arm_db, group="CBrace")
        inv = schema_inventory(arm_db, within=["group"])
        cb, trad = inv.strata
        assert cb.counts == {PRESENT: 0, MISSING: 0, EXCLUDED: 12}
        assert trad.counts == {PRESENT: 8, MISSING: 0, EXCLUDED: 0}

    def test_subject_exclusion_marks_only_that_subject(self, arm_db):
        exclude_schema("withdrew", db=arm_db, subject="C02")
        inv = schema_inventory(arm_db, within=["group"])
        s = by_values(inv)
        assert s[at(group="CBrace", subject="C02", session="FIT", minute="2")] == EXCLUDED
        assert s[at(group="CBrace", subject="C01", session="FIT", minute="2")] == PRESENT
        cb, trad = inv.strata
        assert cb.counts == {PRESENT: 6, MISSING: 0, EXCLUDED: 6}
        assert trad.counts[EXCLUDED] == 0


class TestWithinValidation:
    def test_key_both_cross_and_within(self, arm_db):
        with pytest.raises(ValueError, match="both Cross and Within"):
            schema_inventory(arm_db, ["group", "subject"], within=["group"])

    def test_unknown_within_key(self, arm_db):
        with pytest.raises(ValueError, match="Unknown Within schema key"):
            schema_inventory(arm_db, within=["arm"])

    def test_no_cross_key_left(self, arm_db):
        with pytest.raises(ValueError, match="at least one must be crossed"):
            schema_inventory(arm_db, within=ARM_SCHEMA)

    def test_size_guard_sums_the_strata(self, arm_db):
        # The largest stratum (12) fits; the sum (20) does not.
        with pytest.raises(ValueError, match="20 combinations") as err:
            schema_inventory(arm_db, within=["group"], max_combinations=15)
        assert "group=CBrace: 12" in str(err.value)
        assert schema_inventory(
            arm_db, within=["group"], max_combinations=20
        ).product_size == 20


class TestWithinTableAndCsv:
    def test_csv_includes_the_within_column(self, arm_db, tmp_path):
        inv = schema_inventory(arm_db, within=["group"])
        out = tmp_path / "arms.csv"
        assert write_inventory_csv(inv, out, missing_only=False, collapse=False) == 20
        with out.open(newline="", encoding="utf-8") as fh:
            got = list(csv.reader(fh))
        assert got[0] == [*ARM_SCHEMA, "status"]
        assert ["CBrace", "C02", "FIT", "2", MISSING] in got

    def test_to_dict_has_within_and_strata(self, arm_db):
        d = schema_inventory(arm_db, within=["group"]).to_dict()
        assert d["within"] == ["group"]
        assert d["columns"] == ARM_SCHEMA
        assert d["rows"] == [["CBrace", "C02", "FIT", "2"]]
        assert d["strata"] == [
            {"values": {"group": "CBrace"}, "product_size": 12,
             "counts": {PRESENT: 11, MISSING: 1, EXCLUDED: 0}},
            {"values": {"group": "Traditional"}, "product_size": 8,
             "counts": {PRESENT: 8, MISSING: 0, EXCLUDED: 0}},
        ]
        json.dumps(d)

    def test_to_dict_without_within(self, db):
        d = schema_inventory(db).to_dict()
        assert d["within"] == []
        assert d["strata"] == []


class TestWithinCli:
    @pytest.fixture
    def db_path(self, tmp_path):
        path = tmp_path / "arms_cli.duckdb"
        database = configure_database(path, ARM_SCHEMA)
        populate_arms()
        database.close()
        return path

    def test_within_json(self, db_path, capsys):
        from scidb.inspect.cli import main

        rc = main(["--db", str(db_path), "inventory", "--within", "group", "--json"])
        out = capsys.readouterr().out
        assert rc == 0, out
        payload = json.loads(out)
        assert payload["within"] == ["group"]
        assert payload["keys"] == ["subject", "session", "minute"]
        assert payload["rows"] == [["CBrace", "C02", "FIT", "2"]]
        assert len(payload["strata"]) == 2

    def test_human_render_has_one_line_per_stratum(self, db_path, capsys):
        from scidb.inspect.cli import main

        rc = main(["--db", str(db_path), "inventory", "--within", "group"])
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "within group (2 strata): 19 present, 1 missing, 0 excluded" in out
        assert "group=CBrace: 12 combination(s): 11 present, 1 missing, 0 excluded" in out
        assert "group=Traditional: 8 combination(s): 8 present, 0 missing, 0 excluded" in out

    def test_cross_and_within_overlap_is_an_error(self, db_path, capsys):
        from scidb.inspect.cli import main

        rc = main(["--db", str(db_path), "inventory", "--keys", "group,subject", "--within", "group"])
        assert rc == 1
        assert "both Cross and Within" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Collapsed blocks (.claude/plan-inventory-collapse.md)
# ---------------------------------------------------------------------------

ALL = ALL_LEVELS


def populate_gaps():
    """Traditional: T01, T02 × {BL, POST} × minutes {1, 2}, all present.

    CBrace: C01 × {BL, FIT, POST} × {1, 2} all present; C02 has BL (both
    minutes) and POST minute 1 only, so FIT is wholly missing and POST is
    half missing; C03 exists only at subject level, so every session and
    minute is missing.
    """
    for subject in ("T01", "T02"):
        for session in ("BL", "POST"):
            for minute in ("1", "2"):
                ArmMinute.save(1.0, group="Traditional", subject=subject,
                               session=session, minute=minute)
    for session in ("BL", "FIT", "POST"):
        for minute in ("1", "2"):
            ArmMinute.save(1.0, group="CBrace", subject="C01",
                           session=session, minute=minute)
    for session, minute in (("BL", "1"), ("BL", "2"), ("POST", "1")):
        ArmMinute.save(1.0, group="CBrace", subject="C02",
                       session=session, minute=minute)
    ArmSubject.save(1.0, group="CBrace", subject="C03")


@pytest.fixture
def gap_db(tmp_path):
    database = configure_database(tmp_path / "gaps.duckdb", ARM_SCHEMA)
    populate_gaps()
    yield database
    database.close()


class TestCollapse:
    def test_missing_session_and_subject_collapse(self, gap_db):
        inv = schema_inventory(gap_db, within=["group"])
        columns, rows = inventory_table(inv, missing_only=True)
        assert columns == ARM_SCHEMA
        assert rows == [
            ("CBrace", "C02", "FIT", ALL),    # every minute of FIT
            ("CBrace", "C02", "POST", "2"),   # half a block: listed per row
            ("CBrace", "C03", ALL, ALL),      # recursive: whole subject
        ]

    def test_counts_stay_per_combination(self, gap_db):
        inv = schema_inventory(gap_db, within=["group"])
        assert inv.counts == {PRESENT: 17, MISSING: 9, EXCLUDED: 0}
        assert len(inventory_table(inv, missing_only=True, collapse=False)[1]) == 9

    def test_expand_is_one_row_per_combination(self, gap_db):
        inv = schema_inventory(gap_db, within=["group"])
        _, rows = inventory_table(inv, missing_only=False, collapse=False)
        assert rows == inv.rows

    def test_all_view_collapses_every_uniform_status(self, gap_db):
        inv = schema_inventory(gap_db, within=["group"])
        _, rows = inventory_table(inv, missing_only=False)
        assert rows == [
            ("CBrace", "C01", ALL, ALL, PRESENT),
            ("CBrace", "C02", "BL", ALL, PRESENT),
            ("CBrace", "C02", "FIT", ALL, MISSING),
            ("CBrace", "C02", "POST", "1", PRESENT),
            ("CBrace", "C02", "POST", "2", MISSING),
            ("CBrace", "C03", ALL, ALL, MISSING),
            # A whole stratum: every Cross column collapses, Within stays.
            ("Traditional", ALL, ALL, ALL, PRESENT),
        ]

    def test_all_is_per_stratum(self, gap_db):
        # Traditional's (all) covers BL and POST only; CBrace's covers FIT too.
        inv = schema_inventory(gap_db, within=["group"])
        cb, trad = inv.strata
        assert trad.levels["session"] == ["BL", "POST"]
        assert cb.levels["session"] == ["BL", "FIT", "POST"]
        assert trad.counts[PRESENT] == trad.product_size == 8

    def test_without_within(self, gap_db):
        # Crossing group: C03 is missing everywhere, including Traditional.
        inv = schema_inventory(gap_db)
        _, rows = inventory_table(inv, missing_only=True)
        assert ("CBrace", "C03", ALL, ALL) in rows
        assert ("Traditional", "C01", ALL, ALL) in rows
        assert ("Traditional", "T01", "FIT", ALL) in rows
        expanded = inventory_table(inv, missing_only=True, collapse=False)[1]
        assert len(rows) < len(expanded) == inv.counts[MISSING]

    def test_a_single_level_key_shows_its_value(self, gap_db):
        # Within subject × session, C02/POST has only minute 1: the
        # collapsed row says "1", not "(all)"; C01/BL has 1 and 2.
        inv = schema_inventory(gap_db, ["minute"], within=["subject", "session"])
        _, rows = inventory_table(inv, missing_only=False)
        assert ("C02", "POST", "1", PRESENT) in rows
        assert ("C01", "BL", ALL, PRESENT) in rows

    def test_excluded_row_blocks_a_missing_collapse(self, gap_db):
        exclude_schema("bad", db=gap_db, subject="C02", session="FIT", minute="1")
        inv = schema_inventory(gap_db, within=["group"])
        _, rows = inventory_table(inv, missing_only=True)
        assert ("CBrace", "C02", "FIT", "2") in rows
        assert ("CBrace", "C02", "FIT", ALL) not in rows
        _, all_rows = inventory_table(inv, missing_only=False)
        assert ("CBrace", "C02", "FIT", "1", EXCLUDED) in all_rows

    def test_excluded_subject_collapses_as_excluded(self, gap_db):
        exclude_schema("withdrew", db=gap_db, subject="C03")
        inv = schema_inventory(gap_db, within=["group"])
        _, rows = inventory_table(inv, missing_only=False)
        assert ("CBrace", "C03", ALL, ALL, EXCLUDED) in rows
        assert all(r[1] != "C03" for r in inventory_table(inv, missing_only=True)[1])

    def test_nonadjacent_within_column_keeps_its_value(self, tmp_path):
        database = configure_database(tmp_path / "gaps2.duckdb", ARM_SCHEMA_NONADJACENT)
        populate_gaps()
        try:
            inv = schema_inventory(database, within=["group"])
            columns, rows = inventory_table(inv, missing_only=True)
        finally:
            database.close()
        assert columns == ARM_SCHEMA_NONADJACENT
        assert rows == [
            ("C02", "CBrace", "FIT", ALL),
            ("C02", "CBrace", "POST", "2"),
            ("C03", "CBrace", ALL, ALL),
        ]

    def test_csv_and_to_dict_follow_the_flag(self, gap_db, tmp_path):
        inv = schema_inventory(gap_db, within=["group"])
        out = tmp_path / "c.csv"
        assert write_inventory_csv(inv, out, missing_only=True) == 3
        assert write_inventory_csv(inv, out, missing_only=True, collapse=False) == 9
        d = inv.to_dict()
        assert d["collapse"] is True
        assert d["all_marker"] == ALL
        assert d["total_rows"] == 3
        assert d["rows"][2] == ["CBrace", "C03", ALL, ALL]
        assert d["counts"][MISSING] == 9
        assert inv.to_dict(collapse=False)["total_rows"] == 9


class TestCollapseCli:
    @pytest.fixture
    def db_path(self, tmp_path):
        path = tmp_path / "gaps_cli.duckdb"
        database = configure_database(path, ARM_SCHEMA)
        populate_gaps()
        database.close()
        return path

    def test_collapsed_by_default(self, db_path, capsys):
        from scidb.inspect.cli import main

        rc = main(["--db", str(db_path), "inventory", "--within", "group", "--json"])
        payload = json.loads(capsys.readouterr().out)
        assert rc == 0
        assert payload["rows"] == [
            ["CBrace", "C02", "FIT", ALL],
            ["CBrace", "C02", "POST", "2"],
            ["CBrace", "C03", ALL, ALL],
        ]

    def test_expand(self, db_path, capsys):
        from scidb.inspect.cli import main

        rc = main(["--db", str(db_path), "inventory", "--within", "group", "--expand", "--json"])
        payload = json.loads(capsys.readouterr().out)
        assert rc == 0
        assert payload["collapse"] is False
        assert len(payload["rows"]) == 9

    def test_human_table_shows_the_marker(self, db_path, capsys):
        from scidb.inspect.cli import main

        rc = main(["--db", str(db_path), "inventory", "--within", "group"])
        out = capsys.readouterr().out
        assert rc == 0, out
        assert ALL in out
