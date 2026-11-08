"""Expected locations (scidb.provenance.expectation).

``.claude/plan-variable-expected-locations.md``; rules in
docs/claude/expected-locations.md. Under test:

* the 2x2 of expectation x fact, plus ``excluded`` winning over everything;
* the automatic sharing rule (top key shared, left of a stated shared key
  shared, the rest per parent) and declared values (zero padding kept);
* a parent with nothing below it becomes one wildcard row;
* marks: most specific wins (the rule shared with exclusions), own beats
  inherited, inherited from upstream variables, never applied where the
  variable lacks a key the mark names;
* every row says which quadrant it is in and why.
"""

import json
from datetime import datetime

import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, exclude_schema, for_each
from scifor.project import schema_order
from scidb.provenance import expectation as ex
from scidb.provenance.expectation import (
    ALL_LEVELS,
    EXCLUDED,
    MISSING_EXPECTED,
    MISSING_UNEXPECTED,
    PER_PARENT,
    PRESENT,
    PRESENT_UNEXPECTED,
    SHARED,
    STATUS_LABELS,
    STATUSES,
    TONE,
    expectation,
    mark_expectation,
    mark_history,
    marks,
    resolve_key_settings,
    set_key_setting,
    unmark_expectation,
)
from scidb.provenance.query import related_variable_types
from scidb.store.exclusions import most_specific_match

SCHEMA = ["subject", "session", "trial"]


class ExRaw(BaseVariable):
    pass


class ExFilt(BaseVariable):
    pass


class ExSubject(BaseVariable):
    pass


class ExB(BaseVariable):
    pass


class ExC(BaseVariable):
    pass


def ex_double(signal):
    return signal * 2


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    schema_order.clear_cache()
    _scifor.set_schema([])
    yield
    _scifor.set_schema([])
    schema_order.clear_cache()


def populate():
    """ExRaw at S01/{BL,POST}/{1,2} and S02/BL/{1,2}; S03 exists only at
    subject level (ExSubject)."""
    for session in ("BL", "POST"):
        for trial in ("1", "2"):
            ExRaw.save(1.0, subject="S01", session=session, trial=trial)
    for trial in ("1", "2"):
        ExRaw.save(1.0, subject="S02", session="BL", trial=trial)
    ExSubject.save(5.0, subject="S03")


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "ex.duckdb", SCHEMA)
    populate()
    yield database
    database.close()


def status_of(result, **keys):
    """The status of the row whose values are exactly *keys* (None = (all))."""
    matches = [r for r in result.rows if r.values == keys]
    assert len(matches) == 1, (keys, [r.values for r in result.rows])
    return matches[0]


# ---------------------------------------------------------------------------
# The automatic sharing rule (pure)
# ---------------------------------------------------------------------------


class TestResolveKeySettings:
    def test_default_top_key_shared_rest_per_parent(self):
        s = resolve_key_settings(SCHEMA, {})
        assert [s[k].sharing for k in SCHEMA] == [SHARED, PER_PARENT, PER_PARENT]
        assert s["subject"].rule == ex.RULE_TOP_KEY

    def test_keys_left_of_a_stated_shared_key_are_shared(self):
        s = resolve_key_settings(SCHEMA, {"trial": {"sharing": SHARED}})
        assert [s[k].sharing for k in SCHEMA] == [SHARED, SHARED, SHARED]
        assert s["session"].rule == ex.RULE_LEFT_OF_SHARED
        assert s["trial"].rule == ex.RULE_STATED

    def test_a_stated_per_parent_key_stays_per_parent_even_left_of_shared(self):
        s = resolve_key_settings(
            SCHEMA, {"session": {"sharing": PER_PARENT}, "trial": {"sharing": SHARED}}
        )
        assert s["session"].sharing == PER_PARENT

    def test_values_only_kept_on_a_shared_key(self):
        s = resolve_key_settings(SCHEMA, {"trial": {"sharing": SHARED, "values": ["1"]}})
        assert s["trial"].values == ["1"]


# ---------------------------------------------------------------------------
# Settings storage
# ---------------------------------------------------------------------------


class TestKeySettingStorage:
    def test_round_trip_and_history(self, db):
        set_key_setting(db, "trial", SHARED, ["01", "02"])
        assert ex.read_key_settings(db._duck)["trial"]["values"] == ["01", "02"]
        set_key_setting(db, "trial", None, None)
        assert "trial" not in ex.read_key_settings(db._duck)
        assert len(ex.key_setting_history(db._duck, "trial")) == 2

    def test_writing_the_same_setting_is_a_no_op(self, db):
        set_key_setting(db, "trial", SHARED)
        set_key_setting(db, "trial", SHARED)
        assert len(ex.key_setting_history(db._duck, "trial")) == 1

    def test_values_imply_shared(self, db):
        set_key_setting(db, "trial", None, ["1", "2"])
        assert ex.read_key_settings(db._duck)["trial"]["sharing"] == SHARED

    def test_values_on_a_per_parent_key_are_refused(self, db):
        with pytest.raises(ValueError, match="per parent"):
            set_key_setting(db, "trial", PER_PARENT, ["1"])

    def test_unknown_key_and_sharing_refused(self, db):
        with pytest.raises(ValueError, match="not a schema key"):
            set_key_setting(db, "nope", SHARED)
        with pytest.raises(ValueError, match="sharing"):
            set_key_setting(db, "trial", "sometimes")


# ---------------------------------------------------------------------------
# The universe and the four statuses
# ---------------------------------------------------------------------------


class TestDefaults:
    def test_keys_are_where_most_locations_are(self, db):
        result = expectation(db, ExRaw)
        assert result.keys == SCHEMA
        assert "every location" in result.keys_rule

    def test_per_parent_keys_expect_only_what_exists(self, db):
        result = expectation(db, "ExRaw")
        assert result.counts[PRESENT] == 6
        # S02 has no POST anywhere, so POST is not expected for S02.
        assert not any(r.values.get("session") == "POST" and r.values["subject"] == "S02"
                       for r in result.rows)

    def test_a_parent_with_nothing_below_is_one_wildcard_row(self, db):
        row = status_of(expectation(db, ExRaw), subject="S03", session=None, trial=None)
        assert row.status == MISSING_UNEXPECTED
        assert "any session under subject=S03" in row.why
        header, body = expectation(db, ExRaw).table(problems_only=True)
        assert body == [["S03", ALL_LEVELS, ALL_LEVELS, STATUS_LABELS[MISSING_UNEXPECTED], row.why]]


class TestSharedKeys:
    def test_shared_trial_crosses_everything(self, db):
        set_key_setting(db, "trial", SHARED)
        result = expectation(db, ExRaw)
        assert result.universe_size == 3 * 2 * 2
        row = status_of(result, subject="S02", session="POST", trial="1")
        assert row.status == MISSING_UNEXPECTED
        assert "every subject x session x trial combination is expected" in row.why

    def test_other_variables_having_data_is_said(self, db):
        ExB.save(1.0, subject="S02", session="POST", trial="1")
        set_key_setting(db, "trial", SHARED)
        row = status_of(expectation(db, ExRaw), subject="S02", session="POST", trial="1")
        assert "other variables have data here" in row.why

    def test_declared_values_keep_zero_padding_and_flag_extras(self, db):
        ExRaw.save(1.0, subject="S01", session="BL", trial="09")
        set_key_setting(db, "trial", SHARED, ["1", "2", "03"])
        result = expectation(db, ExRaw)
        row = status_of(result, subject="S01", session="BL", trial="03")
        assert row.status == MISSING_UNEXPECTED
        extra = status_of(result, subject="S01", session="BL", trial="09")
        assert extra.status == PRESENT_UNEXPECTED
        assert "trial 09 is not one of trial's declared values (1, 2, 03)" in extra.why


class TestOffLevel:
    def test_a_record_at_another_key_set_is_unexpectedly_present(self, db):
        ExRaw.save(1.0, subject="S01")
        result = expectation(db, ExRaw)
        assert result.keys == SCHEMA
        row = status_of(result, subject="S01")
        assert row.status == PRESENT_UNEXPECTED
        assert "saved at subject, while this variable is saved at subject/session/trial" in row.why

    def test_a_mark_can_accept_it(self, db):
        ExRaw.save(1.0, subject="S01")
        mark_expectation(db, ExRaw, True, "pilot summary", subject="S01")
        assert status_of(expectation(db, ExRaw), subject="S01").status == PRESENT


class TestExclusionWins:
    def test_excluded_beats_present_and_missing(self, db):
        exclude_schema("dropped out", subject="S02")
        result = expectation(db, ExRaw)
        row = status_of(result, subject="S02", session="BL", trial="1")
        assert row.status == EXCLUDED
        assert row.exclusion_reason == "dropped out"
        assert "dropped out" in row.why
        assert result.tone_counts()[EXCLUDED] == 2


# ---------------------------------------------------------------------------
# Marks
# ---------------------------------------------------------------------------


class TestMarks:
    def test_not_expected_turns_missing_green(self, db):
        mark_expectation(db, ExRaw, False, "never recorded", subject="S03")
        row = status_of(expectation(db, ExRaw), subject="S03", session=None, trial=None)
        assert row.status == MISSING_EXPECTED
        assert row.tone == ex.OK
        assert "marked not expected" in row.why and "never recorded" in row.why

    def test_not_expected_on_present_data_is_a_problem(self, db):
        mark_expectation(db, ExRaw, False, "should not exist", subject="S01", session="POST")
        row = status_of(expectation(db, ExRaw), subject="S01", session="POST", trial="1")
        assert row.status == PRESENT_UNEXPECTED
        assert "but data exists here" in row.why

    def test_expected_adds_a_combination_the_rule_did_not(self, db):
        mark_expectation(db, ExRaw, True, "recruited, file missing", subject="S04")
        row = status_of(expectation(db, ExRaw), subject="S04", session=None, trial=None)
        assert row.status == MISSING_UNEXPECTED
        assert "but nothing has been saved here" in row.why

    def test_most_specific_mark_wins(self, db):
        mark_expectation(db, ExRaw, False, "whole subject", subject="S01")
        mark_expectation(db, ExRaw, True, "this trial is fine", subject="S01", session="BL", trial="1")
        result = expectation(db, ExRaw)
        assert status_of(result, subject="S01", session="BL", trial="1").status == PRESENT
        assert status_of(result, subject="S01", session="BL", trial="2").status == PRESENT_UNEXPECTED

    def test_a_mark_naming_a_key_the_variable_lacks_does_not_apply(self, db):
        mark_expectation(db, ExSubject, False, "trial-level note", subject="S03", trial="1")
        row = status_of(expectation(db, ExSubject), subject="S03")
        assert row.status == PRESENT

    def test_reason_required_and_double_mark_refused(self, db):
        with pytest.raises(ValueError, match="reason"):
            mark_expectation(db, ExRaw, False, "  ", subject="S03")
        mark_expectation(db, ExRaw, False, "x", subject="S03")
        with pytest.raises(ValueError, match="already marked"):
            mark_expectation(db, ExRaw, False, "y", subject="S03")
        mark_expectation(db, ExRaw, True, "changed my mind", subject="S03")
        assert [m.expected for m in marks(db, ["ExRaw"])] == [True]

    def test_unmark_keeps_history(self, db):
        mark_expectation(db, ExRaw, False, "x", subject="S03")
        unmark_expectation(db, ExRaw, "mistake", subject="S03")
        assert marks(db, ["ExRaw"]) == []
        history = mark_history(db, ExRaw)
        assert [h["active"] for h in history] == [True, False]
        with pytest.raises(ValueError, match="no mark"):
            unmark_expectation(db, ExRaw, subject="S03")

    def test_unknown_key_and_no_key_refused(self, db):
        with pytest.raises(ValueError, match="Unknown schema key"):
            mark_expectation(db, ExRaw, False, "x", visit="1")
        with pytest.raises(ValueError, match="at least one"):
            mark_expectation(db, ExRaw, False, "x")


class TestInheritance:
    @pytest.fixture
    def piped(self, db):
        for_each(
            ex_double,
            {"signal": ExRaw},
            [ExFilt],
            subject=["S01", "S02"],
            session=["BL", "POST"],
            trial=["1", "2"],
        )
        return db

    def test_upstream_and_downstream_types(self, piped):
        assert "ExRaw" in related_variable_types(piped._duck, "ExFilt", "upstream")
        assert "ExFilt" in related_variable_types(piped._duck, "ExRaw", "downstream")
        assert not any(t.startswith("__") for t in related_variable_types(piped._duck, "ExFilt"))

    def test_a_mark_upstream_is_inherited(self, piped):
        mark_expectation(piped, ExRaw, False, "sensor off", subject="S03")
        row = status_of(expectation(piped, ExFilt), subject="S03", session=None, trial=None)
        assert row.status == MISSING_EXPECTED
        assert row.inherited_from == "ExRaw"
        assert "in ExRaw" in row.why

    def test_own_mark_beats_inherited(self, piped):
        mark_expectation(piped, ExRaw, False, "sensor off", subject="S03")
        mark_expectation(piped, ExFilt, True, "should have been recomputed", subject="S03")
        row = status_of(expectation(piped, ExFilt), subject="S03", session=None, trial=None)
        assert row.status == MISSING_UNEXPECTED
        assert row.inherited_from is None


class TestRareValues:
    def test_a_value_few_variables_have_is_pointed_at(self, db):
        for v in (ExB, ExC):
            v.save(1.0, subject="S01", session="BL", trial="1")
        ExB.save(1.0, subject="S7", session="BL", trial="1")
        result = expectation(db, ExC)
        row = status_of(result, subject="S7", session="BL", trial="1")
        assert row.status == MISSING_UNEXPECTED
        assert "subject S7 appears only in ExB" in row.why
        assert any("S7" in n for n in result.notes)


# ---------------------------------------------------------------------------
# Shapes and owners
# ---------------------------------------------------------------------------


class TestOwners:
    def test_every_status_has_a_label_and_a_tone(self):
        assert set(STATUS_LABELS) == set(STATUSES) == set(TONE)
        assert {TONE[s] for s in STATUSES} == {ex.OK, ex.PROBLEM, EXCLUDED}

    def test_most_specific_match_is_shared_with_exclusions(self):
        from scidb.store import exclusions

        class R:
            def __init__(self, keys, at):
                self.keys, self.changed_at = keys, at

        old = R({"subject": "S1"}, datetime(2026, 1, 1))
        new = R({"subject": "S1"}, datetime(2026, 2, 1))
        deep = R({"subject": "S1", "trial": "1"}, datetime(2025, 1, 1))
        assert most_specific_match([old, new], {"subject": "S1", "trial": "2"}) is new
        assert most_specific_match([old, deep], {"subject": "S1", "trial": "1"}) is deep
        assert most_specific_match([deep], {"subject": "S1"}) is None
        assert exclusions.most_specific_match is most_specific_match

    def test_to_dict_is_json_and_filters(self, db):
        mark_expectation(db, ExRaw, False, "x", subject="S03")
        result = expectation(db, ExRaw)
        payload = json.loads(json.dumps(result.to_dict()))
        assert payload["counts"][MISSING_EXPECTED] == 1
        assert payload["labels"] == STATUS_LABELS
        assert json.loads(json.dumps(result.to_dict(problems_only=True)))["rows"] == []

    def test_by_counts_per_value(self, db):
        by = dict(expectation(db, ExRaw).by("subject"))
        assert by["S01"][PRESENT] == 4
        assert by["S03"][MISSING_UNEXPECTED] == 1

    def test_unknown_variable_raises(self, db):
        with pytest.raises(ValueError, match="No variable named"):
            expectation(db, "Nope")

    def test_cap_refuses_a_huge_universe(self, db):
        set_key_setting(db, "trial", SHARED)
        with pytest.raises(ValueError, match="limit"):
            expectation(db, ExRaw, max_combinations=5)



class TestParseValuesText:
    def test_lists_and_ranges(self):
        assert ex.parse_values_text("1..3, 7") == ["1", "2", "3", "7"]

    def test_leading_zero_keeps_width(self):
        assert ex.parse_values_text("08..11") == ["08", "09", "10", "11"]

    def test_text_values_and_duplicates(self):
        assert ex.parse_values_text("BL, POST, BL") == ["BL", "POST"]

    def test_bad_range_and_empty_refused(self):
        with pytest.raises(ValueError, match="range"):
            ex.parse_values_text("5..1")
        with pytest.raises(ValueError, match="No values"):
            ex.parse_values_text(" , ")


# ---------------------------------------------------------------------------
# Stage 2: Inspector and CLI (the GUI reuses the same Inspector methods)
# ---------------------------------------------------------------------------


@pytest.fixture
def closed_db(tmp_path):
    """A populated database, closed, for the CLI to open itself."""
    path = tmp_path / "cli.duckdb"
    database = configure_database(path, SCHEMA)
    populate()
    for_each(
        ex_double, {"signal": ExRaw}, [ExFilt],
        subject=["S01", "S02"], session=["BL", "POST"], trial=["1", "2"],
    )
    database.close()
    return path


class TestInspector:
    def test_variable_names_its_producer_consumers_and_neighbours(self, closed_db):
        from scidb.inspect import Inspector

        with Inspector.open(closed_db) as insp:
            filt = insp.variable("ExFilt")
            raw = insp.variable("ExRaw")
        assert filt.producers == ["ex_double"]
        assert filt.fed_by == ["ExRaw"]
        assert raw.consumers == ["ex_double"]
        assert raw.feeds == ["ExFilt"]
        assert raw.saved_directly and not raw.producers

    def test_expectation_unknown_variable_is_not_found(self, closed_db):
        from scidb.inspect import Inspector
        from scidb.inspect.api import NotFoundError

        with Inspector.open(closed_db) as insp:
            with pytest.raises(NotFoundError):
                insp.expectation("Nope")


class TestCli:
    def run(self, path, *argv):
        from scidb.inspect.cli import main

        return main(["--db", str(path), *argv])

    def test_expected_marks_and_history(self, closed_db, capsys):
        assert self.run(closed_db, "expected", "ExFilt", "--problems", "--no-color") == 0
        out = capsys.readouterr().out
        assert "Missing unexpectedly" in out and "any session under subject=S03" in out

        assert self.run(
            closed_db, "mark", "ExRaw", "subject=S03", "--not-expected", "--reason", "never recorded"
        ) == 0
        capsys.readouterr()
        assert self.run(closed_db, "expected", "ExFilt", "--problems") == 0
        assert "nothing unexpected" in capsys.readouterr().out

        assert self.run(closed_db, "marks") == 0
        assert "never recorded" in capsys.readouterr().out
        assert self.run(closed_db, "unmark", "ExRaw", "subject=S03") == 0
        assert self.run(closed_db, "marks", "--history") == 0
        out = capsys.readouterr().out
        assert "unmarked" in out and "not expected" in out

    def test_double_mark_is_refused_with_exit_1(self, closed_db, capsys):
        args = ("mark", "ExRaw", "subject=S03", "--not-expected", "--reason", "x")
        assert self.run(closed_db, *args) == 0
        assert self.run(closed_db, *args) == 1
        assert "already marked" in capsys.readouterr().err

    def test_set_key_and_keys(self, closed_db, capsys):
        assert self.run(closed_db, "set-key", "trial", "shared", "--values", "1..3") == 0
        assert self.run(closed_db, "keys") == 0
        out = capsys.readouterr().out
        assert "1, 2, 3" in out and "left of shared" in out
        assert self.run(closed_db, "keys", "--history", "--json") == 0
        assert json.loads(capsys.readouterr().out)[0]["values"] == ["1", "2", "3"]

    def test_expected_json_csv_and_by(self, closed_db, capsys, tmp_path):
        out_csv = tmp_path / "e.csv"
        assert self.run(closed_db, "expected", "ExRaw", "--json", "--csv", str(out_csv)) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["counts"][PRESENT] == 6
        assert out_csv.read_text().splitlines()[0] == "subject,session,trial,status,why"
        assert self.run(closed_db, "expected", "ExRaw", "--by", "subject") == 0
        assert "S03" in capsys.readouterr().out

    def test_unknown_status_refused(self, closed_db, capsys):
        assert self.run(closed_db, "expected", "ExRaw", "--status", "weird") != 0

    def test_exclusions_history(self, closed_db, capsys):
        assert self.run(closed_db, "exclude", "subject=S02", "--reason", "dropped out") == 0
        assert self.run(closed_db, "include", "subject=S02", "--reason", "came back") == 0
        capsys.readouterr()
        assert self.run(closed_db, "exclusions", "--history") == 0
        out = capsys.readouterr().out
        assert "exclude" in out and "include" in out and "came back" in out
        assert self.run(closed_db, "exclusions") == 0
        assert "no schema exclusions" in capsys.readouterr().out

    def test_vars_shows_producers(self, closed_db, capsys):
        assert self.run(closed_db, "vars", "ExFilt") == 0
        out = capsys.readouterr().out
        assert "produced by    ex_double" in out and "fed by         ExRaw" in out


# ---------------------------------------------------------------------------
# Stage 6: the location picker reads the same owner of "should exist"
# ---------------------------------------------------------------------------


class TestPickerParity:
    def leaves(self, db, variable):
        from scidb.provenance.locations import _leaf_states_of, location_states

        return _leaf_states_of(location_states(variable, db=db), {})

    def combo(self, row):
        return tuple((k, v) for k, v in row.values.items() if v is not None)

    def test_every_missing_unexpected_location_is_red(self, db):
        set_key_setting(db, "trial", SHARED)  # adds S02/POST/*, S03/* beyond what ran
        leaves = self.leaves(db, "ExRaw")
        missing = [r for r in expectation(db, ExRaw).rows if r.status == MISSING_UNEXPECTED]
        assert missing
        for r in missing:
            assert leaves.get(self.combo(r)) == "red", self.combo(r)

    def test_a_whole_missing_parent_is_one_red_node(self, db):
        leaves = self.leaves(db, "ExRaw")
        assert leaves.get((("subject", "S03"),)) == "red"

    def test_missing_as_expected_is_grey(self, db):
        set_key_setting(db, "trial", SHARED)
        mark_expectation(db, ExRaw, False, "never recorded", subject="S02", session="POST")
        leaves = self.leaves(db, "ExRaw")
        assert leaves[(("subject", "S02"), ("session", "POST"), ("trial", "1"))] == "grey"

    def test_unexpected_data_is_counted_in_a_note(self, db):
        from scidb.provenance.locations import location_states

        mark_expectation(db, ExRaw, False, "should not exist", subject="S01", session="POST")
        tree = location_states("ExRaw", db=db)
        assert any("not expected" in n for n in tree.notes)
