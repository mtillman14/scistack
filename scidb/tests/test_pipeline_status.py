"""GitHub #28: pipeline introspection in a scientist's words.

Plan: ``.claude/plan-pipeline-introspection.md``. Doc: ``docs/claude/pipeline-status.md``.

Stage 1  scoped exclusions (``variable=``), the mark presets, for_each honouring them
Stage 2  between keys and planned locations within strata
Stage 3  ``scidb.provenance.status``: location states and the step verdict, with
         parity against ``check_node_state`` (needs running) and
         ``check_combo_state`` (each state)
Stage 4  findings (computed in the run), the outdated flag, "what's new"
Stage 5  ``delete_plan`` location targets, bulk "clean up outdated"
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.provenance import findings as fnd
from scidb.provenance import status as st
from scidb.provenance.state import check_combo_state, check_node_state
from scidb.store import exclusions as ex
from scidb.store.between_keys import between_keys, set_between, set_between_keys
from scifor.project import schema_order

SCHEMA = ["subject", "trial"]


class PsRaw(BaseVariable):
    pass


class PsOther(BaseVariable):
    pass


class PsFilt(BaseVariable):
    pass


class PsBoth(BaseVariable):
    pass


class PsOut(BaseVariable):
    pass


def ps_scale(signal):
    return signal * 2.0


def ps_pair(a, b):
    return a + b


def ps_fragile(signal):
    if float(np.ravel(signal)[0]) > 5:
        raise ValueError("too big")
    return signal


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    schema_order.clear_cache()
    _scifor.set_schema([])
    yield
    _scifor.set_schema([])
    schema_order.clear_cache()


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "ps.duckdb", SCHEMA)
    yield database
    database.close()


def _states(vs: st.VariableStatus) -> dict:
    return {tuple(loc.location[k] for k in vs.keys): loc.state for loc in vs.locations}


def _seed(subjects=("S01", "S02"), trials=("t1", "t2"), cls=PsRaw, value=1.0):
    for s in subjects:
        for t in trials:
            cls.save(np.array([value, value + 1.0]), subject=s, trial=t)


def _run(fn=ps_scale, inputs=None, outputs=(PsFilt,), **kw):
    for_each(fn, inputs or {"signal": PsRaw}, list(outputs), subject=[], trial=[], **kw)


# ---------------------------------------------------------------------------
# Stage 1: scoped exclusions
# ---------------------------------------------------------------------------


class TestScopedExclusions:
    def test_variable_scope_of_a_class_is_its_name(self):
        # Regression: a variable class overloads == (VariableFilter, truthy),
        # so `variable == ""` once turned every class scope into "everything".
        assert ex.variable_scope(PsRaw) == "PsRaw"
        assert ex.variable_scope("PsRaw") == "PsRaw"
        assert ex.variable_scope("") is None
        assert ex.variable_scope(None) is None

    def test_unscoped_and_wildcard_cells_are_none_not_nan(self, db):
        """Regression (2026-11-21): with a scoped and an unscoped row listed
        together, pandas made the unscoped ``scoped_to`` NaN, which the MATLAB
        wrapper's table conversion showed as the text "NaN"."""
        ex.exclude_schema("withdrew", db=db, subject="S01")
        ex.exclude_schema("emg bad", db=db, variable=PsRaw, subject="S01")
        df = ex.list_exclusions(db=db)
        assert sorted(df["scoped_to"], key=str) == [None, "PsRaw"]
        assert list(df["trial"]) == [None, None]  # the wildcard key

    def test_dict_entry_point_takes_the_scope(self, db):
        """The MATLAB wrappers' path: ``*_schema_dict(keys, reason, None,
        variable=...)`` (scimatlab +scidb/+internal/exclusion_args.m)."""
        ex.exclude_schema_dict({"subject": "S01"}, "emg bad", None, variable="PsRaw")
        assert list(ex.list_exclusions(db=db)["scoped_to"]) == ["PsRaw"]
        ex.include_schema_dict({"subject": "S01"}, "fixed", None, variable="PsRaw")
        assert len(ex.list_exclusions(db=db)) == 0

    def test_scoped_row_is_listed_with_its_variable(self, db):
        ex.exclude_schema("emg bad", db=db, variable=PsRaw, subject="S01", trial="t1")
        df = ex.list_exclusions(db=db)
        assert list(df["scoped_to"]) == ["PsRaw"]

    def test_unscoped_question_ignores_scoped_rows(self, db):
        ex.exclude_schema("emg bad", db=db, variable="PsRaw", subject="S01")
        assert ex.exclusion_status([{"subject": "S01", "trial": "t1"}], SCHEMA, db) == [False]
        assert ex.exclusion_status(
            [{"subject": "S01", "trial": "t1"}], SCHEMA, db, variables=["PsRaw"]
        ) == [True]
        assert ex.exclusion_status(
            [{"subject": "S01", "trial": "t1"}], SCHEMA, db, variables=["PsOther"]
        ) == [False]

    def test_for_each_skips_a_location_scoped_to_its_input(self, db):
        _seed()
        _seed(cls=PsOther)
        ex.exclude_schema("emg bad", db=db, variable=PsRaw, subject="S01", trial="t1")
        _run()
        _run(inputs={"signal": PsOther}, outputs=(PsBoth,))
        assert PsFilt.load(subject="S01", trial="t2") is not None
        with pytest.raises(Exception):
            PsFilt.load(subject="S01", trial="t1")
        # A step reading another variable is not affected.
        assert PsBoth.load(subject="S01", trial="t1") is not None

    def test_scoped_include_reopens_a_global_exclusion_for_one_variable(self, db):
        ex.exclude_schema("bad trial", db=db, subject="S01", trial="t1")
        ex.include_schema("emg was fine", db=db, variable="PsRaw", subject="S01", trial="t1")
        combo = [{"subject": "S01", "trial": "t1"}]
        assert ex.exclusion_status(combo, SCHEMA, db, variables=["PsRaw"]) == [True], (
            "the global exclusion still governs everything else the step reads"
        )
        (o,) = ex.governing_overrides(combo, SCHEMA, db, variable="PsRaw")
        assert o.included and o.variable == "PsRaw"

    def test_mark_presets(self, db):
        ex.mark_missing_expected(PsRaw, db=db, subject="S02")
        (e,) = ex.governing_exclusions([{"subject": "S02", "trial": "t1"}], SCHEMA, db, ["PsRaw"])
        assert e.reason == ex.MARKED_MISSING_REASON
        assert e.inherited_from("PsFilt") == "PsRaw"
        assert ex.mark_present_expected(PsRaw, db=db, subject="S02") is True
        assert ex.mark_present_expected(PsRaw, db=db, subject="S02") is False

    def test_a_schema_key_named_variable_makes_the_keyword_ambiguous(self, tmp_path):
        d = configure_database(tmp_path / "v.duckdb", ["variable", "trial"])
        try:
            with pytest.raises(ValueError, match="ambiguous"):
                ex.exclude_schema("x", db=d, variable="PsRaw", trial="1")
            ex.exclude_schema_dict({"variable": "emg"}, "x", db=d, variable="PsRaw")
            assert ex.override_history(d)[0].variable == "PsRaw"
        finally:
            d.close()


# ---------------------------------------------------------------------------
# Stage 2: between keys
# ---------------------------------------------------------------------------


class TestBetweenKeys:
    def test_declared_explicitly_and_read_in_schema_order(self, db):
        assert between_keys(db) == []
        assert set_between(db, "subject", True) is True
        assert set_between(db, "subject", True) is False
        assert between_keys(db) == ["subject"]
        assert set_between_keys(db, []) == ["subject"]
        assert between_keys(db) == []
        with pytest.raises(ValueError):
            set_between_keys(db, ["subject", "trial"])

    def test_planned_locations_take_levels_within_each_stratum(self, db):
        PsRaw.save(1.0, subject="S01", trial="t1")
        PsRaw.save(1.0, subject="S01", trial="t2")
        PsRaw.save(1.0, subject="S02", trial="t3")
        crossed = st.planned_locations(db, SCHEMA)
        assert len(crossed) == 6
        set_between(db, "subject", True)
        within = st.planned_locations(db, SCHEMA)
        assert sorted((p["subject"], p["trial"]) for p in within) == [
            ("S01", "t1"), ("S01", "t2"), ("S02", "t3"),
        ]

    def test_one_call_has_one_planned_location(self, db):
        assert st.planned_locations(db, []) == [{}]


# ---------------------------------------------------------------------------
# Stage 3: states and verdict, with parity
# ---------------------------------------------------------------------------


@pytest.fixture
def states_db(db):
    """(S01,t1) present · (S01,t2) outdated (input re-saved) · (S01,t3) new
    (input added after the run) · (S02,t1) excluded · (S02,t2) present ·
    (S02,t3) missing input (no PsRaw there)."""
    _seed()
    _run()
    PsRaw.save(np.array([7.0, 8.0]), subject="S01", trial="t2")
    PsRaw.save(np.array([9.0, 9.0]), subject="S01", trial="t3")
    ex.exclude_schema("sensor fell off", db=db, subject="S02", trial="t1")
    return db


class TestLocationStates:
    def test_one_of_each(self, states_db):
        v = st.step_status(states_db, ps_scale, [PsFilt])
        states = _states(v.outputs[0])
        assert states == {
            ("S01", "t1"): st.PRESENT,
            ("S01", "t2"): st.OUTDATED,
            ("S01", "t3"): st.NEW,
            ("S02", "t1"): st.EXCLUDED,
            ("S02", "t2"): st.PRESENT,
            ("S02", "t3"): st.MISSING_INPUT,
        }
        out = {tuple(l.location.values()): l for l in v.outputs[0].locations}
        assert out[("S01", "t2")].outdated_by == st.INPUTS_CHANGED
        assert out[("S02", "t3")].missing_input == "PsRaw"
        assert out[("S02", "t1")].exclusion["reason"] == "sensor fell off"

    def test_each_state_agrees_with_check_combo_state(self, states_db):
        v = st.step_status(states_db, ps_scale, [PsFilt])
        expected = {
            st.PRESENT: "up_to_date",
            st.OUTDATED: "stale",
            st.NEW: "missing",
            st.NOT_RUN: "missing",
            st.MISSING_INPUT: "missing",
        }
        for loc in v.outputs[0].locations:
            if loc.state == st.EXCLUDED:
                continue
            got = check_combo_state(ps_scale, [PsFilt], loc.location, db=states_db)
            assert got == expected[loc.state], (loc.location, loc.state, got)

    def test_needs_running_agrees_with_check_node_state(self, states_db):
        v = st.step_status(states_db, ps_scale, [PsFilt])
        node = check_node_state(ps_scale, [PsFilt], db=states_db)
        assert v.needs_running == (node["state"] == "red") is True

    def test_verdict_word_and_reasons(self, states_db):
        v = st.step_status(states_db, ps_scale, [PsFilt])
        assert v.word == st.NOT_COMPLETE
        assert [r.kind for r in v.reasons] == [st.MISSING_INPUT, st.INPUTS_CHANGED]
        assert v.new == 1 and v.excluded == 1
        assert v.chip() == "↓ 1 missing input"
        assert v.new_chip() == "＋1 new"
        assert "＋1 new to process" in v.status_line()


class TestVerdictLifecycle:
    """Resolved 5: never run, then run, then a subject added."""

    def test_not_run_then_succeeded_then_new_subject(self, db):
        _seed(subjects=("S01",))
        before = st.step_status(db, ps_scale, [PsFilt], inputs={"signal": PsRaw})
        assert before.word == st.NOT_COMPLETE
        assert [r.kind for r in before.reasons] == [st.NOT_RUN]
        assert before.needs_running
        assert check_node_state(ps_scale, [PsFilt], inputs={"signal": PsRaw}, db=db)["state"] == "red"

        _run()
        done = st.step_status(db, ps_scale, [PsFilt])
        assert done.word == st.SUCCEEDED
        assert done.present == done.counted == 2
        assert not done.needs_running
        assert check_node_state(ps_scale, [PsFilt], db=db)["state"] == "green"
        assert done.chip() == ""

        _seed(subjects=("S02",))
        later = st.step_status(db, ps_scale, [PsFilt])
        # New data never makes a finished step look broken; it needs running.
        assert later.word == st.SUCCEEDED
        assert later.new == 2
        assert later.needs_running
        assert check_node_state(ps_scale, [PsFilt], db=db)["state"] == "red"
        new_at = sorted(
            tuple(loc.location.values())
            for loc in later.outputs[0].locations if loc.state == st.NEW
        )
        assert new_at == [("S02", "t1"), ("S02", "t2")]

    def test_unresolvable_level_reads_not_run_without_counts(self, db):
        v = st.step_status(db, "never_defined_fn", ["PsFilt"])
        assert v.word == st.NOT_COMPLETE
        assert v.reasons[0].kind == st.NOT_RUN and v.reasons[0].count is None
        assert v.status_line().startswith("✕ Not complete")
        assert "0 of 0" not in v.status_line()


class TestFailedAndMissingInput:
    def test_failed_keeps_the_error_text(self, db):
        PsRaw.save(np.array([1.0]), subject="S01", trial="t1")
        PsRaw.save(np.array([9.0]), subject="S01", trial="t2")
        for_each(ps_fragile, {"signal": PsRaw}, [PsFilt], subject=[], trial=[])
        v = st.step_status(db, ps_fragile, [PsFilt])
        out = {tuple(l.location.values()): l for l in v.outputs[0].locations}
        assert out[("S01", "t2")].state == st.FAILED
        assert "too big" in (out[("S01", "t2")].error or "")
        assert v.reasons[0].kind == st.FAILED and v.chip() == "✕ 1 failed"

    def test_missing_input_names_the_input(self, db):
        PsRaw.save(1.0, subject="S01", trial="t1")
        PsRaw.save(2.0, subject="S01", trial="t2")
        PsOther.save(1.0, subject="S01", trial="t1")
        for_each(ps_pair, {"a": PsRaw, "b": PsOther}, [PsBoth], subject=[], trial=[])
        v = st.step_status(db, ps_pair, [PsBoth])
        out = {tuple(l.location.values()): l for l in v.outputs[0].locations}
        assert out[("S01", "t2")].state == st.MISSING_INPUT
        assert out[("S01", "t2")].missing_input == "PsOther"
        assert not v.needs_running

    def test_scoped_exclusion_is_inherited_by_the_output(self, db):
        _seed()
        ex.exclude_schema("emg bad", db=db, variable=PsRaw, subject="S01", trial="t1")
        _run()
        v = st.step_status(db, ps_scale, [PsFilt])
        out = {tuple(l.location.values()): l for l in v.outputs[0].locations}
        loc = out[("S01", "t1")]
        assert loc.state == st.EXCLUDED
        assert loc.exclusion["inherited_from"] == "PsRaw"
        assert "inherited from PsRaw" in loc.detail
        assert v.word == st.SUCCEEDED and v.chip() == "⊘ 1 excluded"


class TestCodeChanged:
    def test_an_edited_function_reads_code_changed(self, db):
        _seed(subjects=("S01",))
        _run()

        def ps_scale_v2(signal):
            return signal * 3.0

        ps_scale_v2.__name__ = "ps_scale"
        v = st.step_status(db, ps_scale_v2, [PsFilt])
        assert {l.state for l in v.outputs[0].locations} == {st.OUTDATED}
        assert v.reasons[0].kind == st.CODE_CHANGED
        assert v.chip().startswith("✎ code changed")
        assert v.needs_running == (check_node_state(ps_scale_v2, [PsFilt], db=db)["state"] == "red")


# ---------------------------------------------------------------------------
# Stage 4: findings, outdated flag, what's new
# ---------------------------------------------------------------------------


def ps_nan(signal):
    out = np.asarray(signal, dtype=float).copy()
    if out[0] > 5:
        out[1] = np.nan
    return out


def ps_all_nan(signal):
    return np.full(3, np.nan)


def ps_mean(signal):
    return float(np.mean(signal))


class TestFindings:
    def test_nan_introduced_is_stored_and_read_as_a_warning(self, db):
        PsRaw.save(np.array([1.0, 2.0]), subject="S01", trial="t1")
        PsRaw.save(np.array([9.0, 2.0]), subject="S01", trial="t2")
        for_each(ps_nan, {"signal": PsRaw}, [PsFilt], subject=[], trial=[])
        v = st.step_status(db, ps_nan, [PsFilt])
        out = {tuple(l.location.values()): l for l in v.outputs[0].locations}
        assert out[("S01", "t1")].state == st.PRESENT
        assert out[("S01", "t2")].state == st.WARNING
        assert out[("S01", "t2")].findings[0]["kind"] == fnd.NAN_INTRODUCED
        assert v.word == st.SUCCEEDED_WITH_WARNINGS
        assert v.chip() == "⚠ 1 warning"

    def test_all_nan(self, db):
        PsRaw.save(np.array([1.0]), subject="S01", trial="t1")
        for_each(ps_all_nan, {"signal": PsRaw}, [PsFilt], subject=[], trial=[])
        rid = PsFilt.load(subject="S01", trial="t1").record_id
        kinds = [f["kind"] for f in fnd.findings_for_records(db, [rid])[rid]]
        assert kinds == [fnd.ALL_NAN]

    def test_outlier_across_siblings(self, db):
        for i in range(10):
            PsRaw.save(np.array([1.0 + 0.01 * i]), subject=f"S{i:02d}", trial="t1")
        PsRaw.save(np.array([500.0]), subject="S99", trial="t1")
        for_each(ps_mean, {"signal": PsRaw}, [PsOut], subject=[], trial=[])
        rid = PsOut.load(subject="S99", trial="t1").record_id
        assert [f["kind"] for f in fnd.findings_for_records(db, [rid]).get(rid, [])] == [fnd.OUTLIER]

    def test_healthy_records_write_nothing(self, db):
        _seed(subjects=("S01",))
        _run()
        n = db._duck.fetchone("SELECT COUNT(*) FROM _run_outcome WHERE outcome = 'finding'")[0]
        assert n == 0

    def test_check_saved_does_not_write(self, db):
        found = fnd.check_saved(db, [fnd.SavedOutput("PsFilt", "r1", {"subject": "S01"}, np.array([]))])
        assert [f.kind for f in found] == [fnd.EMPTY]
        assert db._duck.fetchone("SELECT COUNT(*) FROM _run_outcome")[0] == 0


class TestOutdatedFlag:
    def test_card_made_by_old_code_is_outdated(self, db):
        from scidb.inspect.variant_cards import outdated_cleanup, variant_outdated

        _seed(subjects=("S01",))
        _run()

        def ps_scale_v2(signal):
            return signal * 3.0

        ps_scale_v2.__name__ = "ps_scale"
        rows = variant_outdated(db, "PsFilt", {"ps_scale": ps_scale_v2})
        assert [r["outdated"] for r in rows] == [True]
        assert "current code is ps_scale" in rows[0]["text"]
        assert [r["outdated"] for r in variant_outdated(db, "PsFilt", {"ps_scale": ps_scale})] == [False]
        assert [r["outdated"] for r in variant_outdated(db, "PsFilt")] == [None]

        plan = outdated_cleanup(db, {"ps_scale": ps_scale_v2})
        (cand,) = plan["candidates"]
        assert cand["variable"] == "PsFilt" and cand["records"] == 2
        assert cand["warn"] and not cand["ticked"], "the only variant is the default: say so"


class TestWhatsNew:
    def test_new_locations_are_announced_once_and_compactly(self, db):
        from scidb.provenance.run_summary import run_summaries
        from scidb.provenance.whats_new import whats_new

        for t in range(1, 11):
            PsRaw.save(float(t), subject="1", trial=str(t))
        for_each(ps_mean, {"signal": PsRaw}, [PsOut], subject=[], trial=[])
        for t in range(11, 21):
            PsRaw.save(float(t), subject="1", trial=str(t))
        for_each(ps_mean, {"signal": PsRaw}, [PsOut], subject=[], trial=[])
        latest = run_summaries(db._duck, "ps_mean", limit=1)[0]
        (news,) = whats_new(db, latest)
        assert news.headline() == "PsOut: 10 new locations — subject=1, trial=11–20"
        assert news.locations_before == 10 and news.locations_after == 20
        assert "subject 1: 10 → 20 trials" in news.changes

        for_each(ps_mean, {"signal": PsRaw}, [PsOut], subject=[], trial=[])
        again = run_summaries(db._duck, "ps_mean", limit=1)[0]
        assert [n.new_locations for n in whats_new(db, again)] == [[]]

    def test_run_report_leads_with_the_verdict(self, db):
        from scidb.provenance.run_report import run_report
        from scidb.provenance.run_summary import run_summaries

        _seed(subjects=("S01",))
        _run()
        s = run_summaries(db._duck, "ps_scale", limit=1)[0]
        d = run_report(db, s.run_id).to_dict()
        assert list(d)[:3] == ["verdict", "whats_new", "findings"]
        assert d["verdict"]["word"] == st.SUCCEEDED
        assert d["whats_new"][0]["new_count"] == 2


# ---------------------------------------------------------------------------
# Stage 5: per-location delete
# ---------------------------------------------------------------------------


class TestDeleteAtLocations:
    def test_a_location_selection_restricts_the_seed_records(self, db):
        from scidb.store.variant_delete import delete_plan, delete_variant

        _seed()
        _run()
        full = delete_plan(db, [{"variable": "PsFilt"}])
        here = delete_plan(db, [{"variable": "PsFilt", "locations": {"include": [[["subject", "S01"]]]}}])
        assert full.seed_by_variable == {"PsFilt": 4}
        assert here.seed_by_variable == {"PsFilt": 2}
        delete_variant(db, here.targets, "erroneous", expect_fingerprint=here.fingerprint)
        assert PsFilt.load(subject="S02", trial="t1") is not None
        with pytest.raises(Exception):
            PsFilt.load(subject="S01", trial="t1")


# ---------------------------------------------------------------------------
# Stage 9: Try on… (nothing written, not even _run_summary)
# ---------------------------------------------------------------------------


def ps_noisy(signal):
    print("hello from the function")
    return np.asarray(signal, dtype=float) * 2.0


class TestTry:
    def test_try_returns_outputs_and_writes_nothing(self, db):
        from scidb.run.try_step import try_step

        _seed(subjects=("S01",))
        n_records = db._duck.fetchone("SELECT COUNT(*) FROM _record")[0]
        res = try_step(
            ps_noisy, {"signal": PsRaw}, [PsFilt], db=db,
            locations={"include": [[["subject", "S01"], ["trial", "t1"]]]},
            subject=[], trial=[],
        )
        assert res.total_rows == 1
        assert res.rows[0]["location"] == {"subject": "S01", "trial": "t1"}
        assert res.rows[0]["outputs"]["PsFilt"]["shape_text"] == "2"
        assert "hello from the function" in res.printed
        assert db._duck.fetchone("SELECT COUNT(*) FROM _record")[0] == n_records
        assert db._duck.fetchone("SELECT COUNT(*) FROM _run_summary")[0] == 0
        assert db._duck.fetchone("SELECT COUNT(*) FROM _run_outcome")[0] == 0

    def test_try_computes_findings_without_storing_them(self, db):
        from scidb.run.try_step import try_step

        PsRaw.save(np.array([1.0]), subject="S01", trial="t1")
        res = try_step(ps_all_nan, {"signal": PsRaw}, [PsFilt], db=db, subject=[], trial=[])
        assert [f["kind"] for f in res.findings] == [fnd.ALL_NAN]
        assert db._duck.fetchone("SELECT COUNT(*) FROM _run_outcome")[0] == 0


# ---------------------------------------------------------------------------
# CLI: one ``status`` subcommand owns the name (overview, or a step verdict)
# ---------------------------------------------------------------------------


class TestStatusCli:
    """Regression: a second ``status`` subparser for the verdict collided with
    the overview (``conflicting subparser: status``) and broke every CLI
    command, since the parser is built whole. One subcommand, two forms."""

    def test_the_parser_builds_with_both_forms(self):
        from scidb.inspect.cli import build_parser

        build_parser()  # raised argparse.ArgumentError before the fix

    def test_bare_status_is_the_overview_and_fn_is_the_verdict(self, db, tmp_path, capsys):
        import json

        from scidb.inspect.cli import main

        _seed(subjects=("S01",))
        _run()
        db.close()
        path = str(tmp_path / "ps.duckdb")

        assert main(["--db", path, "status", "--json"]) == 0
        overview = json.loads(capsys.readouterr().out)
        assert "n_records" in overview

        assert main(["--db", path, "status", "ps_scale", "--json"]) == 0
        verdict = json.loads(capsys.readouterr().out)
        assert verdict["function_name"] == "ps_scale"

        assert main(["--db", path, "status", "ps_scale"]) == 0
        assert capsys.readouterr().out.startswith("ps_scale: ")
