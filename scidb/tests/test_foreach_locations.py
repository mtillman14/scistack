"""``for_each(locations=...)`` through the DB-backed wrapper.

Stage 2 of ``.claude/plan-schema-key-picker-and-level-order.md``. The RULE
lives in ``scifor.locations`` and is tested there against the shared cases
(``docs/claude/location-filter-cases.json``); what is tested here is that scidb
actually HANDS it over — on the real path, on the dry-run path, and through the
EachOf recursion — and that the narrowing shows up as records not written.

That distinction matters: scidb passes its own pre-built combo list to scifor
via ``LoopHooks.all_combos``, so a filter applied only to scifor's Cartesian branch
would be silently inert on every database-backed run, which is every run the
GUI makes.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, EachOf, Fixed, configure_database, for_each

SCHEMA = ["subject", "trial"]
SUBJECTS = ["S01", "S02"]
TRIALS = ["t1", "t2"]


class LocInput(BaseVariable):
    pass


class LocOutput(BaseVariable):
    pass


def double(signal):
    return signal * 2


def scale(signal, factor):
    return signal * factor


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "foreach_locations.duckdb", SCHEMA)
    for subject in SUBJECTS:
        for trial in TRIALS:
            LocInput.save(np.array([1.0, 2.0]), subject=subject, trial=trial)
    yield database
    _scifor.set_schema([])
    database.close()


def _locations_written(db, variable=LocOutput) -> set[tuple[str, str]]:
    frame = db.load_all_as_df(variable, stringify_schema=True)
    if len(frame) == 0:
        return set()
    return {(row.subject, row.trial) for row in frame.itertuples()}


class TestPrefixes:
    def test_a_prefix_narrows_what_runs(self, db):
        for_each(
            double,
            {"signal": LocInput},
            [LocOutput],
            subject=SUBJECTS,
            trial=TRIALS,
            locations={"include": [[["subject", "S01"]]]},
        )
        assert _locations_written(db) == {("S01", "t1"), ("S01", "t2")}

    def test_a_ragged_selection_runs_exactly_its_locations(self, db):
        """All of S01, plus ONE trial of S02 — the shape no per-key narrowing
        can express, and the reason `locations=` exists beside schema_filter."""
        for_each(
            double,
            {"signal": LocInput},
            [LocOutput],
            subject=SUBJECTS,
            trial=TRIALS,
            locations={
                "include": [
                    [["subject", "S01"]],
                    [["subject", "S02"], ["trial", "t2"]],
                ]
            },
        )
        assert _locations_written(db) == {
            ("S01", "t1"),
            ("S01", "t2"),
            ("S02", "t2"),
        }


class TestLevelRules:
    def test_a_level_rule_drops_it_under_every_key(self, db):
        for_each(
            double,
            {"signal": LocInput},
            [LocOutput],
            subject=SUBJECTS,
            trial=TRIALS,
            locations={"exclude_levels": {"trial": ["t1"]}},
        )
        assert _locations_written(db) == {("S01", "t2"), ("S02", "t2")}

    def test_both_halves_compose(self, db):
        for_each(
            double,
            {"signal": LocInput},
            [LocOutput],
            subject=SUBJECTS,
            trial=TRIALS,
            locations={
                "include": [[["subject", "S01"]]],
                "exclude_levels": {"trial": ["t1"]},
            },
        )
        assert _locations_written(db) == {("S01", "t2")}


class TestInertAndRecursion:
    def test_omitting_it_runs_everything(self, db):
        for_each(
            double,
            {"signal": LocInput},
            [LocOutput],
            subject=SUBJECTS,
            trial=TRIALS,
        )
        assert len(_locations_written(db)) == 4

    def test_an_empty_filter_is_inert(self, db):
        for_each(
            double,
            {"signal": LocInput},
            [LocOutput],
            subject=SUBJECTS,
            trial=TRIALS,
            locations={},
        )
        assert len(_locations_written(db)) == 4

    def test_every_each_of_alternative_sees_the_filter(self, db):
        """scidb's EachOf recursion spells out every kwarg by hand, and one
        dropped there fails SILENTLY — `glue=` went missing from this exact
        recursion in 2026-09-10 and every alternative ran unglued."""
        for_each(
            scale,
            {"signal": LocInput, "factor": EachOf(2.0, 10.0)},
            [LocOutput],
            subject=SUBJECTS,
            trial=TRIALS,
            locations={"include": [[["subject", "S01"]]]},
        )
        assert _locations_written(db) == {("S01", "t1"), ("S01", "t2")}
        # Two variants at each of the two locations, and nothing of S02.
        assert len(db.load_all_as_df(LocOutput)) == 4


class TestDryRun:
    def test_the_dry_run_reports_the_narrowed_count(self, db, capsys):
        """The preview must describe the run that would happen. A dry run
        listing four iterations before a real run does two is worse than no
        preview at all."""
        for_each(
            double,
            {"signal": LocInput},
            [LocOutput],
            subject=SUBJECTS,
            trial=TRIALS,
            dry_run=True,
            locations={"include": [[["subject", "S01"]]]},
        )
        out = capsys.readouterr().out
        assert "[dry-run] 2 iterations" in out
        assert _locations_written(db) == set()


class LocRow(BaseVariable):
    pass


class LocRowOut(BaseVariable):
    pass


class TestRowsBelowTheIteratedLevel:
    """A selection on a key the run does not iterate. Found on a [group, subject]
    run with minute=6 selected: the function received every minute, because the
    filter only narrowed combos, and a coarser combo is kept as an ancestor."""

    @pytest.fixture
    def rows(self, db):
        for subject in SUBJECTS:
            for index, trial in enumerate(["t1", "t2", "t3"], start=1):
                LocRow.save(float(index), subject=subject, trial=trial)
        return db

    @staticmethod
    def _trials(*names):
        return {
            "include": [
                [["subject", subject], ["trial", name]]
                for subject in SUBJECTS
                for name in names
            ]
        }

    def _run(self, locations, **kwargs):
        seen = {}

        def record(table):
            seen[table["subject"].iloc[0]] = sorted(table["trial"].tolist())
            return float(len(table))

        for_each(
            record,
            {"table": LocRow},
            [LocRowOut],
            subject=SUBJECTS,
            as_table=True,
            locations=locations,
            **kwargs,
        )
        return seen

    def test_the_function_receives_only_the_selected_rows(self, rows):
        seen = self._run(self._trials("t2", "t3"))
        assert seen == {"S01": ["t2", "t3"], "S02": ["t2", "t3"]}

    def test_a_level_rule_below_the_iterated_level_applies(self, rows):
        seen = self._run({"exclude_levels": {"trial": ["t1"]}})
        assert seen == {"S01": ["t2", "t3"], "S02": ["t2", "t3"]}

    def test_a_ragged_selection_applies_per_call(self, rows):
        seen = self._run(
            {
                "include": [
                    [["subject", "S01"]],
                    [["subject", "S02"], ["trial", "t3"]],
                    [["subject", "S02"], ["trial", "t1"]],
                ]
            }
        )
        assert seen == {"S01": ["t1", "t2", "t3"], "S02": ["t1", "t3"]}

    def test_lineage_records_only_the_rows_the_call_read(self, rows):
        """The output must not claim to derive from rows it never saw."""
        from scidb.provenance import query as provenance_query

        self._run(self._trials("t2", "t3"))
        out = LocRowOut.load(subject="S01")
        consumed = provenance_query.consumed_input_schema_ids(rows._duck, [out.record_id])
        assert len(consumed[out.record_id]) == 2

    def test_skip_computed_reruns_when_the_selection_changes(self, rows):
        self._run(self._trials("t2", "t3"), skip_computed=True)
        seen = self._run(self._trials("t1"), skip_computed=True)
        assert seen == {"S01": ["t1"], "S02": ["t1"]}

    def test_skip_computed_skips_an_unchanged_selection(self, rows):
        self._run(self._trials("t2", "t3"), skip_computed=True)
        assert self._run(self._trials("t2", "t3"), skip_computed=True) == {}

    def test_a_fixed_input_keeps_the_location_it_names(self, rows):
        seen = []

        def record(table, pinned):
            seen.append((sorted(table["trial"].tolist()), float(pinned)))
            return 0.0

        for_each(
            record,
            {"table": LocRow, "pinned": Fixed(LocRow, trial="t1")},
            [LocRowOut],
            subject=["S01"],
            as_table=["table"],
            locations=self._trials("t2", "t3"),
        )
        assert seen == [(["t2", "t3"], 1.0)]

    def test_two_selections_below_the_level_are_both_current(self, rows):
        """Pins today's rule (docs/claude/location-filter-semantics.md): consumed
        input locations are part of the variant key, so a second selection does
        not replace the first at the same output location."""
        self._run(self._trials("t2", "t3"))
        self._run(self._trials("t1"))
        assert len(rows.load_all_as_df(LocRowOut)) == 2 * len(SUBJECTS)

    def test_a_reselected_full_iteration_location_stays_one_record(self, rows):
        """At full iteration a location consumes only itself, so a selection
        decides which calls run and cannot make a second variant."""
        selection = {"exclude_levels": {"trial": ["t1"]}}
        for _ in range(2):
            for_each(
                double,
                {"signal": LocRow},
                [LocRowOut],
                subject=SUBJECTS,
                trial=["t1", "t2", "t3"],
                locations=selection,
            )
        assert len(rows.load_all_as_df(LocRowOut)) == 2 * len(SUBJECTS)
