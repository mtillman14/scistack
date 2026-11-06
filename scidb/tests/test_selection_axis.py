"""A schema selection below the iterated level is a variant.

docs/claude/selection-variant-axis.md. The unit half pins what the axis says for a
pair of tables; the integration half pins that every reader of a variant follows it,
because each reader failing alone would be silent wrong data.
"""

import pandas as pd
import pytest

import scifor as _scifor
from scidb import AcrossVariants, BaseVariable, Fixed, Variant, configure_database, for_each
from scidb.provenance import query as pq
from scidb.provenance.graph import SELECTION_TYPE
from scidb.provenance.state import check_node_state
from scidb.run.selection_axis import SELECTION_PARAM, SelectionAxis, selection_axes_of

SCHEMA = ["subject", "session", "minute"]
LEVEL = ["subject"]


def _frame(*rows):
    return pd.DataFrame(rows, columns=SCHEMA)


def _axes(before, keep, level=LEVEL):
    """Axes for *before* narrowed to the rows *keep* accepts."""
    after = before[before.apply(keep, axis=1)].reset_index(drop=True)
    return selection_axes_of({"x": before}, {"x": after}, level, SCHEMA)


GRID = _frame(
    *[(s, sess, m) for s in ("A", "B") for sess in ("1", "2") for m in ("5", "6")]
)


class TestWhatTheAxisSays:
    def test_a_finer_key_names_only_what_was_narrowed(self):
        axes = _axes(GRID, lambda r: r["minute"] == "6")
        assert axes.uniform.label == "minute=6"

    def test_a_selection_on_the_iterated_key_is_no_axis(self):
        """Which calls happen, not what they read (the user's rule)."""
        axes = _axes(GRID, lambda r: r["subject"] == "A")
        assert axes.uniform is None and not axes.per_call

    def test_full_iteration_has_no_axis(self):
        axes = _axes(GRID, lambda r: r["minute"] == "6", level=SCHEMA)
        assert axes.uniform is None and not axes.per_call

    def test_one_subject_of_a_selection_reads_like_all_of_them(self):
        """Subject B's sessions must not make A's look narrowed, or re-running
        one subject would start a second variant."""
        ragged_sessions = _frame(
            ("A", "1", "5"), ("A", "1", "6"), ("B", "2", "5"), ("B", "2", "6")
        )
        everyone = _axes(ragged_sessions, lambda r: r["minute"] == "6")
        just_a = _axes(ragged_sessions, lambda r: r["minute"] == "6" and r["subject"] == "A")
        assert everyone.uniform.label == just_a.uniform.label == "minute=6"

    def test_a_subject_that_only_has_the_selected_level_shares_the_axis(self):
        """Nothing was narrowed for B alone, but it must not fall out of the
        variant every other subject is in."""
        frame = _frame(("A", "1", "5"), ("A", "1", "6"), ("B", "1", "6"))
        axes = _axes(frame, lambda r: r["minute"] == "6")
        assert axes.for_combo({"subject": "B"}).label == "minute=6"

    def test_a_ragged_selection_answers_per_call(self):
        axes = _axes(GRID, lambda r: r["subject"] == "A" or r["minute"] == "6")
        assert axes.uniform is None
        assert axes.for_combo({"subject": "A"}) is None
        assert axes.for_combo({"subject": "B"}).label == "minute=6"

    def test_kept_rows_no_per_key_rule_can_say_are_listed(self):
        axes = _axes(
            GRID,
            lambda r: (r["session"], r["minute"]) in {("1", "5"), ("2", "6")},
        )
        axis = axes.for_combo({"subject": "A"})
        assert "(per location)" in axis.label
        other = _axes(
            GRID,
            lambda r: (r["session"], r["minute"]) in {("1", "6"), ("2", "5")},
        ).for_combo({"subject": "A"})
        assert axis.label != other.label, "same summary, different rows"

    def test_the_multi_table_rule_widens_rather_than_conflicts(self):
        x = _frame(("A", "1", "4"), ("A", "1", "5"), ("A", "1", "6"))
        y = _frame(("A", "1", "4"), ("A", "1", "5"))
        keep = lambda f: f[f["minute"] != "4"].reset_index(drop=True)  # noqa: E731
        axes = selection_axes_of({"x": x, "y": y}, {"x": keep(x), "y": keep(y)}, LEVEL, SCHEMA)
        assert axes.uniform.label == "minute=5/6"

    def test_an_unchanged_table_says_nothing(self):
        axes = selection_axes_of({"x": GRID}, {"x": GRID}, LEVEL, SCHEMA)
        assert axes.uniform is None and not axes.per_call

    def test_a_uniform_axis_answers_for_any_combo(self):
        axes = _axes(GRID, lambda r: r["minute"] == "6")
        assert axes.for_combo({"subject": "never seen"}).label == "minute=6"


class TestTheLabel:
    def test_it_round_trips_through_the_stored_form(self):
        axis = _axes(GRID, lambda r: r["minute"] == "6").uniform
        assert SelectionAxis.from_json(axis.to_json()) == axis

    def test_numbers_sort_as_numbers(self):
        frame = _frame(*[("A", "1", str(m)) for m in (1, 2, 10, 20)])
        axis = _axes(frame, lambda r: r["minute"] != "20").uniform
        assert axis.label == "minute=1/2/10"

    def test_a_value_holding_the_separator_cannot_collide(self):
        one = SelectionAxis(keep=(("k", ("A/B",)),))
        two = SelectionAxis(keep=(("k", ("A", "B")),))
        assert one.label != two.label

    def test_a_long_list_stays_short_and_distinct(self):
        values = tuple(str(i) for i in range(50))
        long = SelectionAxis(keep=(("k", values),))
        other = SelectionAxis(keep=(("k", values[:-1]),))
        assert len(long.label) < 40
        assert long.label != other.label

    def test_a_null_level_has_a_name(self):
        assert "(none)" in SelectionAxis(keep=(("k", (None, "1")),)).label

    def test_admits_ignores_a_key_the_table_does_not_have(self):
        axis = SelectionAxis(keep=(("minute", ("6",)),))
        assert axis.admits({"subject": "A"}, columns={"subject"})
        assert not axis.admits({"subject": "A"}, columns={"subject", "minute"})
        assert axis.admits({"minute": "6"}, columns={"minute"})
        assert not axis.admits({"minute": "5"}, columns={"minute"})


# --- through the database -----------------------------------------------------


class AxisIn(BaseVariable):
    pass


class AxisOut(BaseVariable):
    pass


class AxisDown(BaseVariable):
    pass


def axis_count(table):
    return float(len(table))


def axis_same(table):
    return float(table)


def axis_pair(table, pinned):
    return float(len(table)) + float(pinned)


SUBJECTS = ["S1", "S2"]
TRIALS = ["t1", "t2", "t3"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "axis.duckdb", ["subject", "trial"])
    for subject in SUBJECTS:
        for index, trial in enumerate(TRIALS, start=1):
            AxisIn.save(float(index), subject=subject, trial=trial)
    yield database
    _scifor.set_schema([])
    database.close()


def _only(*trials):
    return {"exclude_levels": {"trial": [t for t in TRIALS if t not in trials]}}


def _run(database, locations=None, **kwargs):
    return for_each(
        axis_count,
        {"table": AxisIn},
        [AxisOut],
        db=database,
        subject=SUBJECTS,
        as_table=True,
        locations=locations,
        **kwargs,
    )


def _labels(database, variable=AxisOut):
    """{subject: sorted labels of the CURRENT records there}."""
    frame = database.load_all_as_df(variable, stringify_schema=True, include_rid=True)
    rids = frame["__record_id"].tolist()
    params = pq.branch_params_batch(database._duck, rids)
    out: dict = {}
    for subject, rid in zip(frame["subject"], rids, strict=True):
        label = params[rid].get(f"axis_count.{SELECTION_PARAM}")
        out.setdefault(subject, []).append(label)
    return {subject: sorted(v, key=str) for subject, v in out.items()}


class TestTwoSelectionsAreTwoVariants:
    def test_both_stay_current_each_labelled(self, db):
        _run(db, _only("t2", "t3"))
        _run(db, _only("t1"))
        assert _labels(db) == {
            "S1": ["trial=t1", "trial=t2/t3"],
            "S2": ["trial=t1", "trial=t2/t3"],
        }

    def test_the_variants_popup_shows_two_cards(self, db):
        _run(db, _only("t2", "t3"))
        _run(db, _only("t1"))
        cards = db.inspect.variant_cards("AxisOut").cards
        assert len(cards) == 2
        shown = {c.distinguishing[f"axis_count.{SELECTION_PARAM}"] for c in cards}
        assert shown == {"trial=t1", "trial=t2/t3"}

    def test_a_pin_reads_one_of_them(self, db):
        _run(db, _only("t2", "t3"))
        _run(db, _only("t1"))
        seen = []

        def read(table):
            seen.append(sorted(table["subject"].tolist()))
            return 0.0

        for_each(
            read,
            {"table": Variant(AxisOut, **{SELECTION_PARAM: "trial=t1"})},
            [AxisDown],
            db=db,
            as_table=True,
        )
        assert seen == [["S1", "S2"]]

    def test_downstream_splits_rather_than_mixing_them(self, db):
        """The silent wrong number this feature exists to stop: a downstream
        aggregate pooling minute 5's and minute 6's results as replicates."""
        _run(db, _only("t2", "t3"))
        _run(db, _only("t1"))
        sizes = []

        def downstream(table):
            sizes.append(len(table))
            return float(len(table))

        for_each(downstream, {"table": AxisOut}, [AxisDown], db=db, as_table=True)
        assert sizes == [2, 2]
        frame = db.load_all_as_df(AxisDown, stringify_schema=True)
        assert len(frame) == 2

    def test_across_variants_still_pools_on_request(self, db):
        _run(db, _only("t2", "t3"))
        _run(db, _only("t1"))
        sizes = []

        def downstream(table):
            sizes.append(len(table))
            return float(len(table))

        for_each(
            downstream, {"table": AcrossVariants(AxisOut)}, [AxisDown], db=db, as_table=True
        )
        assert sizes == [4]


class TestWhatIsNotAVariant:
    def test_two_spellings_of_one_selection_are_one_variant(self, db):
        _run(db, _only("t3"))
        _run(
            db,
            {"include": [[["subject", s], ["trial", "t3"]] for s in SUBJECTS]},
        )
        assert _labels(db) == {"S1": ["trial=t3"], "S2": ["trial=t3"]}

    def test_selecting_everything_is_the_same_as_no_selection(self, db):
        _run(db)
        _run(db, _only(*TRIALS))
        assert _labels(db) == {"S1": [None], "S2": [None]}

    def test_a_selection_on_the_iterated_key_makes_no_edge(self, db):
        _run(db, {"include": [[["subject", "S1"]]]})
        assert _selection_edges(db) == 0
        assert _labels(db) == {"S1": [None]}

    def test_rerunning_one_subject_replaces_it_in_the_same_variant(self, db):
        _run(db, _only("t3"))
        _run(db, {"include": [[["subject", "S1"], ["trial", "t3"]]]})
        assert _labels(db) == {"S1": ["trial=t3"], "S2": ["trial=t3"]}

    def test_an_unselected_run_records_no_edge_so_its_id_is_unchanged(self, db):
        _run(db)
        assert _selection_edges(db) == 0


def _selection_edges(database) -> int:
    return database._duck.fetchone(
        "SELECT COUNT(*) FROM _invocation_input ii "
        "JOIN _record r ON r.record_id = ii.input_record_id WHERE r.type = ?",
        [SELECTION_TYPE],
    )[0]


class TestItNeverReachesTheCanvasOrTheFunction:
    def test_the_call_site_is_one_node_with_no_new_constant(self, db):
        _run(db, _only("t1"))
        _run(db, _only("t2"))
        variants = [
            v for v in db.list_pipeline_variants() if v["function_name"] == "axis_count"
        ]
        assert len(variants) == 1
        assert variants[0]["constants"] == {}
        assert SELECTION_PARAM not in variants[0].get("input_types", {})

    def test_it_is_not_an_input_variable_or_a_constant(self, db):
        _run(db, _only("t1"))
        out = AxisOut.load(subject="S1")
        edges = pq.invocation_edges_batch(db._duck, [pq.producing_invocation(db._duck, out.record_id)[0]])
        only = next(iter(edges.values()))
        assert {i["param_name"] for i in only["var_inputs"]} == {"table"}
        assert only["constants"] == {}
        assert only["selection"] is not None

    def test_the_two_branch_param_readers_agree(self, db):
        _run(db, _only("t1"))
        out = AxisOut.load(subject="S1")
        assert pq.derived_branch_params(db._duck, out.record_id) == pq.branch_params_batch(
            db._duck, [out.record_id]
        )[out.record_id]

    def test_the_stored_id_recipe_reproduces(self, db):
        """Node state and the skip gate both recompute ids from the edges."""
        _run(db, _only("t1"))
        out = AxisOut.load(subject="S1")
        inv = pq.producing_invocation(db._duck, out.record_id)[0]
        assert pq.invocation_id_is_current(db._duck, inv)


class TestSkipGate:
    def test_an_unchanged_selection_skips(self, db):
        _run(db, _only("t2"), skip_computed=True)
        result = _run(db, _only("t2"), skip_computed=True)
        assert result is None or len(result) == 0

    def test_a_second_selection_runs_and_the_first_still_skips(self, db):
        _run(db, _only("t1"), skip_computed=True)
        _run(db, _only("t2"), skip_computed=True)
        assert _labels(db)["S1"] == ["trial=t1", "trial=t2"]
        result = _run(db, _only("t1"), skip_computed=True)
        assert result is None or len(result) == 0

    def test_the_same_rows_under_another_axis_run_again(self, db):
        """S3 only ever had t1: its rows are the same with or without the
        selection, but only one of those runs puts it in the t1 variant."""
        AxisIn.save(1.0, subject="S3", trial="t1")
        everyone = SUBJECTS + ["S3"]
        for_each(
            axis_count, {"table": AxisIn}, [AxisOut], db=db, subject=everyone,
            as_table=True, skip_computed=True,
        )
        for_each(
            axis_count, {"table": AxisIn}, [AxisOut], db=db, subject=everyone,
            as_table=True, skip_computed=True, locations=_only("t1"),
        )
        assert "trial=t1" in _labels(db)["S3"]


class TestNodeState:
    def test_a_narrowed_node_is_green_after_a_clean_run(self, db):
        """v0.3.2's narrowing alone read red forever: the predictor expected the
        unnarrowed rows, an invocation that can never exist."""
        _run(db, _only("t2"))
        assert check_node_state(axis_count, [AxisOut], db=db)["state"] == "green"

    def test_two_narrowed_variants_are_both_green(self, db):
        _run(db, _only("t1"))
        _run(db, _only("t2"))
        assert check_node_state(axis_count, [AxisOut], db=db)["state"] == "green"

    def test_a_new_selected_row_reads_red(self, db):
        _run(db, _only("t2"))
        AxisIn.save(9.0, subject="S1", trial="t2")
        assert check_node_state(axis_count, [AxisOut], db=db)["state"] == "red"

    def test_a_new_row_outside_the_selection_stays_green(self, db):
        _run(db, _only("t2"))
        AxisIn.save(9.0, subject="S1", trial="t9")
        assert check_node_state(axis_count, [AxisOut], db=db)["state"] == "green"


class TestAddedData:
    def test_a_rerun_after_new_data_replaces_the_result(self, db):
        _run(db)
        AxisIn.save(4.0, subject="S1", trial="t4")
        _run(db)
        frame = db.load_all_as_df(AxisOut, stringify_schema=True)
        assert sorted(frame["subject"].tolist()) == ["S1", "S2"]
        assert AxisOut.load(subject="S1").data == 4.0

    def test_a_new_subject_replaces_a_result_that_pools_subjects(self, db):
        """Appending a subject adds rows to a call iterating above it (user
        decision 2026-11-05: the result updates, it does not coexist)."""
        for_each(axis_count, {"table": AxisIn}, [AxisOut], db=db, as_table=True)
        AxisIn.save(1.0, subject="S3", trial="t1")
        for_each(axis_count, {"table": AxisIn}, [AxisOut], db=db, as_table=True)
        assert len(db.load_all_as_df(AxisOut)) == 1
        assert AxisOut.load().data == 7.0

    def test_a_new_subject_at_the_iterated_level_is_just_a_new_result(self, db):
        _run(db)
        AxisIn.save(1.0, subject="S3", trial="t1")
        for_each(
            axis_count, {"table": AxisIn}, [AxisOut], db=db,
            subject=[*SUBJECTS, "S3"], as_table=True,
        )
        frame = db.load_all_as_df(AxisOut, stringify_schema=True)
        assert sorted(frame["subject"].tolist()) == ["S1", "S2", "S3"]

    def test_a_listed_selection_does_not_pick_up_a_new_subject(self, db):
        """A prefix names places, so a subject added later is outside it. Its own
        level rule is what reaches new data (location-filter-semantics.md)."""
        listed = {"include": [[["subject", s], ["trial", "t1"]] for s in SUBJECTS]}
        _run(db, listed)
        AxisIn.save(1.0, subject="S3", trial="t1")
        for_each(
            axis_count, {"table": AxisIn}, [AxisOut], db=db,
            subject=[*SUBJECTS, "S3"], as_table=True, locations=listed,
        )
        frame = db.load_all_as_df(AxisOut, stringify_schema=True)
        assert sorted(frame["subject"].tolist()) == ["S1", "S2"]

    def test_a_selection_ignores_data_outside_it(self, db):
        _run(db, _only("t2"))
        AxisIn.save(4.0, subject="S1", trial="t4")
        _run(db, {"include": [[["subject", s], ["trial", "t2"]] for s in SUBJECTS]})
        assert _labels(db) == {"S1": ["trial=t2"], "S2": ["trial=t2"]}

    def test_where_variants_still_coexist(self, db):
        """`where=` chose among the rows, so its locations are still its identity."""
        from scidb.model.filters import SchemaKeyInFilter

        for trials in (["t1"], ["t2"]):
            for_each(
                axis_count, {"table": AxisIn}, [AxisOut], db=db, subject=["S1"], as_table=True,
                where=SchemaKeyInFilter("trial", trials),
            )
        assert len(db.load_all_as_df(AxisOut, stringify_schema=True)) == 2

    def test_a_where_run_writes_no_pooled_marker(self, db):
        from scidb.model.filters import SchemaKeyInFilter

        for_each(
            axis_count, {"table": AxisIn}, [AxisOut], db=db, subject=["S1"], as_table=True,
            where=SchemaKeyInFilter("trial", ["t1"]),
        )
        assert db._duck.fetchone("SELECT COUNT(*) FROM _invocation_pooled")[0] == 0

    def test_fixed_pins_still_coexist(self, db):
        """A Fixed input names its record, so two pins are two computations."""
        for pin in ("t1", "t2"):
            for_each(
                axis_pair,
                {"table": AxisIn, "pinned": Fixed(AxisIn, trial=pin)},
                [AxisOut],
                db=db,
                subject=["S1"],
                as_table=["table"],
            )
        assert len(db.load_all_as_df(AxisOut, stringify_schema=True)) == 2

    def test_full_iteration_writes_no_pooled_marker(self, db):
        for_each(
            axis_same, {"table": AxisIn}, [AxisOut], db=db, subject=SUBJECTS, trial=TRIALS,
        )
        assert db._duck.fetchone("SELECT COUNT(*) FROM _invocation_pooled")[0] == 0

    def test_where_loads_still_see_every_consumed_location(self, db):
        _run(db)
        out = AxisOut.load(subject="S1")
        consumed = pq.consumed_input_schema_ids(db._duck, [out.record_id])
        assert len(consumed[out.record_id]) == 3

    def test_two_inputs_of_different_types_do_not_collapse(self, db):
        class AxisOther(BaseVariable):
            pass

        AxisOther.save(1.0, subject="S1", trial="t1")
        for source in (AxisIn, AxisOther):
            for_each(
                axis_count, {"table": source}, [AxisOut], db=db, subject=["S1"], as_table=True,
            )
        assert len(db.load_all_as_df(AxisOut, stringify_schema=True)) == 2


class TestBothSavePaths:
    def test_a_saved_output_writes_the_marker(self, db):
        _run(db)
        (markers,) = db._duck.fetchone("SELECT COUNT(*) FROM _invocation_pooled")
        assert markers == len(SUBJECTS)

    def test_a_generated_file_writes_the_marker_and_the_axis(self, db):
        """generates_file saves through its own path, which must record the same
        facts or a plot would keep the older rule."""
        from scidb import scistack

        @scistack(generates_file=True)
        def axis_plot(table, subject):
            return None

        for_each(
            axis_plot, {"table": AxisIn}, [AxisOut], db=db, subject=["S1"],
            as_table=True, locations=_only("t2"),
        )
        (markers,) = db._duck.fetchone("SELECT COUNT(*) FROM _invocation_pooled")
        assert markers == 1
        assert _selection_edges(db) == 1


class TestRecordsThatMoveOrGo:
    def test_clearing_the_database_empties_the_marker(self, db):
        from scidb.store.tables import cleared_tables

        assert "_invocation_pooled" in cleared_tables()

    def test_a_rename_carries_the_axis_and_the_marker(self, db):
        from scidb import rename_functions
        from scidb.run.foreach_config import function_hash_for

        _run(db, _only("t2"))
        report = rename_functions(
            db, {"axis_count": "lib.axis_count"},
            {"lib.axis_count": function_hash_for(axis_count)},
        )
        assert report.not_carried == []
        (markers,) = db._duck.fetchone(
            "SELECT COUNT(*) FROM _invocation_pooled p "
            "JOIN _invocation i ON i.invocation_id = p.invocation_id "
            "WHERE i.function_name = 'lib.axis_count'"
        )
        assert markers == len(SUBJECTS)
        assert _selection_edges(db) == 2 * len(SUBJECTS), "old and carried calls"

    def test_deleting_one_card_keeps_the_other_and_removes_its_markers(self, db):
        from scidb.store.variant_delete import delete_variant

        _run(db, _only("t1"))
        _run(db, _only("t2"))
        card = next(
            c
            for c in db.inspect.variant_cards("AxisOut").cards
            if c.distinguishing[f"axis_count.{SELECTION_PARAM}"] == "trial=t1"
        )
        target = [{"variable": "AxisOut", "card_id": card.card_id}]
        delete_variant(db, target, "selected the wrong minute")
        assert _labels(db) == {"S1": ["trial=t2"], "S2": ["trial=t2"]}
        (markers,) = db._duck.fetchone("SELECT COUNT(*) FROM _invocation_pooled")
        assert markers == len(SUBJECTS)
