"""A ColumnSelection input must still record what it consumed.

``ColumnSelection`` is deliberately kept out of ``rid_keys``: coupling it into
rid expansion changes ``Variant`` pinning and ``for_columns`` aggregation
semantics (see the comment at that branch in ``foreach._for_each_prepare``).
But ``rid_keys`` was also the only thing feeding the SAVE path's input binding,
so switching off ITERATION switched off LINEAGE with it — two unrelated jobs on
one wire.

The result (2026-09-15, ``GAITRiteSymmetry``): every record built from a
column-selected input was saved with no ``_invocation_input`` edge at all. With
no edge the record cannot be told apart from its own superseded generation —
no upstream code axis can be derived, nothing marks one of them current, and a
plot drew both as replicates of each other.

These pin the fix and, just as importantly, pin that it did NOT re-couple the
two: a plain input's variant grouping must be exactly what it was.
"""

import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "session", "trial", "cycle"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "test_colsel_lineage.duckdb", SCHEMA)
    yield database
    _scifor.set_schema([])
    database.close()


class Gait(BaseVariable):
    """Trial-level table with several columns, so a selection is meaningful.

    Saved at subject/session/trial in a schema that also has `cycle`, so a
    for_each over subject/session/trial runs in AGGREGATION mode — the mode
    the lineage fix covers, and the one the real pipeline uses (4 of 5 keys).
    """


class Summary(BaseVariable):
    pass


def _seed(n_subjects=2):
    for subject in range(1, n_subjects + 1):
        for trial in (1, 2):
            Gait.save(
                pd.DataFrame(
                    {
                        "step": [1.0 * subject, 2.0 * subject],
                        "stride": [3.0 * subject, 4.0 * subject],
                        "width": [0.1, 0.2],
                    }
                ),
                subject=subject,
                session="A",
                trial=trial,
            )


def _input_edges(db, variable: str) -> list[tuple]:
    """(output_record_id, param_name, input_record_id) for a variable's records."""
    return db._duck._fetchall(
        "SELECT io.output_record_id, ii.param_name, ii.input_record_id "
        "FROM _record r "
        "JOIN _invocation_output io ON io.output_record_id = r.record_id "
        "JOIN _invocation_input ii ON ii.invocation_id = io.invocation_id "
        "WHERE r.type = ?",
        [variable],
    )


def _mean_step(g):
    return float(pd.DataFrame(g)["step"].mean())


# ---------------------------------------------------------------------------
# The fix
# ---------------------------------------------------------------------------


def test_column_selection_records_input_edges(db):
    """The regression: a column-selected input produced NO edges at all."""
    _seed()

    for_each(
        _mean_step,
        {"g": Gait[["step", "stride"]]},
        [Summary],
        subject=[1, 2],
        session=["A"],
        trial=[1, 2],
    )

    edges = _input_edges(db, "Summary")
    assert edges, "column-selected input recorded no _invocation_input edges"
    assert {e[1] for e in edges} == {"g"}, edges


def test_edges_point_at_the_records_actually_consumed(db):
    """Not merely present — correct. Each Summary must bind the Gait record at
    its own location, which is what lets a later run be told apart from it."""
    _seed()

    for_each(
        _mean_step,
        {"g": Gait[["step", "stride"]]},
        [Summary],
        subject=[1, 2],
        session=["A"],
        trial=[1, 2],
    )

    gait_ids = {
        row[0]
        for row in db._duck._fetchall(
            "SELECT record_id FROM _record WHERE type = 'Gait'"
        )
    }
    consumed = {e[2] for e in _input_edges(db, "Summary")}
    assert consumed, "no inputs bound"
    assert consumed <= gait_ids, consumed - gait_ids


def test_a_rerun_over_changed_input_is_distinguishable(db):
    """The end the user cares about: re-run after the input changes, and the two
    generations must not look like unrelated records at one location."""
    _seed()
    for_each(
        _mean_step,
        {"g": Gait[["step", "stride"]]},
        [Summary],
        subject=[1],
        session=["A"],
        trial=[1],
    )
    first = {e[2] for e in _input_edges(db, "Summary")}

    # The input changes, the code does not.
    Gait.save(
        pd.DataFrame({"step": [9.0, 9.0], "stride": [3.0, 4.0], "width": [0.1, 0.2]}),
        subject=1,
        session="A",
        trial=1,
    )
    for_each(
        _mean_step,
        {"g": Gait[["step", "stride"]]},
        [Summary],
        subject=[1],
        session=["A"],
        trial=[1],
    )

    consumed = {e[2] for e in _input_edges(db, "Summary")}
    assert len(consumed) > len(first), (
        "the re-run bound the same input record as the first run — the two "
        "generations are still indistinguishable"
    )


# ---------------------------------------------------------------------------
# What must NOT have changed
# ---------------------------------------------------------------------------


def test_plain_input_still_binds_its_edges(db):
    """The unchanged path, asserted alongside so a regression here is obvious."""
    _seed()

    for_each(
        lambda g: float(pd.DataFrame(g)["step"].mean()),
        {"g": Gait},
        [Summary],
        subject=[1, 2],
        session=["A"],
        trial=[1, 2],
    )

    edges = _input_edges(db, "Summary")
    assert edges
    assert {e[1] for e in edges} == {"g"}


def test_column_selection_still_yields_one_call_per_location(db):
    """Lineage was restored WITHOUT re-coupling iteration: a ColumnSelection
    input must not start expanding combos the way a plain rid-tracked input
    does. Four locations, four calls — not eight."""
    _seed()
    calls = []

    def count(g):
        calls.append(pd.DataFrame(g).shape)
        return 1.0

    for_each(
        count,
        {"g": Gait[["step", "stride"]]},
        [Summary],
        subject=[1, 2],
        session=["A"],
        trial=[1, 2],
        save=False,
    )

    assert len(calls) == 4, calls


def test_the_function_still_sees_only_the_selected_columns(db):
    """The selection itself is untouched by the lineage change."""
    _seed()
    seen = []

    def grab(g):
        seen.append(list(pd.DataFrame(g).columns))
        return 1.0

    for_each(
        grab,
        {"g": Gait[["step", "stride"]]},
        [Summary],
        subject=[1],
        session=["A"],
        trial=[1],
        save=False,
    )

    assert seen, "function never called"
    assert all("width" not in cols for cols in seen), seen
    assert all("step" in cols and "stride" in cols for cols in seen), seen


# ---------------------------------------------------------------------------
# Full iteration mode (every schema key iterated)
# ---------------------------------------------------------------------------
#
# The tests above run in AGGREGATION mode (3 of 4 schema keys iterated), which
# is the shape of the real pipeline. Full iteration binds lineage by a
# different route entirely — plain inputs put their rid in a `__rid_*` column
# of the result table — and ColumnSelection has no such column, so it was
# still unbound after the first fix. These pin the second half.


class Cycle(BaseVariable):
    """Saved at EVERY schema key, so a for_each over all four is full
    iteration rather than aggregation."""


def _seed_full():
    for subject in (1, 2):
        for trial in (1, 2):
            Cycle.save(
                pd.DataFrame(
                    {"step": [1.0 * subject, 2.0], "stride": [3.0, 4.0], "width": [0.1, 0.2]}
                ),
                subject=subject,
                session="A",
                trial=trial,
                cycle=1,
            )


def test_full_iteration_column_selection_binds_lineage(db):
    _seed_full()

    for_each(
        _mean_step,
        {"g": Cycle[["step", "stride"]]},
        [Summary],
        subject=[1, 2],
        session=["A"],
        trial=[1, 2],
        cycle=[1],
    )

    edges = _input_edges(db, "Summary")
    assert edges, "full-iteration ColumnSelection recorded no input edges"
    assert {e[1] for e in edges} == {"g"}, edges

    cycle_ids = {
        row[0]
        for row in db._duck._fetchall(
            "SELECT record_id FROM _record WHERE type = 'Cycle'"
        )
    }
    assert {e[2] for e in edges} <= cycle_ids


def test_full_iteration_binds_a_plain_and_a_selected_input_together(db):
    """The merge case: a plain input binds through its `__rid_*` column and a
    selected one only through the location map. Recording just the first reads
    as a complete lineage while half of it is missing."""
    _seed_full()
    for subject in (1, 2):
        for trial in (1, 2):
            Gait.save(
                pd.DataFrame({"step": [5.0], "stride": [6.0], "width": [0.3]}),
                subject=subject,
                session="A",
                trial=trial,
                cycle=1,
            )

    for_each(
        lambda g, p: float(pd.DataFrame(g)["step"].mean())
        + float(pd.DataFrame(p)["step"].mean()),
        {"g": Cycle[["step", "stride"]], "p": Gait},
        [Summary],
        subject=[1, 2],
        session=["A"],
        trial=[1, 2],
        cycle=[1],
    )

    params = {e[1] for e in _input_edges(db, "Summary")}
    assert params == {"g", "p"}, (
        f"expected both inputs bound, got {params} — the selected input was "
        "dropped in favour of the plain one"
    )


def test_full_iteration_without_column_selection_is_unchanged(db):
    """No ColumnSelection anywhere: the location map must stay unbuilt, so the
    run takes exactly the path it always did."""
    _seed_full()

    for_each(
        lambda g: float(pd.DataFrame(g)["step"].mean()),
        {"g": Cycle},
        [Summary],
        subject=[1, 2],
        session=["A"],
        trial=[1, 2],
        cycle=[1],
    )

    edges = _input_edges(db, "Summary")
    assert edges
    assert {e[1] for e in edges} == {"g"}
