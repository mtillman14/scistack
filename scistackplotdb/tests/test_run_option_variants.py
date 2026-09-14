"""Run options (``distribute`` / ``as_table``) as a plot variant axis.

The 2026-09-14 report: Plot Studio drew two points per index per trial for a
loader that had been run once with ``distribute=false`` (every trial got the
whole file) and once with ``distribute=true``. Same code, same constants — so
neither the branch-param columns nor the ``Code:<fn>`` chain told the records
apart, and ``attach_variants`` handed them over as replicates.

``Run:<fn>`` is the third axis: one column per upstream function that ran under
more than one option set, levels being scidb's ``run_options_label`` strings,
answered by the same per-location latest flag as code. These tests cover the
column, the flag, the default pin, the pooling guard, and the two translations
(selection <-> scidb pin, and the exported ``Variant(...)`` expression).

The scenario here varies ``as_table`` rather than ``distribute``: a distribute
re-run changes the output's dimensionality (whole file vs one slice), which
would give the variable two shapes and test the shape classifier instead of the
axis. ``as_table`` leaves both runs scalar. The ``distribute`` half — the
load-path supersession — is scidb's ``tests/test_run_option_variants.py``.

See ``.claude/plan-run-option-variants.md``.
"""

from __future__ import annotations

import pandas as pd
import pytest
from scidb import BaseVariable, for_each
from scistackplot import (
    PlotKind,
    PlotSpec,
    Role,
    RoleError,
    VariantSet,
    default_selection,
    validate,
    variant_set_mask,
)

from scistackplotdb import ScidbSource, load_variable
from scistackplotdb.variants import branch_params_for, selection_for, variant_graph

from conftest import SESSIONS, SUBJECTS, TRIALS, StepLength


class RunLoaded(BaseVariable):
    """One step's output — the variable both runs wrote to."""

    schema_version = 1


def measure(df):
    # One body that works either way, so the two runs share a function hash and
    # differ ONLY in as_table: handed the per-combo scalar it returns the value
    # shifted by 100; handed the one-row table it returns the row count (1.0).
    return float(len(df)) if isinstance(df, pd.DataFrame) else float(df) + 100.0


N_LOCATIONS = len(SUBJECTS) * len(SESSIONS) * len(TRIALS)
PLAIN = "distribute=false"
TABLED = "distribute=false, as_table=[df]"


@pytest.fixture
def both_runs(seeded):
    """The same step run without and then with ``as_table`` over the same
    trial-level input: same code, same constants, two records per location."""
    common = dict(
        inputs={"df": StepLength}, outputs=[RunLoaded], subject=[], session=[], trial=[]
    )
    for_each(measure, **common)
    for_each(measure, as_table=["df"], **common)
    return seeded


# --- the scenario is real -------------------------------------------------


def test_two_records_per_location(both_runs):
    loaded = load_variable(both_runs, "RunLoaded")
    assert len(loaded.frame) == 2 * N_LOCATIONS


# --- the axis --------------------------------------------------------------


def test_run_options_become_a_variant_column(both_runs):
    loaded = load_variable(both_runs, "RunLoaded")

    assert "Run:measure" in loaded.variant_columns, (
        "two records per location differ only by the run options that produced "
        "them; with no variant column they are replicates to every consumer"
    )
    assert set(loaded.frame["Run:measure"]) == {PLAIN, TABLED}


def test_the_axis_knows_its_function(both_runs):
    loaded = load_variable(both_runs, "RunLoaded")
    by_column = {axis["column"]: axis for axis in loaded.variant_axes}

    assert by_column["Run:measure"]["kind"] == "run"
    assert by_column["Run:measure"]["function"] == "measure"
    assert by_column["Run:measure"]["param"] is None


def test_the_latest_flag_is_attached_and_marks_the_newer_run(both_runs):
    """No code axis here, so before the fix there was no latest column at all
    — `if code_keys:` — and nothing for the default pin to hold on to."""
    loaded = load_variable(both_runs, "RunLoaded")

    assert loaded.latest_column is not None
    current = loaded.frame[loaded.frame[loaded.latest_column]]
    assert len(current) == N_LOCATIONS, "one current record per location"
    assert set(current["Run:measure"]) == {TABLED}


def test_a_function_that_ran_one_way_adds_no_column(seeded):
    for_each(
        measure,
        inputs={"df": StepLength},
        outputs=[RunLoaded],
        subject=[],
        session=[],
        trial=[],
    )
    loaded = load_variable(seeded, "RunLoaded")

    assert "Run:measure" not in loaded.frame.columns
    assert loaded.variant_columns == []
    assert loaded.latest_column is None


# --- what the figure does with it ------------------------------------------


def test_default_selection_opens_on_the_current_run_only(both_runs):
    """The pin is the flag, not a `Run:` level: pinning the flag keeps each
    location's own newest run and never combines with a level to select
    nothing (the trap `default_selection`'s docstring describes for code)."""
    table = ScidbSource(both_runs).get_table(["RunLoaded"])
    selection = default_selection(table)

    assert table.latest_column in selection and selection[table.latest_column] is True
    assert "Run:measure" not in selection
    mask = variant_set_mask(table.frame, selection, latest_column=table.latest_column)
    kept = table.frame[mask]
    assert len(kept) == N_LOCATIONS
    assert set(kept["Run:measure"]) == {TABLED}


def test_pooling_the_two_runs_is_refused(both_runs):
    """The guard that exists for exactly this — armed only once the axis exists."""
    table = ScidbSource(both_runs).get_table(["RunLoaded"])
    spec = PlotSpec(
        measures=["RunLoaded"],
        roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )

    with pytest.raises(RoleError, match="would be pooled"):
        validate(spec, table)


def test_a_variant_row_can_name_the_superseded_run(both_runs):
    """Selecting the older run explicitly is legal — the point of an axis over
    'never delete' data — and selects exactly its rows."""
    table = ScidbSource(both_runs).get_table(["RunLoaded"])
    mask = variant_set_mask(
        table.frame, {"Run:measure": PLAIN}, latest_column=table.latest_column
    )
    assert set(table.frame[mask]["Run:measure"]) == {PLAIN}
    assert int(mask.sum()) == N_LOCATIONS


# --- the picker's data model -----------------------------------------------


def test_variant_graph_lists_the_run_axis_with_its_levels(both_runs):
    loaded = load_variable(both_runs, "RunLoaded")
    graph = variant_graph(both_runs, loaded)
    run_axes = [a for a in graph["axes"] if a["kind"] == "run"]

    assert len(run_axes) == 1
    assert run_axes[0]["function"] == "measure"
    assert run_axes[0]["levels"] == [PLAIN, TABLED]


# --- translations ------------------------------------------------------------


def test_selection_round_trips_through_scidb_spelling(both_runs):
    """`Run:<fn>` <-> `__run__.<fn>`, so the location picker can ask scidb
    "which locations have this run" with the pin `Variant` would build."""
    table = ScidbSource(both_runs).get_table(["RunLoaded"])
    selection = {"Run:measure": TABLED}

    pins = branch_params_for(selection)
    assert pins == {"__run__.measure": TABLED}
    assert selection_for(pins, table) == selection


def test_bare_run_pin_resolves_against_the_only_run_axis(both_runs):
    table = ScidbSource(both_runs).get_table(["RunLoaded"])
    assert selection_for({"__run__": PLAIN}, table) == {"Run:measure": PLAIN}


def test_exported_endpoint_spells_the_pin_as_run_options(both_runs):
    from scistackplotdb.endpoint import variant_expression

    expression = variant_expression(
        "RunLoaded", VariantSet(name="current", selection={"Run:measure": TABLED})
    )
    assert expression == f"Variant(RunLoaded, fn='measure', run_options={TABLED!r})"
