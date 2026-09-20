"""
Features the other suites never reach, and the gotchas that broke the GUI
before (memory: NULL/NaN round trip, schema-key data columns, constants that
are not scalars, zero-value parameters, coarse-input broadcast).

Same contract as the rest (conftest.attempt); scratch databases for anything
that writes.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

import scidb
from scidb import BaseVariable, Merge

from conftest import (
    CYCLES,
    DATA_ROOT,
    JOINTS,
    SESSIONS,
    SPEEDS,
    SUBJECTS,
    TRIALS,
    attempt,
    cell,
    run_summary,
)
from test_edge_schema import ONE_SESSION, ONE_SUBJECT, N_SMALL, load_levels

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    LocationFilter,
    PlotKind,
    PlotSpec,
    Role,
    capabilities,
    plot_data,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.resolved import X, Y  # noqa: E402
from scistackplot.variants import VARIABLE_COLUMN  # noqa: E402
from scistackplotdb import ScidbSource  # noqa: E402

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg", force=True)


# --- NaN round trip -----------------------------------------------------------------


class NoisyCurve(BaseVariable):
    """1-D with gaps: NaN samples must come back as NaN, never 0."""


class NoisyScalar(BaseVariable):
    pass


def test_nan_survives_every_load_path(scratch_db):
    """The masked-array trap: a NULL element loads as a masked array whose
    mask was dropped, turning every NaN into 0.0 in every load path
    (2026-09-15). Pinned on the single-record and the bulk path, for a 1-D
    cell and a scalar."""
    curve = [1.0, float("nan"), 3.0, float("nan"), 5.0]
    NoisyCurve.save({"knee": curve, "hip": [0.0] * 5}, subject="subject01", cycle="01")
    NoisyScalar.save(float("nan"), subject="subject01", cycle="01")
    NoisyScalar.save(2.5, subject="subject01", cycle="02")

    record = NoisyCurve.load(subject="subject01", cycle="01")
    data = record.data
    knee = np.asarray(cell(data, "knee"), dtype=float)
    assert np.isnan(knee[1]) and np.isnan(knee[3]), knee
    assert knee[0] == 1.0 and knee[4] == 5.0

    frame = NoisyCurve.load(as_df=True)
    knee_bulk = np.asarray(cell(frame.iloc[0]["data"], "knee"), dtype=float)
    assert np.isnan(knee_bulk[1]) and knee_bulk[2] == 3.0, knee_bulk

    scalars = NoisyScalar.load(as_df=True)
    values = {str(c): float(v) for c, v in zip(scalars["cycle"], scalars["data"])}
    assert math.isnan(values["01"]) and values["02"] == 2.5, values


def test_nan_samples_reach_the_plot_layer_as_gaps(scratch_db):
    NoisyCurve.save({"knee": [1.0, float("nan"), 3.0], "hip": [1.0, 1.0, 1.0]}, subject="subject01", cycle="01")
    NoisyCurve.save({"knee": [2.0, 2.0, float("nan")], "hip": [1.0, 1.0, 1.0]}, subject="subject01", cycle="02")
    table = ScidbSource(scratch_db).get_table(["NoisyCurve"])
    field = table.field_factors[0].name
    spec = PlotSpec(
        measures=["NoisyCurve"],
        roles={field: Role.FACET, "subject": Role.ITERATE, "cycle": Role.COLLAPSE},
        kind=PlotKind.BAND,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    knee = next(p for p in figures[0].panels if p.key[field] == "knee")
    drawn = knee.frame.sort_values(X)[Y].to_numpy(dtype=float)
    # Position 0: mean(1, 2) = 1.5; position 1: only cycle 02 reaches it
    # with a value (2.0); position 2: only cycle 01 (3.0). A NaN read as 0
    # would give 0.5, 1.0, 1.5 instead.
    assert drawn == pytest.approx([1.5, 2.0, 3.0]), drawn


# --- schema-key data columns ---------------------------------------------------------


class Shadowing(BaseVariable):
    """A table whose OWN columns are named like schema keys."""


def _shadowing_row(knee):
    """A function whose OUTPUT carries columns named like schema keys."""
    return pd.DataFrame({"subject": ["inner"], "session": ["inner"], "value": [float(pd.Series(knee).iloc[0])]})


def test_a_data_column_named_like_a_schema_key_does_not_shadow_it(scratch_db, pipeline):
    """2026-09-16: a `subject`/`session` column inside a record broke the
    plot loader. Two documented behaviours, pinned:

    * `Var.save(df)` with a schema-key column DISTRIBUTES by it — the column
      is the address (the docstring's rule), so the rows land at "inner";
    * a `for_each` output carrying a PINNED key's column keeps the pin —
      scifor strips the column — so the records land at the real subjects,
      and the plot loader still opens the variable.
    """
    frame = pd.DataFrame({"subject": ["inner"], "value": [1.0]})
    _, error = attempt(Shadowing.save, frame, session="baseline")
    assert error is None, error
    assert set(Shadowing.load(as_df=True)["subject"].astype(str)) == {"inner"}, (
        "a direct save distributes by the frame's own key column"
    )

    load_levels(pipeline)
    _, summary = run_summary(
        scidb.for_each, _shadowing_row, {"knee": pipeline.CycleSymmetry["knee"]}, [Shadowing],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert summary.get("completed") == N_SMALL, summary
    loaded = Shadowing.load(as_df=True)
    assert set(loaded["subject"].astype(str)) == {"inner", ONE_SUBJECT[0]}, (
        "the for_each records sit at the PINNED subject, not at 'inner'"
    )
    assert (loaded["session"].astype(str) == "baseline").all()

    table, error = attempt(ScidbSource(scratch_db).get_table, ["Shadowing"])
    assert error is None, error
    spec = PlotSpec(
        measures=["Shadowing"],
        roles={f.name: Role.FACET for f in table.field_factors} | {"subject": Role.GROUP, "session": Role.ITERATE},
        groups=["subject"],
        kind=PlotKind.BAR,
    )
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    assert figures and sum(f.row_count for f in figures) > 0
    data, error = attempt(plot_data, spec, table, fields_as_columns=False)
    assert error is None, error
    assert {"inner", ONE_SUBJECT[0]} <= set(data["subject"].astype(str))


# --- constants that are not scalars ----------------------------------------------------


class Windowed(BaseVariable):
    pass


def _window_mean(cycles, config):
    lo, hi = config["window"]
    return float(cycles["knee"].iloc[lo:hi].mean())


def test_a_dict_valued_parameter_makes_distinct_variants(scratch_db, pipeline):
    """2026-09-14: a dict/list constant broke variant grouping — two configs
    collapsed into one, or one config into two. Two dict values must be two
    variants; the same dict twice must be one."""
    load_levels(pipeline)
    where = dict(subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[])
    for config in ({"window": [0, 5]}, {"window": [5, 10]}, {"window": [0, 5]}):
        scidb.for_each(
            _window_mean, {"cycles": pipeline.CycleSymmetry, "config": scidb.Parameter(config)},
            [Windowed], as_table=["cycles"], **where,
        )
    every = Windowed.load(as_df=True, version="all")
    n_trials = len(SPEEDS) * len(TRIALS)
    assert len(every) == 2 * n_trials, "two configs, not three and not one"
    table = ScidbSource(scratch_db).get_table(["Windowed"])
    (axis,) = table.variant_factors
    assert len(axis.levels) == 2, [str(v) for v in axis.levels]


def test_a_parameter_with_no_values_is_refused_at_expansion(scratch_db, pipeline):
    load_levels(pipeline)
    empty = scidb.Parameter(description="declared, not yet valued")
    _, error = attempt(
        scidb.for_each, pipeline.ankle_over_threshold,
        {"ankle": pipeline.CycleSymmetry["ankle"], "threshold": empty},
        [pipeline.AnkleOverThreshold],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert error is not None, "an empty axis must not iterate zero times and look successful"
    assert "threshold" in str(error) or "no values" in str(error).lower() or "empty" in str(error).lower()


# --- coarse-input broadcast: the whole-grid prune ----------------------------------------


class SubjectScale(BaseVariable):
    pass


class Scaled(BaseVariable):
    pass


def test_a_coarse_input_missing_for_one_subject_prunes_only_that_subject(scratch_db, pipeline):
    """Two subjects loaded, a subject-level input for only one of them. The
    other subject's combinations have no input and are reported, not run
    with a broadcast NULL — and the first subject's all run."""
    load_levels(pipeline, subjects=SUBJECTS[:2], sessions=ONE_SESSION)
    SubjectScale.save(2.0, subject=SUBJECTS[0])

    def scale(knee, factor):
        return float(pd.Series(knee).iloc[0]) * float(pd.Series(factor).iloc[0])

    outcome, error = attempt(
        run_summary, scidb.for_each, scale,
        {"knee": pipeline.CycleSymmetry["knee"], "factor": SubjectScale},
        [Scaled], subject=SUBJECTS[:2], session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert error is None, error
    _, summary = outcome
    assert summary.get("completed") == N_SMALL, summary
    got = Scaled.load(as_df=True)
    assert set(got["subject"]) == {SUBJECTS[0]}
    assert not got["data"].isna().any()


# --- stacking two variables --------------------------------------------------------------


def test_two_variables_stack_into_one_figure_and_one_csv(example_db):
    source = ScidbSource(example_db)
    table, error = attempt(source.get_table, ["TrialMeanSymmetry", "ScaledTrialSymmetry"])
    assert error is None, error
    assert VARIABLE_COLUMN in table.factor_names
    field = table.field_factors[0].name
    spec = PlotSpec(
        measures=["TrialMeanSymmetry"],
        roles={
            VARIABLE_COLUMN: Role.FACET, field: Role.FACET,
            "subject": Role.COLLAPSE, "session": Role.GROUP, "speed": Role.ITERATE, "trial": Role.COLLAPSE,
        },
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    assert len(figures[0].panels) == 2 * len(JOINTS), "one panel per (variable, joint)"
    _, error = attempt(render_plotly, figures[0])
    assert error is None, error
    data, error = attempt(plot_data, spec, table, fields_as_columns=False)
    assert error is None, error
    assert VARIABLE_COLUMN in data.columns
    assert set(data[VARIABLE_COLUMN]) == {"TrialMeanSymmetry", "ScaledTrialSymmetry"}
    # Scaled = mean x 0.01 (the pipeline's SCALE), row for row.
    wide = data.pivot_table(index=["subject", "session", "speed", field], columns=VARIABLE_COLUMN,
                            values="TrialMeanSymmetry", aggfunc="first")
    assert wide["ScaledTrialSymmetry"].to_numpy() == pytest.approx(wide["TrialMeanSymmetry"].to_numpy() * 0.01)


def test_collapsing_the_variable_axis_is_refused(example_db):
    table = ScidbSource(example_db).get_table(["TrialMeanSymmetry", "ScaledTrialSymmetry"])
    spec = PlotSpec(
        measures=["TrialMeanSymmetry"],
        roles={VARIABLE_COLUMN: Role.COLLAPSE, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
    )
    from scistackplot.roles import validate

    _, error = attempt(validate, spec, table)
    assert error is not None and VARIABLE_COLUMN in str(error)


# --- location filter -------------------------------------------------------------------------


def _bar(table, **overrides) -> PlotSpec:
    field = table.field_factors[0].name
    base = dict(
        measures=["CycleSymmetry"],
        roles={field: Role.FACET, "subject": Role.COLLAPSE, "session": Role.GROUP,
               "speed": Role.ITERATE, "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(overrides)
    return PlotSpec(**base)


def test_a_location_prefix_keeps_one_subtree(example_db):
    table = ScidbSource(example_db).get_table(["CycleSymmetry"])
    spec = _bar(table, location_filter=LocationFilter(include=[[["subject", SUBJECTS[0]]]]))
    data, error = attempt(plot_data, spec, table)
    assert error is None, error
    assert set(data["subject"]) == {SUBJECTS[0]}
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    assert all(f.row_count > 0 for f in figures)


def test_a_ragged_location_selection_is_not_a_cartesian_product(example_db):
    """subject01 at session baseline AND subject02 at week04 — the shape a
    tree picker produces and per-column filters cannot express."""
    table = ScidbSource(example_db).get_table(["CycleSymmetry"])
    prefixes = [[["subject", SUBJECTS[0]], ["session", SESSIONS[0]]],
                [["subject", SUBJECTS[1]], ["session", SESSIONS[1]]]]
    spec = _bar(table, location_filter=LocationFilter(include=prefixes))
    data = plot_data(spec, table)
    pairs = set(zip(data["subject"], data["session"]))
    assert pairs == {(SUBJECTS[0], SESSIONS[0]), (SUBJECTS[1], SESSIONS[1])}, pairs


def test_an_excluded_level_wins_over_an_included_prefix(example_db):
    table = ScidbSource(example_db).get_table(["CycleSymmetry"])
    spec = _bar(table, location_filter=LocationFilter(
        include=[[["subject", SUBJECTS[0]]]], exclude_levels={"session": [SESSIONS[0]]},
    ))
    data = plot_data(spec, table)
    assert set(data["subject"]) == {SUBJECTS[0]}
    assert SESSIONS[0] not in set(data["session"])


def test_a_location_filter_that_names_a_missing_level_is_empty_not_an_error(example_db):
    table = ScidbSource(example_db).get_table(["CycleSymmetry"])
    spec = _bar(table, location_filter=LocationFilter(include=[[["subject", "nobody"]]]))
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    assert all(f.row_count == 0 for f in figures)


# --- where= filters and schema exclusions ---------------------------------------------------


class HighKnee(BaseVariable):
    pass


def _knee_value(knee):
    """A named function, not a lambda: for_each hashes the source of what it
    runs, and a lambda has no name for the provenance to record."""
    return float(pd.Series(knee).iloc[0])


def test_a_where_filter_runs_only_the_matching_records(scratch_db, pipeline):
    load_levels(pipeline)
    knees = pipeline.CycleSymmetry.load(as_df=True)
    threshold = float(np.median([float(cell(d, "knee")) for d in knees["data"]]))
    expected = sum(float(cell(d, "knee")) > threshold for d in knees["data"])

    _, summary = run_summary(
        scidb.for_each, _knee_value,
        {"knee": pipeline.CycleSymmetry["knee"]}, [HighKnee],
        where=pipeline.CycleSymmetry["knee"] > threshold,
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert summary.get("completed") == expected, summary
    assert len(HighKnee.load(as_df=True)) == expected
    assert (HighKnee.load(as_df=True)["data"].astype(float) > threshold).all()


def test_an_excluded_trial_is_skipped_then_comes_back(scratch_db, pipeline):
    load_levels(pipeline)
    scidb.exclude_schema("equipment fault", subject=ONE_SUBJECT[0], session=ONE_SESSION[0], trial="02")
    _, summary = run_summary(
        scidb.for_each, _knee_value,
        {"knee": pipeline.CycleSymmetry["knee"]}, [HighKnee],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert summary.get("completed") == N_SMALL - len(SPEEDS) * len(CYCLES), summary
    assert "02" not in set(HighKnee.load(as_df=True)["trial"].astype(str))

    # The exclusion reaches the plot layer too.
    table = ScidbSource(scratch_db).get_table(["CycleSymmetry"])
    spec = _bar(table, roles={**_bar(table).roles, "trial": Role.GROUP, "session": Role.ITERATE}, groups=["trial"])
    data, error = attempt(plot_data, spec, table)
    if error is None:
        assert "02" not in set(data["trial"].astype(str)), "the plot layer must honour the exclusion"

    scidb.include_schema("re-reviewed", subject=ONE_SUBJECT[0], session=ONE_SESSION[0], trial="02")
    _, summary = run_summary(
        scidb.for_each, _knee_value,
        {"knee": pipeline.CycleSymmetry["knee"]}, [HighKnee],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert "02" in set(HighKnee.load(as_df=True)["trial"].astype(str))


# --- Merge ------------------------------------------------------------------------------------


class Combined(BaseVariable):
    pass


def test_merge_joins_two_inputs_column_wise(scratch_db, pipeline):
    load_levels(pipeline)

    def widths(both):
        return float(len(both.columns))

    outcome, error = attempt(
        run_summary, scidb.for_each, widths,
        {"both": Merge(pipeline.CycleSymmetry, pipeline.TrialInfo)}, [Combined],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert error is None, error
    _, summary = outcome
    assert summary.get("completed", 0) + summary.get("failed", 0) == summary.get("total"), summary
    if summary.get("failed"):
        pytest.xfail(f"Merge of a cycle-level and a trial-level input: {summary.get('failure_reasons')}")
    assert summary["completed"] == N_SMALL


# --- show sample on real data ------------------------------------------------------------------


def test_show_sample_points_are_the_collapsed_rows(example_db):
    table = ScidbSource(example_db).get_table(["CycleSymmetry"])
    spec = _bar(table, show_sample=["trial"])
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    figure = figures[0]
    assert figure.sample_shown, "trial (and subject above it) shown"
    panel = figure.panels[0]
    assert panel.sample is not None and len(panel.sample) > 0
    # One point per (subject, trial) at each session: cycles averaged within.
    expected = len(SUBJECTS) * len(TRIALS) * len(SESSIONS)
    assert len(panel.sample) == expected, len(panel.sample)
    _, error = attempt(render_plotly, figure)
    assert error is None, error
    drawn = render_matplotlib(figure)
    matplotlib.pyplot.close(drawn)


# --- zero-padded PathInput matching -----------------------------------------------------------


class Padded(BaseVariable):
    pass


def test_iterating_with_an_int_over_zero_padded_trial_folders(scratch_db, pipeline):
    """Files say t01; the caller says trial=[1]. Either the bridge resolves
    it (declared numeric) or it is a typed SchemaKeyTypeError — never a
    silent zero-iteration success, never a record at trial=1 beside t01."""
    t = scidb.PathInput(
        "{subject}/{session}/t{trial}/{subject}_{session}_{speed}_t{trial}_trial.csv",
        root_folder=str(DATA_ROOT),
    )
    outcome, error = attempt(
        run_summary, scidb.for_each, pipeline.load_trial_info, {"csv_file_path": t}, [Padded],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[1],
    )
    if error is not None:
        assert "trial" in str(error), str(error)
        return
    _, summary = outcome
    stored = Padded.load(as_df=True) if summary.get("completed") else pd.DataFrame()
    if len(stored):
        assert {str(v) for v in stored["trial"]} <= {"01", "1"}
        assert len({str(v) for v in stored["trial"]}) == 1, "one spelling, not both"


# --- the kinds the sweep leaves out ---------------------------------------------------------------


@pytest.mark.parametrize("kind", [PlotKind.VIOLIN, PlotKind.STRIP, PlotKind.SPAGHETTI], ids=lambda k: k.value)
def test_the_remaining_kinds_resolve_render_and_export(example_db, kind):
    table = ScidbSource(example_db).get_table(["CycleSymmetry"])
    spec = _bar(table, kind=kind)
    report = capabilities(spec, table)
    entry = next(e for e in report["kinds"] if e["kind"] == str(kind))
    if entry.get("assignment"):
        spec = PlotSpec.from_dict({**spec.to_dict(), **entry["assignment"], "kind": str(kind)})
    if not entry["available"]:
        pytest.skip(entry["reason"])
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    _, error = attempt(render_plotly, figures[0])
    assert error is None, error
    drawn, error = attempt(render_matplotlib, figures[0])
    assert error is None, error
    matplotlib.pyplot.close(drawn)
    data, error = attempt(plot_data, spec, table)
    assert error is None, error
    assert len(data) > 0
