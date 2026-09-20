"""
The pane behaviours pairwise cannot express: variant ROWS, the location
tree's own service, layout matchers, every summary statistic and y-axis
mode, show-sample across kinds, the 2-D heatmap, relational (x_measure)
scatters, and the exported code for every kind the panel draws.
"""

from __future__ import annotations

import pytest

pytest.importorskip("scistack_gui")
pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

import scidb  # noqa: E402
from scistack_gui.services import plot_service  # noqa: E402
from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    FacetOptions,
    MatchOp,
    Matcher,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    Statistic,
    StyleOptions,
    VariantSet,
    YAxis,
    capabilities,
    plot_data,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.resolved import Y, Y_HIGH, Y_LOW  # noqa: E402
from scistackplot.variants import VARIANT_FACTOR  # noqa: E402
from scistackplotdb import ScidbSource  # noqa: E402

from conftest import JOINTS, SESSIONS, SUBJECTS, TRIALS, attempt  # noqa: E402
from test_edge_schema import ONE_SESSION, ONE_SUBJECT, load_levels  # noqa: E402

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg", force=True)


@pytest.fixture(autouse=True)
def _fresh_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


@pytest.fixture(scope="module")
def source(example_db):
    return ScidbSource(example_db)


@pytest.fixture(scope="module")
def symmetry(source):
    return source.get_table(["CycleSymmetry"])


def _field(table) -> str:
    return table.field_factors[0].name


def _bar(table, **overrides) -> PlotSpec:
    base = dict(
        measures=["CycleSymmetry"],
        roles={_field(table): Role.FACET, "subject": Role.COLLAPSE, "session": Role.GROUP,
               "speed": Role.ITERATE, "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(overrides)
    return PlotSpec(**base)


def _draw(spec, table):
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    for figure in figures[:2]:
        _, error = attempt(render_plotly, figure)
        assert error is None, error
        drawn, error = attempt(render_matplotlib, figure)
        assert error is None, error
        matplotlib.pyplot.close(drawn)
    return figures


# --- Variants pane: rows --------------------------------------------------------------


def test_two_named_rows_of_one_variable_are_two_series(source):
    """The Variants pane's core move: two rows of AnkleOverThreshold, one
    per threshold, named by the user — a `Variant` factor with those names."""
    table = source.get_table(["AnkleOverThreshold"])
    (axis,) = table.variant_factors
    spec = PlotSpec(
        measures=["AnkleOverThreshold"],
        roles={VARIANT_FACTOR: Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE,
               "speed": Role.ITERATE, "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE},
        groups=[VARIANT_FACTOR, "session"],
        color=VARIANT_FACTOR,
        kind=PlotKind.BAR,
        variant_sets=[
            VariantSet(name="strict", selection={axis.name: "100"}),
            VariantSet(name="loose", selection={axis.name: "50"}),
        ],
    )
    figures = _draw(spec, table)
    panel = figures[0].panels[0]
    assert set(panel.frame["__color"].astype(str)) == {"strict", "loose"}
    data = plot_data(spec, table)
    assert set(data[VARIANT_FACTOR].astype(str)) == {"strict", "loose"}
    report = capabilities(spec, table)
    assert report["variants"]["sets"] and all(s["row_count"] > 0 for s in report["variants"]["sets"])


def test_a_row_whose_selection_matches_nothing_is_reported_not_silent(source):
    table = source.get_table(["AnkleOverThreshold"])
    (axis,) = table.variant_factors
    spec = PlotSpec(
        measures=["AnkleOverThreshold"],
        roles={VARIANT_FACTOR: Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE,
               "speed": Role.ITERATE, "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE},
        groups=[VARIANT_FACTOR, "session"],
        kind=PlotKind.BAR,
        variant_sets=[
            VariantSet(name="real", selection={axis.name: "50"}),
            VariantSet(name="ghost", selection={axis.name: "999"}),
        ],
    )
    report, error = attempt(capabilities, spec, table)
    assert error is None, error
    counts = {s["name"]: s["row_count"] for s in report["variants"]["sets"]}
    assert counts["ghost"] == 0 and counts["real"] > 0, counts
    figures, error = attempt(resolve, spec, table)
    assert error is None, error


def test_rows_for_two_variables_stack_and_can_be_collapsed_per_variable(source):
    table = source.get_table(["TrialMeanSymmetry", "ScaledTrialSymmetry"])
    field = _field(table)
    spec = PlotSpec(
        measures=["TrialMeanSymmetry"],
        roles={VARIANT_FACTOR: Role.GROUP, field: Role.FACET, "session": Role.GROUP,
               "subject": Role.COLLAPSE, "speed": Role.ITERATE, "trial": Role.COLLAPSE},
        groups=[VARIANT_FACTOR, "session"],
        color=VARIANT_FACTOR,
        kind=PlotKind.BAR,
        variant_sets=[
            VariantSet(name="raw", selection={}),
            VariantSet(name="scaled", selection={}, variable="ScaledTrialSymmetry"),
        ],
    )
    figures = _draw(spec, table)
    assert set(figures[0].panels[0].frame["__color"].astype(str)) == {"raw", "scaled"}


def test_the_default_pin_follows_a_code_edit(scratch_db, pipeline):
    """After a function body edit the opening variant must be the NEW code's
    records (`CodeIsLatest`), and the old ones must still be selectable."""
    from scidb import BaseVariable

    class Doubled(BaseVariable):
        pass

    load_levels(pipeline)
    where = dict(subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[])

    def doubled(knee):
        import pandas as pd
        return float(pd.Series(knee).iloc[0]) * 2

    scidb.for_each(doubled, {"knee": pipeline.CycleSymmetry["knee"]}, [Doubled], **where)

    def doubled(knee):  # noqa: F811
        import pandas as pd
        return float(pd.Series(knee).iloc[0]) * 2 + 100

    scidb.for_each(doubled, {"knee": pipeline.CycleSymmetry["knee"]}, [Doubled], **where)

    source = ScidbSource(scratch_db)
    table = source.get_table(["Doubled"])
    assert table.latest_column and table.default_pin
    from scistackplot import default_spec

    spec = default_spec(table)
    figures = resolve(spec, table)
    drawn = [float(v) for f in figures for p in f.panels for v in p.frame[Y]]
    latest_flag = table.frame[table.latest_column].value_counts(dropna=False).to_dict()
    # Per-record view of one location, straight from the identity query.
    from scidb.provenance_query import variant_identity_batch

    records = Doubled.load(as_df=True, version="all", speed="slow", trial="01", cycle="01")
    identity = variant_identity_batch(scratch_db._duck, list(records["record_id"])) if "record_id" in records.columns else {}
    per_record = [
        {
            "value": float(records[records["record_id"] == rid]["data"].iloc[0]),
            "fn_version": info.get("fn_version"),
            "is_latest": info.get("is_latest"),
            "saved_at": info.get("saved_at"),
            "fn_hash": (info.get("fn_hash") or "")[:8],
        }
        for rid, info in identity.items()
    ]
    assert all(v > 100 for v in drawn), (
        "the opening variant should be the edited code's records; drawn="
        f"{sorted(drawn)[:6]}... pin={table.default_pin} variant_sets="
        f"{[s.to_dict() for s in spec.variant_sets]} {table.latest_column}={latest_flag} "
        f"variant factors={[(f.name, [str(v) for v in f.levels]) for f in table.variant_factors]} "
        f"one location: {per_record}"
    )
    code_axes = [f.name for f in table.variant_factors]
    assert code_axes, "the code version is a selectable axis"


# --- Schema keys pane: the location tree service ----------------------------------------


def test_the_location_tree_answers_for_every_level_of_variable(example_db):
    for variable in ("CycleSymmetry", "TrialInfo", "SessionInfo", "Demographics"):
        tree, error = attempt(plot_service.location_tree, example_db, variable)
        assert error is None, f"{variable}: {error}"
        assert tree, variable


def test_the_location_tree_problems_only_view(example_db):
    tree, error = attempt(plot_service.location_tree, example_db, "CycleSymmetry", problems_only=True)
    assert error is None, error


def test_the_location_tree_under_a_variant_selection(example_db):
    table = ScidbSource(example_db).get_table(["AnkleOverThreshold"])
    (axis,) = table.variant_factors
    tree, error = attempt(
        plot_service.location_tree, example_db, "AnkleOverThreshold", selection={axis.name: "50"}
    )
    assert error is None, error
    assert tree


# --- Layout pane: matchers and grid shape -------------------------------------------------


def test_row_matchers_place_the_joints_and_drop_nothing(symmetry):
    """Two row rules claim ankle and knee; hip matches nothing and takes a
    free cell (the grid grows only if it has to) — never dropped."""
    spec = _bar(symmetry, facet=FacetOptions(
        rows=[Matcher(op=MatchOp.STARTS_WITH, value="a"), Matcher(op=MatchOp.STARTS_WITH, value="k")],
    ))
    figures = _draw(spec, symmetry)
    figure = figures[0]
    assert len(figure.panels) == len(JOINTS), "nothing is dropped"
    assert figure.grid_rows * figure.grid_cols >= len(JOINTS)
    assert figure.row_labels[:2] == ["a", "k"], figure.row_labels
    cells = [(p.grid_row, p.grid_col) for p in figure.panels]
    assert len(set(cells)) == len(cells), f"two panels share a cell: {cells}"


def test_a_matcher_matching_nothing_is_not_an_empty_grid(symmetry):
    spec = _bar(symmetry, facet=FacetOptions(cols=[Matcher(op=MatchOp.REGEX, value="^zzz")]))
    figures = _draw(spec, symmetry)
    assert figures[0].grid_cols >= 1 and len(figures[0].panels) == len(JOINTS)


@pytest.mark.parametrize("n_rows,n_cols", [(1, None), (None, 1), (2, None), (None, 2), (3, 3)])
def test_a_named_grid_shape_holds_every_panel(symmetry, n_rows, n_cols):
    spec = _bar(symmetry, facet=FacetOptions(n_rows=n_rows, n_cols=n_cols))
    figures = _draw(spec, symmetry)
    figure = figures[0]
    assert figure.grid_rows * figure.grid_cols >= len(figure.panels)


# --- Summary and Y-axis panes ----------------------------------------------------------------


@pytest.mark.parametrize("statistic,error", [
    (Statistic.MEAN, ErrorBand.SD), (Statistic.MEAN, ErrorBand.SEM), (Statistic.MEAN, ErrorBand.CI95),
    (Statistic.MEAN, ErrorBand.IQR), (Statistic.MEDIAN, ErrorBand.IQR), (Statistic.MEAN, ErrorBand.NONE),
], ids=lambda v: v.value)
def test_every_summary_statistic_draws_inside_its_limits(symmetry, statistic, error):
    spec = _bar(symmetry, aggregate=Aggregation(statistic=statistic, error=error))
    for figure in _draw(spec, symmetry):
        for panel in figure.panels:
            low, high = panel.y_limits
            assert (panel.frame[Y_LOW] >= low - 1e-9).all() and (panel.frame[Y_HIGH] <= high + 1e-9).all()
            if error is ErrorBand.NONE:
                assert (panel.frame[Y_LOW] == panel.frame[Y]).all()


@pytest.mark.parametrize("y_axis,style", [
    (YAxis(scope=[]), StyleOptions()),
    (YAxis(scope=["speed"]), StyleOptions()),
    (YAxis(scope=["speed", "ColName"]), StyleOptions()),
    (YAxis(minimum=0.0, maximum=250.0), StyleOptions()),
    (YAxis(minimum=50.0), StyleOptions()),
    (YAxis(scope=[]), StyleOptions(log_y=True)),
], ids=["global", "per-speed", "per-speed-and-joint", "manual", "manual-min-only", "log"])
def test_every_y_axis_mode_draws(symmetry, y_axis, style):
    scope = [s if s != "ColName" else _field(symmetry) for s in y_axis.scope]
    spec = _bar(symmetry, y_axis=YAxis(scope=scope, minimum=y_axis.minimum, maximum=y_axis.maximum), style=style)
    figures = _draw(spec, symmetry)
    for figure in figures:
        for panel in figure.panels:
            assert panel.y_limits is not None
            if y_axis.minimum is not None and y_axis.maximum is not None:
                assert panel.y_limits == (y_axis.minimum, y_axis.maximum)


def test_a_log_axis_over_zero_values_is_honest(scratch_db, pipeline):
    """Symmetry values can be 0.00 (perfectly symmetric): a log axis must
    not put log10(0) on the axis or raise."""
    load_levels(pipeline)
    table = ScidbSource(scratch_db).get_table(["CycleSymmetry"])
    spec = _bar(table, style=StyleOptions(log_y=True), kind=PlotKind.SCATTER)
    figures = _draw(spec, table)
    for figure in figures:
        for panel in figure.panels:
            low, high = panel.y_limits
            assert low > 0 and high > low


# --- Show sample across kinds -----------------------------------------------------------------


@pytest.mark.parametrize("kind", [PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.SCATTER, PlotKind.STRIP], ids=lambda k: k.value)
@pytest.mark.parametrize("shown,join", [(["subject"], None), (["trial"], None), (["trial"], True), (["cycle"], False)],
                         ids=["sample", "trial-auto", "trial-joined", "cycle-points"])
def test_show_sample_on_every_summative_kind(symmetry, kind, shown, join):
    spec = _bar(symmetry, kind=kind, show_sample=shown, join_sample=join)
    figures = _draw(spec, symmetry)
    figure = figures[0]
    assert figure.sample_shown, "the overlay is on"
    panel = figure.panels[0]
    assert panel.sample is not None and len(panel.sample) > 0
    per_point = {"subject": 1, "trial": len(TRIALS), "cycle": len(TRIALS) * 10}[shown[0]]
    shown_columns = [c for c in ("subject", "session", "trial", "cycle") if c in panel.sample.columns]
    assert len(panel.sample) == len(SUBJECTS) * len(SESSIONS) * per_point, (
        f"{len(panel.sample)} overlay point(s); columns={list(panel.sample.columns)}; "
        f"per {shown_columns[:3]}:\n{panel.sample.groupby(shown_columns[:3]).size().to_string()}"
    )
    if join is not None:
        assert figure.sample_join is join


def test_show_sample_with_a_coloured_layer_and_a_nested_axis(symmetry):
    spec = _bar(symmetry, roles={**_bar(symmetry).roles, "speed": Role.GROUP},
                groups=["speed", "session"], color="speed", show_sample=["trial"])
    figures = _draw(spec, symmetry)
    assert figures[0].panels[0].sample is not None
    spec = _bar(symmetry, roles={**_bar(symmetry).roles, "speed": Role.GROUP},
                groups=["speed", "session"], show_sample=["trial"])
    figures = _draw(spec, symmetry)
    assert figures[0].x_plan is not None and figures[0].panels[0].sample is not None


def test_show_sample_with_a_variant_axis(source):
    table = source.get_table(["AnkleOverThreshold"])
    (axis,) = table.variant_factors
    spec = PlotSpec(
        measures=["AnkleOverThreshold"],
        roles={axis.name: Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE,
               "speed": Role.ITERATE, "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE},
        groups=[axis.name, "session"], color=axis.name, kind=PlotKind.BAR, show_sample=["trial"],
    )
    figures = _draw(spec, table)
    assert figures[0].panels[0].sample is not None


# --- the 2-D variable: heatmap -------------------------------------------------------------------


def test_the_2d_variable_is_a_heatmap_and_only_a_heatmap(source):
    table = source.get_table(["JointCoupling"])
    assert table.shape_of("JointCoupling") is Shape.MATRIX_2D
    from scistackplot import default_spec

    spec = default_spec(table)
    assert spec.kind is PlotKind.HEATMAP
    report = capabilities(spec, table)
    assert report["available"] == ["heatmap"]
    figures = _draw(spec, table)
    assert figures and figures[0].panels
    assert report["data_export"]["available"] is False


def test_a_heatmap_averages_the_matrices_it_collapses(source):
    table = source.get_table(["JointCoupling"])
    spec = PlotSpec(
        measures=["JointCoupling"],
        roles={"subject": Role.ITERATE, "session": Role.FACET, "speed": Role.ITERATE, "trial": Role.COLLAPSE},
        kind=PlotKind.HEATMAP,
    )
    figures = _draw(spec, table)
    assert len(figures) == len(SUBJECTS) * 2
    for figure in figures:
        assert len(figure.panels) == len(SESSIONS)
        for panel in figure.panels:
            matrix = panel.frame["__z"].iloc[0]
            assert len(matrix) == 3 and len(matrix[0]) == 3
            assert abs(float(matrix[0][0]) - 1.0) < 1e-9, "the diagonal stays 1 under averaging"


def test_grouping_a_2d_variable_is_refused_with_the_reason(source):
    table = source.get_table(["JointCoupling"])
    spec = PlotSpec(measures=["JointCoupling"], roles={"session": Role.GROUP, "subject": Role.ITERATE}, groups=["session"], kind=PlotKind.HEATMAP)
    from scistackplot.roles import validate

    _, error = attempt(validate, spec, table)
    assert error is not None and "2-D" in str(error)


# --- relational (x_measure) scatters ---------------------------------------------------------------


@pytest.mark.parametrize("color", [None, "session", "speed"])
def test_a_relational_scatter_of_two_cycle_level_scalars(source, color):
    """x_measure joins a second SCALAR variable at the same locations: knee
    excursion (from the waveform) against normalized knee symmetry."""
    table = source.get_table(["NormalizedKnee"], x_measure="KneeExcursion")
    roles = {"subject": Role.GROUP, "session": Role.GROUP, "speed": Role.GROUP,
             "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE}
    spec = PlotSpec(
        measures=["NormalizedKnee"], x_measure="KneeExcursion",
        roles=roles, groups=["subject", "session", "speed"], color=color, kind=PlotKind.SCATTER,
    )
    figures = _draw(spec, table)
    assert figures[0].panels[0].frame["__x"].notna().all()
    data, error = attempt(plot_data, spec, table)
    assert error is None, error
    assert "KneeExcursion" in data.columns and "NormalizedKnee" in data.columns
    # One point per (subject, session, speed, trial): cycles averaged into
    # the trial, on BOTH axes.
    assert len(data) == len(SUBJECTS) * len(SESSIONS) * 2 * len(TRIALS)


# --- export: every kind the panel draws compiles and runs ---------------------------------------------


@pytest.mark.parametrize("kind", [PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.SCATTER, PlotKind.STRIP, PlotKind.SPAGHETTI], ids=lambda k: k.value)
def test_export_of_every_scalar_kind_runs_on_the_pipelines_frame(example_db, pipeline, symmetry, kind):
    pytest.importorskip("seaborn")
    spec = _bar(symmetry, kind=kind)
    report = capabilities(spec, symmetry)
    entry = next(e for e in report["kinds"] if e["kind"] == str(kind))
    if entry.get("assignment"):
        spec = PlotSpec.from_dict({**spec.to_dict(), **entry["assignment"], "kind": str(kind)})
    exported = plot_service.export_code(example_db, spec.to_dict())
    source_code = exported.get("function_source") or exported["source"]
    namespace: dict = {}
    exec(compile(source_code, "<generated>", "exec"), namespace)  # noqa: S102
    fn = next(v for k, v in namespace.items() if callable(v) and k.startswith("plot_"))
    frame = example_db.load_all_as_df(pipeline.CycleSymmetry, layout="spread")
    figure = fn(frame.copy(), "figure.png")
    assert figure.axes
    matplotlib.pyplot.close(figure)


@pytest.mark.parametrize("what", ["variant_pin", "location", "level_group", "joined_factor", "show_sample"])
def test_export_of_the_spec_features_compiles(example_db, source, symmetry, what):
    pytest.importorskip("seaborn")
    if what == "variant_pin":
        table = source.get_table(["AnkleOverThreshold"])
        (axis,) = table.variant_factors
        spec = PlotSpec(
            measures=["AnkleOverThreshold"],
            roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "speed": Role.ITERATE, "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE},
            groups=["session"], kind=PlotKind.BAR,
            variant_sets=[VariantSet(name="strict", selection={axis.name: "100"})],
        )
    elif what == "location":
        from scistackplot import LocationFilter
        spec = _bar(symmetry, location_filter=LocationFilter(include=[[["subject", SUBJECTS[0]]]]))
    elif what == "level_group":
        from scistackplot import LevelGroup
        spec = _bar(symmetry, level_groups=[LevelGroup(name="Phase", source="session",
                    mapping={SESSIONS[0]: "early", **{s: "late" for s in SESSIONS[1:]}})],
                    roles={**_bar(symmetry).roles, "Phase": Role.GROUP}, groups=["session", "Phase"], color="Phase")
    elif what == "joined_factor":
        from scistackplot import FactorVariable
        table = source.get_table(["CycleSymmetry"], factor_variables=[FactorVariable(variable="Demographics", column="group")])
        spec = _bar(table, roles={**_bar(table).roles, "group": Role.GROUP}, groups=["session", "group"], color="group",
                    factor_variables=[FactorVariable(variable="Demographics", column="group")])
    else:
        spec = _bar(symmetry, show_sample=["trial"])
    exported, error = attempt(plot_service.export_code, example_db, spec.to_dict())
    assert error is None, f"{what}: {error}"
    compile(exported.get("function_source") or exported["source"], "<generated>", "exec")
