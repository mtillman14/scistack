"""
Plot-spec edge cases (plan section B): the space of settings the panel can
produce, including the contradictory ones.

The contract (conftest.attempt): every call returns, or raises a typed error
with a reason. B1 is the centrepiece — every assignment of the four roles to
the five schema keys, for several kinds — and the rest are the specific
contradictions a user reaches by clicking.
"""

from __future__ import annotations

import hashlib
import itertools

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    Filter,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    RoleError,
    capabilities,
    data_export_options,
    default_spec,
    plot_data,
    render_matplotlib,
    render_plotly,
    resolve,
    resolve_one,
)
from scistackplot.resolved import X, Y, Y_LOW  # noqa: E402
from scistackplot.roles import complete_roles, validate  # noqa: E402
from scistackplot.xaxis import is_spacer  # noqa: E402
from scistackplotdb import ScidbSource  # noqa: E402

from conftest import FULL, JOINTS, N_CYCLES, SESSIONS, SUBJECTS, attempt  # noqa: E402

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg", force=True)

KEYS = ["subject", "session", "speed", "trial", "cycle"]
ROLES = [Role.GROUP, Role.FACET, Role.ITERATE, Role.COLLAPSE]
LETTER = {Role.GROUP: "G", Role.FACET: "F", Role.ITERATE: "I", Role.COLLAPSE: "C"}


@pytest.fixture(scope="module")
def source(example_db):
    return ScidbSource(example_db)


@pytest.fixture(scope="module")
def symmetry(source):
    return source.get_table(["CycleSymmetry"])


@pytest.fixture(scope="module")
def waveform(source):
    return source.get_table(["CycleWaveform"])


def _field(table) -> str:
    return table.field_factors[0].name


def _assignment_spec(table, measure: str, assignment, kind: PlotKind) -> PlotSpec:
    roles = {_field(table): Role.FACET, **dict(zip(KEYS, assignment, strict=True))}
    return PlotSpec(measures=[measure], roles=roles, kind=kind)


_REPORTS: dict = {}


def _report_for(spec: PlotSpec, table):
    """`capabilities` once per assignment, not once per kind: the report
    already judges every kind, and it is the sweep's most expensive call."""
    key = (id(table), tuple(sorted((k, str(v)) for k, v in spec.roles.items())))
    if key not in _REPORTS:
        _REPORTS[key] = attempt(capabilities, spec, table)
    return _REPORTS[key]


def _sweep_contract(spec: PlotSpec, table, *, render_mpl: bool) -> str:
    """Run one (assignment, kind) through the contract; returns a one-word
    outcome for the test id's sake."""
    report, error = _report_for(spec, table)
    if report is None:
        return "report-refused"
    entry = next(e for e in report["kinds"] if e["kind"] == str(spec.kind))
    # The panel applies a kind's suggested assignment on the click that picks
    # it (`kinds[].assignment`), so judge the kind on that spec — the one the
    # user would actually see drawn.
    if entry.get("assignment"):
        spec = PlotSpec.from_dict({**spec.to_dict(), **entry["assignment"]})

    if entry["available"]:
        figures, error = attempt(resolve, spec, table)
        assert error is None, (
            f"the report offered {spec.kind} for {spec.roles} but resolve refused: {error}"
        )
        assert figures, "offered kind resolved to no figures"
        first = figures[0]
        payload, error = attempt(render_plotly, first)
        assert error is None, f"plotly refused an offered figure: {error}"
        if render_mpl:
            drawn, error = attempt(render_matplotlib, first)
            assert error is None, f"matplotlib refused an offered figure: {error}"
            matplotlib.pyplot.close(drawn)
        data, error = attempt(plot_data, spec, table)
        assert (data is not None) != (error is not None)
        if data is not None:
            assert spec.y_measure in data.columns or any(
                str(c) in JOINTS for c in data.columns
            ), list(data.columns)
        return "drawn"

    # Not offered: the real path must refuse it too, with a reason — the two
    # must never disagree, or the panel greys out a kind the renderer would
    # draw. Through `resolve`, not a bare `validate` on the raw table: a
    # scalar kind cell-collapses a 1-D measure first, and the layer cap only
    # exists on the collapsed table.
    _, error = attempt(resolve, spec, table)
    assert error is not None, (
        f"the report refused {spec.kind} ({entry['reason']}) but resolve drew it"
    )
    assert isinstance(error, (RoleError, ValueError))
    return "refused"


def _ordinal(assignment) -> int:
    """A deterministic number for an assignment (enum hashes are salted per
    process, so `hash()` would sample a different eighth on every run)."""
    return sum(ROLES.index(role) * 4 ** i for i, role in enumerate(assignment))


def _always(assignment) -> bool:
    """Assignments the sample must never drop: every uniform one (all
    grouped, all collapsed, ...) and every one at or over the layer cap —
    the corner the sweep first found a bug in."""
    return len(set(assignment)) == 1 or assignment.count(Role.GROUP) >= 4


def _sampled(assignment) -> bool:
    """A deterministic, well-mixed eighth. Not `_ordinal % 8`: that depends
    only on the first two keys' roles and would fix them, sampling nothing of
    the others."""
    digest = hashlib.md5("".join(LETTER[r] for r in assignment).encode()).hexdigest()
    return int(digest, 16) % 8 == 0


ALL_ASSIGNMENTS = list(itertools.product(ROLES, repeat=5))
#: The full 4^5 grid under SCISTACK_INTEGRATION_FULL=1; otherwise a
#: deterministic eighth of it plus the corners — the whole grid took ~20
#: minutes, and the rules it exercises are far fewer than 1024.
ASSIGNMENTS = [a for a in ALL_ASSIGNMENTS if FULL or _always(a) or _sampled(a)]
ASSIGNMENT_IDS = ["".join(LETTER[r] for r in a) for a in ASSIGNMENTS]


# --- B1: the sweep -----------------------------------------------------------------------


@pytest.mark.parametrize("assignment", ASSIGNMENTS, ids=ASSIGNMENT_IDS)
@pytest.mark.parametrize("kind", [PlotKind.BAR, PlotKind.BOX, PlotKind.SCATTER], ids=lambda k: k.value)
def test_every_role_assignment_scalar(symmetry, assignment, kind):
    spec = _assignment_spec(symmetry, "CycleSymmetry", assignment, kind)
    # matplotlib on a deterministic eighth of the grid: full coverage of the
    # rules, without 3000 Agg figures.
    render_mpl = _ordinal(assignment) % 8 == 0
    _sweep_contract(spec, symmetry, render_mpl=render_mpl)


@pytest.mark.parametrize("assignment", ASSIGNMENTS, ids=ASSIGNMENT_IDS)
@pytest.mark.parametrize("kind", [PlotKind.LINE, PlotKind.BAND, PlotKind.BOX], ids=lambda k: k.value)
def test_every_role_assignment_1d(waveform, assignment, kind):
    spec = _assignment_spec(waveform, "CycleWaveform", assignment, kind)
    _sweep_contract(spec, waveform, render_mpl=_ordinal(assignment) % 16 == 0)


# --- B2 / B3: the two extremes -----------------------------------------------------------


def test_everything_iterated_is_one_figure_per_record(symmetry):
    spec = _assignment_spec(symmetry, "CycleSymmetry", [Role.ITERATE] * 5, PlotKind.SCATTER)
    figures = resolve(spec, symmetry)
    assert len(figures) == N_CYCLES
    assert all(len(panel.frame) == 1 for panel in figures[0].panels)


def test_everything_collapsed_is_one_mark(symmetry):
    spec = _assignment_spec(symmetry, "CycleSymmetry", [Role.COLLAPSE] * 5, PlotKind.BAR)
    (figure,) = resolve(spec, symmetry)
    assert len(figure.panels) == len(JOINTS)
    assert all(len(panel.frame) == 1 for panel in figure.panels)
    # The sample is the outermost collapsed key — subject — so the CSV is
    # one row per subject per joint, and its mean per joint is the bar.
    data = plot_data(spec, symmetry, fields_as_columns=False)
    assert len(data) == len(SUBJECTS) * len(JOINTS)
    field = _field(symmetry)
    for panel in figure.panels:
        rows = data[data[field] == panel.key[field]]
        assert panel.frame[Y].iloc[0] == pytest.approx(rows["CycleSymmetry"].mean())


# --- B4-B6: contradictions the validator must name ---------------------------------------


def test_colour_on_a_collapsed_factor_is_refused_by_name(symmetry):
    spec = PlotSpec(
        measures=["CycleSymmetry"],
        roles={_field(symmetry): Role.FACET, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        color="trial",
        kind=PlotKind.BAR,
    )
    _, error = attempt(validate, spec, symmetry)
    assert error is not None and "trial" in str(error)
    report, error = attempt(capabilities, spec, symmetry)
    assert report is not None or "trial" in str(error)


def test_collapsing_the_variant_factor_is_refused(source):
    table = source.get_table(["AnkleOverThreshold"])
    (axis,) = table.variant_factors
    spec = PlotSpec(
        measures=["AnkleOverThreshold"],
        roles={axis.name: Role.COLLAPSE, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
    )
    _, error = attempt(validate, spec, table)
    assert error is not None and axis.name in str(error)


def test_too_many_grouping_layers_is_refused_with_the_cap(symmetry):
    from scistackplot.spec import MAX_X_LAYERS

    spec = PlotSpec(
        measures=["CycleSymmetry"],
        roles={
            _field(symmetry): Role.FACET,
            "subject": Role.GROUP, "session": Role.GROUP, "speed": Role.GROUP,
            "trial": Role.GROUP, "cycle": Role.COLLAPSE,
        },
        groups=["trial", "speed", "session", "subject"],
        kind=PlotKind.BAR,
    )
    _, error = attempt(validate, spec, symmetry)
    assert error is not None
    assert str(MAX_X_LAYERS) in str(error) or "layer" in str(error).lower()


# --- B7-B11: stale and empty specs ---------------------------------------------------------


def _bar(table, **overrides) -> PlotSpec:
    base = dict(
        measures=["CycleSymmetry"],
        roles={
            _field(table): Role.FACET,
            "subject": Role.COLLAPSE, "session": Role.GROUP, "speed": Role.ITERATE,
            "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE,
        },
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(overrides)
    return PlotSpec(**base)


def test_a_filter_that_empties_the_data_draws_an_empty_figure(symmetry):
    spec = _bar(symmetry, filters=[Filter(column="session", include=["week99"])])
    figures, error = attempt(resolve, spec, symmetry)
    assert error is None, error
    assert figures and all(f.row_count == 0 for f in figures)
    for figure in figures:
        payload, error = attempt(render_plotly, figure)
        assert error is None, error
    data, error = attempt(plot_data, spec, symmetry)
    assert error is None, error
    assert len(data) == 0
    assert list(data.columns) == ["subject", "session", "speed", *JOINTS]


def test_a_filter_on_an_unknown_column_is_ignored(symmetry):
    spec = _bar(symmetry, filters=[Filter(column="no_such_column", include=["x"])])
    figures, error = attempt(resolve, spec, symmetry)
    assert error is None, error
    assert figures and figures[0].row_count > 0


def test_a_role_for_a_key_this_table_lacks_is_not_fatal(symmetry):
    """A spec saved against a deeper variable names a key that is not here."""
    roles = dict(_bar(symmetry).roles)
    roles["timepoint"] = Role.GROUP
    spec = _bar(symmetry, roles=roles)
    figures, error = attempt(resolve, spec, symmetry)
    assert error is None, f"a stale role should be dropped, not refused: {error}"
    assert figures and figures[0].row_count > 0


def test_show_sample_on_a_key_that_is_not_collapsed_is_ignored(symmetry):
    spec = _bar(symmetry, show_sample=["session"])
    figures, error = attempt(resolve, spec, symmetry)
    assert error is None, error
    assert figures[0].sample_shown == []


def test_a_depth_that_is_not_collapsed_names_the_choices(symmetry):
    spec = _bar(symmetry)
    for bad in ("session", "no_such_key", "ColName"):
        _, error = attempt(plot_data, spec, symmetry, depth=bad)
        assert error is not None, bad
        assert "subject" in str(error), str(error)


# --- B12: kind switches on one cached plan -------------------------------------------------


def test_kind_switches_on_a_cached_plan_all_draw(symmetry, waveform):
    from scistackplot import reduce as reduce_mod

    reduce_mod._plan_cache.clear()
    for kind in (PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.SCATTER, PlotKind.STRIP, PlotKind.BAR):
        figure, _, _ = resolve_one(_bar(symmetry, kind=kind), symmetry, 0)
        assert figure.panels and figure.row_count > 0, kind
        assert all(p.y_limits is not None for p in figure.panels), kind

    series = PlotSpec(
        measures=["CycleWaveform"],
        roles={
            _field(waveform): Role.FACET,
            "subject": Role.ITERATE, "session": Role.ITERATE, "speed": Role.GROUP,
            "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE,
        },
        groups=["speed"],
        color="speed",
        kind=PlotKind.LINE,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    for kind in (PlotKind.LINE, PlotKind.BAND, PlotKind.BOX, PlotKind.LINE, PlotKind.BAR, PlotKind.BAND):
        spec = PlotSpec.from_dict({**series.to_dict(), "kind": str(kind)})
        figure, _, _ = resolve_one(spec, waveform, 0)
        assert figure.panels and figure.row_count > 0, kind
        report = capabilities(spec, waveform)
        assert report["cell_collapse"]["active"] is (kind not in (PlotKind.LINE, PlotKind.BAND)), kind


# --- B13: pooled with everything else ------------------------------------------------------


def test_pooled_with_show_sample_and_depth(symmetry):
    spec = _bar(symmetry, aggregate=Aggregation(error=ErrorBand.SD, pooled=True), show_sample=["trial"])
    figures, error = attempt(resolve, spec, symmetry)
    assert error is None, error
    options = data_export_options(spec, complete_roles(spec, symmetry), symmetry)
    assert len(options.depths) == 1 and options.pooled
    _, error = attempt(plot_data, spec, symmetry, depth="trial")
    assert error is not None, "pooled offers one depth only"


# --- B14: degenerate data ------------------------------------------------------------------


def test_one_record_is_one_mark(source):
    table = source.get_table(["SubjectProfile"])
    spec = PlotSpec(
        measures=["SubjectProfile"],
        roles={f.name: Role.FACET for f in table.field_factors} | {"subject": Role.GROUP},
        groups=["subject"],
        kind=PlotKind.BAR,
        filters=[Filter(column="subject", include=[SUBJECTS[0]])],
    )
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    (figure,) = figures
    assert all(len(panel.frame) == 1 for panel in figure.panels)
    assert all((panel.frame[Y_LOW] == panel.frame[Y]).all() for panel in figure.panels)


def _null_table() -> LongTable:
    frame = pd.DataFrame({
        "subject": ["01", "01", "02", "02"],
        "session": ["a", "b", "a", "b"],
        "M": [np.nan] * 4,
    })
    return LongTable.from_frame(
        frame, factors=["subject", "session"], measures=["M"], name="M",
        schema_levels=["subject", "session"],
    )


def test_an_all_null_measure_is_honest_not_a_crash():
    table = _null_table()
    spec = PlotSpec(
        measures=["M"], roles={"subject": Role.COLLAPSE, "session": Role.GROUP},
        groups=["session"], kind=PlotKind.BAR, aggregate=Aggregation(error=ErrorBand.SD),
    )
    figures, error = attempt(resolve, spec, table)
    if error is None:
        assert all(f.row_count == 0 for f in figures)
        for figure in figures:
            _, error = attempt(render_plotly, figure)
            assert error is None, error
    data, error = attempt(plot_data, spec, table)
    if error is None:
        assert len(data) == 0


def test_a_default_spec_exists_for_every_variable_the_pipeline_made(source, pipeline):
    """Opening any variable in the panel: the default must resolve."""
    for name in (
        "CycleSymmetry", "CycleWaveform", "TrialInfo", "SessionInfo", "Demographics",
        "TrialMeanSymmetry", "CycleDeviation", "ScaledTrialSymmetry", "SubjectProfile",
        "NormalizedKnee", "TrialCadence", "SpeedChangeFromBaseline", "KneeExcursion",
        "AnkleOverThreshold",
    ):
        table = source.get_table([name])
        spec = default_spec(table)
        figures, error = attempt(resolve, spec, table)
        assert error is None, f"{name}: {error}"
        assert figures, name
        _, error = attempt(render_plotly, figures[0])
        assert error is None, f"{name}: {error}"


# --- B15 / B16: ragged data ------------------------------------------------------------------


def _ragged_struct() -> LongTable:
    rows = []
    for subject in ["01", "02"]:
        for session in ["pre", "post"]:
            for field in ["ankle", "knee", "hip"]:
                if subject == "02" and session == "post":
                    continue  # the whole combination is missing
                if subject == "01" and session == "pre" and field == "hip":
                    continue  # one field missing in one record
                rows.append({"subject": subject, "session": session, "ColName": field, "M": 1.0})
    return LongTable.from_frame(
        pd.DataFrame(rows), factors=["subject", "session", "ColName"], measures=["M"],
        field_factors=["ColName"], name="M",
        level_order={"session": ["pre", "post"], "ColName": ["ankle", "knee", "hip"]},
        schema_levels=["subject", "session"],
    )


def test_a_nested_axis_over_ragged_combinations_has_no_invented_positions():
    table = _ragged_struct()
    spec = PlotSpec(
        measures=["M"],
        roles={"ColName": Role.FACET, "subject": Role.GROUP, "session": Role.GROUP},
        groups=["session", "subject"],
        kind=PlotKind.BAR,
    )
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    (figure,) = figures
    assert figure.x_plan is not None
    leaves = [key for key in figure.x_plan.order if key and not is_spacer(key)]
    assert len(leaves) == 3, leaves  # (01,pre), (01,post), (02,pre) — never (02,post)
    _, error = attempt(render_plotly, figure)
    assert error is None, error
    _, error = attempt(render_matplotlib, figure)
    assert error is None, error


def test_a_missing_field_is_a_skipped_panel_and_an_empty_cell():
    table = _ragged_struct()
    spec = PlotSpec(
        measures=["M"],
        roles={"ColName": Role.FACET, "subject": Role.ITERATE, "session": Role.GROUP},
        groups=["session"],
        kind=PlotKind.BAR,
    )
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    first = next(f for f in figures if f.figure_key["subject"] == "01")
    hip = next((p for p in first.panels if p.key["ColName"] == "hip"), None)
    assert hip is None or "pre" not in set(hip.frame[X]), "no bar for a value that does not exist"
    wide, error = attempt(plot_data, spec, table)
    assert error is None, error
    row = wide[(wide["subject"] == "01") & (wide["session"] == "pre")]
    assert len(row) == 1 and np.isnan(row["hip"].item())
