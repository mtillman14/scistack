"""
The plotting and "Save data" surfaces, swept over the example database.

`test_plot_layer.py` proves a few figures. This file goes wide instead:

* every plot kind the capability report offers, for the scalar AND the 1-D
  variable, resolved and rendered through BOTH renderers — plotly (the panel)
  and matplotlib (the saved file);
* the two-variant `AnkleOverThreshold` on a Variant axis;
* the exported seaborn endpoint run as a real `for_each` (fan-out parity);
* `save_figure` writing a real image;
* the CSV depth x long/wide matrix, a filtered spec, a pooled spec, and the
  GUI's data-save job — each with the parity check against the drawn marks.
"""

from __future__ import annotations

import time

import pandas as pd
import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    Filter,
    PlotKind,
    PlotSpec,
    Role,
    capabilities,
    data_export_options,
    default_spec,
    plot_data,
    render_matplotlib,
    render_plotly,
    resolve,
    resolve_one,
)
from scistackplot.resolved import COLOR, X, Y  # noqa: E402
from scistackplot.roles import complete_roles  # noqa: E402
from scistackplotdb import ScidbSource  # noqa: E402

from conftest import JOINTS, N_CYCLES, SESSIONS, SUBJECTS, TRIALS  # noqa: E402

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg", force=True)


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


def _scalar_spec(table, kind=PlotKind.BAR, **overrides) -> PlotSpec:
    """The user's example: session grouped, a figure per speed, subject /
    trial / cycle collapsed, one panel per joint."""
    base = dict(
        measures=["CycleSymmetry"],
        roles={
            _field(table): Role.FACET,
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "speed": Role.ITERATE,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=["session"],
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(overrides)
    return PlotSpec(**base)


def _series_spec(table, kind=PlotKind.BAND, **overrides) -> PlotSpec:
    base = dict(
        measures=["CycleWaveform"],
        roles={
            _field(table): Role.FACET,
            "subject": Role.ITERATE,
            "session": Role.ITERATE,
            "speed": Role.GROUP,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=["speed"],
        color="speed",
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(overrides)
    return PlotSpec(**base)


def _for_kind(spec: PlotSpec, table, kind: PlotKind) -> PlotSpec:
    """``spec`` switched to ``kind`` the way the panel does it: the report's
    per-kind assignment applied when there is one, else the roles kept."""
    report = capabilities(spec, table)
    entry = next(e for e in report["kinds"] if e["kind"] == str(kind))
    if not entry["available"]:
        pytest.skip(f"{kind}: {entry['reason']}")
    payload = {**spec.to_dict(), "kind": str(kind)}
    if entry.get("assignment"):
        payload.update(entry["assignment"])
    return PlotSpec.from_dict(payload)


def _render_both(figure) -> None:
    payload = render_plotly(figure)
    assert payload["data"], "plotly drew nothing"
    drawn = render_matplotlib(figure)
    try:
        assert drawn.axes, "matplotlib drew nothing"
    finally:
        matplotlib.pyplot.close(drawn)


def _assert_csv_matches_marks(spec: PlotSpec, table, figures) -> None:
    """The parity "Save data" promises, for the kinds whose marks are a plain
    function of the sample rows at one categorical x."""
    if spec.kind not in (PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.SCATTER, PlotKind.STRIP):
        return
    field = _field(table)
    measure = spec.y_measure
    long = plot_data(spec, table, fields_as_columns=False)
    for figure in figures:
        if figure.x_plan is not None:
            return  # nested x: the leaf key is composed, not a column
        rows = long
        for key, value in figure.figure_key.items():
            rows = rows[rows[key].astype(str) == str(value)]
        for panel in figure.panels:
            panel_rows = rows
            for key, value in panel.key.items():
                panel_rows = panel_rows[panel_rows[key].astype(str) == str(value)]
            assert not panel_rows.empty, (figure.figure_key, panel.key)
            x_column = next(
                (
                    name for name, role in complete_roles(spec, table).items()
                    if role is Role.GROUP and name in panel_rows.columns and name != spec.color
                ),
                None,
            )
            if x_column is None:
                return  # the kind's assignment left no plain tick layer to key on
            for level, sample in panel_rows.groupby(x_column):
                drawn = panel.frame[panel.frame[X].astype(str) == str(level)]
                if spec.color:
                    continue  # dodged marks: one per colour level, checked elsewhere
                if spec.kind is PlotKind.BAR:
                    assert drawn[Y].iloc[0] == pytest.approx(sample[measure].mean()), (level, field)
                else:
                    assert sorted(drawn[Y]) == pytest.approx(sorted(sample[measure]))


# --- every kind, both renderers ---------------------------------------------------


@pytest.mark.parametrize("kind", list(PlotKind), ids=[k.value for k in PlotKind])
def test_every_scalar_kind_resolves_renders_and_matches_its_csv(symmetry, kind):
    spec = _for_kind(_scalar_spec(symmetry), symmetry, kind)
    figures = resolve(spec, symmetry)
    assert figures
    _render_both(figures[0])
    _assert_csv_matches_marks(spec, symmetry, figures)


@pytest.mark.parametrize("kind", list(PlotKind), ids=[k.value for k in PlotKind])
def test_every_1d_kind_resolves_and_renders(waveform, kind):
    """Line and band draw the curves; a scalar kind reduces each curve with
    the cell statistic first and then draws like any scalar plot."""
    spec = _for_kind(_series_spec(waveform), waveform, kind)
    figure, labels, _ = resolve_one(spec, waveform, 0)
    assert labels
    assert figure.panels
    _render_both(figure)
    report = capabilities(spec, waveform)
    if kind in (PlotKind.LINE, PlotKind.BAND, PlotKind.HEATMAP):
        assert report["data_export"]["available"] is False
    else:
        assert report["cell_collapse"]["active"] is True
        assert report["data_export"]["available"] is True


def test_the_transport_stride_never_touches_a_save(waveform):
    """The panel downsamples a 1-D figure for transport; a save resolves at
    full resolution. Both must be the same figure at different densities."""
    spec = _series_spec(waveform, kind=PlotKind.LINE)
    preview, _, _ = resolve_one(spec, waveform, 0, max_points=200)
    full, _, _ = resolve_one(spec, waveform, 0, max_points=None)
    assert full.downsampled_from is None
    assert full.row_count >= preview.row_count
    assert preview.downsampled_from is None or preview.downsampled_from == full.row_count


# --- variants ---------------------------------------------------------------------


def test_the_two_parameter_variants_are_a_variant_axis(source, pipeline):
    """`ankle_over_threshold` ran with threshold 50 and 100: one variable, two
    variants, which the plot layer offers as a factor to colour by."""
    table = source.get_table(["AnkleOverThreshold"])
    variants = table.variant_factors
    assert len(variants) == 1, [f.name for f in table.factors]
    axis = variants[0]
    assert sorted(str(v) for v in axis.levels) == ["100", "50"]

    # `default_spec` opens on ONE pinned variant (the panel's default), which
    # answers the threshold axis and strips it. To compare the two variants
    # the spec must leave the axis unpinned and colour by it — what a user
    # does by moving the factor into the grouping.
    opened = default_spec(table)
    assert opened.variant_sets, "the opening spec pins a variant"
    spec = PlotSpec(
        measures=["AnkleOverThreshold"],
        roles={
            axis.name: Role.GROUP,
            "session": Role.GROUP,
            "subject": Role.COLLAPSE,
            "speed": Role.ITERATE,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=[axis.name, "session"],
        color=axis.name,
        kind=PlotKind.BAR,
    )
    figures = resolve(spec, table)
    assert figures
    panel = figures[0].panels[0]
    assert set(panel.frame[COLOR].astype(str)) == {"50", "100"}
    _render_both(figures[0])
    # The variant factor is an ordinary column in the CSV, so the two
    # thresholds can be compared side by side.
    data = plot_data(spec, table)
    assert axis.name in data.columns
    assert set(data[axis.name].astype(str)) == {"50", "100"}


# --- export: the pipeline draws what the panel previewed --------------------------


def test_the_exported_endpoint_fans_out_like_the_preview(symmetry, pipeline, example_db, tmp_path):
    pytest.importorskip("seaborn")
    from scidb import BaseVariable, PathOutput, for_each
    from scistackplotdb import generate_endpoint

    class CycleSymmetryFigure(BaseVariable):
        """Endpoint output: the figure's path."""

    spec = _scalar_spec(symmetry)
    interactive = resolve(spec, symmetry)
    code = generate_endpoint(
        spec, symmetry, input_variable="CycleSymmetry",
        path_template=str(tmp_path / "fig_{speed}.png"),
    )

    failures: dict = {}

    def capturing_for_each(*args, **kwargs):
        def progress(event):
            if event.get("event") == "summary":
                failures.update(event.get("failure_reasons") or {})
        kwargs.setdefault("save", False)  # the file is the test; keep the db clean
        return for_each(*args, _progress_fn=progress, **kwargs)

    namespace = {
        "for_each": capturing_for_each,
        "PathOutput": PathOutput,
        "CycleSymmetry": pipeline.CycleSymmetry,
        "CycleSymmetryFigure": CycleSymmetryFigure,
    }
    exec(compile(code.source, "<generated>", "exec"), namespace)  # noqa: S102
    files = sorted(tmp_path.glob("fig_*.png"))
    reasons = "\n".join(f"  {len(c)}x {r}" for r, c in failures.items()) or "  (none)"
    assert len(files) == len(interactive) == 2, f"\nfailures:\n{reasons}\n\n{code.source}"
    assert {p.stem.removeprefix("fig_") for p in files} == {"slow", "fast"}


# --- the image save -------------------------------------------------------------------


def test_save_figure_writes_the_current_figure(example_db, symmetry, tmp_path):
    pytest.importorskip("scistack_gui")
    from scistack_gui.services import plot_service

    plot_service.invalidate()
    target = tmp_path / "symmetry.png"
    result = plot_service.save_figure(
        example_db, _scalar_spec(symmetry).to_dict(), str(target), figure_index=0
    )
    assert result["ok"] is True, result
    (written,) = result["files"]
    assert written.endswith(".png")
    from pathlib import Path
    assert Path(written).stat().st_size > 0


# --- "Save data": the whole matrix ------------------------------------------------------


def test_every_depth_in_both_layouts(symmetry):
    spec = _scalar_spec(symmetry)
    options = data_export_options(spec, complete_roles(spec, symmetry), symmetry)
    assert [d.key for d in options.depths] == ["subject", "trial", "cycle"]
    default_long = plot_data(spec, symmetry, depth=None, fields_as_columns=False)
    field = _field(symmetry)
    for depth in options.depths:
        long = plot_data(spec, symmetry, depth=depth.key, fields_as_columns=False)
        wide = plot_data(spec, symmetry, depth=depth.key, fields_as_columns=True)
        assert list(long.columns) == depth.columns, depth.key
        assert list(wide.columns) == depth.wide_columns, depth.key
        assert len(wide) * len(JOINTS) == len(long), depth.key
        # Every deeper file re-collapses to the default: the depths are cuts
        # of ONE chain, and the wide file is a reshape of the long one.
        keys = ["subject", "session", "speed", field]
        back = long.groupby(keys, as_index=False, sort=False)["CycleSymmetry"].mean()
        merged = default_long.merge(back, on=keys, suffixes=("", "_back"))
        assert len(merged) == len(default_long), depth.key
        assert merged["CycleSymmetry"].to_numpy() == pytest.approx(
            merged["CycleSymmetry_back"].to_numpy()
        ), depth.key
        index_cols = [c for c in wide.columns if c not in JOINTS]
        pivot = long.pivot_table(
            index=index_cols, columns=field, values="CycleSymmetry", aggfunc="first"
        ).reset_index()
        paired = wide.merge(pivot, on=index_cols, suffixes=("", "_long"))
        assert len(paired) == len(wide), depth.key
        for joint in JOINTS:
            assert paired[joint].to_numpy() == pytest.approx(paired[f"{joint}_long"].to_numpy()), (
                depth.key, joint,
            )


def test_a_filter_narrows_the_csv_and_the_figure_alike(symmetry):
    spec = _scalar_spec(symmetry, filters=[Filter(column="session", include=["baseline"])])
    data = plot_data(spec, symmetry)
    assert set(data["session"]) == {"baseline"}
    for figure in resolve(spec, symmetry):
        for panel in figure.panels:
            assert set(panel.frame[X]) == {"baseline"}
    _assert_csv_matches_marks(spec, symmetry, resolve(spec, symmetry))


def test_pooled_writes_every_collapsed_level(symmetry):
    spec = _scalar_spec(symmetry, aggregate=Aggregation(error=ErrorBand.SD, pooled=True))
    options = data_export_options(spec, complete_roles(spec, symmetry), symmetry)
    assert len(options.depths) == 1 and options.pooled
    long = plot_data(spec, symmetry, fields_as_columns=False)
    assert {"trial", "cycle"} <= set(long.columns)
    assert len(long) == N_CYCLES * len(JOINTS)
    # The pooled bar is the mean of every cycle row, which is what was written.
    figure = resolve(spec, symmetry)[0]
    rows = long[long["speed"] == figure.figure_key["speed"]]
    field = _field(symmetry)
    for panel in figure.panels:
        joint_rows = rows[rows[field] == panel.key[field]]
        drawn = panel.frame.set_index(X)
        for session, sample in joint_rows.groupby("session"):
            assert drawn.loc[session, Y] == pytest.approx(sample["CycleSymmetry"].mean())


def test_the_gui_data_save_job_writes_the_trial_depth(example_db, symmetry, tmp_path, monkeypatch):
    """The path the panel's button takes: a background job, three messages,
    and the file — here at depth="trial", wide."""
    pytest.importorskip("scistack_gui")
    from scistack_gui.api import ws as ws_mod
    from scistack_gui.services import plot_service

    plot_service.invalidate()
    messages: list = []
    monkeypatch.setattr(ws_mod, "push_message", messages.append)
    target = tmp_path / "trials.csv"
    started = plot_service.start_save_job(
        example_db, _scalar_spec(symmetry).to_dict(), str(target),
        what="data", depth="trial", fields_as_columns=True,
    )
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not any(
        m.get("type") in ("plot_save_complete", "plot_save_failed") for m in messages
    ):
        time.sleep(0.05)
    done = next((m for m in messages if m.get("type") == "plot_save_complete"), None)
    failed = next((m for m in messages if m.get("type") == "plot_save_failed"), None)
    assert done is not None, failed or "no completion message"
    assert done["job_id"] == started["job_id"]
    assert done["files"] == [str(target)]
    written = pd.read_csv(target, dtype={"subject": str, "trial": str})
    assert list(written.columns) == ["subject", "session", "speed", "trial", *JOINTS]
    assert len(written) == len(SUBJECTS) * len(SESSIONS) * 2 * len(TRIALS)
    assert done.get("rows") == len(written)
