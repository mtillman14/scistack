"""
Generated code must actually run.

The export path emits literal seaborn/matplotlib source rather than a call back
into this package, so the only meaningful test is to execute it and check that
it returns a Figure — a generated snippet that doesn't run is worse than no
export at all.
"""

from __future__ import annotations

import pytest

from scistackplot import (
    PlotKind,
    PlotSpec,
    Role,
    extract_spec,
    generate_plot_function,
    generate_script,
)
from scistackplot.spec import Aggregation, ErrorBand, Filter, Statistic

pytest.importorskip("seaborn")
matplotlib = pytest.importorskip("matplotlib")


def _run(source: str, frame, function_name: str):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


def test_generated_box_plot_runs(scalar_table, scalar_frame):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)

    assert source.startswith("def plot_steplength(df, filename):")
    figure = _run(source, scalar_frame, "plot_steplength")
    assert figure.axes
    matplotlib.pyplot.close(figure)


def test_generated_code_uses_seaborn_not_this_package(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)

    assert "sns.catplot" in source
    assert "scistackplot.render" not in source
    assert "import scistackplot" not in source


def test_generated_band_plot_runs(series_table, series_frame):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        color="session",
        kind=PlotKind.BAND,
        aggregate=Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD),
    )
    source = generate_plot_function(spec, series_table)

    assert "df.explode" in source
    assert 'errorbar="sd"' in source
    figure = _run(source, series_frame, "plot_signal")
    assert figure.axes
    matplotlib.pyplot.close(figure)


def test_generated_collapse_emits_a_groupby(scalar_table, scalar_frame):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.SCATTER,
    )
    source = generate_plot_function(spec, scalar_table)

    assert "# collapse trial within subject, session" in source
    assert ".groupby(" in source
    figure = _run(source, scalar_frame, "plot_steplength")
    matplotlib.pyplot.close(figure)


def test_generated_filters_are_applied(scalar_table, scalar_frame):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "trial": Role.COLLAPSE, "session": Role.COLLAPSE},
        kind=PlotKind.BOX,
        filters=[Filter(column="session", include=["pre"])],
    )
    source = generate_plot_function(spec, scalar_table)

    assert "isin(['pre'])" in source
    figure = _run(source, scalar_frame, "plot_steplength")
    matplotlib.pyplot.close(figure)


def test_generated_code_keeps_the_legend_for_several_colour_levels(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)

    assert "hue='session'" in source
    assert "legend=False" not in source


def test_generated_code_drops_the_legend_for_one_colour_level(
    scalar_table, scalar_frame
):
    """Same rule as the renderers: one level, no legend — preview and export agree."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
        filters=[Filter(column="session", include=["pre"])],
    )
    source = generate_plot_function(spec, scalar_table)

    assert "legend=False" in source
    figure = _run(source, scalar_frame, "plot_steplength")
    assert figure.legends == [] and all(ax.get_legend() is None for ax in figure.axes)
    matplotlib.pyplot.close(figure)


def test_iterate_factors_are_documented_as_foreach_keys(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)
    assert "One figure per subject" in source
    assert "for_each iteration keys" in source


def test_a_spec_with_no_x_factor_builds_its_own_x_column(scalar_table, scalar_frame):
    """Regression: the x fallback named a column the endpoint does not have.

    `reduce._panel_frame` puts every point at ONE categorical position when no
    grouping layer is a tick. Codegen used to fall back to `table.factor_names[0]`
    instead — a different figure from the preview, and a hard failure because
    that first factor is an iteration key here and so is NOT a column of the
    frame the function receives.
    """
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.ITERATE, "session": Role.ITERATE, "trial": Role.ITERATE},
    )
    source = generate_plot_function(spec, scalar_table)

    assert "df['Observation'] = \"\"" in source
    assert "x='Observation'" in source

    # The frame an endpoint would receive: iterated keys are for_each keywords.
    one_location = scalar_frame[
        (scalar_frame["subject"] == "01")
        & (scalar_frame["session"] == "pre")
        & (scalar_frame["trial"] == "1")
    ].drop(columns=["subject", "session", "trial"])
    figure = _run(source, one_location, "plot_steplength")
    assert figure.axes
    matplotlib.pyplot.close(figure)


def test_embedded_spec_round_trips_out_of_generated_source(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)
    assert extract_spec(source) == spec


def test_extract_spec_returns_none_for_handwritten_code():
    assert extract_spec("def plot_x(df, filename):\n    return None\n") is None


def test_generated_script_is_runnable_source(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    script = generate_script(spec, scalar_table)

    compile(script, "<generated>", "exec")  # syntax must be valid
    assert 'if __name__ == "__main__":' in script


def test_generated_heatmap_runs():
    import numpy as np
    import pandas as pd

    from scistackplot import LongTable

    frame = pd.DataFrame(
        {"subject": ["01", "02"], "Map": [np.zeros((4, 5)), np.ones((4, 5))]}
    )
    table = LongTable.from_frame(frame, factors=["subject"], measures=["Map"])
    spec = PlotSpec(
        measures=["Map"], roles={"subject": Role.COLLAPSE}, kind=PlotKind.HEATMAP
    )

    figure = _run(generate_plot_function(spec, table), frame, "plot_map")
    assert figure.axes
    matplotlib.pyplot.close(figure)


def test_generated_code_melts_struct_fields(struct_table):
    """
    The endpoint receives one column per field, so the generated function has
    to melt exactly as ScidbSource.get_table does — otherwise the exported
    figure is not the previewed figure.
    """
    from scistackplot import default_spec

    spec = default_spec(struct_table, "RawEMG")
    source = generate_plot_function(spec, struct_table)

    assert "df.melt(" in source
    assert "'RHAM'" in source
    assert "var_name='ColName'" in source
    assert "col='ColName'" in source or 'col="ColName"' in source


def test_a_ruled_column_layout_is_exported_as_col_order(bilateral_table):
    """
    A one-axis rule layout IS expressible in seaborn — col_wrap plus col_order —
    so the exported figure must reproduce the arrangement rather than fall back
    to source order and warn about it.
    """
    from scistackplot import FacetOptions, PlotKind, PlotSpec, Role
    from scistackplot.spec import MatchOp, Matcher

    spec = PlotSpec(
        measures=["RawEMG"],
        roles={"ColName": Role.FACET, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        color="subject",
        kind=PlotKind.BAND,
        facet=FacetOptions(
            n_cols=2,
            cols=[
                Matcher(op=MatchOp.CONTAINS, value="QUAD"),
                Matcher(op=MatchOp.CONTAINS, value="HAM"),
            ],
        ),
    )
    source = generate_plot_function(spec, bilateral_table)

    assert "col_wrap=2" in source
    assert "col_order=" in source
    # QUAD first, because that is the column the rules put it in.
    order = source[source.index("col_order=") :]
    assert order.index("QUAD") < order.index("HAM")
    assert "seaborn cannot express" not in source


def test_facet_names_are_exported_as_y_labels_not_titles(
    scalar_table, scalar_frame
):
    """
    Export parity for the facet naming rule (render.base.panel_y_title).

    seaborn captions every facet by default, so the preview's rule — the facet
    values ARE the y-axis title, and nothing sits above the panel — has to be
    said in the generated code too. Run, not just grepped: the ylabel loop reads
    ``axes_dict``, and a snippet that raises is worse than no export at all.
    """
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.FACET, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)
    assert 'g.set_titles("")' in source

    figure = _run(source, scalar_frame, "plot_steplength")
    panels = [ax for ax in figure.axes if ax.get_visible()]
    assert panels
    assert not any(ax.get_title() for ax in panels)
    assert {ax.get_ylabel() for ax in panels} == {
        str(level) for level in scalar_table.factor("session").levels
    }
    matplotlib.pyplot.close(figure)


def test_an_unfaceted_export_keeps_the_measure_on_the_y_axis(scalar_table):
    """No facets, nothing to rename the axis after — and no titles to suppress."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)

    assert "set_titles" not in source
    assert "'StepLength'" in source.split("set_axis_labels")[1].split("\n")[0]


def test_a_layout_with_holes_says_seaborn_cannot_express_it(struct_table):
    """
    Rules that leave an empty cell have no col_wrap equivalent — seaborn would
    close the gap and shift every later panel. Say so instead of exporting a
    figure that quietly differs from the preview.
    """
    from scistackplot import FacetOptions, PlotKind, PlotSpec, Role
    from scistackplot.spec import MatchOp, Matcher

    spec = PlotSpec(
        measures=["RawEMG"],
        roles={"ColName": Role.FACET, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        color="subject",
        kind=PlotKind.BAND,
        facet=FacetOptions(
            # RHAM (0,0), RTA (0,1), LMG (1,2) — cell (1,0) stays empty.
            rows=[
                Matcher(op=MatchOp.STARTS_WITH, value="R"),
                Matcher(op=MatchOp.STARTS_WITH, value="L"),
            ],
            cols=[
                Matcher(op=MatchOp.ENDS_WITH, value="HAM"),
                Matcher(op=MatchOp.ENDS_WITH, value="TA"),
                Matcher(op=MatchOp.ENDS_WITH, value="MG"),
            ],
        ),
    )
    source = generate_plot_function(spec, struct_table)
    assert "seaborn cannot express" in source
    assert "col_order=" not in source


# --- 1-D collapsed to a scalar ----------------------------------------------
#
# The reshape exists twice — once interactively, once in generated code — so
# these tests run BOTH and compare the numbers, not the text. Asserting only on
# the source would pass while the exported figure differed from the preview,
# which is the failure this whole family of tests exists to catch.


def _collapsed_spec(**kwargs) -> PlotSpec:
    return PlotSpec(
        measures=["Signal"],
        kind=PlotKind.VIOLIN,
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        **kwargs,
    )


def test_generated_violin_of_a_1d_measure_runs(series_table, series_frame):
    source = generate_plot_function(_collapsed_spec(), series_table)

    assert "import numpy as np" in source
    assert "def _collapse_signal(_cell):" in source
    assert ".explode(" not in source, "the samples are summarized, not exploded"
    assert "sns.catplot" in source

    figure = _run(source, series_frame, "plot_signal")
    assert figure.axes
    matplotlib.pyplot.close(figure)


def test_the_generated_collapse_matches_the_preview(series_table, series_frame):
    """The numbers the ENDPOINT plots must be the numbers the panel showed.

    Read off the rendered figure rather than off the source, because the
    failure this guards against — the export summarizing differently from the
    preview — produces perfectly valid-looking code.
    """
    from scistackplot import resolve
    from scistackplot.resolved import Y

    # A scatter of a 1-D measure: a scalar kind, so it collapses, and seaborn's
    # strip marks carry the plotted values where a violin's outline does not.
    spec = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.SCATTER,
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session", "subject"],
    )

    figure = _run(generate_plot_function(spec, series_table), series_frame, "plot_signal")
    exported = sorted(
        float(y)
        for axis in figure.axes
        for collection in axis.collections
        for _x, y in collection.get_offsets()
    )
    matplotlib.pyplot.close(figure)

    (preview,) = resolve(spec, series_table)
    drawn = sorted(
        value for panel in preview.panels for value in panel.frame[Y].tolist()
    )

    assert exported == pytest.approx(drawn)
    assert len(drawn) == len(series_frame), "one point per record, not per sample"


def test_the_generated_median_collapse_uses_the_median(series_table, series_frame):
    source = generate_plot_function(
        _collapsed_spec(cell_statistic=Statistic.MEDIAN), series_table
    )
    assert "np.median(_samples)" in source
    figure = _run(source, series_frame, "plot_signal")
    matplotlib.pyplot.close(figure)


def test_the_docstring_says_the_points_are_summaries(series_table):
    source = generate_plot_function(_collapsed_spec(), series_table)
    assert "reduced to its mean" in source


def test_the_embedded_spec_round_trips_with_the_statistic(series_table):
    spec = _collapsed_spec(cell_statistic=Statistic.MEDIAN)
    recovered = extract_spec(generate_plot_function(spec, series_table))
    assert recovered.cell_statistic is Statistic.MEDIAN
    assert recovered.kind is PlotKind.VIOLIN


def test_a_line_of_the_same_measure_still_explodes(series_table, series_frame):
    """The collapse is implied by the KIND: a 1-D kind must be untouched."""
    spec = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.LINE,
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session", "subject"],
        color="subject",
    )
    source = generate_plot_function(spec, series_table)
    assert ".explode(" in source
    assert "_collapse_signal" not in source


# --- the collapse chain, exported ------------------------------------------
#
# The endpoint receives the raw frame, so the chain has to be EMITTED — one
# groupby-mean per collapsed key, deepest first — and the sample left to
# seaborn's estimator/errorbar, which then compute what the preview's
# `_summarize` did. Checked on the unbalanced fixture from
# test_nested_collapse, where nested and pooled provably differ.


@pytest.fixture
def unbalanced():
    import pandas as pd

    from scistackplot import LongTable

    rows = [
        ("01", "pre", "1", 1.0), ("01", "pre", "2", 2.0), ("01", "pre", "3", 3.0),
        ("02", "pre", "1", 9.0),
        ("01", "post", "1", 10.0), ("01", "post", "2", 20.0), ("02", "post", "1", 30.0),
    ]
    frame = pd.DataFrame(rows, columns=["subject", "session", "trial", "M"])
    return frame, LongTable.from_frame(
        frame, factors=["subject", "session", "trial"], measures=["M"], name="M",
        schema_levels=["subject", "session", "trial"],
    )


def _bar_spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def test_the_chain_is_emitted_deepest_first_and_the_sample_is_left(unbalanced):
    _, table = unbalanced
    source = generate_plot_function(_bar_spec(), table)
    assert "# collapse trial within subject, session" in source
    assert "# the sample: subject" in source
    assert source.count("groupby(") == 1, "the sample is seaborn's, not a second groupby"
    assert 'estimator="mean"' in source and "errorbar=" in source
    assert "Collapsed trial -> subject" in source, "the docstring names the chain"
    assert "error bars: sd across subject" in source


def test_exported_bar_heights_are_the_nested_means(unbalanced):
    frame, table = unbalanced
    from scistackplot import resolve
    from scistackplot.resolved import X, Y

    figure = _run(generate_plot_function(_bar_spec(), table), frame, "plot_m")
    heights = sorted(
        round(p.get_height(), 9) for ax in figure.axes for p in ax.patches if p.get_height()
    )
    matplotlib.pyplot.close(figure)

    (preview,) = resolve(_bar_spec(), table)
    drawn = preview.panels[0].frame
    assert heights == pytest.approx(sorted(drawn[Y].tolist()))
    assert drawn.set_index(X)[Y]["pre"] == pytest.approx(5.5), "not the pooled 3.75"


def test_pooled_emits_no_chain(unbalanced):
    frame, table = unbalanced
    spec = _bar_spec(aggregate=Aggregation(error=ErrorBand.SD, pooled=True))
    source = generate_plot_function(spec, table)
    assert "# collapse" not in source
    assert "(pooled)" in source
    figure = _run(source, frame, "plot_m")
    heights = sorted(p.get_height() for ax in figure.axes for p in ax.patches if p.get_height())
    matplotlib.pyplot.close(figure)
    assert 3.75 in [round(h, 9) for h in heights], "mean of {1, 2, 3, 9}"


def test_a_mean_drawing_kind_collapses_the_sample_too(unbalanced):
    _, table = unbalanced
    source = generate_plot_function(_bar_spec(kind=PlotKind.SCATTER), table)
    assert "# collapse trial within subject, session — averaged away" in source
    assert "# collapse subject within session — the sample's mean" in source
    assert source.count("groupby(") == 2


def test_dashes_are_emitted_for_an_uncoloured_series_layer(series_table, series_frame):
    """An uncoloured grouping layer on a band is told apart by dash style in
    the preview (D4); the export says the same with seaborn's `style=` and
    `dashes=`, using the same cycle."""
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        color="session",
        kind=PlotKind.BAND,
    )
    source = generate_plot_function(spec, series_table)
    assert "df['_dash'] = df['subject'].astype(str)" in source
    assert "style='_dash'" in source and "dashes=_dashes" in source
    assert "df['_series'] = df['session'].astype(str).str.cat(df[['subject']]" in source, (
        "series ids compose outermost first, as reduce._series_key does"
    )
    figure = _run(source, series_frame, "plot_signal")
    matplotlib.pyplot.close(figure)


def test_no_dashes_without_an_uncoloured_layer(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["subject"],
        color="subject",
        kind=PlotKind.BAND,
    )
    source = generate_plot_function(spec, series_table)
    assert "_dash" not in source
