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
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.FREE},
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
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)

    assert "sns.catplot" in source
    assert "scistackplot.render" not in source
    assert "import scistackplot" not in source


def test_generated_band_plot_runs(series_table, series_frame):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.COLOR, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BAND,
        aggregate=Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD),
    )
    source = generate_plot_function(spec, series_table)

    assert "df.explode" in source
    assert 'errorbar="sd"' in source
    figure = _run(source, series_frame, "plot_signal")
    assert figure.axes
    matplotlib.pyplot.close(figure)


def test_generated_aggregate_emits_a_groupby(scalar_table, scalar_frame):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.AGGREGATE},
        kind=PlotKind.SCATTER,
    )
    source = generate_plot_function(spec, scalar_table)

    assert "# average over trial" in source
    assert ".groupby(" in source
    figure = _run(source, scalar_frame, "plot_steplength")
    matplotlib.pyplot.close(figure)


def test_generated_filters_are_applied(scalar_table, scalar_frame):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X, "trial": Role.FREE, "session": Role.FREE},
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
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.FREE},
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
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.FREE},
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
        roles={"subject": Role.ITERATE, "session": Role.X, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)
    assert "One figure per subject" in source
    assert "for_each iteration keys" in source


def test_a_spec_with_no_x_factor_builds_its_own_x_column(scalar_table, scalar_frame):
    """Regression: the x fallback named a column the endpoint does not have.

    `reduce._panel_frame` puts every point at ONE categorical position when no
    factor holds Role.X. Codegen used to fall back to `table.factor_names[0]`
    instead — a different figure from the preview, and a hard failure once a
    nested ITERATE key promotes its ancestors, because that first factor is then
    an iteration key and so is NOT a column of the frame the function receives.
    """
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.ITERATE})
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
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )
    source = generate_plot_function(spec, scalar_table)
    assert extract_spec(source) == spec


def test_extract_spec_returns_none_for_handwritten_code():
    assert extract_spec("def plot_x(df, filename):\n    return None\n") is None


def test_generated_script_is_runnable_source(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.FREE},
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
        measures=["Map"], roles={"subject": Role.FREE}, kind=PlotKind.HEATMAP
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
        roles={"ColName": Role.FACET, "subject": Role.COLOR, "trial": Role.FREE},
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
        roles={"ColName": Role.FACET, "subject": Role.COLOR, "trial": Role.FREE},
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
