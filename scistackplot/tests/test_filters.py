"""
Row filters: the schema-key picker's storage, and what the panel reports back.

``PlotSpec.filters`` predates its GUI by some months; these pin the behaviour the
pickers depend on. The load-bearing one is the last group — the "3 of 12
selected" readout is measured with the same :func:`apply_filters` the figure
uses, because a count the figure disagrees with is worse than no count.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, capabilities, resolve
from scistackplot.reduce import apply_filters
from scistackplot.spec import Filter


def _rows(figure) -> int:
    return sum(len(panel.frame) for panel in figure.panels)


# --- the mask --------------------------------------------------------------


def test_include_keeps_only_named_levels(scalar_table):
    spec = PlotSpec(measures=["StepLength"], filters=[Filter("subject", include=["01"])])

    kept = apply_filters(scalar_table.frame, spec)

    assert set(kept["subject"]) == {"01"}
    assert len(kept) == 8  # 2 sessions x 4 trials


def test_exclude_drops_named_levels(scalar_table):
    spec = PlotSpec(measures=["StepLength"], filters=[Filter("session", exclude=["pre"])])

    kept = apply_filters(scalar_table.frame, spec)

    assert set(kept["session"]) == {"post"}


def test_range_bounds_a_measure(scalar_table):
    frame = scalar_table.frame
    median = float(frame["StepLength"].median())
    spec = PlotSpec(measures=["StepLength"], filters=[Filter("StepLength", minimum=median)])

    kept = apply_filters(frame, spec)

    assert kept["StepLength"].min() >= median
    assert len(kept) < len(frame)


def test_filters_combine(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        filters=[
            Filter("subject", include=["01", "02"]),
            Filter("session", include=["post"]),
        ],
    )

    kept = apply_filters(scalar_table.frame, spec)

    assert set(kept["subject"]) == {"01", "02"}
    assert set(kept["session"]) == {"post"}


def test_levels_match_as_text(scalar_frame):
    """A selection crosses JSON as strings; the column may hold ints.

    Zero-padded keys are this project's standing trap, and a picker the user
    just clicked producing an empty figure is the worst possible answer.
    """
    frame = scalar_frame.assign(trial=scalar_frame["trial"].astype(int))
    table = LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["StepLength"],
        schema_levels=["subject", "session", "trial"],
    )
    spec = PlotSpec(measures=["StepLength"], filters=[Filter("trial", include=["1"])])

    assert len(apply_filters(table.frame, spec)) == 6  # 3 subjects x 2 sessions


# --- degrading, not crashing ------------------------------------------------


def test_a_filter_on_an_unknown_column_is_ignored(scalar_table):
    """A spec outlives the table it was written against."""
    spec = PlotSpec(measures=["StepLength"], filters=[Filter("gone", include=["x"])])

    assert len(apply_filters(scalar_table.frame, spec)) == len(scalar_table.frame)


def test_a_filter_that_keeps_nothing_yields_an_empty_figure(scalar_table):
    """Deselecting everything is a legitimate state to be in while clicking."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
        filters=[Filter("subject", include=[])],
    )

    figures = resolve(spec, scalar_table)

    assert len(figures) == 1
    assert _rows(figures[0]) == 0


# --- what the panel is told -------------------------------------------------


def test_capabilities_report_which_levels_survive(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.X},
        filters=[Filter("subject", include=["01", "03"])],
    )

    factors = {f["name"]: f for f in capabilities(spec, scalar_table)["factors"]}

    assert factors["subject"]["levels"] == ["01", "02", "03"]
    assert factors["subject"]["selected"] == ["01", "03"]
    # An unfiltered factor reports everything, not nothing.
    assert factors["session"]["selected"] == factors["session"]["levels"]


def test_the_readout_matches_the_figure(scalar_table):
    """The panel's count and the rendered figure come from one function."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X, "session": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
        filters=[Filter("subject", include=["02"])],
    )

    reported = {
        f["name"]: f["selected"] for f in capabilities(spec, scalar_table)["factors"]
    }
    drawn = resolve(spec, scalar_table)[0]
    plotted = {str(value) for value in drawn.panels[0].frame["__x"]}

    assert reported["subject"] == ["02"]
    assert plotted == {"02"}


def test_with_no_filters_everything_reads_as_selected(scalar_table):
    spec = PlotSpec(measures=["StepLength"], roles={"session": Role.X})

    for factor in capabilities(spec, scalar_table)["factors"]:
        assert factor["selected"] == factor["levels"]


def test_schema_keys_are_identified_for_the_panel(scalar_table, struct_table):
    """The picker must not work this out by intersecting two lists."""
    described = {f["name"]: f for f in scalar_table.describe()["factors"]}
    assert [n for n, f in described.items() if f["is_schema_key"]] == [
        "subject",
        "session",
        "trial",
    ]

    fields = {f["name"]: f for f in struct_table.describe()["factors"]}
    assert fields["ColName"]["is_schema_key"] is False


def test_a_source_with_no_hierarchy_claims_no_schema_keys(scalar_frame):
    """A CSV has no schema; its columns are factors, not keys."""
    table = LongTable.from_frame(
        scalar_frame, factors=["subject", "session"], measures=["StepLength"]
    )

    assert all(
        f["is_schema_key"] is False for f in table.describe()["factors"]
    )
