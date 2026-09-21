"""Resolution semantics — everything that happens before a renderer runs."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    ErrorBand,
    Filter,
    PlotKind,
    PlotSpec,
    Role,
    resolve,
)
from scistackplot.resolved import COLOR, SERIES, X, Y, Y_HIGH, Y_LOW
from scistackplot.spec import Aggregation
from scistackplot.xaxis import leaf_key


# --- fan-out ---------------------------------------------------------------


def test_iterate_fans_out_one_figure_per_level(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )
    figures = resolve(spec, scalar_table)

    assert len(figures) == 3
    assert [f.figure_key["subject"] for f in figures] == ["01", "02", "03"]
    assert all(f.row_count == 8 for f in figures)


def test_no_iterate_gives_exactly_one_figure(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    figures = resolve(spec, scalar_table)
    assert len(figures) == 1
    assert figures[0].row_count == 24


# --- ordering (the zero-padded trap) ---------------------------------------


def test_x_order_is_numeric_not_lexicographic(wide_subject_table):
    spec = PlotSpec(
        measures=["Mass"], roles={"subject": Role.GROUP}, kind=PlotKind.SCATTER
    )
    resolved = resolve(spec, wide_subject_table)[0]

    assert resolved.x_order == [f"{n:02d}" for n in range(1, 11)]
    # The bug this guards: lexicographic order puts "10" second.
    assert resolved.x_order[1] == "02"


def test_iterate_fanout_follows_declared_level_order(wide_subject_table):
    spec = PlotSpec(measures=["Mass"], roles={"subject": Role.ITERATE})
    figures = resolve(spec, wide_subject_table)
    assert [f.figure_key["subject"] for f in figures] == [
        f"{n:02d}" for n in range(1, 11)
    ]


# --- the collapse chain vs. summarizing ------------------------------------


def test_collapse_role_collapses_its_factor(scalar_table):
    """Two collapsed keys: trial averages away within each subject; subject —
    the last — is the sample, and the scatter draws one point per subject."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.SCATTER,
    )
    resolved = resolve(spec, scalar_table)[0]

    # 3 subjects x 2 sessions; the 4 trials were averaged away.
    assert resolved.row_count == 6


def test_a_scatter_draws_every_sample_row(scalar_table):
    """Schema-level parity (2026-09-19): a scatter draws the SAMPLE, as a box
    does — one point per trial here, since trial is the only collapsed key —
    never the sample's mean."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.SCATTER,
    )
    resolved = resolve(spec, scalar_table)[0]
    frame = resolved.panels[0].frame

    assert resolved.row_count == 24, "3 subjects x 2 sessions x 4 trials"
    expected = sorted(
        scalar_table.frame.query("subject == '01' and session == 'pre'")["StepLength"]
    )
    # Colour is paint: session is a tick layer inside subject, so the leaf is
    # composed and `__color` merely repeats its session part.
    got = sorted(frame[(frame[X] == leaf_key(("01", "pre"))) & (frame[COLOR] == "pre")][Y])
    assert got == pytest.approx(expected)
    assert len(got) == 4


def test_scatter_and_box_draw_the_same_rows(scalar_table):
    """The parity rule itself: every non-summarising kind draws the same
    sample rows for the same roles."""
    roles = {"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE}

    def drawn(kind):
        spec = PlotSpec(measures=["StepLength"], roles=roles, groups=["session"], kind=kind)
        frame = resolve(spec, scalar_table)[0].panels[0].frame
        return sorted(zip(frame[X], frame[Y].round(12)))

    assert drawn(PlotKind.SCATTER) == drawn(PlotKind.BOX) == drawn(PlotKind.STRIP)


# --- summarizing into a centre + error band --------------------------------


def test_band_summarizes_replicates_at_each_index(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        color="session",
        kind=PlotKind.BAND,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    resolved = resolve(spec, series_table)[0]
    frame = resolved.panels[0].frame

    # 10 samples x 2 sessions, each summarizing 3 subjects (trials averaged
    # within each subject first).
    assert len(frame) == 20
    assert {Y, Y_LOW, Y_HIGH} <= set(frame.columns)
    assert resolved.encoding.has_error


def test_band_error_is_the_standard_deviation(series_table):
    """Pooled, so the reference is the plain SD over every subject-trial
    trace; the nested default is checked in test_nested_collapse."""
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        color="session",
        kind=PlotKind.BAND,
        aggregate=Aggregation(error=ErrorBand.SD, pooled=True),
    )
    frame = resolve(spec, series_table)[0].panels[0].frame
    row = frame[(frame[X] == 0) & (frame[COLOR] == "pre")].iloc[0]

    raw = series_table.frame.query("session == 'pre'")["Signal"].map(lambda v: v[0])
    assert row[Y] == pytest.approx(raw.mean())
    assert row[Y_HIGH] - row[Y] == pytest.approx(raw.std(ddof=1))


def test_sem_is_narrower_than_sd(series_table):
    def spread(error):
        spec = PlotSpec(
            measures=["Signal"],
            roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
            color="session",
            kind=PlotKind.BAND,
            aggregate=Aggregation(error=error),
        )
        frame = resolve(spec, series_table)[0].panels[0].frame
        return (frame[Y_HIGH] - frame[Y_LOW]).mean()

    assert spread(ErrorBand.SEM) < spread(ErrorBand.SD)


def test_iqr_band_is_asymmetric_around_the_centre(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        color="session",
        kind=PlotKind.BAND,
        aggregate=Aggregation(error=ErrorBand.IQR),
    )
    frame = resolve(spec, series_table)[0].panels[0].frame
    assert (frame[Y_LOW] <= frame[Y]).all()
    assert (frame[Y_HIGH] >= frame[Y]).all()


# --- 1-D explosion ---------------------------------------------------------


def test_1d_measure_is_exploded_into_samples(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "subject", "session"],
        color="session",
        kind=PlotKind.LINE,
    )
    resolved = resolve(spec, series_table)[0]

    assert resolved.row_count == 24 * 10
    assert resolved.labels.x == "index"


def test_line_gets_one_series_per_observation(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "subject", "session"],
        color="session",
        kind=PlotKind.LINE,
    )
    frame = resolve(spec, series_table)[0].panels[0].frame
    assert frame[SERIES].nunique() == 24


# --- faceting --------------------------------------------------------------


def test_facet_produces_one_panel_per_level(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "subject": Role.GROUP,
            "session": Role.FACET,
            "trial": Role.COLLAPSE,
        },
        kind=PlotKind.BOX,
    )
    resolved = resolve(spec, scalar_table)[0]

    assert len(resolved.panels) == 2
    assert {p.title for p in resolved.panels} == {"pre", "post"}


def test_shared_y_limits_span_every_panel(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.FACET, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )
    resolved = resolve(spec, scalar_table)[0]

    low, high = resolved.y_limits
    assert low <= scalar_table.frame["StepLength"].min()
    assert high >= scalar_table.frame["StepLength"].max()


# --- filters ---------------------------------------------------------------


def test_include_filter_drops_other_levels(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "trial": Role.GROUP, "session": Role.GROUP},
        kind=PlotKind.SCATTER,
        filters=[Filter(column="session", include=["pre"])],
    )
    assert resolve(spec, scalar_table)[0].row_count == 12


def test_numeric_range_filter(scalar_table):
    threshold = scalar_table.frame["StepLength"].median()
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "trial": Role.GROUP, "session": Role.GROUP},
        kind=PlotKind.SCATTER,
        filters=[Filter(column="StepLength", minimum=float(threshold))],
    )
    assert resolve(spec, scalar_table)[0].row_count == 12


# --- variants --------------------------------------------------------------


def test_variant_on_colour_keeps_both_variants_separate(variant_table):
    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP, "bandpass.low_hz": Role.GROUP},
        color="bandpass.low_hz",
        kind=PlotKind.SCATTER,
    )
    resolved = resolve(spec, variant_table)[0]

    assert resolved.row_count == 6
    assert resolved.color_order == ["20", "40"]


def test_variants_cannot_be_collapsed(variant_table):
    """Two pipelines' results averaged into one number is not a figure anyone
    asked for; the old 'aggregate' opt-in is gone with the role."""
    from scistackplot import RoleError

    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP, "bandpass.low_hz": Role.COLLAPSE},
        kind=PlotKind.SCATTER,
    )
    with pytest.raises(RoleError, match="cannot be collapsed"):
        resolve(spec, variant_table)


def test_one_named_variant_keeps_only_its_rows(variant_table):
    from scistackplot.spec import VariantSet

    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP},
        kind=PlotKind.SCATTER,
        variant_sets=[VariantSet("20 Hz", {"bandpass.low_hz": "20"})],
    )
    resolved = resolve(spec, variant_table)[0]
    assert resolved.row_count == 3


def test_two_named_variants_become_one_coloured_factor(variant_table):
    """The comparison case: both variants in one figure, told apart by name
    rather than overplotted."""
    from scistackplot import VARIANT_FACTOR
    from scistackplot.spec import VariantSet

    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP},
        kind=PlotKind.SCATTER,
        variant_sets=[
            VariantSet("narrow", {"bandpass.low_hz": "20"}),
            VariantSet("wide", {"bandpass.low_hz": "40"}),
        ],
    )
    resolved = resolve(spec, variant_table)[0]

    assert resolved.row_count == 6
    assert resolved.color_order == ["narrow", "wide"]
    assert VARIANT_FACTOR not in spec.roles, (
        "the role was defaulted, not assigned — the spec stays the user's"
    )


# --- transport budget ------------------------------------------------------


def test_downsampling_records_the_original_size(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "subject", "session"],
        color="session",
        kind=PlotKind.LINE,
    )
    resolved = resolve(spec, series_table, max_points=50)[0]

    assert resolved.downsampled_from == 240
    assert resolved.row_count <= 60


def test_export_path_never_downsamples(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "subject", "session"],
        color="session",
        kind=PlotKind.LINE,
    )
    resolved = resolve(spec, series_table)[0]
    assert resolved.downsampled_from is None
    assert resolved.row_count == 240


# --- serialization ---------------------------------------------------------


def test_resolved_plot_is_json_serializable(scalar_table):
    import json

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    payload = resolve(spec, scalar_table)[0].to_dict()
    json.dumps(payload)  # must not raise on numpy scalars

    assert payload["kind"] == "box"
    # session is a tick layer inside subject (colour is paint), so the first
    # row's `__x` is a composed leaf, serialised as the plain string it is.
    assert payload["panels"][0]["rows"][0]["__x"] == leaf_key(("01", "pre"))


# --- struct/dict fields ----------------------------------------------------


def test_struct_fields_become_one_panel_each(struct_table):
    from scistackplot import default_spec

    spec = default_spec(struct_table, "RawEMG")
    resolved = resolve(spec, struct_table)[0]

    assert len(resolved.panels) == 3
    assert {p.title for p in resolved.panels} == {"RHAM", "RTA", "LMG"}


def test_struct_fields_can_be_moved_to_separate_figures(struct_table):
    spec = PlotSpec(
        measures=["RawEMG"],
        roles={"ColName": Role.ITERATE, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        color="subject",
        kind=PlotKind.BAND,
    )
    figures = resolve(spec, struct_table)

    assert len(figures) == 3
    assert all(len(f.panels) == 1 for f in figures)
