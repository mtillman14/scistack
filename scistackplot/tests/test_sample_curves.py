"""sample_curves: the 1-D rows an SPM test sees are the rows a band draws."""

from __future__ import annotations

import numpy as np
import pytest

from scistackplot import PlotKind, PlotSpec, Role, resolve
from scistackplot.export import sample_curves
from scistackplot.resolved import COLOR, Y


def _band() -> PlotSpec:
    return PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["session"],
        color="session",
        kind=PlotKind.BAND,
    )


def test_one_curve_per_subject_per_session(series_table, series_frame):
    curves = sample_curves(_band(), series_table)
    assert len(curves) == series_frame[["subject", "session"]].drop_duplicates().shape[0]
    assert "trial" not in curves.columns, "trials averaged within subject first"
    first = curves.iloc[0]
    expected = np.mean(np.stack(series_frame[(series_frame.subject == first.subject)
                                             & (series_frame.session == first.session)]["Signal"].map(np.asarray)), axis=0)
    assert np.asarray(first["Signal"]) == pytest.approx(expected)


def test_the_band_centre_is_the_mean_of_the_curves(series_table):
    curves = sample_curves(_band(), series_table)
    (resolved,) = resolve(_band(), series_table)
    drawn = resolved.panels[0].frame
    for session, part in curves.groupby("session"):
        centre = np.mean(np.stack(part["Signal"].map(np.asarray)), axis=0)
        band = drawn[drawn[COLOR].astype(str) == str(session)][Y].to_numpy(float)
        assert band == pytest.approx(centre)


def test_a_scalar_kind_has_no_curves(series_table):
    from dataclasses import replace

    with pytest.raises(ValueError, match="1-D"):
        sample_curves(replace(_band(), kind=PlotKind.BAR), series_table)
