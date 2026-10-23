"""Datetimes from a scidb database into plots (plan .claude/plan-plot-datetimes.md).

Stage 0: ``load_variable`` used to drop the ``<col>__tz`` zone column, so every
tz-aware value reached a figure as a naive UTC instant. It now folds the zone
back (``sciduckdb.merge_tz_columns``), and the table sees each value in its
own zone: a temporal measure, or a datetime factor with identity labels.
"""

from __future__ import annotations

import pandas as pd
import pytest
from scidb import BaseVariable
from scistackplot import FactorVariable, PlotKind, PlotSpec, Role, resolve

from scistackplotdb import ScidbSource
from scistackplotdb.load import load_variable


class SessionStart(BaseVariable):
    """When each session started (session level), in the site's zone."""


class Enrolled(BaseVariable):
    """When each subject enrolled (subject level)."""


class Visit(BaseVariable):
    """A wide per-session record: a score and when it was taken."""


STARTS = {
    ("01", "pre"): pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin"),
    ("01", "post"): pd.Timestamp("2026-07-08 14:30", tz="Europe/Berlin"),
    ("02", "pre"): pd.Timestamp("2026-07-01 08:00", tz="America/New_York"),
    ("02", "post"): pd.Timestamp("2026-07-08 09:00", tz="America/New_York"),
    ("03", "pre"): pd.Timestamp("2026-07-01 12:00"),
    ("03", "post"): pd.NaT,
}


@pytest.fixture
def starts(seeded):
    for (subject, session), value in STARTS.items():
        SessionStart.save(value, subject=subject, session=session)
    return seeded


def test_load_variable_keeps_each_zone(starts):
    loaded = load_variable(starts, "SessionStart", with_variants=False)
    (column,) = loaded.data_columns
    frame = loaded.frame.set_index(["subject", "session"])
    berlin = frame.loc[("01", "pre"), column]
    assert berlin == STARTS[("01", "pre")]
    assert str(berlin.tzinfo) == "Europe/Berlin"
    assert str(frame.loc[("02", "pre"), column].tzinfo) == "America/New_York"
    assert frame.loc[("03", "pre"), column].tzinfo is None
    assert pd.isna(frame.loc[("03", "post"), column])
    assert not any(c.endswith("__tz") for c in loaded.frame.columns)


def test_a_datetime_variable_is_a_temporal_measure(starts):
    table = ScidbSource(starts).get_table(["SessionStart"])
    assert table.measure("SessionStart").temporal
    spec = PlotSpec(
        measures=["SessionStart"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.STRIP,
    )
    (figure,) = resolve(spec, table)
    assert figure.temporal is not None
    assert figure.labels.y.endswith("(local time)")


def test_a_joined_datetime_grouping(seeded):
    for subject, when in (
        ("01", pd.Timestamp("2026-06-01 09:00", tz="Europe/Berlin")),
        ("02", pd.Timestamp("2026-06-02 09:00", tz="America/New_York")),
    ):
        Enrolled.save(when, subject=subject)
    table = ScidbSource(seeded).get_table(
        ["StepLength"], factor_variables=[FactorVariable("Enrolled")]
    )
    info = table.factor("Enrolled")
    assert info.temporal
    # Subject 03 has no enrolment: named, never dropped, and last.
    assert info.levels == [
        "2026-06-01 09:00 CEST",
        "2026-06-02 09:00 EDT",
        "(missing)",
    ]
    assert len(table.frame) == 12


def test_a_datetime_column_of_the_plotted_variable(seeded):
    for subject in ("01", "02"):
        for session, day in (("pre", 1), ("post", 8)):
            Visit.save(
                {
                    "Score": float(day + int(subject)),
                    "At": pd.Timestamp(f"2026-07-{day:02d} 10:00", tz="Europe/Berlin"),
                },
                subject=subject,
                session=session,
            )
    table = ScidbSource(seeded).get_table(
        ["Visit"], factor_variables=[FactorVariable("Visit", "At")]
    )
    info = table.factor("At")
    assert info.temporal
    assert info.levels == ["2026-07-01 10:00 CEST", "2026-07-08 10:00 CEST"]
