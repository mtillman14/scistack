"""What a resolve says about itself at INFO, without being asked to narrate.

`test_narration.py` covers the opt-in `narrate=True` path. This file covers the
DEFAULT path, which is the one that failed: on 2026-09-13 a `plot_resolve`
cleared its entire DuckDB phase in 16 ms, never reached the render, and was
abandoned by the client 30 s later having logged nothing at all between those two
points. Every candidate step logged at DEBUG, and the file sink runs at INFO
(.claude/plot-at-scale-plan.md §1).

Asserting on log text is deliberate here, for the same reason the narration tests
do it: the text is the diagnostic interface, and a phase name that is silently
renamed is exactly the regression worth catching.
"""

from __future__ import annotations

import logging

import pytest

from scistackplot import Filter, PlotKind, PlotSpec, Role, resolve

LAYER = "scistackplot"

#: Every phase `_build_plan` reports. A phase dropping off this list has stopped
#: being measurable, and `y_limits` in particular is load-bearing: it is the one
#: step that deliberately spans figures `resolve_one` never builds, so it is the
#: prime suspect whenever a fan-out is slower than the figure on screen.
PLAN_PHASES = [
    "variant_sets",
    "level_groups",
    "roles",
    "apply_filters",
    "group_fanout",
    "y_limits",
]


def _lines(caplog) -> list[str]:
    return [record.getMessage() for record in caplog.records]


@pytest.fixture
def fanout_spec() -> PlotSpec:
    return PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.ITERATE, "session": Role.COLOR, "trial": Role.FREE},
        kind=PlotKind.LINE,
    )


def test_plan_phases_reported_without_narration(series_table, fanout_spec, caplog):
    """The default path must break `build_plan` down, not just total it."""
    with caplog.at_level(logging.DEBUG, logger=LAYER):
        resolve(fanout_spec, series_table)

    text = "\n".join(_lines(caplog))
    assert "[timing] build_plan" in text
    for phase in PLAN_PHASES:
        assert f"{phase}=" in text, f"phase {phase!r} is not in the build_plan summary"


def test_build_plan_total_covers_its_phases(series_table, fanout_spec, caplog):
    """A phase timed outside the TOTAL window would misreport where time went."""
    import re

    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(fanout_spec, series_table)

    summary = next(l for l in _lines(caplog) if "[timing] build_plan" in l)
    total = float(re.search(r"TOTAL=([\d.]+)s", summary).group(1))
    parts = [float(v) for v in re.findall(r"=(\d+\.\d+)s(?=[,)])", summary)]
    # TOTAL itself is among the matches; drop the largest to avoid counting it.
    parts.remove(max(parts))
    assert total >= sum(parts) - 1e-6


def test_post_filter_extent_is_reported_at_info(series_table, fanout_spec, caplog):
    """How much data survived the filter, in samples — not just rows.

    This is the first number anyone needs when a resolve does not come back, and
    it used to be DEBUG-only and row-only.
    """
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(fanout_spec, series_table)

    line = next(l for l in _lines(caplog) if l.startswith("resolve: measure="))
    assert "post-filter" in line
    assert "rows=" in line
    assert "cells=" in line
    assert "samples=" in line


def test_filtering_to_one_location_shows_in_the_extent(series_table, caplog):
    """The line has to reflect the FILTERED frame, not the loaded one.

    Otherwise it cannot answer the question it exists for: "is this slow because
    the filter did not narrow anything?"
    """
    everything = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.COLOR, "session": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.LINE,
    )
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(everything, series_table)
    wide = next(l for l in _lines(caplog) if l.startswith("resolve: measure="))
    caplog.clear()

    one = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.COLOR, "session": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.LINE,
        filters=[Filter("subject", include=["01"])],
    )
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(one, series_table)
    narrow = next(l for l in _lines(caplog) if l.startswith("resolve: measure="))

    def rows(line: str) -> int:
        import re

        return int(re.search(r"rows=(\d+)", line).group(1))

    assert rows(narrow) < rows(wide)
