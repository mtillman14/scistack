"""What `get_table` says about a cache hit, a miss, and what a miss cost.

Why this exists: on 2026-09-13 a `plot_resolve` on a 419-record variable held the
database for **16 ms** and then never returned a figure. A hold that short has
only two explanations — the table was already cached, or the data is smaller than
it looks — and nothing in the log distinguished them, which made it impossible to
tell a caching question from a data-volume question
(.claude/plot-at-scale-plan.md §1).

So a hit says it is a hit, a miss says what it built, and both say it at INFO.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest

from scistackplot import FactorVariable
from scistackplot.sources.frame import DataFrameSource

LAYER = "scistackplot"


def _lines(caplog) -> list[str]:
    return [record.getMessage() for record in caplog.records]


@pytest.fixture
def source() -> DataFrameSource:
    """A 1-D measure with arrays in the cells — the shape that gets slow."""
    frame = pd.DataFrame(
        {
            "subject": ["01", "02", "03"],
            "Signal": [np.zeros(50), np.zeros(50), np.zeros(50)],
        }
    )
    return DataFrameSource(frame, factors=["subject"], measures=["Signal"])


def test_first_call_reports_a_miss_and_what_it_built(source, caplog):
    with caplog.at_level(logging.INFO, logger=LAYER):
        source.get_table(["Signal"])

    text = "\n".join(_lines(caplog))
    assert "table cache MISS" in text
    assert "[timing] get_table(Signal)" in text
    # Built-size line: rows AND samples, because rows alone cannot tell a frame
    # of short arrays from a frame of quarter-million-sample ones.
    built = next(l for l in _lines(caplog) if ": built " in l)
    assert "rows=3" in built
    assert "samples=150" in built


def test_second_call_reports_a_hit_and_builds_nothing(source, caplog):
    source.get_table(["Signal"])
    caplog.clear()

    with caplog.at_level(logging.INFO, logger=LAYER):
        source.get_table(["Signal"])

    text = "\n".join(_lines(caplog))
    assert "table cache HIT" in text
    assert "MISS" not in text
    # No build timing on a hit — that absence is the diagnostic.
    assert "[timing] get_table" not in text


def test_a_different_key_is_a_miss_not_a_hit(source, caplog):
    """The memo key must keep distinct requests distinct."""
    source.get_table(["Signal"])
    caplog.clear()

    with caplog.at_level(logging.INFO, logger=LAYER):
        source.get_table(["Signal"], factor_variables=[])
        first = "\n".join(_lines(caplog))
    assert "HIT" in first  # same effective key

    caplog.clear()
    with caplog.at_level(logging.INFO, logger=LAYER):
        source.get_table(
            ["Signal"],
            x_measure=None,
            factor_variables=[FactorVariable("subject")],
        )
    assert "MISS" in "\n".join(_lines(caplog))


def test_extent_measures_only_the_requested_measures(caplog):
    """A wide frame must not be reported by its unrelated columns."""
    frame = pd.DataFrame(
        {
            "subject": ["01"],
            "Signal": [np.zeros(10)],
            "Ignored": [np.zeros(9999)],
        }
    )
    source = DataFrameSource(
        frame, factors=["subject"], measures=["Signal", "Ignored"]
    )
    with caplog.at_level(logging.INFO, logger=LAYER):
        source.get_table(["Signal"])

    built = next(l for l in _lines(caplog) if ": built " in l)
    assert "samples=10" in built
