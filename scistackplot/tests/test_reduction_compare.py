"""Compare-to-reference is a reduction step, applied by ``reduce`` and
printed by codegen (P5 stage 6, design review 06 P2).

``compare.ComparisonPlan`` is the step: ``compare.compare_sample`` applies it
and ``export.reduction_code.emit_compare`` prints it, keyed by the plan's one
key rule (``ComparisonPlan.keys_among``). The emitted pandas is executed on
the sample rows ``compare_sample`` compares and must give the same frame
(the standing "export equals preview" rule).

Regression guarded: the export matched the reference level with
``pd.isna`` (so ``NaT`` and ``pd.NA`` read ``"nan"``) while the preview spells
levels with ``panels.panel_key_text`` (only None and float NaN read
``"nan"``): a reference of ``"nan"`` picked different rows in the export.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import PlotKind, PlotSpec, Role, resolve
from scistackplot.export.reduction_code import emit_compare
from scistackplot.reduce.compare import Comparison, CompareMode, ComparisonPlan, compare_sample


def _plan(mode=CompareMode.DIFFERENCE, *, paired=True, statistic="mean", level="pre"):
    return ComparisonPlan(
        layer="session",
        level=level,
        mode=mode,
        paired=paired,
        reason="test",
        factors=("subject", "session"),
        sample=("subject",),
        statistic=statistic,
    )


def _scalar() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "subject": ["01", "01", "02", "02", "03"],
            "session": ["pre", "post", "pre", "post", "post"],
            "y": [2.0, 3.0, -1.0, 4.0, 5.0],
        }
    )


def _run(lines: list[str], frame: pd.DataFrame) -> pd.DataFrame:
    namespace: dict = {"df": frame.copy(), "pd": pd, "np": np}
    exec("\n".join(lines), namespace)  # noqa: S102
    return namespace["df"]


CASES = {
    "paired-difference": _plan(),
    "paired-percent": _plan(CompareMode.PERCENT),
    "summary-mean": _plan(paired=False),
    "summary-median-percent": _plan(CompareMode.PERCENT, paired=False, statistic="median"),
}


@pytest.mark.parametrize("name", list(CASES))
def test_emitted_compare_matches_the_preview(name):
    plan = CASES[name]
    expected, _base, _outcome = compare_sample(_scalar(), plan, "y", quiet=True)
    got = _run(emit_compare(plan, "y", ["subject", "session"]), _scalar())
    pd.testing.assert_frame_equal(got, expected, check_dtype=False)


def test_a_missing_level_is_spelled_like_the_preview():
    frame = pd.DataFrame(
        {
            "subject": ["01", "01", "02", "02"],
            "session": [None, "post", None, "post"],
            "y": [1.0, 4.0, 2.0, 7.0],
        }
    )
    plan = _plan(level="nan")
    expected, _base, _outcome = compare_sample(frame, plan, "y", quiet=True)
    got = _run(emit_compare(plan, "y", ["subject", "session"]), frame)
    pd.testing.assert_frame_equal(got, expected, check_dtype=False)
    assert got["y"].tolist() == [0.0, 3.0, 0.0, 5.0]


def _series() -> pd.DataFrame:
    rows = []
    for subject, session, values in [
        ("01", "pre", [1.0, 2.0]),
        ("01", "post", [3.0, 5.0]),
        ("02", "pre", [1.0, 1.0]),
        ("02", "post", [2.0, 2.0, 9.0]),  # one position more than its reference
    ]:
        for t, value in enumerate(values):
            rows.append({"subject": subject, "session": session, "t": t, "y": value})
    return pd.DataFrame(rows)


def test_emitted_1d_compare_drops_a_ragged_series_like_the_preview():
    plan = _plan()
    expected, _base, _outcome = compare_sample(_series(), plan, "y", "t", quiet=True)
    got = _run(emit_compare(plan, "y", ["subject", "session"], index_column="t"), _series())
    pd.testing.assert_frame_equal(got, expected, check_dtype=False)
    ragged = (got["subject"] == "02") & (got["session"] == "post")
    assert not bool(ragged.any()) and len(got) == 6


def test_the_key_rule_is_one_rule():
    plan = _plan(paired=False)
    frame = _scalar().assign(t=0)
    assert plan.key_columns(frame, "t") == plan.keys_among(list(frame.columns), "t")


def test_the_figure_records_its_comparison(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BAR,
        comparison=Comparison(layer="session", level="pre"),
    )
    figure = resolve(spec, scalar_table)[0]
    plans = figure.reduction.of_type(ComparisonPlan)
    assert len(plans) == 1 and plans[0].layer == "session"
