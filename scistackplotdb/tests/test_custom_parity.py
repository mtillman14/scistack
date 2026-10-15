"""
Custom plot kinds: Plot Studio calls a plot_ function the way for_each does.

Decision D1 (.claude/plan-custom-plot-kinds.md): Plot Studio builds the call's
arguments from its own data path (ScidbSource -> LongTable), not by calling
for_each. That is only right if the function receives the same data either
way, so this runs BOTH against one database and compares, per figure, what
the function was handed: the iterated key selects the call, a coarse input is
broadcast, and as_table passes each input as its own table.
"""

from __future__ import annotations

import pandas as pd
import pytest
from scistackplot import CustomCall, CustomPlot, PlotKind, PlotSpec, Role, resolve_custom

from scistackplotdb import ScidbSource

matplotlib = pytest.importorskip("matplotlib")

CAPTURED: list[dict] = []


def plot_parity(steps, mass, filename, **metadata):
    """Figure-level: records its arguments and returns an empty figure."""
    import matplotlib.pyplot as plt

    CAPTURED.append({"steps": steps, "mass": mass})
    return plt.figure()


def _content(frame: pd.DataFrame, columns: list[str]) -> list[tuple]:
    """The rows on ``columns``, as text, in a stable order: what both paths
    must agree on, whatever bookkeeping columns either one adds."""
    return sorted(
        tuple(str(v) for v in row) for row in frame[columns].itertuples(index=False)
    )


def _pipeline(tmp_path) -> dict[str, dict]:
    from scidb import PathOutput, for_each

    from conftest import Mass, StepLength, StepLengthFigure

    CAPTURED.clear()
    for_each(
        plot_parity,
        inputs={
            "steps": StepLength,
            "mass": Mass,
            "filename": PathOutput(str(tmp_path / "parity_{subject}.png")),
        },
        outputs=[StepLengthFigure],
        as_table=True,
        finalized=False,
        subject=[],
    )
    by_subject = {}
    for call in CAPTURED:
        subjects = set(call["steps"]["subject"].astype(str))
        assert len(subjects) == 1, subjects
        by_subject[subjects.pop()] = call
    return by_subject


def _studio(seeded) -> dict[str, dict]:
    source = ScidbSource(seeded)
    call = CustomCall(
        fn=plot_parity,
        inputs={"steps": source.get_table(["StepLength"]), "mass": source.get_table(["Mass"])},
        variables={"steps": "StepLength", "mass": "Mass"},
        constants={"filename": None},
    )
    spec = PlotSpec(
        measures=["StepLength"],
        # for_each iterates subject and passes the rest whole: nothing collapsed.
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session"],
        kind=PlotKind.CUSTOM,
        custom=CustomPlot(function="plot_parity", as_table=True),
    )
    return {
        str(figure.figure_key["subject"]): figure.custom.arguments({})
        for figure in resolve_custom(spec, call)
    }


def test_plot_studio_hands_the_function_what_for_each_does(seeded, tmp_path):
    pipeline = _pipeline(tmp_path)
    studio = _studio(seeded)
    assert sorted(studio) == sorted(pipeline), "the two paths make different calls"
    for subject in studio:
        piped, shown = pipeline[subject], studio[subject]
        assert _content(shown["steps"], ["session", "trial", "StepLength"]) == _content(
            piped["steps"], ["session", "trial", "StepLength"]
        ), subject
        # The subject-level input: one record, broadcast to the trial-level call.
        assert _content(shown["mass"], ["subject", "Mass"]) == _content(
            piped["mass"] if isinstance(piped["mass"], pd.DataFrame)
            else pd.DataFrame({"subject": [subject], "Mass": [piped["mass"]]}),
            ["subject", "Mass"],
        ), subject
