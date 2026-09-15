"""
The parity test.

``Role.ITERATE`` fans out twice by two different mechanisms: interactively
through a pandas ``groupby`` inside ``resolve()``, and in the pipeline through
``for_each`` + ``PathOutput``. They must produce the same set of figures. If
they ever disagree, the exported pipeline is not what the user previewed —
the worst failure mode this layer has.

This test runs BOTH paths against the same database and compares the figure
sets, rather than asserting on the generated source text (which would pass
happily while the semantics drifted).
"""

from __future__ import annotations

import pytest
from scistackplot import PlotKind, PlotSpec, Role, resolve

from scistackplotdb import ScidbSource, generate_endpoint

pytest.importorskip("seaborn")


def _run_generated(code, tmp_path, extra: dict | None = None):
    """Execute the generated endpoint, returning the files and any failures.

    ``for_each`` records a per-combo failure and carries on (see
    ``scifor.foreach._record_iteration_failure``) — deliberately, since one bad
    combination should not abandon a long run. That makes the naive form of this
    test useless when it fails: every combo raises, no file is written, and the
    assertion says only "0 != 12" while the reason sits in a log nobody captured.

    So the summary event is captured and handed back, and the assertions below
    put it in their message. A parity failure has to explain itself; it is the
    one failure in this layer that means the exported pipeline draws something
    different from what the user previewed.
    """
    from scidb import PathOutput, for_each

    from conftest import StepLength, StepLengthFigure

    failures: dict[str, list[str]] = {}

    def _capturing_for_each(*args, **kwargs):
        def _progress(event):
            if event.get("event") == "summary":
                failures.update(event.get("failure_reasons") or {})

        return for_each(*args, _progress_fn=_progress, **kwargs)

    namespace = {
        "for_each": _capturing_for_each,
        "PathOutput": PathOutput,
        "StepLength": StepLength,
        "StepLengthFigure": StepLengthFigure,
        **(extra or {}),
    }
    exec(compile(code.source, "<generated>", "exec"), namespace)  # noqa: S102
    return sorted(tmp_path.glob("fig_*.png")), failures


def _explain(code, failures) -> str:
    """Assertion message: what the pipeline hit, and the source it hit it in."""
    reasons = "\n".join(
        f"  {len(combos)}x {reason}" for reason, combos in failures.items()
    ) or "  (none recorded)"
    return f"\nfor_each failures:\n{reasons}\n\ngenerated source:\n{code.source}"


def test_interactive_fanout_matches_pipeline_fanout(seeded, tmp_path):
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"])
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.ITERATE, "session": Role.X, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )

    interactive = resolve(spec, table)
    code = generate_endpoint(
        spec,
        table,
        input_variable="StepLength",
        path_template=str(tmp_path / "fig_{subject}.png"),
    )
    files, failures = _run_generated(code, tmp_path)

    assert len(files) == len(interactive) == 3, _explain(code, failures)
    assert {path.stem.removeprefix("fig_") for path in files} == {
        figure.figure_key["subject"] for figure in interactive
    }


def test_two_iterate_keys_fan_out_the_same_both_ways(seeded, tmp_path):
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"])
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "subject": Role.ITERATE,
            "session": Role.ITERATE,
            "trial": Role.X,
        },
        kind=PlotKind.SCATTER,
    )

    interactive = resolve(spec, table)
    code = generate_endpoint(
        spec,
        table,
        input_variable="StepLength",
        path_template=str(tmp_path / "fig_{subject}_{session}.png"),
    )
    files, failures = _run_generated(code, tmp_path)

    assert len(interactive) == 6  # 3 subjects x 2 sessions
    assert len(files) == len(interactive), _explain(code, failures)


def test_a_promoted_ancestor_fans_out_the_same_both_ways(seeded, tmp_path):
    """Iterating a nested key alone: the promotion must reach the pipeline.

    This is the case the promotion exists for, and the one where the two paths
    could most easily diverge — the spec literally names only `trial`, so an
    endpoint built from `spec.iterate_factors` would run once per trial NUMBER
    (2 figures pooling every subject) while the preview showed one figure per
    location (12). The parity here is the guard on that.
    """
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"])
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.ITERATE},
        kind=PlotKind.SCATTER,
    )

    interactive = resolve(spec, table)
    code = generate_endpoint(
        spec,
        table,
        input_variable="StepLength",
        path_template=str(tmp_path / "fig_{subject}_{session}_{trial}.png"),
    )
    files, failures = _run_generated(code, tmp_path)

    assert len(interactive) == 3 * 2 * 2  # every (subject, session, trial)
    assert len(files) == len(interactive), _explain(code, failures)
    assert {path.stem.removeprefix("fig_") for path in files} == {
        "_".join(str(v) for v in figure.figure_key.values())
        for figure in interactive
    }


def test_no_iterate_produces_exactly_one_figure_both_ways(seeded, tmp_path):
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"])
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.X, "subject": Role.COLOR, "trial": Role.FREE},
        kind=PlotKind.BOX,
    )

    interactive = resolve(spec, table)
    code = generate_endpoint(
        spec,
        table,
        input_variable="StepLength",
        path_template=str(tmp_path / "fig_all.png"),
    )
    files, failures = _run_generated(code, tmp_path)

    assert len(interactive) == 1
    assert len(files) == 1, _explain(code, failures)


def test_a_grouping_column_reaches_the_pipeline_the_same_way(
    with_demographics, tmp_path
):
    """One column of a wide sheet, previewed and exported.

    The two paths reach the same column by different routes: interactively it
    is a pandas merge inside ``ScidbSource``, and in the pipeline it is a
    ``Demographics["InterventionGroup"]`` input that ``as_table`` delivers as
    schema keys plus that one column. This runs the real ``for_each`` over the
    real database, which is the only thing that proves the second route.
    """
    from scistackplot import FactorVariable

    from conftest import Demographics

    groups = [FactorVariable("Demographics", "InterventionGroup")]
    source = ScidbSource(with_demographics)
    table = source.get_table(["StepLength"], factor_variables=groups)
    spec = PlotSpec(
        measures=["StepLength"],
        factor_variables=groups,
        roles={
            "InterventionGroup": Role.X,
            "subject": Role.FREE,
            "session": Role.FREE,
            "trial": Role.FREE,
        },
        kind=PlotKind.BAR,
    )

    interactive = resolve(spec, table)
    code = generate_endpoint(
        spec,
        table,
        input_variable="StepLength",
        path_template=str(tmp_path / "fig_all.png"),
    )
    files, failures = _run_generated(
        code, tmp_path, extra={"Demographics": Demographics}
    )

    assert len(interactive) == 1
    assert len(files) == 1, _explain(code, failures)
    # Every group the preview drew, including the subject the sheet omits.
    assert [str(v) for v in interactive[0].x_order] == [
        "Digitimer",
        "Onward",
        "(missing)",
    ]
