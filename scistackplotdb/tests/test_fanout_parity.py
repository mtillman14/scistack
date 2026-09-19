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
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE},
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
            "trial": Role.GROUP,
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
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        color="subject",
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
            "InterventionGroup": Role.GROUP,
            "subject": Role.COLLAPSE,
            "session": Role.COLLAPSE,
            "trial": Role.COLLAPSE,
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


def test_a_pinned_grouping_reaches_the_pipeline_the_same_way(
    two_label_variants, tmp_path
):
    """A grouping variable with TWO variants, previewed and exported.

    The two paths select the pinned records by genuinely different mechanisms:
    interactively it is ``variant_set_mask`` over the loaded frame, and in the
    pipeline it is scidb's load-time ``branch_params_filter`` behind
    ``Variant(GroupLabel, fn=..., scheme=...)``. Nothing makes those agree
    except that they were written to.

    The figures prove it rather than the source text. Both pins are exported and
    run; a pipeline that ignored the pin would load every variant of the sheet
    and draw the SAME figure twice, so the bytes differing is the evidence.

    That evidence needs one prop: the bytes have to be deterministic, or
    "different" could be satisfied by noise. Established by rendering one
    ResolvedPlot twice — deliberately NOT by running a third pipeline, which
    ``for_each`` may legitimately skip as already computed and which would then
    be testing the cache rather than the renderer.
    """
    from scidb import Variant
    from scistackplot import FactorVariable

    from conftest import GroupLabel, scheme_axis

    source = ScidbSource(two_label_variants)
    axis = scheme_axis(source)

    def draw(scheme: str, into):
        into.mkdir(exist_ok=True)
        groups = [FactorVariable("GroupLabel", variant={axis: scheme})]
        table = source.get_table(["StepLength"], factor_variables=groups)
        spec = PlotSpec(
            measures=["StepLength"],
            factor_variables=groups,
            roles={
                "GroupLabel": Role.GROUP,
                "subject": Role.COLLAPSE,
                "session": Role.COLLAPSE,
                "trial": Role.COLLAPSE,
            },
            kind=PlotKind.BAR,
        )
        interactive = resolve(spec, table)
        code = generate_endpoint(
            spec,
            table,
            input_variable="StepLength",
            path_template=str(into / "fig_all.png"),
        )
        files, failures = _run_generated(
            code, into, extra={"GroupLabel": GroupLabel, "Variant": Variant}
        )
        return interactive, code, files, failures

    a_figures, a_code, a_files, a_failures = draw("a", tmp_path / "a")
    b_figures, b_code, b_files, b_failures = draw("b", tmp_path / "b")

    assert len(a_files) == 1, _explain(a_code, a_failures)
    assert len(b_files) == 1, _explain(b_code, b_failures)

    # The pin reached the generated code at all.
    assert "Variant(GroupLabel" in a_code.foreach_source, a_code.foreach_source

    # The preview shows one pin's labels and not the other's.
    a_drawn = [str(v) for v in a_figures[0].x_order]
    b_drawn = [str(v) for v in b_figures[0].x_order]
    assert a_drawn and all(label.startswith("a-") for label in a_drawn), a_drawn
    assert b_drawn and all(label.startswith("b-") for label in b_drawn), b_drawn

    # The prop: identical input, identical bytes.
    import matplotlib.pyplot as plt
    from scistackplot import render_matplotlib

    rendered = []
    for name in ("det1.png", "det2.png"):
        path = tmp_path / name
        figure = render_matplotlib(a_figures[0])
        try:
            figure.savefig(path)
        finally:
            plt.close(figure)
        rendered.append(path.read_bytes())
    assert rendered[0] == rendered[1], (
        "rendering one figure twice gave different bytes, so the comparison "
        "below cannot distinguish a working pin from noise"
    )

    # ...and so the pipeline really did honour the pin.
    assert a_files[0].read_bytes() != b_files[0].read_bytes(), (
        "both pins exported the same figure — the pipeline loaded every variant "
        "of the grouping variable instead of the one that was pinned"
        + _explain(a_code, a_failures)
    )


def test_an_unpinned_grouping_exports_the_variable_bare(seeded, tmp_path):
    """The wrapper appears because a pin was made, not by default.

    Worth its own case beside the one above: if `Variant(...)` were emitted
    unconditionally, the parity test would still pass — both pins would differ —
    while every unpinned grouping in every existing pipeline gained a wrapper
    around an empty selection.
    """
    from scistackplot import FactorVariable

    from conftest import Condition

    groups = [FactorVariable("Condition")]
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"], factor_variables=groups)
    spec = PlotSpec(
        measures=["StepLength"],
        factor_variables=groups,
        roles={
            "Condition": Role.GROUP,
            "subject": Role.COLLAPSE,
            "session": Role.COLLAPSE,
            "trial": Role.COLLAPSE,
        },
        kind=PlotKind.BAR,
    )

    code = generate_endpoint(
        spec,
        table,
        input_variable="StepLength",
        path_template=str(tmp_path / "fig_all.png"),
    )
    files, failures = _run_generated(code, tmp_path, extra={"Condition": Condition})

    assert '"group_condition": Condition,' in code.foreach_source
    assert len(files) == 1, _explain(code, failures)
