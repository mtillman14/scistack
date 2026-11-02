"""Codegen prints ``reduce``'s reduction steps (design review 06, P2).

Each step type in ``scistackplot.reduce.reduction`` has ONE rule (applied by
``reduce``) and ONE emitter (``scistackplot.export.reduction_code``). These
tests execute the emitted pandas on the same frames ``reduce`` masks and
require the same rows: an exported figure filtered differently from the
preview is a codegen bug (the standing "export equals preview" rule).

Regression guarded: before the ``ValueFilter`` step, codegen emitted
``df[col].isin([...])`` and ``df[col] >= x`` while ``reduce`` compared levels
as TEXT and bounds on the column coerced to numbers. A picker selection of
``"1"`` on an integer column kept rows in the preview and none in the export;
a filter naming a column the frame lacked was skipped in the preview and
raised ``KeyError`` in the export.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import resolve
from scistackplot.export.codegen import generate_plot_function
from scistackplot.export.reduction_code import emit_plan, emit_step
from scistackplot.model.spec import Filter, LocationFilter, PlotKind, PlotSpec, Role
from scistackplot.reduce.reduction import (
    LocationMask,
    ReductionPlan,
    ValueFilter,
    filter_steps,
)
from scistackplot.reduce.resolve import apply_filters


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "subject": ["01", "02", "03", "04"],
            "trial": [1, 2, 3, 4],
            "speed": ["1.5", "2.0", "fast", None],
            "StepLength": [0.5, 0.6, 0.7, 0.8],
        }
    )


FILTERS = {
    "text-include": Filter("subject", include=["01", "03"]),
    "int-column-text-selection": Filter("trial", include=["1", "4"]),
    "text-column-int-selection": Filter("subject", exclude=[1]),
    "exclude-ints": Filter("trial", exclude=[2]),
    "bounds-on-numbers": Filter("trial", minimum=2, maximum=3),
    "bounds-on-text-numbers": Filter("speed", minimum=1.6),
    "missing-column": Filter("session", include=["pre"]),
    "everything": Filter("trial", include=[1, 2, 3], exclude=["3"], minimum=1.5),
}


def _run(lines: list[str], frame: pd.DataFrame) -> pd.DataFrame:
    namespace: dict = {"df": frame.copy(), "pd": pd}
    exec("\n".join(lines), namespace)  # noqa: S102
    return namespace["df"]


@pytest.mark.parametrize("name", list(FILTERS))
def test_emitted_value_filter_keeps_what_reduce_keeps(name):
    spec = PlotSpec(measures=["StepLength"], filters=[FILTERS[name]])
    expected = apply_filters(_frame(), spec)
    got = _run(emit_plan(filter_steps(spec)), _frame())
    assert list(got.index) == list(expected.index)


def test_several_filters_and_a_location_mask_compose_like_reduce():
    spec = PlotSpec(
        measures=["StepLength"],
        filters=[Filter("trial", minimum=2), Filter("speed", exclude=["fast"])],
        location_filter=LocationFilter(include=[[["subject", "02"]], [["subject", "03"]]]),
    )
    expected = apply_filters(_frame(), spec)
    got = _run(emit_plan(filter_steps(spec)), _frame())
    assert list(got.index) == list(expected.index) == [1]


def test_the_step_mask_is_the_rule_apply_filters_uses():
    flt = Filter("trial", include=["1", "4"])
    spec = PlotSpec(measures=["StepLength"], filters=[flt])
    mask = ValueFilter.of(flt).mask(_frame())
    assert list(_frame()[mask].index) == list(apply_filters(_frame(), spec).index)


def test_filter_steps_order_and_skip():
    spec = PlotSpec(
        measures=["StepLength"],
        filters=[Filter("StepLength", minimum=0.6), Filter("trial", include=[1])],
        location_filter=LocationFilter(include=[[["subject", "01"]]]),
    )
    plan = filter_steps(spec)
    assert [type(s) for s in plan.steps] == [LocationMask, ValueFilter, ValueFilter]
    skipped = filter_steps(spec, frozenset({"StepLength"}))
    assert [s.column for s in skipped.of_type(ValueFilter)] == ["trial"]


def test_an_inert_location_filter_is_no_step():
    spec = PlotSpec(measures=["StepLength"], location_filter=LocationFilter())
    assert filter_steps(spec) == ReductionPlan()


def test_level_texts_are_sorted_for_stable_code():
    step = ValueFilter.of(Filter("trial", include=[3, "1", 2, 1]))
    assert step.include == ("1", "2", "3")
    assert emit_step(step) == [
        "if 'trial' in df.columns:",
        "    df = df[df['trial'].astype(str).isin(['1', '2', '3'])]",
    ]


def test_generated_function_prints_the_filter_step(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "trial": Role.COLLAPSE, "session": Role.COLLAPSE},
        kind=PlotKind.BOX,
        filters=[Filter(column="session", include=["pre"])],
    )
    source = generate_plot_function(spec, scalar_table)
    for line in emit_step(ValueFilter.of(spec.filters[0])):
        assert line in source


def test_resolved_figure_records_its_filter_steps(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "trial": Role.COLLAPSE, "session": Role.COLLAPSE},
        kind=PlotKind.BOX,
        filters=[Filter(column="session", include=["pre"])],
    )
    figure = resolve(spec, scalar_table)[0]
    assert figure.reduction.of_type(ValueFilter) == [ValueFilter.of(spec.filters[0])]
