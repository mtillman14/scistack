"""Combines are a reduction step, applied by ``reduce`` and printed by codegen.

``reduction.CombineLevels`` holds the one rule (P5 stage 6, design review 06
P2): ``groups.apply_level_groups`` applies it, and
``export.reduction_code.emit_combine`` prints it. These tests execute the
emitted pandas on the frames ``apply_level_groups`` combines and require the
same rows and values (the standing "export equals preview" rule).

Regressions guarded, all from codegen's own copy of the rule:

* a combine whose source column is missing was skipped (with a WARN) in the
  preview and raised ``KeyError`` in the export;
* a combine named after an existing column was skipped in the preview and
  OVERWROTE that column in the export;
* the export combined AFTER the value filters and the categorical rewrite,
  while the preview combines before them, so a filter on the combined factor
  found no column in the export and kept every level.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, resolve
from scistackplot.export.codegen import generate_plot_function
from scistackplot.export.reduction_code import emit_combine, emit_plan
from scistackplot.model.spec import Filter, LevelGroup
from scistackplot.reduce.groups import apply_level_groups, combine_steps
from scistackplot.reduce.reduction import CollapseLevels, CombineLevels, ValueFilter


def _table() -> LongTable:
    frame = pd.DataFrame(
        {
            "subject": ["01", "01", "01", "02", "02", "02"],
            "condition": ["stim1", "stim2", "sham", "stim1", "stim2", "sham"],
            "dose": [1, 2, 3, 1, 2, 3],
            "Force": [1.0, 3.0, 10.0, 5.0, 7.0, 20.0],
        }
    )
    return LongTable.from_frame(
        frame, factors=["subject", "condition", "dose"], measures=["Force"], name="Force"
    )


GROUPS = {
    "drop-unmatched": LevelGroup("Arm", "condition", {"stim1": "STIM", "stim2": "STIM"}),
    "catch-all": LevelGroup("Arm", "condition", {"sham": "SHAM"}, unmatched="STIM"),
    "int-source-text-mapping": LevelGroup("Dose", "dose", {"1": "low", "2": "low", "3": "high"}),
    "empty-label-drops": LevelGroup("Arm", "condition", {"stim1": "STIM", "sham": ""}),
    "missing-source": LevelGroup("Arm", "session", {"pre": "A"}),
    "name-collision": LevelGroup("subject", "condition", {"stim1": "X"}),
}


def _run(lines: list[str], frame: pd.DataFrame) -> pd.DataFrame:
    namespace: dict = {"df": frame.copy(), "pd": pd}
    exec("\n".join(lines), namespace)  # noqa: S102
    return namespace["df"]


@pytest.mark.parametrize("name", list(GROUPS))
def test_emitted_combine_matches_the_preview(name):
    spec = PlotSpec(measures=["Force"], level_groups=[GROUPS[name]])
    table = _table()
    expected = apply_level_groups(spec, table).frame
    got = _run(emit_plan(combine_steps(spec)), table.frame)
    pd.testing.assert_frame_equal(got, expected)


def test_a_skipped_combine_leaves_the_frame_alone():
    table = _table()
    for name in ("missing-source", "name-collision"):
        step = CombineLevels.of(GROUPS[name])
        assert step.skip_reason(table.frame.columns) is not None
        pd.testing.assert_frame_equal(_run(emit_combine(step), table.frame), table.frame)


def test_the_effective_mapping_keeps_spec_order_and_drops_empty_labels():
    step = CombineLevels.of(
        LevelGroup("Arm", "condition", {"stim2": "B", "stim1": "A", "sham": ""}, unmatched="Z")
    )
    assert step.mapping == (("stim2", "B"), ("stim1", "A"))
    assert step.declared_levels() == ["B", "A", "Z"]


def test_only_combines_in_effect_become_steps():
    spec = PlotSpec(
        measures=["Force"],
        level_groups=[
            LevelGroup("Arm", "condition", {"stim1": "S"}, active=False),
            LevelGroup("Arm2", "condition", {"stim1": "S"}),
            LevelGroup("Arm3", "condition", {"stim1": "T"}),  # second of one source
        ],
    )
    assert [step.name for step in combine_steps(spec).of_type(CombineLevels)] == ["Arm2"]


def _filtered_on_the_combine() -> PlotSpec:
    return PlotSpec(
        measures=["Force"],
        roles={
            "Arm": Role.GROUP,
            "subject": Role.COLLAPSE,
            "condition": Role.COLLAPSE,
            "dose": Role.COLLAPSE,
        },
        kind=PlotKind.BOX,
        level_groups=[
            LevelGroup("Arm", "condition", {"stim1": "STIM", "stim2": "STIM", "sham": "SHAM"})
        ],
        filters=[Filter("Arm", include=["STIM"])],
    )


def test_the_export_combines_before_it_filters():
    """A filter on the combined factor needs its column, as in the preview."""
    spec = _filtered_on_the_combine()
    source = generate_plot_function(spec, _table())
    combine_at = source.index(emit_combine(CombineLevels.of(spec.level_groups[0]))[1])
    filter_at = source.index("if 'Arm' in df.columns:")
    assert combine_at < filter_at


def test_the_figure_records_its_combines_before_its_filters():
    spec = _filtered_on_the_combine()
    figure = resolve(spec, _table())[0]
    kinds = [type(step) for step in figure.reduction.steps]
    # The shared steps in the order applied, then the figure's collapse chain.
    assert kinds[:2] == [CombineLevels, ValueFilter]
    assert kinds[2:] and set(kinds[2:]) == {CollapseLevels}
    assert figure.reduction.of_type(CombineLevels) == [CombineLevels.of(spec.level_groups[0])]
