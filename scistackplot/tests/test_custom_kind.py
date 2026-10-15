"""Custom plot kinds, Stage 1: the spec (``.claude/plan-custom-plot-kinds.md``).

A ``plot_`` function draws a ``PlotKind.CUSTOM`` figure, named by
``PlotSpec.custom``. This file pins the rules that hold before anything is
drawn: the pairing of kind and function, that the kind grid never offers the
function, how it serialises, and how a preset treats it. Round-trip coverage
of every field is ``test_restore.py`` (``full_custom_spec``).
"""

from __future__ import annotations

import pytest

from scistackplot import (
    BUILTIN_KINDS,
    CustomPlot,
    LongTable,
    NoteKind,
    PlotKind,
    PlotSpec,
    Role,
    RoleError,
    apply_preset,
    capabilities,
    make_template,
    validate,
)
from scistackplot.roles import custom_kind_problem


def _custom(**changes) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.CUSTOM,
        custom=CustomPlot(function="plot_steps", inputs={"steps": "StepLength"}),
    )
    base.update(changes)
    return PlotSpec(**base)


def _box(**changes) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BOX,
    )
    base.update(changes)
    return PlotSpec(**base)


# ---------------------------------------------------------------------------
# The kind and its function come together
# ---------------------------------------------------------------------------


def test_builtin_kinds_are_every_kind_but_custom():
    assert PlotKind.CUSTOM not in BUILTIN_KINDS
    assert set(BUILTIN_KINDS) | {PlotKind.CUSTOM} == set(PlotKind)


def test_a_custom_kind_with_its_function_is_accepted(scalar_table):
    assert custom_kind_problem(_custom()) is None
    validate(_custom(), scalar_table)


@pytest.mark.parametrize(
    "custom", [None, CustomPlot(function="")], ids=["no-block", "no-name"]
)
def test_a_custom_kind_without_a_function_is_refused(scalar_table, custom):
    spec = _custom(custom=custom)
    with pytest.raises(RoleError, match="plot_ function"):
        validate(spec, scalar_table)


def test_a_builtin_kind_naming_a_function_is_refused(scalar_table):
    spec = _box(custom=CustomPlot(function="plot_steps"))
    with pytest.raises(RoleError, match="plot_steps"):
        validate(spec, scalar_table)


# ---------------------------------------------------------------------------
# The kind grid
# ---------------------------------------------------------------------------


def test_the_kind_grid_never_offers_custom(scalar_table):
    caps = capabilities(_box(), scalar_table)
    listed = {entry["kind"] for entry in caps["kinds"]}
    assert "custom" not in listed
    assert "custom" not in caps["available"]
    assert listed == {str(kind) for kind in BUILTIN_KINDS}


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("as_table", [True, False, ["steps"]])
def test_as_table_round_trips(as_table):
    spec = _custom(custom=CustomPlot(function="plot_steps", as_table=as_table))
    back = PlotSpec.from_json(spec.to_json())
    assert back.custom == spec.custom
    assert back.custom.as_table == as_table


def test_a_builtin_spec_writes_no_custom_block():
    assert "custom" not in _box().to_dict()


# ---------------------------------------------------------------------------
# Presets: the function is DATA, the kind follows it
# ---------------------------------------------------------------------------


def test_a_preset_does_not_change_what_draws_a_custom_plot():
    target = _custom()
    spec = apply_preset(make_template(_box(groups=[], roles={})), target).spec
    assert spec.kind is PlotKind.CUSTOM
    assert spec.custom == target.custom
    # ...while the template's other settings still apply.
    assert spec.roles == {}


def test_a_custom_template_never_makes_a_variable_plot_custom():
    template = make_template(_custom())
    assert "custom" not in template  # DATA: never stored in a preset
    result = apply_preset(template, _box())
    assert result.spec.kind is not PlotKind.CUSTOM
    assert result.spec.custom is None
    assert [n.path for n in result.notes if n.kind is NoteKind.KIND_UNAVAILABLE] == ["kind"]


def test_a_custom_target_keeps_its_kind_when_checked_against_data(scalar_table: LongTable):
    """``_fit_kind`` asks the kind grid, which never lists CUSTOM: it must not
    'fall back' from the function the plot_ node draws with."""
    result = apply_preset(make_template(_box()), _custom(), table=scalar_table)
    assert result.spec.kind is PlotKind.CUSTOM
    assert not [n for n in result.notes if n.kind is NoteKind.KIND_UNAVAILABLE]
