"""Plot presets: settings without the data, applied to another variable.

``.claude/plan-plot-presets.md`` Stage 1. Storage is
``scistackplotdb/tests/test_presets.py``; the salvage of a drifted spec is
``test_restore.py``. This file is about the partition (what a preset owns and
what stays with the variable) and about applying a preset to new data.
"""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from scistackplot import (
    Alias,
    FactorVariable,
    LongTable,
    NoteKind,
    PanelOverride,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    TextSizes,
    VariantSet,
    YAxis,
    apply_preset,
    capabilities,
    make_template,
    shape_warning,
)
from scistackplot.presets import (
    FIELD_CLASSES,
    MEASURE_PLACEHOLDER,
    FieldClass,
    unclassified_fields,
)

from test_restore import full_spec


@pytest.fixture
def width_table(scalar_frame) -> LongTable:
    """The same scalar data under another variable's name: the target."""
    return LongTable.from_frame(
        scalar_frame.rename(columns={"StepLength": "StepWidth"}),
        factors=["subject", "session", "trial"],
        measures=["StepWidth"],
        name="StepWidth",
        schema_levels=["subject", "session", "trial"],
    )


def _box(**changes) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BOX,
        style=StyleOptions(width=7.2, height=4.5, title="Step length", y_label="m"),
    )
    base.update(changes)
    return PlotSpec(**base)


def _target(**changes) -> PlotSpec:
    base = dict(measures=["StepWidth"])
    base.update(changes)
    return PlotSpec(**base)


def _paths(notes, kind=None) -> list[str]:
    return [n.path for n in notes if kind is None or n.kind == kind]


# ---------------------------------------------------------------------------
# The partition
# ---------------------------------------------------------------------------


def test_every_spec_field_is_classified():
    """A new PlotSpec field must be placed in FIELD_CLASSES: does it travel
    with a preset, or stay with the variable?"""
    missing, stale = unclassified_fields()
    assert missing == [], f"classify these in presets.FIELD_CLASSES: {missing}"
    assert stale == [], f"these FIELD_CLASSES entries name no field: {stale}"


def test_data_and_variable_text_are_not_in_the_template():
    template = make_template(full_spec())
    assert "measures" not in template
    assert "variant_sets" not in template
    assert "title" not in template["style"]
    assert "y_label" not in template["style"]
    assert template["style"]["x_label"] == "Session"
    assert "minimum" not in template["y_axis"] and "maximum" not in template["y_axis"]
    assert template["y_axis"]["scope"] == ["subject"]
    (override,) = template["panel_overrides"]
    assert override == {"match": {"trial": "01"}, "y_label_hidden": True}


def test_a_panel_override_holding_only_variable_text_is_not_kept():
    spec = _box(panel_overrides=[PanelOverride(match={"trial": "1"}, y_maximum=2.0)])
    assert make_template(spec).get("panel_overrides") == []


def test_a_template_never_names_its_variable():
    spec = _box(
        aliases={"StepLength": Alias(name="Step length (m)"), "session": Alias(name="Visit")},
        factor_variables=[FactorVariable("StepLength", "Side", {"Code:x": "v1"})],
        variant_sets=[VariantSet(selection={"Code:x": "v1"}, variable="StepLength")],
    )
    template = make_template(spec)
    assert "StepLength" not in json.dumps(template)
    assert template["aliases"] == {"session": {"name": "Visit"}}
    assert template["factor_variables"] == [
        {"variable": MEASURE_PLACEHOLDER, "column": "Side", "variant": {}}
    ]


def test_template_is_plain_json():
    assert json.loads(json.dumps(make_template(full_spec())))


def test_variable_text_paths_are_the_ones_the_user_chose():
    """D2 (user, 2026-10-06): title, y label and y limits stay with the variable."""
    held = {p for p, c in FIELD_CLASSES.items() if c is FieldClass.VARIABLE_TEXT}
    assert held == {
        "style.title",
        "style.y_label",
        "y_axis.minimum",
        "y_axis.maximum",
        "aliases[<measure>]",
        "panel_overrides[].y_minimum",
        "panel_overrides[].y_maximum",
        "panel_overrides[].y_label",
    }


# ---------------------------------------------------------------------------
# Applying (no data)
# ---------------------------------------------------------------------------


def test_applying_to_the_variable_it_was_made_on_is_the_identity():
    spec = full_spec()
    result = apply_preset(make_template(spec), spec)
    assert result.notes == []
    assert result.spec == spec


def test_target_keeps_its_data_and_variable_text():
    target = _target(
        variant_sets=[VariantSet(selection={"Code:w": "v2"})],
        style=StyleOptions(title="Width", y_label="cm", width=3.0),
        y_axis=YAxis(minimum=1.0),
    )
    spec = apply_preset(make_template(full_spec()), target).spec
    assert spec.measures == ["StepWidth"]
    assert spec.variant_sets == target.variant_sets
    assert (spec.style.title, spec.style.y_label) == ("Width", "cm")
    assert (spec.y_axis.minimum, spec.y_axis.maximum) == (1.0, None)
    # ...and everything else is the preset's.
    assert spec.style.width == 7.2
    assert spec.y_axis.scope == ["subject"]
    assert spec.kind is PlotKind.BOX
    assert spec.roles == full_spec().roles


def test_the_preset_replaces_every_template_setting_of_the_target():
    """D3 (user, 2026-10-06): a full replace, not a merge of changed fields."""
    target = _target(kind=PlotKind.SCATTER, roles={"session": Role.FACET}, color="session")
    spec = apply_preset(make_template(_box()), target).spec
    assert spec.kind is PlotKind.BOX
    assert spec.roles == _box().roles
    assert spec.color is None


def test_the_measure_alias_and_panel_limits_stay_with_the_target():
    source = _box(
        aliases={"StepLength": Alias(name="Step length"), "session": Alias(name="Visit")},
        panel_overrides=[PanelOverride(match={"trial": "1"}, y_maximum=2.0, y_label_hidden=True)],
    )
    target = _target(
        aliases={"StepWidth": Alias(name="Step width")},
        panel_overrides=[
            PanelOverride(match={"trial": "1"}, y_minimum=-1.0),
            PanelOverride(match={"trial": "2"}, y_label="cm"),
        ],
    )
    spec = apply_preset(make_template(source), target).spec
    assert spec.aliases == {"session": Alias(name="Visit"), "StepWidth": Alias(name="Step width")}
    assert spec.panel_overrides == [
        PanelOverride(match={"trial": "1"}, y_minimum=-1.0, y_label_hidden=True),
        PanelOverride(match={"trial": "2"}, y_label="cm"),
    ]


def test_an_own_column_grouping_is_rebased_onto_the_target():
    source = _box(factor_variables=[
        FactorVariable("StepLength", "Side", {"Code:x": "v1"}),
        FactorVariable("Demographics", "Sex"),
    ])
    spec = apply_preset(make_template(source), _target()).spec
    assert spec.factor_variables == [
        FactorVariable("StepWidth", "Side"),
        FactorVariable("Demographics", "Sex"),
    ]


def test_a_variable_text_setting_in_a_stored_template_is_still_ignored():
    """A template saved before a setting was reclassified still leaves it with
    the variable: apply strips with the same table."""
    template = make_template(_box())
    template["style"]["title"] = "Old title"
    template["measures"] = ["StepLength"]
    spec = apply_preset(template, _target(style=StyleOptions(title="Width"))).spec
    assert spec.style.title == "Width"
    assert spec.measures == ["StepWidth"]


def test_a_drifted_template_applies_with_notes():
    template = make_template(_box())
    template["old_setting"] = 1
    template["kind"] = "pie"
    result = apply_preset(template, _target())
    assert sorted(_paths(result.notes)) == ["kind", "old_setting"]
    assert result.spec.roles == _box().roles


def test_a_template_that_is_not_a_table_applies_nothing_with_a_note():
    result = apply_preset("garbage", _target(kind=PlotKind.BAR))
    assert _paths(result.notes, NoteKind.INVALID_VALUE) == [""]
    assert result.spec.measures == ["StepWidth"]


# ---------------------------------------------------------------------------
# Applying against the target's data
# ---------------------------------------------------------------------------


def test_a_matching_preset_applies_cleanly(width_table):
    result = apply_preset(make_template(_box(show_sample=["trial"])), _target(), table=width_table)
    assert result.notes == []
    assert result.spec.kind is PlotKind.BOX
    assert result.spec.show_sample == ["trial"]


def test_factors_the_target_lacks_are_reconciled(width_table):
    source = _box(
        roles={"session": Role.GROUP, "limb": Role.FACET, "subject": Role.COLLAPSE,
               "trial": Role.COLLAPSE},
        show_sample=["cycle"],
    )
    result = apply_preset(make_template(source), _target(), table=width_table)
    assert sorted(_paths(result.notes, NoteKind.NOT_IN_DATA)) == ["roles.limb", "show_sample"]
    assert result.spec.show_sample == []


def test_a_kind_the_target_cannot_draw_falls_back_to_the_default(width_table):
    result = apply_preset(make_template(_box(kind=PlotKind.HEATMAP)), _target(), table=width_table)
    assert _paths(result.notes, NoteKind.KIND_UNAVAILABLE) == ["kind"]
    caps = capabilities(result.spec, width_table)
    assert str(result.spec.kind) == caps["default"]
    assert str(result.spec.kind) in caps["available"]


def test_table_for_loads_with_the_applied_spec(width_table):
    seen = []

    def table_for(spec):
        seen.append(spec)
        return width_table

    result = apply_preset(make_template(_box()), _target(), table_for=table_for)
    assert [s.measures for s in seen] == [["StepWidth"]]
    assert seen[0].kind is PlotKind.BOX
    assert result.notes == []


def test_an_own_column_the_target_lacks_is_dropped_with_a_note(width_table):
    def table_for(spec):
        if any(g.is_own_column(spec.y_measure) for g in spec.factor_variables):
            raise ValueError("'StepWidth' has no column 'Side'")
        return width_table

    source = _box(factor_variables=[FactorVariable("StepLength", "Side")])
    result = apply_preset(make_template(source), _target(), table_for=table_for)
    assert _paths(result.notes, NoteKind.NOT_IN_DATA) == ["factor_variables[0]"]
    assert "Side" in result.notes[0].message
    assert result.spec.factor_variables == []


def test_a_loader_that_fails_applies_unchecked_with_a_note():
    def table_for(spec):
        raise RuntimeError("database closed")

    result = apply_preset(make_template(_box()), _target(), table_for=table_for)
    assert _paths(result.notes, NoteKind.NOT_IN_DATA) == [""]
    assert "database closed" in result.notes[0].message
    assert result.spec.kind is PlotKind.BOX


def test_a_series_target_keeps_a_scalar_kind_by_collapsing_cells(series_table):
    """A box preset on a 1-D variable is drawable (each cell reduced to one
    value), so it is kept rather than replaced by the default line."""
    template = make_template(_box())
    result = apply_preset(template, PlotSpec(measures=["Signal"]), table=series_table)
    assert result.spec.kind is PlotKind.BOX
    assert _paths(result.notes, NoteKind.KIND_UNAVAILABLE) == []


# ---------------------------------------------------------------------------
# Shape warning
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "made_on, shape, warns",
    [("scalar", "scalar", False), ("scalar", "1d", True), (None, "scalar", False),
     ("scalar", None, False)],
)
def test_shape_warning(made_on, shape, warns):
    assert (shape_warning(made_on, shape) is not None) is warns


def test_template_round_trip_through_replace_keeps_identity():
    spec = replace(full_spec(), measures=["StepWidth"])
    assert apply_preset(make_template(full_spec()), spec).spec == spec


# ---------------------------------------------------------------------------
# Automatic text size (autosize, 2026-10-06)
# ---------------------------------------------------------------------------


def _with_text(spec: PlotSpec, text: TextSizes) -> PlotSpec:
    return replace(spec, style=replace(spec.style, text=text))


def test_an_auto_preset_carries_auto_and_its_target():
    """`style.text` is template: a preset made on an auto plot applies auto,
    with its Print/Slide band, even over a target whose font was fixed."""
    template = make_template(_with_text(_box(), TextSizes(target="slide")))
    assert template["style"]["text"] == {"target": "slide"}, "base None is dropped, not stored"
    target = _with_text(_target(), TextSizes(base=11.0, x_ticks=9.0))
    applied = apply_preset(template, target).spec
    assert applied.style.text == TextSizes(target="slide")


def test_a_fixed_preset_fixes_an_auto_target():
    template = make_template(_with_text(_box(), TextSizes(base=12.0, legend=9.0)))
    applied = apply_preset(template, _target()).spec
    assert applied.style.text == TextSizes(base=12.0, legend=9.0)


def test_a_preset_never_stores_chosen_auto_sizes():
    """The sizes auto chose are a property of one figure's labels at one
    size, not a setting: a template holds `target` and nothing chosen."""
    template = json.dumps(make_template(_box()))
    assert "auto" not in template and "sizes" not in template


def test_an_applied_auto_preset_is_sized_for_the_new_variable(width_table):
    pytest.importorskip("matplotlib")
    from scistackplot import resolve
    from scistackplot.autosize import settle

    applied = apply_preset(make_template(_box()), _target(), table=width_table).spec
    (resolved,) = resolve(applied, width_table)
    auto = settle(resolved).auto_text
    assert auto is not None and auto.target == "print"
