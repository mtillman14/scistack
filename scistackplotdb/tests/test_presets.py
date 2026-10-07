"""Plot presets: the store, and applying a preset to another variable's data.

``.claude/plan-plot-presets.md`` Stage 2. What a preset owns (the partition)
is ``scistackplot/tests/test_presets.py``. This file is about the store
(the same rules as saved plots, through the shared ``VersionedStore``) and
about applying a preset against a real database.
"""

from __future__ import annotations

import json

import pytest

from scistackplot import (
    FactorVariable,
    NoteKind,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    VariantSet,
    capabilities,
)
from scistackplotdb import (
    PresetError,
    PresetExists,
    ScidbSource,
    apply_preset_to,
    check_preset_name,
    find_preset,
    hide_preset,
    list_presets,
    load_preset,
    preset_history,
    rename_preset,
    save_preset,
    save_plot,
    list_saved_plots,
)
from scistackplotdb.presets import TABLE, _table_exists


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


def _stored(db, preset_id: str, version: int) -> dict:
    row = db._duck._fetchone(
        f"SELECT envelope_json FROM {TABLE} WHERE preset_id = ? AND version = ?",
        [preset_id, version],
    )
    return json.loads(row[0])


def _paths(notes, kind=None) -> list[str]:
    return [n.path for n in notes if kind is None or n.kind == kind]


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------


def test_listing_an_untouched_database_does_not_create_the_table(db):
    assert list_presets(db) == []
    assert not _table_exists(db)


def test_save_stores_the_template_and_where_it_was_made(db):
    info = save_preset(db, "Session box", _box(), made_on_shape="scalar")
    assert (info.name, info.version, info.made_on, info.made_on_shape) == (
        "Session box", 1, "StepLength", "scalar"
    )
    stored = _stored(db, info.preset_id, 1)
    assert stored["format"] == 1
    assert "measures" not in stored["template"]
    assert "title" not in stored["template"]["style"]
    assert stored["made_on"] == {"variable": "StepLength", "shape": "scalar"}
    assert [p.to_dict() for p in list_presets(db)] == [info.to_dict()]


def test_a_dict_spec_is_read_strictly(db):
    """Strict = `PlotSpec.from_dict`, the same reader `save_plot` uses: a
    legacy key or an unknown style key is refused. (An unknown TOP-level key
    is ignored by from_dict itself; that is its rule, not this store's.)"""
    legacy = dict(_box().to_dict(), x_layers=["session"])
    with pytest.raises(PresetError):
        save_preset(db, "x", legacy)
    bad_style = _box().to_dict()
    bad_style["style"]["no_such_style"] = 1
    with pytest.raises(PresetError):
        save_preset(db, "x", bad_style)
    assert list_presets(db) == []


def test_presets_are_project_wide_not_per_variable(db):
    save_preset(db, "Session box", _box())
    with pytest.raises(PresetExists):
        save_preset(db, "Session box", _box(measures=["Signal"]), overwrite=False)


def test_resaving_a_name_appends_a_version(db):
    first = save_preset(db, "Session box", _box())
    second = save_preset(db, "Session box", _box(kind=PlotKind.VIOLIN))
    assert second.preset_id == first.preset_id and second.version == 2
    assert [v.version for v in preset_history(db, first.preset_id)] == [2, 1]
    assert load_preset(db, first.preset_id).template["kind"] == "violin"
    assert load_preset(db, first.preset_id, version=1).template["kind"] == "box"


def test_overwrite_false_refuses_only_another_presets_name(db):
    mine = save_preset(db, "Mine", _box())
    other = save_preset(db, "Other", _box())
    again = save_preset(db, "Mine", _box(), overwrite=False, current_preset_id=mine.preset_id)
    assert again.version == 2
    with pytest.raises(PresetExists) as refused:
        save_preset(db, "Other", _box(), overwrite=False, current_preset_id=mine.preset_id)
    assert refused.value.existing.preset_id == other.preset_id


def test_hide_keeps_rows_and_unhide_refuses_a_taken_name(db):
    info = save_preset(db, "Session box", _box())
    hide_preset(db, info.preset_id)
    assert list_presets(db) == []
    assert [p.hidden for p in list_presets(db, include_hidden=True)] == [True]
    newer = save_preset(db, "Session box", _box())
    assert newer.preset_id != info.preset_id  # a new preset, not old history
    with pytest.raises(PresetError):
        hide_preset(db, info.preset_id, hidden=False)


def test_rename_refuses_a_clash(db):
    a = save_preset(db, "A", _box())
    save_preset(db, "B", _box())
    with pytest.raises(PresetError):
        rename_preset(db, a.preset_id, "B")
    renamed = rename_preset(db, a.preset_id, "  C  ")
    assert renamed.name == "C" and renamed.made_on == "StepLength"


def test_find_preset_by_name(db):
    info = save_preset(db, "Session box", _box())
    assert find_preset(db, "Session box").preset_id == info.preset_id
    assert find_preset(db, "nope") is None


@pytest.mark.parametrize("bad", ["", "   ", None, "x" * 121])
def test_bad_names_are_refused(bad):
    with pytest.raises(PresetError):
        check_preset_name(bad)


def test_presets_and_saved_plots_do_not_share_a_table(db):
    save_preset(db, "Fig", _box())
    save_plot(db, "StepLength", "Fig", _box())
    assert len(list_presets(db)) == 1
    assert len(list_saved_plots(db, "StepLength")) == 1


def test_a_damaged_row_still_lists_and_applies_on_defaults(db):
    from scistackplotdb.presets import ensure_table

    ensure_table(db)
    db._duck._execute(
        f"INSERT INTO {TABLE} VALUES ('bad1', 'Broken', 1, '2026-01-01', FALSE, 'not json')"
    )
    (info,) = list_presets(db)
    assert info.made_on is None
    applied = apply_preset_to(db, "bad1", "StepLength")
    assert applied.spec.measures == ["StepLength"]
    assert _paths(applied.notes, NoteKind.INVALID_VALUE) == [""]


# ---------------------------------------------------------------------------
# Applying against a real database
# ---------------------------------------------------------------------------


def _loader(db):
    source = ScidbSource(db)
    # The GUI's loader (plot_service._table_for): groupings travel too.
    return lambda spec: source.get_table(
        spec.variant_variables(),
        x_measure=spec.x_measure,
        factor_variables=list(spec.factor_variables),
    )


def test_apply_to_another_variable_keeps_its_data_and_never_writes(seeded):
    db = seeded
    info = save_preset(db, "Session box", _box(show_sample=["trial"]), made_on_shape="scalar")
    before = _stored(db, info.preset_id, 1)
    target = PlotSpec(
        measures=["Signal"],
        variant_sets=[VariantSet()],
        style=StyleOptions(title="Signal title"),
    )
    applied = apply_preset_to(db, info.preset_id, target, table_for=_loader(db))
    assert applied.spec.measures == ["Signal"]
    assert applied.spec.style.title == "Signal title"
    assert applied.spec.style.width == 7.2
    assert applied.spec.roles == _box().roles
    assert _stored(db, info.preset_id, 1) == before


def test_apply_to_a_coarser_variable_reports_what_it_lacks(seeded):
    """Mass is subject-level: no session or trial to group or collapse."""
    db = seeded
    info = save_preset(db, "Session box", _box())
    applied = apply_preset_to(db, info.preset_id, "Mass", table_for=_loader(db))
    assert set(_paths(applied.notes, NoteKind.NOT_IN_DATA)) >= {"roles.session", "roles.trial"}
    table = _loader(db)(applied.spec)
    assert str(applied.spec.kind) in capabilities(applied.spec, table)["available"]


def test_apply_with_a_table_and_a_variable_name_starts_from_a_fresh_panel(seeded):
    db = seeded
    info = save_preset(db, "Session box", _box())
    table = ScidbSource(db).get_table(["StepLength"])
    applied = apply_preset_to(db, info.preset_id, "StepLength", table=table)
    assert applied.notes == []
    assert applied.spec.kind is PlotKind.BOX
    assert len(applied.spec.variant_sets) == 1  # default_spec's opening pin


def test_an_own_column_grouping_the_target_lacks_is_dropped(with_gait):
    db = with_gait
    gait = PlotSpec(
        measures=["Gait"],
        roles={"Side": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["Side"],
        kind=PlotKind.BOX,
        factor_variables=[FactorVariable("Gait", "Side")],
    )
    info = save_preset(db, "By side", gait)
    applied = apply_preset_to(db, info.preset_id, "StepLength", table_for=_loader(db))
    assert "factor_variables[0]" in _paths(applied.notes, NoteKind.NOT_IN_DATA)
    assert applied.spec.factor_variables == []
