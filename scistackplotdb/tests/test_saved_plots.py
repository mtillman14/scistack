"""Saved plots: named, per-variable, append-only, never deleted.

``.claude/plan-saved-plots.md`` Stage 2. The spec's own drift handling is
``scistackplot/tests/test_restore.py``. This file is about the store: that a
save lands in the current format, that versions and hiding keep every row,
and that an old or damaged row still opens.
"""

from __future__ import annotations

import json
from dataclasses import fields

import pytest

from scistackplot import NoteKind, PlotKind, PlotSpec, Role, StyleOptions
from scistackplotdb import (
    ScidbSource,
    SavedPlotError,
    check_name,
    hide_saved_plot,
    list_saved_plots,
    load_saved_plot,
    rename_saved_plot,
    save_plot,
    saved_plot_history,
)
from scistackplotdb.saved import TABLE, _table_exists


def _spec(**changes) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BOX,
        style=StyleOptions(width=7.2, height=4.5, title="Step length"),
    )
    base.update(changes)
    return PlotSpec(**base)


VIEW = {"previewMode": "pane", "aspectChoice": "16:9", "figureIndex": 2}


def _stored_json(db, plot_id: str, version: int) -> str:
    row = db._duck._fetchone(
        f"SELECT envelope_json FROM {TABLE} WHERE plot_id = ? AND version = ?",
        [plot_id, version],
    )
    return row[0]


def _insert_raw(db, envelope_text: str, *, variable="StepLength", name="old") -> str:
    """A row as an older build (or a damaged file) might have left it."""
    from scistackplotdb.saved import ensure_table

    ensure_table(db)
    db._duck._execute(
        f"INSERT INTO {TABLE} VALUES (?, ?, ?, 1, '2026-01-01T00:00:00+00:00', FALSE, ?)",
        ["legacy0001", variable, name, envelope_text],
    )
    return "legacy0001"


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


def test_listing_an_untouched_database_does_not_create_the_table(db):
    assert list_saved_plots(db, "StepLength") == []
    assert not _table_exists(db)


def test_save_then_load_restores_spec_and_view(db):
    info = save_plot(db, "StepLength", "Fig 3", _spec(), VIEW)
    assert (info.name, info.version, info.hidden) == ("Fig 3", 1, False)

    opened = load_saved_plot(db, info.plot_id)
    assert opened.spec == _spec()
    assert opened.view == VIEW
    assert opened.notes == []
    assert opened.stored_format == 1


def test_a_dict_spec_is_read_strictly_and_stored_in_the_current_format(db):
    info = save_plot(db, "StepLength", "from dict", _spec().to_dict())
    envelope = json.loads(_stored_json(db, info.plot_id, 1))
    assert envelope["spec"] == _spec().to_dict()
    assert envelope["format"] == 1
    assert "scistackplot" in envelope["saved_with"]

    legacy = dict(_spec().to_dict(), x_layers=["session"])
    with pytest.raises(SavedPlotError):
        save_plot(db, "StepLength", "legacy", legacy)
    assert [p.name for p in list_saved_plots(db, "StepLength")] == ["from dict"]


def test_view_must_be_a_table(db):
    with pytest.raises(SavedPlotError):
        save_plot(db, "StepLength", "x", _spec(), view=["pane"])


# ---------------------------------------------------------------------------
# Versions, variables, hiding, renaming
# ---------------------------------------------------------------------------


def test_resaving_a_name_appends_a_version_and_keeps_the_old_one(db):
    first = save_plot(db, "StepLength", "Fig 3", _spec(kind=PlotKind.BOX))
    second = save_plot(db, "StepLength", "Fig 3", _spec(kind=PlotKind.VIOLIN))
    assert second.plot_id == first.plot_id
    assert second.version == 2

    listed = list_saved_plots(db, "StepLength")
    assert [(p.name, p.version) for p in listed] == [("Fig 3", 2)]
    assert [h.version for h in saved_plot_history(db, first.plot_id)] == [2, 1]
    assert load_saved_plot(db, first.plot_id).spec.kind is PlotKind.VIOLIN
    assert load_saved_plot(db, first.plot_id, version=1).spec.kind is PlotKind.BOX


def test_overwrite_false_refuses_only_another_plots_name(db):
    from scistackplotdb import SavedPlotExists

    a = save_plot(db, "StepLength", "A", _spec())
    b = save_plot(db, "StepLength", "B", _spec())

    # Saving the plot you have open under its own name: a new version, no question.
    again = save_plot(db, "StepLength", " A ", _spec(), overwrite=False, current_plot_id=a.plot_id)
    assert (again.plot_id, again.version) == (a.plot_id, 2)

    # Saving onto someone else's name: refused, naming the plot in the way.
    with pytest.raises(SavedPlotExists) as refused:
        save_plot(db, "StepLength", "B", _spec(), overwrite=False, current_plot_id=a.plot_id)
    assert refused.value.existing.plot_id == b.plot_id
    assert [h.version for h in saved_plot_history(db, b.plot_id)] == [1]

    # A brand-new name is never a question.
    fresh = save_plot(db, "StepLength", "C", _spec(), overwrite=False)
    assert fresh.version == 1


def test_plots_are_per_variable(db):
    save_plot(db, "StepLength", "Fig 3", _spec())
    save_plot(db, "Mass", "Fig 3", _spec(measures=["Mass"]))
    assert [p.variable for p in list_saved_plots(db, "StepLength")] == ["StepLength"]
    assert [p.variable for p in list_saved_plots(db, "Mass")] == ["Mass"]


def test_hiding_keeps_every_row_and_a_reused_name_starts_a_new_plot(db):
    old = save_plot(db, "StepLength", "Fig 3", _spec())
    save_plot(db, "StepLength", "Fig 3", _spec(kind=PlotKind.BAR))
    hide_saved_plot(db, old.plot_id)

    assert list_saved_plots(db, "StepLength") == []
    hidden = list_saved_plots(db, "StepLength", include_hidden=True)
    assert [(p.plot_id, p.hidden) for p in hidden] == [(old.plot_id, True)]
    assert len(saved_plot_history(db, old.plot_id)) == 2  # nothing deleted

    fresh = save_plot(db, "StepLength", "Fig 3", _spec())
    assert fresh.plot_id != old.plot_id
    assert fresh.version == 1

    with pytest.raises(SavedPlotError):
        hide_saved_plot(db, old.plot_id, hidden=False)  # two visible "Fig 3"s


def test_unhiding_brings_a_plot_back(db):
    info = save_plot(db, "StepLength", "Fig 3", _spec())
    hide_saved_plot(db, info.plot_id)
    hide_saved_plot(db, info.plot_id, hidden=False)
    assert [p.plot_id for p in list_saved_plots(db, "StepLength")] == [info.plot_id]


def test_rename_moves_every_version_and_refuses_a_clash(db):
    a = save_plot(db, "StepLength", "A", _spec())
    save_plot(db, "StepLength", "A", _spec(kind=PlotKind.BAR))
    save_plot(db, "StepLength", "B", _spec())

    with pytest.raises(SavedPlotError):
        rename_saved_plot(db, a.plot_id, "B")
    rename_saved_plot(db, a.plot_id, "A")  # its own name: fine

    renamed = rename_saved_plot(db, a.plot_id, "  Final   figure ")
    assert renamed.name == "Final figure"
    assert {h.name for h in saved_plot_history(db, a.plot_id)} == {"Final figure"}
    # The next save under the new name continues the same plot.
    assert save_plot(db, "StepLength", "Final figure", _spec()).plot_id == a.plot_id


@pytest.mark.parametrize("bad", ["", "   ", None, 3, "x" * 121])
def test_bad_names_are_refused(bad):
    with pytest.raises(SavedPlotError):
        check_name(bad)


def test_names_are_trimmed():
    assert check_name("  Fig\t3\n ") == "Fig 3"


def test_opening_a_plot_that_does_not_exist(db):
    with pytest.raises(SavedPlotError):
        load_saved_plot(db, "nope")
    save_plot(db, "StepLength", "Fig 3", _spec())
    with pytest.raises(SavedPlotError):
        load_saved_plot(db, "nope")


# ---------------------------------------------------------------------------
# Old and damaged rows
# ---------------------------------------------------------------------------


def _drifted_envelope() -> str:
    spec = _spec().to_dict()
    spec["kind"] = "pie"
    spec["style"]["widht"] = 3
    spec["x_layers"] = ["session"]
    return json.dumps({"format": 0, "spec": spec, "view": VIEW})


def test_a_drifted_row_opens_with_notes_and_is_not_rewritten(db):
    text = _drifted_envelope()
    plot_id = _insert_raw(db, text)

    opened = load_saved_plot(db, plot_id)
    assert opened.spec.kind is PlotKind.SCATTER
    assert opened.spec.style == _spec().style
    assert sorted(n.path for n in opened.notes) == ["kind", "style.widht", "x_layers"]
    assert opened.view == VIEW
    assert opened.stored_format == 0
    assert _stored_json(db, plot_id, 1) == text  # opening never writes


def test_one_resave_of_a_drifted_row_clears_its_notes(db):
    plot_id = _insert_raw(db, _drifted_envelope())
    opened = load_saved_plot(db, plot_id)
    save_plot(db, opened.info.variable, opened.info.name, opened.spec, opened.view)

    again = load_saved_plot(db, plot_id)
    assert again.info.version == 2
    assert again.notes == []
    stored = json.loads(_stored_json(db, plot_id, 2))
    assert set(stored["spec"]) <= {f.name for f in fields(PlotSpec)}


@pytest.mark.parametrize(
    "text", ["not json", "[1, 2]", json.dumps({"spec": "gone"}), json.dumps({})]
)
def test_a_damaged_row_opens_on_defaults_for_its_variable(db, text):
    plot_id = _insert_raw(db, text)
    opened = load_saved_plot(db, plot_id)
    assert opened.spec.measures == ["StepLength"]
    assert opened.view == {}
    assert opened.notes


def test_a_bad_view_opens_with_the_panel_defaults(db):
    plot_id = _insert_raw(
        db, json.dumps({"format": 1, "spec": _spec().to_dict(), "view": "pane"})
    )
    opened = load_saved_plot(db, plot_id)
    assert opened.view == {}
    assert opened.spec == _spec()


# ---------------------------------------------------------------------------
# Reconciling with today's data
# ---------------------------------------------------------------------------


def test_a_table_reconciles_the_spec_with_todays_data(seeded):
    db = seeded
    spec = _spec(show_sample=["trial", "cycle"])
    info = save_plot(db, "StepLength", "Fig 3", spec)
    table = ScidbSource(db).get_table(["StepLength"])

    without = load_saved_plot(db, info.plot_id)
    assert without.notes == [] and without.spec == spec

    opened = load_saved_plot(db, info.plot_id, table=table)
    assert opened.spec.show_sample == ["trial"]
    assert [(n.path, n.kind) for n in opened.notes] == [
        ("show_sample", NoteKind.NOT_IN_DATA)
    ]
    # Reconciling is a view of the stored copy, never a write to it.
    assert json.loads(_stored_json(db, info.plot_id, 1))["spec"] == spec.to_dict()


def test_table_for_loads_from_the_restored_spec(seeded):
    db = seeded
    info = save_plot(db, "StepLength", "Fig 3", _spec(show_sample=["cycle"]))
    source = ScidbSource(db)
    asked = []

    def table_for(spec):
        asked.append(spec)
        return source.get_table(spec.variant_variables())

    opened = load_saved_plot(db, info.plot_id, table_for=table_for)
    assert [s.show_sample for s in asked] == [["cycle"]]  # the RESTORED spec
    assert opened.spec.show_sample == []
    assert [n.path for n in opened.notes] == ["show_sample"]


def test_a_failing_table_loader_opens_unreconciled_with_a_note(db):
    info = save_plot(db, "StepLength", "Fig 3", _spec(show_sample=["cycle"]))

    def table_for(spec):
        raise KeyError("StepLength")

    opened = load_saved_plot(db, info.plot_id, table_for=table_for)
    assert opened.spec.show_sample == ["cycle"]  # not reconciled, not lost
    assert [(n.path, n.kind) for n in opened.notes] == [("", NoteKind.NOT_IN_DATA)]
