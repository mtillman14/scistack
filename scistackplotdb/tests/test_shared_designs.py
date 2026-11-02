"""The shared design of a linked figure + table: one owner
(docs/claude/studio-unification.md, Stage 6).

A saved plot and the StatStudio tables linked to it read and write ONE
design row set. Display settings stay with each artifact.
"""

from __future__ import annotations

from types import SimpleNamespace

from scistackplot import PlotKind, PlotSpec, Role, StyleOptions
from scistackplotdb import apply_design, is_linked, link_design, load_design, save_design
from scistackplotdb.designs import STORE, TABLE

PLOT = "p" * 32


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


def test_an_unlinked_plot_has_no_design_and_applying_changes_nothing(db):
    assert not is_linked(db, PLOT)
    assert load_design(db, PLOT) is None
    spec = _spec()
    merged, notes, version = apply_design(db, PLOT, spec)
    assert merged == spec and notes == [] and version is None


def test_linking_stores_the_design_once(db):
    first = link_design(db, PLOT, _spec())
    assert first.version == 1 and is_linked(db, PLOT)
    again = link_design(db, PLOT, _spec(roles={"session": Role.ITERATE, "subject": Role.COLLAPSE,
                                               "trial": Role.COLLAPSE}, groups=[]))
    assert again.version == 1, "an already-linked plot keeps its stored design"


def test_a_display_only_change_appends_nothing(db):
    link_design(db, PLOT, _spec())
    stored, changed = save_design(db, PLOT, _spec(kind=PlotKind.BAR, style=StyleOptions(width=3.0, height=2.0)))
    assert not changed and stored.version == 1


def test_a_design_change_appends_a_version_and_both_sides_read_it(db):
    link_design(db, PLOT, _spec())
    regrouped = _spec(roles={"session": Role.ITERATE, "subject": Role.COLLAPSE, "trial": Role.GROUP},
                      groups=["trial"])
    stored, changed = save_design(db, PLOT, regrouped)
    assert changed and stored.version == 2

    # The other studio's spec (a bar chart with its own styling) takes the
    # design and keeps its display settings.
    other = _spec(kind=PlotKind.BAR, style=StyleOptions(width=3.0, height=2.0, title="Mine"))
    merged, _, version = apply_design(db, PLOT, other)
    assert version == 2
    assert merged.roles == regrouped.roles and list(merged.groups) == ["trial"]
    assert merged.kind is PlotKind.BAR and merged.style.title == "Mine"


def test_nothing_is_ever_deleted(db):
    link_design(db, PLOT, _spec())
    save_design(db, PLOT, _spec(groups=[], roles={"session": Role.ITERATE, "subject": Role.COLLAPSE,
                                                  "trial": Role.COLLAPSE}))
    rows = db._duck.fetchall(f"SELECT version FROM {TABLE} ORDER BY version")
    assert [r[0] for r in rows] == [1, 2]


def test_the_plots_bundle_section_carries_designs(db, tmp_path):
    from scidb import configure_database

    from scistackplotdb.bundle_section import PlotsSection

    link_design(db, PLOT, _spec())
    files = PlotsSection().export(SimpleNamespace(db=db))
    assert "designs.json" in files
    other = configure_database(tmp_path / "other.duckdb", ["subject", "session", "trial"])
    try:
        report = PlotsSection().import_(SimpleNamespace(db=other), files)
        assert report[TABLE] == 1
        assert STORE.dump_rows(other) == STORE.dump_rows(db)
    finally:
        other.close()
