"""The project layer of display aliases, as a table carries it.

A table holds a live READER of the project's aliases (``LongTable.
aliases_source``), set by its source at build time — never a snapshot:
an edit must reach the next figure, yet the table is cached and must not be
rebuilt (or re-reduced) for a change of text. Stage 3 of
``.claude/plan-plot-text-sizes-and-aliases.md``.
"""

from __future__ import annotations

from scistackplot import Alias, DataFrameSource


class _ProjectSource(DataFrameSource):
    """A DataFrameSource with a project, the way ScidbSource has one."""

    def __init__(self, frame, project: dict, **kwargs):
        super().__init__(frame, **kwargs)
        self.project = project
        self.reads = 0

    def _aliases_source(self):
        def read():
            self.reads += 1
            return self.project

        return read


def test_a_source_without_a_project_has_no_project_aliases(scalar_frame):
    table = DataFrameSource(scalar_frame).get_table(["StepLength"])
    assert table.aliases_source is None
    assert table.project_aliases() == {}


def test_the_table_reads_the_project_live(scalar_frame):
    project = {"session": {"name": "Session", "levels": {"pre": "Before"}}}
    source = _ProjectSource(scalar_frame, project)
    table = source.get_table(["StepLength"])
    assert table.project_aliases() == {
        "session": Alias(name="Session", levels={"pre": "Before"})
    }

    # An edit to the project reaches the SAME cached table.
    project["session"] = {"name": "Visit"}
    assert source.get_table(["StepLength"]) is table
    assert table.project_aliases() == {"session": Alias(name="Visit")}


def test_the_cached_table_keeps_its_identity(scalar_frame):
    """The plan cache keys on id(table): a fresh copy per call would re-reduce
    for a change of text."""
    source = _ProjectSource(scalar_frame, {})
    assert source.get_table(["StepLength"]) is source.get_table(["StepLength"])


def test_a_failing_reader_costs_the_aliases_not_the_figure(scalar_frame):
    class Broken(_ProjectSource):
        def _aliases_source(self):
            def read():
                raise OSError("config vanished")

            return read

    table = Broken(scalar_frame, {}).get_table(["StepLength"])
    assert table.project_aliases() == {}


def test_derived_tables_keep_the_reader(scalar_frame):
    """Every derived table is `replace(table, …)`, so the reader travels."""
    from dataclasses import replace

    source = _ProjectSource(scalar_frame, {"subject": {"name": "Participant"}})
    table = source.get_table(["StepLength"])
    derived = replace(table, frame=table.frame.head(3))
    assert derived.project_aliases() == {"subject": Alias(name="Participant")}


def test_alias_round_trips_as_plain_data():
    alias = Alias(name="Session", levels={"01": "Visit 1"})
    assert alias.to_dict() == {"name": "Session", "levels": {"01": "Visit 1"}}
    assert Alias.from_dict(alias.to_dict()) == alias
    assert Alias.from_dict(None) == Alias()
    assert Alias().to_dict() == {}
    # Levels are text; an empty name is kept (the plot layer's "show raw").
    assert Alias.from_dict({"name": "", "levels": {1: 2}}) == Alias(name="", levels={"1": "2"})
