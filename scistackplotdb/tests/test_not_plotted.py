""""Not plotted" from the database: "No record" locations (2026-11-01).

`ScidbSource` attaches `LongTable.absent` — the variable's schema locations
the dataset has (some `_schema` row) where it has no record — from
`scidb.inventory.absent_locations`, the owner; `scistackplot.reduce.omissions`
reports them per figure.
"""

from __future__ import annotations

from scistackplot import PlotKind, PlotSpec, Role, resolve
from scistackplot.reduce.omissions import NO_RECORD
from scistackplotdb import ScidbSource


def test_a_location_another_variable_holds_is_no_record(seeded):
    # Another variable saved at a trial StepLength never reached.
    from conftest import Signal
    import numpy as np

    Signal.save(np.zeros(8), subject="01", session="pre", trial="3")
    table = ScidbSource(seeded).get_table(["StepLength"])
    assert table.absent is not None
    assert [tuple(r) for r in table.absent.itertuples(index=False)] == [("01", "pre", "3")]
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.BAR,
    )
    report = resolve(spec, table)[0].omissions
    items = [i for i in report.items if i.reason == NO_RECORD]
    assert [i.keys for i in items] == [{"subject": "01", "session": "pre", "trial": "3"}]
    assert items[0].variable == "StepLength"


def test_every_location_holding_a_record_is_not_listed(seeded):
    table = ScidbSource(seeded).get_table(["StepLength"])
    # The fixture saved StepLength at every trial any variable holds.
    assert table.absent is not None and table.absent.empty
