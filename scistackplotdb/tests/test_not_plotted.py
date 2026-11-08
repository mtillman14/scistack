""""Not plotted" from the database: "No record" locations (2026-11-01).

`ScidbSource` attaches `LongTable.absent` — the variable's schema locations
the dataset has (some `_schema` row) where it has no record — from
`scidb.provenance.inventory.absent_locations`, the owner; `scistackplot.reduce.omissions`
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


def test_nans_inside_saved_vectors_are_listed_where_scidb_nans_lists_them(db):
    """The user's incident: per-minute values packed into one vector, some
    NaN. A bar averages each vector first; "Not plotted" lists the records
    averaged over fewer elements, at the locations ``scidb nans`` reports,
    because both read cells with one reader (scicanonicalhash._cells)."""
    import numpy as np
    from conftest import Signal

    from scidb.inspect.cells import cell_profile
    from scistackplot.reduce.omissions import NAN_ELEMENTS

    Signal.save(np.array([1.0, np.nan, 3.0, 4.0]), subject="01", session="pre", trial="1")
    Signal.save(np.array([1.0, 2.0, 3.0, 4.0]), subject="01", session="pre", trial="2")
    Signal.save(np.array([np.nan, np.nan, 5.0, 6.0]), subject="02", session="pre", trial="1")
    Signal.save(np.array([2.0, 2.5, 3.0, 3.5]), subject="02", session="pre", trial="2")

    table = ScidbSource(db).get_table(["Signal"])
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.BAR,
    )
    report = resolve(spec, table)[0].omissions
    items = sorted(
        (i for i in report.items if i.reason == NAN_ELEMENTS), key=lambda i: i.keys["subject"]
    )
    assert [i.keys for i in items] == [
        {"subject": "01", "session": "pre", "trial": "1"},
        {"subject": "02", "session": "pre", "trial": "1"},
    ]
    assert items[1].detail == "2 of 4 elements NaN; the mean of the other 2"

    inspected = cell_profile(db, "Signal")
    assert sorted((p.location["subject"], p.location["trial"]) for p in inspected.problems) == [
        ("01", "1"), ("02", "1"),
    ]
