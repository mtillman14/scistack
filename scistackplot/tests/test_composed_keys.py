"""Composing ``__series`` and nested-x keys once per COMBINATION, not per row.

A 1-D measure explodes: 24 rows of EMG become 8 906 400 samples, all of which
carry the same 24 answers to "which trace is this?". Asking that question per
row — ``df[cols].astype(str).agg(" | ".join, axis=1)``, a Python call for every
sample — is what made a full-resolution figure take ~770s while the identical
interactive figure took 3.8s (scidb.log 2026-09-11). The interactive path never
paid it because `_downsample` runs before the panels are built.

Two things have to hold, and both are tested here:

* the strings are **exactly** what the join produced — they are identities that
  group polylines, so a changed string silently re-partitions a figure;
* the work scales with the number of DISTINCT combinations, not with rows.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, resolve
from scistackplot.reduce import SERIES_SEPARATOR, _composed_key
from scistackplot.resolved import SERIES
from scistackplot.xaxis import LEAF_SEPARATOR, leaf_key


def _naive(frame: pd.DataFrame, columns: list[str], separator: str) -> list[str]:
    """The implementation being replaced, kept as the reference answer."""
    if not columns or frame.empty:
        return [""] * len(frame)
    return list(frame[columns].astype(str).agg(separator.join, axis=1))


# --- the strings are unchanged ---------------------------------------------


@pytest.mark.parametrize(
    "frame",
    [
        pd.DataFrame({"a": ["01", "02", "01"], "b": ["pre", "pre", "post"]}),
        # One column: the key is just that value, with no separator anywhere.
        pd.DataFrame({"a": ["x", "y", "x", "y"]}),
        # Mixed dtypes: the old form stringified each column independently.
        pd.DataFrame({"a": [1, 2, 2], "b": [1.5, 1.5, 2.0], "c": [True, False, True]}),
        # Repeats in every column but distinct combinations — the case the
        # codes have to keep apart.
        pd.DataFrame({"a": ["p", "p", "q", "q"], "b": ["1", "2", "1", "2"]}),
        pd.DataFrame({"a": [], "b": []}),
    ],
)
def test_composed_key_matches_the_join_it_replaces(frame):
    columns = list(frame.columns)

    composed = list(_composed_key(frame, columns, SERIES_SEPARATOR))

    assert composed == _naive(frame, columns, SERIES_SEPARATOR)


def test_no_columns_is_the_empty_key():
    frame = pd.DataFrame({"a": [1, 2, 3]})
    assert list(_composed_key(frame, [], SERIES_SEPARATOR)) == ["", "", ""]


def test_composed_key_agrees_with_leaf_key_for_the_nested_axis():
    """The nested-x path composes the SAME key `xaxis.leaf_key` builds, and the
    two must not drift: `plan_x_axis` matches panel frames on it."""
    frame = pd.DataFrame({"limb": ["L", "L", "R"], "session": ["pre", "post", "pre"]})

    composed = list(_composed_key(frame, ["limb", "session"], LEAF_SEPARATOR))

    assert composed == [
        leaf_key(values) for values in zip(frame["limb"], frame["session"])
    ]


def test_a_missing_level_gets_text_instead_of_raising():
    """The one place the new form deliberately differs from the old one.

    pandas 3's `astype(str)` PRESERVES missing values rather than writing
    "nan", so ``df[cols].astype(str).agg(" | ".join, axis=1)`` raised
    ``TypeError: sequence item: expected str instance, NAType found`` on any
    factor column with a gap — a figure that died on real data rather than
    drawing it. A missing level is a level.
    """
    frame = pd.DataFrame({"a": ["01", None, "01"], "b": [1.0, np.nan, np.nan]})

    # Precondition for the docstring above, asserted rather than assumed: a
    # missing value is still missing after `astype(str)`, so there was never a
    # string for `" | ".join` to join.
    #
    # This is a pandas 3 fact. pandas 2's `astype(str)` writes the text "nan",
    # so the TypeError cannot arise there and the precondition is simply false —
    # assert it where it applies rather than everywhere. The composed output
    # below is the same on both: pandas 3 reaches "nan" via the implementation's
    # `fillna(MISSING_LEVEL_TEXT)`, pandas 2 was already there.
    if int(pd.__version__.split(".")[0]) >= 3:
        assert frame["b"].astype(str).isna().any()

    composed = list(_composed_key(frame, ["a", "b"], SERIES_SEPARATOR))

    assert composed == ["01 | 1.0", "nan | nan", "01 | nan"]


def test_a_separator_inside_a_level_composes_as_it_always_did():
    """A level whose text contains the separator can collide with a different
    combination — two traces drawn as one line.

    Inherited, not introduced: the join this replaces produced the identical
    collision, and no separator choice rules it out. Pinned here so the day it
    matters, it is a known property rather than a discovery.
    """
    frame = pd.DataFrame({"a": ["x | y", "x"], "b": ["z", "y | z"]})

    composed = list(_composed_key(frame, ["a", "b"], SERIES_SEPARATOR))

    assert composed == _naive(frame, ["a", "b"], SERIES_SEPARATOR)
    assert composed == ["x | y | z", "x | y | z"]


# --- and the cost scales with combinations, not rows ------------------------


class CountingLevel:
    """A factor level that records every time something asks for its text.

    Deliberately NOT a `str` subclass: pandas 3 infers its own string dtype for
    anything that is a `str`, which would quietly replace these with plain
    strings and leave the budget below passing no matter what the code does.
    An arbitrary object stays in an object column, where `astype(str)` — the
    form this test exists to keep out — is a `__str__` call per row.
    """

    calls = 0

    def __init__(self, text: str) -> None:
        self.text = text

    def __str__(self) -> str:
        CountingLevel.calls += 1
        return self.text

    # Never counted: a debugger or an assertion message must not move the number.
    def __repr__(self) -> str:
        return f"CountingLevel({self.text!r})"

    def __hash__(self) -> int:
        return hash(self.text)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, CountingLevel) and other.text == self.text


def test_series_keys_are_built_per_combination_not_per_row():
    """The regression guard, stated as a budget rather than a stopwatch.

    Four traces of 500 samples: the composition may stringify a LEVEL a handful
    of times, and must not stringify a row. The previous form called `str` 2 000
    times here — and 8.9 million times on the data that prompted this.
    """
    samples = 500
    frame = pd.DataFrame(
        {
            "subject": pd.Series(
                [CountingLevel(t) for t in ("01", "01", "02", "02")], dtype=object
            ),
            "trial": pd.Series(
                [CountingLevel(t) for t in ("1", "2", "1", "2")], dtype=object
            ),
            "Signal": [list(np.linspace(0.0, 1.0, samples)) for _ in range(4)],
        }
    )
    table = LongTable.from_frame(
        frame,
        factors=["subject", "trial"],
        measures=["Signal"],
        name="Signal",
        schema_levels=["subject", "trial"],
    )
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "subject"],
        color="subject",
        kind=PlotKind.LINE,
    )

    # Precondition: if pandas ever converts these away, the budget stops
    # measuring anything and has to say so rather than pass.
    assert isinstance(table.frame["subject"].iloc[0], CountingLevel)

    CountingLevel.calls = 0
    figures = resolve(spec, table)

    rows = figures[0].row_count
    assert rows == 4 * samples, "precondition: the measure exploded"
    # Four traces, named exactly as before.
    assert set(figures[0].panels[0].frame[SERIES]) == {
        "01 | 1",
        "01 | 2",
        "02 | 1",
        "02 | 2",
    }
    # A generous ceiling: the point is the ORDER of magnitude. Anything per-row
    # lands at >= 2000 here, and at 8.9 million on the user's data.
    assert CountingLevel.calls < rows // 10, (
        f"{CountingLevel.calls} str() calls for {rows} rows — something is "
        "asking each ROW for its text again"
    )
