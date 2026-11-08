"""scicanonicalhash._cells: the one reader of what is inside a cell.

scidb (``scidb nans``, the data preview) and scistackplot (1-D plots, the
cell collapse, "Not plotted") both read cells through it; before
2026-11-08 they had their own readers and disagreed on complex, 0-d and
text arrays. EDGE_CASES are those disagreements, now decided.
"""

import math

import numpy as np
import pytest

from scicanonicalhash._cells import (
    ALL_NAN,
    BLANK,
    CONSTANT,
    EDGE_CASES,
    EMPTY,
    PARTIAL_NAN,
    PROBLEM_KINDS,
    PROBLEM_LABELS,
    cell_stats,
    classify,
    is_blank,
    nan_count,
    numeric_elements,
)


def _same(got, want) -> bool:
    if want is None or got is None:
        return got is None and want is None
    return len(got) == len(want) and all(
        (math.isnan(a) and math.isnan(b)) or a == b for a, b in zip(got.tolist(), want)
    )


@pytest.mark.parametrize("name, value, want", EDGE_CASES, ids=[c[0] for c in EDGE_CASES])
def test_the_reading_rule(name, value, want):
    assert _same(numeric_elements(value), want), (name, numeric_elements(value))


class TestClassify:
    @pytest.mark.parametrize(
        "value, kind",
        [
            (np.array([1.0, np.nan, 3.0]), PARTIAL_NAN),
            (np.array([np.nan, np.nan]), ALL_NAN),
            (np.array([2.0, 2.0, 2.0]), CONSTANT),
            (np.array([], dtype=float), EMPTY),
            ([1, 2, 3], None),
            ([1.0, None, 3.0], PARTIAL_NAN),
            (np.nan, BLANK),
            (None, BLANK),
            (np.array(np.nan), BLANK),  # a 0-d NaN is a missing scalar
            (np.array(7.0), None),  # a 0-d number is a scalar, not an array
            (5.0, None),
            ("text", None),
            (np.array([7.0]), None),  # one value is not "constant"
            (np.array([[1.0, np.nan], [3.0, 4.0]]), PARTIAL_NAN),
            (np.array([1 + 2j, np.nan]), None),  # complex: not numeric
        ],
    )
    def test_kinds(self, value, kind):
        assert classify(value)[0] == kind

    def test_counts(self):
        assert classify(np.array([1.0, np.nan, np.nan, 4.0])) == (PARTIAL_NAN, 4, 2)
        assert nan_count(np.array([1.0, np.nan])) == (1, 2)

    def test_pandas_missing_is_blank(self):
        pd = pytest.importorskip("pandas")
        assert is_blank(pd.NA) and is_blank(pd.NaT)
        assert not is_blank(np.array([np.nan]))  # an array, not a scalar

    def test_stats_and_wording(self):
        stats = cell_stats([np.array([1.0, np.nan]), np.array([np.nan, np.nan]), 3.0, np.nan])
        assert stats.cells == 4 and stats.array_cells == 2
        assert stats.elements == 4 and stats.nan_elements == 3
        assert stats.kinds[PARTIAL_NAN] == 1 and stats.kinds[ALL_NAN] == 1 and stats.kinds[BLANK] == 1
        text = stats.describe()
        assert "3 of 4 values are NaN (75.0%)" in text and "1 all NaN" in text

    def test_every_kind_has_a_label(self):
        assert set(PROBLEM_LABELS) == set(PROBLEM_KINDS)


def test_it_is_not_public():
    """Private: not exported, and scicanonicalhash/__init__ never imports it."""
    import scicanonicalhash

    assert "_cells" not in scicanonicalhash.__all__
