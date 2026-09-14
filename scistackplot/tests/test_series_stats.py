"""``series_stats``: the numpy per-position reductions, against the one-shot pad.

The blocked form exists because the first real run padded 4,190 cells to the
longest one (325,855 samples, mean 41,565) and asked for 10.2 GiB. These pin
that blocking changes the working set and nothing else — every statistic equals
what one big padded array gives — and that a block only ever holds the cells
that reach it.
"""

from __future__ import annotations

import numpy as np
import pytest
from scistackplot.series_stats import (
    PAD_BUDGET,
    blocks,
    cell_arrays,
    explode,
    pad,
    position_mean,
    position_stats,
)
from scistackplot.spec import ErrorBand, Statistic


@pytest.fixture
def ragged():
    """Lengths 3..1000 (several blocks of 256) with NaNs sprinkled in, an all-NaN
    cell, and an empty one."""
    rng = np.random.default_rng(11)
    cells = []
    for n in (300, 3, 1000, 600, 7, 1000, 450):
        a = rng.normal(size=n)
        a[rng.integers(0, n, size=2)] = np.nan
        cells.append(a)
    cells.append(np.full(9, np.nan))
    cells.append(np.empty(0))
    return cells


def _reference(cells, statistic, error):
    """The one-shot pad, reduced along axis 0 — what the blocked form replaced."""
    import warnings

    padded = pad(cells)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        count = np.sum(~np.isnan(padded), axis=0)
        centre = np.nanmedian(padded, 0) if statistic is Statistic.MEDIAN else np.nanmean(padded, 0)
        if error is ErrorBand.NONE:
            low = high = centre
        elif error is ErrorBand.IQR:
            low, high = np.nanpercentile(padded, 25, 0), np.nanpercentile(padded, 75, 0)
        else:
            sd = np.where(count >= 2, np.nanstd(padded, 0, ddof=1), 0.0)
            n = np.where(count > 0, count, 1)
            k = {ErrorBand.SD: 1.0, ErrorBand.SEM: None, ErrorBand.CI95: 1.96}[error]
            spread = sd if error is ErrorBand.SD else (1.0 if k is None else k) * sd / np.sqrt(n)
            low, high = centre - spread, centre + spread
    return centre, low, high, count


CASES = [
    (Statistic.MEAN, ErrorBand.SD),
    (Statistic.MEAN, ErrorBand.SEM),
    (Statistic.MEAN, ErrorBand.CI95),
    (Statistic.MEAN, ErrorBand.NONE),
    (Statistic.MEDIAN, ErrorBand.IQR),
    (Statistic.MEDIAN, ErrorBand.SD),
]


@pytest.mark.parametrize("statistic,error", CASES, ids=[f"{s}-{e}" for s, e in CASES])
@pytest.mark.parametrize("budget", [PAD_BUDGET, 50, 1], ids=["default", "tiny", "one"])
def test_blocked_stats_equal_the_one_shot_pad(ragged, statistic, error, budget, monkeypatch):
    import scistackplot.series_stats as mod

    monkeypatch.setattr(mod, "PAD_BUDGET", budget)
    got = position_stats(ragged, statistic, error)
    want = _reference(ragged, statistic, error)
    for g, w in zip(got, want, strict=True):
        np.testing.assert_allclose(g, w, equal_nan=True)


@pytest.mark.parametrize("budget", [PAD_BUDGET, 50, 1], ids=["default", "tiny", "one"])
def test_blocked_mean_equals_the_one_shot_pad(ragged, budget, monkeypatch):
    import warnings

    import scistackplot.series_stats as mod

    monkeypatch.setattr(mod, "PAD_BUDGET", budget)
    mean, count = position_mean(ragged)
    padded = pad(ragged)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        np.testing.assert_allclose(mean, np.nanmean(padded, 0), equal_nan=True)
    np.testing.assert_array_equal(count, np.sum(~np.isnan(padded), 0))


class TestBlocks:
    def test_a_block_holds_only_the_cells_that_reach_it(self, ragged):
        lengths = sorted(a.size for a in ragged)
        for start, padded in blocks(ragged, budget=50):
            reaching = sum(1 for n in lengths if n > start)
            assert padded.shape[0] == reaching, (start, padded.shape)

    def test_the_working_set_respects_the_budget(self, ragged):
        for _, padded in blocks(ragged, budget=64):
            # 256 positions is the floor even when the budget is smaller than
            # that; above it a block never exceeds the budget.
            assert padded.size <= max(64, 256 * padded.shape[0])

    def test_blocks_cover_every_position_once(self, ragged):
        width = max(a.size for a in ragged)
        covered = []
        for start, padded in blocks(ragged, budget=50):
            covered.extend(range(start, start + padded.shape[1]))
        assert covered == list(range(width))

    def test_a_ragged_group_never_pads_to_the_longest_cell(self):
        """The 10 GiB case in miniature: one long cell among many short ones."""
        cells = [np.ones(5)] * 100 + [np.ones(10_000)]
        biggest = max(padded.size for _, padded in blocks(cells, budget=2_000))
        assert biggest <= max(2_000, 256 * 101)
        # …and the long tail is reduced by that one cell alone.
        tail = [padded.shape[0] for start, padded in blocks(cells, budget=2_000) if start >= 5]
        assert tail and all(n == 1 for n in tail)

    def test_empty_input(self):
        assert list(blocks([])) == []
        assert list(blocks([np.empty(0)])) == []


class TestExplode:
    def test_matches_a_hand_built_explode(self):
        cells = [np.array([1.0, np.nan, 3.0]), np.array([]), np.array([4.0])]
        row, pos, val, total, stride = explode(cells)
        assert total == 3 and stride == 1
        np.testing.assert_array_equal(row, [0, 0, 2])
        np.testing.assert_array_equal(pos, [0, 2, 0])  # the NaN leaves a gap
        np.testing.assert_array_equal(val, [1.0, 3.0, 4.0])

    def test_a_budget_strides_the_kept_rows(self):
        cells = [np.arange(10.0), np.arange(10.0)]
        row, pos, val, total, stride = explode(cells, max_points=5)
        assert total == 20 and stride == 4
        np.testing.assert_array_equal(val, [0.0, 4.0, 8.0, 2.0, 6.0])

    def test_cell_arrays_normalises_every_cell_kind(self):
        out = cell_arrays([np.array([1, 2]), [3.0, 4.0], None, 5.0, np.array(7.0)])
        assert [a.tolist() for a in out] == [[1.0, 2.0], [3.0, 4.0], [], [], []]
