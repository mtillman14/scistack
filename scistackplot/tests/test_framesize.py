"""Frame size in cells and samples, not just rows.

The unit matters: 4190 rows of a 200-sample array and 4190 rows of a
250,000-sample array are the same row count and four orders of magnitude apart
in work. Every load/reduce log line reported only rows, which on 2026-09-13 made
a plot that never returned indistinguishable from a plot over a small table
(.claude/plot-at-scale-plan.md §1).
"""

import numpy as np
import pandas as pd

from scistackplot.framesize import (
    NUMPY_BYTES_PER_SAMPLE,
    PYLIST_BYTES_PER_SAMPLE,
    cell_samples,
    format_extent,
    frame_extent,
)


class TestCellSamples:
    def test_sequence_length(self):
        assert cell_samples([1.0, 2.0, 3.0]) == 3
        assert cell_samples(np.zeros(7)) == 7

    def test_scalar_counts_as_one(self):
        assert cell_samples(3.5) == 1
        assert cell_samples(np.float64(3.5)) == 1

    def test_none_and_strings_count_as_nothing(self):
        # A string's length is a character count, which would be a sample count
        # that is not one — worse than reporting zero.
        assert cell_samples(None) == 0
        assert cell_samples("abc") == 0
        assert cell_samples(b"abc") == 0


class TestFrameExtent:
    def test_counts_rows_cells_and_samples_separately(self):
        frame = pd.DataFrame(
            {
                "EMG": [[1.0] * 100, [1.0] * 300],
                "subject": ["s1", "s2"],
            }
        )
        extent = frame_extent(frame, ["EMG"])
        assert extent["rows"] == 2
        assert extent["cells"] == 2
        assert extent["samples"] == 400

    def test_row_count_alone_cannot_distinguish_these(self):
        """The whole reason this module exists."""
        small = pd.DataFrame({"y": [[0.0] * 10] * 4})
        large = pd.DataFrame({"y": [[0.0] * 100_000] * 4})
        assert len(small) == len(large)
        assert frame_extent(small, ["y"])["samples"] == 40
        assert frame_extent(large, ["y"])["samples"] == 400_000

    def test_only_named_columns_are_scanned(self):
        frame = pd.DataFrame({"y": [[0.0] * 10], "other": [[0.0] * 9999]})
        assert frame_extent(frame, ["y"])["samples"] == 10

    def test_missing_columns_are_ignored_not_raised(self):
        # Callers pass measure names that a filtered frame may not carry.
        frame = pd.DataFrame({"y": [[0.0] * 5]})
        assert frame_extent(frame, ["y", "absent"])["samples"] == 5

    def test_none_cells_skipped(self):
        frame = pd.DataFrame({"y": [[0.0] * 5, None]})
        extent = frame_extent(frame, ["y"])
        assert extent["rows"] == 2
        assert extent["cells"] == 1
        assert extent["samples"] == 5

    def test_boxed_flag_distinguishes_list_from_ndarray(self):
        """The flag that says whether an Arrow fetch path would help."""
        boxed = frame_extent(pd.DataFrame({"y": [[0.0] * 4]}), ["y"])
        assert boxed["boxed"] is True
        assert boxed["est_bytes"] == 4 * PYLIST_BYTES_PER_SAMPLE

        unboxed = frame_extent(pd.DataFrame({"y": [np.zeros(4)]}), ["y"])
        assert unboxed["boxed"] is False
        assert unboxed["est_bytes"] == 4 * NUMPY_BYTES_PER_SAMPLE

    def test_scalar_column_is_one_sample_per_row(self):
        frame = pd.DataFrame({"y": [1.0, 2.0, 3.0]})
        extent = frame_extent(frame, ["y"])
        assert extent["cells"] == 3
        assert extent["samples"] == 3

    def test_empty_frame(self):
        extent = frame_extent(pd.DataFrame({"y": []}), ["y"])
        assert extent["rows"] == 0
        assert extent["samples"] == 0
        assert extent["est_bytes"] == 0


class TestFormatExtent:
    def test_reports_rows_cells_samples_and_boxing(self):
        text = format_extent(frame_extent(pd.DataFrame({"y": [[0.0] * 100]}), ["y"]))
        assert "rows=1" in text
        assert "cells=1" in text
        assert "samples=100" in text
        assert "boxed" in text

    def test_large_frames_report_gigabytes(self):
        # 4190 cells x 250k samples, the shape that could not be plotted.
        extent = {
            "rows": 4190,
            "cells": 4190,
            "samples": 4190 * 250_000,
            "est_bytes": 4190 * 250_000 * PYLIST_BYTES_PER_SAMPLE,
            "boxed": True,
        }
        text = format_extent(extent)
        assert "GB" in text
        assert "boxed" in text

    def test_ndarray_frames_say_so(self):
        text = format_extent(frame_extent(pd.DataFrame({"y": [np.zeros(8)]}), ["y"]))
        assert "ndarray" in text
        assert "boxed" not in text
