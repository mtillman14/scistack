"""Tests for @scistack(generates_file=True) — side-effect function tracking via for_each.

generates_file functions produce a file as a side effect (a plot, report, etc.)
rather than returning data to store. for_each records them in the provenance graph
(lineage-only — no data table row) and auto-injects the combo's metadata kwargs so
the function can build its output path.

(The former manual ``r = fn(x); save(Figure, r)`` pattern and the @lineage_fcn
rerun cache were removed with the @lineage_fcn → @scistack migration; generates_file
now lives only on the for_each path. See .claude/remove-lineage-fcn.md.)
"""

from functools import partial

import numpy as np

from scidb import BaseVariable, scistack
from scidb import for_each as _for_each

# skip_computed=True: these tests cover the skip-already-computed path.
for_each = partial(_for_each, skip_computed=True)

# --- Variable classes for testing ---


class RawSignal(BaseVariable):
    """Raw input data."""

    schema_version = 1


class Figure(BaseVariable):
    """Represents a generated file (plot, report, etc.)."""

    schema_version = 1


# --- for_each integration tests ---


class TestForEachIntegration:
    """Tests for_each with generates_file functions."""

    def test_for_each_passes_metadata_to_generates_file_fn(self, db):
        """generates_file=True function auto-receives metadata kwargs in for_each."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")

        received_kwargs = {}

        @scistack(generates_file=True)
        def make_plot(data, subject, session):
            nonlocal received_kwargs
            received_kwargs = {"subject": subject, "session": session}
            return None

        for_each(
            make_plot,
            inputs={"data": RawSignal},
            outputs=[Figure],
            subject=[1],
            session=["A"],
        )

        assert received_kwargs == {"subject": "1", "session": "A"}

    def test_for_each_cache_hit_on_second_run(self, db):
        """Second for_each run should skip_computed for all iterations."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")
        RawSignal.save(np.array([4, 5, 6]), subject=1, session="B")

        call_count = 0

        @scistack(generates_file=True)
        def make_plot(data, subject, session):
            nonlocal call_count
            call_count += 1
            return None

        # First run
        for_each(
            make_plot,
            inputs={"data": RawSignal},
            outputs=[Figure],
            subject=[1],
            session=["A", "B"],
        )
        assert call_count == 2

        # Second run — should skip both (already computed)
        for_each(
            make_plot,
            inputs={"data": RawSignal},
            outputs=[Figure],
            subject=[1],
            session=["A", "B"],
        )
        assert call_count == 2  # NOT called again

    def test_for_each_outputs_figure_type(self, db):
        """outputs=[Figure] should work with generates_file function."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")

        @scistack(generates_file=True)
        def make_plot(data, subject, session):
            return None

        # Should not raise
        for_each(
            make_plot,
            inputs={"data": RawSignal},
            outputs=[Figure],
            subject=[1],
            session=["A"],
        )

    def test_normal_fn_no_metadata_by_default(self, db):
        """Normal (non-generates_file) functions do NOT receive metadata by default."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")

        call_args = {}

        @scistack
        def process(data):
            nonlocal call_args
            call_args = {"called": True}
            return data * 2

        for_each(
            process,
            inputs={"data": RawSignal},
            outputs=[RawSignal],
            subject=[1],
            session=["A"],
        )

        # Function was called but without metadata kwargs
        assert call_args == {"called": True}
