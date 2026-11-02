"""Regression tests: for_each outputs must be visible to list_pipeline_variants().

After for_each saves a computed record, the provenance graph must record the
producing function so that db.list_pipeline_variants() can discover it.

Without this, the GUI shows output variables as green (up-to-date) even when
some combos are missing, because the pipeline graph never learns about the
function that produced them.
"""

import logging
from functools import partial

import numpy as np

from scidb import BaseVariable, scistack
from scidb import for_each as _for_each

# skip_computed=True: these tests cover the skip-already-computed path.
for_each = partial(_for_each, skip_computed=True)

# ---------------------------------------------------------------------------
# Variable types
# ---------------------------------------------------------------------------


class RawData(BaseVariable):
    schema_version = 1


class ProcessedData(BaseVariable):
    schema_version = 1


class AuxData(BaseVariable):
    schema_version = 1


class Figure(BaseVariable):
    schema_version = 1


# ---------------------------------------------------------------------------
# Helpers
#
# Per-combo [skip]/[recompute] lines are log records on the "scidb" logger
# (DEBUG-destined per the logging redesign), so the tests capture them via
# caplog rather than stdout.
# ---------------------------------------------------------------------------


def _messages(caplog) -> list[str]:
    # skip_computed decisions come from the "scidb" logger; filter by name so
    # scifor's per-iteration records (which also propagate to root) don't
    # inflate the counts.
    return [r.getMessage() for r in caplog.records if r.name == "scidb"]


def _skip_lines(caplog) -> list[str]:
    return [m for m in _messages(caplog) if m.startswith("[skip]")]


def _recompute_lines(caplog) -> list[str]:
    return [m for m in _messages(caplog) if m.startswith("[recompute]")]


# ===========================================================================
# list_pipeline_variants visibility
# ===========================================================================


class TestListPipelineVariantsVisibility:
    """Scihist outputs must appear in db.list_pipeline_variants()."""

    def test_single_output_visible(self, db):
        """After for_each(skip_computed=True), list_pipeline_variants finds the function."""

        @scistack
        def double(x):
            return x * 2

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        variants = db.list_pipeline_variants()
        fn_names = {v["function_name"] for v in variants}
        assert "double" in fn_names

    def test_output_type_correct(self, db):
        """The variant's output_type matches the output variable class."""

        @scistack
        def double(x):
            return x * 2

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        variants = db.list_pipeline_variants(output_type="ProcessedData")
        assert len(variants) >= 1
        assert variants[0]["function_name"] == "double"

    def test_record_count_matches(self, db):
        """Variant record_count should match the number of saved combos."""

        @scistack
        def double(x):
            return x * 2

        for s in [1, 2, 3]:
            RawData.save(np.array([s * 10]), subject=s, trial=1)

        for_each(
            double,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1, 2, 3],
            trial=[1],
        )

        variants = db.list_pipeline_variants(output_type="ProcessedData")
        total = sum(v["record_count"] for v in variants)
        assert total == 3

    def test_multiple_functions_both_visible(self, db):
        """Two different pipeline functions produce two separate variants."""

        @scistack
        def step1(x):
            return x + 1

        @scistack
        def step2(y):
            return y * 2

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            step1,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )
        for_each(
            step2,
            inputs={"y": ProcessedData},
            outputs=[AuxData],
            subject=[1],
            trial=[1],
        )

        variants = db.list_pipeline_variants()
        fn_names = {v["function_name"] for v in variants}
        assert "step1" in fn_names
        assert "step2" in fn_names

    def test_constant_variants_visible(self, db):
        """Different constant values produce distinct variants."""

        @scistack
        def scale(x, factor):
            return x * factor

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            scale,
            inputs={"x": RawData, "factor": 2},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )
        for_each(
            scale,
            inputs={"x": RawData, "factor": 3},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        variants = db.list_pipeline_variants(output_type="ProcessedData")
        assert len(variants) >= 2
        all_fn = {v["function_name"] for v in variants}
        assert all_fn == {"scale"}

    def test_generates_file_visible(self, db):
        """generates_file=True functions should also be visible."""

        @scistack(generates_file=True)
        def make_plot(data, subject, trial):
            return None

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            make_plot,
            inputs={"data": RawData},
            outputs=[Figure],
            subject=[1],
            trial=[1],
        )

        variants = db.list_pipeline_variants(output_type="Figure")
        fn_names = {v["function_name"] for v in variants}
        assert "make_plot" in fn_names


# ===========================================================================
# skip_computed with __fn in version_keys
# ===========================================================================


class TestSkipComputedWithFnVersionKeys:
    """skip_computed must still work after __fn/__fn_hash are in version_keys."""

    def test_skip_works_after_fn_version_keys_added(self, db, caplog):
        """Records with __fn in version_keys are found by skip_computed lookup."""

        @scistack
        def double(x):
            return x * 2

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        # Second run — must skip
        with caplog.at_level(logging.DEBUG, logger="scidb"):
            for_each(
                double,
                inputs={"x": RawData},
                outputs=[ProcessedData],
                subject=[1],
                trial=[1],
            )
        assert len(_skip_lines(caplog)) == 1
        assert not _recompute_lines(caplog)

    def test_skip_works_with_constants(self, db, caplog):
        """skip_computed correctly finds records when constants + __fn are in version_keys."""

        @scistack
        def scale(x, factor):
            return x * factor

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            scale,
            inputs={"x": RawData, "factor": 2},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="scidb"):
            for_each(
                scale,
                inputs={"x": RawData, "factor": 2},
                outputs=[ProcessedData],
                subject=[1],
                trial=[1],
            )
        assert len(_skip_lines(caplog)) == 1
        assert not _recompute_lines(caplog)

    def test_input_change_still_recomputes(self, db, caplog):
        """Changing upstream data still triggers recompute (not broken by __fn in lookup)."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )
        assert call_count[0] == 1

        # Change input
        RawData.save(np.array([10, 20, 30]), subject=1, trial=1)
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="scidb"):
            for_each(
                double,
                inputs={"x": RawData},
                outputs=[ProcessedData],
                subject=[1],
                trial=[1],
            )
        assert _recompute_lines(caplog)
        assert call_count[0] == 2

    def test_function_change_still_computes(self, db, caplog):
        """Changing the function name means no existing record matches __fn,
        so the combo is treated as missing (computed, not skipped)."""
        call_count = [0]

        @scistack
        def process_v1(x):
            return x * 2

        @scistack
        def process_v2(x):
            call_count[0] += 1
            return x * 3

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            process_v1,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="scidb"):
            for_each(
                process_v2,
                inputs={"x": RawData},
                outputs=[ProcessedData],
                subject=[1],
                trial=[1],
            )
        # process_v2 has a different __fn, so no existing record matches →
        # skip_computed sees "output missing" and lets it run
        assert call_count[0] == 1
        assert not _skip_lines(caplog)

    def test_load_still_works_with_schema_keys_only(self, db):
        """BaseVariable.load(subject=1, trial=1) returns correct data despite __fn in version_keys."""

        @scistack
        def double(x):
            return x * 2

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        loaded = ProcessedData.load(subject=1, trial=1)
        np.testing.assert_array_equal(loaded.data, np.array([2, 4, 6]))

    def test_multiple_subjects_skip_and_recompute_mixed(self, db, caplog):
        """With 3 subjects, changing one still correctly skips the other two."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        for s in [1, 2, 3]:
            RawData.save(np.array([s * 10]), subject=s, trial=1)

        for_each(
            double,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1, 2, 3],
            trial=[1],
        )
        assert call_count[0] == 3

        # Change only subject=2
        RawData.save(np.array([999]), subject=2, trial=1)
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="scidb"):
            for_each(
                double,
                inputs={"x": RawData},
                outputs=[ProcessedData],
                subject=[1, 2, 3],
                trial=[1],
            )

        assert len(_skip_lines(caplog)) == 2
        assert len(_recompute_lines(caplog)) == 1
        assert any("subject=2" in l for l in _recompute_lines(caplog))
        assert call_count[0] == 4  # 3 original + 1 recompute
