"""Comprehensive tests for skip_computed in for_each(skip_computed=True).

Each test class covers a distinct aspect of the skip logic:
  - Basic skip / compute decisions
  - Upstream input changes (various data types)
  - Function bytecode changes
  - Deep multi-step pipelines
  - Constants / branch_params variants
  - Fixed() inputs
  - Multiple schema dimensions
"""

import logging
from functools import partial

import numpy as np
import pytest

from scidb import BaseVariable, Fixed, scistack
from scidb import for_each as _for_each

# skip_computed=True: these tests cover the skip-already-computed path.
for_each = partial(_for_each, skip_computed=True)


@pytest.fixture(autouse=True)
def _capture_debug_logs(caplog):
    """Per-combo [skip]/[recompute] lines are log records (DEBUG-destined
    since the logging redesign), not stdout — capture them for every test."""
    with caplog.at_level(logging.DEBUG):
        yield


# ---------------------------------------------------------------------------
# Variable types
# All defined at module level so BaseVariable's registry can find them.
# ---------------------------------------------------------------------------


class RawSignal(BaseVariable):
    schema_version = 1


class Filtered(BaseVariable):
    schema_version = 1


class Intermediate(BaseVariable):
    schema_version = 1


class ScalarOut(BaseVariable):
    schema_version = 1


class DictOut(BaseVariable):
    schema_version = 1


class Baseline(BaseVariable):
    """Used as a Fixed() input in tests."""

    schema_version = 1


class AltSignal(BaseVariable):
    """Second signal type for multi-input tests."""

    schema_version = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _skip_lines(text: str) -> list[str]:
    return [l for l in text.splitlines() if l.startswith("[skip]")]


def _recompute_lines(text: str) -> list[str]:
    return [l for l in text.splitlines() if l.startswith("[recompute]")]


def _drain(caplog) -> str:
    """Captured log messages as text, then clear the capture.

    Mirrors the old capsys ``readouterr().out`` fetch-and-reset contract the
    assertions below were written against."""
    text = "\n".join(r.getMessage() for r in caplog.records)
    caplog.clear()
    return text


# ===========================================================================
# Basic skip / compute behaviour
# ===========================================================================


class TestSkipComputedBasic:
    def test_first_run_always_computes(self, db, caplog):
        """No prior output → hook returns False → function runs."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )

        assert call_count[0] == 1
        assert not _skip_lines(_drain(caplog))

    def test_second_run_skips_when_nothing_changed(self, db, caplog):
        """Identical state on second call → [skip] printed, output unchanged."""

        @scistack
        def double(x):
            return x * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )
        caplog.clear()

        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )

        out = _drain(caplog)
        assert len(_skip_lines(out)) == 1
        assert not _recompute_lines(out)
        np.testing.assert_array_equal(
            Filtered.load(subject=1, trial=1).data, np.array([2, 4, 6])
        )

    def test_skip_computed_false_bypasses_hook(self, db, caplog):
        """skip_computed=False: combo is never filtered → no [skip] line."""

        @scistack
        def double(x):
            return x * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )
        caplog.clear()

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            skip_computed=False,
            subject=[1],
            trial=[1],
        )

        assert not _skip_lines(_drain(caplog))

    def test_output_saved_without_lineage_always_recomputes(self, db, caplog):
        """Output written via raw .save() has no _lineage row → recompute."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        # Save output manually — no lineage record written
        Filtered.save(np.array([2, 4, 6]), subject=1, trial=1)

        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )

        assert call_count[0] == 1
        assert not _skip_lines(_drain(caplog))

    def test_multiple_subjects_all_skip(self, db, caplog):
        """Three subjects, nothing changed → three [skip] lines, no re-execution."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        for s in [1, 2, 3]:
            RawSignal.save(np.array([s, s + 1]), subject=s, trial=1)

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1, 2, 3],
            trial=[1],
        )
        count_after_first = call_count[0]
        caplog.clear()

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1, 2, 3],
            trial=[1],
        )

        out = _drain(caplog)
        assert len(_skip_lines(out)) == 3
        assert not _recompute_lines(out)
        # When every combo is skipped the function must not run again — the
        # filtered (empty) combo set must drive zero iterations, not fall back
        # to re-running the full set.
        assert call_count[0] == count_after_first

    def test_skip_then_rerun_after_change(self, db, caplog):
        """Skip → input changes → recompute → skip again on third run."""

        @scistack
        def double(x):
            return x * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )

        # Second run: skip
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )
        assert len(_skip_lines(_drain(caplog))) == 1

        # Change input
        RawSignal.save(np.array([10, 20, 30]), subject=1, trial=1)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )
        assert len(_recompute_lines(_drain(caplog))) == 1

        # Fourth run: skip again (now stable)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )
        assert len(_skip_lines(_drain(caplog))) == 1


# ===========================================================================
# Upstream input changes
# ===========================================================================


class TestSkipComputedInputChanges:
    def test_new_data_triggers_recompute(self, db, caplog):
        """Resaving input with different data → new record_id → [recompute]."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )
        assert call_count[0] == 1

        RawSignal.save(np.array([10, 20, 30]), subject=1, trial=1)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )

        assert call_count[0] == 2
        out = _drain(caplog)
        assert _recompute_lines(out)
        np.testing.assert_array_equal(
            Filtered.load(subject=1, trial=1).data, np.array([20, 40, 60])
        )

    def test_identical_data_resaved_still_skips(self, db, caplog):
        """Same content resaved → same record_id (content-addressed) → skip."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        data = np.array([1, 2, 3])
        RawSignal.save(data, subject=1, trial=1)
        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )
        count_after_first = call_count[0]

        # Exact same bytes → same content_hash → same record_id
        RawSignal.save(data.copy(), subject=1, trial=1)
        caplog.clear()

        for_each(
            double, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )

        out = _drain(caplog)
        assert _skip_lines(out)
        assert not _recompute_lines(out)
        assert call_count[0] == count_after_first

    def test_only_changed_subject_recomputes(self, db, caplog):
        """When subject=2's input changes, only that combo shows [recompute]."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        for s in [1, 2]:
            RawSignal.save(np.array([s * 10, s * 10 + 1]), subject=s, trial=1)

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1, 2],
            trial=[1],
        )
        count_after_first = call_count[0]
        caplog.clear()

        # Only subject=2 changes
        RawSignal.save(np.array([999, 1000]), subject=2, trial=1)

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1, 2],
            trial=[1],
        )

        out = _drain(caplog)
        assert any("subject=1" in l for l in _skip_lines(out))
        assert any("subject=2" in l for l in _recompute_lines(out))
        # Only one extra call (for subject=2)
        assert call_count[0] == count_after_first + 1

    def test_scalar_input_change(self, db, caplog):
        """Scalar float input: change triggers recompute."""
        call_count = [0]

        @scistack
        def increment(x):
            call_count[0] += 1
            return float(x) + 1.0

        ScalarOut.save(5.0, subject=1, trial=1)
        for_each(
            increment,
            inputs={"x": ScalarOut},
            outputs=[ScalarOut],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        ScalarOut.save(99.0, subject=1, trial=1)
        for_each(
            increment,
            inputs={"x": ScalarOut},
            outputs=[ScalarOut],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert _recompute_lines(out)
        assert call_count[0] == 2

    def test_dict_of_arrays_input_change(self, db, caplog):
        """Dict-of-arrays data type: content change detected correctly."""
        call_count = [0]

        @scistack
        def process_dict(x):
            call_count[0] += 1
            return {"vals": x["vals"] * 2}

        DictOut.save({"vals": np.array([1, 2, 3])}, subject=1, trial=1)
        for_each(
            process_dict,
            inputs={"x": DictOut},
            outputs=[DictOut],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        # Change the dict content
        DictOut.save({"vals": np.array([10, 20, 30])}, subject=1, trial=1)
        for_each(
            process_dict,
            inputs={"x": DictOut},
            outputs=[DictOut],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert _recompute_lines(out)
        assert call_count[0] == 2

    def test_dict_of_arrays_unchanged_skips(self, db, caplog):
        """Dict-of-arrays: same content → skip."""

        @scistack
        def process_dict(x):
            return {"vals": x["vals"] * 2}

        DictOut.save({"vals": np.array([1, 2, 3])}, subject=1, trial=1)
        for_each(
            process_dict,
            inputs={"x": DictOut},
            outputs=[DictOut],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        for_each(
            process_dict,
            inputs={"x": DictOut},
            outputs=[DictOut],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert _skip_lines(out)
        assert not _recompute_lines(out)


# ===========================================================================
# Function bytecode changes
# ===========================================================================


class TestSkipComputedFunctionChanges:
    def test_different_function_triggers_recompute(self, db, caplog):
        """Swapping to a function with a different name → no matching record
        (different __fn in version_keys) → function runs (not skipped)."""
        call_count_v2 = [0]

        @scistack
        def process_v1(x):
            return x * 2

        @scistack
        def process_v2(x):
            call_count_v2[0] += 1
            return x * 3  # deliberately different

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            process_v1,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        for_each(
            process_v2,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        # Different __fn means no existing record matches → "output missing",
        # not "[recompute]". The function still runs correctly.
        assert not _skip_lines(out)
        assert call_count_v2[0] >= 1

    def test_same_function_object_skips(self, db, caplog):
        """Using the exact same function object on rerun → skip."""

        @scistack
        def process(x):
            return x * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            process, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )
        caplog.clear()

        for_each(
            process, inputs={"x": RawSignal}, outputs=[Filtered], subject=[1], trial=[1]
        )

        out = _drain(caplog)
        assert _skip_lines(out)
        assert not _recompute_lines(out)


# ===========================================================================
# Multi-step (deep) pipelines
# ===========================================================================


class TestSkipComputedDeepPipeline:
    def test_unchanged_two_step_pipeline_both_skip(self, db, caplog):
        """Two-step pipeline with no changes: both steps skip."""

        @scistack
        def step1(x):
            return x + 1

        @scistack
        def step2(y):
            return y * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            step1,
            inputs={"x": RawSignal},
            outputs=[Intermediate],
            subject=[1],
            trial=[1],
        )
        for_each(
            step2,
            inputs={"y": Intermediate},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        for_each(
            step1,
            inputs={"x": RawSignal},
            outputs=[Intermediate],
            subject=[1],
            trial=[1],
        )
        for_each(
            step2,
            inputs={"y": Intermediate},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert len(_skip_lines(out)) == 2
        assert not _recompute_lines(out)

    def test_raw_input_change_propagates_through_pipeline(self, db, caplog):
        """Raw input changes → step1 recomputes → step2 then also recomputes."""
        step1_calls = [0]
        step2_calls = [0]

        @scistack
        def step1(x):
            step1_calls[0] += 1
            return x + 1

        @scistack
        def step2(y):
            step2_calls[0] += 1
            return y * 2

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            step1,
            inputs={"x": RawSignal},
            outputs=[Intermediate],
            subject=[1],
            trial=[1],
        )
        for_each(
            step2,
            inputs={"y": Intermediate},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        assert step1_calls[0] == 1
        assert step2_calls[0] == 1

        # Update raw input, re-run step1
        RawSignal.save(np.array([10, 20, 30]), subject=1, trial=1)
        for_each(
            step1,
            inputs={"x": RawSignal},
            outputs=[Intermediate],
            subject=[1],
            trial=[1],
        )
        assert step1_calls[0] == 2
        caplog.clear()

        # step2 should detect Intermediate changed → recompute
        for_each(
            step2,
            inputs={"y": Intermediate},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert _recompute_lines(out)
        assert step2_calls[0] == 2
        np.testing.assert_array_equal(
            Filtered.load(subject=1, trial=1).data, np.array([22, 42, 62])
        )

    def test_step1_skip_does_not_change_step2_decision(self, db, caplog):
        """If step1 skips (nothing changed), step2 also correctly skips."""

        @scistack
        def step1(x):
            return x + 1

        @scistack
        def step2(y):
            return y * 2

        RawSignal.save(np.array([5, 6, 7]), subject=1, trial=1)
        for_each(
            step1,
            inputs={"x": RawSignal},
            outputs=[Intermediate],
            subject=[1],
            trial=[1],
        )
        for_each(
            step2,
            inputs={"y": Intermediate},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        # Re-run both with no changes
        for_each(
            step1,
            inputs={"x": RawSignal},
            outputs=[Intermediate],
            subject=[1],
            trial=[1],
        )
        for_each(
            step2,
            inputs={"y": Intermediate},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        skips = _skip_lines(out)
        assert len(skips) == 2
        assert not _recompute_lines(out)

    def test_three_step_pipeline_middle_change(self, db, caplog):
        """Three-step pipeline: changing middle output triggers step3 recompute."""

        @scistack
        def step1(x):
            return x + 1

        @scistack
        def step2(y):
            return y * 2

        @scistack
        def step3(z):
            return z - 1

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            step1,
            inputs={"x": RawSignal},
            outputs=[Intermediate],
            subject=[1],
            trial=[1],
        )
        for_each(
            step2,
            inputs={"y": Intermediate},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        for_each(
            step3, inputs={"z": Filtered}, outputs=[ScalarOut], subject=[1], trial=[1]
        )
        caplog.clear()

        # Change raw input and re-run step1 and step2
        RawSignal.save(np.array([10, 20, 30]), subject=1, trial=1)
        for_each(
            step1,
            inputs={"x": RawSignal},
            outputs=[Intermediate],
            subject=[1],
            trial=[1],
        )
        for_each(
            step2,
            inputs={"y": Intermediate},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        # step3 should detect Filtered changed → recompute
        for_each(
            step3, inputs={"z": Filtered}, outputs=[ScalarOut], subject=[1], trial=[1]
        )

        out = _drain(caplog)
        assert _recompute_lines(out)


# ===========================================================================
# Constants / branch_params variants
# ===========================================================================


class TestSkipComputedConstants:
    def test_each_constant_variant_tracked_independently(self, db, caplog):
        """factor=2 and factor=3 are separate pipeline branches; each skips."""
        call_count = [0]

        @scistack
        def scale(x, factor):
            call_count[0] += 1
            return x * factor

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 2},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 3},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        count_after_setup = call_count[0]
        caplog.clear()

        # Re-run both variants — both should skip
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 2},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 3},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert len(_skip_lines(out)) == 2
        assert not _recompute_lines(out)
        assert call_count[0] == count_after_setup

    def test_new_constant_variant_computes(self, db, caplog):
        """A constant value with no prior output always computes (no output yet)."""
        call_count = [0]

        @scistack
        def scale(x, factor):
            call_count[0] += 1
            return x * factor

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 2},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        # factor=7 has never been run → no output → must compute
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 7},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert not _skip_lines(out)
        assert call_count[0] == 2

    def test_input_change_recomputes_all_constant_variants(self, db, caplog):
        """Input change affects all constant variants of the downstream function."""
        call_count = [0]

        @scistack
        def scale(x, factor):
            call_count[0] += 1
            return x * factor

        RawSignal.save(np.array([1, 2, 3]), subject=1, trial=1)
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 2},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 3},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        count_after_setup = call_count[0]

        # Change input
        RawSignal.save(np.array([10, 20, 30]), subject=1, trial=1)
        caplog.clear()

        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 2},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        for_each(
            scale,
            inputs={"x": RawSignal, "factor": 3},
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert len(_recompute_lines(out)) == 2
        assert call_count[0] == count_after_setup + 2


# ===========================================================================
# Fixed() inputs
# ===========================================================================


class TestSkipComputedFixed:
    def test_fixed_input_unchanged_skips(self, db, caplog):
        """Fixed input at a specific schema location: unchanged → skip."""

        @scistack
        def subtract_baseline(signal, baseline):
            return signal - baseline

        RawSignal.save(np.array([10, 20, 30]), subject=1, trial=1)
        Baseline.save(np.array([1, 2, 3]), subject=1, trial=1)

        for_each(
            subtract_baseline,
            inputs={
                "signal": RawSignal,
                "baseline": Fixed(Baseline, subject=1, trial=1),
            },
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        for_each(
            subtract_baseline,
            inputs={
                "signal": RawSignal,
                "baseline": Fixed(Baseline, subject=1, trial=1),
            },
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert _skip_lines(out)
        assert not _recompute_lines(out)

    def test_fixed_input_changed_recomputes(self, db, caplog):
        """Fixed input resaved with new data → recompute."""
        call_count = [0]

        @scistack
        def subtract_baseline(signal, baseline):
            call_count[0] += 1
            return signal - baseline

        RawSignal.save(np.array([10, 20, 30]), subject=1, trial=1)
        Baseline.save(np.array([1, 2, 3]), subject=1, trial=1)

        for_each(
            subtract_baseline,
            inputs={
                "signal": RawSignal,
                "baseline": Fixed(Baseline, subject=1, trial=1),
            },
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        assert call_count[0] == 1

        # Update the fixed baseline
        Baseline.save(np.array([5, 5, 5]), subject=1, trial=1)
        caplog.clear()

        for_each(
            subtract_baseline,
            inputs={
                "signal": RawSignal,
                "baseline": Fixed(Baseline, subject=1, trial=1),
            },
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert _recompute_lines(out)
        assert call_count[0] == 2
        np.testing.assert_array_equal(
            Filtered.load(subject=1, trial=1).data, np.array([5, 15, 25])
        )

    def test_signal_changed_but_fixed_unchanged_recomputes(self, db, caplog):
        """Signal changes while fixed baseline stays the same → recompute."""
        call_count = [0]

        @scistack
        def subtract_baseline(signal, baseline):
            call_count[0] += 1
            return signal - baseline

        RawSignal.save(np.array([10, 20, 30]), subject=1, trial=1)
        Baseline.save(np.array([1, 1, 1]), subject=1, trial=1)

        for_each(
            subtract_baseline,
            inputs={
                "signal": RawSignal,
                "baseline": Fixed(Baseline, subject=1, trial=1),
            },
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )
        caplog.clear()

        # Change only the signal
        RawSignal.save(np.array([100, 200, 300]), subject=1, trial=1)

        for_each(
            subtract_baseline,
            inputs={
                "signal": RawSignal,
                "baseline": Fixed(Baseline, subject=1, trial=1),
            },
            outputs=[Filtered],
            subject=[1],
            trial=[1],
        )

        out = _drain(caplog)
        assert _recompute_lines(out)
        assert call_count[0] == 2


# ===========================================================================
# Multiple schema dimensions (subject × trial grid)
# ===========================================================================


class TestSkipComputedMultipleSchemaKeys:
    def test_full_grid_all_skip(self, db, caplog):
        """2×2 subject/trial grid: all four combos skip when unchanged."""

        @scistack
        def double(x):
            return x * 2

        for s in [1, 2]:
            for t in [1, 2]:
                RawSignal.save(np.array([s * 10 + t]), subject=s, trial=t)

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1, 2],
            trial=[1, 2],
        )
        caplog.clear()

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1, 2],
            trial=[1, 2],
        )

        out = _drain(caplog)
        assert len(_skip_lines(out)) == 4
        assert not _recompute_lines(out)

    def test_partial_grid_change_recomputes_only_affected(self, db, caplog):
        """Only subject=2, trial=2 input changes → only that combo recomputes."""
        call_count = [0]

        @scistack
        def double(x):
            call_count[0] += 1
            return x * 2

        for s in [1, 2]:
            for t in [1, 2]:
                RawSignal.save(np.array([s * 10 + t]), subject=s, trial=t)

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1, 2],
            trial=[1, 2],
        )
        count_after_first = call_count[0]
        caplog.clear()

        # Only change subject=2, trial=2
        RawSignal.save(np.array([999]), subject=2, trial=2)

        for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            subject=[1, 2],
            trial=[1, 2],
        )

        out = _drain(caplog)
        assert len(_skip_lines(out)) == 3
        assert len(_recompute_lines(out)) == 1
        assert any("subject=2" in l and "trial=2" in l for l in _recompute_lines(out))
        assert call_count[0] == count_after_first + 1
