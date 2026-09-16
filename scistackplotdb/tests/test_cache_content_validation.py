"""The plot source's caches must notice new records — and only new records.

``ScidbSource`` caches a whole variable's frame. Nothing tells it when records
appear: a MATLAB run driven from a terminal never reports completion back to
the GUI process, and neither does the ``scidb`` CLI or a second GUI. On
2026-09-15 that left Plot Studio serving pre-run frames for a whole session
after a re-run wrote 390 corrected records — a figure silently disagreeing with
the database it claimed to show.

So the cache validates itself against a content fingerprint
(``scidb.provenance_query.variable_content_fingerprint``) on every hit. Because
``record_id`` is a content hash, that is PRECISE rather than merely
conservative, and both halves of that are pinned here:

* a re-run producing IDENTICAL data must KEEP the cache (re-reading a large
  variable because someone re-ran a no-op is the cost this avoids);
* a run producing DIFFERENT data must DROP it.
"""

from __future__ import annotations

import numpy as np
import pytest
from scidb.provenance_query import variable_content_fingerprint
from scistackplotdb import ScidbSource

from conftest import SESSIONS, SUBJECTS, TRIALS, Signal, StepLength


@pytest.fixture
def source(seeded):
    return ScidbSource(seeded)


def _resave_identical(db):
    """Re-save every StepLength with the value it already holds.

    The no-op re-run: same content, therefore the same record_ids, therefore
    nothing for a cache to miss. (`_record_save` still gets a fresh audit row,
    which is exactly why the fingerprint is not a timestamp.)
    """
    for subject in SUBJECTS:
        for session in SESSIONS:
            for trial in TRIALS:
                existing = StepLength.load(
                    subject=subject, session=session, trial=trial
                )
                StepLength.save(
                    existing.data, subject=subject, session=session, trial=trial
                )


class TestFingerprint:
    def test_identical_resave_keeps_the_fingerprint(self, seeded):
        before = variable_content_fingerprint(seeded._duck, "StepLength")
        _resave_identical(seeded)
        assert variable_content_fingerprint(seeded._duck, "StepLength") == before

    def test_new_content_changes_the_fingerprint(self, seeded):
        before = variable_content_fingerprint(seeded._duck, "StepLength")
        StepLength.save(99.0, subject=SUBJECTS[0], session=SESSIONS[0], trial=TRIALS[0])
        assert variable_content_fingerprint(seeded._duck, "StepLength") != before

    def test_unknown_variable_is_empty_not_an_error(self, seeded):
        assert variable_content_fingerprint(seeded._duck, "NoSuchVariable") == (0, 0)


class TestCacheKeptWhenDataIsUnchanged:
    def test_identical_resave_keeps_the_cached_frame(self, source, seeded):
        first = source._variable_frame("StepLength")
        _resave_identical(seeded)
        second = source._variable_frame("StepLength")
        # The SAME object: not rebuilt, not merely equal.
        assert second is first

    def test_untouched_variable_keeps_its_cache(self, source, seeded):
        signal_first = source._variable_frame("Signal")
        source._variable_frame("StepLength")
        # Write to a DIFFERENT variable: Signal's fingerprint is unmoved.
        StepLength.save(99.0, subject=SUBJECTS[0], session=SESSIONS[0], trial=TRIALS[0])
        assert source._variable_frame("Signal") is signal_first

    def test_repeated_hits_do_not_rebuild(self, source):
        first = source._variable_frame("StepLength")
        for _ in range(3):
            assert source._variable_frame("StepLength") is first


class TestCacheDroppedWhenDataChanges:
    def test_new_value_rebuilds_the_frame(self, source, seeded):
        first = source._variable_frame("StepLength")
        StepLength.save(99.0, subject=SUBJECTS[0], session=SESSIONS[0], trial=TRIALS[0])
        second = source._variable_frame("StepLength")
        assert second is not first

    def test_the_rebuilt_frame_holds_the_new_value(self, source, seeded):
        source._variable_frame("StepLength")
        StepLength.save(99.0, subject=SUBJECTS[0], session=SESSIONS[0], trial=TRIALS[0])
        frame = source._variable_frame("StepLength")
        values = frame.frame[frame.value_column].to_numpy()
        assert 99.0 in values, values

    def test_a_new_record_at_a_new_location_rebuilds(self, source, seeded):
        first = source._variable_frame("StepLength")
        StepLength.save(1.5, subject="99", session=SESSIONS[0], trial=TRIALS[0])
        assert source._variable_frame("StepLength") is not first

    def test_variant_frame_also_revalidates(self, source, seeded):
        """The stale `variant_table` in the 2026-09-15 session came from THIS
        cache, not the data one — it has its own entry point."""
        first = source._variant_frame("Signal")
        Signal.save(
            np.arange(8, dtype=float),
            subject=SUBJECTS[0],
            session=SESSIONS[0],
            trial=TRIALS[0],
        )
        assert source._variant_frame("Signal") is not first

    def test_derived_tables_are_dropped_too(self, source, seeded):
        """`get_table` builds on the frame, so a stale frame means a stale
        table; invalidation has to reach both."""
        source._variable_frame("StepLength")
        assert source._table_cache() is not None
        source.get_table(["StepLength"])
        assert len(source._table_cache()) >= 1
        StepLength.save(99.0, subject=SUBJECTS[0], session=SESSIONS[0], trial=TRIALS[0])
        source._variable_frame("StepLength")
        assert len(source._table_cache()) == 0


class TestFailureIsNotFatal:
    def test_an_unavailable_fingerprint_keeps_the_cache(self, source, monkeypatch):
        """Unable to prove staleness is not a reason to refuse to draw."""
        first = source._variable_frame("StepLength")
        monkeypatch.setattr(source, "_content_fingerprint", lambda _v: None)
        assert source._variable_frame("StepLength") is first

    def test_a_raising_query_is_swallowed(self, source, seeded, monkeypatch):
        """The guard itself must never turn a plot into an exception."""
        source._variable_frame("StepLength")

        def boom(*_args, **_kwargs):
            raise RuntimeError("database went away")

        monkeypatch.setattr(
            "scidb.provenance_query.variable_content_fingerprint", boom
        )
        assert source._content_fingerprint("StepLength") is None
        assert source._variable_frame("StepLength") is not None


class TestSharedStorageRule:
    """The plot stack loads BYPASSING ``_storage_to_python`` — deliberately,
    for measured reasons (``load.load_variable``: 18x on a 17.4 M-sample
    column). What it must not have is its own COPY of what a stored cell
    means: it did, and the identical mask-dropping bug was found and fixed
    twice, two days apart (2026-09-13 here, 2026-09-15 in sciduckdb). The rule
    now lives in sciduckdb, which owns storage semantics.
    """

    def test_float_row_delegates_to_sciduckdb(self):
        from scistackplotdb.load import _float_row

        out = _float_row(np.ma.MaskedArray([0.0, 0.5], mask=[True, False]))
        assert np.isnan(out[0]), out
        assert out[1] == 0.5

    def test_both_layers_agree_on_a_null_bearing_cell(self):
        from sciduckdb.sciduckdb import _storage_to_python
        from scistackplotdb.load import _float_row

        cell = np.ma.MaskedArray([0.0, 0.49, 0.45], mask=[True, False, False])
        via_storage = _storage_to_python(
            cell, {"python_type": "ndarray", "numpy_dtype": "float64"}
        )
        via_plot = _float_row(cell)
        np.testing.assert_array_equal(np.isnan(via_storage), np.isnan(via_plot))
        np.testing.assert_allclose(via_storage[1:], via_plot[1:])

    def test_a_real_zero_survives_both_layers(self):
        from sciduckdb.sciduckdb import _storage_to_python
        from scistackplotdb.load import _float_row

        cell = np.array([0.0, 0.49])
        assert _float_row(cell)[0] == 0.0
        assert (
            _storage_to_python(
                cell, {"python_type": "ndarray", "numpy_dtype": "float64"}
            )[0]
            == 0.0
        )


class TestSupersededWithoutAnAxis:
    """A superseded record must be filterable even with no axis to explain it.

    Until 2026-09-15 the `CodeIsLatest` flag was attached only when a code or
    run-option axis existed. Those axes are how a superseded record is usually
    made *explicable* to a reader, but they are not what makes it superseded:
    re-running over changed inputs supersedes the previous output with no code
    edit at all, and a record whose LINEAGE was severed can produce no code
    axis however many versions exist upstream. `GAITRiteSymmetry` hit both —
    780 records, `variants=none`, no flag — and the old generation drew
    alongside the new one as extra replicates.
    """

    def test_single_generation_gains_no_flag(self, seeded):
        """The ordinary case must be untouched: nothing superseded, no column,
        no default pin."""
        from scistackplotdb.load import LATEST_COLUMN, load_variable

        loaded = load_variable(seeded, "StepLength")
        assert loaded.latest_column is None
        assert LATEST_COLUMN not in loaded.frame.columns

    def test_a_superseded_record_gets_the_flag(self, seeded):
        """Re-save one location: two records there, the older superseded."""
        from scistackplotdb.load import load_variable

        StepLength.save(
            42.0, subject=SUBJECTS[0], session=SESSIONS[0], trial=TRIALS[0]
        )
        loaded = load_variable(seeded, "StepLength")
        if loaded.latest_column is None:
            pytest.skip(
                "this database marks no record superseded — is_latest is "
                "chain-derived and a plain re-save may share a chain"
            )
        flags = loaded.frame[loaded.latest_column]
        assert not flags.all(), "nothing marked superseded despite a second record"
        assert flags.any(), "everything marked superseded"

    def test_the_flag_is_not_offered_as_a_plottable_factor(self, seeded):
        """It is a filter helper, not a condition anyone plots by — attaching
        it more often must not add an axis to the variant UI."""
        from scistackplotdb.load import load_variable

        StepLength.save(
            42.0, subject=SUBJECTS[0], session=SESSIONS[0], trial=TRIALS[0]
        )
        loaded = load_variable(seeded, "StepLength")
        assert loaded.latest_column not in loaded.variant_columns
