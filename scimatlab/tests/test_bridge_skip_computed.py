"""Tests for skip_computed on the MATLAB-driven for_each bridge.

These verify that ``scimatlab.bridge.for_each_prepare(skip_computed=True)``
builds and applies scidb's pre-combo skip hook so combos whose outputs already
exist (with unchanged upstream provenance) are filtered out of ``full_combos``
before MATLAB's loop runs. Runs entirely in Python without MATLAB.

The provenance for the "already computed" state is established with a real
``scidb.for_each`` run; the stored function hash/name are read back from the
``_invocation`` table and fed to the bridge — exactly the values MATLAB would
pass as ``fn_hash`` / ``fn_name``. This is what exercises the new wiring:
the sentinel ``fn`` on the MATLAB path has no meaningful bytecode hash, so the
hook must compare against the supplied ``fn_hash`` instead.
"""

import sys
from pathlib import Path

# Add source paths for the monorepo packages (mirrors test_bridge_where.py)
_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
from scidb.database import configure_database
from scidb.foreach import for_each as scidb_for_each
from scimatlab.bridge import for_each_prepare, for_each_save, register_matlab_variable


def double(x):
    """Plain (undecorated) function — mirrors a MATLAB function handle."""
    return x * 2


def _stored_invocation(db, fn_name):
    """(function_name, function_hash) of the latest invocation for fn_name."""
    rows = db._duck._fetchall(
        "SELECT function_name, function_hash FROM _invocation WHERE function_name = ?",
        [fn_name],
    )
    assert rows, f"no _invocation row recorded for {fn_name!r}"
    return rows[-1]


def _var_spec(type_name):
    return {"kind": "var_type", "type_name": type_name}


def _prep(db, fn_hash, outputs, *, skip_computed, **iters):
    """Call the bridge prepare with a {x: RawSignal} input, return full_combos."""
    meta = {k: list(v) for k, v in iters.items()}
    prep = for_each_prepare(
        "double",
        fn_hash,
        {"x": _var_spec("RawSignal_Skip")},
        list(outputs),
        meta,
        db=db,
        skip_computed=skip_computed,
    )
    return list(prep["full_combos"])


class TestBridgeSkipComputed:
    def _seed(self, tmp_path):
        """Configure DB, register types, save input + compute one output."""
        db = configure_database(tmp_path / "skip.duckdb", ["subject", "trial"])
        RawSignal = register_matlab_variable("RawSignal_Skip")
        Filtered = register_matlab_variable("Filtered_Skip")

        # Input present for two subjects; output computed only for subject 1.
        db.save_variable(RawSignal, np.array([1, 2, 3]), subject=1, trial=1)
        db.save_variable(RawSignal, np.array([4, 5, 6]), subject=2, trial=1)
        scidb_for_each(
            double,
            inputs={"x": RawSignal},
            outputs=[Filtered],
            db=db,
            subject=[1],
            trial=[1],
        )
        _, fn_hash = _stored_invocation(db, "double")
        return db, fn_hash

    def test_skip_when_unchanged(self, tmp_path):
        """Prior output + matching fn_hash → combo filtered out (skipped)."""
        db, fn_hash = self._seed(tmp_path)
        try:
            combos = _prep(
                db,
                fn_hash,
                ["Filtered_Skip"],
                skip_computed=True,
                subject=[1],
                trial=[1],
            )
            assert combos == [], f"expected combo skipped, got {combos}"
        finally:
            db.close()

    def test_no_skip_when_flag_off(self, tmp_path):
        """skip_computed=False → combo is kept even though output exists."""
        db, fn_hash = self._seed(tmp_path)
        try:
            combos = _prep(
                db,
                fn_hash,
                ["Filtered_Skip"],
                skip_computed=False,
                subject=[1],
                trial=[1],
            )
            assert len(combos) == 1, f"expected combo kept, got {combos}"
        finally:
            db.close()

    def test_recompute_when_function_hash_changes(self, tmp_path):
        """Matching output but a changed fn_hash → recompute (combo kept).

        This is the bug the override fixes: without passing fn_hash through, the
        sentinel's hash would never match and every combo would recompute; here
        we prove a *genuinely* different hash is what triggers recompute.
        """
        db, _real_hash = self._seed(tmp_path)
        try:
            combos = _prep(
                db,
                "deadbeefdeadbeef",
                ["Filtered_Skip"],
                skip_computed=True,
                subject=[1],
                trial=[1],
            )
            assert len(combos) == 1, f"expected recompute, got {combos}"
        finally:
            db.close()

    def test_no_skip_when_output_missing(self, tmp_path):
        """A combo whose output was never computed is kept (first run)."""
        db, fn_hash = self._seed(tmp_path)
        try:
            # subject=2 has an input but no Filtered output yet.
            combos = _prep(
                db,
                fn_hash,
                ["Filtered_Skip"],
                skip_computed=True,
                subject=[2],
                trial=[1],
            )
            assert len(combos) == 1, f"expected first-run combo kept, got {combos}"
        finally:
            db.close()

    def test_skip_without_db_is_safe(self, tmp_path):
        """skip_computed=True with no db available is a no-op, not a crash."""
        db, fn_hash = self._seed(tmp_path)
        try:
            # Pass db=None explicitly; bridge falls back to global db (the one
            # we configured), so the skip still applies — the point here is it
            # must not raise.
            prep = for_each_prepare(
                "double",
                fn_hash,
                {"x": _var_spec("RawSignal_Skip")},
                ["Filtered_Skip"],
                {"subject": [1], "trial": [1]},
                db=None,
                skip_computed=True,
            )
            assert isinstance(list(prep["full_combos"]), list)
        finally:
            db.close()


class TestRowSelectionAlignsWithSkippedCombos:
    """`row_selection` is what MATLAB indexes by loop position, so it must be
    aligned with the combos MATLAB actually loops over — `full_combos` AFTER
    the skip hook — not with every combination that was expanded. The first
    bridge run of the selection seam asserted the two were the same length
    and blew up the moment skip_computed removed anything."""

    def test_selection_list_matches_the_combos_matlab_loops_over(self, tmp_path):
        db = configure_database(tmp_path / "align.duckdb", ["subject", "trial"])
        RawSignal = register_matlab_variable("RawSignal_Align")
        Filtered = register_matlab_variable("Filtered_Align")
        try:
            db.save_variable(RawSignal, np.array([1, 2, 3]), subject=1, trial=1)
            db.save_variable(RawSignal, np.array([4, 5, 6]), subject=2, trial=1)
            # Compute subject 1 only; subject 2 is still owed.
            scidb_for_each(double, inputs={"x": RawSignal}, outputs=[Filtered], db=db, subject=[1], trial=[1])
            _, fn_hash = _stored_invocation(db, "double")

            prep = for_each_prepare(
                "double",
                fn_hash,
                {"x": _var_spec("RawSignal_Align")},
                ["Filtered_Align"],
                {"subject": [1, 2], "trial": [1]},
                db=db,
                skip_computed=True,
            )
            combos = list(prep["full_combos"])
            selection = list(prep["row_selection"])
            assert len(combos) == 1, combos  # subject 1 skipped
            assert len(selection) == len(combos)
            # ...and it is subject 2's record that the surviving combo reads.
            (subject2_rid,) = [
                r[0]
                for r in db._duck._fetchall(
                    "SELECT r.record_id FROM _record r JOIN _schema s ON s.schema_id = r.schema_id "
                    "WHERE r.type = ? AND s.subject = '2'",
                    ["RawSignal_Align"],
                )
            ]
            assert selection[0]["x"] == [subject2_rid]
            for_each_save(prep["handle"], [], save=False)
        finally:
            db.close()
