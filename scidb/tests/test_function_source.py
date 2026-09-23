"""Source capture — the code behind a stored ``function_hash``.

``_invocation`` records *which* code produced a record but not *what it said*,
so a superseded version was a fossil: plottable, never re-runnable, never even
readable. Capture happens at write time because it is the only time it can: the
moment the file is edited the old body is gone from disk.

Two properties carry the whole feature and both are tested here:

* the stored key is the SAME hash ``_invocation.function_hash`` holds, or the
  source can never be found again;
* the closure is stored, not one function, because the Python hash is recursive
  over user-defined callees.

See docs/claude/variant-selection.md §3.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.foreach_config import function_sources_for
from scidb.provenance import insert_function_sources
from scidb.provenance_query import function_source

SCHEMA = ["subject"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "test_function_source.duckdb", SCHEMA)
    yield database
    _scifor.set_schema([])
    database.close()


class Raw(BaseVariable):
    pass


class Doubled(BaseVariable):
    pass


# --- the recipe -----------------------------------------------------------


def _helper(value):
    """A user-defined callee, so the hash is recursive over it."""
    return value * 2


def _entry(value):
    return _helper(value) + 1


class TestFunctionSourcesFor:
    def test_hash_matches_the_stored_recipe(self):
        """The load-bearing guarantee. If these drift, source is filed under a
        key nothing was ever stored beneath."""
        from scidb.foreach_config import _compute_fn_hash

        derived, _entry_name, _units = function_sources_for(_entry)

        assert derived == _compute_fn_hash(_entry)

    def test_the_callee_closure_is_collected(self):
        """The entry point alone would not reproduce the hashed code — the
        helper's body is part of what the hash identifies."""
        _hash, entry, units = function_sources_for(_entry)

        assert entry == "_entry"
        assert set(units) == {"_entry", "_helper"}
        assert "value * 2" in units["_helper"]

    def test_editing_a_helper_reversions_its_caller(self, monkeypatch):
        """A helper's body is part of its caller's identity. That recursion is
        exactly why a version needs the whole closure stored and not one
        function — reconstituting the entry point alone would not reproduce the
        code the hash names.

        The edit is made in module globals because that is where the hasher
        looks; see ``test_a_locally_defined_callee_is_invisible`` below.
        """
        before_hash, entry, before_units = function_sources_for(_entry)

        def edited(value):
            return value * 3

        edited.__qualname__ = "_helper"
        monkeypatch.setitem(_entry.__globals__, "_helper", edited)

        after_hash, _entry_name, after_units = function_sources_for(_entry)

        assert after_hash != before_hash, "an edited helper must re-version its caller"
        assert before_units["_helper"] != after_units["_helper"]
        assert after_units[entry] == before_units[entry], (
            "the caller's own body did not change — only what it calls"
        )

    def test_a_locally_defined_callee_is_invisible(self):
        """Known hasher limitation, pinned here so it is not rediscovered as a
        bug in source capture.

        ``_resolve_call_target`` looks a call target up in the function's
        ``__globals__``, so a helper defined inside an enclosing function's
        scope resolves to nothing: it contributes neither to the hash nor to the
        captured closure. Listed as an accepted limitation in
        ``.claude/recursive-function-hashing.md``.

        The consequence is worth knowing and is NOT specific to source capture:
        editing a nested helper does not re-version its caller, so a node whose
        only change is inside one will not redden. Module-level helpers — the
        normal case, and the one every pipeline function uses — work correctly.
        """

        def local_helper(value):
            return value * 2

        def caller(value):
            return local_helper(value) + 1

        _hash, entry, units = function_sources_for(caller)

        assert list(units) == [entry], (
            "if this fails the hasher gained closure resolution — good news, "
            "but the limitation notes in docs/claude/variant-selection.md and "
            ".claude/recursive-function-hashing.md are then stale"
        )

    def test_a_matlab_style_object_uses_its_explicit_text(self):
        """Duck-typed, matching how `function_hash_for` handles `source_hash` —
        scidb must not import scimatlab."""

        class FakeMatlabFcn:
            source_hash = "deadbeef"
            source_text = "function y = f(x)\ny = x;\nend\n"
            __name__ = "f"

        fn_hash, entry, units = function_sources_for(FakeMatlabFcn())

        assert fn_hash == "deadbeef"
        assert entry == "f"
        assert units == {"f": "function y = f(x)\ny = x;\nend\n"}


# --- storage --------------------------------------------------------------


class TestStorage:
    def test_round_trips(self, db):
        insert_function_sources(
            db._duck, "abc123", {"f": "def f(): pass", "g": "def g(): pass"}, "f"
        )

        got = function_source(db._duck, "abc123")

        assert got["entry"] == "f"
        assert got["units"] == {"f": "def f(): pass", "g": "def g(): pass"}

    def test_uncaptured_is_distinguishable_from_empty(self, db):
        """Callers must be able to tell "never stored" from "stored as nothing",
        or the UI offers to re-run a version it cannot reconstitute."""
        assert function_source(db._duck, "never_stored") == {
            "entry": None,
            "units": {},
        }
        assert function_source(db._duck, "") == {"entry": None, "units": {}}

    def test_rewriting_the_same_hash_is_idempotent(self, db):
        insert_function_sources(db._duck, "abc123", {"f": "original"}, "f")
        insert_function_sources(db._duck, "abc123", {"f": "SHOULD NOT WIN"}, "f")

        assert function_source(db._duck, "abc123")["units"] == {"f": "original"}

    def test_two_versions_coexist(self, db):
        """A new version files beside the old, never over it — the old body is
        unrecoverable from disk by then."""
        insert_function_sources(db._duck, "v1hash", {"f": "def f(): return 1"}, "f")
        insert_function_sources(db._duck, "v2hash", {"f": "def f(): return 2"}, "f")

        assert function_source(db._duck, "v1hash")["units"]["f"].endswith("return 1")
        assert function_source(db._duck, "v2hash")["units"]["f"].endswith("return 2")

    def test_empty_units_writes_nothing(self, db):
        assert insert_function_sources(db._duck, "abc123", {}, None) == 0
        assert function_source(db._duck, "abc123")["units"] == {}


# --- the write path ------------------------------------------------------


class TestCaptureDuringForEach:
    def test_a_run_stores_its_own_source_under_its_own_hash(self, db):
        """End to end: the hash reachable from a saved record must resolve to
        the code that produced it."""
        Raw.save(np.array([1.0, 2.0]), subject="01")

        def double_it(raw):
            return float(np.sum(raw)) * 2

        for_each(double_it, inputs={"raw": Raw}, outputs=[Doubled], subject=[])

        row = db._duck._fetchone(
            "SELECT inv.function_hash FROM _invocation inv "
            "WHERE inv.function_name = 'double_it'"
        )
        assert row is not None, "the run should have written an invocation"

        got = function_source(db._duck, row[0])
        # Units are keyed by __qualname__, so a function defined inside a test
        # method carries its enclosing scope: `...<locals>.double_it`.
        assert got["entry"].endswith("double_it")
        assert "np.sum(raw)" in got["units"][got["entry"]]

    def test_an_edited_body_stores_a_second_version(self, db):
        """The comparison case: both bodies readable, filed under their own
        hashes, neither overwritten."""
        Raw.save(np.array([1.0, 2.0]), subject="01")

        def v1(raw):
            return float(np.sum(raw))

        def v2(raw):
            return float(np.sum(raw)) + 1000.0

        for body in (v1, v2):
            body.__name__ = "summarize"
            for_each(body, inputs={"raw": Raw}, outputs=[Doubled], subject=[])

        hashes = [
            r[0]
            for r in db._duck._fetchall(
                "SELECT DISTINCT function_hash FROM _invocation "
                "WHERE function_name = 'summarize'"
            )
        ]
        assert len(hashes) == 2, "a body edit must produce a second version"

        bodies = [function_source(db._duck, h)["units"] for h in hashes]
        captured = sorted(next(iter(u.values())) for u in bodies if u)
        assert len(captured) == 2
        assert any("1000.0" in body for body in captured)
        assert any("1000.0" not in body for body in captured)


# ---------------------------------------------------------------------------
# A digest with no text is "nothing to capture" (cleanup-audit F13)
# ---------------------------------------------------------------------------


def test_a_digest_only_stand_in_captures_nothing_and_reports_its_digest():
    """The MATLAB bridge hands for_each a Python sentinel carrying the .m
    file's digest as ``source_hash``. Hashing the sentinel's OWN body gave a
    hash that never matched the stored one ("the two recipes have drifted" on
    every MATLAB run) — and a match would have stored the sentinel as the
    user's code."""
    from scidb.foreach_config import function_sources_for

    def stand_in():
        raise RuntimeError("never called")

    stand_in.__name__ = "loadGaitRiteOneFile"
    stand_in.source_hash = "221afae652af0000"

    fn_hash, entry, units = function_sources_for(stand_in)
    assert fn_hash == "221afae652af0000"
    assert entry == "loadGaitRiteOneFile"
    assert units == {}
