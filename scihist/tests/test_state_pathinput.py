"""Tests for check_node_state with PathInput-only functions.

PathInput-only functions have **no DB-variable inputs**, so there is no live
source for the set of combos they *should* produce (the filesystem state at
for_each time is gone by query time, and nothing in the DB enumerates it). The
persisted ``_for_each_expected`` snapshot that used to supply this was removed
(it stored a predicted invocation_id that had to equal a separately-realized one
— a drift hazard; see
.claude/remove-for-each-expected-and-trim-record-metadata.md).

Consequence — the contract these tests pin: a PathInput-only loader's expected
set is exactly the invocations it has **realized**. So it reports **green** when
it has run (any combos) and **red** when it never has. It can NEVER report grey:
a partially-run loader looks green, because the combos that were never run leave
no trace to count as missing.
"""

from scihist.state import check_node_state, check_pathinput_node_state

from scidb import BaseVariable, exclude_schema, scistack
from scihist import for_each

# ---------------------------------------------------------------------------
# Variable types
# ---------------------------------------------------------------------------


class PathInputOutput(BaseVariable):
    schema_version = 1


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------


@scistack
def import_from_file(filepath):
    """PathInput-only function: reads a single number from a file."""
    with open(filepath) as fh:
        return float(fh.read().strip())


# PathInput template + file helpers (real for_each path). Using the real path
# means the resolved filepath is excluded from graph constants at the source, so
# the loader's invocations have no variable-input edges and node-state treats
# them as inputless (realized == expected).
_GRID = [("1", "A"), ("1", "B"), ("2", "A"), ("2", "B")]


def _write_combo_files(root, combos):
    """Create ``sub{subject}/trial{trial}.txt`` files under ``root``."""
    from pathlib import Path

    for i, (subj, trial) in enumerate(combos):
        d = Path(root) / f"sub{subj}"
        d.mkdir(parents=True, exist_ok=True)
        (d / f"trial{trial}.txt").write_text(str(float(i + 1)))


def _path_input(root):
    from scifor import PathInput

    return PathInput("sub{subject}/trial{trial}.txt", root_folder=str(root), name="sub{subject}/trial{trial}.txt")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCheckNodeStatePathInput:
    """Integration tests: check_node_state with a real PathInput-only function.

    Drives the actual for_each + PathInput path (not a per-combo save
    simulation). The loader's expected set is its realized invocations, so it is
    green-when-run / red-when-not, never grey.
    """

    def _run(self, db, tmp_path, files):
        """Write files for ``files`` combos, then run import_from_file over the
        full 2×2 grid via the real for_each + PathInput path."""
        _write_combo_files(tmp_path, files)
        for_each(
            import_from_file,
            inputs={"filepath": _path_input(tmp_path)},
            outputs=[PathInputOutput],
            subject=["1", "2"],
            trial=["A", "B"],
            db=db,
        )

    def test_green_when_all_succeed(self, db, tmp_path):
        """All 4 combos have files → green."""
        self._run(db, tmp_path, _GRID)

        result = check_node_state(import_from_file, [PathInputOutput], db=db)
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 4
        assert result["counts"]["missing"] == 0

    def test_green_when_partial_success(self, db, tmp_path):
        """3/4 combos have files → still green (a loader cannot detect the
        un-run 4th combo: it left no trace, so there is nothing to count as
        missing). This is the accepted limitation of dropping the persisted
        expected-combo snapshot."""
        self._run(db, tmp_path, [("1", "A"), ("1", "B"), ("2", "B")])

        result = check_node_state(import_from_file, [PathInputOutput], db=db)
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 3
        assert result["counts"]["missing"] == 0

    def test_red_when_none_succeed(self, db, tmp_path):
        """No files → no combos run → no realized invocations → red."""
        from pathlib import Path

        Path(tmp_path).mkdir(parents=True, exist_ok=True)
        for_each(
            import_from_file,
            inputs={"filepath": _path_input(tmp_path)},
            outputs=[PathInputOutput],
            subject=["1", "2"],
            trial=["A", "B"],
            db=db,
        )

        result = check_node_state(import_from_file, [PathInputOutput], db=db)
        assert result["state"] == "red"
        assert result["counts"]["up_to_date"] == 0

    def test_rerun_over_subset_stays_green(self, db, tmp_path):
        """Re-run over a smaller grid; prior records remain → green.

        The 4 records from the first run still exist as realized invocations, so
        the loader's expected set (its realized invocations) is fully present →
        green, regardless of the second, smaller run.
        """
        self._run(db, tmp_path, _GRID)

        # Re-run over a 2-combo subset.
        for_each(
            import_from_file,
            inputs={"filepath": _path_input(tmp_path)},
            outputs=[PathInputOutput],
            subject=["1"],
            trial=["A", "B"],
            db=db,
        )

        result = check_node_state(import_from_file, [PathInputOutput], db=db)
        assert result["state"] == "green"
        assert result["counts"]["missing"] == 0


class TestCheckPathInputNodeState:
    """The explicit PathInput outdated check (``check_pathinput_node_state``).

    Unlike ``check_node_state`` (which reads green for a partially-run loader),
    this check reconstructs the *should-run* set as ``PathInput.discover()`` ∩ the
    iteration grid (minus exclusions) — i.e. exactly what for_each would produce
    now — and goes **red** when an in-grid file has not yet been produced.
    """

    def _path(self, tmp_path):
        return {"filepath": _path_input(tmp_path)}

    def _run_full(self, db, tmp_path):
        """Files + run over the 2×2 grid → realized = {1,2}×{A,B}."""
        _write_combo_files(tmp_path, _GRID)
        for_each(
            import_from_file,
            inputs=self._path(tmp_path),
            outputs=[PathInputOutput],
            subject=["1", "2"],
            trial=["A", "B"],
            db=db,
        )

    def test_green_when_all_realized(self, db, tmp_path):
        """Every discovered ∩ grid combo has produced output → green."""
        self._run_full(db, tmp_path)
        result = check_pathinput_node_state(
            import_from_file,
            [PathInputOutput],
            self._path(tmp_path),
            db=db,
            subject=["1", "2"],
            trial=["A", "B"],
        )
        assert result["state"] == "green"
        assert result["counts"] == {"up_to_date": 4, "stale": 0, "missing": 0}

    def test_red_when_new_in_grid_file_appears(self, db, tmp_path):
        """A new file for a combo *inside* the grid, not yet produced → red."""
        self._run_full(db, tmp_path)
        _write_combo_files(tmp_path, [("3", "A")])  # new on-disk data for subject 3

        # The grid now includes subject 3, so the new file is in scope.
        result = check_pathinput_node_state(
            import_from_file,
            [PathInputOutput],
            self._path(tmp_path),
            db=db,
            subject=["1", "2", "3"],
            trial=["A", "B"],
        )
        assert result["state"] == "red"
        missing = [
            c["schema_combo"] for c in result["combos"] if c["state"] == "missing"
        ]
        assert {"subject": "3", "trial": "A"} in missing

    def test_new_file_outside_grid_stays_green(self, db, tmp_path):
        """A new file *outside* the grid is not iterated → stays green."""
        self._run_full(db, tmp_path)
        _write_combo_files(tmp_path, [("3", "A")])  # subject 3 is NOT in the grid below

        result = check_pathinput_node_state(
            import_from_file,
            [PathInputOutput],
            self._path(tmp_path),
            db=db,
            subject=["1", "2"],
            trial=["A", "B"],
        )
        assert result["state"] == "green"
        assert result["counts"]["missing"] == 0

    def test_fileless_grid_combo_stays_green(self, db, tmp_path):
        """A grid combo with no file produces nothing → not expected → green."""
        self._run_full(db, tmp_path)
        # Grid declares subject 3, but no file exists for it.
        result = check_pathinput_node_state(
            import_from_file,
            [PathInputOutput],
            self._path(tmp_path),
            db=db,
            subject=["1", "2", "3"],
            trial=["A", "B"],
        )
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 4
        assert result["counts"]["missing"] == 0

    def test_exclusion_flips_back_to_green(self, db, tmp_path):
        """Excluding the new in-grid data drops it from should → green again."""
        self._run_full(db, tmp_path)
        _write_combo_files(tmp_path, [("3", "A")])

        grid = {"subject": ["1", "2", "3"], "trial": ["A", "B"]}
        red = check_pathinput_node_state(
            import_from_file,
            [PathInputOutput],
            self._path(tmp_path),
            db=db,
            **grid,
        )
        assert red["state"] == "red"

        exclude_schema("not part of this analysis", db=db, subject="3")
        green = check_pathinput_node_state(
            import_from_file,
            [PathInputOutput],
            self._path(tmp_path),
            db=db,
            **grid,
        )
        assert green["state"] == "green"
        assert green["counts"]["missing"] == 0


# ---------------------------------------------------------------------------
# Function-body edits (Stage 6)
#
# A PathInput-only loader used to be exempt from code-change detection
# entirely: its expected set came from `realized_inputless_invocations`, which
# read the graph structurally with no reference to fn_hash, so expected was
# identically present and the node reported green however the body changed.
# A function WITH variable inputs never had this hole — `fn_hash` is folded
# into `invocation_id` by the prediction path, which inputless configs skip.
# ---------------------------------------------------------------------------


@scistack
def _import_edited(filepath):
    """Same loader, different body — models the user editing and saving."""
    with open(filepath) as fh:
        return float(fh.read().strip()) * 2


# `__fn` comes from __name__ while function_hash is an AST hash of the source,
# so this is one function with two versions, not two functions.
_import_edited.__name__ = "import_from_file"


class TestBodyEditRedensAPathInputLoader:
    def _run(self, fn, db, tmp_path):
        for_each(
            fn,
            inputs={"filepath": _path_input(tmp_path)},
            outputs=[PathInputOutput],
            subject=["1", "2"],
            trial=["A", "B"],
            db=db,
        )

    def test_green_before_the_edit(self, db, tmp_path):
        _write_combo_files(tmp_path, _GRID)
        self._run(import_from_file, db, tmp_path)

        assert check_node_state(import_from_file, [PathInputOutput], db=db)["state"] == "green"

    def test_red_after_the_edit(self, db, tmp_path):
        """The reported bug: edit the body, and the node stayed green."""
        _write_combo_files(tmp_path, _GRID)
        self._run(import_from_file, db, tmp_path)

        result = check_node_state(_import_edited, [PathInputOutput], db=db)
        assert result["state"] == "red"
        assert result["counts"]["up_to_date"] == 0

    def test_green_again_after_re_running_the_edited_body(self, db, tmp_path):
        """And it must not get STUCK red — re-running is the way out."""
        _write_combo_files(tmp_path, _GRID)
        self._run(import_from_file, db, tmp_path)
        assert check_node_state(_import_edited, [PathInputOutput], db=db)["state"] == "red"

        self._run(_import_edited, db, tmp_path)

        result = check_node_state(_import_edited, [PathInputOutput], db=db)
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 4

    def test_two_historical_hashes_do_not_confuse_the_current_one(self, db, tmp_path):
        """≥2 historical hashes for one combo — the case that made the OLD
        equality-based staleness check misfire, and the reason that check was
        abandoned. Selection here is by exact hash, not by "any recorded row",
        so old versions coexisting is simply history."""
        _write_combo_files(tmp_path, _GRID)
        self._run(import_from_file, db, tmp_path)
        self._run(_import_edited, db, tmp_path)

        from scidb.provenance_query import function_versions_recorded

        assert len(function_versions_recorded(db._duck, "import_from_file")) == 2

        # Whichever version you ask about, you get ITS answer — both are green
        # here because both have actually run over the whole grid.
        for fn in (import_from_file, _import_edited):
            result = check_node_state(fn, [PathInputOutput], db=db)
            assert result["state"] == "green"
            assert result["counts"]["up_to_date"] == 4

    def test_reverting_the_edit_restores_green(self, db, tmp_path):
        """A hash is an identity, not a timestamp: going back to code that has
        already run is up to date, not stale."""
        _write_combo_files(tmp_path, _GRID)
        self._run(import_from_file, db, tmp_path)

        assert check_node_state(_import_edited, [PathInputOutput], db=db)["state"] == "red"
        assert check_node_state(import_from_file, [PathInputOutput], db=db)["state"] == "green"

    def test_call_id_scoping_still_narrows_to_the_current_version(self, db, tmp_path):
        """The GUI always passes call_id, so the fix has to hold there too.

        `function_variant_configs` dedupes configs *fn-hash-independently*, so
        one call site's `invocation_ids` span BOTH versions — the hash filter
        is what narrows within the site. If the two ever stopped composing,
        the GUI would silently keep reporting green.
        """
        from scidb.foreach_config import ForEachConfig, function_hash_for
        from scifor import PathInput  # noqa: F401 — used via _path_input

        _write_combo_files(tmp_path, _GRID)
        self._run(import_from_file, db, tmp_path)

        call_id = ForEachConfig(
            import_from_file, {"filepath": _path_input(tmp_path)}
        ).to_call_id()

        assert (
            check_node_state(
                import_from_file, [PathInputOutput], db=db, call_id=call_id
            )["state"]
            == "green"
        )
        # Same call site, edited body → needs re-running.
        assert (
            check_node_state(
                _import_edited, [PathInputOutput], db=db, call_id=call_id
            )["state"]
            == "red"
        )
        # ...and the two versions really are one call site, not two.
        assert function_hash_for(import_from_file) != function_hash_for(_import_edited)
