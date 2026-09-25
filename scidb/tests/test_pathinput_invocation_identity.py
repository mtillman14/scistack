"""A PathInput is identified by its NAME (2026-09-25).

Found on a real canvas: two ``pandas.read_csv`` nodes reading two different
files saved into ONE invocation, because the PathInput was not part of
``invocation_id`` and a PathInput-only call has no other edge.
``pipeline_variants`` then reported one call site with both outputs and only
the file read last; its wiring matched neither node, and the GUI rewrote both
nodes' edges onto a third (docs/claude/identity-layers-pathinput.md).

The fix makes the PathInput part of identity by its required ``name=`` —
never its template or root folder — so:

* two differently named PathInputs are two invocations;
* the same PathInput whose files moved (another machine, a new data root) is
  the SAME invocation and the same records, and ``skip_computed`` skips it;
* the stored spec follows the files, for display and re-discovery.
"""

from __future__ import annotations

import pytest
import scifor as _scifor

from scidb import BaseVariable, EachOf, PathInput, configure_database, for_each
from scidb import provenance_query as pq
from scidb.state import check_node_state

SUBJECTS = ["01", "02"]


class TableA(BaseVariable):
    pass


class TableB(BaseVariable):
    pass


class Scaled(BaseVariable):
    pass


def read_number(filepath):
    with open(filepath) as handle:
        return float(handle.read().strip())


def scale_by_file(value, filepath):
    with open(filepath) as handle:
        return float(value) * float(handle.read().strip())


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "pi_identity.duckdb", ["subject"])
    yield db
    _scifor.set_schema([])
    db.close()


def _tree(root, files: dict):
    for subject in SUBJECTS:
        (root / subject).mkdir(parents=True, exist_ok=True)
        for name, text in files.items():
            (root / subject / name).write_text(text)
    return root


@pytest.fixture
def root(tmp_path):
    return _tree(tmp_path / "data", {"a.txt": "1", "b.txt": "2", "factor.txt": "10"})


def _invocations_by_output(db, fn_name: str) -> dict[str, set[str]]:
    rows = db._duck._fetchall(
        "SELECT r.type, io.invocation_id FROM _invocation_output io "
        "JOIN _invocation i ON i.invocation_id = io.invocation_id "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "WHERE i.function_name = ?",
        [fn_name],
    )
    out: dict[str, set[str]] = {}
    for rtype, inv in rows:
        out.setdefault(rtype, set()).add(inv)
    return out


def _record_ids(db, type_name: str) -> set[str]:
    return {
        r[0]
        for r in db._duck._fetchall("SELECT record_id FROM _record WHERE type = ?", [type_name])
    }


def _run_both(root):
    for_each(
        read_number,
        {"filepath": PathInput("{subject}/a.txt", root_folder=str(root), name="FileA")},
        [TableA],
        subject=SUBJECTS,
    )
    for_each(
        read_number,
        {"filepath": PathInput("{subject}/b.txt", root_folder=str(root), name="FileB")},
        [TableB],
        subject=SUBJECTS,
    )


class TestTheNameIsRequired:
    def test_constructor_refuses_no_name(self):
        with pytest.raises(TypeError):
            PathInput("{subject}/a.txt")  # type: ignore[call-arg]

    @pytest.mark.parametrize("bad", ["", "   ", " padded"])
    def test_constructor_refuses_a_blank_or_padded_name(self, bad):
        with pytest.raises(ValueError):
            PathInput("{subject}/a.txt", name=bad)

    def test_key_is_the_name_alone(self, tmp_path):
        here = PathInput("{s}/a.txt", root_folder=str(tmp_path / "x"), name="Raw")
        there = PathInput("elsewhere/{s}.csv", root_folder="/mnt/other", name="Raw")
        assert here.to_key() == there.to_key()
        assert "template" not in here.to_key()
        assert '"template"' in here.to_spec() and '"name": "Raw"' in here.to_spec()


class TestTwoNamesAreTwoInvocations:
    def test_outputs_hang_on_disjoint_invocations(self, db, root):
        _run_both(root)
        by_output = _invocations_by_output(db, "read_number")
        assert set(by_output) == {"TableA", "TableB"}
        assert by_output["TableA"].isdisjoint(by_output["TableB"]), (
            f"one invocation produced both tables: {by_output}"
        )

    def test_each_invocation_stores_its_own_spec(self, db, root):
        _run_both(root)
        by_output = _invocations_by_output(db, "read_number")
        for rtype, template in (("TableA", "a.txt"), ("TableB", "b.txt")):
            for inv in by_output[rtype]:
                specs = pq.invocation_path_inputs(db._duck, inv)
                assert list(specs) == ["filepath"], specs
                assert template in specs["filepath"], (rtype, specs)

    def test_pipeline_variants_reports_two_call_sites(self, db, root):
        """The GUI's view: two call sites, each with ONE output and its own
        PathInput — not one site drawing both outputs from the last file."""
        _run_both(root)
        rows = [v for v in pq.pipeline_variants(db._duck) if v["function_name"] == "read_number"]
        by_output = {v["output_type"]: v for v in rows}
        assert set(by_output) == {"TableA", "TableB"}
        assert by_output["TableA"]["call_id"] != by_output["TableB"]["call_id"]
        assert by_output["TableA"]["path_input_names"] == {"filepath": "FileA"}
        assert by_output["TableB"]["path_input_names"] == {"filepath": "FileB"}

    def test_rerun_of_the_same_pathinput_reuses_the_invocation(self, db, root):
        _run_both(root)
        before = _invocations_by_output(db, "read_number")
        _run_both(root)
        assert _invocations_by_output(db, "read_number") == before


class TestMovedDataKeepsItsIdentity:
    """The portability guarantee: another machine, or a moved data root."""

    def _run(self, root, **kw):
        for_each(
            read_number,
            {"filepath": PathInput("{subject}/a.txt", root_folder=str(root), name="FileA")},
            [TableA],
            subject=SUBJECTS,
            **kw,
        )

    def test_same_invocation_and_same_records(self, db, root, tmp_path):
        self._run(root)
        invs, rids = _invocations_by_output(db, "read_number"), _record_ids(db, "TableA")
        moved = _tree(tmp_path / "moved", {"a.txt": "1"})
        self._run(moved)
        assert _invocations_by_output(db, "read_number") == invs
        assert _record_ids(db, "TableA") == rids

    def test_skip_computed_skips_moved_files(self, db, root, tmp_path):
        self._run(root)
        saves = db._duck._fetchone("SELECT COUNT(*) FROM _record_save")[0]
        # Different CONTENT under the new root: a skip that looked at the
        # location would recompute and save 5.0.
        moved = _tree(tmp_path / "moved", {"a.txt": "5"})
        self._run(moved, skip_computed=True)
        assert db._duck._fetchone("SELECT COUNT(*) FROM _record_save")[0] == saves
        assert TableA.load(subject="01").data == 1.0

    def test_the_stored_spec_follows_the_files(self, db, root, tmp_path):
        self._run(root)
        moved = _tree(tmp_path / "moved", {"a.txt": "1"})
        self._run(moved)
        [inv] = _invocations_by_output(db, "read_number")["TableA"]
        assert str(moved) in pq.invocation_path_inputs(db._duck, inv)["filepath"]

    def test_a_rename_is_a_different_pathinput(self, db, root):
        self._run(root)
        before = _invocations_by_output(db, "read_number")["TableA"]
        for_each(
            read_number,
            {"filepath": PathInput("{subject}/a.txt", root_folder=str(root), name="Renamed")},
            [TableA],
            subject=SUBJECTS,
        )
        assert _invocations_by_output(db, "read_number")["TableA"] - before


class TestOneNamePerCall:
    def test_two_arguments_with_one_name_and_different_files_are_refused(self, db, root):
        def two(a, b):
            return 0.0

        with pytest.raises(ValueError, match="FileA"):
            for_each(
                two,
                {
                    "a": PathInput("{subject}/a.txt", root_folder=str(root), name="FileA"),
                    "b": PathInput("{subject}/b.txt", root_folder=str(root), name="FileA"),
                },
                [TableA],
                subject=SUBJECTS,
            )

    def test_eachof_alternatives_share_one_name(self, db, tmp_path):
        """One PathInput in two places (assessment / training folders)."""
        first = _tree(tmp_path / "one", {"a.txt": "1"})
        second = tmp_path / "two"
        (second / "03").mkdir(parents=True)
        (second / "03" / "a.txt").write_text("3")
        spread = EachOf(
            PathInput("{subject}/a.txt", root_folder=str(first), name="FileA"),
            PathInput("{subject}/a.txt", root_folder=str(second), name="FileA"),
        )
        for_each(read_number, {"filepath": spread}, [TableA], subject=[])
        assert len(_invocations_by_output(db, "read_number")["TableA"]) == 1


class TestPredictionIncludesThePathInput:
    def test_mixed_pathinput_and_variable_input_is_green_after_run(self, db, root):
        """A PathInput beside a variable input goes through the PREDICT side,
        and the never-run fallback (`config_from_inputs`) must carry the
        PathInput too: without it the node predicted extra, PathInput-less
        invocations and stayed red after a full run ('missing': 2)."""
        for subject in SUBJECTS:
            TableA.save(1.0, subject=subject)
        pi = PathInput("{subject}/factor.txt", root_folder=str(root), name="Factor")
        inputs = {"value": TableA, "filepath": pi}
        for_each(scale_by_file, inputs, [Scaled], subject=SUBJECTS)
        result = check_node_state(scale_by_file, [Scaled], inputs=inputs, db=db)
        assert result["state"] == "green", result["counts"]
