"""Regression tests for the layout-file write race of 2026-09-01.

A single node drop sends TWO ``put_layout`` RPCs ~1ms apart — the create
(with node_type/label) from PipelineDAG's ``onDrop``, then a position-only
re-center once React Flow has measured the node — and ``server.py`` runs one
thread per RPC. Both handlers did an unguarded read-modify-write of the same
``*.layout.json``::

    11:00:52 [layout] Loading layout file from ...test_afl.layout.json   <- thread A
    11:00:52 [layout] Loading layout file from ...test_afl.layout.json   <- thread B
    11:00:52 [layout] Saving layout file to ...                          <- both write
    11:00:52 ERROR: RPC << put_layout FAILED: [Errno 13] Permission denied
    11:00:52 [layout] Layout file saved successfully                     <- the other won

Two defects, both covered here:

- The later writer silently discarded the earlier writer's edit. The
  Windows/SMB sharing violation the log caught is only one symptom: because
  the old ``_save`` opened the target with ``"w"`` (truncating), a
  concurrent READER on any platform could load an empty or half-written
  file. Reverting layout.py and running this module on macOS raises
  ``JSONDecodeError("Expecting value: line 1 column 1")`` out of ``_load``
  (verified 2026-09-01) -- hence ``_load`` holding the lock as well.
- ``write_manual_node`` wrote the JSON position BEFORE the structural DuckDB
  row, so the failing save aborted the function before the node was ever
  created. The node vanished on the next DAG refresh; the user re-dropped
  the same Raw_EMG node three times in two minutes.

See .claude/plan-layout-write-race-and-duplicate-seed-roots.md.

Since plan-canvas-as-document Stage 0a (2026-10-21) positions and notes are
DuckDB rows, not a JSON file: each write is one statement, so the lost-update
shape cannot recur by construction. These tests pin that the same concurrent
RPC shapes still lose nothing.
"""

import threading

import pytest

from scistack_gui import layout as layout_store
from scistack_gui import pipeline_store
from scistack_gui.db import get_db


def _hammer(targets, threads=8, iterations=10):
    """Run *targets* round-robin across threads; collect any exception."""
    errors: list[BaseException] = []
    barrier = threading.Barrier(threads)

    def run(idx):
        target = targets[idx % len(targets)]
        barrier.wait()  # maximise overlap, as the 1ms window did
        try:
            for i in range(iterations):
                target(idx, i)
        except BaseException as exc:  # noqa: BLE001 - recorded for assertion
            errors.append(exc)

    workers = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    return errors


def _saved_positions() -> dict:
    """Every saved position, merged across scopes, read from the database."""
    merged: dict = {}
    for scope in layout_store.read_positions_by_scope().values():
        merged.update(scope)
    return merged



class TestConcurrentWrites:
    def test_concurrent_position_writes_do_not_raise(self, layout_path):
        """The shape that produced the PermissionError on the share."""

        def write(idx, i):
            layout_store.write_node_position(f"var__N{idx}_{i}", idx * 1.0, i * 1.0)

        errors = _hammer([write])

        assert not errors, f"concurrent layout writes raised: {errors[:3]}"

    def test_concurrent_writes_do_not_lose_updates(self, layout_path):
        """The real defect: read-modify-write without a lock drops edits.

        Every thread writes its own node ids, so a correct implementation
        ends with all of them on disk. Unlocked, whichever thread saves last
        writes back a document it loaded before the others' edits landed.
        """
        threads, iterations = 8, 10

        def write(idx, i):
            layout_store.write_node_position(f"var__N{idx}_{i}", idx * 1.0, i * 1.0)

        errors = _hammer([write], threads=threads, iterations=iterations)
        assert not errors, f"concurrent layout writes raised: {errors[:3]}"

        expected = {
            f"var__N{idx}_{i}" for idx in range(threads) for i in range(iterations)
        }
        assert set(_saved_positions()) == expected

    def test_position_and_note_writes_do_not_clobber_each_other(self, layout_path):
        """Positions and notes are separate tables; neither write may clobber the other.

        Positions and notes never touch the same key, so an unlocked run
        loses whole notes/positions rather than corrupting either.
        """

        def write_position(idx, i):
            layout_store.write_node_position(f"var__P{idx}_{i}", 1.0, 2.0)

        def write_note(idx, i):
            layout_store.write_note(f"variable:V{idx}_{i}", "note text")

        errors = _hammer([write_position, write_note], threads=8, iterations=10)
        assert not errors, f"concurrent position+note writes raised: {errors[:3]}"

        positions = _saved_positions()
        # Threads alternate targets, so even indices wrote positions.
        expected_positions = {
            f"var__P{idx}_{i}" for idx in range(0, 8, 2) for i in range(10)
        }
        expected_notes = {
            f"variable:V{idx}_{i}" for idx in range(1, 8, 2) for i in range(10)
        }
        assert set(positions) == expected_positions
        assert set(layout_store.read_notes()) == expected_notes

    def test_drop_sequence_keeps_node_and_position(self, layout_path):
        """The exact 11:00:52 shape: create + re-center for ONE node id.

        Both RPCs land at once; afterwards the node must exist structurally
        AND have a position.
        """
        node_id = "var__Raw_EMG__ssh5rr"
        barrier = threading.Barrier(2)
        errors: list[BaseException] = []

        def create():
            barrier.wait()
            try:
                layout_store.write_manual_node(
                    node_id, 605.5, 125.8, "variableNode", "Raw_EMG"
                )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def recenter():
            barrier.wait()
            try:
                layout_store.write_node_position(node_id, 511.5, 97.8)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        workers = [threading.Thread(target=create), threading.Thread(target=recenter)]
class TestOneFile:
    def test_no_layout_sidecar_is_written(self, layout_path):
        """Positions, the palette and notes all live in the .duckdb (user
        requirement 2026-10-21: one file per project)."""
        layout_store.write_node_position("var__A", 1.0, 2.0)
        layout_store.write_note("variable:A", "a note")
        layout_store.write_constant("cutoff")
        assert not layout_path.exists()
        assert not list(layout_path.parent.glob("*.layout.json*"))

    def test_an_old_sidecar_is_ignored(self, layout_path):
        """Clean break: an existing .layout.json is not read."""
        layout_path.write_text('{"positions": {"main": {"var__Old": {"x": 1, "y": 2}}}}')
        assert "var__Old" not in _saved_positions()


class TestManualNodeWriteOrder:
    def test_manual_node_survives_position_write_failure(
        self, layout_path, monkeypatch
    ):
        """The node-vanishing regression.

        The position write used to run first, so a failing save aborted
        ``write_manual_node`` before the structural DuckDB row existed and
        the node was never created at all. The structural row is what makes
        the node exist; the position is cosmetic and recoverable.
        """

        def denied(*_args):
            raise RuntimeError("position write failed")

        monkeypatch.setattr(layout_store, "_upsert_position", denied)

        with pytest.raises(RuntimeError):
            layout_store.write_manual_node(
                "var__Raw_EMG__abqvvj", 672.6, 128.9, "variableNode", "Raw_EMG"
            )

        monkeypatch.undo()
        assert "var__Raw_EMG__abqvvj" in pipeline_store.get_manual_nodes(get_db())

    def test_manual_node_writes_both_on_success(self, layout_path):
        """The reorder must not change the successful path."""
        layout_store.write_manual_node(
            "var__Raw_EMG__vipabc", 690.9, 147.9, "variableNode", "Raw_EMG"
        )

        assert "var__Raw_EMG__vipabc" in pipeline_store.get_manual_nodes(get_db())
        assert _saved_positions()["var__Raw_EMG__vipabc"] == {
            "x": 690.9,
            "y": 147.9,
        }
