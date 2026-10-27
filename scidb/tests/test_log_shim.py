"""Regression: scidb.log must keep re-exporting the scistacklog facade.

MATLAB's +scidb/Log.m delegates via py.scidb.log.Log.*, and many modules
import ``from scidb.log import Log`` — this import path is a public contract
even though the implementation moved to the scistacklog package.

Also pins the level policy contracts that Stage 3 established:
- configure_database writes a run-context header and the log file
- [timing] summaries are INFO (present in the file at the default level);
  the MATLAB timing-archive grep workflow depends on them
- per-record/per-column internals are DEBUG (absent at the default level)
"""

import numpy as np
from scidb.log import Log as shim_log

import scidb
from scistacklog import Log as real_log


def test_scidb_log_reexports_scistacklog_facade():
    assert shim_log is real_log


def test_scidb_package_export_matches():
    assert scidb.Log is real_log


class _ShimVar(scidb.BaseVariable):
    schema_version = 1


def test_configure_database_writes_run_context_header(db, tmp_path):
    log_text = (tmp_path / "scistack.log").read_text(encoding="utf-8")
    assert "configure_database:" in log_text
    assert "python=" in log_text
    assert "pid=" in log_text


def test_timing_summary_in_file_at_default_level(db, tmp_path):
    db.save_batch(
        _ShimVar,
        [
            (np.array([1.0, 2.0]), {"subject": "S01", "trial": "1"}),
            (np.array([3.0, 4.0]), {"subject": "S01", "trial": "2"}),
        ],
    )
    log_text = (tmp_path / "scistack.log").read_text(encoding="utf-8")
    assert "[timing] save_batch(_ShimVar):" in log_text
    # Per-phase timing table is DEBUG-only (and phase names carry no
    # numbering — snake_case operations only).
    assert "  save_batch canonical_hash" not in log_text


def test_per_record_internals_absent_at_default_level(db, tmp_path):
    _ShimVar.save(np.array([1.0, 2.0]), subject="S01", trial="1")
    log_text = (tmp_path / "scistack.log").read_text(encoding="utf-8")
    # DEBUG-tier internals must not reach the file at the INFO default.
    assert "save_variable(_ShimVar): metadata=" not in log_text
    assert "[content_hash]" not in log_text
    # But the one-line save outcome (INFO) is present.
    assert "save_variable(_ShimVar): saved -> record_id=" in log_text


# -- early attach -----------------------------------------------------------
#
# The file sink used to be opened only by configure_database(), i.e. after a
# caller had already done work worth logging. The GUI server's whole startup
# discovery pass (the registry scan that decides which functions, variables
# and PathInputs exist) landed on stderr only, so scistack.log began mid-startup
# and could not answer "why is this PathInput missing?". attach_log_file lets
# a caller open the sink first; configure_database must then leave it alone.


def test_log_path_for_is_beside_the_database(tmp_path):
    from scidb.log import log_path_for

    assert log_path_for(tmp_path / "proj.duckdb") == tmp_path / "scistack.log"


def test_attach_log_file_opens_the_sink_before_any_database(tmp_path):
    from scidb.log import attach_log_file

    try:
        path = attach_log_file(tmp_path / "not_created_yet.duckdb")
        assert path == tmp_path / "scistack.log"
        assert real_log.get_path() == str(path)
        real_log.info("before configure_database")
        assert "before configure_database" in path.read_text(encoding="utf-8")
    finally:
        real_log.set_path(None)


def test_attach_log_file_is_idempotent(tmp_path):
    """Re-attaching the same path must keep the SAME handler.

    configure_database() calls this after the GUI server already has, and a
    tear-down/reopen there would drop buffered output and churn the file.
    """
    from scidb.log import attach_log_file

    try:
        attach_log_file(tmp_path / "proj.duckdb")
        handler = real_log._file_handler
        attach_log_file(tmp_path / "proj.duckdb")
        assert real_log._file_handler is handler
    finally:
        real_log.set_path(None)


def test_setup_phase_timings_reach_the_file(tmp_path):
    """Connection setup must be timed, and timed INTO the log file.

    A MATLAB run pays the whole of configure_database on every invocation —
    the generated script closes the database at the end to hand the write
    lock back to the GUI, so nothing is carried over. Attributing that cost
    (DuckDB open vs. the idempotent DDL re-run against a network share)
    needs the phase breakdown, and needs it in the file rather than on
    stderr: MATLAB is usually the FIRST scidb caller in its process, so
    without the early attach these lines are written before any sink exists
    and are lost exactly when they are wanted.
    """
    real_log.set_path(None)
    db_path = tmp_path / "timing.duckdb"

    db = scidb.configure_database(str(db_path), ["subject"])
    try:
        log_text = (tmp_path / "scistack.log").read_text(encoding="utf-8")
    finally:
        db.close()
        real_log.set_path(None)

    # Both levels of the breakdown, at the default (INFO) level: the outer
    # call and the connection construction inside it.
    assert "[timing] configure_database:" in log_text
    assert "[timing] DatabaseManager.__init__:" in log_text
    # The INFO summary carries its phases inline, so the expensive step is
    # named without needing DEBUG.
    assert "database_manager=" in log_text
    assert "register_types=" in log_text
    assert "duck_open=" in log_text
    assert "ensure_provenance_tables=" in log_text


def test_configure_database_keeps_an_already_attached_sink(tmp_path):
    """The early-attached file keeps receiving records after DB setup."""
    from scidb.log import attach_log_file

    db_path = tmp_path / "proj.duckdb"
    log_path = attach_log_file(db_path)
    real_log.info("discovery ran before the database existed")
    handler = real_log._file_handler

    db = scidb.configure_database(str(db_path), ["subject"])
    try:
        assert real_log._file_handler is handler
        log_text = log_path.read_text(encoding="utf-8")
        assert "discovery ran before the database existed" in log_text
        assert "configure_database:" in log_text
    finally:
        db.close()
        real_log.set_path(None)
