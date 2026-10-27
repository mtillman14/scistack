"""
Tests for the DuckDB connection lifecycle — verifies that the Python
process releases the file lock between requests so MATLAB can access
the same database.
"""

import duckdb
import pytest


@pytest.fixture
def db_path(tmp_path):
    """Create a minimal SciStack database for lifecycle tests."""
    p = tmp_path / "test.duckdb"
    con = duckdb.connect(str(p))
    con.execute("CREATE TABLE _schema (schema_id INTEGER, subject INTEGER)")
    con.execute("INSERT INTO _schema VALUES (1, 1)")
    con.close()
    return p


@pytest.fixture(autouse=True)
def _reset_db_module():
    """Reset db.py module-level state between tests."""
    from scistack_gui import db as db_mod

    old_db = db_mod._db
    old_path = db_mod._db_path
    old_open = db_mod._db_open
    old_ref = db_mod._db_refcount
    yield
    db_mod._db = old_db
    db_mod._db_path = old_path
    db_mod._db_open = old_open
    db_mod._db_refcount = old_ref


class TestAcquireReleaseCycle:
    """Verify that acquire/release correctly opens and closes the connection."""

    def test_release_closes_when_refcount_zero(self, db_path):
        import scistack_gui.db as db_mod
        from scistack_gui.db import (
            acquire_db_connection,
            init_db,
            release_db_connection,
        )

        init_db(db_path)
        assert db_mod._db_open is True

        # Simulate close_initial_connection
        db_mod._db._duck.close()
        db_mod._db_open = False

        # Acquire opens it
        acquire_db_connection()
        assert db_mod._db_open is True
        assert db_mod._db_refcount == 1

        # Release closes it
        release_db_connection()
        assert db_mod._db_open is False
        assert db_mod._db_refcount == 0

    def test_nested_acquire_keeps_open(self, db_path):
        import scistack_gui.db as db_mod
        from scistack_gui.db import (
            acquire_db_connection,
            init_db,
            release_db_connection,
        )

        init_db(db_path)
        db_mod._db._duck.close()
        db_mod._db_open = False

        acquire_db_connection()
        acquire_db_connection()
        assert db_mod._db_refcount == 2

        release_db_connection()
        assert db_mod._db_open is True  # still held by 1 caller
        assert db_mod._db_refcount == 1

        release_db_connection()
        assert db_mod._db_open is False
        assert db_mod._db_refcount == 0

    def test_second_process_can_open_after_release(self, db_path):
        """After Python releases the lock, another connection can open the file."""
        from scistack_gui.db import (
            acquire_db_connection,
            init_db,
            release_db_connection,
        )

        init_db(db_path)
        acquire_db_connection()
        release_db_connection()

        # Simulate MATLAB opening the same file
        con2 = duckdb.connect(str(db_path))
        rows = con2.execute("SELECT COUNT(*) FROM _schema").fetchall()
        assert rows[0][0] >= 1
        con2.close()

    def test_reacquire_after_external_close(self, db_path):
        """Python can reacquire the connection after MATLAB releases it."""
        import scistack_gui.db as db_mod
        from scistack_gui.db import (
            acquire_db_connection,
            init_db,
            release_db_connection,
        )

        db = init_db(db_path)
        acquire_db_connection()
        release_db_connection()
        # Connection now closed

        # Simulate MATLAB opening and closing
        con2 = duckdb.connect(str(db_path))
        con2.execute("CREATE TABLE IF NOT EXISTS _test (x INTEGER)")
        con2.close()

        # Python reacquires
        acquire_db_connection()
        assert db_mod._db_open is True
        # Can query the table MATLAB created
        rows = db._duck._fetchall("SELECT * FROM _test")
        assert rows is not None
        release_db_connection()


class TestAcquireRaisesDoesNotLeakRefcount:
    """Regression: a failed reopen (e.g., external process still holds the
    DuckDB lock) must not leak the refcount, or subsequent acquires will
    keep the lock permanently held and MATLAB can never reopen the file.

    Reproduces the leak seen in scistack.log where a ``get_pipeline`` RPC
    fired while MATLAB held the lock, ``duckdb.connect()`` raised inside
    ``_db.reopen()``, and the pre-incremented refcount was never rolled
    back.  The next successful acquire then recorded ``refcount=2`` with
    ``reopened=True`` — proof that one ghost holder was stuck at 1.

    A lock conflict now surfaces as :class:`DatabaseLockedError` instead of
    the raw ``OSError`` DuckDB raises, so the JSON-RPC dispatcher can give
    it its own error code and an actionable message rather than a generic
    failure. The refcount contract this class guards is unchanged — that is
    still what the assertions below are about.
    """

    def test_reopen_failure_does_not_increment_refcount(self, db_path):
        import scistack_gui.db as db_mod
        from scistack_gui.db import (
            DatabaseLockedError,
            acquire_db_connection,
            init_db,
            release_db_connection,
        )

        init_db(db_path)
        db_mod._db._duck.close()
        db_mod._db_open = False
        assert db_mod._db_refcount == 0

        # Make reopen() fail the way duckdb.connect() does under a lock conflict.
        def _boom():
            raise OSError("Could not set lock on file (simulated)")

        original_reopen = db_mod._db._duck.reopen
        db_mod._db._duck.reopen = _boom
        try:
            # timeout=0: no point backing off against a lock that, in this
            # test, never clears.
            with pytest.raises(DatabaseLockedError):
                acquire_db_connection(timeout=0)
        finally:
            db_mod._db._duck.reopen = original_reopen

        # The failed acquire must NOT have incremented the refcount.
        assert db_mod._db_refcount == 0, (
            f"refcount leaked to {db_mod._db_refcount} after failed reopen"
        )
        assert db_mod._db_open is False

        # A subsequent successful acquire should still see refcount go 0 → 1.
        acquire_db_connection()
        assert db_mod._db_refcount == 1
        assert db_mod._db_open is True

        release_db_connection()
        assert db_mod._db_refcount == 0
        assert db_mod._db_open is False


class TestMatlabCommandIncludesCleanup:
    """Verify the generated MATLAB script always closes the DB.

    The script uses the ``scidb.close_database`` helper (not a bare
    ``db.close()``) so the lock-release log fires exactly when the lock
    actually drops — see ``+scidb/close_database.m``.
    """

    def test_template_has_close(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="my_func",
            db_path="/data/test.duckdb",
            schema_keys=["subject"],
        )
        # Must call scidb.close_database(db) in both try and catch branches.
        assert cmd.count("scidb.close_database(db)") == 2
        assert "catch scistack_err__" in cmd

    def test_variants_has_close(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="my_func",
            db_path="/data/test.duckdb",
            schema_keys=["subject"],
            variants=[
                {
                    "input_types": {"x": "RawData"},
                    "output_type": "ProcessedData",
                    "constants": {},
                    "record_count": 1,
                }
            ],
        )
        assert cmd.count("scidb.close_database(db)") == 2
        assert "catch scistack_err__" in cmd


class TestLockConflictReporting:
    """A DuckDB file held by another process (in practice: MATLAB) must be
    reported as a recoverable, named condition — never as a silent failure.

    This is the bug behind todo #13: ``acquire_db_connection`` raising made
    the JSON-RPC request thread die without emitting a response, so the GUI
    hung forever with no error anywhere.
    """

    def test_conflicting_lock_becomes_database_locked_error(self, db_path):
        import scistack_gui.db as db_mod
        from scistack_gui.db import DatabaseLockedError, acquire_db_connection, init_db

        init_db(db_path)
        db_mod._db._duck.close()
        db_mod._db_open = False

        # A second process holding the file is exactly what DuckDB reports
        # with this message shape.
        def _locked_reopen():
            raise OSError(
                "IO Error: Could not set lock on file "
                '"/data/test.duckdb": Conflicting lock is held in '
                "/usr/local/MATLAB/bin/glnxa64/MATLAB (PID 4242).\n"
            )

        db_mod._db.reopen = _locked_reopen
        with pytest.raises(DatabaseLockedError) as excinfo:
            acquire_db_connection(timeout=0)

        err = excinfo.value
        assert err.pid == "4242"
        assert "MATLAB" in (err.holder or "")
        assert "MATLAB" in str(err)
        # A failed acquire must NOT bump the refcount, or the lock would be
        # held permanently after the next successful acquire/release pair.
        assert db_mod._db_refcount == 0
        assert db_mod._db_open is False

    def test_unrelated_io_error_is_not_reclassified(self, db_path):
        """A genuine I/O failure must not be retried or blamed on MATLAB."""
        import scistack_gui.db as db_mod
        from scistack_gui.db import DatabaseLockedError, acquire_db_connection, init_db

        init_db(db_path)
        db_mod._db._duck.close()
        db_mod._db_open = False

        def _broken_reopen():
            raise OSError("IO Error: Cannot open file: permission denied")

        db_mod._db.reopen = _broken_reopen
        with pytest.raises(OSError) as excinfo:
            acquire_db_connection(timeout=5)
        assert not isinstance(excinfo.value, DatabaseLockedError)
        assert db_mod._db_refcount == 0

    def test_retry_succeeds_when_the_lock_clears(self, db_path):
        """MATLAB grabs the file for one write and lets go; a short backoff
        should absorb that rather than surfacing an error."""
        import scistack_gui.db as db_mod
        from scistack_gui.db import acquire_db_connection, init_db

        init_db(db_path)
        real_reopen = db_mod._db._duck.reopen
        db_mod._db._duck.close()
        db_mod._db_open = False

        attempts = []

        def _reopen_after_two_failures():
            attempts.append(1)
            if len(attempts) < 3:
                raise OSError(
                    "IO Error: Could not set lock on file: Conflicting lock "
                    "is held in MATLAB (PID 7).\n"
                )
            real_reopen()

        db_mod._db.reopen = _reopen_after_two_failures
        acquire_db_connection(timeout=5)

        assert len(attempts) == 3
        assert db_mod._db_open is True
        assert db_mod._db_refcount == 1


class TestLockContentionIsLoggedAsOneEpisode:
    """One 2.8s wait used to emit 66 lines (33 WARN/INFO pairs) and never
    state how long it took — 44% of the 2026-09-01 log was noise of this
    shape. The window must still be visible at default level, though: that it
    was invisible is what made a MATLAB-held database look like a GUI hang.
    """

    @staticmethod
    def _lock_until(db_mod, real_reopen, clears_after):
        """A reopen that raises a lock conflict for the first N attempts."""
        attempts = []

        def _reopen():
            attempts.append(1)
            if len(attempts) < clears_after:
                raise OSError(
                    "IO Error: Could not set lock on file: Conflicting lock "
                    "is held in MATLAB (PID 7).\n"
                )
            real_reopen()

        db_mod._db.reopen = _reopen
        return attempts

    def _prepare(self, db_path):
        import scistack_gui.db as db_mod
        from scistack_gui.db import init_db

        init_db(db_path)
        real_reopen = db_mod._db._duck.reopen
        db_mod._db._duck.close()
        db_mod._db_open = False
        return db_mod, real_reopen

    def test_one_warning_and_one_summary_for_a_multi_attempt_wait(
        self, db_path, caplog, monkeypatch
    ):
        import logging

        from scistack_gui.db import acquire_db_connection

        db_mod, real_reopen = self._prepare(db_path)
        monkeypatch.setattr(db_mod, "ACQUIRE_RETRY_INTERVAL", 0.0)
        self._lock_until(db_mod, real_reopen, clears_after=6)

        with caplog.at_level(logging.INFO, logger="scistack_gui.db"):
            acquire_db_connection(timeout=5)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1, "every blocked attempt warned again"
        assert "reopen blocked" in warnings[0].getMessage()

        infos = [r for r in caplog.records if r.levelno == logging.INFO]
        summaries = [r for r in infos if "acquired after" in r.getMessage()]
        assert len(summaries) == 1
        # The count is the point: it is what says how bad the contention was.
        assert "6 attempt(s)" in summaries[0].getMessage()
        assert "MATLAB" in summaries[0].getMessage()

        # Per-attempt chatter must not come back at default level.
        assert not [r for r in infos if "retrying in" in r.getMessage()]

    def test_uncontended_acquire_logs_no_summary(self, db_path, caplog):
        """The summary marks an episode; with nothing to wait for there is no
        episode, and a quiet path must stay quiet."""
        import logging

        from scistack_gui.db import acquire_db_connection

        db_mod, real_reopen = self._prepare(db_path)
        self._lock_until(db_mod, real_reopen, clears_after=1)

        with caplog.at_level(logging.INFO, logger="scistack_gui.db"):
            acquire_db_connection(timeout=5)

        assert "acquired after" not in caplog.text
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_giving_up_still_warns_and_raises(self, db_path, caplog):
        """Demoting the retry chatter must not quiet the failure itself."""
        import logging

        from scistack_gui.db import DatabaseLockedError, acquire_db_connection

        db_mod, real_reopen = self._prepare(db_path)
        self._lock_until(db_mod, real_reopen, clears_after=99)

        with caplog.at_level(logging.INFO, logger="scistack_gui.db"):
            with pytest.raises(DatabaseLockedError):
                acquire_db_connection(timeout=0)

        assert "giving up after" in caplog.text
        assert "acquired after" not in caplog.text


class TestRequestAlwaysGetsAResponse:
    """``_handle_request`` is a thread target: an exception escaping it
    strands the extension's pending promise forever. Every path must emit
    exactly one JSON-RPC frame."""

    def _capture_frames(self, monkeypatch):
        from scistack_gui import server

        frames = []
        monkeypatch.setattr(server, "_send", lambda obj: frames.append(obj))
        return frames

    def test_locked_database_yields_a_typed_error_frame(self, monkeypatch):
        from scistack_gui import db as db_mod
        from scistack_gui import server

        frames = self._capture_frames(monkeypatch)

        def _locked(timeout=db_mod.ACQUIRE_RETRY_TIMEOUT):
            raise db_mod.DatabaseLockedError("/db.duckdb", "MATLAB", "99", "raw")

        monkeypatch.setattr(db_mod, "acquire_db_connection", _locked)
        monkeypatch.setitem(server.METHODS, "get_pipeline", lambda p: {"ok": True})

        server._handle_request({"id": 7, "method": "get_pipeline", "params": {}})

        assert len(frames) == 1
        assert frames[0]["id"] == 7
        assert frames[0]["error"]["code"] == server.ERR_DATABASE_LOCKED
        assert "MATLAB" in frames[0]["error"]["message"]

    def test_acquire_failure_does_not_release(self, monkeypatch):
        """release_db_connection must not run for an acquire that failed —
        it would drive the refcount negative and close a live connection."""
        from scistack_gui import db as db_mod
        from scistack_gui import server

        self._capture_frames(monkeypatch)
        released = []

        def _locked(timeout=db_mod.ACQUIRE_RETRY_TIMEOUT):
            raise db_mod.DatabaseLockedError("/db.duckdb", "MATLAB", "99", "raw")

        monkeypatch.setattr(db_mod, "acquire_db_connection", _locked)
        monkeypatch.setattr(
            db_mod, "release_db_connection", lambda: released.append(1)
        )
        monkeypatch.setitem(server.METHODS, "get_pipeline", lambda p: {"ok": True})

        server._handle_request({"id": 8, "method": "get_pipeline", "params": {}})

        assert released == []

    def test_handler_exception_yields_an_error_frame(self, monkeypatch):
        from scistack_gui import db as db_mod
        from scistack_gui import server

        frames = self._capture_frames(monkeypatch)
        monkeypatch.setattr(db_mod, "acquire_db_connection", lambda timeout=5.0: None)
        monkeypatch.setattr(db_mod, "release_db_connection", lambda: None)

        def _boom(params):
            raise ValueError("handler blew up")

        monkeypatch.setitem(server.METHODS, "get_pipeline", _boom)
        server._handle_request({"id": 9, "method": "get_pipeline", "params": {}})

        assert len(frames) == 1
        assert frames[0]["error"]["code"] == -32000
        assert "handler blew up" in frames[0]["error"]["message"]

    def test_release_failure_does_not_double_respond(self, monkeypatch):
        """The success frame is already out; a failing release must neither
        kill the thread nor emit a second frame for the same id."""
        from scistack_gui import db as db_mod
        from scistack_gui import server

        frames = self._capture_frames(monkeypatch)
        monkeypatch.setattr(db_mod, "acquire_db_connection", lambda timeout=5.0: None)

        def _bad_release():
            raise RuntimeError("release exploded")

        monkeypatch.setattr(db_mod, "release_db_connection", _bad_release)
        monkeypatch.setitem(server.METHODS, "get_pipeline", lambda p: {"ok": True})

        server._handle_request({"id": 10, "method": "get_pipeline", "params": {}})

        assert len(frames) == 1
        assert frames[0]["result"] == {"ok": True}

    def test_respond_failure_falls_back_to_an_error_frame(self, monkeypatch):
        """If the success frame itself can't be serialised, the request must
        still be answered — otherwise the caller waits forever."""
        from scistack_gui import db as db_mod
        from scistack_gui import server

        frames = []
        calls = []

        def _send(obj):
            calls.append(obj)
            # Fail only the first (success) frame, as json.dumps would for a
            # non-serialisable result.
            if "result" in obj:
                raise TypeError("Object of type set is not JSON serializable")
            frames.append(obj)

        monkeypatch.setattr(server, "_send", _send)
        monkeypatch.setattr(db_mod, "acquire_db_connection", lambda timeout=5.0: None)
        monkeypatch.setattr(db_mod, "release_db_connection", lambda: None)
        monkeypatch.setitem(server.METHODS, "get_pipeline", lambda p: {"bad": {1, 2}})

        server._handle_request({"id": 11, "method": "get_pipeline", "params": {}})

        assert len(frames) == 1
        assert frames[0]["id"] == 11
        assert frames[0]["error"]["code"] == -32000


class TestWindowsLockMessageIsRecognized:
    """DuckDB's lock-conflict wording is platform-specific.

    The POSIX form ("Could not set lock on file ...: Conflicting lock is held
    in ...") and the Windows form share no common phrase, so a matcher written
    against one fails open on the other — it classifies a lock conflict as a
    generic I/O error, skips the retry loop entirely, and surfaces a raw
    ``duckdb.IOException`` to the frontend.

    That happened on 2026-09-01: three RPCs (``list_hypotheses``,
    ``get_hidden_pipelines``, ``get_pipeline``) failed with tracebacks in a
    ~2.1 s window while MATLAB finished writing — comfortably inside the 5 s
    retry budget that never ran. It cannot reproduce on Linux CI in any other
    way, which is why the message text is pinned here directly.
    """

    WINDOWS_MESSAGE = (
        'IO Error: Cannot open file "\\\\fs2.smpp.local\\RTO\\LabMembers\\'
        'MTillman\\GitRepos\\aging-well-abilitylab\\test_afl.duckdb": The '
        "process cannot access the file because it is being used by another "
        "process.\n\nFile is already open in \n"
        "C:\\Program Files\\MATLAB\\R2023b\\bin\\win64\\MATLAB.exe (PID 53772)"
    )

    def test_windows_message_classifies_as_a_lock_conflict(self, db_path):
        import scistack_gui.db as db_mod
        from scistack_gui.db import DatabaseLockedError, acquire_db_connection, init_db

        init_db(db_path)
        db_mod._db._duck.close()
        db_mod._db_open = False

        def _locked_reopen():
            raise OSError(self.WINDOWS_MESSAGE)

        db_mod._db.reopen = _locked_reopen
        with pytest.raises(DatabaseLockedError) as excinfo:
            acquire_db_connection(timeout=0)

        err = excinfo.value
        assert err.pid == "53772"
        assert "MATLAB" in (err.holder or ""), (
            f"holder not extracted from the Windows form: {err.holder!r}"
        )
        assert err.retryable is True
        assert db_mod._db_refcount == 0

    def test_windows_lock_is_retried_not_raised_immediately(self, db_path):
        """The 2.1 s real-world window must be absorbed, not reported."""
        import scistack_gui.db as db_mod
        from scistack_gui.db import acquire_db_connection, init_db

        init_db(db_path)
        real_reopen = db_mod._db.reopen
        db_mod._db._duck.close()
        db_mod._db_open = False

        calls = {"n": 0}

        def _locked_then_free():
            calls["n"] += 1
            if calls["n"] < 3:
                raise OSError(self.WINDOWS_MESSAGE)
            real_reopen()

        db_mod._db.reopen = _locked_then_free
        acquire_db_connection(timeout=5)

        assert calls["n"] == 3, "the Windows lock message did not trigger a retry"
        assert db_mod._db_open is True
        assert db_mod._db_refcount == 1
