"""
Tests for scistack_gui.matlab.sidecar (Stage 3,
plan-matlab-pipeline-execution.md).

No real MATLAB in this sandbox — subprocess.Popen is mocked with a fake
process exposing writable stdin and a queue-backed stdout iterator, the
same style used for the MATLAB-existence subprocess check in
test_builtin_functions.py's TestMatlabBuiltin (monkeypatch the module's
own `shutil`/`subprocess` references).
"""

from __future__ import annotations

import queue
import threading
import time

import pytest
from scistack_gui.matlab import sidecar as ms


class FakeStdin:
    def __init__(self):
        self.written: list[str] = []

    def write(self, s: str) -> None:
        self.written.append(s)

    def flush(self) -> None:
        pass


class FakeStdout:
    """Queue-backed line iterator — mirrors a real pipe: __next__ blocks
    until a line is pushed or close() signals EOF."""

    _CLOSED = object()

    def __init__(self):
        self._q: "queue.Queue[object]" = queue.Queue()

    def push(self, line: str) -> None:
        self._q.put(line)

    def close(self) -> None:
        self._q.put(self._CLOSED)

    def __iter__(self):
        return self

    def __next__(self):
        item = self._q.get()
        if item is self._CLOSED:
            raise StopIteration
        return item


class FakeProcess:
    def __init__(self):
        self.stdin = FakeStdin()
        self.stdout = FakeStdout()
        self.pid = 4242
        self._returncode: int | None = None
        self.killed = False

    def poll(self):
        return self._returncode

    def kill(self):
        self.killed = True
        self._returncode = -9
        self.stdout.close()


@pytest.fixture(autouse=True)
def _reset_singleton():
    """The module-level sidecar singleton must not leak between tests."""
    ms._sidecar = None
    yield
    ms._sidecar = None


class TestStart:
    def test_returns_false_when_matlab_not_on_path(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: None)
        called = []
        monkeypatch.setattr(ms.subprocess, "Popen", lambda *a, **k: called.append(1))

        sidecar = ms.MatlabSidecar()
        assert sidecar.start() is False
        assert called == []
        assert sidecar.is_running is False

    def test_launches_popen_with_expected_flags(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: "/usr/local/bin/matlab")
        captured = {}

        def fake_popen(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["kwargs"] = kwargs
            return FakeProcess()

        monkeypatch.setattr(ms.subprocess, "Popen", fake_popen)

        sidecar = ms.MatlabSidecar()
        assert sidecar.start() is True
        assert sidecar.is_running is True
        assert captured["cmd"] == [
            "/usr/local/bin/matlab", "-nodesktop", "-nosplash", "-nodisplay",
        ]
        assert captured["kwargs"]["stdin"] == ms.subprocess.PIPE
        assert captured["kwargs"]["stdout"] == ms.subprocess.PIPE
        assert captured["kwargs"]["text"] is True

    def test_idempotent_while_running(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: "/usr/local/bin/matlab")
        call_count = {"n": 0}

        def fake_popen(cmd, **kwargs):
            call_count["n"] += 1
            return FakeProcess()

        monkeypatch.setattr(ms.subprocess, "Popen", fake_popen)

        sidecar = ms.MatlabSidecar()
        assert sidecar.start() is True
        assert sidecar.start() is True
        assert call_count["n"] == 1


class TestRunCommand:
    def _started_sidecar(self, monkeypatch) -> tuple[ms.MatlabSidecar, FakeProcess]:
        monkeypatch.setattr(ms.shutil, "which", lambda name: "/usr/local/bin/matlab")
        proc_holder: list[FakeProcess] = []

        def fake_popen(cmd, **kwargs):
            proc = FakeProcess()
            proc_holder.append(proc)
            return proc

        monkeypatch.setattr(ms.subprocess, "Popen", fake_popen)
        sidecar = ms.MatlabSidecar()
        assert sidecar.start() is True
        return sidecar, proc_holder[0]

    def test_raises_when_not_started(self):
        sidecar = ms.MatlabSidecar()
        with pytest.raises(RuntimeError, match="not running"):
            sidecar.run_command("disp('hi')", lambda line: None)

    def test_writes_wrapped_command_to_stdin(self, monkeypatch):
        sidecar, proc = self._started_sidecar(monkeypatch)
        proc.stdout.push(f"{ms.DONE_SENTINEL}\n")

        ok = sidecar.run_command("disp('hi')", lambda line: None, timeout=2)
        assert ok is True

        written = "".join(proc.stdin.written)
        assert "try\n" in written
        assert "disp('hi')" in written
        assert f"disp('{ms.DONE_SENTINEL}');" in written
        assert "catch scistack_sidecar_err__" in written
        assert ms.ERROR_SENTINEL in written

    def test_success_relays_lines_and_stops_at_done_sentinel(self, monkeypatch):
        sidecar, proc = self._started_sidecar(monkeypatch)
        proc.stdout.push("line one\n")
        proc.stdout.push("line two\n")
        proc.stdout.push(f"{ms.DONE_SENTINEL}\n")
        proc.stdout.push("should not be relayed\n")  # never reached

        relayed: list[str] = []
        ok = sidecar.run_command("disp('hi')", relayed.append, timeout=2)

        assert ok is True
        assert relayed == ["line one\n", "line two\n"]

    def test_error_sentinel_relays_error_line_and_returns_false(self, monkeypatch):
        sidecar, proc = self._started_sidecar(monkeypatch)
        proc.stdout.push("line one\n")
        proc.stdout.push(f"{ms.ERROR_SENTINEL}: Undefined function.\n")

        relayed: list[str] = []
        ok = sidecar.run_command("bogus_call()", relayed.append, timeout=2)

        assert ok is False
        assert relayed == ["line one\n", f"{ms.ERROR_SENTINEL}: Undefined function.\n"]

    def test_process_exit_without_sentinel_raises(self, monkeypatch):
        sidecar, proc = self._started_sidecar(monkeypatch)
        proc.stdout.push("some output\n")
        proc.stdout.close()  # EOF with no sentinel — process died

        with pytest.raises(RuntimeError, match="exited before completion"):
            sidecar.run_command("disp('hi')", lambda line: None, timeout=2)

    def test_timeout_raises(self, monkeypatch):
        sidecar, _proc = self._started_sidecar(monkeypatch)
        # No output pushed at all — run_command must give up, not hang.
        with pytest.raises(TimeoutError):
            sidecar.run_command("disp('hi')", lambda line: None, timeout=0.05)


class TestStop:
    def test_stop_kills_process_and_clears_running_state(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: "/usr/local/bin/matlab")
        proc_holder: list[FakeProcess] = []

        def fake_popen(cmd, **kwargs):
            proc = FakeProcess()
            proc_holder.append(proc)
            return proc

        monkeypatch.setattr(ms.subprocess, "Popen", fake_popen)

        sidecar = ms.MatlabSidecar()
        sidecar.start()
        assert sidecar.is_running is True

        sidecar.stop()
        assert sidecar.is_running is False
        assert proc_holder[0].killed is True

    def test_stop_on_never_started_sidecar_is_a_noop(self):
        sidecar = ms.MatlabSidecar()
        sidecar.stop()  # must not raise
        assert sidecar.is_running is False


class TestSingletonAndCapability:
    def test_get_sidecar_returns_same_instance(self):
        a = ms.get_sidecar()
        b = ms.get_sidecar()
        assert a is b

    def test_sidecar_capable_reflects_which(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: None)
        assert ms.sidecar_capable() is False
        monkeypatch.setattr(ms.shutil, "which", lambda name: "/usr/local/bin/matlab")
        assert ms.sidecar_capable() is True


class TestBusyGuard:
    """One command at a time. Two concurrent writers would interleave their
    text into MATLAB's stdin and then race for each other's sentinels,
    corrupting BOTH runs — so a second command is refused, not queued."""

    def _started(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: "/usr/local/bin/matlab")
        holder: list[FakeProcess] = []

        def fake_popen(cmd, **kwargs):
            proc = FakeProcess()
            holder.append(proc)
            return proc

        monkeypatch.setattr(ms.subprocess, "Popen", fake_popen)
        sidecar = ms.MatlabSidecar()
        assert sidecar.start() is True
        return sidecar, holder[0]

    def test_second_concurrent_command_is_refused(self, monkeypatch):
        sidecar, proc = self._started(monkeypatch)
        started = threading.Event()
        release = threading.Event()

        def slow_reader(line):
            started.set()
            release.wait(timeout=5)

        def first():
            sidecar.run_command("disp('a')", slow_reader)

        t = threading.Thread(target=first, daemon=True)
        t.start()
        proc.stdout.push("working\n")
        assert started.wait(timeout=5)

        with pytest.raises(ms.SidecarBusyError, match="already running"):
            sidecar.run_command("disp('b')", lambda line: None)

        release.set()
        proc.stdout.push(f"{ms.DONE_SENTINEL}\n")
        t.join(timeout=5)

    def test_busy_clears_after_a_command_finishes(self, monkeypatch):
        sidecar, proc = self._started(monkeypatch)
        proc.stdout.push(f"{ms.DONE_SENTINEL}\n")
        assert sidecar.run_command("disp('a')", lambda line: None) is True
        assert sidecar.is_busy is False

        proc.stdout.push(f"{ms.DONE_SENTINEL}\n")
        assert sidecar.run_command("disp('b')", lambda line: None) is True

    def test_busy_clears_even_when_a_command_raises(self, monkeypatch):
        sidecar, proc = self._started(monkeypatch)
        proc.stdout.close()  # EOF -> run_command raises
        with pytest.raises(RuntimeError):
            sidecar.run_command("disp('a')", lambda line: None)
        assert sidecar.is_busy is False

    def test_stop_clears_busy(self, monkeypatch):
        """stop() must clear it unconditionally: an in-flight command's own
        `finally` only runs once its blocked stdout read unblocks, so a
        restart could otherwise see a stale 'busy' and refuse the very
        command it was recycled to accept."""
        sidecar, _ = self._started(monkeypatch)
        sidecar._busy = True
        sidecar.stop()
        assert sidecar.is_busy is False


class TestRestartAndStatus:
    def _started(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: "/usr/local/bin/matlab")
        holder: list[FakeProcess] = []

        def fake_popen(cmd, **kwargs):
            proc = FakeProcess()
            holder.append(proc)
            return proc

        monkeypatch.setattr(ms.subprocess, "Popen", fake_popen)
        sidecar = ms.MatlabSidecar()
        assert sidecar.start() is True
        return sidecar, holder

    def test_restart_replaces_the_process(self, monkeypatch):
        sidecar, holder = self._started(monkeypatch)
        assert sidecar.restart() is True
        assert len(holder) == 2
        assert holder[0].killed is True
        assert sidecar.is_running is True

    def test_start_after_a_crash_drains_the_stale_eof(self, monkeypatch):
        """A dead process's reader thread queues an EOF `None`. Left there,
        the NEXT run_command reads it and aborts a perfectly healthy engine
        with 'process exited before completion'."""
        sidecar, holder = self._started(monkeypatch)
        holder[0].kill()  # closes stdout -> reader queues the EOF sentinel
        time.sleep(0.05)

        assert sidecar.start() is True
        assert len(holder) == 2

        holder[1].stdout.push(f"{ms.DONE_SENTINEL}\n")
        assert sidecar.run_command("disp('after crash')", lambda line: None) is True

    def test_status_reports_each_state(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: None)
        sidecar = ms.MatlabSidecar()
        assert sidecar.status()["state"] == "unavailable"

        sidecar, holder = self._started(monkeypatch)
        assert sidecar.status()["state"] == "ready"
        assert sidecar.status()["pid"] == holder[0].pid

        sidecar._busy = True
        assert sidecar.status()["state"] == "busy"
        sidecar._busy = False

        sidecar.stop()
        assert sidecar.status()["state"] == "stopped"


class TestHealthProbe:
    def _started(self, monkeypatch):
        monkeypatch.setattr(ms.shutil, "which", lambda name: "/usr/local/bin/matlab")
        holder: list[FakeProcess] = []

        def fake_popen(cmd, **kwargs):
            proc = FakeProcess()
            holder.append(proc)
            return proc

        monkeypatch.setattr(ms.subprocess, "Popen", fake_popen)
        sidecar = ms.MatlabSidecar()
        assert sidecar.start() is True
        return sidecar, holder[0]

    def test_healthy_when_pyenv_reports_a_version(self, monkeypatch):
        sidecar, proc = self._started(monkeypatch)
        proc.stdout.push("SCISTACK_PYENV:3.11\n")
        proc.stdout.push(f"{ms.DONE_SENTINEL}\n")
        assert sidecar.check_health() is None
        assert sidecar.status()["error"] is None

    def test_missing_pyenv_reported_as_a_setup_problem(self, monkeypatch):
        """MATLAB launches fine but can't reach Python. Without this probe
        it fails on the first py.* call deep inside configure_database and
        reads as a pipeline error rather than a setup one."""
        sidecar, proc = self._started(monkeypatch)
        proc.stdout.push(f"{ms.ERROR_SENTINEL}: Undefined function 'pyenv'\n")
        err = sidecar.check_health()
        assert err is not None
        assert "pyenv" in err
        assert sidecar.status()["error"] == err

    def test_not_running_is_reported(self):
        assert "not running" in ms.MatlabSidecar().check_health()

    def test_probe_during_a_run_is_skipped_not_failed(self, monkeypatch):
        """A health probe must never disturb an in-flight run, nor report
        the refusal as ill health."""
        sidecar, _ = self._started(monkeypatch)
        sidecar._busy = True
        assert sidecar.check_health() is None
