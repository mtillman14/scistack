"""
Standalone/browser MATLAB sidecar (Stage 3, plan-matlab-pipeline-execution.md).

A lazily-started, kept-warm MATLAB process the Python backend drives
directly over its own stdin/stdout REPL — for environments with no VS Code
+ MathWorks terminal integration (``matlabTerminal.ts``/``dagPanel.ts``,
which handle Tier 2). No new ``.m`` file is needed: MATLAB's own
command-line REPL *is* the "server".

Protocol: a command is wrapped in an outer ``try/catch`` that ALWAYS
``disp``s exactly one sentinel line before returning to the prompt — a
success sentinel on the normal path, an error sentinel (carrying the
caught exception's message) in the catch — regardless of whether the
command text itself already has its own inner try/catch (Stage 1's
generated scripts do; nesting is valid MATLAB and the outer catch also
guards commands with no error handling of their own). ``run_command``
writes the wrapped text, then reads stdout lines from a background reader
thread via a queue until one of the two sentinels appears.

No real MATLAB environment exists in this dev sandbox (see the plan
doc's note) — this module is exercised here only via mocked
``subprocess.Popen``; real verification needs the user's own MATLAB
install.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import threading
import time
from queue import Empty, Queue
from typing import Callable

logger = logging.getLogger(__name__)

DONE_SENTINEL = "__SCISTACK_SIDECAR_DONE__"
ERROR_SENTINEL = "__SCISTACK_SIDECAR_ERROR__"


def _wrap_for_sentinel(command_text: str) -> str:
    """Wrap arbitrary MATLAB text so the REPL always emits exactly one of
    the two sentinels before returning to the prompt, whether or not
    ``command_text`` raises (or already has its own try/catch)."""
    return (
        "try\n"
        f"{command_text}\n"
        f"    disp('{DONE_SENTINEL}');\n"
        "catch scistack_sidecar_err__\n"
        f"    disp(['{ERROR_SENTINEL}: ' scistack_sidecar_err__.message]);\n"
        "end\n"
    )


class SidecarBusyError(RuntimeError):
    """Raised when a second command arrives while one is still running.

    The protocol is one command at a time over a single stdin/stdout pair:
    two concurrent writers interleave their text into MATLAB's input and
    then race for each other's sentinels, which corrupts BOTH runs and
    reports whichever finishes first as the answer to both. Refusing is the
    only safe response — see :attr:`MatlabSidecar._busy`.
    """


class MatlabSidecar:
    """One lazily-started, kept-warm MATLAB process, driven over its own
    stdin/stdout.

    Serialised by an explicit ``_busy`` flag. This used to rely on callers
    happening to run one MATLAB pipeline at a time; that held by accident,
    not by contract, and nothing enforced it. A second concurrent
    ``run_command`` now raises :class:`SidecarBusyError` instead of silently
    interleaving.
    """

    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None
        self._stdout_queue: "Queue[str | None]" = Queue()
        self._reader_thread: threading.Thread | None = None
        self._busy = False
        self._busy_lock = threading.Lock()
        self._health_error: str | None = None
        """Why the engine was judged unusable, or None. A MATLAB that
        launches but can't reach Python is a SETUP problem; without this it
        surfaces as a pipeline failure on the first py.* call."""
        self._health_checked = False
        """Whether the probe has run for the CURRENT process. Cleared by
        start(), so a restart re-probes."""

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    @property
    def is_busy(self) -> bool:
        return self._busy

    def status(self) -> dict:
        """Snapshot for the GUI's engine indicator: ``{state, pid, error}``
        where state is one of ``unavailable`` (no ``matlab`` on PATH),
        ``stopped``, ``ready``, ``busy``."""
        if not sidecar_capable():
            state = "unavailable"
        elif not self.is_running:
            state = "stopped"
        elif self._busy:
            state = "busy"
        else:
            state = "ready"
        return {
            "state": state,
            "pid": self._proc.pid if self._proc is not None else None,
            "error": self._health_error,
        }

    def start(self) -> bool:
        """Launch MATLAB if not already running. Returns False (no
        exception) if ``matlab`` isn't on PATH — the caller falls back to
        the copy-paste command instead."""
        if self.is_running:
            return True
        matlab_bin = shutil.which("matlab")
        if matlab_bin is None:
            logger.warning("MatlabSidecar.start: 'matlab' not found on PATH")
            return False

        if self._proc is not None:
            # Replacing a process that already exited (crash, external kill,
            # or a force-cancel). Its reader thread pushed an EOF `None` when
            # the pipe closed; leaving that queued makes the NEXT
            # run_command see "process exited before completion" and abort a
            # perfectly healthy engine. stop() drains for the explicit path;
            # this covers the implicit auto-restart one.
            logger.info(
                "MatlabSidecar.start: previous MATLAB process is gone "
                "(exit=%s) — restarting and draining its stale output",
                self._proc.poll(),
            )
            self._drain_queue()
            self._proc = None
            self._reader_thread = None
        logger.info("MatlabSidecar.start: launching %s", matlab_bin)
        self._proc = subprocess.Popen(
            [matlab_bin, "-nodesktop", "-nosplash", "-nodisplay"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._reader_thread = threading.Thread(
            target=self._read_stdout, daemon=True, name="MatlabSidecar-reader"
        )
        self._reader_thread.start()
        logger.info("MatlabSidecar.start: MATLAB process started (pid=%s)", self._proc.pid)
        self._health_error = None
        self._health_checked = False
        return True

    def restart(self) -> bool:
        """Kill and relaunch. Used to recover from a wedged engine, and by
        the caller that just force-cancelled one."""
        logger.info("MatlabSidecar.restart: recycling the MATLAB process")
        self.stop()
        return self.start()

    def check_health(self, timeout: float = 60.0) -> "str | None":
        """Verify the engine can actually reach Python. Returns an error
        string, or ``None`` if healthy.

        **Probed once per process lifetime, then cached.** ``pyenv`` cannot
        change under a running MATLAB, so re-probing before every run would
        add a full round-trip to each one for an answer that cannot have
        changed. ``start()`` clears the cache, so a restart re-probes.

        ``start()`` only checks that ``matlab`` is on PATH. A MATLAB that
        launches fine but has no ``pyenv`` configured fails on the FIRST
        ``py.*`` call — which happens deep inside ``scidb.configure_database``,
        so it surfaces to the user as a pipeline error rather than the setup
        problem it is. Probing once, up front, lets the caller say so
        plainly (same spirit as docs/claude/phase-8-startup-diagnostics.md).

        Best-effort: a probe that times out is reported, not raised, so a
        slow-but-working engine is never killed on our say-so.
        """
        if not self.is_running:
            return "MATLAB is not running."
        if self._health_checked:
            return self._health_error

        lines: list[str] = []
        try:
            ok = self.run_command(
                "    disp(['SCISTACK_PYENV:' char(pyenv().Version)]);",
                lines.append,
                timeout=timeout,
            )
        except SidecarBusyError:
            return None  # mid-run: not a health signal, don't disturb it
        except Exception as e:
            self._health_checked = True
            self._health_error = f"MATLAB health probe failed: {e}"
            return self._health_error

        self._health_checked = True
        joined = "".join(lines)
        if not ok or "SCISTACK_PYENV:" not in joined:
            self._health_error = (
                "MATLAB started, but its Python bridge is not configured "
                "(pyenv). Run pyenv('Version', '<path-to-python>') in MATLAB, "
                "or set it in your startup.m — otherwise every scidb call "
                "will fail. See scimatlab/README.md."
            )
            return self._health_error

        self._health_error = None
        logger.info("MatlabSidecar.check_health: pyenv OK (%s)", joined.strip())
        return None

    def _read_stdout(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        try:
            for line in proc.stdout:
                self._stdout_queue.put(line)
        finally:
            # Process' stdout closed (exited or was killed) — unblock any
            # in-flight run_command wait with an explicit "no more output"
            # sentinel rather than hanging forever.
            self._stdout_queue.put(None)

    def stop(self) -> None:
        """Kill the sidecar process — used for best-effort cancellation
        (``force_cancel_run``) and to recover from a wedged/dead process.
        Safe to call when nothing is running."""
        # Unconditionally, and BEFORE the early return: killing the process
        # means nothing is in flight by definition. An in-flight
        # run_command's own `finally` also clears this, but that thread is
        # blocked reading stdout and only unblocks once the process dies —
        # so without this a restart() could observe a stale "busy" and
        # refuse the very command it was recycled to accept.
        self._busy = False
        if self._proc is None:
            return
        logger.info("MatlabSidecar.stop: terminating MATLAB process (pid=%s)", self._proc.pid)
        try:
            self._proc.kill()
        except Exception:
            logger.exception("MatlabSidecar.stop: failed to kill MATLAB process")
        self._proc = None
        self._reader_thread = None
        self._drain_queue()

    def _drain_queue(self) -> None:
        """Discard output queued from a dead process, so the next
        ``start()``/``run_command()`` begins clean. Shared by ``stop()`` and
        ``start()``'s auto-restart path."""
        while True:
            try:
                self._stdout_queue.get_nowait()
            except Empty:
                break

    def run_command(
        self,
        command_text: str,
        on_line: Callable[[str], None],
        timeout: float | None = None,
    ) -> bool:
        """Write ``command_text`` to MATLAB's stdin, relaying each stdout
        line to ``on_line`` as it arrives, until the wrapper's sentinel
        appears. Returns True on the success sentinel, False on the error
        sentinel (the error line itself is still relayed to ``on_line``
        first, so the caller's run console shows the MATLAB error message).

        Raises RuntimeError if the sidecar isn't running (call ``start()``
        first) or MATLAB's process exits mid-command;
        :class:`SidecarBusyError` if another command is still in flight;
        TimeoutError if ``timeout`` elapses with no sentinel seen.
        """
        if not self.is_running:
            raise RuntimeError("MatlabSidecar.run_command: sidecar is not running")

        # Claim the engine before writing a single byte. Checked and set
        # under one lock so two threads can't both see "free" and proceed.
        with self._busy_lock:
            if self._busy:
                raise SidecarBusyError(
                    "The MATLAB engine is already running a command. Wait for "
                    "it to finish, or cancel it, before starting another."
                )
            self._busy = True
        try:
            return self._run_locked(command_text, on_line, timeout)
        finally:
            self._busy = False

    def _run_locked(
        self,
        command_text: str,
        on_line: Callable[[str], None],
        timeout: float | None,
    ) -> bool:
        proc = self._proc
        assert proc is not None and proc.stdin is not None

        wrapped = _wrap_for_sentinel(command_text)
        logger.info(
            "MatlabSidecar.run_command: writing %d-char wrapped command", len(wrapped)
        )
        proc.stdin.write(wrapped)
        proc.stdin.flush()

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(
                    f"MatlabSidecar.run_command: timed out after {timeout}s"
                )
            try:
                line = self._stdout_queue.get(timeout=remaining)
            except Empty:
                raise TimeoutError(
                    f"MatlabSidecar.run_command: timed out after {timeout}s"
                ) from None
            if line is None:
                raise RuntimeError(
                    "MatlabSidecar.run_command: MATLAB process exited before "
                    "completion"
                )
            if DONE_SENTINEL in line:
                logger.info("MatlabSidecar.run_command: done sentinel seen")
                return True
            if ERROR_SENTINEL in line:
                logger.warning("MatlabSidecar.run_command: error sentinel seen: %s", line.strip())
                on_line(line)
                return False
            on_line(line)


# ---------------------------------------------------------------------------
# Process-wide singleton (mirrors scistack_gui.runtime.db's `_db` module-level
# singleton pattern) — kept warm across pipeline runs instead of relaunching
# MATLAB (slow to start) on every request.
# ---------------------------------------------------------------------------

_sidecar: MatlabSidecar | None = None
_sidecar_lock = threading.Lock()


def get_sidecar() -> MatlabSidecar:
    """The process-wide ``MatlabSidecar`` singleton. No MATLAB process is
    launched until the first ``start()``/``run_command()`` call actually
    needs one."""
    global _sidecar
    with _sidecar_lock:
        if _sidecar is None:
            _sidecar = MatlabSidecar()
        return _sidecar


def sidecar_capable() -> bool:
    """True if ``matlab`` is on PATH — the cheap capability check the
    Stage 4 routing ladder uses BEFORE committing to a sidecar launch."""
    return shutil.which("matlab") is not None
