"""The server's stdin reader (scistack_gui/stdin_reader.py).

On Windows a read left pending on the stdin pipe stalled every first-time
native import on a request thread: Plot Studio's first ``plot_describe`` hung
in ``import_plot_packages`` (scidb.log 2026-10-20). The reader polls the pipe
there instead. These tests pin the line reassembly both paths share, the poll
loop, that the server reads only through this owner, and — on Windows — the
hang itself.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from scistack_gui import stdin_reader
from scistack_gui.stdin_reader import IDLE_POLL_S, poll_chunks, split_lines


# --- split_lines ------------------------------------------------------------


def test_several_requests_in_one_chunk_are_separate_lines():
    assert list(split_lines([b'{"id": 1}\n{"id": 2}\n'])) == ['{"id": 1}', '{"id": 2}']


def test_a_request_split_across_chunks_is_reassembled():
    assert list(split_lines([b'{"id"', b": 1}\n"])) == ['{"id": 1}']


def test_a_multibyte_character_split_across_chunks_decodes():
    data = '{"path": "Müller"}\n'.encode("utf-8")
    cut = data.index("ü".encode("utf-8")) + 1  # inside the two-byte ü
    assert list(split_lines([data[:cut], data[cut:]])) == ['{"path": "Müller"}']


def test_crlf_line_endings_are_stripped():
    assert list(split_lines([b"a\r\nb\r\n"])) == ["a", "b"]


def test_a_final_line_without_newline_is_still_yielded():
    assert list(split_lines([b"a\nb"])) == ["a", "b"]


def test_a_line_that_is_not_utf8_is_skipped_and_reading_continues(caplog):
    with caplog.at_level(logging.WARNING):
        lines = list(split_lines([b"good\n\xff\xfe\nnext\n"]))

    assert lines == ["good", "next"]
    assert "not UTF-8" in caplog.text


# --- poll_chunks --------------------------------------------------------------


def test_poll_reads_only_what_is_waiting_and_sleeps_when_empty():
    peeks = iter([0, 4, 0, 2, None])
    reads: list[int] = []
    sleeps: list[float] = []
    data = iter([b"ab\nc", b"d\n"])

    def read(n: int) -> bytes:
        reads.append(n)
        return next(data)

    chunks = list(poll_chunks(lambda: next(peeks), read, sleeps.append))

    assert chunks == [b"ab\nc", b"d\n"]
    assert reads == [4, 2]  # exactly the bytes peeked: never a read that waits
    assert sleeps == [IDLE_POLL_S, IDLE_POLL_S]


def test_poll_stops_when_a_read_returns_nothing():
    chunks = list(poll_chunks(lambda: 3, lambda n: b"", lambda s: None))
    assert chunks == []


# --- request_lines over a real pipe -------------------------------------------


def test_request_lines_reads_a_real_pipe_until_it_closes():
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b'{"id": 1}\n{"id": 2}\n')
    os.close(write_fd)
    with open(read_fd, "r") as stream:
        assert list(stdin_reader.request_lines(stream)) == ['{"id": 1}', '{"id": 2}']


# --- one owner ------------------------------------------------------------------


def test_the_server_reads_stdin_only_through_the_reader():
    source = (Path(__file__).parents[1] / "scistack_gui" / "server.py").read_text(
        encoding="utf-8"
    )
    code = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    assert "in sys.stdin" not in code
    assert "sys.stdin.read" not in code
    assert "request_lines()" in code


# --- the hang itself (Windows only) ---------------------------------------------


@pytest.mark.skipif(sys.platform != "win32", reason="Windows pipe semantics")
def test_a_native_import_on_another_thread_finishes_while_the_reader_waits():
    """The 2026-10-20 reproduction: main thread waiting on a stdin pipe that
    stays open and empty, worker thread doing the process's first native
    import. With ``for line in sys.stdin`` the worker never finished."""
    pytest.importorskip("numpy")
    child = textwrap.dedent(
        """
        import os, threading, time
        from scistack_gui.stdin_reader import request_lines

        def work():
            time.sleep(1.0)  # let the main thread reach its read first
            import numpy  # first native extension load in this process
            os._exit(0)

        threading.Thread(target=work, daemon=True).start()
        for _ in request_lines():
            pass
        os._exit(3)  # stdin closed before the import finished
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", child], stdin=subprocess.PIPE)
    try:
        code = proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        pytest.fail("native import stalled behind the stdin read (the 2026-10-20 hang)")
    finally:
        proc.stdin.close()
    assert code == 0
