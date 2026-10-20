"""
Request lines from the extension's stdin pipe — the one owner of that read.

Windows: never leave a read pending on the pipe.
    The extension hands the server an anonymous pipe as stdin, and that handle
    is synchronous. While the main thread sits in a blocking ``ReadFile`` on
    it, Windows serializes other operations on the same handle, and loading a
    native extension (a ``.pyd`` and the DLLs it pulls in) on another thread
    queries the standard handles during DLL initialization. So a request thread
    doing its first ``import numpy`` / ``scipy`` / ``matplotlib`` waited until
    the next line arrived — and the main thread starts the next read within
    microseconds, so in practice forever. Plot Studio's first ``plot_describe``
    hung this way (scidb.log 2026-10-20 11:59:00, ``import_plot_packages
    started`` and never done). Standalone reproduction: main thread blocked
    reading a pipe, worker thread importing scistackplot → stuck in
    ``create_module`` for ``numpy._core._multiarray_umath``; the same worker
    finished in 6.2 s once the main thread polled instead.

    So on a Windows pipe the reader asks ``PeekNamedPipe`` how many bytes are
    waiting, reads exactly those (a read that returns at once), and otherwise
    sleeps :data:`IDLE_POLL_S`. No read is ever outstanding.

Everywhere else (POSIX, or a Windows console/file): a plain blocking read of
``sys.stdin.buffer``, which has no such interaction.

Both paths split bytes on ``\\n`` and decode each line as UTF-8 here, so the
request encoding has one owner (the extension writes UTF-8; the text-mode
``sys.stdin`` decoded with the Windows locale code page).
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Callable, Iterable, Iterator

logger = logging.getLogger(__name__)

#: How long the Windows reader sleeps when the pipe is empty. Bounds the extra
#: latency of a request arriving at an idle server; a burst arrives in one read.
IDLE_POLL_S = 0.01

#: ``GetFileType`` result for a pipe.
_FILE_TYPE_PIPE = 3
#: ``PeekNamedPipe`` error once the writer has closed its end — a normal EOF.
_ERROR_BROKEN_PIPE = 109


def request_lines(stream=None) -> Iterator[str]:
    """Yield each newline-delimited request from *stream* (default stdin),
    decoded, without its line ending, until the writer closes the pipe."""
    stream = stream if stream is not None else sys.stdin
    poller = _windows_pipe_poller(stream)
    if poller is not None:
        peek, read = poller
        logger.info(
            "[stdin] reader=windows-pipe-poll (PeekNamedPipe, %.0f ms idle "
            "poll): no read is left pending, so DLL loads on request threads "
            "cannot stall behind it",
            IDLE_POLL_S * 1000,
        )
        chunks = poll_chunks(peek, read, time.sleep)
    else:
        logger.info("[stdin] reader=blocking (%s)", sys.platform)
        chunks = iter(lambda: stream.buffer.read1(65536), b"")
    yield from split_lines(chunks)


def poll_chunks(
    peek: Callable[[], int | None],
    read: Callable[[int], bytes],
    sleep: Callable[[float], None],
) -> Iterator[bytes]:
    """Byte chunks from a pipe that is only read when data is waiting.

    ``peek`` returns how many bytes are available, or ``None`` once the pipe
    is closed; ``read(n)`` returns up to *n* bytes without blocking (they are
    already there). Injected so the loop is testable off Windows.
    """
    while True:
        available = peek()
        if available is None:
            return
        if available:
            chunk = read(available)
            if not chunk:
                return
            yield chunk
        else:
            sleep(IDLE_POLL_S)


def split_lines(chunks: Iterable[bytes]) -> Iterator[str]:
    """Reassemble *chunks* into decoded lines.

    A line may span chunks; a chunk may hold several lines; a final line with
    no newline is still yielded (as ``for line in file`` does). A line that is
    not UTF-8 is logged and skipped — like invalid JSON, it is one bad
    request, not a reason to stop serving.
    """
    pending = b""
    for chunk in chunks:
        pending += chunk
        *complete, pending = pending.split(b"\n")
        for raw in complete:
            line = _decode(raw)
            if line is not None:
                yield line
    if pending:
        line = _decode(pending)
        if line is not None:
            yield line


def _decode(raw: bytes) -> str | None:
    try:
        return raw.rstrip(b"\r").decode("utf-8")
    except UnicodeDecodeError as exc:
        logger.warning(
            "[stdin] skipped a %d-byte request that is not UTF-8: %s", len(raw), exc
        )
        return None


def _windows_pipe_poller(stream):
    """``(peek, read)`` for *stream* when it is a pipe on Windows, else None."""
    if sys.platform != "win32":
        return None
    try:
        import ctypes
        import msvcrt
        import os
        from ctypes import wintypes

        fd = stream.fileno()
        # os.read on a text-mode CRT descriptor would translate CRLF and stop
        # at Ctrl-Z; Python sets stdin binary at startup, but say so here.
        msvcrt.setmode(fd, os.O_BINARY)
        handle = wintypes.HANDLE(msvcrt.get_osfhandle(fd))
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetFileType.argtypes = [wintypes.HANDLE]
        kernel32.GetFileType.restype = wintypes.DWORD
        kernel32.PeekNamedPipe.argtypes = [
            wintypes.HANDLE,
            ctypes.c_void_p,
            wintypes.DWORD,
            ctypes.c_void_p,
            ctypes.POINTER(wintypes.DWORD),
            ctypes.c_void_p,
        ]
        kernel32.PeekNamedPipe.restype = wintypes.BOOL
        file_type = kernel32.GetFileType(handle)
    except Exception:
        logger.warning(
            "[stdin] could not inspect the stdin handle; falling back to a "
            "blocking read (native imports on request threads may stall)",
            exc_info=True,
        )
        return None
    if file_type != _FILE_TYPE_PIPE:
        logger.info("[stdin] stdin is not a pipe (GetFileType=%d)", file_type)
        return None

    avail = wintypes.DWORD()

    def peek() -> int | None:
        if kernel32.PeekNamedPipe(handle, None, 0, None, ctypes.byref(avail), None):
            return avail.value
        error = ctypes.get_last_error()
        if error != _ERROR_BROKEN_PIPE:
            logger.warning(
                "[stdin] PeekNamedPipe failed (error %d); treating stdin as closed",
                error,
            )
        return None

    def read(n: int) -> bytes:
        return os.read(fd, n)

    return peek, read
