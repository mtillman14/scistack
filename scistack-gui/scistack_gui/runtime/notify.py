"""
Push notifications to the VS Code extension host via stdout.

Replaces ws.py's push_message/broadcast for the JSON-RPC server mode.
In standalone (FastAPI) mode, ws.py is used instead.

All output is newline-delimited JSON on stdout. User code stdout is captured
by redirect_stdout in run.py, so it never leaks into the JSON-RPC stream.
"""

import json
import logging
import sys
import threading

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_enabled = False


def enable():
    """Enable JSON-RPC notification output on stdout."""
    global _enabled
    _enabled = True


def notify(method: str, params: dict) -> None:
    """
    Write a JSON-RPC notification to stdout (thread-safe).

    This is the JSON-RPC equivalent of ws_hub.push_message / ws_hub.broadcast.
    Called from background threads (e.g. run.py) and async handlers.

    Every emission is logged. The frame itself goes to *stdout*, where the
    extension host consumes and parses it -- so it never reaches the output
    channel, and a notification storm (e.g. a ``dag_updated`` that makes every
    client re-fetch the whole graph) is otherwise invisible in a captured log.
    The log line is the only trace.
    """
    if not _enabled:
        logger.debug("[notify] Suppressed (output disabled): %s", method)
        return
    msg = json.dumps({"jsonrpc": "2.0", "method": method, "params": params})
    logger.info(
        "[notify] Emitting %s to extension host (thread=%s)",
        method,
        threading.get_ident(),
    )
    with _lock:
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()


def push_message(msg: dict) -> None:
    """
    Compatibility shim: translates the old ws_hub.push_message format
    ({"type": "run_output", ...}) into a JSON-RPC notification.

    This allows run.py and pipeline.py to work with both the FastAPI
    WebSocket path (ws_hub.push_message) and the JSON-RPC path (notify)
    without changing their call sites.
    """
    msg_type = msg.pop("type", "message")
    notify(msg_type, msg)
