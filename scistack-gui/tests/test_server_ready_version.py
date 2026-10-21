"""The ``ready`` notification carries the installed scistack-gui version.

The VS Code extension and the PyPI package are released from one git tag, but
a user updates them separately. The extension compares its own version with
this field and warns on a mismatch, so it must be present on every ``ready``
— both the database server and the plot-only server send it through
``server._send_ready``, the single place the message is built.
"""

from __future__ import annotations

import importlib.metadata

import scistack_gui
from scistack_gui import server


def test_ready_carries_the_installed_version(monkeypatch):
    sent: list[dict] = []
    monkeypatch.setattr(server, "_send", lambda obj: sent.append(obj))

    server._send_ready({"db_name": "x.duckdb", "schema_keys": ["subject"]})

    assert len(sent) == 1
    frame = sent[0]
    assert frame["method"] == "ready"
    assert frame["params"]["db_name"] == "x.duckdb"
    assert frame["params"]["schema_keys"] == ["subject"]
    assert frame["params"]["version"] == importlib.metadata.version("scistack-gui")
    assert frame["params"]["version"] == scistack_gui.__version__


def test_both_ready_senders_use_the_one_builder():
    """No second hand-built ``"method": "ready"`` frame may reappear."""
    import inspect

    source = inspect.getsource(server)
    assert source.count('"method": "ready"') == 1


# ---------------------------------------------------------------------------
# Debugger status: the ONE answer to "is a debugpy listener running"
# ---------------------------------------------------------------------------
# Regression: scistack.debug on + no debugpy in the interpreter made every Run
# fail its debugger attach with "connection refused 127.0.0.1:5678", while
# the server's only record of the failed listen predated scidb.log.


def test_debugger_off_when_not_requested():
    assert server._start_debugger({}) == {"state": "off", "port": None, "error": None}


def test_debugger_failed_when_debugpy_missing(monkeypatch):
    import sys

    # None in sys.modules makes `import debugpy` raise ImportError.
    monkeypatch.setitem(sys.modules, "debugpy", None)
    status = server._start_debugger(
        {"SCISTACK_GUI_DEBUG": "1", "SCISTACK_GUI_DEBUG_PORT": "5690"}
    )
    assert status["state"] == "failed"
    assert status["port"] == 5690
    assert "ImportError" in status["error"] or "ModuleNotFoundError" in status["error"]


def test_debugger_listening_reports_its_port(monkeypatch):
    import sys
    import types

    calls: list = []
    fake = types.SimpleNamespace(listen=lambda addr: calls.append(addr))
    monkeypatch.setitem(sys.modules, "debugpy", fake)
    status = server._start_debugger({"SCISTACK_GUI_DEBUG": "1"})
    assert status == {"state": "listening", "port": 5678, "error": None}
    assert calls == [("127.0.0.1", 5678)]


def test_debugger_failed_when_listen_raises(monkeypatch):
    import sys
    import types

    def _busy(addr):
        raise OSError("address already in use")

    monkeypatch.setitem(sys.modules, "debugpy", types.SimpleNamespace(listen=_busy))
    status = server._start_debugger({"SCISTACK_GUI_DEBUG": "1"})
    assert status["state"] == "failed"
    assert "address already in use" in status["error"]


def test_ready_carries_the_debugger_status(monkeypatch, caplog):
    import logging

    sent: list[dict] = []
    monkeypatch.setattr(server, "_send", lambda obj: sent.append(obj))
    failed = {"state": "failed", "port": 5678, "error": "ModuleNotFoundError: x"}
    monkeypatch.setattr(server, "_DEBUGGER_STATUS", failed)

    with caplog.at_level(logging.INFO, logger="scistack_gui.server"):
        server._send_ready({"db_name": "x.duckdb"})

    assert sent[0]["params"]["debugger"] == failed
    # Logged at ready time, i.e. after scidb.log is attached.
    assert any(
        "no debugpy listener" in r.getMessage() and r.levelno == logging.WARNING
        for r in caplog.records
    )
