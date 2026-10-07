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
