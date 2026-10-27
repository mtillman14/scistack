"""Regression: the log must be able to answer "what did that RPC return?".

Both halves of this file come from one incident. Three PathInputs were
discovered at project creation and rendered on the canvas, but the sidebar's
list was empty, and ``scistack.log`` could settle neither question it was asked:

* whether ``get_path_inputs`` had even been called, let alone returned an
  empty list — every RPC line was DEBUG, so an empty read was invisible;
* what the startup discovery pass found — the file sink was only opened by
  ``configure_database()``, which runs *after* discovery, so the scan that
  decides which functions, variables and PathInputs exist never reached the
  file at all.

So: read RPCs report their result sizes at INFO, and ``main`` attaches the
log file before it loads any user code.
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path

import pytest
from scistack_gui import server


@pytest.fixture(autouse=True)
def _quiet_db(monkeypatch):
    """``_handle_request`` acquires/releases the DB around every call; no
    database is loaded here, so make both a no-op."""
    monkeypatch.setattr("scistack_gui.db.acquire_db_connection", lambda *a, **k: None)
    monkeypatch.setattr("scistack_gui.db.release_db_connection", lambda *a, **k: None)


def _dispatch(monkeypatch, method, result):
    monkeypatch.setitem(server.METHODS, method, lambda params: result)
    server._handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": {}}
    )


def _rpc_lines(caplog, level):
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == level and r.getMessage().startswith("RPC << ")
    ]


# ---------------------------------------------------------------------------
# _summarize_result
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "result,expected",
    [
        ([], "list[0]"),
        ([{"name": "a"}, {"name": "b"}, {"name": "c"}], "list[3]"),
        ({"nodes": [1, 2, 3], "edges": []}, "nodes[3], edges[0]"),
        # Scalars carry no count worth reporting.
        ({"db_loaded": True, "db_name": "x.duckdb"}, None),
        ({}, None),
        (True, None),
        (None, None),
    ],
)
def test_summarize_result(result, expected):
    assert server._summarize_result(result) == expected


def test_summarize_result_truncates_wide_dicts():
    wide = {f"key{i}": list(range(i)) for i in range(40)}
    summary = server._summarize_result(wide)
    assert summary.endswith("...")
    assert len(summary) <= 163


# ---------------------------------------------------------------------------
# INFO line on the way out
# ---------------------------------------------------------------------------
def test_empty_read_rpc_is_visible_at_info(monkeypatch, caplog, capsys):
    """The exact case that had no record: a sidebar list came back empty."""
    with caplog.at_level(logging.DEBUG):
        _dispatch(monkeypatch, "get_path_inputs", [])
    assert any(
        "RPC << get_path_inputs -> list[0]" in m
        for m in _rpc_lines(caplog, logging.INFO)
    )


def test_populated_read_rpc_reports_its_count(monkeypatch, caplog, capsys):
    with caplog.at_level(logging.DEBUG):
        _dispatch(
            monkeypatch,
            "get_path_inputs",
            [{"name": "emgPath"}, {"name": "grPath"}, {"name": "xsensPath"}],
        )
    assert any(
        "RPC << get_path_inputs -> list[3]" in m
        for m in _rpc_lines(caplog, logging.INFO)
    )


def test_dict_result_reports_each_collection(monkeypatch, caplog, capsys):
    with caplog.at_level(logging.DEBUG):
        _dispatch(monkeypatch, "get_pipeline", {"nodes": [1, 2, 3], "edges": []})
    assert any(
        "RPC << get_pipeline -> nodes[3], edges[0]" in m
        for m in _rpc_lines(caplog, logging.INFO)
    )


def test_write_rpc_stays_at_debug(monkeypatch, caplog, capsys):
    """Only reads answer at INFO — writes and per-frame chatter must not
    flood the file."""
    with caplog.at_level(logging.DEBUG):
        _dispatch(monkeypatch, "create_parameter", {"ok": True, "values": [1, 2]})
    assert _rpc_lines(caplog, logging.INFO) == []
    assert any("RPC << create_parameter OK" in m for m in _rpc_lines(caplog, logging.DEBUG))


def test_countless_read_rpc_stays_at_debug(monkeypatch, caplog, capsys):
    with caplog.at_level(logging.DEBUG):
        _dispatch(monkeypatch, "get_info", {"db_loaded": True, "db_name": "x.duckdb"})
    assert _rpc_lines(caplog, logging.INFO) == []
    assert any("RPC << get_info OK" in m for m in _rpc_lines(caplog, logging.DEBUG))


# ---------------------------------------------------------------------------
# Startup ordering
# ---------------------------------------------------------------------------
def _call_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def test_log_file_is_attached_before_any_discovery():
    """``main`` must open the log file before it loads user code.

    Source-order guard rather than a behavioural one because ``main``
    parses argv, creates the database and blocks on stdin — but the
    invariant is exactly an ordering, and getting it wrong silently costs
    the most valuable part of the startup record.
    """
    tree = ast.parse(Path(server.__file__).read_text(encoding="utf-8"))
    main_fn = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    calls: dict[str, list[int]] = {}
    for node in ast.walk(main_fn):
        if isinstance(node, ast.Call):
            calls.setdefault(_call_name(node), []).append(node.lineno)

    assert "attach_log_file" in calls, (
        "server.main must call scidb.log.attach_log_file so the file sink is "
        "open before discovery — see this module's docstring."
    )
    attach_line = min(calls["attach_log_file"])
    for discovery_call in ("load_config", "load_from_config", "exec_module"):
        assert discovery_call in calls
        assert attach_line < min(calls[discovery_call]), (
            f"{discovery_call}() runs before the log file is attached; its "
            "output would only reach stderr."
        )
