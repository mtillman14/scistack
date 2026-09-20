"""
A webview render error reaches scidb.log.

Background: on 2026-09-15 the grouping picker crashed inside React and the
Plot Studio tab went blank. The Python log showed the popup's pipeline fetch
and then nothing, because a browser-side crash sends no request. The frontend
now has an error boundary that reports what it caught through
``report_client_error``; these tests pin down that the report is logged at
ERROR with its component stack, on both transports, and that it needs no
database — a crash is most worth hearing about while MATLAB holds the file.
"""

import logging

import pytest

from scistack_gui.services.client_errors import MAX_FIELD, report_client_error


@pytest.fixture
def caught(caplog):
    caplog.set_level(logging.ERROR, logger="scistack_gui.services.client_errors")
    return caplog


def test_the_report_is_logged_at_error_with_its_component_stack(caught):
    report_client_error(
        {
            "where": "Plot Studio tab",
            "message": "useScope must be used within ScopeProvider",
            "stack": "Error: useScope must be used within ScopeProvider\n    at useScope",
            "component_stack": "\n    at PipelineFunctionNode\n    at ReactFlow",
        }
    )
    errors = [r for r in caught.records if r.levelno == logging.ERROR]
    text = "\n".join(r.getMessage() for r in errors)
    assert "[client] render error in Plot Studio tab: useScope must be used" in text
    assert "PipelineFunctionNode" in text, "the component stack names the culprit"
    assert "at useScope" in text, "the JS stack is kept too"


def test_missing_fields_still_log_something_readable(caught):
    result = report_client_error({})
    assert result == {"where": "webview", "message": "(no message)"}
    assert any("[client] render error in webview" in r.getMessage() for r in caught.records)


def test_oversized_stacks_are_clipped(caught):
    report_client_error({"message": "x" * (MAX_FIELD * 3)})
    longest = max(len(r.getMessage()) for r in caught.records)
    assert longest < MAX_FIELD + 100


def test_the_rpc_is_registered_and_needs_no_database():
    from scistack_gui import server

    assert server.METHODS["report_client_error"].handler.needs_db is False
    # Otherwise `_handle_request` would try to open DuckDB first and a crash
    # during a MATLAB run would be reported as "database locked" instead.
    assert "report_client_error" in server.SELF_MANAGED_DB_METHODS


def test_the_json_rpc_path_answers_without_touching_the_database(monkeypatch, caught):
    from scistack_gui import db as db_mod
    from scistack_gui import server

    def _never(*args, **kwargs):
        raise AssertionError("report_client_error must not acquire the database")

    monkeypatch.setattr(db_mod, "acquire_db_connection", _never)
    frames = []
    monkeypatch.setattr(server, "_send", lambda frame: frames.append(frame))

    server._handle_request(
        {
            "id": 3,
            "method": "report_client_error",
            "params": {"where": "pipeline view", "message": "boom"},
        }
    )
    assert frames and frames[0].get("result") == {"where": "pipeline view", "message": "boom"}
    assert any("render error in pipeline view: boom" in r.getMessage() for r in caught.records)


def test_the_http_path_logs_too(client, caught):
    r = client.post(
        "/api/client-error",
        json={"where": "Plot Studio tab", "message": "boom", "component_stack": "at X"},
    )
    assert r.status_code == 200
    assert r.json() == {"where": "Plot Studio tab", "message": "boom"}
    assert any("render error in Plot Studio tab: boom" in m.getMessage() for m in caught.records)
