"""
A webview's bundle build reaches scidb.log, with a WARN when it is stale.

Background: on 2026-10-16 Plot Studio had no Copy PNG button under
scistack-gui 0.1.37. VS Code was running a hand-built 0.1.0 extension whose
webview bundle predated the button, and nothing in scidb.log said which
bundle the panel had loaded. Each webview now reports its build once on load
(``report_client_build``); the VS Code host adds the loading extension and the
version verdict (``extension/src/webviewBuild.ts`` owns that rule). These
tests pin that the line names the bundle and the extension path, that a
``mismatch`` verdict is a WARNING, that Python never re-decides the verdict,
and that the method needs no database on either transport.
"""

import logging

import pytest

import scistack_gui
from scistack_gui.services.client_build import MAX_FIELD, report_client_build

LOGGER = "scistack_gui.services.client_build"

FROM_HOST = {
    "version": "0.1.30",
    "commit": "abc1234",
    "built_at": "2026-10-15T22:14:00.000Z",
    "target": "webview",
    "view": "plot",
    "extension_version": "0.1.37",
    "extension_path": r"C:\Users\me\.vscode\extensions\scistack.scistack-gui-0.1.37",
    "extension_mode": "Production",
    "verdict": "mismatch",
    "verdict_detail": "webview bundle from release 0.1.30, scistack-gui 0.1.37",
}


@pytest.fixture
def caught(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    return caplog


def _messages(caught, level):
    return [r.getMessage() for r in caught.records if r.name == LOGGER and r.levelno == level]


def test_the_line_names_the_bundle_and_the_extension_that_loaded_it(caught):
    report_client_build({**FROM_HOST, "version": "0.1.37", "verdict": "match", "verdict_detail": ""})
    (line,) = _messages(caught, logging.INFO)
    assert "[client] plot view runs bundle 0.1.37" in line
    assert "commit abc1234" in line
    assert "built 2026-10-15T22:14:00.000Z" in line
    assert FROM_HOST["extension_path"] in line, "the path is what finds a stale copy"
    assert "(Production)" in line
    assert f"scistack-gui {scistack_gui.__version__}" in line
    assert line.endswith("— match")
    assert not _messages(caught, logging.WARNING)


def test_a_mismatch_verdict_is_a_warning(caught):
    result = report_client_build(FROM_HOST)
    (warning,) = _messages(caught, logging.WARNING)
    assert "stale webview bundle in the plot view" in warning
    assert "0.1.30" in warning
    assert result == {"server_version": scistack_gui.__version__, "verdict": "mismatch"}


def test_python_logs_the_hosts_verdict_and_never_re_decides_it(caught):
    """A bundle version unequal to the server's, but judged ``skipped`` by the
    host (a dev build), must not WARN here: one owner of the rule."""
    report_client_build({**FROM_HOST, "version": "0.0.0", "verdict": "skipped",
                         "verdict_detail": "webview bundle is an unstamped dev build"})
    assert not _messages(caught, logging.WARNING)
    (line,) = _messages(caught, logging.INFO)
    assert "skipped (webview bundle is an unstamped dev build)" in line


def test_without_an_extension_host_the_report_is_unchecked(caught):
    """The standalone browser build has no host to add a verdict."""
    result = report_client_build({"version": "0.0.0", "commit": "abc1234", "view": "pipeline",
                                  "target": "standalone"})
    (line,) = _messages(caught, logging.INFO)
    assert "via no extension host" in line
    assert "unchecked (no extension host)" in line
    assert result["verdict"] == "unchecked (no extension host)"
    assert not _messages(caught, logging.WARNING)


def test_an_empty_report_still_logs_something_readable(caught):
    report_client_build({})
    (line,) = _messages(caught, logging.INFO)
    assert "[client] unknown view runs bundle unknown" in line


def test_oversized_fields_are_clipped(caught):
    report_client_build({"commit": "x" * (MAX_FIELD * 3)})
    (line,) = _messages(caught, logging.INFO)
    assert len(line) < MAX_FIELD + 400


def test_the_rpc_is_registered_and_needs_no_database():
    from scistack_gui import server

    assert server.METHODS["report_client_build"].handler.needs_db is False
    assert "report_client_build" in server.SELF_MANAGED_DB_METHODS
    # A plot-only (CSV) server opens a Plot Studio tab too.
    assert "report_client_build" in server.WITHOUT_DB_METHODS


def test_the_json_rpc_path_answers_without_touching_the_database(monkeypatch, caught):
    from scistack_gui import db as db_mod
    from scistack_gui import server

    def _never(*args, **kwargs):
        raise AssertionError("report_client_build must not acquire the database")

    monkeypatch.setattr(db_mod, "acquire_db_connection", _never)
    frames = []
    monkeypatch.setattr(server, "_send", lambda frame: frames.append(frame))

    server._handle_request({"id": 4, "method": "report_client_build", "params": FROM_HOST})
    assert frames and frames[0].get("result", {}).get("verdict") == "mismatch"
    assert any("stale webview bundle" in m for m in _messages(caught, logging.WARNING))


def test_the_http_path_logs_too(client, caught):
    r = client.post("/api/client-build", json={**FROM_HOST, "verdict": None, "extension_path": None})
    assert r.status_code == 200
    assert r.json()["server_version"] == scistack_gui.__version__
    assert any("via no extension host" in m for m in _messages(caught, logging.INFO))
