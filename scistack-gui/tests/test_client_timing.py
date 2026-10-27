"""
A Plot Studio ◀ ▶ step's click-to-drawn timing reaches scistack.log.

Background: on 2026-10-16 "the arrows are slow" could be explained only up to
the backend's "resolved bar: figure N rendered" line; the panel's debounce
before the request and Plotly's draw after it were invisible. The panel now
times each step (frontend PlotStudio/figureStepTiming.ts) and reports it
through ``report_client_timing``; these tests pin down the line's shape, both
transports, and that it needs no database.
"""

import logging

import pytest

from scistack_gui.services.client_timing import report_client_timing

STEP = {
    "where": "Plot Studio figure_step",
    "label": "group=CBrace, subject=CB06-CB",
    "figure_index": 4,
    "clicks": 1,
    "queued_ms": 180.4,
    "round_trip_ms": 910,
    "draw_ms": 95,
    "total_ms": 1185,
}


@pytest.fixture
def timed(caplog):
    caplog.set_level(logging.INFO, logger="scistack_gui.services.client_timing")
    return caplog


def test_the_step_is_one_info_line_with_every_phase(timed):
    result = report_client_timing(STEP)
    assert result == {
        "where": "Plot Studio figure_step",
        "total_ms": 1185,
        "queued_ms": 180,
        "round_trip_ms": 910,
        "draw_ms": 95,
    }
    (line,) = [r.getMessage() for r in timed.records if r.levelno == logging.INFO]
    assert line == (
        "[client] Plot Studio figure_step figure 4 (group=CBrace, subject=CB06-CB): "
        "1185 ms click->drawn = queued 180 + round trip 910 + draw 95 (1 click(s))"
    )


def test_missing_fields_still_log_something_readable(timed):
    report_client_timing({})
    assert any("[client] webview figure ?" in r.getMessage() for r in timed.records)


def test_the_rpc_is_registered_and_needs_no_database():
    from scistack_gui import server

    assert server.METHODS["report_client_timing"].handler.needs_db is False
    assert "report_client_timing" in server.SELF_MANAGED_DB_METHODS


def test_the_json_rpc_path_answers_without_touching_the_database(monkeypatch, timed):
    from scistack_gui import db as db_mod
    from scistack_gui import server

    def _never(*args, **kwargs):
        raise AssertionError("report_client_timing must not acquire the database")

    monkeypatch.setattr(db_mod, "acquire_db_connection", _never)
    frames = []
    monkeypatch.setattr(server, "_send", lambda frame: frames.append(frame))

    server._handle_request({"id": 4, "method": "report_client_timing", "params": STEP})
    assert frames and frames[0].get("result", {}).get("total_ms") == 1185
    assert any("click->drawn" in r.getMessage() for r in timed.records)


def test_the_http_path_logs_too(client, timed):
    r = client.post("/api/client-timing", json=STEP)
    assert r.status_code == 200
    assert r.json()["round_trip_ms"] == 910
    assert any("click->drawn" in m.getMessage() for m in timed.records)
