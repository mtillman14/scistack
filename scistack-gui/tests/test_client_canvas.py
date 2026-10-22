"""
The canvas's applied refreshes, and anything they wiped, reach scidb.log.

Background: on 2026-10-22 (after canvas-as-document) a node or edge just
placed vanished for a moment and came back. scidb.log showed the server's
``get_pipeline`` answers but not which refresh the canvas applied, in what
order, or what it wiped. The canvas now reports each applied refresh
(frontend components/DAG/canvasRefreshLog.ts) through
``report_canvas_refresh``; these tests pin the lines, both transports, that
it needs no database, and the ``source`` label on the placement's own push.
"""

import logging

import pytest

from scistack_gui.services.client_canvas import report_canvas_refresh

WIPED = {
    "seq": 7,
    "trigger": "dag_updated:db_watcher",
    "scope": "main",
    "nodes": 3,
    "edges": 2,
    "get_pipeline_ms": 702.4,
    "get_layout_ms": 151,
    "total_ms": 853,
    "superseded_by": 8,
    "settled": 0,
    "folded": 0,
    "dropped": [
        {
            "kind": "node",
            "id": "fn__filter__abc123",
            "age_ms": 610,
            "confirm_after_start_ms": 240,
            "verdict": "stale_snapshot",
        }
    ],
}


@pytest.fixture
def logged(caplog):
    caplog.set_level(logging.INFO, logger="scistack_gui.services.client_canvas")
    return caplog


def test_one_info_line_per_refresh_with_counts_timings_and_supersession(logged):
    result = report_canvas_refresh(WIPED)
    assert result == {"seq": 7, "trigger": "dag_updated:db_watcher", "wiped": 1}
    (info,) = [r.getMessage() for r in logged.records if r.levelno == logging.INFO]
    assert info == (
        "[client] canvas refresh #7 (dag_updated:db_watcher, scope=main): applied "
        "3 node(s), 2 edge(s); get_pipeline 702 ms + get_layout 151 ms = 853 ms; "
        "pending settled 0, folded 0, wiped 1; SUPERSEDED by #8 (an older picture applied)"
    )


def test_each_wiped_item_is_a_warning_with_its_verdict(logged):
    report_canvas_refresh(WIPED)
    (warn,) = [r.getMessage() for r in logged.records if r.levelno == logging.WARNING]
    assert warn == (
        "[client] canvas refresh #7 (dag_updated:db_watcher) wiped the just-placed "
        "node fn__filter__abc123 (on screen 610 ms; its write was confirmed 240 ms "
        "AFTER this refresh started) -> stale_snapshot"
    )


def test_unconfirmed_and_server_omitted_wording(logged):
    report_canvas_refresh({
        "seq": 2,
        "dropped": [
            {"kind": "edge", "id": "manual__e1", "age_ms": 90,
             "confirm_after_start_ms": None, "verdict": "stale_snapshot"},
            {"kind": "node", "id": "var__X__q", "age_ms": 900,
             "confirm_after_start_ms": -300, "verdict": "server_omitted"},
        ],
    })
    warns = [r.getMessage() for r in logged.records if r.levelno == logging.WARNING]
    assert "its write was still unconfirmed) -> stale_snapshot" in warns[0]
    assert "confirmed -300 ms before this refresh started) -> server_omitted" in warns[1]


def test_a_clean_refresh_has_no_warning_and_no_superseded_suffix(logged):
    report_canvas_refresh({"seq": 1, "trigger": "effect:graph_version=0", "settled": 1})
    assert not [r for r in logged.records if r.levelno == logging.WARNING]
    (info,) = [r.getMessage() for r in logged.records]
    assert "SUPERSEDED" not in info and "settled 1" in info


def test_the_rpc_is_registered_and_needs_no_database():
    from scistack_gui import server

    assert server.METHODS["report_canvas_refresh"].handler.needs_db is False
    assert "report_canvas_refresh" in server.SELF_MANAGED_DB_METHODS


def test_the_json_rpc_path_answers_without_touching_the_database(monkeypatch, logged):
    from scistack_gui import db as db_mod
    from scistack_gui import server

    def _never(*args, **kwargs):
        raise AssertionError("report_canvas_refresh must not acquire the database")

    monkeypatch.setattr(db_mod, "acquire_db_connection", _never)
    frames = []
    monkeypatch.setattr(server, "_send", lambda frame: frames.append(frame))

    server._handle_request({"id": 4, "method": "report_canvas_refresh", "params": WIPED})
    assert frames and frames[0].get("result", {}).get("wiped") == 1
    assert any("wiped the just-placed" in r.getMessage() for r in logged.records)


def test_the_http_path_logs_too(client, logged):
    r = client.post("/api/client-canvas-refresh", json=WIPED)
    assert r.status_code == 200
    assert r.json()["seq"] == 7
    assert any("canvas refresh #7" in m.getMessage() for m in logged.records)


def test_a_placement_labels_its_own_dag_updated(monkeypatch):
    from scistack_gui.services import layout_service

    pushed = []
    monkeypatch.setattr("scistack_gui.api.ws.push_message", lambda msg: pushed.append(msg))
    layout_service._notify_dag_updated("put_layout")
    layout_service._notify_dag_updated()
    assert pushed == [
        {"type": "dag_updated", "source": "put_layout"},
        {"type": "dag_updated"},
    ]
