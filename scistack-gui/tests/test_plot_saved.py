"""Saved plots in the GUI backend (services/saved_plot_service.py).

The store and the drift handling are tested where they live
(scistackplotdb/tests/test_saved_plots.py, scistackplot/tests/test_restore.py).
These tests check the adaptation: one round trip opens a plot with its
capability report, replies are JSON-safe, refusals are 400s, both transports
reach the same code, and opening a plot writes nothing.
"""

import json

import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistack_gui.services import plot_service, saved_plot_service

VIEW = {"previewMode": "pane", "aspectChoice": "16:9", "figureIndex": 1}


@pytest.fixture(autouse=True)
def _clear_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


def _spec(db) -> dict:
    return plot_service.describe(db, "RawSignal")["spec"]


def test_save_list_and_open_round_trip(populated_db):
    spec = _spec(populated_db)
    saved = saved_plot_service.save(populated_db, "RawSignal", "Fig 1", spec, VIEW)
    assert saved["plot"]["name"] == "Fig 1"
    assert [p["name"] for p in saved["plots"]] == ["Fig 1"]

    listed = saved_plot_service.list_plots(populated_db, "RawSignal")
    assert listed["plots"] == saved["plots"]

    opened = saved_plot_service.open_plot(populated_db, saved["plot"]["plot_id"])
    assert opened["plot"]["spec"] == spec
    assert opened["plot"]["view"] == VIEW
    assert opened["plot"]["notes"] == []
    assert opened["capabilities"] is not None
    json.dumps(opened)  # crosses the webview boundary


def test_open_reconciles_with_todays_data(populated_db):
    spec = {**_spec(populated_db), "show_sample": ["trial"]}  # no trial key here
    saved = saved_plot_service.save(populated_db, "RawSignal", "Fig 1", spec, None)

    opened = saved_plot_service.open_plot(populated_db, saved["plot"]["plot_id"])
    assert opened["plot"]["spec"].get("show_sample", []) == []
    assert [(n["path"], n["kind"]) for n in opened["plot"]["notes"]] == [
        ("show_sample", "not_in_data")
    ]


def test_a_drifted_row_opens_with_notes(populated_db):
    from scistackplotdb.saved import TABLE, ensure_table

    spec = _spec(populated_db)
    spec["kind"] = "pie"
    spec["x_layers"] = ["session"]
    ensure_table(populated_db)
    populated_db._duck._execute(
        f"INSERT INTO {TABLE} VALUES ('old00001', 'RawSignal', 'Old', 1, "
        f"'2026-01-01T00:00:00+00:00', FALSE, ?)",
        [json.dumps({"format": 0, "spec": spec, "view": VIEW})],
    )
    opened = saved_plot_service.open_plot(populated_db, "old00001")
    assert sorted(n["path"] for n in opened["plot"]["notes"]) == ["kind", "x_layers"]
    assert opened["plot"]["view"] == VIEW


def test_older_versions_open_by_number(populated_db):
    spec = _spec(populated_db)
    first = saved_plot_service.save(populated_db, "RawSignal", "Fig 1", spec, None)
    saved_plot_service.save(
        populated_db, "RawSignal", "Fig 1", {**spec, "kind": "line"}, None
    )
    plot_id = first["plot"]["plot_id"]
    versions = saved_plot_service.history(populated_db, plot_id)["versions"]
    assert [v["version"] for v in versions] == [2, 1]
    old = saved_plot_service.open_plot(populated_db, plot_id, version=1)
    assert old["plot"]["spec"]["kind"] == spec["kind"]


def test_rename_and_hide_return_the_refreshed_list(populated_db):
    spec = _spec(populated_db)
    plot_id = saved_plot_service.save(populated_db, "RawSignal", "A", spec, None)["plot"]["plot_id"]

    renamed = saved_plot_service.rename(populated_db, plot_id, "B")
    assert [p["name"] for p in renamed["plots"]] == ["B"]

    hidden = saved_plot_service.hide(populated_db, plot_id)
    assert hidden["plots"] == []
    assert saved_plot_service.hide(populated_db, plot_id, hidden=False)["plots"][0]["name"] == "B"


def test_saving_onto_another_plots_name_is_a_question(populated_db):
    spec = _spec(populated_db)
    a = saved_plot_service.save(populated_db, "RawSignal", "A", spec, None)["plot"]
    b = saved_plot_service.save(populated_db, "RawSignal", "B", spec, None)["plot"]

    asked = saved_plot_service.save(
        populated_db, "RawSignal", "B", spec, None,
        overwrite=False, current_plot_id=a["plot_id"],
    )
    assert asked["ok"] is False
    assert asked["exists"]["plot_id"] == b["plot_id"]

    confirmed = saved_plot_service.save(
        populated_db, "RawSignal", "B", spec, None,
        overwrite=True, current_plot_id=a["plot_id"],
    )
    assert confirmed["ok"] is True
    assert (confirmed["plot"]["plot_id"], confirmed["plot"]["version"]) == (b["plot_id"], 2)

    own = saved_plot_service.save(
        populated_db, "RawSignal", "A", spec, None,
        overwrite=False, current_plot_id=a["plot_id"],
    )
    assert own["ok"] is True and own["plot"]["version"] == 2


def test_opening_writes_no_variant_pins(populated_db):
    """The panel persists pins when its spec's variant_sets change; a second
    writer here would give that concept two owners."""
    from scistack_gui import intent_store

    spec = _spec(populated_db)
    plot_id = saved_plot_service.save(populated_db, "RawSignal", "A", spec, None)["plot"]["plot_id"]
    before = intent_store.variant_selections(populated_db, "RawSignal")
    saved_plot_service.open_plot(populated_db, plot_id)
    assert intent_store.variant_selections(populated_db, "RawSignal") == before


# --- transports ---------------------------------------------------------------


def test_http_and_rpc_reach_the_same_service(client, populated_db):
    from scistack_gui.server import METHODS

    spec = _spec(populated_db)
    response = client.post(
        "/api/plot/saved/save",
        json={"variable": "RawSignal", "name": "Fig 1", "spec": spec, "view": VIEW},
    )
    assert response.status_code == 200

    http_list = client.post("/api/plot/saved/list", json={"variable": "RawSignal"}).json()
    rpc_list = METHODS["plot_saved_list"]({"variable": "RawSignal"})
    assert http_list == rpc_list

    plot_id = rpc_list["plots"][0]["plot_id"]
    http_open = client.post("/api/plot/saved/open", json={"plot_id": plot_id}).json()
    rpc_open = METHODS["plot_saved_open"]({"plot_id": plot_id})
    assert http_open["plot"]["spec"] == rpc_open["plot"]["spec"] == spec


def test_refusals_are_400s_over_http(client, populated_db):
    spec = _spec(populated_db)
    blank = client.post(
        "/api/plot/saved/save",
        json={"variable": "RawSignal", "name": "   ", "spec": spec},
    )
    assert blank.status_code == 400
    assert "name" in blank.text

    legacy = client.post(
        "/api/plot/saved/save",
        json={"variable": "RawSignal", "name": "x", "spec": {**spec, "x_layers": []}},
    )
    assert legacy.status_code == 400

    missing = client.post("/api/plot/saved/open", json={"plot_id": "nope"})
    assert missing.status_code == 400


def test_only_open_takes_its_own_connection():
    from scistack_gui import server

    assert "plot_saved_open" in server.SELF_MANAGED_DB_METHODS
    for method in (
        "plot_saved_list",
        "plot_saved_save",
        "plot_saved_rename",
        "plot_saved_hide",
        "plot_saved_history",
    ):
        assert method in server.METHODS
        assert method not in server.SELF_MANAGED_DB_METHODS
