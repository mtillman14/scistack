"""Plot presets in the GUI backend (services/plot_preset_service.py).

What a preset owns and how it applies are tested where they live
(scistackplot/tests/test_presets.py, scistackplotdb/tests/test_presets.py).
These tests check the adaptation: applying is one round trip with a
capability report, replies are JSON-safe, refusals are 400s, both transports
reach the same code, and applying writes nothing.
"""

import json

import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistack_gui.services import plot_preset_service, plot_service


@pytest.fixture(autouse=True)
def _clear_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


def _spec(db, variable="RawSignal") -> dict:
    return plot_service.describe(db, variable)["spec"]


def _styled(db) -> dict:
    spec = _spec(db)
    return {
        **spec,
        "kind": "band",
        "roles": {**spec.get("roles", {}), "subject": "collapse"},
        "style": {**spec.get("style", {}), "width": 5.5, "title": "Raw", "alpha": 0.4},
    }


def test_save_list_and_apply_to_another_variable(populated_db):
    saved = plot_preset_service.save(
        populated_db, "Band", _styled(populated_db), made_on_shape="1d"
    )
    assert saved["ok"] is True
    preset = saved["preset"]
    assert (preset["name"], preset["made_on"], preset["made_on_shape"]) == ("Band", "RawSignal", "1d")
    assert [p["name"] for p in saved["presets"]] == ["Band"]

    target = _spec(populated_db, "FilteredSignal")
    applied = plot_preset_service.apply(populated_db, preset["preset_id"], target)
    spec = applied["preset"]["spec"]
    assert spec["measures"] == ["FilteredSignal"]
    assert spec["variant_sets"] == target.get("variant_sets", [])
    assert spec["style"]["width"] == 5.5 and spec["style"]["alpha"] == 0.4
    assert "title" not in spec["style"]  # variable text stays with the variable
    assert applied["capabilities"] is not None
    json.dumps(applied)  # crosses the webview boundary


def test_list_warns_about_a_different_shape(populated_db):
    plot_preset_service.save(populated_db, "Band", _styled(populated_db), made_on_shape="1d")
    assert plot_preset_service.list_presets(populated_db, "1d")["presets"][0]["shape_warning"] is None
    assert plot_preset_service.list_presets(populated_db, "scalar")["presets"][0]["shape_warning"]


def test_saving_onto_another_presets_name_is_a_question(populated_db):
    spec = _styled(populated_db)
    a = plot_preset_service.save(populated_db, "A", spec)["preset"]
    b = plot_preset_service.save(populated_db, "B", spec)["preset"]
    asked = plot_preset_service.save(
        populated_db, "B", spec, overwrite=False, current_preset_id=a["preset_id"]
    )
    assert asked["ok"] is False and asked["exists"]["preset_id"] == b["preset_id"]


def test_rename_hide_and_history(populated_db):
    spec = _styled(populated_db)
    preset_id = plot_preset_service.save(populated_db, "A", spec)["preset"]["preset_id"]
    plot_preset_service.save(populated_db, "A", {**spec, "kind": "line"})
    assert [v["version"] for v in plot_preset_service.history(populated_db, preset_id)["versions"]] == [2, 1]
    assert [p["name"] for p in plot_preset_service.rename(populated_db, preset_id, "B")["presets"]] == ["B"]
    assert plot_preset_service.hide(populated_db, preset_id)["presets"] == []
    assert plot_preset_service.hide(populated_db, preset_id, hidden=False)["presets"][0]["name"] == "B"


def test_applying_writes_no_variant_pins(populated_db):
    from scistack_gui import intent_store

    preset_id = plot_preset_service.save(populated_db, "A", _styled(populated_db))["preset"]["preset_id"]
    before = intent_store.variant_selections(populated_db, "FilteredSignal")
    plot_preset_service.apply(populated_db, preset_id, _spec(populated_db, "FilteredSignal"))
    assert intent_store.variant_selections(populated_db, "FilteredSignal") == before


# --- transports ---------------------------------------------------------------


def test_http_and_rpc_reach_the_same_service(client, populated_db):
    from scistack_gui.server import METHODS

    response = client.post(
        "/api/plot/presets/save", json={"name": "Band", "spec": _styled(populated_db)}
    )
    assert response.status_code == 200

    http_list = client.post("/api/plot/presets/list", json={}).json()
    rpc_list = METHODS["plot_preset_list"]({})
    assert http_list == rpc_list

    preset_id = rpc_list["presets"][0]["preset_id"]
    target = _spec(populated_db, "FilteredSignal")
    http_apply = client.post(
        "/api/plot/presets/apply", json={"preset_id": preset_id, "spec": target}
    ).json()
    rpc_apply = METHODS["plot_preset_apply"]({"preset_id": preset_id, "spec": target})
    assert http_apply["preset"]["spec"] == rpc_apply["preset"]["spec"]


def test_refusals_are_400s_over_http(client, populated_db):
    blank = client.post(
        "/api/plot/presets/save", json={"name": "  ", "spec": _styled(populated_db)}
    )
    assert blank.status_code == 400
    assert "name" in blank.text

    missing = client.post(
        "/api/plot/presets/apply", json={"preset_id": "nope", "spec": _spec(populated_db)}
    )
    assert missing.status_code == 400


def test_only_apply_takes_its_own_connection():
    from scistack_gui import server

    assert "plot_preset_apply" in server.SELF_MANAGED_DB_METHODS
    for method in (
        "plot_preset_list",
        "plot_preset_save",
        "plot_preset_rename",
        "plot_preset_hide",
        "plot_preset_history",
    ):
        assert method in server.METHODS
        assert method not in server.SELF_MANAGED_DB_METHODS
