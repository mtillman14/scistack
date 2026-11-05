"""The duplicate-location popup's "Number the rows" button (set_distribute)."""

from __future__ import annotations

from scidb.model.exceptions import DuplicateLocationError

from scistack_gui.domain.run_options import run_option_defaults
from scistack_gui.stores import pipeline_store


def test_set_distribute_keeps_the_nodes_other_settings(client, populated_db):
    node = "fn__bandpass_filter__zz9dst"
    assert client.put(
        f"/api/layout/{node}",
        json={"x": 0, "y": 0, "node_type": "functionNode", "label": "bandpass_filter"},
    ).status_code == 200
    assert client.put(
        f"/api/layout/{node}/config",
        json={"config": {"runOptions": {"on_error": "stop"}}},
    ).status_code == 200

    r = client.put(f"/api/layout/{node}/distribute", json={"node_id": node, "value": True})
    assert r.status_code == 200, r.text

    options = pipeline_store.get_node_config(populated_db, node)["runOptions"]
    assert options["distribute"] is True
    assert options["on_error"] == "stop"


def test_on_duplicate_is_not_a_run_option():
    assert "on_duplicate" not in run_option_defaults()


def test_the_popup_offers_numbering_only_when_a_lower_key_exists():
    collision = [{"location": {"subject": "S1"}, "count": 2, "differing_columns": ["age"]}]

    lower = DuplicateLocationError("Demo", collision, ["subject", "bout"])
    assert lower.distribute_target == "bout"
    assert [s["action"] for s in lower.remedy["steps"]][-1] == "distribute_rows"
    assert "distribute=True" in str(lower) and "'bout'" in str(lower)

    deepest = DuplicateLocationError("Demo", collision, ["subject"])
    assert deepest.distribute_target is None
    assert "distribute_rows" not in [s["action"] for s in deepest.remedy["steps"]]
    assert "distribute=True" not in str(deepest)


def test_data_preview_is_served_as_json(client, populated_db):
    """The GUI only displays scidb.inspect.preview, over both transports."""
    r = client.post("/api/variables/preview", json={"variable": "RawSignal", "limit": 2})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["n_records"] == 4
    assert len(body["rows"]) == 2 and body["truncated"] is True
    assert {"subject", "session"} <= set(body["levels"])


def test_data_preview_reports_a_refusal_instead_of_failing_the_call(client, populated_db):
    r = client.post(
        "/api/variables/preview", json={"variable": "RawSignal", "location": {"nope": "1"}}
    )
    assert r.status_code == 200
    assert r.json()["ok"] is False
    assert "not schema keys" in r.json()["error"]
