"""The step verdicts, the data view and "Clean up outdated…" in the GUI
(scistack_gui.services.step_status_service; GitHub #28).

Every state and word is scidb's (scidb/tests/test_pipeline_status.py); these
pin that the canvas payload carries the verdict, that the ``data_view``
handler reaches each part, and that the data view's actions reach scidb.

Fixture (conftest ``populated_db``): RawSignal at subject {1,2} x session
{pre,post}, ``bandpass_filter`` run everywhere -> FilteredSignal.
"""


def post(client, path, body):
    r = client.post(path, json=body)
    assert r.status_code == 200, r.text
    return r.json()


def _fn_node(graph):
    return next(n for n in graph["nodes"] if n["type"] == "functionNode" and n["data"]["label"] == "bandpass_filter")


def test_the_canvas_node_carries_its_verdict(client):
    graph = client.get("/api/pipeline").json()
    verdict = _fn_node(graph)["data"]["verdict"]
    assert verdict["word"] == "succeeded"
    assert verdict["chip"] == "" and verdict["new_chip"] == ""
    assert verdict["needs_running"] is False
    assert verdict["status_line"].startswith("✅ Succeeded")
    assert "outputs" not in verdict, "the node carries words and counts, not locations"
    assert graph["exclusions"] == []


def test_view_part_has_one_column_per_variable(client):
    view = post(client, "/api/data-view", {"part": "view"})
    names = [c["variable"] for c in view["columns"]]
    assert names.index("RawSignal") < names.index("FilteredSignal"), "pipeline order"
    filt = next(c for c in view["columns"] if c["variable"] == "FilteredSignal")
    assert filt["footer"] == "4/4"
    assert set(filt["cells"]) == {
        "subject=1|session=pre", "subject=1|session=post",
        "subject=2|session=pre", "subject=2|session=post",
    }
    assert view["dropoff"][0]["count"] == 2


def test_view_part_carries_the_legend(client):
    """The data view's legend (its filter buttons) is built from the reply:
    every state, in scidb's order, each with a glyph, a label and a meaning."""
    view = post(client, "/api/data-view", {"part": "view"})
    assert view["states"][0] == "present" and len(view["states"]) == 8
    for s in view["states"]:
        assert view["glyphs"][s] and view["labels"][s] and view["meanings"][s], s


def test_step_and_cell_parts(client):
    step = post(client, "/api/data-view", {"part": "step", "function_name": "bandpass_filter"})
    assert step["word"] == "succeeded" and len(step["outputs"][0]["locations"]) == 4
    rid = step["outputs"][0]["locations"][0]["record_ids"][0]
    cell = post(client, "/api/data-view", {"part": "cell", "variable": "FilteredSignal", "record_ids": [rid]})
    (rec,) = cell["records"]
    assert rec["made_by"].startswith("made by bandpass_filter with low_hz=20 from RawSignal@")


def test_exclude_for_one_variable_reaches_the_step(client):
    post(client, "/api/inventory/exclude", {
        "columns": ["subject", "session"], "row": ["1", "pre"],
        "reason": "marked missing as expected", "variable": "RawSignal",
    })
    step = post(client, "/api/data-view", {"part": "step", "function_name": "bandpass_filter"})
    loc = next(
        l for l in step["outputs"][0]["locations"]
        if l["location"] == {"subject": "1", "session": "pre"}
    )
    assert loc["state"] == "excluded"
    assert loc["exclusion"]["inherited_from"] == "RawSignal"
    graph = client.get("/api/pipeline").json()
    assert [e["variable"] for e in graph["exclusions"]] == ["RawSignal"]
    assert _fn_node(graph)["data"]["verdict"]["chip"] == "⊘ 1 excluded"


def test_between_keys_round_trip(client):
    assert post(client, "/api/data-view", {"part": "between"})["between"] == []
    out = post(client, "/api/schema/between", {"keys": ["subject"]})
    assert out["between"] == ["subject"] and out["changed"] == ["subject"]
    assert post(client, "/api/data-view", {"part": "between"})["between"] == ["subject"]


def test_cleanup_plan_has_nothing_when_the_code_is_current(client):
    plan = post(client, "/api/data-view", {"part": "cleanup_plan"})
    assert plan["candidates"] == []


def test_bad_parts_are_400(client):
    assert client.post("/api/data-view", json={"part": "bogus"}).status_code == 400
    assert client.post("/api/data-view", json={"part": "step"}).status_code == 400
