"""One name, one PathInput (2026-09-25).

A PathInput's ``name=`` is its identity (docs/claude/identity-layers-pathinput.md),
so the GUI holds it unique where it can:

* a canvas shows at most ONE node per PathInput — a second drop on the same
  canvas is refused (``layout_service.put_layout``); another canvas (a
  hypothesis tab / sub-pipeline) may show it too;
* a name declared in two different source files is a load error, not a
  silent shadow (``registry._register_path_input``);
* a binding whose PathInput carries a different ``name=`` is a load error
  unless the name is also bound under itself (a re-export).
"""

from __future__ import annotations

from scifor import PathInput
from scistack_gui import registry
from scistack_gui.ids import ROOT_SCOPE
from scistack_gui.services import layout_service


def _drop(client, node_id, label, pipeline_id=ROOT_SCOPE):
    return client.put(
        f"/api/layout/{node_id}",
        json={
            "x": 0,
            "y": 0,
            "node_type": "pathInputNode",
            "label": label,
            "pipeline_id": pipeline_id,
        },
    ).json()


class TestOneNodePerCanvas:
    def test_second_drop_on_the_same_canvas_is_refused(self, client_with_variable_file):
        client = client_with_variable_file
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
        assert _drop(client, "pathInput__gait_data__aaaaaa", "gait_data")["ok"]

        second = _drop(client, "pathInput__gait_data__bbbbbb", "gait_data")
        assert second["ok"] is False
        assert second["reason"] == "duplicate_path_input"
        assert "already on this canvas" in second["error"]

    def test_the_same_pathinput_on_another_canvas_is_fine(self, client_with_variable_file):
        client = client_with_variable_file
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
        assert _drop(client, "pathInput__gait_data__aaaaaa", "gait_data")["ok"]
        pid = client.post("/api/pipelines", json={"name": "other"}).json()["pipeline_id"]
        assert _drop(client, "pathInput__gait_data__cccccc", "gait_data", pid)["ok"]

    def test_a_different_pathinput_is_not_blocked(self, client_with_variable_file):
        client = client_with_variable_file
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
        client.post("/api/path-inputs", json={"name": "other_data", "template": "{subject}.txt"})
        assert _drop(client, "pathInput__gait_data__aaaaaa", "gait_data")["ok"]
        assert _drop(client, "pathInput__other_data__dddddd", "other_data")["ok"]

    def test_moving_the_placed_node_is_not_a_second_drop(self, client_with_variable_file):
        """A position write (drag, re-center) carries no type/label and is
        never checked — only a CREATION can duplicate."""
        client = client_with_variable_file
        client.post("/api/path-inputs", json={"name": "gait_data", "template": "{subject}.csv"})
        assert _drop(client, "pathInput__gait_data__aaaaaa", "gait_data")["ok"]
        existing = layout_service._placed_path_input("gait_data", ROOT_SCOPE)
        assert existing is not None
        moved = client.put(
            f"/api/layout/{existing}", json={"x": 50, "y": 50, "pipeline_id": ROOT_SCOPE}
        ).json()
        assert moved["ok"]


class TestOneDeclarationPerName:
    def test_a_second_file_declaring_the_name_is_a_load_error(self, tmp_path):
        first = PathInput("{s}/a.csv", name="raw")
        second = PathInput("{s}/b.csv", name="raw")
        registry._register_path_input("raw", first, source=str(tmp_path / "one.py"))
        registry._register_path_input("raw", second, source=str(tmp_path / "two.py"))

        assert registry.get_path_input("raw") is first, "the first declaration keeps the name"
        errors = [e for e in registry.get_load_errors() if "two.py" in e["source"]]
        assert errors and "already declared" in errors[0]["error"]

    def test_a_mismatched_binding_is_a_load_error(self, tmp_path):
        import types

        module = types.ModuleType("mismatch")
        module.GAIT = PathInput("{s}/a.csv", name="GaitSpeed")
        registry._scan_module_path_inputs(module, source=str(tmp_path / "m.py"))

        assert registry.get_path_input("GAIT") is None
        assert registry.get_path_input("GaitSpeed") is None
        assert any("nothing binds it" in e["error"] for e in registry.get_load_errors())

    def test_a_reexport_is_not_an_error(self, tmp_path):
        import types

        module = types.ModuleType("reexport")
        module.GaitSpeed = PathInput("{s}/a.csv", name="GaitSpeed")
        module.alias = module.GaitSpeed
        registry._scan_module_path_inputs(module, source=str(tmp_path / "r.py"))

        assert registry.get_path_input("GaitSpeed") is module.GaitSpeed
        assert registry.get_path_input("alias") is None
        assert not registry.get_load_errors()
