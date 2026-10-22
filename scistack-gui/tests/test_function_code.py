"""New functions written from the GUI (services/function_code_service.py).

The functions tab's "+" used to offer only a library reference, so a new name
failed with "'name' not found in 'builtins'". A new function is now one file
in the project's ``functions_dir``, discovered like any module and edited in
the sidebar's code panel. Uses ``glue_project``: the same config-mode project
glue nodes are written into.
"""

from pathlib import Path

from scistack_gui import registry as _registry
from scistack_gui.services import function_code_service as fcs


def test_create_writes_a_file_that_is_discovered(client, glue_project):
    out = fcs.create_function("smooth_signal", ["signal", "width"])
    assert out["ok"], out
    path = Path(out["path"])
    assert path.parent.name == "scistack_functions"
    assert path.read_text() == "def smooth_signal(signal, width):\n    return signal\n"
    assert _registry.lookup_function("smooth_signal") is not None


def test_functions_dir_is_recorded_in_the_config(client, glue_project):
    fcs.create_function("step_a")
    assert "functions_dir" in (glue_project / "scistack.toml").read_text()


def test_a_name_that_exists_or_is_glue_or_invalid_is_refused(client, glue_project):
    assert fcs.create_function("step_b")["ok"]
    assert "already exists" in fcs.create_function("step_b")["error"]
    assert "glue" in fcs.create_function("glue_thing")["error"]
    assert not fcs.create_function("not valid")["ok"]
    assert not fcs.create_function("ok_name", ["bad param"])["ok"]


def test_the_body_is_editable_and_a_broken_one_is_rolled_back(client, glue_project):
    fcs.create_function("step_c")
    src = fcs.read_function_source("step_c")
    assert src["ok"] and src["editable"]

    good = "def step_c(data):\n    return data * 2\n"
    assert fcs.update_function_source("step_c", good)["ok"]
    assert fcs.read_function_source("step_c")["source"] == good

    bad = "def step_c(data)\n    return data\n"
    out = fcs.update_function_source("step_c", bad)
    assert not out["ok"] and "not saved" in out["error"]
    assert fcs.read_function_source("step_c")["source"] == good, "rolled back"


def test_a_function_outside_functions_dir_is_read_only(client, glue_project, tmp_path):
    mod = tmp_path / "own_module.py"
    mod.write_text("def mine(x):\n    return x\n")
    import importlib.util

    spec = importlib.util.spec_from_file_location("own_module", mod)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    _registry._functions["mine"] = m.mine
    assert fcs.read_function_source("mine")["editable"] is False
    assert "read-only" in fcs.update_function_source("mine", "x")["error"]


def test_the_handlers_answer(client, glue_project):
    r = client.post("/api/functions/new", json={"name": "step_d", "params": ["data"]})
    assert r.json()["ok"], r.json()
    r = client.get("/api/functions/step_d/code").json()
    assert r["editable"] and "def step_d" in r["source"]
    r = client.put(
        "/api/functions/step_d/code",
        json={"name": "step_d", "source": "def step_d(data):\n    return data\n"},
    )
    assert r.json()["ok"]
