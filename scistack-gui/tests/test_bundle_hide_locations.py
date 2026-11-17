"""Hiding PathInput locations through the GUI's own export (issue #16).

``scidb/tests/test_path_redaction.py`` proves the redaction with fake sections;
these go through the real ``GuiSection`` and the HTTP handler a user's export
button calls: a PathInput on the canvas, a root_folder in the registry, a
folder run before (the path-input history table), a note quoting a path.
"""

from __future__ import annotations

import json
import zipfile

import duckdb
import pytest

from scifor import PathInput

from scistack_gui.project import registry as _registry
from scistack_gui.runtime.db import get_db
from scistack_gui.stores import layout as layout_store
from scistack_gui.stores import pipeline_store as ps

from tests.test_bundle_cli import pinned_root  # noqa: F401  (fixture)

RAW_ROOT = "/exporter/archive/raw"
OLD_ROOT = "/exporter/older/raw"


@pytest.fixture
def raw_on_canvas(client, pinned_root):  # noqa: F811
    """A PathInput RAW with a root folder, placed on the root canvas, and a
    history row for a folder it was once run from."""
    _registry._register_path_input(
        "RAW",
        PathInput(
            "{subject}/{session}.csv", name="RAW", root_folder=RAW_ROOT,
            aliases={"session": {"BL": ["Baseline"]}},
        ),
        source="test",
    )
    client.put("/api/layout/pi_raw", json={
        "x": 0, "y": 0, "node_type": "pathInputNode", "label": "RAW", "pipeline_id": "main",
    })
    ps.record_path_input_value(get_db(), "RAW", "{subject}/{session}.csv", OLD_ROOT)
    return pinned_root


def _export(client, root, *, hide, data=False, name="out"):
    out = root / f"{name}.scistack"
    r = client.post("/api/bundles/export", json={
        "out_path": str(out),
        "options": {"hide_path_locations": hide, "include_data": data},
    })
    return out, r


def _text_members(out) -> str:
    with zipfile.ZipFile(out) as zf:
        return "\n".join(
            zf.read(n).decode("utf-8", "replace")
            for n in zf.namelist() if not n.endswith(".parquet")
        )


def _gui_path_input(out, name):
    with zipfile.ZipFile(out) as zf:
        gui = json.loads(zf.read("gui/gui.json"))
    return next(p for p in gui["path_inputs"] if p["name"] == name)


def test_preserve_keeps_the_folder_in_the_canvas_declaration(client, raw_on_canvas):
    out, r = _export(client, raw_on_canvas, hide=False)
    assert r.status_code == 200, r.text
    assert _gui_path_input(out, "RAW")["root_folder"] == RAW_ROOT
    assert r.json()["redaction"] is None


def test_hide_removes_the_folder_from_the_canvas_declaration_and_the_response(client, raw_on_canvas):
    out, r = _export(client, raw_on_canvas, hide=True)
    assert r.status_code == 200, r.text

    shown = _gui_path_input(out, "RAW")
    assert shown["root_folder"] is None
    assert shown["template"] == "{subject}/{session}.csv"
    assert shown["aliases"] == {"session": {"BL": ["Baseline"]}}

    for secret in (RAW_ROOT, str(raw_on_canvas)):
        assert secret not in _text_members(out)
        # The response's own "path" is the file just written; the report must not repeat a folder.
        assert secret not in json.dumps(r.json()["redaction"])
    assert "RAW" in {name for _, name in r.json()["redaction"]["hidden"]}


def test_hide_with_data_rewrites_the_path_input_history_table(client, raw_on_canvas):
    out, r = _export(client, raw_on_canvas, hide=True, data=True)
    assert r.status_code == 200, r.text

    with zipfile.ZipFile(out) as zf:
        names = zf.namelist()
        assert "gui/verbatim/_pipeline_path_input_history.parquet" in names
        parquet = raw_on_canvas / "history.parquet"
        parquet.write_bytes(zf.read("gui/verbatim/_pipeline_path_input_history.parquet"))
    rows = duckdb.connect().execute(
        "SELECT name, template, root_folder FROM read_parquet(?)", [parquet.as_posix()]
    ).fetchall()
    assert rows == [("RAW", "{subject}/{session}.csv", "")]
    # The older folder is gone from every text member as well.
    assert OLD_ROOT not in _text_members(out)
    # Saved data is exported as it is, and the export says so.
    assert any("saved data" in w for w in r.json()["warnings"])


def test_the_live_canvas_and_history_are_untouched_by_a_hiding_export(client, raw_on_canvas):
    _export(client, raw_on_canvas, hide=True, data=True)
    assert _registry.get_path_inputs_registry()["RAW"].root_folder is not None
    row = get_db()._duck.fetchone("SELECT root_folder FROM _pipeline_path_input_history")
    assert row[0] == OLD_ROOT


def test_a_note_quoting_the_project_folder_refuses_the_export(client, raw_on_canvas):
    layout_store.write_note("variable:RawSignal", f"raw files are under {raw_on_canvas}/data")
    out, r = _export(client, raw_on_canvas, hide=True)
    assert r.status_code == 400, r.text
    assert "gui/gui.json" in r.text
    assert str(raw_on_canvas) not in r.text
    assert not out.exists() and not out.with_name(out.name + ".tmp").exists()

    # Without hiding, the same note exports fine.
    out2, r2 = _export(client, raw_on_canvas, hide=False, name="plain")
    assert r2.status_code == 200, r2.text


def test_hide_rewrites_folders_quoted_in_the_logs(client, raw_on_canvas):
    """Issue #20: the logs section quotes folders freely (every file a run
    read). Logs are SciStack's own text, so a hiding export rewrites them
    instead of refusing — and nothing of the folder is left."""
    from scistacklog import Log

    before = (Log.get_path(), Log.get_journal_path())
    Log.set_path(str(raw_on_canvas / "scistack-debug.log"), journal_path=str(raw_on_canvas / "scistack.log"))
    try:
        Log.info(f"loading {RAW_ROOT}/S01/BL.csv from {raw_on_canvas}")
        out, r = _export(client, raw_on_canvas, hide=True, name="logs")
    finally:
        Log.set_path(*before)
    assert r.status_code == 200, r.text
    with zipfile.ZipFile(out) as zf:
        debug = zf.read("logs/scistack-debug.log").decode("utf-8")
    line = next(l for l in debug.splitlines() if "/S01/BL.csv" in l)
    assert RAW_ROOT not in line and str(raw_on_canvas) not in line
    # Tokens are PathRedaction._token(label); a PathInput's label is "PathInput <name>".
    assert "<PathInput RAW>" in line and "<project>" in line
    assert any(k.startswith("logs/") for k in r.json()["redaction"]["replaced"])
