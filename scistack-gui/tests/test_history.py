"""Undo/redo: change records captured at the handler choke point.

See ``docs/claude/undo-redo.md``. The record/undo/redo machinery is
``scistack_gui.history``; ``api/handlers.py`` wraps undoable handlers in it;
``api/history.py`` exposes undo/redo on both transports.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

from scistack_gui import history
from scistack_gui import layout as layout_store
from scistack_gui.api.handlers import CHANGE_HEADER, pop_change, rpc_methods
from scistack_gui.api.tables import ALL_HANDLERS

GUI_PKG = Path(__file__).parent.parent / "scistack_gui"


@pytest.fixture(autouse=True)
def _fresh_history():
    history.clear()
    yield
    history.clear()


def _change(cid: str, label: str | None = None) -> dict:
    return {CHANGE_HEADER: json.dumps({"id": cid, "label": label})}


def _pipeline_ids(client) -> set[str]:
    return {p["pipeline_id"] for p in client.get("/api/pipelines").json()["pipelines"]}


def _undo(client, cid: str) -> dict:
    return client.post("/api/history/undo", json={"change_id": cid}).json()


def _redo(client, cid: str) -> dict:
    return client.post("/api/history/redo", json={"change_id": cid}).json()


# ---------------------------------------------------------------------------
# Rows: record -> undo -> redo, through the HTTP transport
# ---------------------------------------------------------------------------


class TestRows:
    def test_create_undo_redo_round_trip(self, client):
        before = _pipeline_ids(client)
        reply = client.post("/api/pipelines", json={"name": "Alpha"}, headers=_change("c1"))
        pid = reply.json()["pipeline_id"]
        assert pid in _pipeline_ids(client)

        assert _undo(client, "c1")["status"] == "ok"
        assert _pipeline_ids(client) == before

        assert _redo(client, "c1")["status"] == "ok"
        assert pid in _pipeline_ids(client), "redo brings back the identical row (same id)"

    def test_undo_twice_is_a_noop(self, client):
        client.post("/api/pipelines", json={"name": "Beta"}, headers=_change("c2"))
        assert _undo(client, "c2")["status"] == "ok"
        snapshot = _pipeline_ids(client)
        assert _undo(client, "c2")["status"] == "noop"
        assert _pipeline_ids(client) == snapshot

    def test_redo_of_an_applied_change_is_a_noop(self, client):
        client.post("/api/pipelines", json={"name": "Gamma"}, headers=_change("c3"))
        assert _redo(client, "c3")["status"] == "noop"

    def test_unknown_change(self, client):
        assert _undo(client, "never-recorded")["status"] == "unknown"

    def test_conflict_writes_nothing(self, client):
        pid = client.post(
            "/api/pipelines", json={"name": "Delta"}, headers=_change("c4")
        ).json()["pipeline_id"]
        # A later, unrecorded edit to the same row.
        client.put(f"/api/pipelines/{pid}", json={"name": "Delta renamed"})

        result = _undo(client, "c4")
        assert result["status"] == "conflict"
        assert any("_pipelines" in c for c in result["conflicts"])
        names = {p["name"] for p in client.get("/api/pipelines").json()["pipelines"]}
        assert "Delta renamed" in names, "a refused undo writes nothing"
        # And the record is still applied: a later undo after the conflict
        # clears is not prevented by the refusal.
        assert history.get_record("c4").state == "applied"

    def test_rpc_transport_records_and_strips_the_change(self, client):
        methods = rpc_methods(ALL_HANDLERS)
        reply = methods["create_pipeline"]({"name": "Rpc", "_change": {"id": "r1"}})
        assert reply["ok"] is True
        record = history.get_record("r1")
        assert record is not None and "_pipelines" in record.rows
        assert record.label == "new pipeline", "the Handler's undo_label is the default"

    def test_not_undoable_handler_records_nothing(self, client):
        client.post("/api/pipelines", json={"name": "Eps"}, headers=_change("c5"))
        client.post("/api/plot/invalidate", headers=_change("c6"))
        assert history.get_record("c6") is None

    def test_no_change_id_records_nothing(self, client):
        client.post("/api/pipelines", json={"name": "Zeta"})
        assert history.status()["records"] == 0


# ---------------------------------------------------------------------------
# The recording primitive itself
# ---------------------------------------------------------------------------


class TestRecording:
    def test_pk_table_row_update_and_insert(self, populated_db):
        duck = populated_db._duck
        duck._execute("INSERT INTO _node_config VALUES ('n1', '{\"a\": 1}')")
        with history.recording("t1", label="x", method="m"):
            duck._execute("UPDATE _node_config SET config = '{\"a\": 2}' WHERE node_id = 'n1'")
            duck._execute("INSERT INTO _node_config VALUES ('n2', '{}')")

        assert history.undo("t1")["status"] == "ok"
        rows = dict(duck._fetchall("SELECT node_id, config FROM _node_config"))
        assert rows == {"n1": '{"a": 1}'}

        assert history.redo("t1")["status"] == "ok"
        rows = dict(duck._fetchall("SELECT node_id, config FROM _node_config"))
        assert rows == {"n1": '{"a": 2}', "n2": "{}"}

    def test_table_without_primary_key(self, populated_db):
        duck = populated_db._duck
        with history.recording("t2", label="x", method="m"):
            duck._execute(
                "INSERT INTO _pipeline_path_input_renames (old_name, new_name) VALUES ('a', 'b')"
            )
        assert history.undo("t2")["status"] == "ok"
        assert duck._fetchall("SELECT * FROM _pipeline_path_input_renames") == []
        assert history.redo("t2")["status"] == "ok"
        assert [r[:2] for r in duck._fetchall("SELECT * FROM _pipeline_path_input_renames")] == [
            ("a", "b")
        ]

    def test_empty_change(self, populated_db):
        with history.recording("t3", label="x", method="m"):
            pass
        assert history.undo("t3")["status"] == "empty"
        assert history.undo("t3")["status"] == "noop"

    def test_a_raising_block_records_nothing(self, populated_db):
        with pytest.raises(RuntimeError):
            with history.recording("t4", label="x", method="m"):
                populated_db._duck._execute("INSERT INTO _node_config VALUES ('n9', '{}')")
                raise RuntimeError("boom")
        assert history.get_record("t4") is None

    def test_same_id_merges_earliest_before_latest_after(self, populated_db):
        duck = populated_db._duck
        with history.recording("t5", label="x", method="m"):
            duck._execute("INSERT INTO _node_config VALUES ('m', '1')")
        with history.recording("t5", label="x", method="m"):
            duck._execute("UPDATE _node_config SET config = '2' WHERE node_id = 'm'")
        record = history.get_record("t5")
        assert record.rows["_node_config"][("m",)] == (None, ("m", "2"))
        assert history.undo("t5")["status"] == "ok"
        assert duck._fetchall("SELECT * FROM _node_config WHERE node_id = 'm'") == []

    def test_records_are_bounded(self, populated_db, monkeypatch):
        monkeypatch.setattr(history, "MAX_RECORDS", 3)
        for i in range(5):
            with history.recording(f"b{i}", label="x", method="m"):
                pass
        assert history.get_record("b0") is None
        assert history.get_record("b4") is not None


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------


class TestFiles:
    def test_layout_positions_merge_and_undo(self, client, layout_path):
        existed = layout_path.exists()
        original = layout_path.read_bytes() if existed else None
        # One gesture, two requests (a drop sends create + re-centre).
        client.put("/api/layout/n_drop", json={"x": 1, "y": 2}, headers=_change("g1"))
        client.put("/api/layout/n_drop", json={"x": 5, "y": 6}, headers=_change("g1"))
        assert layout_store.read_layout()["positions"]["n_drop"] == {"x": 5.0, "y": 6.0}

        record = history.get_record("g1")
        assert len(record.files) == 1
        assert not next(iter(record.files.values())).reload, "positions need no reload"

        assert _undo(client, "g1")["status"] == "ok"
        assert "n_drop" not in layout_store.read_layout()["positions"]
        assert (layout_path.read_bytes() if layout_path.exists() else None) == original

    def test_project_alias_file_restored(self, client, tmp_path):
        toml_file = tmp_path / "scistack.toml"
        toml_file.write_text("modules = []\n")
        original = toml_file.read_bytes()
        reply = client.post(
            "/api/plot/project-alias",
            json={"thing": "session", "level": "BL", "alias": "Baseline"},
            headers=_change("a1"),
        )
        assert reply.json()["ok"] is True
        assert toml_file.read_bytes() != original

        result = _undo(client, "a1")
        assert result["status"] == "ok" and result["reload"] is True
        assert toml_file.read_bytes() == original

    def test_hand_edited_file_is_a_conflict(self, client, tmp_path):
        toml_file = tmp_path / "scistack.toml"
        toml_file.write_text("modules = []\n")
        client.post(
            "/api/plot/project-alias",
            json={"thing": "session", "level": "BL", "alias": "Baseline"},
            headers=_change("a2"),
        )
        edited = toml_file.read_text() + "\n# a hand edit\n"
        toml_file.write_text(edited)

        result = _undo(client, "a2")
        assert result["status"] == "conflict"
        assert any("scistack.toml" in c for c in result["conflicts"])
        assert toml_file.read_text() == edited

    def test_a_created_file_is_removed_by_undo(self, tmp_path):
        target = tmp_path / "new_file.py"
        with history.recording("f1", label="x", method="m"):
            history.note_write(target)
            target.write_text("x = 1\n")
        assert history.undo("f1")["status"] == "ok"
        assert not target.exists()
        assert history.redo("f1")["status"] == "ok"
        assert target.read_text() == "x = 1\n"


# ---------------------------------------------------------------------------
# Transport details
# ---------------------------------------------------------------------------


def test_pop_change_strips_and_validates():
    params = {"name": "x", "_change": {"id": "abc", "label": "L"}}
    assert pop_change(params) == {"id": "abc", "label": "L"}
    assert params == {"name": "x"}
    assert pop_change({"_change": "not json"}) is None
    assert pop_change({"_change": {"label": "no id"}}) is None
    assert pop_change({"_change": json.dumps({"id": "s"})}) == {"id": "s", "label": None}
    assert pop_change({}) is None


def test_history_status_lists_undoable_methods(client):
    status = client.get("/api/history/status").json()
    assert status["methods"]["put_edge"] == "connect"
    assert "start_run" not in status["methods"]
    assert "plot_saved_save" not in status["methods"], "saved plots: not undoable (2026-10-03)"


# ---------------------------------------------------------------------------
# Guards: a future feature cannot silently skip undo
# ---------------------------------------------------------------------------


def test_every_mutating_handler_decides_undo():
    """Every non-GET handler (including RPC-only ones) states undoable."""
    undecided = [
        h.name
        for h in ALL_HANDLERS
        if (h.path is None or h.http_method != "GET") and h.undoable is None
    ]
    assert undecided == [], (
        f"declare undoable=True or undoable=False on {undecided} "
        "(docs/claude/undo-redo.md, 'Adding undo to a new feature')"
    )


def test_every_gui_table_is_tracked():
    created: set[str] = set()
    for path in GUI_PKG.rglob("*.py"):
        created |= set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", path.read_text()))
    assert created, "the scan found no tables — the pattern is stale"
    assert created == set(history.tracked_tables()), (
        "a GUI table must be listed in its module's UNDOABLE_TABLES "
        "(docs/claude/undo-redo.md)"
    )


#: Functions that write files without recording them, and why.
_WRITE_ALLOWLIST = {
    ("services/code_export_service.py", "export_pipeline_to_code"): "export",
    ("services/portability_service.py", "export_pipeline_to_file"): "export",
    ("bundle_section.py", "import_"): "writes a NEW project's code (bundle import), not an edit to undo",
    ("bundle_section.py", "_verbatim_import"): "writes a NEW project's layout (bundle import), not an edit to undo",
    ("services/plot_service.py", "_figure_number"): "export manifest",
    ("layout.py", "_replace_with_retry"): "helper; its caller _save notes the write",
}

_WRITE_MODES = re.compile(r"[wax+]")


def _is_file_write(call: ast.Call) -> bool:
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr in ("write_text", "write_bytes"):
        return True
    if (
        isinstance(func, ast.Attribute)
        and func.attr in ("replace", "rename")
        and isinstance(func.value, ast.Name)
        and func.value.id == "os"
    ):
        return True
    is_open = (isinstance(func, ast.Name) and func.id == "open") or (
        isinstance(func, ast.Attribute) and func.attr in ("open", "fdopen")
    )
    if not is_open:
        return False
    mode = None
    positional = 0 if isinstance(func, ast.Attribute) and func.attr == "open" else 1
    if len(call.args) > positional and isinstance(call.args[positional], ast.Constant):
        mode = call.args[positional].value
    for kw in call.keywords:
        if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
            mode = kw.value.value
    return isinstance(mode, str) and bool(_WRITE_MODES.search(mode))


def _own_calls(fn: ast.AST) -> list[ast.Call]:
    """Calls in *fn*'s body, not inside a function nested in it."""
    out: list[ast.Call] = []
    stack = list(ast.iter_child_nodes(fn))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(node, ast.Call):
            out.append(node)
        stack.extend(ast.iter_child_nodes(node))
    return out


def test_every_file_write_notes_history():
    """Every function in scistack_gui that writes a file calls
    ``history.note_write`` (or is allowlisted with a reason)."""
    missing: list[str] = []
    for path in sorted(GUI_PKG.rglob("*.py")):
        rel = path.relative_to(GUI_PKG).as_posix()
        if rel == "history.py":
            continue
        tree = ast.parse(path.read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            own = _own_calls(fn)
            writes = [c for c in own if _is_file_write(c)]
            if not writes:
                continue
            notes = any(
                (isinstance(c.func, ast.Attribute) and c.func.attr == "note_write")
                or (isinstance(c.func, ast.Name) and c.func.id == "note_write")
                for c in own
            )
            if not notes and (rel, fn.name) not in _WRITE_ALLOWLIST:
                missing.append(f"{rel}:{fn.lineno} {fn.name}")
    assert missing == [], (
        "these functions write files without history.note_write — undo would "
        f"miss them: {missing}"
    )
