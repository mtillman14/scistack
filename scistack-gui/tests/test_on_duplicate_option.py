"""The ``on_duplicate`` run option in the GUI (scidb.combine).

- on_duplicate_from_config: the one reader; unknown values fall back, logged
- the single-node Run passes it to for_each
- the MATLAB command and the Python export emit it only when "combine"
- set_on_duplicate (the remedy popup's "Combine the rows" button) merges it
  into the node's saved run options without touching the rest

Plan: .claude/plan-combine-duplicate-locations.md (stage 6).
"""

from __future__ import annotations

import logging

from scistack_gui.services.run_failures import on_duplicate_from_config

from .test_run_failures import _fake_for_each, _run


def test_reader_defaults_and_falls_back(caplog):
    assert on_duplicate_from_config(None) == "error"
    assert on_duplicate_from_config({}) == "error"
    assert on_duplicate_from_config({"on_duplicate": "combine"}) == "combine"
    with caplog.at_level(logging.WARNING):
        assert on_duplicate_from_config({"on_duplicate": "merge"}) == "error"
    assert "on_duplicate" in caplog.text


def test_run_passes_on_duplicate_to_for_each(client, bp_node_id, monkeypatch):
    seen: list[dict] = []
    _run(
        client, bp_node_id, monkeypatch, _fake_for_each([], seen=seen),
        run_options={"on_duplicate": "combine"},
    )
    assert seen and all(k["on_duplicate"] == "combine" for k in seen)


def test_run_default_is_error(client, bp_node_id, monkeypatch):
    seen: list[dict] = []
    _run(client, bp_node_id, monkeypatch, _fake_for_each([], seen=seen))
    assert seen and all(k["on_duplicate"] == "error" for k in seen)


def test_matlab_pairs_emit_only_combine():
    from scistack_gui.api.matlab_command import _format_run_option_pairs

    assert "'on_duplicate', 'combine'" in _format_run_option_pairs(
        {"on_duplicate": "combine"}, "f"
    )
    assert _format_run_option_pairs({"on_duplicate": "error"}, "f") == ""


def test_python_export_emits_only_combine(client, bp_node_id):
    from scistack_gui import pipeline_store
    from scistack_gui.db import get_db
    from scistack_gui.services.code_export_service import export_pipeline_to_code

    db = get_db()
    pipeline_store.update_node_config(db, bp_node_id, {"runOptions": {"on_duplicate": "combine"}})
    assert "on_duplicate='combine'" in export_pipeline_to_code(db, "main")["script"]
    pipeline_store.update_node_config(db, bp_node_id, {"runOptions": {"on_duplicate": "error"}})
    assert "on_duplicate" not in export_pipeline_to_code(db, "main")["script"]


def test_set_on_duplicate_merges_into_run_options(client, bp_node_id):
    from scistack_gui import pipeline_store
    from scistack_gui.db import get_db

    db = get_db()
    pipeline_store.update_node_config(
        db, bp_node_id,
        {"runOptions": {"distribute": True, "on_error": "stop"}, "schemaLevel": ["subject"]},
    )
    r = client.put(
        f"/api/layout/{bp_node_id}/on-duplicate",
        json={"node_id": bp_node_id, "value": "combine"},
    )
    assert r.status_code == 200
    config = pipeline_store.get_node_config(db, bp_node_id)
    assert config["runOptions"] == {
        "distribute": True, "on_error": "stop", "on_duplicate": "combine",
    }
    assert config["schemaLevel"] == ["subject"]


def test_set_on_duplicate_rejects_unknown_value(client, bp_node_id):
    r = client.put(
        f"/api/layout/{bp_node_id}/on-duplicate",
        json={"node_id": bp_node_id, "value": "merge"},
    )
    assert r.status_code >= 400
