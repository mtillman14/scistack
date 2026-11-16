"""The duplicate-location popup's "Change the schema keys" dialog, backend half
(``schema_keys_service``; GitHub issue #10, .claude/plan-change-schema-keys.md).

Pinned: the proposal is the wired PathInput's placeholder keys then the data
columns (each tagged with its source); applying reopens the database with the
new keys; a dataset that holds data is refused and left open.
"""

from __future__ import annotations

import numpy as np
import pytest
import scifor as _scifor
from scifor import PathInput

from scidb import BaseVariable, configure_database
from scistack_gui.ids import in_handle, path_input_node_id
from scistack_gui.project import registry as _registry
from scistack_gui.runtime import db as _gui_db
from scistack_gui.services import schema_keys_service
from scistack_gui.stores import node_wiring
from scistack_gui.stores import pipeline_store as ps


class SkGuiVar(BaseVariable):
    pass


@pytest.fixture
def empty_db(tmp_path):
    _scifor.set_schema([])
    path = tmp_path / "keys.duckdb"
    db = configure_database(path, ["subject"])
    _gui_db._db = db
    _gui_db._db_path = path
    ps._ensure_tables(db)
    yield db
    _gui_db.close_db()
    _registry._path_inputs.pop("KeysPI", None)
    _scifor.set_schema([])


@pytest.fixture
def wired_node(empty_db):
    _registry._path_inputs["KeysPI"] = PathInput(
        "{subject}/{visit}/data.csv", name="KeysPI", root_folder="."
    )
    node = str(node_wiring.mint_node_id("read_csv_like"))
    ps.write_manual_node(empty_db, node, "functionNode", "read_csv_like")
    ps.write_manual_edge(empty_db, {
        "id": "pi_edge", "source": str(path_input_node_id("KeysPI")),
        "target": node, "targetHandle": in_handle("path"),
    })
    return node


def test_proposal_is_path_input_keys_then_data_columns_tagged(empty_db, wired_node):
    p = schema_keys_service.schema_key_proposal(empty_db, wired_node, ["subject", "score", "age"])
    assert p["existing"] == ["subject"]
    assert p["dataset_empty"] is True
    assert p["proposed"] == [
        {"key": "subject", "source": "path_input"},
        {"key": "visit", "source": "path_input"},
        {"key": "score", "source": "data_column"},
        {"key": "age", "source": "data_column"},
    ]


def test_apply_reopens_the_database_with_the_new_keys(empty_db, wired_node):
    result = schema_keys_service.change_schema_keys(empty_db, ["subject", "visit"], {})
    assert result["ok"] is True
    assert result["new_keys"] == ["subject", "visit"]
    assert _gui_db.get_db().dataset_schema_keys == ["subject", "visit"]
    SkGuiVar.save(np.arange(3.0), db=_gui_db.get_db(), subject="S1", visit="1")


def test_a_used_key_is_refused_and_the_database_stays_open(empty_db):
    SkGuiVar.save(np.arange(3.0), db=empty_db, subject="S1")
    result = schema_keys_service.change_schema_keys(
        empty_db, ["participant"], {"subject": "participant"}
    )
    assert result["ok"] is False
    assert "in use" in result["error"]
    assert _gui_db.get_db().dataset_schema_keys == ["subject"]
    assert _gui_db.get_db()._duck.is_open
