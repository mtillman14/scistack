"""The data-inventory popup's handlers (``.claude/plan-data-inventory.md`` Stage 3).

Statuses, views and the CSV layout belong to ``scidb.inventory`` (tested in
scidb/tests/test_inventory.py); these tests cover only what the GUI layer
adds: the row cap for the popup, the CSV export path, and errors reaching the
client as 400s.
"""

from __future__ import annotations

import csv

import numpy as np
import pytest
from conftest import RawSignal


@pytest.fixture
def inv_client(client):
    # populated_db holds subject {1,2} × session {pre,post}, all present.
    # Subject 3 has only "pre", so (3, post) is missing.
    RawSignal.save(np.random.randn(10), subject=3, session="pre")
    return client


def test_missing_only(inv_client):
    r = inv_client.post("/api/inventory", json={"keys": ["subject", "session"]})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["columns"] == ["subject", "session"]
    assert d["rows"] == [["3", "post"]]
    assert d["counts"] == {"present": 5, "missing": 1, "excluded": 0}
    assert d["product_size"] == 6
    assert d["truncated"] is False


def test_all_is_truncated_to_the_limit_with_full_counts(inv_client):
    r = inv_client.post(
        "/api/inventory", json={"keys": None, "missing_only": False, "collapse": False, "limit": 2}
    )
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["columns"] == ["subject", "session", "status"]
    assert len(d["rows"]) == 2
    assert d["total_rows"] == 6
    assert d["truncated"] is True
    assert d["counts"]["present"] == 5


def test_unknown_key_is_a_400(inv_client):
    r = inv_client.post("/api/inventory", json={"keys": ["visit"]})
    assert r.status_code == 400
    assert "Unknown schema key" in r.text


def test_export_writes_every_row(inv_client, tmp_path):
    out = tmp_path / "exports" / "inventory.csv"
    r = inv_client.post(
        "/api/inventory/export",
        json={"keys": ["subject", "session"], "missing_only": False, "collapse": False,
              "out_path": str(out)},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"path": str(out), "rows": 6}
    with out.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == ["subject", "session", "status"]
    assert ["3", "post", "missing"] in rows
    assert len(rows) == 7


def test_export_refuses_a_relative_path(inv_client):
    r = inv_client.post(
        "/api/inventory/export", json={"keys": None, "out_path": "relative.csv"}
    )
    assert r.status_code == 400
    assert "absolute" in r.text


def test_within_passes_through(inv_client):
    # Within subject: each subject crosses only its own sessions, so subject
    # 3 (only "pre") has nothing missing.
    r = inv_client.post("/api/inventory", json={"keys": None, "within": ["subject"]})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["within"] == ["subject"]
    assert d["keys"] == ["session"]
    assert d["columns"] == ["subject", "session"]
    assert d["rows"] == []
    assert d["product_size"] == 5
    assert [s["values"] for s in d["strata"]] == [
        {"subject": "1"}, {"subject": "2"}, {"subject": "3"},
    ]


def test_within_overlap_is_a_400(inv_client):
    r = inv_client.post(
        "/api/inventory", json={"keys": ["subject", "session"], "within": ["subject"]}
    )
    assert r.status_code == 400
    assert "both Cross and Within" in r.text


def test_export_with_within(inv_client, tmp_path):
    out = tmp_path / "within.csv"
    r = inv_client.post(
        "/api/inventory/export",
        json={"within": ["subject"], "missing_only": False, "collapse": False,
              "out_path": str(out)},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"path": str(out), "rows": 5}
    with out.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == ["subject", "session", "status"]
    assert ["3", "pre", "present"] in rows


def test_collapse_passes_through(inv_client, tmp_path):
    # Subjects 1 and 2 are wholly present: one row each, session "(all)".
    r = inv_client.post("/api/inventory", json={"keys": None, "missing_only": False})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["collapse"] is True
    assert sorted(d["rows"]) == sorted([
        ["1", d["all_marker"], "present"],
        ["2", d["all_marker"], "present"],
        ["3", "pre", "present"],
        ["3", "post", "missing"],
    ])
    out = tmp_path / "collapsed.csv"
    r = inv_client.post(
        "/api/inventory/export",
        json={"keys": None, "missing_only": False, "out_path": str(out)},
    )
    assert r.json() == {"path": str(out), "rows": 4}


# --- Exclude / Include actions (.claude/plan-inventory-exclusion-actions.md) ---
# Row -> keys is scidb.inventory.row_keyset and the rules are scidb.exclusions
# (both tested in scidb/tests); these cover the wiring and the 400s.


def _all_rows(client) -> dict:
    r = client.post("/api/inventory", json={"keys": None, "missing_only": False})
    assert r.status_code == 200, r.text
    return r.json()


def test_exclude_a_row_changes_the_counts(inv_client):
    r = inv_client.post(
        "/api/inventory/exclude",
        json={"columns": ["subject", "session"], "row": ["3", "post"], "reason": "never collected"},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"keys": {"subject": "3", "session": "post"}}
    d = _all_rows(inv_client)
    assert d["counts"] == {"present": 5, "missing": 0, "excluded": 1}


def test_exclude_a_collapsed_row_excludes_the_block(inv_client):
    d = _all_rows(inv_client)
    row = next(r for r in d["rows"] if r[0] == "1")
    assert row[1] == d["all_marker"]
    r = inv_client.post(
        "/api/inventory/exclude", json={"columns": d["columns"], "row": row, "reason": "withdrew"}
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"keys": {"subject": "1"}}
    assert _all_rows(inv_client)["counts"]["excluded"] == 2


def test_row_override_and_partial_reinclude(inv_client):
    cols = ["subject", "session", "status"]
    inv_client.post(
        "/api/inventory/exclude",
        json={"columns": cols, "row": ["2", "(all)", "present"], "reason": "withdrew"},
    )
    r = inv_client.post(
        "/api/inventory/row-override", json={"columns": cols, "row": ["2", "pre", "excluded"]}
    )
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["keys"] == {"subject": "2", "session": "pre"}
    assert d["effective"] == "excluded"
    assert d["governed_by"]["keys"] == {"subject": "2"}
    assert d["governed_by"]["reason"] == "withdrew"

    r = inv_client.post(
        "/api/inventory/include",
        json={"columns": cols, "row": ["2", "pre", "excluded"], "reason": "pre was fine"},
    )
    assert r.status_code == 200, r.text
    rows = {tuple(x[:2]): x[2] for x in inv_client.post(
        "/api/inventory", json={"keys": None, "missing_only": False, "collapse": False}
    ).json()["rows"]}
    assert rows[("2", "pre")] == "present"
    assert rows[("2", "post")] == "excluded"


def test_blank_reason_is_a_400(inv_client):
    r = inv_client.post(
        "/api/inventory/exclude",
        json={"columns": ["subject", "session"], "row": ["3", "post"], "reason": "   "},
    )
    assert r.status_code == 400
    assert "reason is required" in r.text


def test_duplicate_exclude_is_a_400_with_scidbs_message(inv_client):
    body = {"columns": ["subject", "session"], "row": ["3", "post"], "reason": "x"}
    assert inv_client.post("/api/inventory/exclude", json=body).status_code == 200
    r = inv_client.post("/api/inventory/exclude", json=body)
    assert r.status_code == 400
    assert "already excluded" in r.text


def test_include_of_an_included_row_is_a_400(inv_client):
    r = inv_client.post(
        "/api/inventory/include",
        json={"columns": ["subject", "session"], "row": ["1", "pre"], "reason": "x"},
    )
    assert r.status_code == 400
    assert "already included" in r.text


def test_whole_dataset_row_is_a_400(inv_client):
    r = inv_client.post(
        "/api/inventory/exclude",
        json={"columns": ["subject", "status"], "row": ["(all)", "present"], "reason": "x"},
    )
    assert r.status_code == 400
    assert "whole dataset" in r.text
