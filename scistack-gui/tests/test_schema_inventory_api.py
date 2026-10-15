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
        "/api/inventory", json={"keys": None, "missing_only": False, "limit": 2}
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
        json={"keys": ["subject", "session"], "missing_only": False, "out_path": str(out)},
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
        json={"within": ["subject"], "missing_only": False, "out_path": str(out)},
    )
    assert r.status_code == 200, r.text
    assert r.json() == {"path": str(out), "rows": 5}
    with out.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == ["subject", "session", "status"]
    assert ["3", "pre", "present"] in rows
