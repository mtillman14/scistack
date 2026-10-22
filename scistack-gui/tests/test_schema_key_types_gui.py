"""The GUI's schema-key type surface: the "Numeric" checkbox and the
digit-only ``key_regex`` auto-declaration (``.claude/plan-schema-key-types-gui.md``
Stage 4).

The database owns the declaration (``scidb.schema_types``, tested in
scidb/tests/test_schema_key_types_stored.py). These tests cover only what the
GUI adds: the RPC shapes, ``needs_confirm`` instead of an exception, and Q2 —
a digit-only regex saved through the GUI declares an UNDECLARED key numeric
when that re-spells nothing.
"""

from __future__ import annotations

import numpy as np
import pytest
from conftest import RawSignal

from scistack_gui.services import schema_key_type_service as svc


def _db():
    from scistack_gui.db import get_db

    return get_db()


def test_get_lists_every_key_undeclared(client):
    r = client.get("/api/schema/key-types")
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["schema_keys"] == ["subject", "session"]
    assert d["key_types"] == {}
    by_key = {k["key"]: k for k in d["keys"]}
    assert by_key["subject"]["loads_as"] == "numbers"
    assert by_key["session"]["loads_as"] == "strings"
    assert by_key["session"]["key_type"] is None


def test_set_numeric_then_read_back(client):
    r = client.put("/api/schema/key-types/subject", json={"key": "subject", "key_type": "numeric"})
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True
    assert client.get("/api/schema/key-types").json()["key_types"] == {"subject": "numeric"}
    # Unticking clears it.
    client.put("/api/schema/key-types/subject", json={"key": "subject", "key_type": None})
    assert client.get("/api/schema/key-types").json()["key_types"] == {}


def test_text_values_block_numeric(client):
    r = client.put("/api/schema/key-types/session", json={"key": "session", "key_type": "numeric"})
    d = r.json()
    assert d["ok"] is False
    assert "not numbers" in d["error"]
    assert sorted(d["impact"]["invalid"]) == ["post", "pre"]


def test_respelling_needs_confirmation(client):
    RawSignal.save(np.random.randn(10), subject="01", session="pre")
    r = client.post(
        "/api/schema/key-types/impact", json={"key": "subject", "key_type": "numeric"}
    )
    assert r.json()["respelled"] == [["01", "1"]]

    d = client.put(
        "/api/schema/key-types/subject", json={"key": "subject", "key_type": "numeric"}
    ).json()
    assert d["ok"] is False and d["needs_confirm"] is True
    assert d["impact"]["merged"] == {"1": ["01", "1"]}

    d = client.put(
        "/api/schema/key-types/subject",
        json={"key": "subject", "key_type": "numeric", "confirm_respelling": True},
    ).json()
    assert d["ok"] is True
    assert d["key_types"] == {"subject": "numeric"}


def test_unknown_key_is_a_400(client):
    r = client.post("/api/schema/key-types/impact", json={"key": "visit", "key_type": "numeric"})
    assert r.status_code == 400


class TestDigitOnlyKeys:
    def test_every_arm_must_agree(self):
        assert svc.digit_only_keys({"trial": r"\d+", "speed": "[A-Za-z]+"}) == ["trial"]
        assert svc.digit_only_keys(
            {"trial": r"\d+"}, [{"key_regex": {"trial": "[a-z]+"}}]
        ) == []
        assert svc.digit_only_keys(None, [{"key_regex": {"trial": r"\d{3}"}}]) == ["trial"]


class TestAutoDeclare:
    def test_undeclared_clean_key_is_declared(self, client):
        out = svc.auto_declare_from_key_regex(_db(), {"subject": r"\d+"})
        assert out == {"declared": ["subject"], "hints": []}
        assert _db().dataset_schema_key_types == {"subject": "numeric"}
        from scidb import schema_types

        assert schema_types.history(_db()._duck, "subject")[-1]["source"] == svc.SOURCE_KEY_REGEX

    def test_already_declared_is_left_alone(self, client):
        _db().set_schema_key_type("subject", "string")
        out = svc.auto_declare_from_key_regex(_db(), {"subject": r"\d+"})
        assert out == {"declared": [], "hints": []}
        assert _db().dataset_schema_key_types == {"subject": "string"}

    def test_text_values_give_a_hint_not_a_declaration(self, client):
        out = svc.auto_declare_from_key_regex(_db(), {"session": r"\d+"})
        assert out["declared"] == []
        assert out["hints"][0]["key"] == "session"
        assert _db().dataset_schema_key_types == {}

    def test_respelling_gives_a_hint_not_a_declaration(self, client):
        RawSignal.save(np.random.randn(10), subject="01", session="pre")
        out = svc.auto_declare_from_key_regex(_db(), {"subject": r"\d+"})
        assert out["declared"] == []
        assert "Tick Numeric" in out["hints"][0]["reason"]

    def test_non_schema_placeholder_is_ignored(self, client):
        out = svc.auto_declare_from_key_regex(_db(), {"trial": r"\d+"})
        assert out == {"declared": [], "hints": []}

    def test_path_input_save_runs_it(self, client):
        from scistack_gui.services import layout_service

        out = layout_service._auto_declare_schema_key_types({"subject": r"\d+"}, None)
        assert out["declared"] == ["subject"]
        # No digit-only pattern: nothing asked of the database.
        assert layout_service._auto_declare_schema_key_types({"subject": "[a-z]+"}, None) == {
            "declared": [],
            "hints": [],
        }


@pytest.mark.parametrize("bad", ["int", "Numeric"])
def test_bad_type_is_a_400(client, bad):
    r = client.post("/api/schema/key-types/impact", json={"key": "subject", "key_type": bad})
    assert r.status_code == 400
