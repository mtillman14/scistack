"""Variant deletion: the one real delete in scidb.

``.claude/plan-variants-popup.md`` Stage 3; rules in
``docs/claude/variant-pins-and-deletion.md`` §3. What is pinned here:

1. **The plan is the delete.** ``delete_plan`` writes nothing, and
   ``delete_variant`` removes exactly what it named.
2. **Downstream goes too**: records computed from the deleted ones.
3. **What survives**: shared constants, raw inputs, the other variant, and any
   run that also produced surviving records.
4. **Older saves of the variant go too**, or deleting the latest would promote
   an older one back to "latest".
5. **All or nothing**: a failure mid-way, or a plan that changed since it was
   shown, deletes nothing.
6. A tombstone is written; a pin left pointing at nothing is released.
7. The constant target (D9) reaches every variable built with that value.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb import variant_delete
from scidb.exceptions import AmbiguousVersionError
from scidb.inspect.cli import main as cli_main
from scidb.variant_delete import delete_plan, delete_variant, tombstones
from scidb.variant_pins import active_pins, pin_history, pin_variant

SCHEMA = ["subject", "trial"]
SUBJECTS = ["01", "02"]


class VdRaw(BaseVariable):
    pass


class VdFiltered(BaseVariable):
    pass


class VdSteps(BaseVariable):
    pass


class VdOther(BaseVariable):
    pass


def vd_bandpass(signal, low_hz):
    return np.asarray(signal) * float(low_hz)


def vd_detect(filtered):
    return float(np.sum(filtered))


def vd_other(signal, low_hz):
    return float(np.sum(signal)) - float(low_hz)


def _build(path, low20_subjects=SUBJECTS):
    _scifor.set_schema([])
    db = configure_database(path, SCHEMA)
    for subject in SUBJECTS:
        VdRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)
    for_each(
        vd_bandpass, {"signal": VdRaw, "low_hz": 10}, [VdFiltered],
        subject=list(SUBJECTS), trial=[],
    )
    for_each(
        vd_bandpass, {"signal": VdRaw, "low_hz": 20}, [VdFiltered],
        subject=list(low20_subjects), trial=[],
    )
    for_each(vd_detect, {"filtered": VdFiltered}, [VdSteps], subject=[], trial=[])
    return db


@pytest.fixture
def db(tmp_path):
    database = _build(tmp_path / "vd.duckdb")
    yield database
    database.close()
    _scifor.set_schema([])


@pytest.fixture
def gap_db(tmp_path):
    database = _build(tmp_path / "vd_gap.duckdb", low20_subjects=["01"])
    yield database
    database.close()
    _scifor.set_schema([])


def _card(db, variable, low_hz):
    for card in db.inspect.variant_cards(variable).cards:
        if card.selection.get("vd_bandpass.low_hz") == low_hz:
            return card
    raise AssertionError(f"no {variable} card with low_hz={low_hz}")


def _target(db, low_hz, variable="VdFiltered"):
    return [{"variable": variable, "card_id": _card(db, variable, low_hz).card_id}]


def _count(db, table, where="", params=None):
    return db._duck._fetchone(f'SELECT COUNT(*) FROM "{table}" {where}', params or [])[0]


def _snapshot(db):
    tables = [
        "_record", "_record_save", "_invocation", "_invocation_input",
        "_invocation_output", "_run", "_run_invocation", "_constant",
        "VdFiltered_data", "VdSteps_data", "VdRaw_data",
    ]
    return {t: _count(db, t) for t in tables}


def _low_hz_values(db, variable):
    from scidb.provenance_query import branch_params_batch

    rids = [r[0] for r in db._duck._fetchall(
        "SELECT record_id FROM _record WHERE type = ?", [variable]
    )]
    return sorted(bp.get("vd_bandpass.low_hz") for bp in branch_params_batch(db._duck, rids).values())


# ---------------------------------------------------------------------------
# 1-3. Plan, delete, what survives
# ---------------------------------------------------------------------------


class TestPlan:
    def test_the_plan_writes_nothing(self, db):
        before = _snapshot(db)
        plan = delete_plan(db, _target(db, 10))
        assert _snapshot(db) == before
        assert plan.by_variable == {"VdFiltered": 2, "VdSteps": 2}
        assert plan.seed_by_variable == {"VdFiltered": 2}
        assert plan.downstream_by_variable == {"VdSteps": 2}
        assert plan.lost_locations == {}
        assert plan.fingerprint

    def test_lost_locations_name_where_nothing_is_left(self, gap_db):
        plan = delete_plan(gap_db, _target(gap_db, 10))
        assert [loc["subject"] for loc in plan.lost_locations["VdFiltered"]] == ["02"]
        assert [loc["subject"] for loc in plan.lost_locations["VdSteps"]] == ["02"]


class TestDelete:
    def test_deletes_exactly_the_plan(self, db):
        plan = delete_plan(db, _target(db, 10))
        result = delete_variant(db, _target(db, 10), "bad calibration",
                                expect_fingerprint=plan.fingerprint)
        assert result.plan.record_ids == plan.record_ids
        assert _count(db, "_record", "WHERE record_id IN (" + ", ".join("?" * len(plan.record_ids)) + ")",
                      plan.record_ids) == 0
        assert _low_hz_values(db, "VdFiltered") == [20, 20]
        assert _low_hz_values(db, "VdSteps") == [20, 20]

    def test_data_rows_and_saves_go(self, db):
        before = _snapshot(db)
        result = delete_variant(db, _target(db, 10), "bad calibration")
        after = _snapshot(db)
        assert after["VdFiltered_data"] == before["VdFiltered_data"] - 2
        assert after["VdSteps_data"] == before["VdSteps_data"] - 2
        assert result.deleted_rows["VdFiltered_data"] == 2

    def test_what_survives(self, db):
        before = _snapshot(db)
        delete_variant(db, _target(db, 10), "bad calibration")
        after = _snapshot(db)
        # Shared constants and the raw input are never touched.
        assert after["_constant"] == before["_constant"]
        assert after["VdRaw_data"] == before["VdRaw_data"]
        # The low_hz=10 bandpass run produced only deleted records: gone. The
        # detect run also produced the surviving low_hz=20 steps: kept.
        runs = {r[0] for r in db._duck._fetchall("SELECT function_name FROM _run")}
        assert runs == {"vd_bandpass", "vd_detect"}
        assert after["_run"] == before["_run"] - 1

    def test_the_load_afterwards_is_unambiguous(self, db):
        with pytest.raises(AmbiguousVersionError):
            VdFiltered.load(subject="01")
        delete_variant(db, _target(db, 10), "bad calibration")
        loaded = VdFiltered.load(subject="01")
        assert loaded.branch_params["vd_bandpass.low_hz"] == 20


class TestOlderSaves:
    def test_an_older_save_of_the_variant_is_deleted_too(self, db):
        """Re-run low_hz=10 over changed input at subject 01: the card now holds
        an old and a new record there. Deleting the card must take both, or the
        old one becomes the latest low_hz=10 record again."""
        VdRaw.save(np.array([5.0, 6.0]), subject="01", trial=1)
        for_each(vd_bandpass, {"signal": VdRaw, "low_hz": 10}, [VdFiltered],
                 subject=["01"], trial=[])
        card = _card(db, "VdFiltered", 10)
        assert card.record_count == 3, card.record_ids
        delete_variant(db, _target(db, 10), "bad calibration")
        assert 10 not in _low_hz_values(db, "VdFiltered")


# ---------------------------------------------------------------------------
# 5. All or nothing
# ---------------------------------------------------------------------------


class TestAtomic:
    def test_a_failure_mid_way_deletes_nothing(self, db, monkeypatch):
        before = _snapshot(db)
        real = variant_delete._delete_in

        def failing(duck, table, column, ids):
            if table == "_record":
                raise RuntimeError("simulated failure")
            return real(duck, table, column, ids)

        monkeypatch.setattr(variant_delete, "_delete_in", failing)
        with pytest.raises(RuntimeError, match="simulated"):
            delete_variant(db, _target(db, 10), "bad calibration")
        assert _snapshot(db) == before
        assert tombstones(db) == []

    def test_a_changed_plan_is_refused(self, db):
        before = _snapshot(db)
        with pytest.raises(ValueError, match="changed since the plan"):
            delete_variant(db, _target(db, 10), "x", expect_fingerprint="0000000000000000")
        assert _snapshot(db) == before

    def test_a_reason_is_required(self, db):
        with pytest.raises(ValueError, match="reason"):
            delete_variant(db, _target(db, 10), " ")

    def test_an_unknown_card_is_refused(self, db):
        with pytest.raises(ValueError, match="no card"):
            delete_plan(db, [{"variable": "VdFiltered", "card_id": "nope"}])


# ---------------------------------------------------------------------------
# 6. Tombstone and pins
# ---------------------------------------------------------------------------


class TestTombstoneAndPins:
    def test_a_tombstone_is_written(self, db):
        result = delete_variant(db, _target(db, 10), "bad calibration")
        (stone,) = db.inspect.tombstones()
        assert stone.tombstone_id == result.tombstone.tombstone_id
        assert stone.reason == "bad calibration"
        assert stone.by_variable == {"VdFiltered": 2, "VdSteps": 2}
        assert len(stone.record_ids) == 4
        assert [t.tombstone_id for t in db.inspect.tombstones("VdSteps")] == [stone.tombstone_id]
        assert db.inspect.tombstones("VdRaw") == []

    def test_a_pin_on_the_deleted_variant_is_released(self, db):
        pin_variant(db, "VdFiltered", _card(db, "VdFiltered", 10).selection, "test")
        result = delete_variant(db, _target(db, 10), "bad calibration")
        assert result.released_pins == ["VdFiltered"]
        assert active_pins(db) == {}
        (pin,) = pin_history(db, "VdFiltered")
        assert "variant deleted" in pin.release_reason

    def test_a_pin_on_the_surviving_variant_is_kept(self, db):
        pin_variant(db, "VdFiltered", _card(db, "VdFiltered", 20).selection, "test")
        result = delete_variant(db, _target(db, 10), "bad calibration")
        assert result.released_pins == []
        assert "VdFiltered" in active_pins(db)


# ---------------------------------------------------------------------------
# 7. Constant target (D9)
# ---------------------------------------------------------------------------


class TestConstantTarget:
    def test_every_variable_built_with_the_value_goes(self, db):
        for_each(vd_other, {"signal": VdRaw, "low_hz": 10}, [VdOther], subject=[], trial=[])
        for_each(vd_other, {"signal": VdRaw, "low_hz": 20}, [VdOther], subject=[], trial=[])
        targets = [
            {"function": "vd_bandpass", "param": "low_hz", "value": 10},
            {"function": "vd_other", "param": "low_hz", "value": 10},
        ]
        plan = delete_plan(db, targets)
        assert plan.by_variable == {"VdFiltered": 2, "VdOther": 2, "VdSteps": 2}
        delete_variant(db, targets, "retire low_hz=10")
        assert _count(db, "VdOther_data") == 2
        assert _low_hz_values(db, "VdFiltered") == [20, 20]

    def test_an_unused_value_is_a_warning_not_a_delete(self, db):
        plan = delete_plan(db, [{"function": "vd_bandpass", "param": "low_hz", "value": 99}])
        assert plan.record_ids == []
        assert plan.warnings

    def test_a_parameter_target_follows_parameter_node_name(self, db):
        """No ``declared_name`` was recorded here (plain constants), so the
        edges belong to the Parameter named like their argument, ``low_hz``.
        That is the canvas Parameter node's identity, and it spans every
        function the Parameter feeds."""
        for_each(vd_other, {"signal": VdRaw, "low_hz": 10}, [VdOther], subject=[], trial=[])
        plan = delete_plan(db, [{"parameter": "low_hz", "value": 10}])
        assert plan.by_variable == {"VdFiltered": 2, "VdOther": 2, "VdSteps": 2}

    def test_an_unknown_parameter_name_is_a_warning(self, db):
        plan = delete_plan(db, [{"parameter": "NoSuchParameter", "value": 10}])
        assert plan.record_ids == [] and plan.warnings


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "vd_cli.duckdb"
    _build(path).close()
    _scifor.set_schema([])
    return path


class TestCli:
    def test_dry_run_then_delete(self, db_path, capsys):
        base = ["--db", str(db_path)]
        rc = cli_main(base + ["delete-variant", "VdFiltered", "--variant",
                              "vd_bandpass.low_hz=10", "--reason", "bad", "--json"])
        out = capsys.readouterr().out
        assert rc == 0, out
        plan = json.loads(out)
        assert plan["by_variable"] == {"VdFiltered": 2, "VdSteps": 2}

        rc = cli_main(base + ["delete-variant", "VdFiltered", "--variant",
                              "vd_bandpass.low_hz=10", "--reason", "bad"])
        out = capsys.readouterr().out
        assert "Nothing was deleted" in out

        rc = cli_main(base + ["delete-variant", "VdFiltered", "--variant",
                              "vd_bandpass.low_hz=10", "--reason", "bad", "--yes",
                              "--fingerprint", plan["fingerprint"]])
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "delete_variant" in out

        rc = cli_main(base + ["tombstones", "--json"])
        (stone,) = json.loads(capsys.readouterr().out)
        assert stone["reason"] == "bad"
