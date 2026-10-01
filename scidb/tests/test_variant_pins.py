"""Variant pins: the DEFAULT variant of a variable.

``.claude/plan-variants-popup.md`` Stage 2; rules in
``docs/claude/variant-pins-and-deletion.md`` §2. What is pinned here:

1. **An unnamed load gets the default**: a for_each input and ``load()``.
2. **A named load still gets anything**: ``Variant(...)``, ``AcrossVariants``.
   A pin hides nothing.
3. **Gaps are strict**: where the pinned variant does not exist, the default
   has nothing.
4. **Downstream follows**; **a downstream pin overrides** an upstream one and
   says so on its card.
5. **The node-state predictor agrees with the loader**, so a node fed by a
   pinned input can be green.
6. Release, re-pin, refusal and history; the CLI.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
import scifor as _scifor
from scidb import AcrossVariants, BaseVariable, Variant, configure_database, for_each
from scidb.exceptions import AmbiguousVersionError, NotFoundError
from scidb.foreach import _load_input
from scidb.foreach_config import _compute_fn_hash
from scidb.inspect.cli import main as cli_main
from scidb.provenance_query import (
    branch_params_batch,
    expected_invocations_for_function,
    present_invocation_schema_pairs,
)
from scidb.variant_pins import (
    active_pins,
    effective_default,
    pin_history,
    pin_variant,
    release_pin,
)

SCHEMA = ["subject", "trial"]
SUBJECTS = ["01", "02"]


class VpnRaw(BaseVariable):
    pass


class VpnFiltered(BaseVariable):
    pass


class VpnSteps(BaseVariable):
    pass


class VpnConsumed(BaseVariable):
    pass


def vpn_bandpass(signal, low_hz):
    return np.asarray(signal) * float(low_hz)


def vpn_detect(filtered):
    return float(np.sum(filtered))


def vpn_consume(filtered):
    return float(np.sum(filtered)) + 1.0


def _configure(path):
    _scifor.set_schema([])
    return configure_database(path, SCHEMA)


def _build(path, low20_subjects=SUBJECTS):
    """``VpnRaw → vpn_bandpass(low_hz=10|20) → VpnFiltered → vpn_detect → VpnSteps``.

    ``low_hz=20`` runs only at ``low20_subjects``, so a gap can be made.
    """
    db = _configure(path)
    for subject in SUBJECTS:
        VpnRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)
    for_each(
        vpn_bandpass,
        {"signal": VpnRaw, "low_hz": 10},
        [VpnFiltered],
        subject=list(SUBJECTS),
        trial=[],
    )
    for_each(
        vpn_bandpass,
        {"signal": VpnRaw, "low_hz": 20},
        [VpnFiltered],
        subject=list(low20_subjects),
        trial=[],
    )
    for_each(vpn_detect, {"filtered": VpnFiltered}, [VpnSteps], subject=[], trial=[])
    return db


@pytest.fixture
def db(tmp_path):
    database = _build(tmp_path / "vpn.duckdb")
    yield database
    database.close()
    _scifor.set_schema([])


@pytest.fixture
def gap_db(tmp_path):
    database = _build(tmp_path / "vpn_gap.duckdb", low20_subjects=["01"])
    yield database
    database.close()
    _scifor.set_schema([])


def _card(db, variable, low_hz):
    for card in db.inspect.variant_cards(variable).cards:
        if card.selection.get("vpn_bandpass.low_hz") == low_hz:
            return card
    raise AssertionError(f"no {variable} card with low_hz={low_hz}")


def _pin(db, variable, low_hz, reason="test"):
    return pin_variant(db, variable, _card(db, variable, low_hz).selection, reason)


def _low_hz_of(db, variable):
    rids = [
        r[0]
        for r in db._duck._fetchall(
            "SELECT record_id FROM _record WHERE type = ?", [variable]
        )
    ]
    return sorted(
        bp.get("vpn_bandpass.low_hz") for bp in branch_params_batch(db._duck, rids).values()
    )


def _as_list(loaded):
    return loaded if isinstance(loaded, list) else [loaded]


# ---------------------------------------------------------------------------
# 1-2. Unnamed loads get the default; named loads get anything
# ---------------------------------------------------------------------------


class TestUnnamedLoadsGetTheDefault:
    def test_without_a_pin_both_variants_flow(self, db):
        """Control: today's behaviour, unchanged with no pin. Two variants at
        one location and no variant named is ambiguous, which is exactly what a
        pin resolves."""
        with pytest.raises(AmbiguousVersionError):
            VpnFiltered.load(subject="01")
        assert effective_default(db, "VpnFiltered") is None

    def test_load_returns_the_pinned_variant(self, db):
        _pin(db, "VpnFiltered", 20)
        (loaded,) = _as_list(VpnFiltered.load(subject="01"))
        assert loaded.branch_params["vpn_bandpass.low_hz"] == 20

    def test_for_each_input_gets_only_the_default(self, db):
        _pin(db, "VpnFiltered", 20)
        for_each(
            vpn_consume, {"filtered": VpnFiltered}, [VpnConsumed], subject=[], trial=[]
        )
        assert _low_hz_of(db, "VpnConsumed") == [20, 20]

    def test_a_named_variant_still_loads(self, db):
        _pin(db, "VpnFiltered", 20)
        for_each(
            vpn_consume,
            {"filtered": Variant(VpnFiltered, low_hz=10)},
            [VpnConsumed],
            subject=[],
            trial=[],
        )
        assert _low_hz_of(db, "VpnConsumed") == [10, 10]

    def test_a_named_load_kwarg_still_loads(self, db):
        _pin(db, "VpnFiltered", 20)
        (loaded,) = _as_list(VpnFiltered.load(subject="01", low_hz=10))
        assert loaded.branch_params["vpn_bandpass.low_hz"] == 10

    def test_across_variants_still_pools_everything(self, db):
        _pin(db, "VpnFiltered", 20)
        unnamed = _load_input(VpnFiltered, db, None)
        pooled = _load_input(AcrossVariants(VpnFiltered), db, None)
        assert len(unnamed) == len(SUBJECTS)
        assert len(pooled) == 2 * len(SUBJECTS)

    def test_version_all_is_not_narrowed(self, db):
        _pin(db, "VpnFiltered", 20)
        assert len(_as_list(VpnFiltered.load(version="all", subject="01"))) == 2


# ---------------------------------------------------------------------------
# 3. Strict gaps
# ---------------------------------------------------------------------------


class TestStrictGaps:
    def test_a_location_without_the_pinned_variant_has_no_default(self, gap_db):
        _pin(gap_db, "VpnFiltered", 20)
        (loaded,) = _as_list(VpnFiltered.load(subject="01"))
        assert loaded.branch_params["vpn_bandpass.low_hz"] == 20
        with pytest.raises(NotFoundError):
            VpnFiltered.load(subject="02")

    def test_downstream_skips_the_gap(self, gap_db):
        _pin(gap_db, "VpnFiltered", 20)
        for_each(
            vpn_consume, {"filtered": VpnFiltered}, [VpnConsumed], subject=[], trial=[]
        )
        assert _low_hz_of(gap_db, "VpnConsumed") == [20]


# ---------------------------------------------------------------------------
# 4. Downstream follows; a downstream pin overrides
# ---------------------------------------------------------------------------


class TestDownstream:
    def test_an_upstream_pin_narrows_the_downstream_default(self, db):
        _pin(db, "VpnFiltered", 20)
        (loaded,) = _as_list(VpnSteps.load(subject="01"))
        assert loaded.branch_params["vpn_bandpass.low_hz"] == 20
        selection, sources = effective_default(db, "VpnSteps")
        assert sources == ["VpnFiltered"]
        assert selection["vpn_bandpass.low_hz"] == 20

    def test_cards_mark_the_default_downstream(self, db):
        _pin(db, "VpnFiltered", 20)
        result = db.inspect.variant_cards("VpnSteps")
        flags = {c.selection["vpn_bandpass.low_hz"]: c.is_default for c in result.cards}
        assert flags == {10: False, 20: True}
        assert result.default_sources == ["VpnFiltered"]
        assert not any(c.is_pinned for c in result.cards)

    def test_a_downstream_pin_wins_and_says_so(self, db):
        _pin(db, "VpnFiltered", 20)
        _pin(db, "VpnSteps", 10)
        (loaded,) = _as_list(VpnSteps.load(subject="01"))
        assert loaded.branch_params["vpn_bandpass.low_hz"] == 10
        pinned = _card(db, "VpnSteps", 10)
        assert pinned.is_pinned and pinned.is_default
        assert pinned.pin_conflict and "VpnFiltered" in pinned.pin_conflict
        # The upstream variable's own default is untouched by the downstream pin.
        (filtered,) = _as_list(VpnFiltered.load(subject="01"))
        assert filtered.branch_params["vpn_bandpass.low_hz"] == 20

    def test_an_unrelated_variable_is_not_narrowed(self, db):
        _pin(db, "VpnFiltered", 20)
        assert effective_default(db, "VpnRaw") is None


# ---------------------------------------------------------------------------
# 5. The predictor agrees with the loader
# ---------------------------------------------------------------------------


def _present_for(db, fn_name):
    inv_ids = [
        r[0]
        for r in db._duck._fetchall(
            "SELECT invocation_id FROM _invocation WHERE function_name = ?", [fn_name]
        )
    ]
    return present_invocation_schema_pairs(db._duck, inv_ids)


class TestNodeState:
    def test_a_node_fed_by_a_pinned_input_is_complete(self, db):
        _pin(db, "VpnFiltered", 20)
        for_each(
            vpn_consume, {"filtered": VpnFiltered}, [VpnConsumed], subject=[], trial=[]
        )
        expected = expected_invocations_for_function(
            db,
            "vpn_consume",
            _compute_fn_hash(vpn_consume),
            inputs_fallback={"filtered": VpnFiltered},
        )
        missing = expected - _present_for(db, "vpn_consume")
        assert expected, "the prediction must not be empty"
        assert not missing, f"{len(missing)} invocation(s) expected over the non-default variant"

    def test_without_the_pin_the_same_node_owes_the_other_variant(self, db):
        """Control: release the pin and the node really is incomplete."""
        _pin(db, "VpnFiltered", 20)
        for_each(
            vpn_consume, {"filtered": VpnFiltered}, [VpnConsumed], subject=[], trial=[]
        )
        release_pin(db, "VpnFiltered", "control")
        expected = expected_invocations_for_function(
            db,
            "vpn_consume",
            _compute_fn_hash(vpn_consume),
            inputs_fallback={"filtered": VpnFiltered},
        )
        assert expected - _present_for(db, "vpn_consume")


# ---------------------------------------------------------------------------
# 6. Release, re-pin, refusals, history
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_release_restores_todays_behaviour(self, db):
        _pin(db, "VpnFiltered", 20)
        released = release_pin(db, "VpnFiltered", "done")
        assert released.released_at and released.release_reason == "done"
        assert active_pins(db) == {}
        with pytest.raises(AmbiguousVersionError):
            VpnFiltered.load(subject="01")

    def test_pin_newest_moves_to_the_latest_saved_card(self, db):
        _pin(db, "VpnFiltered", 10)
        from scidb.variant_pins import pin_newest

        pin = pin_newest(db, "VpnFiltered", "moved after run")
        assert pin.selection == {"vpn_bandpass.low_hz": 20}  # low_hz=20 ran second
        assert active_pins(db)["VpnFiltered"].pin_id == pin.pin_id

    def test_release_without_a_pin_is_a_no_op(self, db):
        assert release_pin(db, "VpnFiltered", "nothing") is None

    def test_re_pinning_replaces_and_keeps_history(self, db):
        first = _pin(db, "VpnFiltered", 10)
        second = _pin(db, "VpnFiltered", 20)
        assert active_pins(db)["VpnFiltered"].pin_id == second.pin_id
        history = pin_history(db, "VpnFiltered")
        assert [p.pin_id for p in history] == [first.pin_id, second.pin_id]
        assert history[0].released_at is not None
        assert "replaced" in history[0].release_reason

    def test_a_selection_matching_nothing_is_refused(self, db):
        with pytest.raises(ValueError, match="matches no records"):
            pin_variant(db, "VpnFiltered", {"vpn_bandpass.low_hz": 99}, "typo")

    def test_a_selection_spanning_two_cards_is_refused(self, db):
        with pytest.raises(ValueError, match="2 variant cards"):
            pin_variant(db, "VpnFiltered", {}, "too broad")

    def test_a_reason_is_required(self, db):
        with pytest.raises(ValueError, match="reason"):
            _pin(db, "VpnFiltered", 20, reason="  ")

    def test_display_spelling_is_canonicalized(self, two_code_db):
        pin = pin_variant(two_code_db, "VpnSteps", {"Code:vpn_scale": "v1"}, "old body")
        assert pin.selection == {"__code__.vpn_scale": "v1"}


@pytest.fixture
def two_code_db(tmp_path):
    db = _configure(tmp_path / "vpn_code.duckdb")
    for subject in SUBJECTS:
        VpnRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)

    def scale_v1(raw):
        return float(np.sum(raw))

    def scale_v2(raw):
        return float(np.sum(raw)) + 100.0

    for body in (scale_v1, scale_v2):
        body.__name__ = "vpn_scale"
        for_each(body, inputs={"raw": VpnRaw}, outputs=[VpnSteps], subject=[], trial=[])
    yield db
    db.close()
    _scifor.set_schema([])


class TestCodePin:
    def test_pinning_the_older_code_version_loads_it(self, two_code_db):
        """A code pin loads uncollapsed (``pin_loads_uncollapsed``); without
        that, the latest-collapse would have dropped v1 before the filter."""
        pin_variant(two_code_db, "VpnSteps", {"__code__.vpn_scale": "v1"}, "old body")
        (loaded,) = _as_list(VpnSteps.load(subject="01"))
        assert float(loaded.data) == 3.0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "vpn_cli.duckdb"
    _build(path).close()
    _scifor.set_schema([])
    return path


class TestCli:
    def test_pin_pins_unpin_round_trip(self, db_path, capsys):
        rc = cli_main(
            [
                "--db",
                str(db_path),
                "pin",
                "VpnFiltered",
                "--variant",
                "vpn_bandpass.low_hz=20",
                "--reason",
                "cleaner",
                "--json",
            ]
        )
        out = capsys.readouterr().out
        assert rc == 0, out
        assert json.loads(out)["operation"] == "pin_variant"

        rc = cli_main(["--db", str(db_path), "pins", "--json"])
        out = capsys.readouterr().out
        assert rc == 0, out
        (pin,) = json.loads(out)
        assert pin["variable"] == "VpnFiltered"
        assert pin["selection"] == {"vpn_bandpass.low_hz": 20}

        rc = cli_main(["--db", str(db_path), "variants", "VpnSteps", "--cards"])
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "default (pin(s) on VpnFiltered)" in out
        assert "DEFAULT vpn_bandpass.low_hz=20" in out

        rc = cli_main(
            ["--db", str(db_path), "unpin", "VpnFiltered", "--reason", "done"]
        )
        assert rc == 0, capsys.readouterr().out
        rc = cli_main(["--db", str(db_path), "pins"])
        assert "(no active pins)" in capsys.readouterr().out
        rc = cli_main(["--db", str(db_path), "pins", "--history", "--json"])
        (released,) = json.loads(capsys.readouterr().out)
        assert released["released_at"] and released["release_reason"] == "done"
