"""The Variants popup's backend: a shell over scidb that adds only GUI facts.

``.claude/plan-variants-popup.md`` Stage 4. Pinned here:

* the cards the popup gets ARE ``Inspector.variant_cards``, field for field;
* pins and deletes go through scidb and come back visible on the cards;
* a delete refuses a plan that changed, and maps refusals to 400, not 500;
* "also remove the value from the Parameter" widens the delete through
  scidb's Parameter identity and then edits source through the existing path;
* a run that would write to a pinned variable is reported before it starts;
* every method is reachable on both transports, and the reads do not hold the
  database lock.
"""

import json
from types import SimpleNamespace

import pytest
from conftest import FilteredSignal, RawSignal, bandpass_filter

from scidb import for_each
from scistack_gui.services import variant_cards_service as svc


@pytest.fixture
def two_cards(populated_db):
    """``populated_db`` holds FilteredSignal at low_hz=20; add low_hz=30."""
    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 30},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )
    return populated_db


def _card(payload, low_hz):
    for card in payload["cards"]:
        if card["selection"].get("bandpass_filter.low_hz") == low_hz:
            return card
    raise AssertionError(f"no card with low_hz={low_hz}")


@pytest.fixture
def declared_low_hz(monkeypatch):
    """A canvas Parameter ``low_hz`` declaring 20 and 30, editable."""
    from scistack_gui import registry
    from scistack_gui.services import target_file_service

    monkeypatch.setattr(
        registry,
        "get_parameters_registry",
        lambda: {"low_hz": SimpleNamespace(values=[20, 30])},
    )
    monkeypatch.setattr(
        target_file_service,
        "entity_editability",
        lambda kind, name: {"editable": True, "message": ""},
    )


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------


class TestCards:
    def test_cards_are_inspector_variant_cards(self, two_cards):
        payload = svc.variable_variants(two_cards, "FilteredSignal")
        direct = two_cards.inspect.variant_cards("FilteredSignal")
        assert [c["card_id"] for c in payload["cards"]] == [c.card_id for c in direct.cards]
        assert [c["selection"] for c in payload["cards"]] == [c.selection for c in direct.cards]
        assert payload["varying_axes"] == direct.varying_axes == ["bandpass_filter.low_hz"]
        assert payload["command"] == "scidb variants FilteredSignal --cards"
        json.dumps(payload)

    def test_selection_values_keep_their_type(self, two_cards):
        card = _card(svc.variable_variants(two_cards, "FilteredSignal"), 20)
        assert card["selection"] == {"bandpass_filter.low_hz": 20}

    def test_parameter_offers_without_a_declared_parameter(self, two_cards):
        card = _card(svc.variable_variants(two_cards, "FilteredSignal"), 20)
        (offer,) = card["parameter_offers"]
        assert offer["parameter"] == "low_hz" and offer["value"] == 20
        assert offer["declared"] is False and offer["editable"] is False

    def test_parameter_offers_with_a_declared_parameter(self, two_cards, declared_low_hz):
        card = _card(svc.variable_variants(two_cards, "FilteredSignal"), 20)
        (offer,) = card["parameter_offers"]
        assert offer["declared"] is True and offer["editable"] is True

    def test_raw_variable_has_one_card_and_no_offers(self, two_cards):
        payload = svc.variable_variants(two_cards, "RawSignal")
        (card,) = payload["cards"]
        assert card["parameter_offers"] == []


def test_jsonable_converts_dataclasses_inside_lists():
    """Regression (2026-09-30): only the top level was converted, so a LIST of
    ``VariantPin`` (``pin_history``) became a list of repr strings and the
    popup indexed a string."""
    from scidb.variant_pins import VariantPin

    pin = VariantPin("p1", "X", {"f.k": 1}, "why", None, "2026-09-30T00:00:00")
    out = svc._jsonable([pin, {"nested": [pin]}])
    assert out[0]["pin_id"] == "p1" and out[0]["selection"] == {"f.k": 1}
    assert out[1]["nested"][0]["reason"] == "why"


# ---------------------------------------------------------------------------
# Pins
# ---------------------------------------------------------------------------


class TestPins:
    def test_pin_then_release_shows_on_the_cards(self, two_cards):
        sel = _card(svc.variable_variants(two_cards, "FilteredSignal"), 30)["selection"]
        svc.pin_variant(two_cards, "FilteredSignal", sel, "cleaner")
        payload = svc.variable_variants(two_cards, "FilteredSignal")
        assert _card(payload, 30)["is_pinned"] and _card(payload, 30)["is_default"]
        assert not _card(payload, 20)["is_default"]
        assert payload["pin"]["selection"] == sel

        svc.release_pin(two_cards, "FilteredSignal", "done")
        payload = svc.variable_variants(two_cards, "FilteredSignal")
        assert payload["pin"] is None
        assert [p["release_reason"] for p in payload["pin_history"]] == ["done"]

    def test_pin_newest_moves_to_the_last_saved_card(self, two_cards):
        sel20 = _card(svc.variable_variants(two_cards, "FilteredSignal"), 20)["selection"]
        svc.pin_variant(two_cards, "FilteredSignal", sel20, "first")
        out = svc.pin_newest_variant(two_cards, "FilteredSignal", "after run")
        assert out["pin"]["selection"] == {"bandpass_filter.low_hz": 30}


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_plan_then_delete(self, two_cards):
        card = _card(svc.variable_variants(two_cards, "FilteredSignal"), 20)
        plan = svc.delete_variant_plan(two_cards, "FilteredSignal", card["card_id"])
        assert plan["by_variable"] == {"FilteredSignal": 4}
        assert plan["total_records"] == 4
        assert "record_ids" not in plan

        out = svc.delete_variant(
            two_cards, "FilteredSignal", card["card_id"], "bad run", plan["fingerprint"]
        )
        assert out["extra"]["by_variable"] == {"FilteredSignal": 4}
        payload = svc.variable_variants(two_cards, "FilteredSignal")
        assert [c["selection"] for c in payload["cards"]] == [{"bandpass_filter.low_hz": 30}]
        (stone,) = payload["tombstones"]
        assert stone["reason"] == "bad run" and "record_ids" not in stone

    def test_a_changed_plan_is_refused(self, two_cards):
        card = _card(svc.variable_variants(two_cards, "FilteredSignal"), 20)
        with pytest.raises(ValueError, match="changed since the plan"):
            svc.delete_variant(two_cards, "FilteredSignal", card["card_id"], "x", "0" * 16)

    def test_removing_the_parameter_value_widens_and_edits_source(
        self, two_cards, declared_low_hz, monkeypatch
    ):
        from scistack_gui.services import layout_service

        calls = []
        monkeypatch.setattr(
            layout_service,
            "update_parameter",
            lambda name, values, *a, **k: calls.append((name, values)) or {"ok": True},
        )
        card = _card(svc.variable_variants(two_cards, "FilteredSignal"), 20)
        remove = [{"parameter": "low_hz", "value": 20}]
        plan = svc.delete_variant_plan(two_cards, "FilteredSignal", card["card_id"], remove)
        out = svc.delete_variant(
            two_cards, "FilteredSignal", card["card_id"], "retire 20 Hz",
            plan["fingerprint"], remove,
        )
        assert calls == [("low_hz", [30])]
        assert out["parameter_edits"] == [{"parameter": "low_hz", "value": 20, "ok": True}]


# ---------------------------------------------------------------------------
# Run conflicts
# ---------------------------------------------------------------------------


class TestRunConflicts:
    def test_no_pins_no_conflicts_and_no_target_derivation(self, two_cards, monkeypatch):
        from scistack_gui.services import execution_service

        monkeypatch.setattr(
            execution_service,
            "derive_target_for_node",
            lambda db, node_id: pytest.fail("must not derive targets with no pins"),
        )
        assert svc.run_pin_conflicts(two_cards, ["fn__x"]) == {"conflicts": []}

    def test_a_run_writing_to_a_pinned_variable_is_reported(self, two_cards, monkeypatch):
        from scistack_gui.services import execution_service

        sel = _card(svc.variable_variants(two_cards, "FilteredSignal"), 30)["selection"]
        svc.pin_variant(two_cards, "FilteredSignal", sel, "cleaner")
        monkeypatch.setattr(
            execution_service,
            "derive_target_for_node",
            lambda db, node_id: [{"output_type": "FilteredSignal"}, {"output_type": "Other"}],
        )
        (conflict,) = svc.run_pin_conflicts(two_cards, ["fn__bandpass"])["conflicts"]
        assert conflict["node_id"] == "fn__bandpass" and conflict["function_name"] is None
        assert conflict["variable"] == "FilteredSignal"
        assert conflict["pin"]["selection"] == sel


# ---------------------------------------------------------------------------
# Both transports
# ---------------------------------------------------------------------------

_READS = ["variable_variants", "delete_variant_plan", "run_pin_conflicts"]
_WRITES = ["pin_variant", "release_pin", "pin_newest_variant", "delete_variant"]


def test_json_rpc_methods_and_lock_policy(two_cards):
    from scistack_gui import server

    for name in _READS + _WRITES:
        assert name in server.METHODS, name
    for name in _READS:
        assert name in server.SELF_MANAGED_DB_METHODS, name
    for name in _WRITES:
        assert name not in server.SELF_MANAGED_DB_METHODS, name
    payload = server.METHODS["variable_variants"]({"variable": "FilteredSignal"})
    assert len(payload["cards"]) == 2


def test_http_cards_and_pin(client, two_cards):
    response = client.post("/api/variants/cards", json={"variable": "FilteredSignal"})
    assert response.status_code == 200, response.text
    sel = _card(response.json(), 30)["selection"]
    response = client.post(
        "/api/variants/pin",
        json={"variable": "FilteredSignal", "selection": sel, "reason": "cleaner"},
    )
    assert response.status_code == 200, response.text


def test_http_refusals_are_400(client, two_cards):
    response = client.post("/api/variants/cards", json={"variable": "NoSuchThing"})
    assert response.status_code == 400, response.text
    response = client.post(
        "/api/variants/pin",
        json={"variable": "FilteredSignal", "selection": {}, "reason": "too broad"},
    )
    assert response.status_code == 400, response.text


def test_a_pipeline_run_is_checked_by_function_name(two_cards, monkeypatch):
    """A pipeline run's plan names steps by function, not by canvas node."""
    from scistack_gui.services import execution_service

    sel = _card(svc.variable_variants(two_cards, "FilteredSignal"), 30)["selection"]
    svc.pin_variant(two_cards, "FilteredSignal", sel, "cleaner")
    monkeypatch.setattr(
        execution_service,
        "derive_fn_targets",
        lambda db, fn: [{"output_type": "FilteredSignal"}] if fn == "bandpass_filter" else [],
    )
    out = svc.run_pin_conflicts(two_cards, [], ["bandpass_filter", "other_fn"])
    (conflict,) = out["conflicts"]
    assert conflict["function_name"] == "bandpass_filter" and conflict["node_id"] is None
