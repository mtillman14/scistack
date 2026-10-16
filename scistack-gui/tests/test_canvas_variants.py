"""The Variable node shows the CURRENT variant, the one the Variants popup marks.

Before 2026-10-16 the node printed every record of the type: all variants
pooled, plus older saves newer ones had replaced. Now it carries the popup's
current card(s) (scidb ``_mark_defaults``' ``is_default``), with their
"V{n}". Pinned here:

* the node's current cards ARE the popup's ``is_default`` cards, same ids and
  V-numbers, never a second rule;
* a pin moves the node to the pinned variant, and a release moves it back;
* a sweep with no pin reports every live variant as current;
* ``total_records`` keeps the pooled count (the tooltip's "in all").
"""

import pytest
from conftest import FilteredSignal, RawSignal, bandpass_filter

from scidb import for_each
from scistack_gui.domain.graph_builder import build_variable_nodes
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


def _node(client, variable="FilteredSignal"):
    nodes = client.get("/api/pipeline").json()["nodes"]
    (node,) = [n for n in nodes if n["id"] == f"var__{variable}"]
    return node["data"]


def _popup_current(db, variable="FilteredSignal"):
    payload = svc.variable_variants(db, variable)
    return [(c["card_id"], c["ordinal"]) for c in payload["cards"] if c["is_default"]]


def test_the_node_carries_the_popups_current_cards(client, two_cards):
    data = _node(client)
    variant = data["variant"]
    assert variant["variant_count"] == 2
    assert [(c["card_id"], c["ordinal"]) for c in variant["current"]] == _popup_current(two_cards)
    # No pin: both sweep values are live, so both are current (newest first).
    assert [c["ordinal"] for c in variant["current"]] == [2, 1]
    assert variant["has_default"] is False


def test_the_pooled_count_is_kept_for_the_tooltip(client, two_cards):
    data = _node(client)
    per_card = sum(c["record_count"] for c in data["variant"]["current"])
    assert data["total_records"] == data["variant"]["pooled_record_count"] == per_card == 8


def test_a_pin_moves_the_node_to_the_pinned_variant(client, two_cards):
    payload = svc.variable_variants(two_cards, "FilteredSignal")
    older = next(c for c in payload["cards"] if c["selection"]["bandpass_filter.low_hz"] == 20)
    svc.pin_variant(two_cards, "FilteredSignal", older["selection"], "test")

    variant = _node(client)["variant"]
    (current,) = variant["current"]
    assert current["card_id"] == older["card_id"]
    assert current["ordinal"] == 1
    assert current["is_pinned"] is True
    assert variant["has_default"] is True
    assert current["record_count"] == 4
    assert [(current["card_id"], current["ordinal"])] == _popup_current(two_cards)

    svc.release_pin(two_cards, "FilteredSignal", "test")
    assert len(_node(client)["variant"]["current"]) == 2


def test_a_single_variant_is_current_alone(client, populated_db):
    variant = _node(client)["variant"]
    assert variant["variant_count"] == 1
    (current,) = variant["current"]
    assert current["ordinal"] == 1 and current["record_count"] == 4


def test_build_variable_nodes_passes_the_payload_through():
    payload = {"variant_count": 1, "current": [], "has_default": False}
    (node,) = build_variable_nodes({"A"}, {"A": 3}, {}, {"A": payload})
    assert node["data"]["variant"] == payload
    assert node["data"]["total_records"] == 3
    (bare,) = build_variable_nodes({"B"}, {}, {})
    assert "variant" not in bare["data"]
