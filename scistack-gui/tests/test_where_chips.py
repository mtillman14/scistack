"""The where= chips a function node shows, and removing one (issue #13).

The chip text is scidb's ``filter_label``; the frontend draws it and never
words a filter. ``set_where_filters`` is the one write used by a chip's Remove
and the check's Apply; it keeps the node's other settings.
"""

from __future__ import annotations

from scistack_gui.domain.graph_builder import where_chips
from scistack_gui.stores import pipeline_store

NODE = "fn__bandpass_filter__zz9whr"


def test_a_chip_is_the_filters_own_text_with_its_row_index():
    chips = where_chips(
        [
            {"variable": "Age", "op": ">", "value": "80"},
            {"variable": "", "op": "==", "value": ""},  # a blank row: no chip
            {"variable": "Gait", "column": "Side", "op": "==", "value": "L"},
        ]
    )
    assert [(c["index"], c["text"], c["variable"], c["column"]) for c in chips] == [
        (0, "Age > 80", "Age", None),
        (2, "Gait['Side'] == 'L'", "Gait", "Side"),
    ]


def test_a_half_filled_row_is_a_chip_saying_so_not_a_missing_one():
    chips = where_chips([{"variable": "Age", "op": ">", "value": ""}])
    assert len(chips) == 1 and "incomplete" in chips[0]["text"]


def test_no_filters_means_no_chips():
    assert where_chips(None) == []
    assert where_chips([]) == []


def test_set_where_filters_replaces_the_filters_and_keeps_other_settings(client, populated_db):
    assert client.put(
        f"/api/layout/{NODE}",
        json={"x": 0, "y": 0, "node_type": "functionNode", "label": "bandpass_filter"},
    ).status_code == 200
    assert client.put(
        f"/api/layout/{NODE}/config",
        json={"config": {"runOptions": {"on_error": "stop"}, "whereFilters": [
            {"variable": "A", "op": ">", "value": "1"},
            {"variable": "B", "op": "<", "value": "2"},
        ]}},
    ).status_code == 200

    rows = [{"variable": "A", "op": ">", "value": "1"}]
    r = client.put(f"/api/layout/{NODE}/where", json={"node_id": NODE, "filters": rows})
    assert r.status_code == 200, r.text

    config = pipeline_store.get_node_config(populated_db, NODE)
    assert config["whereFilters"] == rows
    assert config["runOptions"]["on_error"] == "stop"
