"""The ``variable_provenance`` RPC is a thin shell over ``Inspector.provenance``.

Stage 4 of ``.claude/plan-variant-provenance-introspection.md``. The one
property worth pinning is that the GUI panel and ``scidb trace --variant …
--runs --json`` see the SAME shapes from the SAME object: no provenance logic
lives in this layer (CLAUDE.md NOTE 3), so the service may only add fields, and
never a different answer.
"""

import dataclasses
import json

import numpy as np
from conftest import FilteredSignal, RawSignal, bandpass_filter

from scidb import for_each
from scistack_gui.services import provenance_service


def _selection_for(db) -> dict:
    """The picker's own spelling of "the low_hz=20 variant"."""
    return {"bandpass_filter.low_hz": 20}


def test_rpc_returns_what_inspector_provenance_returns(populated_db):
    db = populated_db
    schema = {"subject": 1, "session": "pre"}

    via_rpc = provenance_service.variable_provenance(
        db, "FilteredSignal", selection=_selection_for(db), schema=schema
    )
    direct = dataclasses.asdict(
        db.inspect.provenance("FilteredSignal", _selection_for(db), **schema)
    )

    # Every field the Inspector produced arrives unchanged; the service adds
    # `variable` and the flattened `runs` and nothing else.
    for key, value in direct.items():
        assert via_rpc[key] == value, key
    assert set(via_rpc) - set(direct) == {"variable", "runs"}
    assert via_rpc["variable"] == "FilteredSignal"


def test_nodes_reach_the_run(populated_db):
    payload = provenance_service.variable_provenance(
        populated_db,
        "FilteredSignal",
        selection=_selection_for(populated_db),
        schema={"subject": 1, "session": "pre"},
    )
    filtered = next(n for n in payload["nodes"] if n["variable"] == "FilteredSignal")

    assert filtered["invocation_id"]
    assert filtered["call_id"]
    assert filtered["run_options"] == "distribute=false"
    assert filtered["runs"] and filtered["runs"][0]["run_id"]
    # The flat list carries the producing function's name for the panel.
    assert payload["runs"][0]["function_name"] == "bandpass_filter"


def test_flat_runs_are_newest_first_and_deduplicated(populated_db):
    payload = provenance_service.variable_provenance(
        populated_db,
        "FilteredSignal",
        selection=_selection_for(populated_db),
        schema={"subject": 1, "session": "pre"},
    )
    stamps = [r["timestamp"] for r in payload["runs"]]
    keys = [(r["run_id"], r["invocation_id"]) for r in payload["runs"]]

    assert stamps == sorted(stamps, reverse=True)
    assert len(keys) == len(set(keys))


def test_display_spelling_is_accepted(populated_db):
    """A second code version makes `Code:bandpass_filter` a real axis; the
    panel sends that column name and scidb canonicalizes it."""

    def bandpass_filter_v2(signal, low_hz):  # noqa: D401 - a second body
        return np.asarray(signal, dtype=float) * float(low_hz) + 1.0

    bandpass_filter_v2.__name__ = bandpass_filter.__name__
    for_each(
        bandpass_filter_v2,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )

    payload = provenance_service.variable_provenance(
        populated_db,
        "FilteredSignal",
        selection={"Code:bandpass_filter": "v1"},
        schema={"subject": 1, "session": "pre"},
    )

    assert payload["selection"] == {"__code__.bandpass_filter": "v1"}
    filtered = next(n for n in payload["nodes"] if n["variable"] == "FilteredSignal")
    assert filtered["code_version"] == "v1"


def test_a_pin_that_matches_nothing_is_an_answer_not_a_500(populated_db):
    payload = provenance_service.variable_provenance(
        populated_db,
        "FilteredSignal",
        selection={"bandpass_filter.low_hz": 999},
    )

    assert payload["error"]
    assert payload["nodes"] == []
    assert payload["root_record_id"] is None


def test_without_schema_keys_the_pin_spans_locations(populated_db):
    payload = provenance_service.variable_provenance(
        populated_db, "FilteredSignal", selection=_selection_for(populated_db)
    )

    assert len(payload["matched_record_ids"]) == 4  # 2 subjects × 2 sessions
    assert payload["root_record_id"] in payload["matched_record_ids"]


def test_payload_is_json_serializable(populated_db):
    payload = provenance_service.variable_provenance(
        populated_db,
        "FilteredSignal",
        selection=_selection_for(populated_db),
        schema={"subject": 1, "session": "pre"},
    )

    json.dumps(payload)


def test_http_route_matches_the_service(client, populated_db):
    body = {
        "variable": "FilteredSignal",
        "selection": _selection_for(populated_db),
        "schema_keys": {"subject": 1, "session": "pre"},
    }
    response = client.post("/api/provenance/variable", json=body)

    assert response.status_code == 200, response.text
    direct = provenance_service.variable_provenance(
        populated_db,
        "FilteredSignal",
        selection=body["selection"],
        schema=body["schema_keys"],
    )
    assert response.json()["root_record_id"] == direct["root_record_id"]
    assert [n["record_id"] for n in response.json()["nodes"]] == [
        n["record_id"] for n in direct["nodes"]
    ]


def test_json_rpc_handler_is_the_same_function(populated_db):
    from scistack_gui import server

    assert "variable_provenance" in server.METHODS
    assert "variable_provenance" in server.SELF_MANAGED_DB_METHODS
    payload = server.METHODS["variable_provenance"](
        {
            "variable": "FilteredSignal",
            "selection": _selection_for(populated_db),
            "schema_keys": {"subject": 1, "session": "pre"},
        }
    )

    assert payload["root_record_id"]
