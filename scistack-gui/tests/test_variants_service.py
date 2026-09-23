"""The ``variable_topologies`` RPC is a thin shell over ``Inspector.topologies``.

Stage 1 of ``.claude/plan-topologies-panel.md``. The property worth pinning is
the one the plan is built on: the panel and ``scidb variants <name>`` read the
SAME object, so no rule about variants may live in this layer (CLAUDE.md
NOTE 3). The service may reshape and it may ADD (``verdict``, ``locations``),
and it may never answer differently.

The second property is the ``load:`` verdict itself. It is the only place in
the GUI where "this variant is not what a run will read" is visible, and two
variants that look equally alive is the shape of the 2026-09-22 bug — so the
run-option case has its own test rather than riding on the happy path.
"""

import json

import numpy as np
import pytest
from conftest import FilteredSignal, RawSignal, bandpass_filter

from scidb import BaseVariable, for_each
from scidb.exceptions import NotFoundError
from scistack_gui.services import variants_service


def _flat(payload: dict) -> list[dict]:
    return [v for t in payload["topologies"] for v in t["variants"]]


# ---------------------------------------------------------------------------
# Parity with the object the CLI renders
# ---------------------------------------------------------------------------


def test_grouping_is_inspector_topologies_grouping(populated_db):
    payload = variants_service.variable_topologies(populated_db, "FilteredSignal")
    direct = populated_db.inspect.topologies("FilteredSignal")

    assert payload["topology_count"] == len(direct)
    assert payload["variant_count"] == sum(len(group) for _k, group in direct)
    for shipped, ((fn_name, input_types, output_type), group) in zip(
        payload["topologies"], direct, strict=True
    ):
        assert shipped["function_name"] == fn_name
        assert shipped["output_type"] == output_type
        # A LIST of pairs, not a dict: this is the topology key and its order
        # is the heading that names the node.
        assert shipped["input_types"] == [list(pair) for pair in input_types]
        assert len(shipped["variants"]) == len(group)
        for row, summary in zip(shipped["variants"], group, strict=True):
            assert row["call_id"] == summary.call_id
            assert row["record_count"] == summary.record_count
            assert row["constants"] == summary.constants
            assert row["current"] == summary.current


def test_every_field_of_the_summary_survives_except_the_one_replaced(populated_db):
    import dataclasses

    payload = variants_service.variable_topologies(populated_db, "FilteredSignal")
    row = _flat(payload)[0]
    summary = populated_db.inspect.topologies("FilteredSignal")[0][1][0]
    fields = {f.name for f in dataclasses.fields(summary)}

    # `schema_ids` is replaced by its view (`locations`) — a loader has
    # hundreds of them and the panel never shows the raw ids.
    assert fields - set(row) == {"schema_ids"}
    assert set(row) - fields == {"verdict", "verdict_label", "locations"}


def test_the_command_on_screen_is_the_command_that_reproduces_it(populated_db):
    payload = variants_service.variable_topologies(populated_db, "FilteredSignal")
    assert payload["command"] == "scidb variants FilteredSignal"


def test_payload_is_json_serializable(populated_db):
    payload = variants_service.variable_topologies(populated_db, "FilteredSignal")
    json.dumps(payload)


# ---------------------------------------------------------------------------
# The `load:` verdict — the reason the view exists
# ---------------------------------------------------------------------------


def test_a_single_run_is_current(populated_db):
    rows = _flat(variants_service.variable_topologies(populated_db, "FilteredSignal"))
    assert [r["verdict"] for r in rows] == ["current"]
    assert rows[0]["verdict_label"] == "load: CURRENT"


def test_a_run_option_flip_leaves_the_older_variant_superseded(populated_db):
    """The 2026-09-22 shape: two variants, one of which a load will not read.

    ``distribute`` is folded into the invocation id, so the second run is a
    genuinely different variant rather than a re-run — and run options are
    judged per function, globally, so the first one stops being returned.

    The sequence is the one ``scidb/tests/test_run_option_variants.py::both_runs``
    pins: run at the DEEPEST level without distribute, then re-run **one level
    up** with it, so each session gets its own slice.

    The function takes **no variable inputs**, exactly as `make_rows` there
    does, and that is not incidental. A function fed by a variable cannot be
    re-run one level up without something to aggregate its input with, so the
    coarser call loads nothing and writes nothing — which is what an earlier
    version of this test did, and it came back with one variant.
    """
    import pandas as pd

    class Rows(BaseVariable):
        pass

    def make_rows():
        # One row per session, so the distributed run has something to slice.
        return pd.DataFrame({"v": [10.0, 20.0]})

    for_each(make_rows, inputs={}, outputs=[Rows], subject=[1], session=["pre", "post"])
    for_each(make_rows, inputs={}, outputs=[Rows], distribute=True, subject=[1])

    payload = variants_service.variable_topologies(populated_db, "Rows")
    rows = _flat(payload)

    assert len(rows) == 2, [r["run_options"] for r in rows]
    # Chronological order: the distribute=false run came first.
    older, newer = rows
    assert "distribute=false" in (older["run_options"] or "")
    assert "distribute=true" in (newer["run_options"] or "")
    assert newer["verdict"] == "current"
    assert older["verdict"] in ("superseded", "partially_superseded"), (
        "the older run-option set still reads as live — two variants that look "
        "equally alive is the shape of the bug this view exists for"
    )
    # Every verdict is from the closed vocabulary — the panel styles off these
    # rather than parsing the label.
    assert {r["verdict"] for r in rows} <= {
        "current",
        "superseded",
        "partially_superseded",
    }


def test_the_verdict_is_scidbs_verdict_not_a_second_opinion(populated_db):
    from scidb.inspect.api import variant_verdict

    payload = variants_service.variable_topologies(populated_db, "FilteredSignal")
    for shipped, (_key, group) in zip(
        payload["topologies"], populated_db.inspect.topologies("FilteredSignal"),
        strict=True,
    ):
        for row, summary in zip(shipped["variants"], group, strict=True):
            verdict, label = variant_verdict(summary)
            assert (row["verdict"], row["verdict_label"]) == (verdict, label)


# ---------------------------------------------------------------------------
# Locations
# ---------------------------------------------------------------------------


def test_locations_are_sampled_by_default_and_counted_in_full(populated_db):
    payload = variants_service.variable_topologies(
        populated_db, "FilteredSignal", max_locations=2
    )
    locations = _flat(payload)[0]["locations"]

    assert locations["total"] == 4  # 2 subjects × 2 sessions
    assert len(locations["sample"]) == 2
    assert locations["keys"] == ["subject", "session"]
    assert set(locations["sample"][0]) == {"subject", "session"}


def test_max_locations_none_returns_every_one(populated_db):
    payload = variants_service.variable_topologies(
        populated_db, "FilteredSignal", max_locations=None
    )
    locations = _flat(payload)[0]["locations"]

    assert locations["total"] == 4
    assert len(locations["sample"]) == 4


# ---------------------------------------------------------------------------
# Names that are not variables, and variables with nothing to show
# ---------------------------------------------------------------------------


def test_an_unknown_name_raises_not_found(populated_db):
    with pytest.raises(NotFoundError):
        variants_service.variable_topologies(populated_db, "NoSuchThing")


def test_a_raw_saved_variable_has_no_topologies_and_that_is_not_an_error(
    populated_db,
):
    payload = variants_service.variable_topologies(populated_db, "RawSignal")

    assert payload["topologies"] == []
    assert payload["topology_count"] == 0
    assert payload["variant_count"] == 0


# ---------------------------------------------------------------------------
# Both transports
# ---------------------------------------------------------------------------


def test_http_route_matches_the_service(client, populated_db):
    response = client.post(
        "/api/provenance/variable-topologies", json={"variable": "FilteredSignal"}
    )

    assert response.status_code == 200, response.text
    direct = variants_service.variable_topologies(populated_db, "FilteredSignal")
    assert response.json()["topology_count"] == direct["topology_count"]
    assert response.json()["variant_count"] == direct["variant_count"]


def test_http_unknown_name_is_a_400_not_a_500(client):
    response = client.post(
        "/api/provenance/variable-topologies", json={"variable": "NoSuchThing"}
    )
    assert response.status_code == 400, response.text


def test_http_all_locations_overrides_the_default_sample(client):
    response = client.post(
        "/api/provenance/variable-topologies",
        json={"variable": "FilteredSignal", "all_locations": True},
    )
    assert response.status_code == 200, response.text
    locations = _flat(response.json())[0]["locations"]
    assert len(locations["sample"]) == locations["total"] == 4


def test_json_rpc_handler_is_the_same_function(populated_db):
    from scistack_gui import server

    assert "variable_topologies" in server.METHODS
    # The connection is taken inside the service for as long as the queries
    # need it: the panel is opened while a user is reading, and must never
    # hold the file against MATLAB for the whole round trip.
    assert "variable_topologies" in server.SELF_MANAGED_DB_METHODS

    payload = server.METHODS["variable_topologies"]({"variable": "FilteredSignal"})
    assert payload["variable"] == "FilteredSignal"
    assert payload["topology_count"] == 1


def test_a_second_topology_reads_as_a_second_shape_not_a_second_variant(
    populated_db,
):
    """Two DIFFERENT functions producing one variable are two topologies.

    This is the distinction the flat table cannot make, and the reason the
    view is two levels: a second topology is a node the user did not know
    existed, while a second variant is a run they forgot about.
    """

    def scale_signal(signal, low_hz):
        return np.asarray(signal, dtype=float) / float(low_hz)

    for_each(
        scale_signal,
        inputs={"signal": RawSignal, "low_hz": 5},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )

    payload = variants_service.variable_topologies(populated_db, "FilteredSignal")

    assert payload["topology_count"] == 2
    assert {t["function_name"] for t in payload["topologies"]} == {
        "bandpass_filter",
        "scale_signal",
    }
    # Oldest topology first — the order things happened, which is how someone
    # opening this because a variable grew reads it.
    assert payload["topologies"][0]["function_name"] == "bandpass_filter"
