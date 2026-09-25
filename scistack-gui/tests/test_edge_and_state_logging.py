"""The log lines that let scidb.log answer "why does the canvas look like this".

Regression suite for the 2026-09-25 scidb.log triage (Stroke-R01-Aim1), where
three canvas questions had no answer in the log:

1. A ``pandas.read_csv`` node drew two outputs. Its call site had recorded
   GaitSpeedTable AND SymmetryTable from the same CSV, and nothing said so.
2. An edge "appeared on its own". build_edges only COUNTED DB-derived, hidden
   and superseded edges; the ids were at DEBUG.
3. A manual node's colour was decided at DEBUG, and a successful first run of
   a new wiring was preceded by a WARN saying "This node cannot run".
"""

import json
import logging

import numpy as np
from scidb import BaseVariable

from scistack_gui.domain.graph_builder import build_edges

from tests.test_graph_builder import _cid, _variant, aggregate_variants

GB_LOGGER = "scistack_gui.domain.graph_builder"


def _messages(caplog, needle):
    return [r for r in caplog.records if needle in r.getMessage()]


# ---------------------------------------------------------------------------
# build_edges names its edges
# ---------------------------------------------------------------------------


class TestBuildEdgesDetail:
    F_KEY = ("f", _cid("f-call"))
    F_NODE = f"fn__f__{_cid('f-call')}"

    def test_db_derived_hidden_and_superseded_edges_are_named(self, caplog):
        hidden_id = f"e__f__{self.F_KEY[1]}__Hidden"
        manual_twin = {
            "id": "manual__twin",
            "source": "var__Raw",
            "target": self.F_NODE,
            "targetHandle": "in__signal",
        }
        with caplog.at_level(logging.INFO, logger=GB_LOGGER):
            build_edges(
                fn_input_params={self.F_KEY: {"signal": "Raw"}},
                fn_outputs={self.F_KEY: {"Hidden"}},
                const_fns={},
                path_inputs={},
                manual_edges=[manual_twin],
                hidden_ids=set(),
                hidden_edge_ids={hidden_id},
            )
        [detail] = _messages(caplog, "build_edges detail")
        text = detail.getMessage()
        assert detail.levelno == logging.INFO
        assert f"var__Raw->{self.F_NODE}" in text
        assert hidden_id in text
        assert "manual__twin" in text

    def test_long_db_edge_lists_are_capped(self, caplog):
        outputs = {f"Out{i}" for i in range(25)}
        with caplog.at_level(logging.INFO, logger=GB_LOGGER):
            build_edges(
                fn_input_params={},
                fn_outputs={self.F_KEY: outputs},
                const_fns={},
                path_inputs={},
                manual_edges=[],
                hidden_ids=set(),
            )
        [detail] = _messages(caplog, "build_edges detail")
        assert "... +5 more" in detail.getMessage()

    def test_an_empty_graph_logs_no_detail(self, caplog):
        with caplog.at_level(logging.INFO, logger=GB_LOGGER):
            build_edges({}, {}, {}, {}, [], set())
        assert not _messages(caplog, "build_edges detail")


# ---------------------------------------------------------------------------
# One call site, several output types
# ---------------------------------------------------------------------------


class TestMultiOutputCallSite:
    def test_same_inputs_saved_into_two_variables_is_named(self, caplog):
        """The Stroke-R01-Aim1 shape: one CSV read into two variables."""
        pi_json = json.dumps(
            {"__type": "PathInput", "template": "/data/matched.csv", "root_folder": None}
        )
        cid = _cid("read_csv-matched")
        variants = [
            _variant("read_csv", "GaitSpeedTable", inputs={"path": pi_json}, call_id=cid),
            _variant("read_csv", "SymmetryTable", inputs={"path": pi_json}, call_id=cid),
        ]
        with caplog.at_level(logging.INFO, logger=GB_LOGGER):
            agg = aggregate_variants(variants)
        assert agg.fn_outputs[("read_csv", cid)] == {"GaitSpeedTable", "SymmetryTable"}
        [line] = _messages(caplog, "from ONE set of inputs")
        text = line.getMessage()
        assert f"read_csv/{cid}" in text
        assert "['GaitSpeedTable', 'SymmetryTable']" in text

    def test_a_single_output_call_site_is_not_logged(self, caplog):
        variants = [_variant("f", "Out", inputs={"x": "Raw"})]
        with caplog.at_level(logging.INFO, logger=GB_LOGGER):
            aggregate_variants(variants)
        assert not _messages(caplog, "from ONE set of inputs")


# ---------------------------------------------------------------------------
# Manual function nodes: colour and first-run wording
# ---------------------------------------------------------------------------


def _wire_new_bandpass_node(client, suffix):
    """A manual bandpass_filter node on a wiring history has never seen.

    populated_db already ran bandpass_filter(RawSignal) -> FilteredSignal, so
    the function HAS history and this node's wiring matches none of it.
    """
    in_cls = type(f"LogIn{suffix}", (BaseVariable,), {})
    type(f"LogOut{suffix}", (BaseVariable,), {})
    in_cls.save(np.zeros(5), subject=1, session="pre")

    fn_node = f"mf_log_{suffix}"
    client.put(f"/api/layout/mv_in_{suffix}", json={
        "x": 0, "y": 0, "node_type": "variableNode", "label": f"LogIn{suffix}",
    })
    client.put(f"/api/layout/{fn_node}", json={
        "x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter",
    })
    client.put(f"/api/layout/mv_out_{suffix}", json={
        "x": 20, "y": 0, "node_type": "variableNode", "label": f"LogOut{suffix}",
    })
    client.put(f"/api/edges/e_in_{suffix}", json={
        "source": f"mv_in_{suffix}", "target": fn_node, "target_handle": "in__signal",
    })
    client.put(f"/api/edges/e_out_{suffix}", json={
        "source": fn_node, "target": f"mv_out_{suffix}",
    })
    return fn_node


class TestManualNodeLogging:
    def test_first_run_of_a_new_wiring_is_info_not_warning(self, client, caplog):
        from scistack_gui.db import get_db
        from scistack_gui.services.execution_service import derive_target_for_node

        fn_node = _wire_new_bandpass_node(client, "A")
        with caplog.at_level(logging.INFO, logger="scistack_gui.services.execution_service"):
            targets = derive_target_for_node(get_db(), fn_node)

        assert len(targets) == 1, "the new wiring runs from its own edges"
        [line] = _messages(caplog, "matches none of the")
        assert line.levelno == logging.INFO
        assert "First run of this wiring" in line.getMessage()
        assert "cannot run" not in line.getMessage()

    def test_manual_fn_node_state_is_logged_at_info(self, client, caplog):
        fn_node = _wire_new_bandpass_node(client, "B")
        with caplog.at_level(logging.INFO, logger="scistack_gui.api.pipeline"):
            resp = client.get("/api/pipeline")
        assert resp.status_code == 200

        [line] = _messages(caplog, f"manual fn node {fn_node} ")
        text = line.getMessage()
        assert line.levelno == logging.INFO
        assert "state=" in text
        assert "LogOutB" in text
