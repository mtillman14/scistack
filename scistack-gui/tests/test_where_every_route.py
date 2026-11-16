"""A node's where= filters reach every way the node can run (issue #13).

Before 2026-11-16 only the per-node Python run read ``whereFilters``, and it
read them from the request. A pipeline run, the generated MATLAB command and
both code exports ran unfiltered while the panel showed a filter. Now every
route reads the STORED config through ``where_service``, and the tests below
hold one node config to one answer on each route.
"""

from __future__ import annotations

import logging

import pytest
from scidb import BaseVariable
from scistack_gui.runtime.db import get_db
from scistack_gui.services import run_service, where_service
from scistack_gui.services.code_export_service import export_pipeline_to_code
from scistack_gui.services.matlab_command_format import (
    BINDING,
    MATLAB_OPTION_EMISSION,
    _format_where_pair,
)
from scistack_gui.stores import pipeline_store


class WrQuality(BaseVariable):
    """A per-recording score the tests filter on."""

    schema_version = 1


QUALITY_ROW = {"variable": "WrQuality", "op": ">", "value": "0.5"}
BLANK_ROW = {"variable": "", "op": "==", "value": ""}


@pytest.fixture
def filtered_node(client, bp_node_id):
    """The seeded bandpass node with ``WrQuality > 0.5`` stated on it.
    Subject 1 scores 0.9 and subject 2 scores 0.2, in both sessions."""
    db = get_db()
    for subject, score in ((1, 0.9), (2, 0.2)):
        for session in ("pre", "post"):
            WrQuality.save(score, subject=subject, session=session)
    pipeline_store.update_node_config(
        db, bp_node_id, {where_service.CONFIG_KEY: [QUALITY_ROW, BLANK_ROW]}
    )
    return bp_node_id


class TestStoredFilters:
    def test_no_node_and_no_filters_mean_no_where(self, client, bp_node_id):
        db = get_db()
        assert where_service.where_for_node(db, None, route="test") is None
        assert where_service.where_for_node(db, bp_node_id, route="test") is None
        assert where_service.matlab_where_for_node(db, bp_node_id, route="test") == ""

    def test_blank_rows_are_not_filters(self, filtered_node):
        assert where_service.stated_filters(get_db(), filtered_node) == [QUALITY_ROW]

    def test_the_filter_is_logged_with_its_route(self, filtered_node, caplog):
        with caplog.at_level(logging.INFO, logger=where_service.logger.name):
            where = where_service.where_for_node(get_db(), filtered_node, route="test route")
        assert where.to_key() == "WrQuality > 0.5"
        assert "route=test route" in caplog.text and "WrQuality > 0.5" in caplog.text

    def test_a_half_filled_row_is_refused_not_dropped(self, client, bp_node_id):
        db = get_db()
        pipeline_store.update_node_config(
            db, bp_node_id, {where_service.CONFIG_KEY: [{"variable": "WrQuality", "op": ">", "value": ""}]}
        )
        with pytest.raises(ValueError, match="no value"):
            where_service.where_for_node(db, bp_node_id, route="test")

    def test_an_unknown_variable_is_refused(self, client, bp_node_id):
        db = get_db()
        pipeline_store.update_node_config(
            db, bp_node_id, {where_service.CONFIG_KEY: [{"variable": "Nope", "op": ">", "value": "1"}]}
        )
        with pytest.raises(ValueError, match="not a known variable"):
            where_service.where_for_node(db, bp_node_id, route="test")


class TestMatlabSpelling:
    def test_one_filter_is_bare_and_several_are_a_cell(self):
        assert where_service.matlab_where_source([]) == ""
        assert where_service.matlab_where_source([QUALITY_ROW]) == "WrQuality() > 0.5"
        both = where_service.matlab_where_source(
            [QUALITY_ROW, {"variable": "Sex", "op": "!=", "value": "M"}]
        )
        assert both == '{WrQuality() > 0.5, Sex() ~= "M"}'

    def test_a_column_and_the_value_types(self):
        assert (
            where_service.matlab_where_source(
                [{"variable": "Gait", "column": "Side", "op": "==", "value": 'say "L"'}]
            )
            == 'Gait("Side") == "say ""L"""'
        )
        assert where_service.matlab_where_source(
            [{"variable": "Flag", "op": "==", "value": "True"}]
        ) == "Flag() == true"

    def test_the_pair_and_its_place_in_the_emission_table(self):
        assert MATLAB_OPTION_EMISSION["where"] == BINDING
        assert _format_where_pair(None) == ""
        assert _format_where_pair("WrQuality() > 0.5") == "'where', WrQuality() > 0.5"


class TestEveryRoute:
    def test_python_export_writes_the_filter(self, filtered_node):
        script = export_pipeline_to_code(get_db(), "main")["script"]
        assert "where=WrQuality > 0.5" in script

    def test_python_export_without_a_filter_says_nothing(self, client, bp_node_id):
        assert "where=" not in export_pipeline_to_code(get_db(), "main")["script"]

    def test_compiled_pipeline_step_carries_the_filter(self, filtered_node):
        from scistack_gui.services.execution_service import (
            _discard_compiled,
            build_backend_pipeline,
        )

        built: dict = {}
        try:
            pipe = build_backend_pipeline(get_db(), "main", built)
            wheres = [spec.options.get("where") for _key, spec in pipe._composed_steps()]
        finally:
            _discard_compiled(built)
        assert [w.to_key() for w in wheres if w is not None] == ["WrQuality > 0.5"]

    def test_per_node_run_filters_and_records_the_clause(self, filtered_node, monkeypatch):
        db = get_db()
        emitted: list[dict] = []
        monkeypatch.setattr(run_service, "push_message", emitted.append)
        monkeypatch.setattr(run_service, "enter_project_root", lambda: None)

        run_service._run_in_thread("where-run", "bandpass_filter", [], db, node_id=filtered_node)

        (done,) = [m for m in emitted if m.get("type") == "run_done"]
        assert done["success"] is True, done
        clauses = db._duck.fetchdf(
            "SELECT where_clause FROM _run ORDER BY timestamp DESC LIMIT 1"
        )["where_clause"].tolist()
        assert clauses == ["WrQuality > 0.5"]

    def test_per_node_run_ends_on_an_unusable_filter(self, client, bp_node_id, monkeypatch):
        db = get_db()
        pipeline_store.update_node_config(
            db, bp_node_id, {where_service.CONFIG_KEY: [{"variable": "Nope", "op": ">", "value": "1"}]}
        )
        emitted: list[dict] = []
        monkeypatch.setattr(run_service, "push_message", emitted.append)
        monkeypatch.setattr(run_service, "enter_project_root", lambda: None)

        run_service._run_in_thread("where-bad", "bandpass_filter", [], db, node_id=bp_node_id)

        (done,) = [m for m in emitted if m.get("type") == "run_done"]
        assert done["success"] is False
        assert "not a known variable" in done["error"]
