"""
Saving a plot onto the canvas (GitHub issue #19): ``endpoint_node_service``.

"Save … Add to pipeline" writes the figure's ``plot_`` function between
generated markers, declares its output, and places the node on the canvas,
wired from the input variable to the output variable, with its output path,
iteration level and ``as_table`` stated. The marker is the link between the
saved plot and its function, so re-saving updates rather than duplicates.
"""

from __future__ import annotations

import pytest

from scistack_gui.services import endpoint_node_service as en
from scistack_gui.services import plot_service, saved_plot_service
from scistack_gui.stores import pipeline_store

from tests.test_plot_service import _pooled_spec


# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------


def test_names_come_from_the_saved_name():
    assert en.function_name_for("plot", "Step length by group") == "plot_step_length_by_group"
    assert en.output_variable_for("plot", "Step length by group") == "StepLengthByGroupFigure"
    assert en.output_variable_for("stat", "gait (v2)") == "GaitV2Stats"
    assert en.output_variable_for("plot", "2025 cohort") == "Plot2025CohortFigure"
    assert en.function_name_for("plot", "!!!") == "plot_figure"


# ---------------------------------------------------------------------------
# The generated block
# ---------------------------------------------------------------------------


SOURCE_V1 = "def plot_gait(df, filename):\n    return 1\n"
SOURCE_V2 = "def plot_gait(df, filename):\n    return 2\n"


class TestWriteFunction:
    def test_a_block_is_written_then_replaced_in_place(self, client_with_variable_file, tmp_path):
        first = en.write_function("plot", "plot_gait", SOURCE_V1, owner_id="p1")
        assert first["action"] == "wrote"
        second = en.write_function("plot", "plot_gait", SOURCE_V2, owner_id="p1")
        assert second["action"] == "replaced"

        text = (tmp_path / "scistack_plots.py").read_text()
        assert text.count("def plot_gait(") == 1
        assert "return 2" in text and "return 1" not in text
        assert "# scistack:generated plot id=p1" in text
        assert "# scistack:end plot id=p1" in text

    def test_the_block_keeps_its_function_name(self, client_with_variable_file):
        en.write_function("plot", "plot_gait", SOURCE_V1, owner_id="p1")
        assert en.generated_function_name("plot", "p1") == "plot_gait"
        assert en.generated_function_name("plot", "other") is None

    def test_two_saved_plots_get_two_blocks(self, client_with_variable_file, tmp_path):
        en.write_function("plot", "plot_gait", SOURCE_V1, owner_id="p1")
        en.write_function("plot", "plot_other", "def plot_other(df, filename):\n    return 3\n", owner_id="p2")
        en.write_function("plot", "plot_gait", SOURCE_V2, owner_id="p1")
        text = (tmp_path / "scistack_plots.py").read_text()
        assert "def plot_other(" in text and "return 3" in text
        assert text.count("def plot_gait(") == 1

    def test_a_same_named_function_outside_the_block_is_never_overwritten(
        self, client_with_variable_file, tmp_path
    ):
        en.write_function("plot", "plot_gait", SOURCE_V1)  # plain Add to pipeline: no block
        with pytest.raises(en.EndpointWriteRefused, match="already defines plot_gait outside"):
            en.write_function("plot", "plot_gait", SOURCE_V2, owner_id="p1")
        assert "return 1" in (tmp_path / "scistack_plots.py").read_text()

    def test_a_block_missing_its_end_marker_is_refused(self, client_with_variable_file, tmp_path):
        en.write_function("plot", "plot_gait", SOURCE_V1, owner_id="p1")
        path = tmp_path / "scistack_plots.py"
        path.write_text(path.read_text().replace("# scistack:end plot id=p1", ""))
        with pytest.raises(en.EndpointWriteRefused, match="lost its end marker"):
            en.write_function("plot", "plot_gait", SOURCE_V2, owner_id="p1")


# ---------------------------------------------------------------------------
# Save -> canvas, end to end
# ---------------------------------------------------------------------------


def _fn_node(db, label):
    from scistack_gui.ids import ROOT_SCOPE
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    graph = get_pipeline_graph(db, ROOT_SCOPE, run_states=False)
    nodes = [n for n in graph["nodes"] if n["type"] == "functionNode" and n["data"]["label"] == label]
    return nodes, graph["edges"]


class TestSaveOntoCanvas:
    def _save(self, db, spec, name="Raw gait", **kw):
        return saved_plot_service.save(db, "RawSignal", name, spec, {}, add_to_pipeline=True, **kw)

    def test_save_places_a_wired_configured_node(self, client_with_variable_file, populated_db, tmp_path):
        pytest.importorskip("seaborn")
        spec = _pooled_spec(populated_db)
        reply = self._save(populated_db, spec)

        assert reply["ok"] is True
        [placed] = reply["pipeline"]
        assert placed.get("error") is None, placed
        assert placed["placed"] is True
        assert placed["function_name"] == "plot_raw_gait"
        assert placed["output_variable"] == "RawGaitFigure"
        assert "# scistack:generated plot id=" in (tmp_path / "scistack_plots.py").read_text()

        nodes, edges = _fn_node(populated_db, "plot_raw_gait")
        assert len(nodes) == 1
        node_id = nodes[0]["id"]
        into = [e for e in edges if e["target"] == node_id]
        assert any(e.get("targetHandle") == "in__df" for e in into)
        assert any(e["source"] == node_id for e in edges)

        code = plot_service.generate_code(populated_db, spec, function_name="plot_raw_gait")
        cfg = pipeline_store.get_node_config(populated_db, node_id)
        assert cfg["pathOutputs"] == {"filename": code.path_template}
        assert cfg["schemaLevel"] == code.iterate_keys
        assert cfg["runOptions"]["as_table"] == ["df"]

    def test_resaving_updates_the_same_node(self, client_with_variable_file, populated_db, tmp_path):
        pytest.importorskip("seaborn")
        spec = _pooled_spec(populated_db)
        first = self._save(populated_db, spec)
        plot_id = first["plot"]["plot_id"]
        _, edges_before = _fn_node(populated_db, "plot_raw_gait")

        second = self._save(populated_db, spec, current_plot_id=plot_id)
        [again] = second["pipeline"]
        assert again["action"] == "replaced"
        assert again["placed"] is False  # the node was already there
        assert again["edges"] == 0
        nodes, edges_after = _fn_node(populated_db, "plot_raw_gait")
        assert len(nodes) == 1
        assert len(edges_after) == len(edges_before)
        assert (tmp_path / "scistack_plots.py").read_text().count("def plot_raw_gait(") == 1

    def test_a_renamed_saved_plot_keeps_its_function(self, client_with_variable_file, populated_db):
        pytest.importorskip("seaborn")
        spec = _pooled_spec(populated_db)
        first = self._save(populated_db, spec)
        plot_id = first["plot"]["plot_id"]
        saved_plot_service.rename(populated_db, plot_id, "Renamed")
        second = self._save(populated_db, spec, name="Renamed", current_plot_id=plot_id)
        assert second["pipeline"][0]["function_name"] == "plot_raw_gait"

    def test_without_the_flag_nothing_is_written(self, client_with_variable_file, populated_db, tmp_path):
        pytest.importorskip("seaborn")
        reply = saved_plot_service.save(populated_db, "RawSignal", "Raw gait", _pooled_spec(populated_db), {})
        assert "pipeline" not in reply
        assert not (tmp_path / "scistack_plots.py").exists()

    def test_a_pinned_variant_is_written_but_not_placed(
        self, client_with_variable_file, populated_db, tmp_path, monkeypatch
    ):
        """The canvas has no edge form for a Variant(...) pin yet: the save
        still succeeds, the function is written, and the reply says why."""
        from scistackplotdb import EndpointCode, EndpointInput

        def pinned_code(db, spec_payload, *, function_name=None, output_variable=None, **_):
            return EndpointCode(
                function_name=function_name,
                function_source=f"def {function_name}(df, filename):\n    return None\n",
                foreach_source="for_each(...)",
                path_template="x.png",
                output_variable=output_variable,
                inputs={
                    "df": EndpointInput("Variant(RawSignal, code_version='latest')"),
                    "filename": EndpointInput('PathOutput("x.png")', template="x.png"),
                },
                as_table=["df"],
            )

        monkeypatch.setattr(plot_service, "generate_code", pinned_code)
        reply = self._save(populated_db, _pooled_spec(populated_db))
        assert reply["ok"] is True
        [result] = reply["pipeline"]
        assert result["placed"] is False
        assert "df = Variant(RawSignal" in result["skipped"]
        assert "def plot_raw_gait(" in (tmp_path / "scistack_plots.py").read_text()
        nodes, _ = _fn_node(populated_db, "plot_raw_gait")
        assert nodes == []

    def test_a_failure_is_reported_beside_the_save(self, client_with_variable_file, populated_db, monkeypatch):
        def boom(*a, **k):
            raise en.EndpointWriteRefused("nope")

        monkeypatch.setattr(en, "write_function", boom)
        pytest.importorskip("seaborn")
        reply = self._save(populated_db, _pooled_spec(populated_db))
        assert reply["ok"] is True  # the saved plot IS saved
        assert reply["pipeline"] == [{"kind": "plot", "placed": False, "error": "nope"}]
