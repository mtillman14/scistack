"""Phase 2 tests: Inspector.pipeline / .variants, graph builder, renderers, CLI.

Fixture pipeline (schema keys subject/session):
    PipeRaw ─ bandpass2(low_hz∈{20,30}) → PipeFilt      (one step, 2 variants)
    PipeRaw ─ stats2()                  → PipeStats     (one step, 1 variant)

The "red" variant of the fixture saves one extra PipeRaw record afterwards
without re-running, so both steps have a missing expected invocation.
"""

import dataclasses
import json

import numpy as np
import pytest
from scidb.inspect import Inspector, render
from scidb.inspect.cli import main

from scidb import BaseVariable, NotFoundError, configure_database, for_each

SCHEMA_KEYS = ["subject", "session"]


class PipeRaw(BaseVariable):
    schema_version = 1


class PipeFilt(BaseVariable):
    schema_version = 1


class PipeStats(BaseVariable):
    schema_version = 1


def bandpass2(signal, low_hz):
    return signal * low_hz


def stats2(signal):
    return float(np.sum(signal))


def build_pipeline_db(db_path, extra_unrun_input: bool = False):
    db = configure_database(db_path, SCHEMA_KEYS)
    PipeRaw.save(np.array([1.0, 2.0]), subject="S01", session="1")
    PipeRaw.save(np.array([3.0, 4.0]), subject="S02", session="1")
    subjects = ["S01", "S02"]
    for_each(
        bandpass2,
        {"signal": PipeRaw, "low_hz": 20},
        [PipeFilt],
        subject=subjects,
        session=["1"],
    )
    for_each(
        bandpass2,
        {"signal": PipeRaw, "low_hz": 30},
        [PipeFilt],
        subject=subjects,
        session=["1"],
    )
    for_each(stats2, {"signal": PipeRaw}, [PipeStats], subject=subjects, session=["1"])
    if extra_unrun_input:
        # New input data with no re-run → every step has missing expected work.
        PipeRaw.save(np.array([5.0, 6.0]), subject="S03", session="1")
    db.close()


@pytest.fixture(scope="module")
def green_db(tmp_path_factory):
    path = tmp_path_factory.mktemp("pipe_green") / "pipe.duckdb"
    build_pipeline_db(path)
    return path


@pytest.fixture(scope="module")
def red_db(tmp_path_factory):
    path = tmp_path_factory.mktemp("pipe_red") / "pipe.duckdb"
    build_pipeline_db(path, extra_unrun_input=True)
    return path


@pytest.fixture
def insp(green_db):
    with Inspector.open(green_db) as inspector:
        yield inspector


class TestGraphStructure:
    def test_steps_grouped_by_wiring_not_call_id(self, insp):
        g = insp.pipeline()
        by_fn = {f.function_name: f for f in g.functions}
        # Two low_hz sweeps share wiring → ONE bandpass2 step with 2 variants.
        assert len(g.functions) == 2
        assert by_fn["bandpass2"].variant_count == 2
        assert by_fn["bandpass2"].constants == {"low_hz": ["20", "30"]}
        assert len(by_fn["bandpass2"].call_ids) == 2  # variants keep call_ids
        assert by_fn["stats2"].variant_count == 1
        assert by_fn["stats2"].constants == {}

    def test_variable_nodes_and_counts(self, insp):
        g = insp.pipeline()
        by_name = {v.name: v for v in g.variables}
        assert by_name["PipeRaw"].record_count == 2
        assert by_name["PipeFilt"].record_count == 4
        assert by_name["PipeStats"].record_count == 2
        assert by_name["PipeRaw"].produced_by == []
        assert len(by_name["PipeRaw"].consumed_by) == 2
        assert len(by_name["PipeFilt"].produced_by) == 1

    def test_edges(self, insp):
        g = insp.pipeline()
        by_fn = {f.function_name: f for f in g.functions}
        bp = by_fn["bandpass2"]
        var_to_fn = [
            (e.source, e.target, e.param) for e in g.edges if e.target == bp.id
        ]
        assert var_to_fn == [("var__PipeRaw", bp.id, "signal")]
        fn_to_var = [(e.source, e.target) for e in g.edges if e.source == bp.id]
        assert "var__PipeFilt" in {t for _, t in fn_to_var}

    def test_record_count_per_step(self, insp):
        g = insp.pipeline()
        by_fn = {f.function_name: f for f in g.functions}
        assert by_fn["bandpass2"].record_count == 4
        assert by_fn["stats2"].record_count == 2

    def test_type_filter_keeps_ancestors_only(self, insp):
        g = insp.pipeline(output_type="PipeStats")
        assert {v.name for v in g.variables} == {"PipeRaw", "PipeStats"}
        assert {f.function_name for f in g.functions} == {"stats2"}

    def test_type_filter_unknown_raises(self, insp):
        with pytest.raises(NotFoundError):
            insp.pipeline(output_type="NoSuchType")

    def test_json_round_trip(self, insp):
        g = insp.pipeline()
        parsed = json.loads(json.dumps(dataclasses.asdict(g), default=str))
        assert len(parsed["functions"]) == 2


class TestNodeState:
    def test_fully_run_is_green_on_stored_hash_basis(self, insp):
        g = insp.pipeline()
        for f in g.functions:
            assert f.state == "green", f.function_name
            assert f.state_basis == "stored_hash"
            assert f.state_counts["missing"] == 0
            assert f.state_counts["up_to_date"] > 0

    def test_new_unrun_input_turns_red(self, red_db):
        with Inspector.open(red_db) as inspector:
            g = inspector.pipeline()
        for f in g.functions:
            assert f.state == "red", f.function_name
            assert f.state_counts["missing"] >= 1

    def test_live_fn_registry_basis(self, insp):
        g = insp.pipeline(fn_registry={"bandpass2": bandpass2, "stats2": stats2})
        for f in g.functions:
            assert f.state_basis == "live_fn"
            assert f.state == "green", f.function_name


class TestVariants:
    def test_by_output_type(self, insp):
        vs = insp.variants("PipeFilt")
        assert len(vs) == 2
        assert {v.constants["low_hz"] for v in vs} == {"20", "30"}
        assert all(v.function_name == "bandpass2" for v in vs)
        assert all(v.record_count == 2 for v in vs)
        assert len({v.call_id for v in vs}) == 2

    def test_by_function_name(self, insp):
        vs = insp.variants("bandpass2")
        assert len(vs) == 2

    def test_accepts_class(self, insp):
        assert len(insp.variants(PipeFilt)) == 2

    def test_raw_variable_returns_empty(self, insp):
        assert insp.variants("PipeRaw") == []

    def test_unknown_name_raises(self, insp):
        with pytest.raises(NotFoundError):
            insp.variants("NoSuchThing")


class TestRenderers:
    def test_tree(self, insp):
        text = render.render_pipeline_tree(insp.pipeline())
        assert "● PipeRaw" in text
        assert "bandpass2" in text and "[green" in text
        assert "low_hz = {20, 30}" in text
        assert "PipeFilt" in text

    def test_tree_expand_variants(self, insp):
        text = render.render_pipeline_tree(insp.pipeline(), expand_variants=True)
        assert "low_hz=20" in text and "low_hz=30" in text

    def test_tree_include_values(self, insp):
        text = render.render_pipeline_tree(insp.pipeline(), include_values=True)
        assert "signal ◀ PipeRaw" in text

    def test_mermaid(self, insp):
        text = render.render_pipeline_mermaid(insp.pipeline())
        assert text.startswith("flowchart TD")
        assert "stgreen" in text
        assert "var__PipeRaw" in text

    def test_dot(self, insp):
        text = render.render_pipeline_dot(insp.pipeline())
        assert text.startswith("digraph pipeline {")
        assert text.rstrip().endswith("}")
        assert '"var__PipeRaw"' in text


class TestRenderStyle:
    """The style seam: presentation is retuned via RenderStyle only —
    renderer code and callers unchanged."""

    def test_ascii_preset_is_pure_ascii(self, insp):
        from scidb.inspect.render import ASCII_STYLE

        text = render.render_pipeline_tree(insp.pipeline(), style=ASCII_STYLE)
        text.encode("ascii")  # raises if any Unicode glyph leaked through
        assert "* PipeRaw" in text
        assert "`->" in text or "+->" in text

    def test_custom_tag_wording_without_renderer_change(self, insp):
        from scidb.inspect.render import RenderStyle

        style = RenderStyle(tag_fmt="<{tag}>", stored_hash_note="")
        text = render.render_pipeline_tree(insp.pipeline(), style=style)
        assert "<green>" in text
        assert "last-run recipe" not in text

    def test_schema_tree_shares_the_style(self, insp):
        from scidb.inspect.render import ASCII_STYLE

        text = render.render_schema_tree(insp.schema_tree(), style=ASCII_STYLE)
        text.encode("ascii")

    def test_cli_style_flag(self, green_db, capsys):
        assert main(["--db", str(green_db), "pipeline", "--style", "ascii"]) == 0
        out = capsys.readouterr().out
        out.encode("ascii")
        assert "* PipeRaw" in out

    def test_cli_style_env_var(self, green_db, capsys, monkeypatch):
        monkeypatch.setenv("SCIDB_STYLE", "ascii")
        assert main(["--db", str(green_db), "pipeline"]) == 0
        capsys.readouterr().out.encode("ascii")

    def test_cli_unknown_env_style_fails_cleanly(self, green_db, capsys, monkeypatch):
        monkeypatch.setenv("SCIDB_STYLE", "neon")
        assert main(["--db", str(green_db), "pipeline"]) == 1
        assert "Unknown render style" in capsys.readouterr().err


class TestCli:
    def test_pipeline_json(self, green_db, capsys):
        assert main(["--db", str(green_db), "pipeline", "--format", "json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert {f["function_name"] for f in payload["functions"]} == {
            "bandpass2",
            "stats2",
        }

    def test_pipeline_json_flag(self, green_db, capsys):
        assert main(["--db", str(green_db), "pipeline", "--json"]) == 0
        assert json.loads(capsys.readouterr().out)["functions"]

    def test_pipeline_tree_default(self, green_db, capsys):
        assert main(["--db", str(green_db), "pipeline"]) == 0
        assert "bandpass2" in capsys.readouterr().out

    def test_pipeline_mermaid_to_file(self, green_db, tmp_path, capsys):
        out_file = tmp_path / "pipe.mmd"
        assert (
            main(
                [
                    "--db",
                    str(green_db),
                    "pipeline",
                    "--format",
                    "mermaid",
                    "-o",
                    str(out_file),
                ]
            )
            == 0
        )
        captured = capsys.readouterr()
        assert captured.out == ""  # file mode keeps stdout clean
        assert out_file.read_text().startswith("flowchart TD")

    def test_pipeline_type_filter(self, green_db, capsys):
        assert (
            main(
                [
                    "--db",
                    str(green_db),
                    "pipeline",
                    "--type",
                    "PipeStats",
                    "--format",
                    "json",
                ]
            )
            == 0
        )
        payload = json.loads(capsys.readouterr().out)
        assert {v["name"] for v in payload["variables"]} == {"PipeRaw", "PipeStats"}

    def test_variants_command(self, green_db, capsys):
        assert main(["--db", str(green_db), "variants", "PipeFilt", "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert len(payload) == 2

    def test_variants_human(self, green_db, capsys):
        assert main(["--db", str(green_db), "variants", "PipeFilt"]) == 0
        out = capsys.readouterr().out
        assert "low_hz=20" in out and "low_hz=30" in out


class TestTopologies:
    """Two levels, because they are two questions.

    `variants()` alone cannot answer "why does this variable have more
    variants than I expected": a second TOPOLOGY — a node the user did not
    know existed — reads exactly like a second variant of the node they did.
    Grouping by DAG shape separates the two, and ordering by when things were
    saved lets the list be read in the order they happened.

    Added 2026-09-22 with the load-path verdict, which is what makes the view
    a diagnostic: a variant node state counts and a load drops is invisible
    everywhere else, and is the shape of the bug this came from
    (docs/claude/run-option-variants.md §"The third consumer").
    """

    def test_two_constant_variants_are_one_topology(self, insp):
        """Same function, same inputs, same output — different constants.
        One node on the canvas, two runs of it."""
        groups = insp.topologies("PipeFilt")
        assert len(groups) == 1
        (fn_name, inputs, output_type), variants = groups[0]
        assert fn_name == "bandpass2"
        assert output_type == "PipeFilt"
        assert dict(inputs) == {"signal": "PipeRaw"}
        assert {v.constants["low_hz"] for v in variants} == {"20", "30"}

    def test_every_variant_appears_exactly_once(self, insp):
        flat = insp.variants("PipeFilt")
        grouped = [v for _k, group in insp.topologies("PipeFilt") for v in group]
        assert sorted(v.call_id for v in grouped) == sorted(v.call_id for v in flat)

    def test_variants_are_ordered_by_when_they_were_saved(self, insp):
        stamps = [v.first_saved for v in insp.variants("PipeFilt")]
        assert all(s is not None for s in stamps), "no chronology to sort by"
        assert stamps == sorted(stamps)

    def test_a_variant_knows_where_its_records_are(self, insp):
        for v in insp.variants("PipeFilt"):
            assert v.schema_ids, "a count of records is not a count of places"
            assert len(v.schema_ids) <= v.record_count

    def test_an_unsuperseded_variant_reads_as_current(self, insp):
        """Nothing here has been re-run another way, so everything is live."""
        for v in insp.variants("PipeFilt"):
            assert v.current is True
            assert v.current_record_count == v.record_count

    def test_the_renderer_names_the_shape_and_the_verdict(self, insp):
        from scidb.inspect import render

        text = render.render_topologies(
            "PipeFilt", insp.topologies("PipeFilt"), db=insp._db
        )
        assert "topology" in text
        assert "bandpass2(signal: PipeRaw) -> PipeFilt" in text
        assert "load: CURRENT" in text
        assert "location(s)" in text

    def test_an_unknown_name_still_raises(self, insp):
        with pytest.raises(NotFoundError):
            insp.topologies("NoSuchThing")
