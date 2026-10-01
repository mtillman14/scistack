"""PathInputs are part of a call's identity, not an afterthought.

A target's bindings used to live in three parallel dicts (``input_types`` /
``path_input_params`` / ``parameter_params``) and every consumer had to
remember all three. The two that forgot were both identity functions:

* ``variant_resolver.compute_call_id`` hashed only ``input_types``, so its
  predicted call_id omitted the PathInput that scidb's
  ``ForEachConfig._serialize_inputs`` puts INTO ``__inputs`` — meaning a combo
  hidden before its first run was hidden under an id no record would ever
  carry.
* ``graph_builder.wiring_id`` had the same blind spot, so two call sites fed
  by different PathInputs into the same output collapsed onto one canvas node.

Both now read the unified ``bindings`` dict.
"""

import pytest
import scifor as _scifor

from scistack_gui.domain.edge_resolver import (
    BINDING_PARAMETER,
    BINDING_PATHINPUT,
    BINDING_VARIABLE,
    pathinput_binding,
    resolve_function_edges,
    variable_binding,
)
from scistack_gui.domain.graph_builder import path_input_bindings_by_fkey, wiring_id
from scistack_gui.domain.variant_resolver import compute_call_id


# ---------------------------------------------------------------------------
# One dict, filled from the edges
# ---------------------------------------------------------------------------


class TestResolveIntoOneDict:
    def test_all_three_kinds_land_in_bindings(self):
        edges = [
            {
                "id": "e1",
                "source": "var__RawEMG",
                "target": "fn__f",
                "targetHandle": "in__signal",
            },
            {
                "id": "e2",
                "source": "pathInput__test_pi",
                "target": "fn__f",
                "targetHandle": "in__filepath_or_buffer",
            },
            {
                "id": "e3",
                "source": "param__test",
                "target": "fn__f",
                "targetHandle": "in__low_hz",
            },
            {"id": "e4", "source": "fn__f", "target": "var__Out", "targetHandle": ""},
        ]
        resolved = resolve_function_edges(
            fn_node_ids={"fn__f"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )

        assert resolved.bindings == {
            "signal": {"kind": BINDING_VARIABLE, "ref": ["RawEMG"]},
            "filepath_or_buffer": {"kind": BINDING_PATHINPUT, "ref": "test_pi"},
            "low_hz": {"kind": BINDING_PARAMETER, "ref": "test"},
        }
        assert resolved.output_types == ["Out"]

    def test_views_are_derived_not_separate_state(self):
        resolved = resolve_function_edges(
            fn_node_ids={"fn__f"},
            manual_edges=[
                {
                    "id": "e1",
                    "source": "pathInput__pi",
                    "target": "fn__f",
                    "targetHandle": "in__path",
                },
                {
                    "id": "e2",
                    "source": "var__V",
                    "target": "fn__f",
                    "targetHandle": "in__v",
                },
            ],
            manual_nodes={},
            existing_node_labels={},
        )
        assert resolved.path_input_params == {"path": "pi"}
        assert resolved.input_types == {"v": "V"}
        assert resolved.parameter_params == {}

    def test_multiple_variables_on_one_handle_accumulate(self):
        """Several variable edges onto one handle is EachOf, not a conflict."""
        resolved = resolve_function_edges(
            fn_node_ids={"fn__f"},
            manual_edges=[
                {
                    "id": "e1",
                    "source": "var__A",
                    "target": "fn__f",
                    "targetHandle": "in__x",
                },
                {
                    "id": "e2",
                    "source": "var__B",
                    "target": "fn__f",
                    "targetHandle": "in__x",
                },
            ],
            manual_nodes={},
            existing_node_labels={},
        )
        assert resolved.bindings["x"]["ref"] == ["A", "B"]


# ---------------------------------------------------------------------------
# wiring_id: PathInputs are part of the shape
# ---------------------------------------------------------------------------


class TestWiringIdIncludesPathInputs:
    def test_different_path_inputs_are_different_wirings(self):
        a = wiring_id("read_csv", {}, {"Out"}, {"filepath_or_buffer": "raw_pi"})
        b = wiring_id("read_csv", {}, {"Out"}, {"filepath_or_buffer": "processed_pi"})
        assert a != b, "two PathInputs must not collapse onto one canvas node"

    def test_same_path_input_is_the_same_wiring(self):
        a = wiring_id("read_csv", {}, {"Out"}, {"filepath_or_buffer": "pi"})
        b = wiring_id("read_csv", {}, {"Out"}, {"filepath_or_buffer": "pi"})
        assert a == b

    def test_no_path_inputs_omits_the_term(self):
        """Omitted rather than hashed as {} — mirrors scidb's to_version_keys
        dropping __inputs entirely — so only PathInput-fed nodes are affected
        by this term."""
        assert wiring_id("f", {"x": "V"}, {"Out"}, {}) == wiring_id(
            "f", {"x": "V"}, {"Out"}, {}
        )
        assert wiring_id("f", {"x": "V"}, {"Out"}, {}) != wiring_id(
            "f", {"x": "V"}, {"Out"}, {"p": "pi"}
        )


class TestPathInputBindingsByFkey:
    def test_inverts_declared_name_keying(self):
        path_inputs = {
            "pi_a": {"functions": {(("read_csv", "cid1"), "filepath_or_buffer")}},
            "pi_b": {"functions": {(("read_csv", "cid2"), "filepath_or_buffer")}},
        }
        assert path_input_bindings_by_fkey(path_inputs) == {
            ("read_csv", "cid1"): {"filepath_or_buffer": "pi_a"},
            ("read_csv", "cid2"): {"filepath_or_buffer": "pi_b"},
        }

    def test_empty_is_empty(self):
        assert path_input_bindings_by_fkey({}) == {}


# ---------------------------------------------------------------------------
# compute_call_id: must agree with scidb's recipe
# ---------------------------------------------------------------------------


@pytest.fixture
def declared_path_input(monkeypatch, tmp_path):
    """Put one live PathInput in the registry, as source declaration would."""
    from scistack_gui import registry

    pi = _scifor.PathInput("{subject}/{subject}_data.csv", root_folder=str(tmp_path), name="{subject}/{subject}_data.csv")
    monkeypatch.setattr(registry, "get_path_inputs_registry", lambda: {"test_pi": pi})
    return pi


class TestComputeCallIdIncludesPathInputs:
    def test_matches_scidb_version_key_recipe(self, declared_path_input):
        """The predicted id must be the id scidb writes: __inputs carries the
        PathInput under its parameter name, keyed by the PathInput's own
        to_key() (ForEachConfig._serialize_inputs)."""
        from scidb import BaseVariable
        from scidb.foreach_config import ForEachConfig

        target = {
            "bindings": {
                "filepath_or_buffer": pathinput_binding("test_pi"),
                "signal": variable_binding(["RawEMG"]),
            },
            "constants": {"low_hz": 20},
            "output_type": "Out",
        }

        class RawEMG(BaseVariable):
            pass

        def read_csv(filepath_or_buffer, signal, low_hz):
            return None

        # The REAL forward id, from live inputs — not a hand-built payload.
        expected = ForEachConfig(
            read_csv,
            {"filepath_or_buffer": declared_path_input, "signal": RawEMG, "low_hz": 20},
        ).to_call_id()
        assert compute_call_id("read_csv", target) == expected

    def test_different_templates_get_different_ids(self, monkeypatch, tmp_path):
        from scistack_gui import registry

        pi_a = _scifor.PathInput("{subject}/a.csv", root_folder=str(tmp_path), name="{subject}/a.csv")
        pi_b = _scifor.PathInput("{subject}/b.csv", root_folder=str(tmp_path), name="{subject}/b.csv")
        monkeypatch.setattr(
            registry, "get_path_inputs_registry", lambda: {"a": pi_a, "b": pi_b}
        )

        id_a = compute_call_id(
            "f", {"bindings": {"p": pathinput_binding("a")}, "constants": {}}
        )
        id_b = compute_call_id(
            "f", {"bindings": {"p": pathinput_binding("b")}, "constants": {}}
        )
        assert id_a is not None and id_a != id_b

    def test_undeclared_path_input_fails_safe(self, monkeypatch):
        """No live PathInput means we cannot reproduce scidb's key — return
        None ('unknown, don't filter') rather than hash a known-wrong value."""
        from scistack_gui import registry

        monkeypatch.setattr(registry, "get_path_inputs_registry", lambda: {})
        assert (
            compute_call_id(
                "f", {"bindings": {"p": pathinput_binding("gone")}, "constants": {}}
            )
            is None
        )

    def test_parameter_bindings_do_not_enter_inputs(self, monkeypatch):
        """A Parameter's concrete values travel in __constants, exactly as in
        scidb's ForEachConfig — so binding one must not change the id."""
        from scistack_gui.domain.edge_resolver import parameter_binding

        with_param = compute_call_id(
            "f",
            {
                "bindings": {
                    "x": variable_binding(["V"]),
                    "low_hz": parameter_binding("test"),
                },
                "constants": {"low_hz": 20},
            },
        )
        without_param = compute_call_id(
            "f",
            {"bindings": {"x": variable_binding(["V"])}, "constants": {"low_hz": 20}},
        )
        assert with_param == without_param

    def test_unresolved_each_of_still_fails_safe(self):
        assert (
            compute_call_id(
                "f", {"bindings": {"x": variable_binding(["A", "B"])}, "constants": {}}
            )
            is None
        )


class TestWiringIdIsScidbs:
    """`wiring_id` is scidb's recipe (`scidb.provenance.compute_wiring_id`),
    imported — not a GUI prediction (Stage 1 of the 2026-09-20 plan)."""

    def test_the_gui_hashes_with_scidbs_recipe(self):
        from scidb.provenance import compute_wiring_id

        from scistack_gui.domain.graph_builder import wiring_id

        args = ("load", {"x": "Raw"}, {"Out"}, {"f": "files"})
        assert wiring_id(*args) == compute_wiring_id(*args)

    def test_raw_and_partitioned_views_hash_alike(self):
        """A raw variant's input_types carries the PathInput SPEC beside the
        variable inputs; the canvas view partitions it out. The recipe strips
        it itself, so the two views cannot hash differently — the mismatch
        that once made a graduated PathInput-fed node unrunnable."""
        from scistack_gui.domain.graph_builder import wiring_id

        spec = '{"__type": "PathInput", "template": "{subject}/x.csv", "root_folder": null}'
        raw = wiring_id("load", {"x": "Raw", "path": spec}, {"Out"}, {"path": "files"})
        partitioned = wiring_id("load", {"x": "Raw"}, {"Out"}, {"path": "files"})
        assert raw == partitioned

    def test_the_bytes_did_not_move(self):
        """Node ids key saved layout positions: the recipe's output for a
        fixed input is pinned so a refactor cannot silently re-key every
        canvas."""
        from scistack_gui.domain.graph_builder import wiring_id

        assert wiring_id("bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {}) == (
            wiring_id("bandpass_filter", {"signal": "RawSignal"}, ["FilteredSignal"], None)
        )
        # Regenerate this constant ONLY with a layout migration.
        import hashlib
        import json

        payload = json.dumps(
            {"fn": "bandpass_filter", "inputs": {"signal": "RawSignal"}, "outputs": ["FilteredSignal"]},
            sort_keys=True,
            default=str,
        )
        assert wiring_id("bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {}) == (
            hashlib.sha256(payload.encode()).hexdigest()[:16]
        )
