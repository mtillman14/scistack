"""`scidb.bindings` — the typed spine for record ids (Stage 2a).

Pure: no database. The load-bearing assertion is that a `Binding` hashes
into `compute_invocation_id` as the same bytes the tuple it replaces did;
`test_identity_parity.py` pins the same thing end to end.
"""

from __future__ import annotations

import math

import pytest

from scidb.bindings import (
    EMPTY_SIGNATURE,
    RID_PREFIX,
    Binding,
    InputBinding,
    InputKind,
    RecordPool,
    RunBindings,
    VariantGroup,
    is_internal_column,
    is_rid_column,
    merge_branch_params,
    param_of,
    param_of_vsig,
    rid_column,
    rid_columns,
    signature_conflicts_with,
    variant_signature,
    vsig_column,
)
from scidb.provenance import compute_invocation_id


class TestSpelling:
    def test_the_prefix_is_spelled_here_and_nowhere_else(self):
        assert rid_column("value") == f"{RID_PREFIX}value" == "__rid_value"

    def test_param_of_reads_either_spelling(self):
        assert param_of("__rid_value") == "value"
        assert param_of("value") == "value"
        assert param_of(None) == "None"  # str() of anything, never an error

    def test_rid_columns_picks_the_rid_columns_in_order(self):
        assert rid_columns(["subject", "__rid_b", "a", "__rid_a"]) == ["__rid_b", "__rid_a"]
        assert not is_rid_column("__vsig_value")
        assert is_internal_column("__vsig_value") and is_internal_column("__rid_value")
        assert not is_internal_column("__record_id")


class TestBinding:
    def test_iterates_as_the_three_tuple(self):
        assert tuple(Binding("p", "r", None)) == ("p", "r", None)

    def test_coerce_accepts_both_tuple_arities_and_itself(self):
        b = Binding("p", "r", "sel")
        assert Binding.coerce(b) is b
        assert Binding.coerce(("p", "r")) == Binding("p", "r", None)
        assert Binding.coerce(("p", "r", "sel")) == b
        with pytest.raises(ValueError):
            Binding.coerce(("p",))

    def test_same_bytes_as_the_tuple_it_replaces(self):
        tuples = [("value", "r1", '{"columns": ["a"]}'), ("scale", "c1")]
        bindings = [Binding("value", "r1", '{"columns": ["a"]}'), Binding("scale", "c1")]
        assert compute_invocation_id("h", None, False, tuples) == compute_invocation_id(
            "h", None, False, bindings
        )


class TestRunBindings:
    def _run(self):
        return RunBindings(
            {
                "value": InputBinding("value", InputKind.ITERATE, "Wide", '{"columns": ["a"]}'),
                "ref": InputBinding("ref", InputKind.PINNED, "Ref", pinned_rid="fixed1"),
                "cols": InputBinding("cols", InputKind.LINEAGE_ONLY, "Table"),
            }
        )

    def test_full_iteration_reads_the_combo_and_appends_the_pin(self):
        edges = self._run().for_combo({"subject": "01", "__rid_value": "r1", "__rid_cols": "c9"})
        assert set(edges) == {
            Binding("value", "r1", '{"columns": ["a"]}'),
            Binding("cols", "c9", None),
            Binding("ref", "fixed1", None),
        }

    def test_a_pin_already_on_the_combo_is_not_doubled(self):
        edges = self._run().for_combo({"__rid_ref": "fixed1", "__rid_value": "r1"})
        assert sum(1 for e in edges if e.param == "ref") == 1

    def test_aggregation_reads_the_pool_under_the_real_name(self):
        run = self._run()
        run.iterated_keys = ("subject",)
        run["value"].kind = InputKind.AGGREGATED
        run["value"].pool = RecordPool(
            ("subject",),
            {("01",): {EMPTY_SIGNATURE: VariantGroup(EMPTY_SIGNATURE, ("r1", "r2"))}},
        )
        run["cols"].kind = InputKind.AGGREGATED
        run["cols"].pool = RecordPool(
            ("subject",),
            {("01",): {EMPTY_SIGNATURE: VariantGroup(EMPTY_SIGNATURE, ("c1",))}},
            split=False,
        )
        assert run.split_params == ["value"] and run.vsig_columns == ["__vsig_value"]
        edges = run.for_combo({"subject": "01", "__vsig_value": EMPTY_SIGNATURE})
        assert [e for e in edges if e.param == "value"] == [
            Binding("value", "r1", '{"columns": ["a"]}'),
            Binding("value", "r2", '{"columns": ["a"]}'),
        ]
        assert Binding("cols", "c1", None) in edges
        assert Binding("ref", "fixed1", None) in edges
        # A location with no data binds only the pin.
        assert run.rids_for_combo({"subject": "02"}) == {"ref": ["fixed1"]}

    def test_a_nan_or_none_rid_is_not_an_edge(self):
        edges = self._run().for_combo({"__rid_value": math.nan, "__rid_cols": None})
        assert {e.param for e in edges} == {"ref"}

    def test_kinds_are_queryable(self):
        run = self._run()
        assert [b.param for b in run.of_kind(InputKind.PINNED)] == ["ref"]
        assert run["value"].column == "__rid_value"
        assert "cols" in run and "nope" not in run
        assert run.pinned_rids == {"ref": "fixed1"}

    def test_pin_binds_a_late_resolved_fixed_input(self):
        run = self._run()
        run.pin("ref", "fixed2")
        run.pin("extra", "e1")
        assert run["ref"].pinned_rid == "fixed2"
        assert run["extra"].kind == InputKind.PINNED
        assert {e.param: e.rid for e in run.for_combo({})} == {"ref": "fixed2", "extra": "e1"}

    def test_branch_params_merge_across_the_edges(self):
        run = self._run()
        run.rid_to_bp = {"r1": {"bandpass.low_hz": 20}, "fixed1": {"ref.side": "L"}}
        merged, conflicts = run.branch_params_for(run.for_combo({"__rid_value": "r1"}))
        assert merged == {"bandpass.low_hz": 20, "ref.side": "L"}
        assert conflicts == {}


# ---------------------------------------------------------------------------
# Variant groups: the aggregation selection
# ---------------------------------------------------------------------------


class TestVariantSignature:
    def test_one_recipe_on_both_sides_of_a_json_round_trip(self):
        import json

        bp = {"scale.k": (1, 2), "bandpass.low_hz": 20}
        loaded = json.loads(json.dumps(bp))  # what the save path sees
        assert variant_signature(bp) == variant_signature(loaded)
        assert variant_signature(None) == variant_signature({}) == EMPTY_SIGNATURE

    def test_key_order_does_not_matter(self):
        assert variant_signature({"a": 1, "b": 2}) == variant_signature({"b": 2, "a": 1})

    def test_vsig_column_spelling_has_one_owner(self):
        assert vsig_column("value") == "__vsig_value"
        assert param_of_vsig("__vsig_value") == "value" == param_of_vsig("value")

    def test_save_kwarg_alignment(self):
        sig = variant_signature({"__save__.side": "left", "bandpass.low_hz": 20})
        assert signature_conflicts_with(sig, {"subject": "01", "side": "right"})
        assert not signature_conflicts_with(sig, {"subject": "01", "side": "left"})
        assert not signature_conflicts_with(sig, {"subject": "01"})  # side not iterated
        assert not signature_conflicts_with(EMPTY_SIGNATURE, {"side": "right"})


class TestRecordPool:
    def _pool(self, split=True):
        s20 = variant_signature({"bandpass.low_hz": 20})
        s30 = variant_signature({"bandpass.low_hz": 30})
        pool = RecordPool(
            ("subject",),
            {
                ("01",): {s20: VariantGroup(s20, ("a", "b")), s30: VariantGroup(s30, ("c",))},
                ("02",): {s20: VariantGroup(s20, ("d",))},
            },
            split=split,
        )
        return pool, s20, s30

    def test_split_reads_the_group_the_combo_names(self):
        pool, s20, s30 = self._pool()
        assert pool.rids_at({"subject": "01", "__vsig_value": s20}, "value") == ["a", "b"]
        assert pool.rids_at({"subject": "01", "__vsig_value": s30}, "value") == ["c"]
        assert pool.rids_at({"subject": "02", "__vsig_value": s30}, "value") == []
        assert pool.signatures() == [s20, s30]

    def test_pooled_reads_every_group(self):
        pool, s20, s30 = self._pool(split=False)
        assert pool.rids_at({"subject": "01"}, "value") == ["a", "b", "c"]

    def test_a_coarse_input_is_found_beneath_its_location(self):
        """Keyed by the iterated keys the input POPULATES: a subject-level
        input under a per-session aggregation serves every session."""
        pool, s20, _ = self._pool()
        combo = {"subject": "01", "session": "3", "__vsig_value": s20}
        assert pool.rids_at(combo, "value") == ["a", "b"]

    def test_a_group_knows_its_branch_params(self):
        _, s20, _ = self._pool()
        assert VariantGroup(s20, ("a",)).branch_params == {"bandpass.low_hz": 20}


class TestSignatureHasOneRecipe:
    """`variant_signature` replaced five hand-rolled
    `json.dumps(..., sort_keys=True)` calls (the collapse key, the PathOutput
    `{variant}` text, the aggregation split, the predictor, the GUI's variant
    summary). They had drifted — see
    `test_pathoutput_variants.py::TestVariantTokenIsOneDigest`."""

    def test_a_non_serialisable_value_does_not_raise(self):
        from datetime import date

        assert variant_signature({"d": date(2026, 9, 20)})  # default=str

    def test_nested_dicts_sort_too(self):
        assert variant_signature({"cfg": {"b": 1, "a": 2}}) == variant_signature(
            {"cfg": {"a": 2, "b": 1}}
        )


class TestMergeReportsEveryValue:
    def test_conflicts_name_the_key_and_every_value_seen(self):
        merged, conflicts = merge_branch_params([{"a": 1}, {"a": 2, "b": 3}, {"a": 4}])
        assert merged == {"a": 4, "b": 3}
        assert conflicts == {"a": [1, 2, 4]}

    def test_agreement_is_not_a_conflict(self):
        merged, conflicts = merge_branch_params([{"a": 1}, {"a": 1}])
        assert merged == {"a": 1} and conflicts == {}


# ---------------------------------------------------------------------------
# input_spec: ONE unwrap
# ---------------------------------------------------------------------------


class TestInputSpecUnwrap:
    """Six hand-rolled unwraps, each knowing a different subset of the
    wrappers, are why a `Variant`-pinned input vanished from the predicted
    config and a column selection under a `Variant` reached the graph as
    "no selection"."""

    def _types(self):
        from scidb import AcrossVariants, BaseVariable, Variant
        from scifor import Fixed

        class Wide(BaseVariable):
            pass

        return Wide, Variant, AcrossVariants, Fixed

    def test_every_wrapper_peels_in_any_order(self):
        from scidb.input_spec import type_name

        Wide, Variant, AcrossVariants, Fixed = self._types()
        for spec in (
            Wide,
            Wide["a"],
            Fixed(Wide, subject="01"),
            Variant(Wide, low_hz=20),
            AcrossVariants(Wide),
            AcrossVariants(Variant(Fixed(Wide["a"], subject="01"), low_hz=20)),
            Variant(AcrossVariants(Wide), low_hz=20),
        ):
            assert type_name(spec) == "Wide", spec

    def test_a_non_variable_has_no_type(self):
        from scifor import PathInput

        from scidb.input_spec import type_name, variable_type

        assert type_name(20) is None
        assert type_name("hello") is None
        assert variable_type(PathInput("{subject}/a.csv")) is None

    def test_find_wrapper_reaches_a_selection_under_any_wrapper(self):
        from scifor import ColumnSelection, Fixed

        from scidb.input_spec import find_wrapper

        Wide, Variant, AcrossVariants, _Fixed = self._types()
        for spec in (
            Wide["a"],
            Fixed(Wide["a"], subject="01"),
            Variant(Wide["a"], low_hz=20),
        ):
            found = find_wrapper(spec, ColumnSelection)
            assert found is not None and list(found.columns) == ["a"], spec
        assert find_wrapper(Wide, ColumnSelection) is None
        assert find_wrapper(Variant(Wide, low_hz=20), ColumnSelection) is None

    def test_wrappers_of_reports_the_stack(self):
        from scidb.input_spec import wrappers_of

        Wide, Variant, AcrossVariants, Fixed = self._types()
        assert set(wrappers_of(Variant(Fixed(Wide, subject="01"), low_hz=20))) == {
            Variant,
            Fixed,
        }
        assert wrappers_of(Wide) == []
