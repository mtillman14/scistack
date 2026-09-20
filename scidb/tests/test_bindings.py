"""`scidb.bindings` — the typed spine for record ids (Stage 2a).

Pure: no database. The load-bearing assertion is that a `Binding` hashes
into `compute_invocation_id` as the same bytes the tuple it replaces did;
`test_identity_parity.py` pins the same thing end to end.
"""

from __future__ import annotations

import math

import pytest

from scidb.bindings import (
    COMBO_KEY,
    EMPTY_SIGNATURE,
    RID_PREFIX,
    Binding,
    InputBinding,
    InputKind,
    RecordPool,
    RunBindings,
    Selection,
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
    """The combo carries ONE key — ``COMBO_KEY``, the index of its
    ``Selection`` — and every reader (scifor's row filter, the graph edges,
    the skip gate) resolves through it. No per-input prefixed keys."""

    def _run(self):
        return RunBindings(
            {
                "value": InputBinding("value", InputKind.ITERATE, "Wide", '{"columns": ["a"]}'),
                "ref": InputBinding("ref", InputKind.PINNED, "Ref", pinned_rid="fixed1"),
                "cols": InputBinding("cols", InputKind.LINEAGE_ONLY, "Table"),
            }
        )

    def test_a_combo_names_its_selection_and_the_pin_is_appended(self):
        run = self._run()
        handle = run.add_selection(Selection({"value": ("r1",), "cols": ("c9",)}))
        combo = {"subject": "01", COMBO_KEY: handle}
        assert run.selection_of(combo) is run.selections[0]
        assert set(run.for_combo(combo)) == {
            Binding("value", "r1", '{"columns": ["a"]}'),
            Binding("cols", "c9", None),
            Binding("ref", "fixed1", None),
        }

    def test_a_pin_already_in_the_selection_is_not_doubled(self):
        run = self._run()
        handle = run.add_selection(Selection({"value": ("r1",), "ref": ("fixed1",)}))
        edges = run.for_combo({COMBO_KEY: handle})
        assert sum(1 for e in edges if e.param == "ref") == 1

    def test_the_handle_survives_a_result_row(self):
        """scifor writes the combo into the result row; the row's handle may
        come back as a float (pandas), a string (MATLAB), or an int."""
        run = self._run()
        handle = run.add_selection(Selection({"value": ("r1",)}))
        for spelled in (handle, float(handle), str(handle)):
            assert run.selection_of({COMBO_KEY: spelled}) is run.selections[0]
        assert run.selection_of({}) is None
        assert run.selection_of({COMBO_KEY: math.nan}) is None
        assert run.selection_of({COMBO_KEY: 99}) is None

    def test_aggregation_selection_names_the_group_and_every_pooled_record(self):
        run = self._run()
        run.iterated_keys = ("subject",)
        s20 = variant_signature({"bandpass.low_hz": 20})
        run["value"].kind = InputKind.AGGREGATED
        run["value"].pool = RecordPool(
            ("subject",), {("01",): {s20: VariantGroup(s20, ("r1", "r2"))}}
        )
        run["cols"].kind = InputKind.AGGREGATED
        run["cols"].pool = RecordPool(
            ("subject",),
            {("01",): {EMPTY_SIGNATURE: VariantGroup(EMPTY_SIGNATURE, ("c1",))}},
            split=False,
        )
        assert run.split_params == ["value"]
        # What expansion builds for the (subject=01, group s20) call:
        combo = {"subject": "01"}
        sel = Selection(
            {
                "value": tuple(run["value"].pool.rids_at(combo, s20)),
                "cols": tuple(run["cols"].pool.rids_at(combo)),
            },
            {"value": s20},
        )
        combo[COMBO_KEY] = run.add_selection(sel)
        edges = run.for_combo(combo)
        assert [e for e in edges if e.param == "value"] == [
            Binding("value", "r1", '{"columns": ["a"]}'),
            Binding("value", "r2", '{"columns": ["a"]}'),
        ]
        assert Binding("cols", "c1", None) in edges
        assert Binding("ref", "fixed1", None) in edges
        # A combo with no selection binds only the pin.
        assert run.rids_for_combo({"subject": "02"}) == {"ref": ["fixed1"]}

    def test_a_selection_with_no_records_is_not_an_edge(self):
        run = self._run()
        handle = run.add_selection(Selection({"value": ()}))
        assert {e.param for e in run.for_combo({COMBO_KEY: handle})} == {"ref"}

    def test_kinds_are_queryable(self):
        run = self._run()
        assert [b.param for b in run.of_kind(InputKind.PINNED)] == ["ref"]
        assert run["value"].column == "__rid_value"
        assert "cols" in run and "nope" not in run
        assert run.pinned_rids == {"ref": "fixed1"}
        assert run.tracked_columns == ["__rid_value"]

    def test_pin_binds_a_late_resolved_fixed_input(self):
        """A Fixed pin Step 12 could not settle is looked up at save time,
        AFTER the selections were built — so it is appended, not baked in."""
        run = self._run()
        handle = run.add_selection(Selection({"value": ("r1",)}))
        run.pin("ref", "fixed2")
        run.pin("extra", "e1")
        assert run["ref"].pinned_rid == "fixed2"
        assert run["extra"].kind == InputKind.PINNED
        assert {e.param: e.rid for e in run.for_combo({COMBO_KEY: handle})} == {
            "value": "r1",
            "ref": "fixed2",
            "extra": "e1",
        }

    def test_branch_params_merge_across_the_edges(self):
        run = self._run()
        run.rid_to_bp = {"r1": {"bandpass.low_hz": 20}, "fixed1": {"ref.side": "L"}}
        handle = run.add_selection(Selection({"value": ("r1",)}))
        merged, conflicts = run.branch_params_for(run.for_combo({COMBO_KEY: handle}))
        assert merged == {"bandpass.low_hz": 20, "ref.side": "L"}
        assert conflicts == {}

    def test_selection_with_rid_is_a_copy(self):
        base = Selection({"value": ("r1",)})
        more = base.with_rid("cols", "c1").with_rid("cols", "c2")
        assert base.rids == {"value": ("r1",)}
        assert more.rids == {"value": ("r1",), "cols": ("c1", "c2")}
        assert more.all_rids() == {"r1", "c1", "c2"}


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

    def test_split_reads_the_group_named(self):
        pool, s20, s30 = self._pool()
        assert pool.rids_at({"subject": "01"}, s20) == ["a", "b"]
        assert pool.rids_at({"subject": "01"}, s30) == ["c"]
        assert pool.rids_at({"subject": "02"}, s30) == []
        assert pool.rids_at({"subject": "01"}) == []  # no group named: the empty one
        assert pool.signatures() == [s20, s30]

    def test_pooled_reads_every_group(self):
        pool, s20, s30 = self._pool(split=False)
        assert pool.rids_at({"subject": "01"}) == ["a", "b", "c"]

    def test_a_coarse_input_is_found_beneath_its_location(self):
        """Keyed by the iterated keys the input POPULATES: a subject-level
        input under a per-session aggregation serves every session."""
        pool, s20, _ = self._pool()
        assert pool.rids_at({"subject": "01", "session": "3"}, s20) == ["a", "b"]

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
