"""
Unit tests for scistack_gui.domain.variant_resolver.

All functions are pure — no DB or fixtures required.
"""

from scistack_gui.domain.variant_resolver import (
    build_inferred_variants,
    build_schema_kwargs,
    compute_call_id,
    deduplicate_variants,
    reconcile_manual_inputs,
    filter_hidden_constant_value_targets,
    filter_hidden_targets,
    filter_variants,
    hidden_call_ids_for_fn,
    merge_pending_constants,
    resolve_target_call_id,
)

# ---------------------------------------------------------------------------
# build_inferred_variants
# ---------------------------------------------------------------------------


class TestBuildInferredVariants:
    def test_no_constants_single_output(self):
        result = build_inferred_variants(
            input_types={"signal": ["RawEMG"]},
            output_types=["Filtered"],
            inferred_constants={},
        )
        assert result == [
            {
                "input_types": {"signal": ["RawEMG"]},
                "output_type": "Filtered",
                "constants": {},
            }
        ]

    def test_no_constants_multiple_outputs(self):
        result = build_inferred_variants(
            input_types={"signal": ["Raw"]},
            output_types=["A", "B"],
            inferred_constants={},
        )
        assert len(result) == 2
        output_types = {v["output_type"] for v in result}
        assert output_types == {"A", "B"}
        assert all(v["constants"] == {} for v in result)

    def test_single_constant_cross_products_with_outputs(self):
        result = build_inferred_variants(
            input_types={},
            output_types=["Out"],
            inferred_constants={"low_hz": [10, 20]},
        )
        assert len(result) == 2
        constant_values = {v["constants"]["low_hz"] for v in result}
        assert constant_values == {10, 20}

    def test_two_constants_full_cross_product(self):
        result = build_inferred_variants(
            input_types={},
            output_types=["Out"],
            inferred_constants={"a": [1, 2], "b": ["x", "y"]},
        )
        assert len(result) == 4
        combos = {(v["constants"]["a"], v["constants"]["b"]) for v in result}
        assert combos == {(1, "x"), (1, "y"), (2, "x"), (2, "y")}

    def test_constants_with_multiple_outputs(self):
        result = build_inferred_variants(
            input_types={},
            output_types=["A", "B"],
            inferred_constants={"k": [1, 2]},
        )
        # 2 constant values × 2 outputs = 4
        assert len(result) == 4

    def test_empty_output_types_returns_empty(self):
        result = build_inferred_variants(
            input_types={"x": ["T"]},
            output_types=[],
            inferred_constants={},
        )
        assert result == []

    def test_input_types_preserved_in_all_variants(self):
        inputs = {"signal": ["Raw"], "ref": ["Ref"]}
        result = build_inferred_variants(
            input_types=inputs,
            output_types=["Out"],
            inferred_constants={"k": [1, 2]},
        )
        for v in result:
            assert v["input_types"] is inputs


# ---------------------------------------------------------------------------
# filter_variants
# ---------------------------------------------------------------------------


class TestFilterVariants:
    def _make_variants(self, const_dicts):
        return [
            {"input_types": {}, "output_type": "Out", "constants": c}
            for c in const_dicts
        ]

    def test_exact_match(self):
        variants = self._make_variants([{"hz": 10}, {"hz": 20}])
        result = filter_variants(variants, selected_variants=[{"hz": 10}])
        assert len(result) == 1
        assert result[0]["constants"]["hz"] == 10

    def test_no_match_returns_all(self):
        variants = self._make_variants([{"hz": 10}, {"hz": 20}])
        result = filter_variants(variants, selected_variants=[{"hz": 99}])
        assert result == variants

    def test_multiple_selected_matches_each(self):
        variants = self._make_variants([{"hz": 10}, {"hz": 20}, {"hz": 30}])
        result = filter_variants(variants, selected_variants=[{"hz": 10}, {"hz": 30}])
        assert len(result) == 2
        values = {v["constants"]["hz"] for v in result}
        assert values == {10, 30}

    def test_string_value_matching(self):
        # selected values stored as strings should still match typed values.
        variants = self._make_variants([{"hz": 10}])
        result = filter_variants(variants, selected_variants=[{"hz": "10"}])
        assert len(result) == 1

    def test_subset_matching(self):
        # selected is a subset of constants in each variant.
        variants = self._make_variants([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        result = filter_variants(variants, selected_variants=[{"a": 1}])
        assert len(result) == 1
        assert result[0]["constants"]["a"] == 1


# ---------------------------------------------------------------------------
# deduplicate_variants
# ---------------------------------------------------------------------------


class TestDeduplicateVariants:
    def _make(self, consts):
        return [
            {"input_types": {}, "output_type": "Out", "constants": c} for c in consts
        ]

    def test_no_duplicates_unchanged(self):
        variants = self._make([{"hz": 10}, {"hz": 20}])
        result = deduplicate_variants(variants)
        assert len(result) == 2

    def test_exact_duplicate_removed(self):
        variants = self._make([{"hz": 10}, {"hz": 10}])
        result = deduplicate_variants(variants)
        assert len(result) == 1

    def test_first_occurrence_kept(self):
        v1 = {"input_types": {}, "output_type": "A", "constants": {"hz": 10}}
        v2 = {"input_types": {}, "output_type": "B", "constants": {"hz": 10}}
        result = deduplicate_variants([v1, v2])
        assert result == [v1]

    def test_empty_list(self):
        assert deduplicate_variants([]) == []

    def test_empty_constants_deduplicated(self):
        variants = self._make([{}, {}])
        result = deduplicate_variants(variants)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# merge_pending_constants
# ---------------------------------------------------------------------------


class TestMergePendingConstants:
    def _make(self, const_dicts, out="Out"):
        return [
            {"input_types": {"x": ["T"]}, "output_type": out, "constants": c}
            for c in const_dicts
        ]

    def test_new_pending_value_added(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {"hz": {"20"}})
        const_values = {v["constants"]["hz"] for v in result}
        assert 20 in const_values or "20" in const_values

    def test_existing_value_not_duplicated(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {"hz": {"10"}})
        hz_values = [v["constants"]["hz"] for v in result]
        assert hz_values.count(10) + hz_values.count("10") == 1

    def test_pending_constant_not_in_fn_ignored(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {"other_param": {"99"}})
        assert len(result) == 1

    def test_empty_pending_returns_unchanged(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {})
        assert result == variants

    def test_empty_variants_returns_unchanged(self):
        result = merge_pending_constants([], {"hz": {"20"}})
        assert result == []

    def test_cross_product_with_other_constants(self):
        # hz: [10, 20] already exist; pending scale: ["2"]
        # Should add 2 new variants (one per existing hz value).
        variants = self._make([{"hz": 10, "scale": 1}, {"hz": 20, "scale": 1}])
        result = merge_pending_constants(variants, {"scale": {"2"}})
        scales = [v["constants"]["scale"] for v in result]
        assert 2 in scales or "2" in scales
        # The new scale value should pair with each hz value.
        new_variants = [v for v in result if str(v["constants"]["scale"]) == "2"]
        hz_vals = {v["constants"]["hz"] for v in new_variants}
        assert hz_vals == {10, 20}

    def test_coercion_numeric_string_to_int(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {"hz": {"42"}})
        new = [v for v in result if v["constants"]["hz"] != 10]
        assert len(new) == 1
        assert new[0]["constants"]["hz"] == 42

    def test_coercion_non_numeric_stays_string(self):
        variants = self._make([{"mode": "fast"}])
        result = merge_pending_constants(variants, {"mode": {"slow"}})
        new = [v for v in result if v["constants"]["mode"] != "fast"]
        assert new[0]["constants"]["mode"] == "slow"


# ---------------------------------------------------------------------------
# compute_call_id
# ---------------------------------------------------------------------------


class TestComputeCallId:
    def _target(self, constants=None, input_types=None):
        types = input_types or {"signal": "RawEMG"}
        return {
            # Identity reads `bindings`; `input_types` rides along as the
            # display view of the same variable bindings.
            "bindings": {
                p: {"kind": "variable", "ref": t if isinstance(t, list) else [t]}
                for p, t in types.items()
            },
            "input_types": types,
            "output_type": "Out",
            "constants": constants or {"hz": 10},
        }

    @staticmethod
    def _forward(**options):
        """scidb's own forward id for the same call — the contract is
        `compute_call_id == ForEachConfig.to_call_id`, not a hand-built
        payload that could drift along with the code under test."""
        from scidb import BaseVariable
        from scidb.foreach_config import ForEachConfig

        class RawEMG(BaseVariable):
            pass

        def bandpass_filter(signal, hz):
            return signal

        return ForEachConfig(bandpass_filter, {"signal": RawEMG, "hz": 10}, **options).to_call_id()

    def test_matches_real_call_id(self):
        assert compute_call_id("bandpass_filter", self._target()) == self._forward()

    def test_distribute_false_matches_distribute_omitted(self):
        assert compute_call_id(
            "fn", self._target(), distribute=False
        ) == compute_call_id("fn", self._target())

    def test_distribute_true_changes_id(self):
        assert compute_call_id(
            "fn", self._target(), distribute=True
        ) != compute_call_id("fn", self._target(), distribute=False)

    def test_pooled_binding_matches_scidb_forward_call_id(self):
        """`pool_variants` on a binding is scidb's `AcrossVariants(X)` at the
        call site: the GUI's id must equal `ForEachConfig.to_call_id`, and
        differ from the split call site's."""
        from scidb import AcrossVariants, BaseVariable
        from scidb.foreach_config import ForEachConfig

        class RawEMG(BaseVariable):
            pass

        def bandpass_filter(signal, hz):
            return signal

        target = self._target()
        target["bindings"]["signal"]["pool_variants"] = True
        forward = ForEachConfig(
            bandpass_filter, {"signal": AcrossVariants(RawEMG), "hz": 10}
        ).to_call_id()
        assert compute_call_id("bandpass_filter", target) == forward
        assert compute_call_id("bandpass_filter", self._target()) != forward

    def test_as_table_list_is_order_independent(self):
        assert compute_call_id(
            "fn", self._target(), as_table=["b", "a"]
        ) == compute_call_id("fn", self._target(), as_table=["a", "b"])

    def test_as_table_matches_real_call_id(self):
        result = compute_call_id("bandpass_filter", self._target(), as_table=["signal"])
        assert result == self._forward(as_table=["signal"])

    def test_as_table_true_matches_real_call_id(self):
        """`True` resolves to every loadable input on BOTH sides. It hashed
        as the literal `True` here and as the resolved names in scidb until
        2026-09-20, so this call site never matched its own records."""
        result = compute_call_id("bandpass_filter", self._target(), as_table=True)
        assert result == self._forward(as_table=True)
        assert result == self._forward(as_table=["signal"])

    def test_distribute_matches_real_call_id(self):
        result = compute_call_id("bandpass_filter", self._target(), distribute=True)
        assert result == self._forward(distribute=True)

    def test_multi_type_input_returns_none(self):
        target = self._target(input_types={"signal": ["RawEMG", "RawEEG"]})
        assert compute_call_id("fn", target) is None

    def test_single_item_list_input_resolved_not_none(self):
        target = self._target(input_types={"signal": ["RawEMG"]})
        assert compute_call_id("fn", target) is not None
        assert compute_call_id("fn", target) == compute_call_id(
            "fn", self._target(input_types={"signal": "RawEMG"})
        )


# ---------------------------------------------------------------------------
# hidden_call_ids_for_fn
# ---------------------------------------------------------------------------


class TestHiddenCallIdsForFn:
    # parse_fn_node_id only recognizes a 16-hex-char suffix as a call_id
    # (shorter/non-hex suffixes are treated as legacy manual ids and
    # silently ignored) -- these must be realistic 16-hex ids or the
    # parse itself (not the function-name filter) would swallow them.
    _CID_A = "0123456789abcdef"
    _CID_B = "fedcba9876543210"

    def test_filters_to_matching_function(self):
        hidden = {
            f"fn__bandpass_filter__{self._CID_A}",
            f"fn__other_fn__{self._CID_B}",
        }
        assert hidden_call_ids_for_fn(hidden, "bandpass_filter") == {self._CID_A}

    def test_no_matches_returns_empty(self):
        hidden = {f"fn__other_fn__{self._CID_B}"}
        assert hidden_call_ids_for_fn(hidden, "bandpass_filter") == set()

    def test_ignores_non_fn_ids(self):
        hidden = {"var__Filtered", "param__hz", "fn__bandpass_filter"}
        assert hidden_call_ids_for_fn(hidden, "bandpass_filter") == set()

    def test_ignores_non_hex_short_suffix(self):
        """A random 6-char manual-node suffix (not a real call_id) must
        not be mistaken for one."""
        hidden = {"fn__bandpass_filter__abc123"}
        assert hidden_call_ids_for_fn(hidden, "bandpass_filter") == set()


# ---------------------------------------------------------------------------
# resolve_target_call_id / filter_hidden_targets
# ---------------------------------------------------------------------------


class TestFilterHiddenTargets:
    def _target(self, constants, call_id=None, input_types=None):
        t = {
            "input_types": input_types or {"signal": "RawEMG"},
            "output_type": "Out",
            "constants": constants,
        }
        if call_id is not None:
            t["call_id"] = call_id
        return t

    def test_no_hidden_ids_returns_unchanged(self):
        targets = [self._target({"hz": 10}, call_id="abc")]
        assert filter_hidden_targets(targets, "fn", set(), {}) == targets

    def test_real_call_id_hidden_dropped(self):
        targets = [self._target({"hz": 10}, call_id="abc")]
        assert filter_hidden_targets(targets, "fn", {"abc"}, {}) == []

    def test_real_call_id_not_hidden_kept(self):
        targets = [self._target({"hz": 10}, call_id="abc")]
        assert len(filter_hidden_targets(targets, "fn", {"other"}, {})) == 1

    def test_overridden_target_never_trusts_stale_call_id(self):
        # constants say hz=20 but the call_id field is left over from
        # before an override touched it (still says the hz=10 combo) —
        # hiding by the stale id must NOT match; only the freshly
        # recomputed id (matching the CURRENT constants) should.
        target = self._target({"hz": 20}, call_id="stale-id-for-hz-10")
        fresh_id = compute_call_id("fn", target)

        kept = filter_hidden_targets(
            [target], "fn", {"stale-id-for-hz-10"}, {"hz": {"20"}}
        )
        assert kept == [target]

        dropped = filter_hidden_targets([target], "fn", {fresh_id}, {"hz": {"20"}})
        assert dropped == []

    def test_untouched_target_reuses_real_call_id_not_recomputed(self):
        # constants weren't touched by any pending override -> the real
        # call_id is trusted as-is, even if it wouldn't match a freshly
        # computed hash (simulating legacy/out-of-band call_ids).
        target = self._target({"hz": 10}, call_id="legacy-id")
        assert filter_hidden_targets([target], "fn", {"legacy-id"}, {}) == []

    def test_never_run_combo_hidden_via_computed_id(self):
        target = self._target({"hz": 10})  # no call_id at all yet
        cid = compute_call_id("fn", target)
        assert filter_hidden_targets([target], "fn", {cid}, {}) == []

    def test_unresolvable_multitype_target_never_filtered(self):
        target = self._target({"hz": 10}, input_types={"signal": ["A", "B"]})
        result = filter_hidden_targets([target], "fn", {"anything"}, {})
        assert result == [target]

    def test_resolve_target_call_id_matches_filter_behavior(self):
        target = self._target({"hz": 10}, call_id="abc")
        assert resolve_target_call_id("fn", target, set()) == "abc"
        assert (
            resolve_target_call_id("fn", target, {"hz"}) == compute_call_id("fn", target)
        )


class TestFilterHiddenConstantValueTargets:
    """Coarser than filter_hidden_targets: hides every target using
    (const_name, value), across every function, by direct content match —
    no call_id hashing (see filter_hidden_constant_value_targets docstring
    for why that's sufficient, including for never-run combos)."""

    def _target(self, constants):
        return {"input_types": {"signal": "RawEMG"}, "output_type": "Out", "constants": constants}

    def test_no_hidden_values_returns_unchanged(self):
        targets = [self._target({"hz": 10})]
        assert filter_hidden_constant_value_targets(targets, {}) == targets

    def test_matching_value_dropped(self):
        targets = [self._target({"hz": 10})]
        assert filter_hidden_constant_value_targets(targets, {"hz": {"10"}}) == []

    def test_non_matching_value_kept(self):
        targets = [self._target({"hz": 10})]
        result = filter_hidden_constant_value_targets(targets, {"hz": {"99"}})
        assert result == targets

    def test_string_coercion_matches_non_string_constant(self):
        # constants dicts hold typed values (e.g. int 10); hidden_values are
        # always strings (persisted that way) -- comparison must coerce.
        targets = [self._target({"hz": 10})]
        assert filter_hidden_constant_value_targets(targets, {"hz": {"10"}}) == []

    def test_other_constant_name_unaffected(self):
        targets = [self._target({"hz": 10})]
        result = filter_hidden_constant_value_targets(targets, {"other": {"10"}})
        assert result == targets

    def test_hides_across_multiple_functions_worth_of_targets(self):
        targets = [
            {"input_types": {}, "output_type": "A", "constants": {"hz": 10}},
            {"input_types": {}, "output_type": "B", "constants": {"hz": 10}},
            {"input_types": {}, "output_type": "C", "constants": {"hz": 20}},
        ]
        result = filter_hidden_constant_value_targets(targets, {"hz": {"10"}})
        assert result == [targets[2]]

    def test_target_missing_the_constant_entirely_is_kept(self):
        targets = [{"input_types": {}, "output_type": "A", "constants": {}}]
        assert filter_hidden_constant_value_targets(targets, {"hz": {"10"}}) == targets

    def test_hidden_value_is_matched_by_the_declared_name(self):
        """``constants`` is keyed by the FUNCTION PARAMETER; hidden_values by
        the Parameter node's DECLARED name. When they differ, comparing them
        directly matches nothing and every unchecked value runs anyway —
        the target's Parameter binding is the translation."""
        targets = [
            {
                "input_types": {},
                "output_type": "A",
                "constants": {"window": 10},
                "bindings": {"window": {"kind": "parameter", "ref": "test"}},
            }
        ]
        assert filter_hidden_constant_value_targets(targets, {"test": {"10"}}) == []

    def test_parameter_name_does_not_match_when_declaration_differs(self):
        """The converse: hiding under the PARAMETER's name must not fire when
        the declaration is called something else — that value belongs to a
        different Parameter node's checkbox."""
        targets = [
            {
                "input_types": {},
                "output_type": "A",
                "constants": {"window": 10},
                "bindings": {"window": {"kind": "parameter", "ref": "test"}},
            }
        ]
        result = filter_hidden_constant_value_targets(targets, {"window": {"10"}})
        assert result == targets

    def test_missing_parameter_binding_falls_back_to_the_param_name(self):
        """DB-history targets carry no bindings; there the two names are the
        same string by construction."""
        targets = [{"input_types": {}, "output_type": "A", "constants": {"hz": 10}}]
        assert filter_hidden_constant_value_targets(targets, {"hz": {"10"}}) == []


class TestReconcileManualInputsHiddenEdges:
    def _target(self, input_types=None, output_type="Out", constants=None):
        # `or` would silently treat an explicitly-passed {} as "use the
        # default" (empty dict is falsy) — tests below rely on {} being
        # respected as-is (e.g. a constant-only target with no var inputs).
        types = {"signal": "RawEMG"} if input_types is None else input_types
        return {
            "bindings": {
                p: {"kind": "variable", "ref": t if isinstance(t, list) else [t]}
                for p, t in types.items()
            },
            "input_types": types,
            "output_type": output_type,
            "constants": constants or {},
        }

    def test_no_hidden_edges_returns_unchanged(self):
        targets = [self._target()]
        assert reconcile_manual_inputs(targets, "fn", set()) == targets

    def test_disconnected_var_input_dropped(self):
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": "RawEMG"})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        hidden = {f"e__RawEMG__fn__{wid}"}
        assert reconcile_manual_inputs([target], "fn", hidden) == []

    def test_disconnected_constant_input_dropped(self):
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({}, constants={"low_hz": 20})
        wid = wiring_id("fn", {}, {"Out"}, {})
        hidden = {f"e__low_hz__fn__{wid}"}
        assert reconcile_manual_inputs([target], "fn", hidden) == []

    def test_unrelated_hidden_edge_keeps_target(self):
        target = self._target({"signal": "RawEMG"})
        assert reconcile_manual_inputs([target], "fn", {"e__Other__fn__deadbeef"}) == [
            target
        ]

    def test_every_variant_of_disconnected_wiring_dropped_not_just_one(self):
        # Two constant-value variants of the SAME wiring — disconnecting
        # the shared var input drops the WHOLE wiring, not one combo.
        from scistack_gui.domain.graph_builder import wiring_id

        t1 = self._target({"signal": "RawEMG"}, constants={"hz": 10})
        t2 = self._target({"signal": "RawEMG"}, constants={"hz": 20})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        hidden = {f"e__RawEMG__fn__{wid}"}
        assert reconcile_manual_inputs([t1, t2], "fn", hidden) == []

    def test_different_wiring_of_same_function_name_unaffected(self):
        # compute_rolling_vo2 fed by RawVO2 in one wiring, RawHeartRate in
        # another — disconnecting one must not touch the other.
        from scistack_gui.domain.graph_builder import wiring_id

        vo2 = self._target({"signal": "RawVO2"})
        hr = self._target({"signal": "RawHeartRate"})
        wid_vo2 = wiring_id("fn", {"signal": "RawVO2"}, {"Out"}, {})
        hidden = {f"e__RawVO2__fn__{wid_vo2}"}
        assert reconcile_manual_inputs([vo2, hr], "fn", hidden) == [hr]

    def test_multitype_input_list_checked_per_element(self):
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": ["A", "B"]})
        wid = wiring_id("fn", {"signal": ["A", "B"]}, {"Out"}, {})
        hidden = {f"e__B__fn__{wid}"}
        assert reconcile_manual_inputs([target], "fn", hidden) == []

    def test_empty_targets_returns_empty(self):
        assert reconcile_manual_inputs([], "fn", {"anything"}) == []

    def test_manual_reconnect_substitutes_new_input_type(self):
        # A manual edge onto the SAME handle a hidden inbound edge fed,
        # with a DIFFERENT source variable, must keep the target but with
        # its input_types substituted — and drop any stale call_id.
        from scistack_gui.domain.graph_builder import fn_node_id, wiring_id

        target = self._target({"signal": "RawEMG"})
        target["call_id"] = "stale0000000000"
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        hidden = {f"e__RawEMG__fn__{wid}"}
        manual_edges = [
            {
                "target": fn_node_id("fn", wid),
                "targetHandle": "in__signal",
                "source": "var__OtherEMG",
            }
        ]
        result = reconcile_manual_inputs([target], "fn", hidden, manual_edges)
        assert result == [
            {
                "bindings": {"signal": {"kind": "variable", "ref": ["OtherEMG"]}},
                # The view stays in step with the bindings it views.
                "input_types": {"signal": "OtherEMG"},
                "output_type": "Out",
                "constants": {},
            }
        ]
        assert "call_id" not in result[0]

    def test_manual_reconnect_to_different_handle_still_drops(self):
        from scistack_gui.domain.graph_builder import fn_node_id, wiring_id

        target = self._target({"signal": "RawEMG"})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        hidden = {f"e__RawEMG__fn__{wid}"}
        manual_edges = [
            {
                "target": fn_node_id("fn", wid),
                "targetHandle": "in__other_param",
                "source": "var__OtherEMG",
            }
        ]
        assert reconcile_manual_inputs([target], "fn", hidden, manual_edges) == []

    def test_partial_reconnection_multi_handle_still_drops(self):
        # Hidden var input AND hidden constant; manual edge covers only the
        # var handle — the target must still be dropped entirely.
        from scistack_gui.domain.graph_builder import fn_node_id, wiring_id

        target = self._target({"signal": "RawEMG"}, constants={"low_hz": 20})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        hidden = {f"e__RawEMG__fn__{wid}", f"e__low_hz__fn__{wid}"}
        manual_edges = [
            {
                "target": fn_node_id("fn", wid),
                "targetHandle": "in__signal",
                "source": "var__OtherEMG",
            }
        ]
        assert reconcile_manual_inputs([target], "fn", hidden, manual_edges) == []

    def test_manual_reconnect_covers_one_of_multitype_list_elements(self):
        # Multitype ["A", "B"] input, B's edge hidden, C wired manually onto
        # the handle. The edges VISIBLE on the DAG are the ground truth
        # (docs/claude/manual-edges-on-history-nodes.md): A's edge is still
        # visible, so the handle now reads EachOf [A, C] — not C alone, which
        # is what it meant before 2026-09-15 and which silently dropped a
        # wire the user could see.
        from scistack_gui.domain.graph_builder import fn_node_id, wiring_id

        target = self._target({"signal": ["A", "B"]})
        wid = wiring_id("fn", {"signal": ["A", "B"]}, {"Out"}, {})
        hidden = {f"e__B__fn__{wid}"}
        manual_edges = [
            {"target": fn_node_id("fn", wid), "targetHandle": "in__signal", "source": "var__C"}
        ]
        result = reconcile_manual_inputs([target], "fn", hidden, manual_edges)
        assert result == [
            {
                "bindings": {"signal": {"kind": "variable", "ref": ["A", "C"]}},
                "input_types": {"signal": ["A", "C"]},
                "output_type": "Out",
                "constants": {},
            }
        ]


class TestReconcileManualInputsUnboundParams:
    """Manual variable edges onto a parameter HISTORY NEVER BOUND (added to
    the signature after the recorded runs — grSides/Demographics,
    2026-09-15). No hidden edge is involved, so the old
    filter_disconnected_targets never even looked at these."""

    def _target(self, input_types, constants=None, call_id="stale"):
        return {
            "bindings": {
                p: {"kind": "variable", "ref": t if isinstance(t, list) else [t]}
                for p, t in input_types.items()
            },
            "input_types": dict(input_types),
            "output_type": "Out",
            "constants": constants or {},
            "call_id": call_id,
        }

    def _edge(self, wid, handle, source, fn="fn"):
        from scistack_gui.domain.graph_builder import fn_node_id

        return {"target": fn_node_id(fn, wid), "targetHandle": handle, "source": source}

    def test_unbound_param_bound_from_manual_edge(self):
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": "RawEMG"}, constants={"low_hz": 20})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        edges = [self._edge(wid, "in__side", "var__Demographics")]

        result = reconcile_manual_inputs([target], "fn", set(), edges)

        assert len(result) == 1
        assert result[0]["bindings"]["side"] == {"kind": "variable", "ref": ["Demographics"]}
        assert result[0]["bindings"]["signal"] == {"kind": "variable", "ref": ["RawEMG"]}
        assert result[0]["input_types"] == {"signal": "RawEMG", "side": "Demographics"}
        assert result[0]["constants"] == {"low_hz": 20}, "history constants are kept"
        assert "call_id" not in result[0], "stale call_id must be recomputed"

    def test_runs_with_no_hidden_edges_at_all(self):
        # The trigger used to be "hidden_edge_ids non-empty"; an unbound
        # param has no hidden edge, so the reconcile must not short-circuit.
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": "RawEMG"})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        edges = [self._edge(wid, "in__side", "var__Demographics")]
        assert reconcile_manual_inputs([target], "fn", None, edges)[0]["bindings"]["side"][
            "ref"
        ] == ["Demographics"]

    def test_placement_qualified_target_id_matches(self):
        from scistack_gui.domain.graph_builder import fn_node_id, wiring_id

        target = self._target({"signal": "RawEMG"})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        edges = [
            {
                "target": fn_node_id("fn", wid) + "::main",
                "targetHandle": "in__side",
                "source": "var__Demographics::main",
            }
        ]
        result = reconcile_manual_inputs([target], "fn", set(), edges)
        assert result[0]["bindings"]["side"]["ref"] == ["Demographics"]

    def test_manual_edge_beside_visible_history_edge_is_each_of(self):
        # Same picture on a fresh node means EachOf; a history node must
        # read it the same way rather than ignore the wire the user drew.
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": "RawEMG"})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        edges = [self._edge(wid, "in__signal", "var__OtherEMG")]
        result = reconcile_manual_inputs([target], "fn", set(), edges)
        assert result[0]["bindings"]["signal"] == {
            "kind": "variable",
            "ref": ["RawEMG", "OtherEMG"],
        }
        assert result[0]["input_types"] == {"signal": ["RawEMG", "OtherEMG"]}

    def test_hidden_reconnect_and_unbound_param_in_one_pass(self):
        # Both on one node: the substitution changes the wiring id, so a
        # two-pass design keyed on the new id would miss the second edge.
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": "RawEMG"})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        hidden = {f"e__RawEMG__fn__{wid}"}
        edges = [
            self._edge(wid, "in__signal", "var__OtherEMG"),
            self._edge(wid, "in__side", "var__Demographics"),
        ]
        result = reconcile_manual_inputs([target], "fn", hidden, edges)
        assert result[0]["input_types"] == {"signal": "OtherEMG", "side": "Demographics"}

    def test_unbound_override_never_readmits_uncovered_hidden_handle(self):
        # Partial reconnection stays dropped even when an unrelated manual
        # edge binds a new param on the same node.
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": "RawEMG"}, constants={"low_hz": 20})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        hidden = {f"e__low_hz__fn__{wid}"}
        edges = [self._edge(wid, "in__side", "var__Demographics")]
        assert reconcile_manual_inputs([target], "fn", hidden, edges) == []

    def test_non_variable_source_is_not_an_override(self):
        # PathInput / Parameter sources have their own binding rules.
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": "RawEMG"})
        wid = wiring_id("fn", {"signal": "RawEMG"}, {"Out"}, {})
        edges = [self._edge(wid, "in__side", "pathInput__files")]
        result = reconcile_manual_inputs([target], "fn", set(), edges)
        assert result == [target]

    def test_edge_on_another_wiring_does_not_leak(self):
        from scistack_gui.domain.graph_builder import wiring_id

        target = self._target({"signal": "RawEMG"})
        other_wid = wiring_id("fn", {"signal": "Other"}, {"Out"}, {})
        edges = [self._edge(other_wid, "in__side", "var__Demographics")]
        assert reconcile_manual_inputs([target], "fn", set(), edges) == [target]


class TestBuildSchemaKwargs:
    def test_no_filter_no_level_returns_all(self):
        result = build_schema_kwargs(
            schema_level=None,
            all_schema_keys=["subject", "session"],
            schema_filter=None,
            distinct_values={"subject": [1, 2], "session": ["pre", "post"]},
        )
        assert result == {"subject": [1, 2], "session": ["pre", "post"]}

    def test_schema_level_limits_keys(self):
        result = build_schema_kwargs(
            schema_level=["subject"],
            all_schema_keys=["subject", "session"],
            schema_filter=None,
            distinct_values={"subject": [1, 2], "session": ["pre", "post"]},
        )
        assert result == {"subject": [1, 2]}
        assert "session" not in result

    def test_schema_filter_narrows_values(self):
        result = build_schema_kwargs(
            schema_level=None,
            all_schema_keys=["subject", "session"],
            schema_filter={"subject": [1]},
            distinct_values={"subject": [1, 2], "session": ["pre", "post"]},
        )
        assert result["subject"] == [1]
        assert result["session"] == ["pre", "post"]

    def test_schema_filter_empty_list_falls_back_to_distinct(self):
        result = build_schema_kwargs(
            schema_level=None,
            all_schema_keys=["subject"],
            schema_filter={"subject": []},
            distinct_values={"subject": [1, 2, 3]},
        )
        assert result["subject"] == [1, 2, 3]

    def test_schema_level_and_filter_combined(self):
        result = build_schema_kwargs(
            schema_level=["subject"],
            all_schema_keys=["subject", "session"],
            schema_filter={"subject": [2]},
            distinct_values={"subject": [1, 2], "session": ["pre"]},
        )
        assert result == {"subject": [2]}

    def test_key_missing_from_distinct_returns_empty_list(self):
        result = build_schema_kwargs(
            schema_level=None,
            all_schema_keys=["subject"],
            schema_filter=None,
            distinct_values={},
        )
        assert result == {"subject": []}
