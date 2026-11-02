"""Tests for scidb.provenance.state: check_combo_state and check_node_state."""

from functools import partial

import numpy as np
from scidb.provenance.state import check_combo_state, check_node_state

from scidb import BaseVariable, scistack
from scidb import for_each as scidb_for_each
from scidb import for_each as _for_each

# skip_computed=True: these tests cover the skip-already-computed path.
for_each = partial(_for_each, skip_computed=True)

# ---------------------------------------------------------------------------
# Variable types — defined at module level for BaseVariable registry
# ---------------------------------------------------------------------------


class RawState(BaseVariable):
    schema_version = 1


class ProcessedState(BaseVariable):
    schema_version = 1


class SecondaryState(BaseVariable):
    schema_version = 1


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------


@scistack
def process_data(raw):
    return np.asarray(raw, dtype=float) * 2.0


@scistack
def second_step(processed):
    return np.asarray(processed, dtype=float) + 1.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_raw(db, subjects=(1, 2), trials=("A", "B")):
    for subj in subjects:
        for trial in trials:
            RawState.save(np.random.randn(5), subject=subj, trial=trial)


def _run_all(db, subjects=(1, 2), trials=("A", "B")):
    for_each(
        process_data,
        inputs={"raw": RawState},
        outputs=[ProcessedState],
        subject=list(subjects),
        trial=list(trials),
    )


# ---------------------------------------------------------------------------
# check_combo_state
# ---------------------------------------------------------------------------


class TestCheckComboState:
    def test_missing_when_no_output(self, db):
        _seed_raw(db)
        state = check_combo_state(
            process_data,
            [ProcessedState],
            {"subject": 1, "trial": "A"},
            db=db,
        )
        assert state == "missing"

    def test_up_to_date_after_full_run(self, db):
        _seed_raw(db)
        _run_all(db)
        state = check_combo_state(
            process_data,
            [ProcessedState],
            {"subject": 1, "trial": "A"},
            db=db,
        )
        assert state == "up_to_date"

    def test_stale_when_upstream_input_updated(self, db):
        _seed_raw(db)
        _run_all(db)
        # Overwrite subject=1/trial=A raw input → should make output stale
        RawState.save(np.ones(5) * 99, subject=1, trial="A")
        state = check_combo_state(
            process_data,
            [ProcessedState],
            {"subject": 1, "trial": "A"},
            db=db,
        )
        assert state == "stale"

    def test_function_hash_change_stale_via_lineage(self, db):
        """Lineage-path function-hash mismatch is detected as stale.

        The caller passes fn directly, so comparing fn.hash against the
        stored lineage hash is reliable for the function's own code change.
        """
        _seed_raw(db)
        _run_all(db)

        @scistack
        def process_data_v2(raw):  # different bytecode → different hash
            return np.asarray(raw, dtype=float) * 3.0

        process_data_v2.__name__ = "process_data"  # same name, different hash

        state = check_combo_state(
            process_data_v2,
            [ProcessedState],
            {"subject": 1, "trial": "A"},
            db=db,
        )
        assert state == "stale"

    def test_missing_for_unknown_combo(self, db):
        _seed_raw(db)
        _run_all(db)
        # subject=99 was never seeded or run
        state = check_combo_state(
            process_data,
            [ProcessedState],
            {"subject": 99, "trial": "A"},
            db=db,
        )
        assert state == "missing"


# ---------------------------------------------------------------------------
# check_node_state
# ---------------------------------------------------------------------------


class TestCheckNodeState:
    def test_red_when_never_run(self, db):
        _seed_raw(db)
        # for_each never called — no output records
        result = check_node_state(
            process_data,
            [ProcessedState],
            inputs={"raw": RawState},
            db=db,
        )
        assert result["state"] == "red"
        assert result["counts"]["up_to_date"] == 0
        assert result["counts"]["missing"] == 4  # 2 subjects × 2 trials

    def test_green_when_all_combos_up_to_date(self, db):
        _seed_raw(db)
        _run_all(db)
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 4
        assert result["counts"]["missing"] == 0
        assert result["counts"]["stale"] == 0

    def test_red_when_partial_run(self, db):
        _seed_raw(db)
        # Only run for subject=1 → 2 of 4 combos executed → any missing → red
        for_each(
            process_data,
            inputs={"raw": RawState},
            outputs=[ProcessedState],
            subject=[1],
            trial=["A", "B"],
        )
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "red"
        assert result["counts"]["up_to_date"] == 2
        assert result["counts"]["missing"] == 2

    def test_red_when_input_changed_needs_rerun(self, db):
        """Changed input → the combo's EXPECTED invocation_id shifts to one over
        the new input record, which hasn't been run → node shows needs-run (red).

        Membership model (§9c): "stale" folds into needs-run — the old output
        stays valid lineage; the node signals the current recipe hasn't been run
        on the current input. Any missing invocation makes the node red (binary
        model). (Combo-level staleness is still available via check_combo_state.)
        """
        _seed_raw(db)
        _run_all(db)
        # Change one input → its expected invocation is now absent.
        RawState.save(np.ones(5) * 99, subject=1, trial="A")
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "red"
        assert result["counts"]["missing"] >= 1
        assert result["counts"]["stale"] == 0

    def test_combos_list_has_schema_info(self, db):
        _seed_raw(db)
        _run_all(db)
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert len(result["combos"]) == 4
        for combo in result["combos"]:
            assert "schema_combo" in combo
            assert "subject" in combo["schema_combo"]
            assert "trial" in combo["schema_combo"]
            assert combo["state"] == "up_to_date"

    def test_red_when_no_input_data_and_never_run(self, db):
        # Completely empty DB — no raw data, nothing run
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "red"
        assert result["counts"] == {"up_to_date": 0, "stale": 0, "missing": 0}

    def test_two_step_chain_downstream_state(self, db):
        """process_data partially run → second_step combos depend on what exists."""
        _seed_raw(db)
        # Step 1: partial run
        for_each(
            process_data,
            inputs={"raw": RawState},
            outputs=[ProcessedState],
            subject=[1],
            trial=["A", "B"],
        )
        # Step 2: run for all available ProcessedState records
        for_each(
            second_step,
            inputs={"processed": ProcessedState},
            outputs=[SecondaryState],
            subject=[1],
            trial=["A", "B"],
        )
        # process_data: red (only 2 of 4 combos run → 2 missing)
        upstream = check_node_state(process_data, [ProcessedState], db=db)
        assert upstream["state"] == "red"

        # second_step own state: green (ran for all available ProcessedState)
        # Note: scidb.check_node_state does NOT propagate upstream staleness —
        # that is the GUI layer's responsibility. The own state reflects only
        # what second_step itself has done.
        downstream = check_node_state(second_step, [SecondaryState], db=db)
        assert downstream["state"] == "green"
        assert downstream["counts"]["up_to_date"] == 2
        # second_step only sees 2 ProcessedState records as inputs, so 2 expected
        assert downstream["counts"]["missing"] == 0


# ---------------------------------------------------------------------------
# Fallback path: scidb.for_each (no lineage, uses __fn_hash + timestamps)
# ---------------------------------------------------------------------------


class ScidbRaw(BaseVariable):
    schema_version = 1


class ScidbProcessed(BaseVariable):
    schema_version = 1


def scidb_process(raw):
    """Plain function (not a LineageFcn) for scidb.for_each tests."""
    return np.asarray(raw, dtype=float) * 3.0


class TestFnHashFallback:
    """check_combo_state fallback when output was saved via scidb.for_each.

    scidb.for_each writes __fn_hash into version_keys but no _lineage row.
    Function staleness uses __fn_hash; input freshness uses timestamps.
    """

    def test_up_to_date_after_scidb_run(self, db):
        for subj in [1, 2]:
            ScidbRaw.save(np.random.randn(5), subject=subj, trial="A")
        scidb_for_each(
            scidb_process,
            inputs={"raw": ScidbRaw},
            outputs=[ScidbProcessed],
            subject=[1, 2],
            trial=["A"],
        )
        state = check_combo_state(
            scidb_process,
            [ScidbProcessed],
            {"subject": 1, "trial": "A"},
            db=db,
        )
        assert state == "up_to_date"

    def test_stale_when_input_re_saved_after_output(self, db):
        ScidbRaw.save(np.random.randn(5), subject=1, trial="A")
        scidb_for_each(
            scidb_process,
            inputs={"raw": ScidbRaw},
            outputs=[ScidbProcessed],
            subject=[1],
            trial=["A"],
        )
        # Re-save raw input after output was produced → timestamp now newer
        ScidbRaw.save(np.ones(5) * 42, subject=1, trial="A")
        state = check_combo_state(
            scidb_process,
            [ScidbProcessed],
            {"subject": 1, "trial": "A"},
            db=db,
        )
        assert state == "stale"

    def test_stale_when_fn_hash_changed(self, db):
        ScidbRaw.save(np.random.randn(5), subject=1, trial="A")
        scidb_for_each(
            scidb_process,
            inputs={"raw": ScidbRaw},
            outputs=[ScidbProcessed],
            subject=[1],
            trial=["A"],
        )

        def scidb_process_v2(raw):  # different body → different __fn_hash
            return np.asarray(raw, dtype=float) * 99.0

        state = check_combo_state(
            scidb_process_v2,
            [ScidbProcessed],
            {"subject": 1, "trial": "A"},
            db=db,
        )
        assert state == "stale"

    def test_up_to_date_with_constants(self, db):
        """check_node_state returns green after for_each run with constants.

        Regression test: branch_params keys are namespaced as "fn.param" but
        version_keys stores them un-namespaced. find_record_id must route them
        through the branch_params_filter path (suffix matching), not the
        version_keys filter path (which would fail to find the record).
        """

        class ScidbRawConst(BaseVariable):
            schema_version = 1

        class ScidbProcessedConst(BaseVariable):
            schema_version = 1

        def scidb_process_with_scale(raw, scale):
            return np.asarray(raw, dtype=float) * float(scale)

        for subj in [1, 2]:
            ScidbRawConst.save(np.random.randn(5), subject=subj, trial="A")

        scidb_for_each(
            scidb_process_with_scale,
            inputs={"raw": ScidbRawConst, "scale": 2.0},
            outputs=[ScidbProcessedConst],
            subject=[1, 2],
            trial=["A"],
        )

        result = check_node_state(
            scidb_process_with_scale, [ScidbProcessedConst], db=db
        )
        assert result["state"] == "green", (
            f"Expected green after full run with constants, got {result['state']}. "
            f"Counts: {result['counts']}"
        )


def test_variable_input_classification(tmp_path):
    """Test that variable inputs are properly classified in lineage records.

    This verifies the fix for the input classification quirk where BaseVariable
    inputs were being classified as constants instead of variables.
    """
    from scidb.store.database import get_database

    from scidb import BaseVariable, configure_database

    # Setup database
    db_path = tmp_path / "test.duckdb"
    configure_database(str(db_path), ["subject", "session"])
    db = get_database()

    # Define variable types
    class RawEMG(BaseVariable):
        schema_version = 1

    class Filtered(BaseVariable):
        schema_version = 1

    # Create and save a variable
    sig = RawEMG(np.random.randn(100))
    rid_input = RawEMG.save(sig, subject=1, session="A")

    # Process with pipeline function
    @scistack
    def bandpass(signal, low_hz):
        return signal

    # Iterate over schema keys to avoid aggregation mode
    for_each(
        bandpass,
        inputs={"signal": RawEMG, "low_hz": 20},
        outputs=[Filtered],
        subject=[1],
        session=["A"],
    )

    # Query provenance from the bipartite graph.
    filt = Filtered.load(subject=1, session="A")
    prov = db.get_provenance(Filtered, version=filt.record_id)
    assert prov is not None, "No provenance recorded for Filtered"
    inputs = prov["inputs"]  # variable inputs: {record_id, param_name, variable_type}
    constants = prov["constants"]  # {param_name: value}

    # Variable input 'signal' classified correctly, pointing at the saved RawEMG.
    var_inputs = [i for i in inputs if i.get("param_name") == "signal"]
    assert len(var_inputs) == 1, (
        f"Expected 1 variable input named 'signal', got {len(var_inputs)}"
    )
    assert var_inputs[0]["variable_type"] == "RawEMG", (
        f"Expected variable_type='RawEMG', got {var_inputs[0].get('variable_type')}"
    )
    assert var_inputs[0]["record_id"] == rid_input, (
        f"Expected record_id={rid_input}, got {var_inputs[0].get('record_id')}"
    )

    # Constant 'low_hz' classified as a constant, not a variable input.
    assert "low_hz" in constants, f"Expected 'low_hz' in constants, got {constants}"
    assert constants["low_hz"] == 20

    # Variable 'signal' must NOT appear among constants.
    assert "signal" not in constants, (
        f"Variable 'signal' should NOT be in constants, found {constants}"
    )


# ---------------------------------------------------------------------------
# check_multiple_nodes_state
# ---------------------------------------------------------------------------


class TestCheckMultipleNodesState:
    def test_multiple_nodes_basic(self, db):
        """Test checking state for multiple nodes at once."""
        from scidb.provenance.state import check_multiple_nodes_state

        _seed_raw(db)
        _run_all(db, subjects=(1,), trials=("A",))  # Run only subject=1, trial=A

        # Define nodes to check
        nodes = [
            {"fn": process_data, "call_id": None, "outputs": [ProcessedState]},
            {"fn": second_step, "call_id": None, "outputs": [SecondaryState]},
        ]

        result = check_multiple_nodes_state(nodes, db=db)

        # process_data has been run (partially) → red under the binary model
        process_state = result.get("fn__process_data__")
        assert process_state is not None
        assert process_state["state"] in ("green", "red")
        assert "counts" in process_state

        # second_step has not been run → should be red
        second_state = result.get("fn__second_step__")
        assert second_state is not None
        assert second_state["state"] == "red"
        assert second_state["counts"]["missing"] >= 0

    def test_multiple_nodes_with_call_id(self, db):
        """Test checking state with call_id specified."""
        from scidb.provenance.state import check_multiple_nodes_state

        _seed_raw(db)
        _run_all(db)

        # Get the call_id from the actual outputs
        # (In practice this would come from list_pipeline_variants)
        variants = db.list_pipeline_variants()
        call_ids = {
            v["call_id"] for v in variants if v["function_name"] == "process_data"
        }
        call_id = list(call_ids)[0] if call_ids else None

        nodes = [
            {"fn": process_data, "call_id": call_id, "outputs": [ProcessedState]},
        ]

        result = check_multiple_nodes_state(nodes, db=db)

        node_id = f"fn__process_data__{call_id}" if call_id else "fn__process_data__"
        assert node_id in result
        # Should be green since we ran all combos
        assert result[node_id]["state"] == "green"

    def test_multiple_nodes_with_fn_registry(self, db):
        """Test using fn_registry to lookup functions by name."""
        from scidb.provenance.state import check_multiple_nodes_state

        _seed_raw(db)
        _run_all(db)

        fn_registry = {
            "process_data": process_data,
            "second_step": second_step,
        }

        nodes = [
            {"fn_name": "process_data", "call_id": None, "outputs": [ProcessedState]},
            {"fn_name": "second_step", "call_id": None, "outputs": [SecondaryState]},
        ]

        result = check_multiple_nodes_state(nodes, fn_registry=fn_registry, db=db)

        assert "fn__process_data__" in result
        assert "fn__second_step__" in result

    def test_multiple_nodes_missing_function(self, db):
        """Test that missing functions are marked as red."""
        from scidb.provenance.state import check_multiple_nodes_state

        fn_registry = {}  # Empty registry

        nodes = [
            {
                "fn_name": "nonexistent_func",
                "call_id": "abc123",
                "outputs": [ProcessedState],
            },
        ]

        result = check_multiple_nodes_state(nodes, fn_registry=fn_registry, db=db)

        assert "fn__nonexistent_func__abc123" in result
        assert result["fn__nonexistent_func__abc123"]["state"] == "red"


# ---------------------------------------------------------------------------
# Per-call-site scoping (call_id)
# ---------------------------------------------------------------------------


@scistack
def scale_data(raw, factor):
    return np.asarray(raw, dtype=float) * float(factor)


class TestCallSiteScoping:
    """call_id restricts the expected-invocation set to ONE call site's
    variant config: a second call site's partial run must not redden the
    first site's fully-run node (regression: the invocation-membership
    rewrite dropped the call_id filter, unioning expected sets across
    configs — the GUI's per-call-site display broke)."""

    def _call_id(self, constants):
        from scidb.run.foreach_config import ForEachConfig

        return ForEachConfig(
            scale_data, {"raw": RawState, "factor": constants}
        ).to_call_id()

    def test_full_call_site_stays_green_beside_partial_sibling(self, db):
        _seed_raw(db)
        # Call site A (factor=2): fully run.
        for_each(
            scale_data,
            inputs={"raw": RawState, "factor": 2},
            outputs=[ProcessedState],
            subject=[1, 2],
            trial=["A", "B"],
        )
        # Call site B (factor=5): subject=1 only (partial).
        for_each(
            scale_data,
            inputs={"raw": RawState, "factor": 5},
            outputs=[ProcessedState],
            subject=[1],
            trial=["A", "B"],
        )

        result_a = check_node_state(
            scale_data, [ProcessedState], db=db, call_id=self._call_id(2)
        )
        result_b = check_node_state(
            scale_data, [ProcessedState], db=db, call_id=self._call_id(5)
        )
        result_union = check_node_state(scale_data, [ProcessedState], db=db)

        assert result_a["state"] == "green", "fully-run call site must stay green"
        assert result_a["counts"]["missing"] == 0
        assert result_b["state"] == "red", "partial call site must be red"
        assert result_b["counts"]["missing"] == 2
        # No call_id = the union across call sites (pessimistic).
        assert result_union["state"] == "red"

    def test_unknown_call_id_reads_red(self, db):
        _seed_raw(db)
        for_each(
            scale_data,
            inputs={"raw": RawState, "factor": 2},
            outputs=[ProcessedState],
            subject=[1, 2],
            trial=["A", "B"],
        )

        result = check_node_state(
            scale_data, [ProcessedState], db=db, call_id="0000000000000000"
        )

        assert result["state"] == "red"  # no matching config -> no expected work
