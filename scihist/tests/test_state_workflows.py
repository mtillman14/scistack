"""End-to-end workflow tests for scihist.state.

Complements test_state.py / test_state_realworld.py / test_state_pathinput.py
by covering user workflows that previously had no explicit test:

1. Multi-step propagation (3+ node chains) — data-change staleness walks
   through the full lineage graph.
2. Fork / Join DAG shapes.
3. Mixed input types (PathInput + Variable + Constant in a single function).
4. True multi-output functions (single @scistack returning a tuple).

Design decisions codified here:

- **Node state is binary: green | red.** A node is green only when every
  expected invocation (live-derived from current inputs) is present; any
  missing invocation makes it red. There is no grey/partial state.

- **Data-change propagation is lazy (one level per re-run).** Re-saving an
  input reds only its DIRECT consumer — that consumer's input changed, so its
  expected invocation over the new input is absent. Downstream nodes stay green
  until the consumer is re-run, which produces new records that then red the
  next level. (No eager deep walk.) Superseded input records are not counted
  (latest-record selection in `_current_records_by_schema`).

- **Function-code-change propagation is shallow.** `check_node_state(fn, ...)`
  detects a fn-hash change for `fn` itself (its expected invocation_ids shift to
  the new hash and are absent → red). Propagating an ancestor's code change to
  descendants is a GUI-layer DAG-walk concern; once the ancestor is re-run, the
  new record_id cascades as a data change.
"""

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from scihist.state import check_node_state

from scidb import BaseVariable, scistack
from scidb import for_each as scidb_for_each
from scifor import PathInput
from scihist import for_each

# The tests' own fixture: sub{01..03}/trial{01..05}.csv (time, force_left,
# force_right). It lived in examples/aim2/data until 1ab0ba51 reorganised the
# example dataset and silently broke these suites -- test data is not example
# data.
DATA_DIR = Path(__file__).parent / "data" / "state_trials"


# ---------------------------------------------------------------------------
# Variable types (module-level so BaseVariable registry picks them up)
# ---------------------------------------------------------------------------


class WfRaw(BaseVariable):
    schema_version = 1


class WfStep1(BaseVariable):
    schema_version = 1


class WfStep2(BaseVariable):
    schema_version = 1


class WfStep3(BaseVariable):
    schema_version = 1


class WfForkLeft(BaseVariable):
    schema_version = 1


class WfForkRight(BaseVariable):
    schema_version = 1


class WfJoined(BaseVariable):
    schema_version = 1


class WfBaseline(BaseVariable):
    schema_version = 1


class WfMixedOut(BaseVariable):
    schema_version = 1


class WfMultiA(BaseVariable):
    schema_version = 1


class WfMultiB(BaseVariable):
    schema_version = 1


class WfMultiC(BaseVariable):
    schema_version = 1


class WfVariantRaw(BaseVariable):
    schema_version = 1


class WfVariantOut(BaseVariable):
    schema_version = 1


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------


@scistack
def step1(raw):
    return np.asarray(raw, dtype=float) * 2.0


@scistack
def step2(s1):
    return np.asarray(s1, dtype=float) + 1.0


@scistack
def step3(s2):
    return np.asarray(s2, dtype=float) - 0.5


@scistack
def fork_left(raw):
    return np.asarray(raw, dtype=float) * 10.0


@scistack
def fork_right(raw):
    return np.asarray(raw, dtype=float) * 100.0


@scistack
def join_sides(left, right):
    return float(np.sum(np.asarray(left) + np.asarray(right)))


@scistack
def mixed_inputs(filepath, baseline, scale):
    """PathInput + Variable + Constant, all in one function."""
    df = pd.read_csv(filepath)
    return float(
        np.mean(df["force_left"].values) - np.mean(np.asarray(baseline))
    ) * float(scale)


def scale_raw(raw, scale):
    """Plain (non-lineage) fn so scidb.for_each writes constants to
    branch_params — which is what makes each (scale=N) a distinct
    variant at the state-tracking level.
    """
    return np.asarray(raw, dtype=float) * float(scale)


@scistack
def multi_output(raw):
    """Single function, three outputs — the canonical load_csv.m shape.
    Returns a tuple; scifor spreads each element to the corresponding output
    class (by output count — the marker carries no unpack option).
    """
    arr = np.asarray(raw, dtype=float)
    return arr * 1.0, arr * 2.0, arr * 3.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_raw(db, subjects=(1, 2), trials=("A", "B")):
    for subj in subjects:
        for trial in trials:
            WfRaw.save(
                np.array([1.0, 2.0, 3.0, 4.0, 5.0]), subject=subj, trial=trial, db=db
            )


# ---------------------------------------------------------------------------
# 1. Multi-step propagation
# ---------------------------------------------------------------------------


class TestMultiStepPropagation:
    """WfRaw → step1 → step2 → step3. A re-save of WfRaw must cascade to
    every downstream node via the deep lineage walk.
    """

    def _run_full_chain(self, db):
        _seed_raw(db)
        for_each(
            step1,
            inputs={"raw": WfRaw},
            outputs=[WfStep1],
            subject=[1, 2],
            trial=["A", "B"],
            db=db,
        )
        for_each(
            step2,
            inputs={"s1": WfStep1},
            outputs=[WfStep2],
            subject=[1, 2],
            trial=["A", "B"],
            db=db,
        )
        for_each(
            step3,
            inputs={"s2": WfStep2},
            outputs=[WfStep3],
            subject=[1, 2],
            trial=["A", "B"],
            db=db,
        )

    def test_root_change_reds_only_direct_consumer(self, db):
        """Re-save WfRaw[1,A] → step1 needs-run (red); step2/step3 stay green.

        Membership model (§9c) propagates LAZILY: only step1's direct input
        changed, so only step1 turns red. step2's input (WfStep1) is unchanged
        until step1 is re-run, so step2/step3 remain green. (Staleness flows one
        level per re-run, not eagerly down the whole chain.) The superseded old
        WfRaw[1,A] is not counted (latest-record selection), so up_to_date is 3.
        """
        self._run_full_chain(db)
        assert check_node_state(step1, [WfStep1], db=db)["state"] == "green"
        assert check_node_state(step2, [WfStep2], db=db)["state"] == "green"
        assert check_node_state(step3, [WfStep3], db=db)["state"] == "green"

        WfRaw.save(np.array([99.0] * 5), subject=1, trial="A", db=db)

        r1 = check_node_state(step1, [WfStep1], db=db)
        r2 = check_node_state(step2, [WfStep2], db=db)
        r3 = check_node_state(step3, [WfStep3], db=db)

        assert r1["state"] == "red"
        assert r1["counts"]["missing"] == 1
        assert r1["counts"]["up_to_date"] == 3
        assert r2["state"] == "green", (
            f"lazy: step2 input unchanged. Got {r2['state']}."
        )
        assert r3["state"] == "green", (
            f"lazy: step3 input unchanged. Got {r3['state']}."
        )

    def test_midchain_fn_change_affects_only_checked_node(self, db):
        """Changing step2's code is detected only by check_node_state(step2).

        scihist cannot cascade ancestor-fn-code changes to descendants
        without a registry of current function objects. The GUI layer's
        DAG walk is responsible for propagating step2's 'red' to step3.
        Once step2 is re-run, the new record_id cascades as a data change.
        """
        self._run_full_chain(db)

        @scistack
        def step2_v2(s1):
            return np.asarray(s1, dtype=float) + 42.0

        step2_v2.__name__ = "step2"

        r1 = check_node_state(step1, [WfStep1], db=db)
        r2 = check_node_state(step2_v2, [WfStep2], db=db)
        r3 = check_node_state(step3, [WfStep3], db=db)

        assert r1["state"] == "green"
        # Edited fn → new function_hash → every expected invocation_id shifts to
        # the new-hash call, none of which are present → needs-run (red). Under
        # the binary model there is no grey; a code edit reds the node.
        assert r2["state"] == "red", "step2 fn hash change → needs-run (red)"
        assert r3["state"] == "green", (
            "step3's own state stays green — scihist cannot see step2_v2. "
            "GUI layer must propagate step2's needs-run to step3."
        )

    def test_new_upstream_combo_only_reds_direct_consumer(self, db):
        """New WfRaw combo → step1 red; step2/step3 own-state stays green
        until step1 is re-run (which would add new WfStep1 records that
        step2 then sees as missing).
        """
        self._run_full_chain(db)
        WfRaw.save(np.array([7.0] * 5), subject=3, trial="A", db=db)

        r1 = check_node_state(step1, [WfStep1], db=db)
        r2 = check_node_state(step2, [WfStep2], db=db)
        r3 = check_node_state(step3, [WfStep3], db=db)

        assert r1["state"] == "red"
        assert r1["counts"]["missing"] == 1
        assert r1["counts"]["up_to_date"] == 4
        assert r2["state"] == "green"
        assert r3["state"] == "green"


# ---------------------------------------------------------------------------
# 2. Fork / Join DAG shapes
# ---------------------------------------------------------------------------


class TestForkJoinPropagation:
    """WfRaw feeds fork_left and fork_right; join_sides consumes both."""

    def _run_fork_join(self, db):
        _seed_raw(db)
        for_each(
            fork_left,
            inputs={"raw": WfRaw},
            outputs=[WfForkLeft],
            subject=[1, 2],
            trial=["A", "B"],
            db=db,
        )
        for_each(
            fork_right,
            inputs={"raw": WfRaw},
            outputs=[WfForkRight],
            subject=[1, 2],
            trial=["A", "B"],
            db=db,
        )
        for_each(
            join_sides,
            inputs={"left": WfForkLeft, "right": WfForkRight},
            outputs=[WfJoined],
            subject=[1, 2],
            trial=["A", "B"],
            db=db,
        )

    def test_fork_one_upstream_reds_both_branches(self, db):
        """Re-save WfRaw[1,A] → fork_left and fork_right each need-run for that combo."""
        self._run_fork_join(db)
        assert check_node_state(fork_left, [WfForkLeft], db=db)["state"] == "green"
        assert check_node_state(fork_right, [WfForkRight], db=db)["state"] == "green"

        WfRaw.save(np.array([42.0] * 5), subject=1, trial="A", db=db)

        rl = check_node_state(fork_left, [WfForkLeft], db=db)
        rr = check_node_state(fork_right, [WfForkRight], db=db)
        assert rl["state"] == "red"
        assert rr["state"] == "red"
        assert rl["counts"]["missing"] == 1
        assert rr["counts"]["missing"] == 1

    def test_join_stays_green_when_root_changes(self, db):
        """Re-save WfRaw[1,A] → join_sides stays green (lazy propagation).

        The join's immediate inputs (WfForkLeft/Right) haven't been re-run, so
        they're unchanged and the join's expected invocations are all present.
        The needs-run signal sits at the forks until they're re-run.
        """
        self._run_fork_join(db)
        assert check_node_state(join_sides, [WfJoined], db=db)["state"] == "green"

        WfRaw.save(np.array([42.0] * 5), subject=1, trial="A", db=db)

        rj = check_node_state(join_sides, [WfJoined], db=db)
        assert rj["state"] == "green", (
            f"lazy: join inputs unchanged until forks re-run. "
            f"Got {rj['state']} counts={rj['counts']}"
        )

    def test_join_reds_when_direct_input_resaved(self, db):
        """Re-save WfForkLeft directly → join_sides needs-run for that combo."""
        self._run_fork_join(db)
        WfForkLeft.save(np.array([1e6] * 5), subject=1, trial="A", db=db)

        rj = check_node_state(join_sides, [WfJoined], db=db)
        assert rj["state"] == "red"
        assert rj["counts"]["missing"] == 1


# ---------------------------------------------------------------------------
# 3. Mixed input types
# ---------------------------------------------------------------------------


class TestMixedInputTypes:
    """Single function with PathInput + Variable + Constant inputs.

    PathInput and Variable share the (subject, trial) schema — seed one
    WfBaseline record per combo. Constant: scale=2.0.
    """

    SUBJECTS = ["01", "02"]
    TRIALS = ["01", "02"]

    def _seed_baselines(self, db):
        for subj in self.SUBJECTS:
            for trial in self.TRIALS:
                WfBaseline.save(np.array([0.1] * 5), subject=subj, trial=trial, db=db)

    def _run_mixed(self, db, root=str(DATA_DIR)):
        self._seed_baselines(db)
        for_each(
            mixed_inputs,
            inputs={
                "filepath": PathInput(
                    "sub{subject}/trial{trial}.csv",
                    root_folder=root,
                ),
                "baseline": WfBaseline,
                "scale": 2.0,
            },
            outputs=[WfMixedOut],
            subject=self.SUBJECTS,
            trial=self.TRIALS,
            db=db,
        )

    def test_green_after_full_run_with_all_three_input_types(self, db):
        self._run_mixed(db)
        r = check_node_state(mixed_inputs, [WfMixedOut], db=db)
        assert r["state"] == "green", f"Got {r['state']} counts={r['counts']}"
        assert r["counts"]["up_to_date"] == 4
        assert r["counts"]["missing"] == 0

    def test_grey_when_pathinput_file_missing(self, db, tmp_path):
        for subj in self.SUBJECTS:
            (tmp_path / f"sub{subj}").mkdir()
            for trial in self.TRIALS:
                if (subj, trial) == ("02", "02"):
                    continue
                shutil.copy(
                    DATA_DIR / f"sub{subj}" / f"trial{trial}.csv",
                    tmp_path / f"sub{subj}" / f"trial{trial}.csv",
                )

        self._run_mixed(db, root=str(tmp_path))
        r = check_node_state(mixed_inputs, [WfMixedOut], db=db)
        # Variable input (WfBaseline) exists for the missing-file combo, so the
        # un-run invocation is detectable as missing → red (binary model).
        assert r["state"] == "red", f"Got {r['state']} counts={r['counts']}"
        assert r["counts"]["up_to_date"] == 3
        assert r["counts"]["missing"] == 1

    def test_red_when_variable_input_resaved(self, db):
        """Re-saving a Variable input → mixed fn needs-run for affected combos."""
        self._run_mixed(db)
        assert check_node_state(mixed_inputs, [WfMixedOut], db=db)["state"] == "green"

        WfBaseline.save(np.array([9.9] * 5), subject="01", trial="01", db=db)

        r = check_node_state(mixed_inputs, [WfMixedOut], db=db)
        assert r["state"] == "red"
        assert r["counts"]["missing"] >= 1
        assert r["counts"]["stale"] == 0


# ---------------------------------------------------------------------------
# 4. True multi-output (single @scistack → tuple)
# ---------------------------------------------------------------------------


class TestMultiOutputSingleFunction:
    """One @scistack function returns a tuple; for_each saves
    each tuple element to a separate output type. check_node_state(fn, [A,B,C])
    aggregates across all three output classes.
    """

    def _run_multi(self, db):
        _seed_raw(db)
        for_each(
            multi_output,
            inputs={"raw": WfRaw},
            outputs=[WfMultiA, WfMultiB, WfMultiC],
            subject=[1, 2],
            trial=["A", "B"],
            db=db,
        )

    def test_green_when_all_three_outputs_present(self, db):
        self._run_multi(db)
        r = check_node_state(
            multi_output,
            [WfMultiA, WfMultiB, WfMultiC],
            db=db,
        )
        assert r["state"] == "green", f"Got {r['state']} counts={r['counts']}"
        assert r["counts"]["up_to_date"] == 4

    def test_excluded_output_record_does_not_affect_node_state(self, db):
        """Excluding ONE of a multi-output invocation's output records does NOT
        change node state — completeness is pure invocation membership, not
        per-output-class presence. The invocation that produced (1,A) still
        exists (it also produced A and C), so the combo stays up_to_date → the
        node remains green. (Membership-only contract; the old behaviour that
        greyed on a single excluded output was dropped.)"""
        self._run_multi(db)

        db._duck._execute(
            "UPDATE _record SET excluded = TRUE "
            "WHERE type = ? AND schema_id IN ("
            "  SELECT schema_id FROM _schema WHERE subject = ? AND trial = ?"
            ")",
            [WfMultiB.__name__, "1", "A"],
        )

        r = check_node_state(
            multi_output,
            [WfMultiA, WfMultiB, WfMultiC],
            db=db,
        )
        assert r["state"] == "green", f"Got {r['state']} counts={r['counts']}"
        assert r["counts"]["missing"] == 0
        assert r["counts"]["up_to_date"] == 4

    def test_all_outputs_need_rerun_together_on_input_resave(self, db):
        """A single upstream input serves all 3 outputs (one invocation per
        combo) — re-saving WfRaw makes that combo's invocation need-run."""
        self._run_multi(db)
        assert (
            check_node_state(
                multi_output,
                [WfMultiA, WfMultiB, WfMultiC],
                db=db,
            )["state"]
            == "green"
        )

        WfRaw.save(np.array([123.0] * 5), subject=1, trial="A", db=db)

        r = check_node_state(
            multi_output,
            [WfMultiA, WfMultiB, WfMultiC],
            db=db,
        )
        assert r["state"] == "red"
        assert r["counts"]["missing"] == 1
        assert r["counts"]["up_to_date"] == 3


# ---------------------------------------------------------------------------
# 5. Multiple constant variants of the same function
# ---------------------------------------------------------------------------


class TestMultipleConstantVariants:
    """Running the same function with two different constant values produces
    two independent variants. check_node_state aggregates across variants.

    Uses scidb.for_each (not scihist.for_each) because constants on that path
    are namespaced into ``branch_params`` (e.g. ``scale_raw.scale``), which
    is what lets _get_expected_combos enumerate them as distinct variants.
    scihist.for_each puts constants in ``version_keys``, where they do not
    generate separate branch_params rows and so cannot be distinguished by
    the per-combo state check.
    """

    def _seed_raw(self, db, subjects=(1, 2), trials=("A", "B")):
        for subj in subjects:
            for trial in trials:
                WfVariantRaw.save(
                    np.array([1.0, 2.0, 3.0, 4.0, 5.0]),
                    subject=subj,
                    trial=trial,
                    db=db,
                )

    def test_green_when_both_variants_fully_run(self, db):
        self._seed_raw(db)
        for scale in (2.0, 3.0):
            scidb_for_each(
                scale_raw,
                inputs={"raw": WfVariantRaw, "scale": scale},
                outputs=[WfVariantOut],
                subject=[1, 2],
                trial=["A", "B"],
                db=db,
            )

        r = check_node_state(scale_raw, [WfVariantOut], db=db)
        # 2 variants × 4 combos = 8 total.
        assert r["state"] == "green", f"Got {r['state']} counts={r['counts']}"
        assert r["counts"]["up_to_date"] == 8
        assert r["counts"]["missing"] == 0

    def test_red_when_one_variant_fully_run_other_partial(self, db):
        """scale=2.0 ran for all 4 combos, scale=3.0 for 2 combos → red."""
        self._seed_raw(db)

        scidb_for_each(
            scale_raw,
            inputs={"raw": WfVariantRaw, "scale": 2.0},
            outputs=[WfVariantOut],
            subject=[1, 2],
            trial=["A", "B"],
            db=db,
        )
        scidb_for_each(
            scale_raw,
            inputs={"raw": WfVariantRaw, "scale": 3.0},
            outputs=[WfVariantOut],
            subject=[1],
            trial=["A", "B"],  # only subject=1
            db=db,
        )

        r = check_node_state(scale_raw, [WfVariantOut], db=db)
        # variant A: 4 up_to_date, variant B: 2 up_to_date + 2 missing.
        # Live prediction enumerates each config (scale=2, scale=3) over current
        # WfVariantRaw, so subject=2 combos count as missing for scale=3.0 → red.
        assert r["state"] == "red", f"Got {r['state']} counts={r['counts']}"
        assert r["counts"]["up_to_date"] == 6
        assert r["counts"]["missing"] == 2

    def test_variants_independent_under_input_resave(self, db):
        """Re-saving WfVariantRaw[1,A] makes the (1,A) combo need-run in BOTH
        variants — they share the upstream record.
        """
        self._seed_raw(db)
        for scale in (2.0, 3.0):
            scidb_for_each(
                scale_raw,
                inputs={"raw": WfVariantRaw, "scale": scale},
                outputs=[WfVariantOut],
                subject=[1, 2],
                trial=["A", "B"],
                db=db,
            )
        assert check_node_state(scale_raw, [WfVariantOut], db=db)["state"] == "green"

        # Re-save ONE raw combo → both variants' (1, A) invocations need re-run.
        WfVariantRaw.save(np.array([99.0] * 5), subject=1, trial="A", db=db)

        r = check_node_state(scale_raw, [WfVariantOut], db=db)
        assert r["state"] == "red"
        # One superseded input × 2 variants = 2 needs-run combos. The superseded
        # old (1,A) input is not counted (latest-record selection), so up_to_date
        # is 6 (3 surviving combos × 2 variants), not 8.
        assert r["counts"]["missing"] == 2, (
            f"Expected 2 needs-run combos (one per variant), got {r['counts']}"
        )
        assert r["counts"]["up_to_date"] == 6
