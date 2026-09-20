"""Identity parity: forward == backward, for every id the system computes.

Stage 1 of `.claude/plan-architecture-2026-09-20.md`. Every bug of the
2026-09-19 session had one shape — two derivations of one fact, agreeing by
convention — and this is the test tier that would have caught each of them
at authoring time: run something for REAL, reconstruct its identity from
what provenance recorded, and assert the two are the same bytes.

Three identities, three questions:

* **invocation_id** — is the id reconstructible from the stored graph alone
  (function hash, run options, edges with selectors, constants)? If not,
  something that affected identity was never written down.
* **call_id** — does the id a call computes BEFORE saving
  (``ForEachConfig.to_call_id``) equal the id reconstructed from its records
  AFTER (``pipeline_variants[].call_id`` / ``config_call_id``)?
  ``check_node_state`` compares exactly these two, so a mismatch is a node
  that can never plan green.
* **selector** — is what a call ASKED for per parameter what the edges
  RECORD?

A shape whose two sides are KNOWN to disagree is pinned as
``xfail(strict=True)`` with the disagreement stated, so the decision it
needs is an executable to-do rather than a comment.
"""

from __future__ import annotations

import pandas as pd
import pytest
import scifor as _scifor
from scifor import Fixed, PathInput

from scidb import BaseVariable, configure_database, for_each
from scidb import provenance_query as pq
from scidb.foreach_config import ForEachConfig
from scidb.provenance import compute_invocation_id, constant_record_id_from_hash
from scidb.provenance_save import compute_input_selectors

KEYS = ["subject", "trial", "cycle"]

#: Aggregation edges — several records consumed under one parameter — were
#: recorded under INDEXED names (`value_0`, `value_1`) until 2026-09-20, so
#: every backward reconstruction named parameters the forward call did not
#: have. They are written under the real name now, several edges per param,
#: and the aggregation cases below are the regression guard.


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "parity.duckdb", KEYS)
    yield db
    _scifor.set_schema([])
    db.close()


class Wide(BaseVariable):
    pass


class Out(BaseVariable):
    pass


class Ref(BaseVariable):
    """A subject-level reference record, read fully pinned."""


def _seed():
    for subject in ("01", "02"):
        for trial in ("1", "2"):
            for cycle in ("1", "2"):
                Wide.save(
                    pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]}),
                    subject=subject,
                    trial=trial,
                    cycle=cycle,
                )


def first_a(value):
    return float(pd.Series(value).iloc[0]) if not isinstance(value, pd.DataFrame) else float(value["a"].iloc[0])


def scaled(value, factor):
    return float(len(value)) * factor


def pooled(value):
    return float(len(value))


def with_ref(value, ref):
    # `ref` is a one-row, one-column record, so it arrives as its VALUE — the
    # same shape a plain input of that record would take. Until 2026-09-20 a
    # Fixed input leaked its `__rid_ref` column (it was never one of scifor's
    # extended schema keys, so scifor counted it as data), which made the
    # frame two data columns wide and kept it a DataFrame. The selection seam
    # drops `__record_id` before scifor extracts, so the leak is gone.
    ref_value = ref["r"].iloc[0] if isinstance(ref, pd.DataFrame) else ref
    return float(len(value)) + float(ref_value)


def widen(value, factor):
    """Keeps the a/b columns, so a column selection downstream names a real
    one; `factor` gives the output a branch param to pin on."""
    df = pd.DataFrame(value)
    return df[["a", "b"]] * factor


# ---------------------------------------------------------------------------
# invocation_id: reconstructible from the graph
# ---------------------------------------------------------------------------


def _recompute_invocation_ids(db, type_name: str) -> list[tuple[str, str]]:
    """``[(stored, recomputed)]`` for every record of *type_name*."""
    duck = db._duck
    out = []
    for (rid,) in duck._fetchall("SELECT record_id FROM _record WHERE type = ?", [type_name]):
        inv = pq.producing_invocation(duck, rid)
        if inv is None:
            continue
        inv_id, _fn, fn_hash = inv
        sig = pq.stored_invocation_signature(duck, rid)
        as_table, distribute, across_variants = duck._fetchone(
            "SELECT as_table, distribute, across_variants FROM _invocation "
            "WHERE invocation_id = ?",
            [inv_id],
        )
        bindings = [(p, r, s) for p, edges in sig["var_inputs"].items() for r, s in edges]
        bindings += [
            (p, constant_record_id_from_hash(h), None) for p, h in sig["const_hashes"].items()
        ]
        out.append(
            (
                inv_id,
                compute_invocation_id(
                    fn_hash, as_table, distribute, bindings, across_variants=across_variants
                ),
            )
        )
    return out


class TestInvocationIdIsReconstructible:
    def _assert_parity(self, db):
        pairs = _recompute_invocation_ids(db, "Out")
        assert pairs, "no records produced"
        mismatched = [(s, r) for s, r in pairs if s != r]
        assert not mismatched, (
            f"{len(mismatched)}/{len(pairs)} invocation id(s) cannot be rebuilt from "
            f"the stored edges — something that affected identity was not recorded"
        )

    def test_plain_input(self, db):
        _seed()
        for_each(first_a, {"value": Wide}, [Out], subject=[], trial=[], cycle=[])
        self._assert_parity(db)

    def test_column_selection(self, db):
        _seed()
        for_each(first_a, {"value": Wide["a"]}, [Out], subject=[], trial=[], cycle=[])
        self._assert_parity(db)

    def test_for_columns(self, db):
        _seed()
        for_each(first_a, {"value": Wide.for_columns()}, [Out], subject=[], trial=[], cycle=[])
        self._assert_parity(db)

    def test_aggregation_with_a_selection(self, db):
        """The 2026-09-19 bug: aggregation rows wrote their edges without the
        selector, so the stored graph could not rebuild the id."""
        _seed()
        for_each(pooled, {"value": Wide["a"]}, [Out], subject=[], trial=[])
        self._assert_parity(db)

    def test_constants(self, db):
        _seed()
        for_each(scaled, {"value": Wide, "factor": 2.0}, [Out], subject=[], trial=[], cycle=[])
        self._assert_parity(db)

    def test_fixed_reference(self, db):
        """A pinned reference record is an EDGE: without it the id cannot be
        rebuilt, and until 2026-09-20 a partial pin recorded none."""
        _seed()
        Ref.save(pd.DataFrame({"r": [1.0]}), subject="01")
        Ref.save(pd.DataFrame({"r": [2.0]}), subject="02")
        for_each(
            with_ref,
            {"value": Wide, "ref": Fixed(Ref, subject="01")},
            [Out],
            subject=[],
            trial=[],
            cycle=[],
        )
        self._assert_parity(db)
        edges = db._duck._fetchall(
            "SELECT DISTINCT param_name FROM _invocation_input ii "
            "JOIN _invocation inv ON inv.invocation_id = ii.invocation_id "
            "WHERE inv.function_name = ?",
            ["with_ref"],
        )
        assert {p for (p,) in edges} == {"value", "ref"}, edges


# ---------------------------------------------------------------------------
# call_id: forward (before save) == backward (from records)
# ---------------------------------------------------------------------------


def _backward_call_ids(db, fn_name: str) -> set[str]:
    return {v["call_id"] for v in db.list_pipeline_variants() if v["function_name"] == fn_name}


class TestCallIdForwardEqualsBackward:
    def _check(self, db, fn, inputs, iterate, **options):
        # Built BEFORE the run: if for_each mutated a spec in place, the id
        # computed afterwards would silently describe a different call.
        config = ForEachConfig(fn, inputs, **options)
        forward = config.to_call_id()
        for_each(fn, inputs, [Out], **iterate, **options)
        assert ForEachConfig(fn, inputs, **options).to_call_id() == forward, (
            "for_each mutated the inputs dict in place"
        )
        variants = [v for v in db.list_pipeline_variants() if v["function_name"] == fn.__name__]
        backward = {v["call_id"] for v in variants}
        assert backward, "no variant recorded"
        forward_keys = {
            "__fn": config.to_version_keys()["__fn"],
            "__inputs": config.call_site_inputs(),
            "__constants": config.to_version_keys().get("__constants"),
        }
        assert forward in backward, (
            f"forward call_id {forward} is not among pipeline_variants' {sorted(backward)}\n"
            f"  forward keys:  {forward_keys}\n"
            f"  backward rows: "
            + "; ".join(
                f"input_types={v.get('input_types')} constants={v.get('constants')} "
                f"run_options={v.get('run_options')}"
                for v in variants
            )
        )
        # The SECOND backward reconstruction — the one check_node_state
        # compares a pipeline step's forward id against.
        via_configs = {
            pq.config_call_id(fn.__name__, cfg)
            for cfg in pq.function_variant_configs(db._duck, fn.__name__)
        }
        assert forward in via_configs, (
            f"forward call_id {forward} is not among config_call_id's {sorted(via_configs)}: "
            f"check_node_state would never match this call site"
        )

    def test_plain_input(self, db):
        _seed()
        self._check(db, first_a, {"value": Wide}, dict(subject=[], trial=[], cycle=[]))

    def test_constant(self, db):
        _seed()
        self._check(
            db, scaled, {"value": Wide, "factor": 2.0}, dict(subject=[], trial=[], cycle=[])
        )

    def test_column_selection(self, db):
        """A column selection is invocation identity (the selector), not
        call-site identity — the canvas draws one node for `Var` and
        `Var["a"]`, and the forward id agrees since 2026-09-20."""
        _seed()
        self._check(db, first_a, {"value": Wide["a"]}, dict(subject=[], trial=[], cycle=[]))

    def test_for_columns(self, db):
        _seed()
        self._check(
            db, first_a, {"value": Wide.for_columns()}, dict(subject=[], trial=[], cycle=[])
        )

    def test_as_table(self, db):
        _seed()
        self._check(db, pooled, {"value": Wide}, dict(subject=[], trial=[]), as_table=["value"])

    def test_as_table_true(self, db):
        """`as_table=True` — "every loadable input" — is resolved to names by
        the ONE assembly (`CallSite`); the forward id hashed the literal
        `True` and the backward id the names until 2026-09-20, so this call
        site never matched its own records."""
        _seed()
        self._check(db, pooled, {"value": Wide}, dict(subject=[], trial=[]), as_table=True)
        assert (
            ForEachConfig(pooled, {"value": Wide}, as_table=True).to_call_id()
            == ForEachConfig(pooled, {"value": Wide}, as_table=["value"]).to_call_id()
        )

    def test_distribute(self, db):
        _seed()

        def spread(value):
            return pd.DataFrame({"cycle": ["1", "2"], "x": [1.0, 2.0]})

        self._check(db, spread, {"value": Wide}, dict(subject=[], trial=[]), distribute=True)

    def test_path_input(self, db, tmp_path):
        _seed()  # so `subject=[]` resolves to 01/02 from the database
        root = tmp_path / "files"
        for subject in ("01", "02"):
            (root / subject).mkdir(parents=True, exist_ok=True)
            (root / subject / "note.txt").write_text(subject)

        def read_note(path):
            with open(path) as handle:
                return float(len(handle.read()))

        template = PathInput("{subject}/note.txt", root_folder=str(root))
        self._check(db, read_note, {"path": template}, dict(subject=[]))

    def test_fixed_full_iteration(self, db):
        """A Fixed pin is the EDGE (the pinned record id), not the call site —
        one node on the canvas for every pin of one type. Decided 2026-09-20.
        The realistic shape: a reference record, fully pinned, read beside a
        per-combo input."""
        _seed()
        Ref.save(pd.DataFrame({"r": [1.0]}), subject="01")
        Ref.save(pd.DataFrame({"r": [2.0]}), subject="02")
        self._check(
            db,
            with_ref,
            {"value": Wide, "ref": Fixed(Ref, subject="01")},
            dict(subject=[], trial=[], cycle=[]),
        )

    def test_fixed_aggregation(self, db):
        """The same pin on an aggregating call — the path that recorded NO edge
        for a Fixed input at all until 2026-09-20."""
        _seed()
        Ref.save(pd.DataFrame({"r": [1.0]}), subject="01")
        Ref.save(pd.DataFrame({"r": [2.0]}), subject="02")
        self._check(
            db,
            with_ref,
            {"value": Wide, "ref": Fixed(Ref, subject="01")},
            dict(subject=[], trial=[]),
        )


# ---------------------------------------------------------------------------
# selector: asked == recorded
# ---------------------------------------------------------------------------


def _recorded_selectors(db, fn_name: str) -> dict:
    out: dict = {}
    for cfg in pq.function_variant_configs(db._duck, fn_name):
        out.update(cfg.get("selectors") or {})
    return out


class TestSelectorAskedEqualsRecorded:
    def _check(self, db, fn, inputs, iterate):
        from scidb.foreach import _resolve_for_columns

        for_each(fn, inputs, [Out], **iterate)
        # What the call asked for, AFTER `for_columns()` has been resolved to
        # the concrete column list — the same step the save path runs before
        # it computes the selectors it records.
        resolved = _resolve_for_columns(inputs, db)
        asked = {p: s for p, s in compute_input_selectors(resolved).items() if s}
        recorded = _recorded_selectors(db, fn.__name__)
        assert recorded == asked, f"asked {asked}, recorded {recorded}"

    def test_column_selection_full_iteration(self, db):
        _seed()
        self._check(db, first_a, {"value": Wide["a"]}, dict(subject=[], trial=[], cycle=[]))

    def test_for_columns_full_iteration(self, db):
        _seed()
        self._check(db, first_a, {"value": Wide.for_columns()}, dict(subject=[], trial=[], cycle=[]))

    def test_column_selection_aggregation(self, db):
        _seed()
        self._check(db, pooled, {"value": Wide["a"]}, dict(subject=[], trial=[]))

    def test_for_columns_aggregation(self, db):
        """The exact shape of the 2026-09-19 bug."""
        _seed()
        self._check(db, first_a, {"value": Wide.for_columns()}, dict(subject=[], trial=[]))

    def test_no_selection_records_none(self, db):
        _seed()
        self._check(db, first_a, {"value": Wide}, dict(subject=[], trial=[], cycle=[]))


# ---------------------------------------------------------------------------
# What the parity buys: an aggregating step plans green and skips on re-run
# ---------------------------------------------------------------------------


class TestAggregationIsWholeAgain:
    """The two consumers that broke while the vocabulary was split."""

    def test_an_aggregating_step_plans_green_after_its_run(self, db):
        from scidb.state import check_node_state

        _seed()
        for_each(pooled, {"value": Wide}, [Out], subject=[], trial=[], as_table=["value"])
        forward = ForEachConfig(pooled, {"value": Wide}, as_table=["value"]).to_call_id()
        node = check_node_state(pooled, [Out], inputs={"value": Wide}, db=db, call_id=forward)
        assert node["state"] == "green", node

    def test_an_aggregating_rerun_skips_every_combo(self, db, caplog):
        import logging

        _seed()
        for_each(pooled, {"value": Wide}, [Out], subject=[], trial=[], as_table=["value"])
        before = len(Out.load(as_df=True, version="all"))
        with caplog.at_level(logging.INFO):
            for_each(
                pooled,
                {"value": Wide},
                [Out],
                subject=[],
                trial=[],
                as_table=["value"],
                skip_computed=True,
            )
        assert len(Out.load(as_df=True, version="all")) == before
        assert "skip_computed: 4/4 combos skipped" in caplog.text, caplog.text


# ---------------------------------------------------------------------------
# aggregation over variants: one invocation per variant group, predicted
# ---------------------------------------------------------------------------


class Scaled(BaseVariable):
    """A per-cycle record that exists in two variants (`scaled` at 2 and 3)."""


class Widened(BaseVariable):
    """Two-column per-cycle records, for selecting a column of a variant."""


def _seed_variants():
    _seed()
    for factor in (2.0, 3.0):
        for_each(
            scaled, {"value": Wide, "factor": factor}, [Scaled], subject=[], trial=[], cycle=[]
        )


class TestVariantSplitIsPredicted:
    """An aggregating call over variant records auto-splits: one invocation
    per variant group per location (`scidb.bindings.RecordPool`). Until
    2026-09-20 the expected-invocation predictor pooled the groups into one
    invocation that was never written, so such a node could never plan
    green — the "known gap" both sides now close with ONE signature recipe
    (`variant_signature`)."""

    def test_the_run_writes_one_invocation_per_group(self, db):
        _seed_variants()
        for_each(pooled, {"value": Scaled}, [Out], subject=[], trial=[])
        # 2 subjects x 2 trials x 2 variant groups
        n = db._duck._fetchone(
            "SELECT COUNT(DISTINCT invocation_id) FROM _invocation WHERE function_name = ?",
            ["pooled"],
        )[0]
        assert n == 8, n

    def test_every_written_invocation_is_predicted(self, db):
        _seed_variants()
        for_each(pooled, {"value": Scaled}, [Out], subject=[], trial=[])
        from scilineage.hashing import compute_function_hash

        expected = pq.expected_invocations_for_function(
            db, "pooled", compute_function_hash(pooled, truncate=16)
        )
        written = {
            inv
            for (inv,) in db._duck._fetchall(
                "SELECT invocation_id FROM _invocation WHERE function_name = ?", ["pooled"]
            )
        }
        predicted = {inv for inv, _sid in expected}
        assert predicted == written, (
            f"predicted-only: {sorted(predicted - written)}, "
            f"written-only: {sorted(written - predicted)}"
        )

    def test_the_step_plans_green_after_its_run(self, db):
        from scidb.state import check_node_state

        _seed_variants()
        for_each(pooled, {"value": Scaled}, [Out], subject=[], trial=[])
        forward = ForEachConfig(pooled, {"value": Scaled}).to_call_id()
        node = check_node_state(pooled, [Out], inputs={"value": Scaled}, db=db, call_id=forward)
        assert node["state"] == "green", node


class TestTheDeclarationOnlyPredictsForANeverRunCallSite:
    """A declaration says which TYPES feed which parameters; it cannot say
    at which schema level the call iterates, because that is per run. So the
    never-run fallback always predicts one invocation per input LOCATION —
    right for a node that has never run, wrong for an aggregating one that
    has, whose per-location invocations were never written.

    It went unnoticed because the only aggregating node-state test used
    `as_table`, whose call id differs from the option-less shape
    `config_from_inputs` builds — so the fallback was skipped there for an
    unrelated reason.
    """

    def test_a_plain_aggregating_node_plans_green(self, db):
        from scidb.state import check_node_state

        _seed()
        for_each(pooled, {"value": Wide}, [Out], subject=[], trial=[])
        forward = ForEachConfig(pooled, {"value": Wide}).to_call_id()
        node = check_node_state(pooled, [Out], inputs={"value": Wide}, db=db, call_id=forward)
        assert node["state"] == "green", node

    def test_a_never_run_node_still_predicts_from_its_declaration(self, db):
        """The fallback's real job: with no history there is nothing else to
        predict from, and the node must read red rather than green-by-default."""
        from scidb.state import check_node_state

        _seed()
        node = check_node_state(pooled, [Out], inputs={"value": Wide}, db=db)
        assert node["state"] == "red", node
        assert node["counts"]["missing"] > 0, node

    def test_a_rerun_skips_every_group(self, db, caplog):
        import logging

        _seed_variants()
        for_each(pooled, {"value": Scaled}, [Out], subject=[], trial=[])
        before = len(Out.load(as_df=True, version="all"))
        with caplog.at_level(logging.INFO):
            for_each(pooled, {"value": Scaled}, [Out], subject=[], trial=[], skip_computed=True)
        assert len(Out.load(as_df=True, version="all")) == before
        assert "skip_computed: 8/8 combos skipped" in caplog.text, caplog.text

    def test_invocation_ids_rebuild_from_the_graph(self, db):
        _seed_variants()
        for_each(pooled, {"value": Scaled}, [Out], subject=[], trial=[])
        pairs = _recompute_invocation_ids(db, "Out")
        assert pairs and all(s == r for s, r in pairs)


class TestAcrossVariantsIsAFact:
    """`AcrossVariants(X)` pools every variant group into ONE call. Nothing
    on the edges says so (a pooled call and a one-group split call write
    the same edges), so it is recorded as a run option —
    `_invocation.across_variants`, folded into the invocation id like
    `as_table` — and both backward reconstructions read it: the call id
    (forward `__across_variants` == backward) and the expected-invocation
    predictor (pool, don't split). Until 2026-09-20 neither could see it and
    a pooled step could never plan green."""

    def _run(self):
        from scidb import AcrossVariants

        _seed_variants()
        for_each(pooled, {"value": AcrossVariants(Scaled)}, [Out], subject=[], trial=[])

    def test_the_run_writes_one_invocation_per_location(self, db):
        self._run()
        rows = db._duck._fetchall(
            "SELECT across_variants FROM _invocation WHERE function_name = ?", ["pooled"]
        )
        assert len(rows) == 4, rows  # 2 subjects x 2 trials, both groups pooled
        assert all(list(av) == ["value"] for (av,) in rows), rows

    def test_call_id_forward_equals_backward(self, db):
        from scidb import AcrossVariants

        self._run()
        forward = ForEachConfig(pooled, {"value": AcrossVariants(Scaled)}).to_call_id()
        assert forward in _backward_call_ids(db, "pooled")
        via_configs = {
            pq.config_call_id("pooled", cfg)
            for cfg in pq.function_variant_configs(db._duck, "pooled")
        }
        assert forward in via_configs
        # ...and differs from the split call site's id: two different call sites.
        assert forward != ForEachConfig(pooled, {"value": Scaled}).to_call_id()

    def test_every_written_invocation_is_predicted(self, db):
        from scilineage.hashing import compute_function_hash

        self._run()
        expected = pq.expected_invocations_for_function(
            db, "pooled", compute_function_hash(pooled, truncate=16)
        )
        written = {
            inv
            for (inv,) in db._duck._fetchall(
                "SELECT invocation_id FROM _invocation WHERE function_name = ?", ["pooled"]
            )
        }
        predicted = {inv for inv, _sid in expected}
        assert predicted == written, (
            f"predicted-only: {sorted(predicted - written)}, "
            f"written-only: {sorted(written - predicted)}"
        )

    def test_the_step_plans_green_after_its_run(self, db):
        from scidb import AcrossVariants
        from scidb.state import check_node_state

        self._run()
        inputs = {"value": AcrossVariants(Scaled)}
        forward = ForEachConfig(pooled, inputs).to_call_id()
        node = check_node_state(pooled, [Out], inputs=inputs, db=db, call_id=forward)
        assert node["state"] == "green", node

    def test_invocation_ids_rebuild_from_the_graph(self, db):
        self._run()
        pairs = _recompute_invocation_ids(db, "Out")
        assert pairs and all(s == r for s, r in pairs)

    def test_pooled_and_split_ids_differ_on_identical_edges(self):
        """The identity term: the same edges, pooled, are a different call."""
        edges = [("value", "r1", None), ("value", "r2", None)]
        split = compute_invocation_id("h", None, False, edges)
        pooled_id = compute_invocation_id("h", None, False, edges, across_variants=["value"])
        assert split != pooled_id
        # Empty pooling leaves every pre-existing id byte-identical.
        assert compute_invocation_id("h", None, False, edges, across_variants=[]) == split


class TestSkipGateReadsRunOptions:
    """The skip gate compares a candidate's edges and constants; run options
    were invisible to it. Two consequences, both closed 2026-09-20: a record
    produced under OTHER options (pooled where this call splits) with the
    same edge set counted as "already computed", and a `for_columns()` step
    — whose hook was built BEFORE the empty column list was resolved —
    compared `[]` against the concrete recorded columns and recomputed
    forever."""

    def test_for_columns_rerun_skips_every_combo(self, db, caplog):
        import logging

        _seed()
        for_each(first_a, {"value": Wide.for_columns()}, [Out], subject=[], trial=[], cycle=[])
        before = len(Out.load(as_df=True, version="all"))
        with caplog.at_level(logging.INFO):
            for_each(
                first_a,
                {"value": Wide.for_columns()},
                [Out],
                subject=[],
                trial=[],
                cycle=[],
                skip_computed=True,
            )
        assert len(Out.load(as_df=True, version="all")) == before
        import re

        m = re.search(r"skip_computed: (\d+)/(\d+) combos skipped", caplog.text)
        assert m and m.group(1) == m.group(2) != "0", caplog.text

    def test_a_pooled_record_does_not_satisfy_a_split_call(self, db, caplog):
        """One variant group only, so pooled and split consume the SAME edges;
        only the recorded run option tells them apart."""
        import logging

        from scidb import AcrossVariants

        _seed()
        for_each(scaled, {"value": Wide, "factor": 2.0}, [Scaled], subject=[], trial=[], cycle=[])
        for_each(pooled, {"value": AcrossVariants(Scaled)}, [Out], subject=[], trial=[])
        before = len(Out.load(as_df=True, version="all"))
        with caplog.at_level(logging.DEBUG, logger="scidb"):
            for_each(pooled, {"value": Scaled}, [Out], subject=[], trial=[], skip_computed=True)
        assert len(Out.load(as_df=True, version="all")) == before + 4
        assert "run options changed" in caplog.text, caplog.text


class TestSelectionUnderAnyWrapper:
    """A column selection is invocation identity wherever it sits in the
    wrapper stack. `compute_input_selectors` enumerated two stackings by
    hand (bare and under `Fixed`), so one under a `Variant` reached the
    graph as "no selection" — the write-side round-trip guard's exact
    failure case (`docs/claude/input-binding-round-trip.md` §5)."""

    def test_a_selection_under_a_variant_is_recorded(self, db, caplog):
        import logging

        from scidb import Variant

        _seed()
        # `widen` keeps the a/b columns, so `Widened["a"]` names a real one,
        # and its constant gives the records a branch param to pin on.
        for_each(widen, {"value": Wide, "factor": 2.0}, [Widened], subject=[], trial=[], cycle=[])
        with caplog.at_level(logging.WARNING, logger="scidb"):
            for_each(
                first_a,
                {"value": Variant(Widened["a"], factor=2.0)},
                [Out],
                subject=[],
                trial=[],
                cycle=[],
            )
        assert "dropped" not in caplog.text.lower(), caplog.text
        recorded = _recorded_selectors(db, "first_a")
        assert recorded.get("value"), f"no selector recorded, got {recorded}"

    def test_the_call_site_is_the_type_whatever_the_wrapper(self, db):
        """Four spellings of "read Wide", one call site."""
        from scidb import AcrossVariants, Variant
        from scifor import Fixed

        plain = ForEachConfig(first_a, {"value": Wide}).to_call_id()
        for spec in (
            Wide["a"],
            Fixed(Wide, subject="01"),
            Variant(Wide, low_hz=20),
            AcrossVariants(Wide),
        ):
            got = ForEachConfig(first_a, {"value": spec}).to_call_id()
            if isinstance(spec, AcrossVariants):
                # Pooling is a RUN OPTION and does fork the call site.
                assert got != plain, spec
            else:
                assert got == plain, spec


class TestPathInputIsItsTemplate:
    """A PathInput has a real ``.load()`` AND a ``__name__``, so "the
    variable type this binds" must exclude it explicitly — otherwise its
    call-site identity becomes the display string ``PathInput('{s}/a.csv')``
    instead of ``to_key()``, and two templates differing only by
    ``root_folder`` collapse into ONE call site. The exclusion
    ``foreach._is_loadable`` has always had, now in ``input_spec`` too."""

    @staticmethod
    def _fn(path):
        return 1.0

    def test_two_root_folders_are_two_call_sites(self, tmp_path):
        a = PathInput("{subject}/a.csv", root_folder=str(tmp_path / "one"))
        b = PathInput("{subject}/a.csv", root_folder=str(tmp_path / "two"))
        assert (
            ForEachConfig(self._fn, {"path": a}).to_call_id()
            != ForEachConfig(self._fn, {"path": b}).to_call_id()
        )

    def test_the_call_site_carries_the_to_key_not_the_display_name(self, tmp_path):
        pi = PathInput("{subject}/a.csv", root_folder=str(tmp_path))
        site = ForEachConfig(self._fn, {"path": pi}).call_site_inputs()
        assert site["path"] == pi.to_key()
        assert "root_folder" in site["path"]


class TestAFixedInputArrivesLikeAnyOther:
    """A Fixed input's record reaches the function in the same shape a plain
    input's would: a one-row, one-column record as its value, never with a
    bookkeeping column attached. Until 2026-09-20 `__rid_ref` leaked into the
    frame (it was never one of scifor's extended schema keys, so scifor
    treated it as data), which silently widened every Fixed input to a
    DataFrame."""

    def test_no_internal_column_and_a_scalar_record_is_a_scalar(self, db):
        _seed()
        Ref.save(pd.DataFrame({"r": [1.0]}), subject="01")
        seen: list = []

        def probe(value, ref):
            seen.append(ref)
            return 1.0

        for_each(
            probe,
            {"value": Wide, "ref": Fixed(Ref, subject="01")},
            [Out],
            subject=[],
            trial=[],
            cycle=[],
        )
        assert seen, "the function never ran"
        for ref in seen:
            if isinstance(ref, pd.DataFrame):
                assert not any(str(c).startswith("__") for c in ref.columns), list(ref.columns)
            assert float(ref if not isinstance(ref, pd.DataFrame) else ref["r"].iloc[0]) == 1.0
