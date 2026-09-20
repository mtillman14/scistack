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
        as_table, distribute = duck._fetchone(
            "SELECT as_table, distribute FROM _invocation WHERE invocation_id = ?", [inv_id]
        )
        bindings = [(p, r, s) for p, (r, s) in sig["var_inputs"].items()]
        bindings += [
            (p, constant_record_id_from_hash(h), None) for p, h in sig["const_hashes"].items()
        ]
        out.append((inv_id, compute_invocation_id(fn_hash, as_table, distribute, bindings)))
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


# ---------------------------------------------------------------------------
# call_id: forward (before save) == backward (from records)
# ---------------------------------------------------------------------------


def _backward_call_ids(db, fn_name: str) -> set[str]:
    return {v["call_id"] for v in db.list_pipeline_variants() if v["function_name"] == fn_name}


class TestCallIdForwardEqualsBackward:
    def _check(self, db, fn, inputs, iterate, **options):
        for_each(fn, inputs, [Out], **iterate, **options)
        forward = ForEachConfig(fn, inputs, **options).to_call_id()
        backward = _backward_call_ids(db, fn.__name__)
        assert backward, "no variant recorded"
        assert forward in backward, (
            f"forward call_id {forward} is not among the recorded {sorted(backward)}: "
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

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Fixed metadata forks the FORWARD call_id (test_unified_modifier_classes"
            "::test_different_fixed_metadata_forks_call_id) while the BACKWARD id "
            "collapses it (function_variant_configs keys on the input TYPE; the "
            "pinned record is an edge, not a config). A Fixed-pinned pipeline step "
            "therefore never plans green. Decision needed: is Fixed(subject=1) vs "
            "Fixed(subject=2) one call site or two? The canvas says one."
        ),
    )
    def test_fixed(self, db):
        _seed()
        self._check(
            db,
            pooled,
            {"value": Fixed(Wide, subject="01")},
            dict(trial=[]),
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
        for_each(fn, inputs, [Out], **iterate)
        asked = {p: s for p, s in compute_input_selectors(inputs).items() if s}
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
