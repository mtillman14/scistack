"""Node state for a node whose input is deliberately pinned to one variant.

A ``Variant`` pin is a *declaration*: "run this step over the low_hz=20 records
only". The node has then done everything it was asked to do, and must read as
complete. If instead the expected set is predicted over every variant of the
input, the node expects invocations it was never meant to perform and reads
needs-run forever — no amount of running can make it green.

This is the question Stage 4 of ``.claude/plan-variant-selection.md`` turns on,
and it is **not** specific to code versions: it already applies to the
branch-param pinning ``scidb.Variant`` has supported all along. Establishing
which way the current code behaves is the prerequisite for designing either.

Mechanism, from reading the code (``docs/claude/variant-selection.md`` §8):

* ``config_from_inputs`` unwraps ``Fixed``/``ColumnSelection`` to reach a
  ``type``, but has no ``Variant`` branch — a ``Variant`` instance is not a
  ``type``, so the param never lands in ``input_types``.
* ``_predict_config_invocations`` enumerates ``_current_records_by_schema``,
  which is latest-per-(location, producing-variant) — i.e. **one record per
  variant**, all of them.

Neither carries the pin, so the prediction should cross-product both variants.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, Variant, configure_database, for_each
from scidb.foreach_config import _compute_fn_hash
from scidb.provenance_query import (
    expected_invocations_for_function,
    present_invocation_schema_pairs,
)

SCHEMA = ["subject"]
SUBJECTS = ["01", "02"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "test_variant_pin_state.duckdb", SCHEMA)
    yield database
    _scifor.set_schema([])
    database.close()


class Raw(BaseVariable):
    pass


class Scaled(BaseVariable):
    pass


class Consumed(BaseVariable):
    pass


def scale(raw, factor):
    return float(np.sum(raw)) * factor


def consume(scaled):
    return float(scaled) + 1.0


def _present_for(db, fn_name: str) -> set:
    """The (invocation, schema) pairs ``fn_name`` has actually produced.

    ``present_invocation_schema_pairs`` takes invocation ids, so the realized
    ones are looked up first — "present" is by definition what exists.
    """
    inv_ids = [
        row[0]
        for row in db._duck._fetchall(
            "SELECT invocation_id FROM _invocation WHERE function_name = ?",
            [fn_name],
        )
    ]
    return present_invocation_schema_pairs(db._duck, inv_ids)


@pytest.fixture
def two_variants_then_a_pinned_consumer(db):
    """``Scaled`` exists at two branch-param variants; ``consume`` is declared
    over exactly one of them and run to completion."""
    for subject in SUBJECTS:
        Raw.save(np.array([1.0, 2.0]), subject=subject)

    for factor in (2.0, 3.0):
        for_each(
            scale,
            inputs={"raw": Raw, "factor": factor},
            outputs=[Scaled],
            subject=[],
        )

    for_each(
        consume,
        inputs={"scaled": Variant(Scaled, factor=2.0)},
        outputs=[Consumed],
        subject=[],
    )
    return db


def test_the_pin_actually_narrowed_the_run(two_variants_then_a_pinned_consumer):
    """Precondition. Without this the rest proves nothing: the pin must really
    have produced one output per subject rather than two."""
    db = two_variants_then_a_pinned_consumer

    scaled = db._duck._fetchone("SELECT COUNT(*) FROM _record WHERE type = 'Scaled'")
    consumed = db._duck._fetchone(
        "SELECT COUNT(*) FROM _record WHERE type = 'Consumed'"
    )

    assert scaled[0] == 2 * len(SUBJECTS), "two variants per subject"
    assert consumed[0] == len(SUBJECTS), "the pin should have halved the fan-out"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "The Variant pin is not carried into expected-invocation prediction. "
        "HALF of the original cause is gone since 2026-09-20: "
        "config_from_inputs used to DROP the param entirely (a Variant is not "
        "a type, and that copy of the unwrap did not know about Variant); it "
        "now yields the type, through the one unwrap (scidb.input_spec). What "
        "remains is the other half, and it is a design question rather than a "
        "missing case: _predict_config_invocations enumerates every CURRENT "
        "record of the input type, so the node expects invocations over the "
        "variant it was told not to touch. Unlike AcrossVariants, the pin "
        "cannot simply be recorded as a fact -- it is a load-time filter like "
        "where=, whose whole effect IS the surviving input set, already on the "
        "edges. Narrowing the prediction has to come from the DECLARATION "
        "(inputs_fallback), which only the live-inputs caller has. "
        "Stage 4 of .claude/plan-variant-selection.md."
    ),
)
def test_a_pinned_node_is_complete(two_variants_then_a_pinned_consumer):
    """The whole question, stated once.

    A node that has run every invocation its declaration calls for is done.
    Expected must not exceed present.
    """
    db = two_variants_then_a_pinned_consumer

    expected = expected_invocations_for_function(
        db,
        "consume",
        _compute_fn_hash(consume),
        inputs_fallback={"scaled": Variant(Scaled, factor=2.0)},
    )
    present = _present_for(db, "consume")

    missing = expected - present
    assert not missing, (
        f"{len(missing)} invocation(s) expected but never run — the pin was "
        f"ignored when predicting what this node owes"
    )


def test_an_unpinned_consumer_is_complete(db):
    """Control. The same shape WITHOUT a pin must already be green, or the test
    above is measuring something other than the pin."""
    for subject in SUBJECTS:
        Raw.save(np.array([1.0, 2.0]), subject=subject)
    for_each(
        scale, inputs={"raw": Raw, "factor": 2.0}, outputs=[Scaled], subject=[]
    )
    for_each(consume, inputs={"scaled": Scaled}, outputs=[Consumed], subject=[])

    expected = expected_invocations_for_function(
        db, "consume", _compute_fn_hash(consume), inputs_fallback={"scaled": Scaled}
    )
    present = _present_for(db, "consume")

    assert not (expected - present)


def test_the_pinned_param_reaches_the_predicted_config(
    two_variants_then_a_pinned_consumer,
):
    """Half of the xfail above, now closed and guarded separately.

    ``config_from_inputs`` used to have its own unwrap that did not know
    about ``Variant``, so a pinned input was not merely mis-predicted — it
    was ABSENT, and the config predicted a call with no variable inputs at
    all. One unwrap (``scidb.input_spec``) now, so every identity path sees
    the same set of wrappers.
    """
    from scidb.provenance_query import config_from_inputs

    cfg = config_from_inputs({"scaled": Variant(Scaled, factor=2.0)})
    assert cfg["input_types"] == {"scaled": "Scaled"}, cfg
