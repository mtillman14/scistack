"""
A constant is not a variant axis.

``branch_params_batch`` returns every *upstream constant* of a record, whether
or not it varies — the name promises a branch, the query does not check for one.
``attach_variants`` used to turn all of them into variant columns, so a function
called once with a fixed argument contributed a one-level factor: tagged
``variant`` in the Factors list, carrying a role selector, and indistinguishable
from a genuinely swept parameter until you counted its levels.

Measured on a real project 2026-09-11 (`scidb.log`): ``filterDelsys.config`` and
``filterDelsys.Fs`` each held ONE level across both records of ``FilteredEMG``
and both demanded a role. Code axes never had this problem — scidb's
``code_version_ordinals`` omits single-version functions — so this restores the
symmetry the two kinds of axis were always supposed to have.

See ``.claude/plan-plot-studio-variant-axis-fixes.md`` Finding 1.
"""

from __future__ import annotations

import numpy as np
import pytest
from scidb import EachOf, for_each

from scistackplotdb import load_variable

from conftest import Scaled, Signal


def _scale(signal, gain, offset):
    return float(np.mean(signal) * gain + offset)


@pytest.fixture
def one_swept_one_fixed(seeded):
    """``gain`` is swept over two values; ``offset`` is the same every time.

    The pairing is the point: a fixture with only a constant could pass by
    dropping branch params wholesale, and one with only a sweep could pass by
    keeping them wholesale. Both must be present for the assertions to mean
    anything.
    """
    for_each(
        _scale,
        inputs={"signal": Signal, "gain": EachOf(2.0, 3.0), "offset": 0.5},
        outputs=[Scaled],
        subject=[],
        session=[],
        trial=[],
    )
    return seeded


# --- the scenario is real -------------------------------------------------


def test_both_params_are_recorded_as_branch_params(one_swept_one_fixed):
    """Precondition. scidb records the constant too — this fix is about what
    the PLOTTING layer does with it, not about changing what scidb stores."""
    from scidb.provenance_query import branch_params_batch

    loaded = load_variable(one_swept_one_fixed, "Scaled")
    params = branch_params_batch(
        one_swept_one_fixed._duck, loaded.frame["record_id"].tolist()
    )

    keys = {key for bp in params.values() for key in bp}
    assert "_scale.gain" in keys
    assert "_scale.offset" in keys, (
        "if scidb stops recording constants this test file is obsolete, not "
        "failing — but the fix below would then be unnecessary, so say so here"
    )


# --- what we actually want ------------------------------------------------


def test_the_swept_param_is_an_axis(one_swept_one_fixed):
    loaded = load_variable(one_swept_one_fixed, "Scaled")

    assert "_scale.gain" in loaded.variant_columns
    assert set(loaded.frame["_scale.gain"]) == {"2.0", "3.0"}


def test_the_constant_param_is_not_an_axis(one_swept_one_fixed):
    """The regression. One value cannot separate two records, so offering it as
    a factor asks the user to choose between one thing."""
    loaded = load_variable(one_swept_one_fixed, "Scaled")

    assert "_scale.offset" not in loaded.variant_columns
    assert "_scale.offset" not in loaded.frame.columns


def test_the_constant_param_is_not_in_variant_axes(one_swept_one_fixed):
    """``variant_axes`` feeds the selection popup. An axis with one level there
    draws a control whose every setting shows the same data."""
    loaded = load_variable(one_swept_one_fixed, "Scaled")

    columns = {axis["column"] for axis in loaded.variant_axes}
    assert columns == {"_scale.gain"}


def test_the_swept_param_still_reaches_the_factor_list(one_swept_one_fixed):
    """The other half of the guard: dropping constants must not drop the axis
    the user actually swept. It stays a factor, and stays tagged ``variant`` so
    the Variants section can answer it."""
    from scistackplotdb import ScidbSource

    table = ScidbSource(one_swept_one_fixed).get_table(["Scaled"])
    gain = table.factor("_scale.gain")

    assert gain.is_variant
    assert len(gain.levels) == 2
    assert "_scale.offset" not in table.factor_names


# --- the boundary case ----------------------------------------------------


def test_a_param_present_on_only_some_records_survives(seeded):
    """Absence counts as a value.

    Two DIFFERENTLY NAMED functions write ``Scaled``: one takes ``offset``, the
    other does not. The names differ deliberately — same-named bodies would be a
    version edit, and the newer would supersede the older instead of coexisting
    (`.claude/branch-params-identical-constants-analysis.md` §7: differing
    ``version_keys`` put the two call sites in different partitions, so both
    records survive). Coexisting is the whole precondition.

    The records genuinely differ, and ``scale_with_offset.offset`` is what says
    so. Testing only the non-null values would call this column a constant and
    drop it, reintroducing exactly the overplotting the mechanism exists to
    prevent — which is why the rule counts absence as a value.
    """

    def scale_with_offset(signal, offset):
        return float(np.mean(signal) + offset)

    def scale_plain(signal):
        return float(np.mean(signal))

    for_each(
        scale_with_offset,
        inputs={"signal": Signal, "offset": 0.5},
        outputs=[Scaled],
        subject=[],
        session=[],
        trial=[],
    )
    for_each(
        scale_plain,
        inputs={"signal": Signal},
        outputs=[Scaled],
        subject=[],
        session=[],
        trial=[],
    )

    loaded = load_variable(seeded, "Scaled")
    column = "scale_with_offset.offset"

    assert len(loaded.frame) == 2 * 3 * 2 * 2, (
        "precondition: both call sites' records must coexist, or the column "
        "has nothing to distinguish"
    )
    assert column in loaded.variant_columns, (
        "one non-null value, but the records lacking it are different records — "
        "dropping this column makes them replicates"
    )
    assert loaded.frame[column].isna().any(), "the records without it are the point"
    assert loaded.frame[column].notna().any()
