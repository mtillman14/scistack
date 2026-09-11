"""
A plot opens on exactly ONE variant.

Plotting one thing is the common case; comparing is what a second row is for.
Before this, only code axes were pinned (``table.default_pin`` = the per-location
``CodeIsLatest`` flag) and branch-param axes were left open — so a variable
produced at five filter cutoffs opened as **five overlaid series**, because
``default_roles`` found a multi-level variant factor with no role and put it on
COLOUR. Nobody asked for that figure, and two pipeline variants drawn on one
axis read as replicates of one condition.

``variants.default_selection`` owns the rule:

* code axes -> the latest body (via the per-location flag);
* branch-param axes -> the first level in declared order;
* applied blindly, so a combination nobody ran selects nothing rather than
  quietly becoming a different combination.

The subtlest thing here is why the pin uses the boolean flag rather than the
string ``"latest"`` — see ``test_pinning_a_param_does_not_turn_latest_global``,
which is the regression that rule exists to prevent.

See ``.claude/plan-default-variant-selection.md``.
"""

from __future__ import annotations

import pandas as pd
from scistackplot import (
    LongTable,
    PlotSpec,
    default_selection,
    default_spec,
    resolve,
    variant_set_mask,
)
from scistackplot.roles import validate
from scistackplot.variants import apply_variant_sets


def _table(
    frame: pd.DataFrame,
    variants: list[str],
    *,
    latest_column: str | None = None,
    level_order: dict | None = None,
) -> LongTable:
    factors = [c for c in frame.columns if c not in ("value", latest_column)]
    return LongTable.from_frame(
        frame,
        factors=factors,
        measures=["value"],
        variant_factors=variants,
        name="value",
        latest_column=latest_column,
        level_order=level_order,
    )


def _swept_param_table() -> LongTable:
    """One subject, one function, five values of one parameter. No code edit."""
    return _table(
        pd.DataFrame(
            {
                "subject": ["01"] * 5,
                "f.Parameter1": ["1", "2", "3", "4", "5"],
                "value": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        ),
        ["f.Parameter1"],
    )


def _ten_variant_table() -> LongTable:
    """The worked example: 2 function bodies x 5 parameter values = 10 variants.

    ``CodeIsLatest`` is True on the v2 rows — one subject, so per-location and
    global "latest" happen to agree here. They are separated in
    ``test_pinning_a_param_does_not_turn_latest_global``.
    """
    rows = [
        {
            "subject": "01",
            "Code:f": code,
            "f.Parameter1": value,
            "CodeIsLatest": code == "v2",
            "value": float(index),
        }
        for index, (code, value) in enumerate(
            (code, value)
            for code in ("v1", "v2")
            for value in ("1", "2", "3", "4", "5")
        )
    ]
    return _table(
        pd.DataFrame(rows),
        ["Code:f", "f.Parameter1"],
        latest_column="CodeIsLatest",
    )


# --- the rule -------------------------------------------------------------


def test_a_swept_param_is_pinned_to_its_first_level():
    table = _swept_param_table()

    assert default_selection(table) == {"f.Parameter1": "1"}


def test_first_means_declared_order_not_row_order():
    """Stability is the only thing that matters about WHICH value it picks, so
    it comes from the level order, not from whichever row happens to be first."""
    frame = pd.DataFrame(
        {
            "subject": ["01"] * 3,
            "f.p": ["10", "2", "1"],  # row order is deliberately not sorted
            "value": [1.0, 2.0, 3.0],
        }
    )
    table = _table(frame, ["f.p"])

    # Natural sort, so "2" precedes "10" — the zero-padding trap in reverse.
    assert table.factor("f.p").levels == ["1", "2", "10"]
    assert default_selection(table) == {"f.p": "1"}


def test_each_axis_is_pinned_independently():
    frame = pd.DataFrame(
        {
            "subject": ["01"] * 4,
            "f.a": ["1", "1", "2", "2"],
            "f.b": ["x", "y", "x", "y"],
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )

    assert default_selection(_table(frame, ["f.a", "f.b"])) == {
        "f.a": "1",
        "f.b": "x",
    }


def test_a_table_with_no_variants_pins_nothing():
    """The CSV path, and any scidb variable whose pipeline never branched. It
    must cost nothing and change nothing.

    Since Stage 7 the spec still carries its one seeded row — the Variants
    section always has something to show — but that row pins NOTHING and is
    inert, which is what "changes nothing" means here. See
    `test_the_seeded_row_stays_inert` for the other half.
    """
    frame = pd.DataFrame({"subject": ["01", "02"], "value": [1.0, 2.0]})
    table = _table(frame, [])

    assert default_selection(table) == {}

    rows = default_spec(table, "value").variant_sets
    assert len(rows) == 1
    assert rows[0].selection == {}
    assert rows[0].variable is None


# --- the worked example ---------------------------------------------------


def test_ten_variants_open_on_one_series():
    """2 bodies x 5 values. The opening figure is one line, not five or ten."""
    table = _ten_variant_table()
    spec = default_spec(table, "value")

    assert len(spec.variant_sets) == 1
    assert spec.variant_sets[0].selection == {
        "CodeIsLatest": True,
        "f.Parameter1": "1",
    }

    resolved = resolve(spec, table)[0]
    assert resolved.row_count == 1, "one variant means one row per location"


def test_the_pinned_axes_leave_the_factor_list():
    """Once the row answers an axis, keeping it as a factor states the same
    thing twice — and as an unassigned multi-level variant factor ``validate``
    would refuse the figure outright."""
    table = _ten_variant_table()
    spec = default_spec(table, "value")
    derived = apply_variant_sets(spec, table)

    assert "Code:f" not in derived.factor_names
    assert "f.Parameter1" not in derived.factor_names
    validate(spec, derived)  # must not raise


def test_the_swept_param_is_no_longer_put_on_colour():
    """The reported symptom, stated as an assertion: five values used to become
    five colours before the user had said anything."""
    table = _swept_param_table()
    spec = default_spec(table, "value")

    assert spec.roles.get("f.Parameter1") is None
    assert resolve(spec, table)[0].row_count == 1


# --- the trap the rule is built around ------------------------------------


def test_pinning_a_param_does_not_turn_latest_global():
    """The whole reason the pin uses the FLAG and not ``Code:f = "latest"``.

    ``resolve_selection`` treats a selection as "nothing else pinned" only when
    every key in it says ``"latest"``. Pin a branch param alongside a
    ``"latest"`` code axis and the code axis stops resolving through the
    per-location flag and becomes the **global highest ordinal** — silently
    dropping every location never re-run under the newest code.

    Subject 02 is on v1 and has no v2 record. It must still be in the figure.
    """
    frame = pd.DataFrame(
        {
            "subject": ["01", "01", "02"],
            "Code:f": ["v1", "v2", "v1"],
            "f.p": ["1", "1", "1"],
            "CodeIsLatest": [False, True, True],  # 02's v1 IS latest, for 02
            "value": [1.0, 2.0, 3.0],
        }
    )
    table = _table(frame, ["Code:f", "f.p"], latest_column="CodeIsLatest")

    selection = default_selection(table)
    assert selection == {"CodeIsLatest": True, "f.p": "1"}
    assert "Code:f" not in selection, (
        'spelling this as Code:f="latest" would make the param pin flip it to '
        "the global highest ordinal"
    )

    kept = frame[variant_set_mask(frame, selection, latest_column="CodeIsLatest")]
    assert set(kept["subject"]) == {"01", "02"}, (
        "subject 02 was never re-run under v2; per-location latest keeps its "
        "own newest record instead of deleting the subject from the figure"
    )


def test_a_code_axis_without_a_latest_flag_falls_back_to_the_highest_ordinal():
    """Impossible from scidb, which always writes the flag beside the code
    columns, but a hand-built or CSV table may declare one without the other."""
    frame = pd.DataFrame(
        {
            "subject": ["01", "01", "01"],
            "Code:f": ["v1", "v2", "v10"],
            "value": [1.0, 2.0, 3.0],
        }
    )

    # v10 beats v9/v2 — the ordinal is compared numerically, not lexically.
    assert default_selection(_table(frame, ["Code:f"])) == {"Code:f": "v10"}


# --- the ragged case ------------------------------------------------------


def test_a_pin_matching_no_rows_resolves_rather_than_raising():
    """Applied blindly, on purpose: a rule that quietly picks a different value
    to avoid an empty figure is no longer predictable. The empty figure is a
    legitimate state and ``resolve`` must render it, not raise — the message
    explaining it is the Variants section's job.
    """
    # Parameter1=1 exists only under v1; the latest code only ever ran value 2.
    frame = pd.DataFrame(
        {
            "subject": ["01", "01"],
            "Code:f": ["v1", "v2"],
            "f.Parameter1": ["1", "2"],
            "CodeIsLatest": [False, True],
            "value": [1.0, 2.0],
        }
    )
    table = _table(
        frame, ["Code:f", "f.Parameter1"], latest_column="CodeIsLatest"
    )

    spec = default_spec(table, "value")
    assert spec.variant_sets[0].selection == {
        "CodeIsLatest": True,
        "f.Parameter1": "1",
    }

    resolved = resolve(spec, table)
    assert resolved, "an empty figure is still a figure"
    assert resolved[0].row_count == 0


# --- Stage 7: the Variants section always has a row, named for its variable --
#
# Reported 2026-09-11: a project with no variant axes opened with an EMPTY
# Variants list, so the section said nothing about the variable being plotted,
# and the user's first row only appeared once they added a second. The names
# were no better — scidb.log 12:26:18 shows two rows called "FilteredEMG" and
# "latest", the second of which plots RawEMG and never says so.


def test_a_table_with_no_variants_still_opens_with_one_row(scalar_table):
    from scistackplot import default_spec

    spec = default_spec(scalar_table, "StepLength")

    assert len(spec.variant_sets) == 1
    assert spec.variant_sets[0].selection == {}


def test_the_seeded_row_stays_inert(scalar_table):
    """It must not make every table grow a one-level `Variant` factor.

    The row carries no `variable` — None means "the primary measure" — so with
    an empty selection `defined_sets` ignores it and `apply_variant_sets`
    returns the table untouched. Naming the measure explicitly would have said
    something, and said it about every project that has no variants at all.
    """
    from scistackplot import apply_variant_sets, default_spec, defined_sets

    spec = default_spec(scalar_table, "StepLength")

    assert spec.variant_sets[0].variable is None
    assert defined_sets(spec.variant_sets) == []
    assert apply_variant_sets(spec, scalar_table) is scalar_table


def test_the_seeded_row_is_named_for_the_measure(scalar_table):
    from scistackplot import default_spec
    from scistackplot.variants import set_name

    spec = default_spec(scalar_table, "StepLength")

    assert set_name(spec.variant_sets[0], 0, primary="StepLength") == "StepLength"


# --- naming: the variable first, the selection as a qualifier ---------------


def test_a_row_names_its_own_variable_over_the_primary():
    from scistackplot import VariantSet
    from scistackplot.variants import set_name

    row = VariantSet(variable="RawEMG")

    assert set_name(row, 0, primary="FilteredEMG") == "RawEMG"


def test_a_row_with_a_variable_and_a_selection_names_both():
    """The reported bug: a row plotting RawEMG was called "latest"."""
    from scistackplot import VariantSet
    from scistackplot.variants import set_name

    row = VariantSet(variable="RawEMG", selection={"bandpass.low_hz": "20"})

    assert set_name(row, 0, primary="FilteredEMG") == "RawEMG · low_hz=20"


def test_the_latest_flag_reads_as_current():
    """`CodeIsLatest=True` in a legend tells a reader nothing."""
    from scistackplot import VariantSet
    from scistackplot.variants import set_name

    row = VariantSet(selection={"CodeIsLatest": True})

    name = set_name(row, 0, primary="Scaled", latest_column="CodeIsLatest")

    assert name == "Scaled · current"


def test_a_false_latest_flag_does_not_read_as_current():
    """"Not the current code" is a real thing to ask for, and must not be
    labelled as its opposite."""
    from scistackplot import VariantSet
    from scistackplot.variants import set_name

    row = VariantSet(selection={"CodeIsLatest": False})

    name = set_name(row, 0, primary="Scaled", latest_column="CodeIsLatest")

    assert name == "Scaled · not CodeIsLatest"


def test_an_explicit_name_wins():
    from scistackplot import VariantSet
    from scistackplot.variants import set_name

    row = VariantSet(name="baseline", variable="RawEMG", selection={"a": "1"})

    assert set_name(row, 0, primary="FilteredEMG") == "baseline"


def test_two_rows_on_one_variable_stay_distinguishable():
    """Names are the Variant factor's LEVELS, so two rows sharing a name would
    merge into one legend entry and silently pool their data."""
    from scistackplot import VariantSet
    from scistackplot.variants import set_name

    rows = [
        VariantSet(selection={"bandpass.low_hz": "20"}),
        VariantSet(selection={"bandpass.low_hz": "40"}),
    ]

    names = [set_name(row, i, primary="FilteredEMG") for i, row in enumerate(rows)]

    assert names == ["FilteredEMG · low_hz=20", "FilteredEMG · low_hz=40"]
    assert len(set(names)) == 2


def test_an_unfilled_row_with_no_primary_is_still_not_set():
    """The library caller's view, where there is no primary to fall back on."""
    from scistackplot import VariantSet
    from scistackplot.variants import set_name

    assert set_name(VariantSet(), 0) == "(not set)"
    assert set_name(VariantSet(), 1) == "(not set 2)"


def test_adding_a_row_beside_the_seeded_one_draws_BOTH():
    """The trap Stage 7 set for itself.

    Row 0 is seeded empty and inert, so a naive `defined_sets` would fold only
    the row the user just added — replacing the figure with that row alone
    instead of drawing it alongside what was already there. The GUI used to
    avoid this by quietly filling in row 0's variable on the way to adding the
    second, which put the rule in TypeScript; it now lives in `defined_sets`.
    """
    from scistackplot import (
        DataFrameSource,
        VariantSet,
        apply_variant_sets,
        defined_sets,
    )
    from scistackplot.variants import VARIANT_FACTOR

    # Stacked through the REAL path rather than hand-built: a stacked table
    # names its value column after the primary measure, and that name is also
    # the first level of `Variable` — which is exactly what lets a row with no
    # `variable` of its own (row 0) claim the primary's rows via
    # `row_mask`'s `variant.variable or spec.y_measure` fallback. A hand-built
    # frame that got this wrong would test nothing real.
    source = DataFrameSource(
        pd.DataFrame(
            {
                "subject": ["01", "01"],
                "FilteredEMG": [1.0, 2.0],
                "RawEMG": [3.0, 4.0],
            }
        ),
        factors=["subject"],
        measures=["FilteredEMG", "RawEMG"],
        name="FilteredEMG",
    )
    table = source.get_table(["FilteredEMG", "RawEMG"])

    spec = PlotSpec(
        measures=["FilteredEMG"],
        variant_sets=[
            VariantSet(),                      # seeded row 0 — the primary
            VariantSet(variable="RawEMG"),     # what "+ Add variant" produces
        ],
    )

    assert len(defined_sets(spec.variant_sets)) == 2

    derived = apply_variant_sets(spec, table)
    levels = [
        f.levels for f in derived.factors if f.name == VARIANT_FACTOR
    ][0]

    assert levels == ["FilteredEMG", "RawEMG"], "the seeded row must still be drawn"
    assert len(derived.frame) == 4


def test_a_lone_seeded_row_still_folds_to_nothing():
    """The other side: one unnarrowed series needs no `Variant` factor, and
    every project without variants must keep paying nothing for this."""
    from scistackplot import VariantSet, apply_variant_sets, defined_sets

    frame = pd.DataFrame({"subject": ["01", "02"], "value": [1.0, 2.0]})
    table = _table(frame, [])
    spec = PlotSpec(measures=["value"], variant_sets=[VariantSet()])

    assert defined_sets(spec.variant_sets) == []
    assert apply_variant_sets(spec, table) is table
