"""
Stage 8: variants pool WITHIN a variable, never across two.

Raised by the user 2026-09-11: "for aggregating across Variants, that should
only ever happen WITHIN a Variable. There should never be Variants of Var1
aggregated with Variants of Var2, just because they're all called 'Variants'."

It was a real latent bug, reachable the moment Stage 8 made FREE/AGGREGATE legal
on a variant factor:

* ``apply_variant_sets`` folds every row of every variable into ONE ``Variant``
  factor — three variants each of EMG and force give it six levels;
* ``_answered`` stripped ``Variable`` from the factor list, so it never reached
  ``roles``;
* ``_collapse_aggregates`` builds its groupby key from the ROLES, so a column
  with no role is not in it — and ``Variant = AGGREGATE`` averaged EMG together
  with force. ``FREE`` + a distribution kind is the same failure.

The fix is a ``Variable`` factor that appears exactly when ``Variant`` does not
already separate the variables by itself, defaults to FACET, and is refused
FREE/AGGREGATE outright.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import (
    DataFrameSource,
    PlotKind,
    PlotSpec,
    Role,
    RoleError,
    VariantSet,
    apply_variant_sets,
    resolve,
    validate,
)
from scistackplot.resolved import Y
from scistackplot.variants import VARIABLE_COLUMN, VARIANT_FACTOR


def _two_variable_source() -> DataFrameSource:
    """Two measures at two variant levels, over two subjects.

    ``bandpass.low_hz`` is a branch param, so it is a VARIANT axis — the thing
    that folds into ``Variant`` and would otherwise pool across ``Variable``.
    """
    rows = []
    for subject in ("01", "02"):
        for low_hz in ("20", "40"):
            rows.append(
                {
                    "subject": subject,
                    "bandpass.low_hz": low_hz,
                    # Deliberately far apart: an average ACROSS the two lands
                    # between them and is unmistakable in an assertion.
                    "FilteredEMG": 1.0,
                    "Force": 100.0,
                }
            )
    return DataFrameSource(
        pd.DataFrame(rows),
        factors=["subject", "bandpass.low_hz"],
        measures=["FilteredEMG", "Force"],
        variant_factors=["bandpass.low_hz"],
        name="FilteredEMG",
    )


def _stacked():
    return _two_variable_source().get_table(["FilteredEMG", "Force"])


def _one_row_each_spec(**overrides) -> PlotSpec:
    """One row per variable — the ordinary "stack RawEMG onto FilteredEMG" case.

    ``Variant``'s levels ARE the variables here, so ``Variable`` must NOT also
    appear: the same distinction arriving as both a colour and a facet.
    """
    base = dict(
        measures=["FilteredEMG"],
        variant_sets=[VariantSet(), VariantSet(variable="Force")],
        kind=PlotKind.SCATTER,
    )
    base.update(overrides)
    return PlotSpec(**base)


def _variants_within_each_spec(**overrides) -> PlotSpec:
    """TWO variants of EACH variable — four rows, two variables.

    The shape the user described ("3 Variants of Var1 and 3 of Var2").
    ``Variant`` is a flat 4-level factor whose levels span both variables, so
    ``Variable`` has to stay a factor to stop an aggregate crossing it.
    """
    base = dict(
        measures=["FilteredEMG"],
        variant_sets=[
            VariantSet(selection={"bandpass.low_hz": "20"}),
            VariantSet(selection={"bandpass.low_hz": "40"}),
            VariantSet(variable="Force", selection={"bandpass.low_hz": "20"}),
            VariantSet(variable="Force", selection={"bandpass.low_hz": "40"}),
        ],
        kind=PlotKind.SCATTER,
    )
    base.update(overrides)
    return PlotSpec(**base)


# --- the factor appears exactly when it is needed --------------------------


def test_one_variable_reports_no_variable_factor(scalar_table):
    """With one variable the column is constant; offering a one-level factor
    would be noise, and the row's name already says which variable it is."""
    spec = PlotSpec(measures=["StepLength"], variant_sets=[VariantSet()])

    derived = apply_variant_sets(spec, scalar_table)

    assert VARIABLE_COLUMN not in derived.factor_names


def test_one_row_per_variable_needs_no_variable_factor():
    """``Variant`` already separates them, so a ``Variable`` factor beside it
    would encode the same distinction twice — colour AND facet for one thing.
    This is the common stacking case."""
    derived = apply_variant_sets(_one_row_each_spec(), _stacked())

    assert derived.factor(VARIANT_FACTOR).levels == ["FilteredEMG", "Force"]
    assert VARIABLE_COLUMN not in derived.factor_names


def test_variants_within_each_variable_make_variable_a_factor():
    """Four rows, two variables: ``Variant`` is flat and spans both, so nothing
    else would keep an aggregate from crossing them."""
    derived = apply_variant_sets(_variants_within_each_spec(), _stacked())

    assert len(derived.factor(VARIANT_FACTOR).levels) == 4
    assert VARIABLE_COLUMN in derived.factor_names
    assert derived.factor(VARIABLE_COLUMN).levels == ["FilteredEMG", "Force"]


# --- and it must separate them ---------------------------------------------


def test_variable_defaults_to_facet():
    """Not FREE, which ``validate`` refuses — defaulting to it would hand the
    user an error instead of a figure. FACET is the shape the request came in
    as: "two mean + error band plots", one panel each."""
    from scistackplot import complete_roles

    spec = _variants_within_each_spec()

    roles = complete_roles(spec, apply_variant_sets(spec, _stacked()))

    assert roles[VARIABLE_COLUMN] is Role.FACET


def test_variable_cannot_be_collapsed():
    """Averaging EMG with force is not a figure anyone wants."""
    spec = _variants_within_each_spec(roles={VARIABLE_COLUMN: Role.COLLAPSE})

    with pytest.raises(RoleError, match="different variables"):
        validate(spec, apply_variant_sets(spec, _stacked()))


@pytest.mark.parametrize("role", [Role.GROUP, Role.FACET, Role.ITERATE])
def test_variable_accepts_every_separating_role(role):
    spec = _variants_within_each_spec(roles={VARIABLE_COLUMN: role})

    validate(spec, apply_variant_sets(spec, _stacked()))  # must not raise


# --- the regression itself -------------------------------------------------


def test_aggregating_variants_collapses_WITHIN_each_variable():
    """The bug, stated as an assertion.

    Two variables x two variants x two subjects. Averaging the VARIANT away
    must leave one value per (variable, subject) — four rows, each still 1.0 or
    100.0. An average that crossed ``Variable`` would land at 50.5.

    ``resolve`` takes the RAW table: it folds the variants itself, and handing
    it a pre-folded one applies them twice (two ``Variant`` factors).
    """
    spec = _variants_within_each_spec(
        roles={
            VARIABLE_COLUMN: Role.FACET,
            VARIANT_FACTOR: Role.GROUP,
            "subject": Role.COLLAPSE,
        },
    )

    resolved = resolve(spec, _stacked())[0]
    values = sorted(
        value for panel in resolved.panels for value in panel.frame[Y].tolist()
    )

    assert values == [1.0, 1.0, 100.0, 100.0]


def test_boxed_variants_keep_each_variable_apart():
    """The other renderer, same failure: a distribution draws whatever sample
    rows remain, so a ``Variable`` with no role would put EMG and force into
    one box."""
    spec = _variants_within_each_spec(
        roles={
            VARIABLE_COLUMN: Role.FACET,
            VARIANT_FACTOR: Role.GROUP,
            "subject": Role.COLLAPSE,
        },
        kind=PlotKind.BOX,
    )

    resolved = resolve(spec, _stacked())[0]

    # One panel per variable, and no panel mixes the two magnitudes.
    assert len(resolved.panels) == 2
    for panel in resolved.panels:
        assert panel.frame[Y].nunique() == 1
