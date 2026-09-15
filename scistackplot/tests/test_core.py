"""Shape classification, spec serialization, role validation, capability rules."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    PlotKind,
    default_spec,
    PlotSpec,
    Role,
    RoleError,
    Shape,
    available_plots,
    capabilities,
    classify_column,
    classify_value,
    default_plot,
    default_roles,
    natural_sort_key,
    validate,
)


# --- shape -----------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        (1, Shape.SCALAR),
        (1.5, Shape.SCALAR),
        (np.float64(2.0), Shape.SCALAR),
        (True, Shape.CATEGORICAL),
        ("pre", Shape.CATEGORICAL),
        ([1.0, 2.0], Shape.SERIES_1D),
        (np.arange(5), Shape.SERIES_1D),
        (np.zeros((3, 3)), Shape.MATRIX_2D),
        ([[1.0], [2.0]], Shape.MATRIX_2D),
        (None, Shape.UNKNOWN),
        ([], Shape.UNKNOWN),
    ],
)
def test_classify_value(value, expected):
    assert classify_value(value) is expected


def test_classify_column_skips_leading_nulls():
    series = pd.Series([None, None, [1.0, 2.0, 3.0]], dtype=object)
    assert classify_column(series) is Shape.SERIES_1D


def test_bool_column_is_categorical_not_scalar():
    assert classify_column(pd.Series([True, False, True])) is Shape.CATEGORICAL


# --- ordering --------------------------------------------------------------


def test_natural_sort_orders_numbers_numerically():
    values = ["10", "2", "1"]
    assert sorted(values, key=natural_sort_key) == ["1", "2", "10"]


def test_natural_sort_handles_prefixed_ids():
    values = ["s10", "s2", "s1"]
    assert sorted(values, key=natural_sort_key) == ["s1", "s2", "s10"]


# --- spec serialization ----------------------------------------------------


def test_spec_json_round_trip():
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.X, "limb": Role.COLOR, "subject": Role.FREE},
        kind=PlotKind.BOX,
    )
    restored = PlotSpec.from_json(spec.to_json())
    assert restored == spec


def test_spec_toml_round_trip():
    pytest.importorskip("tomli_w")
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.COLOR, "trial": Role.FREE},
        kind=PlotKind.BAND,
    )
    assert PlotSpec.from_toml(spec.to_toml()) == spec


def test_spec_round_trip_preserves_factor_variables():
    from scistackplot import FactorVariable

    spec = PlotSpec(
        measures=["StepLength"],
        factor_variables=[
            FactorVariable("Condition"),
            FactorVariable("Demographics", "InterventionGroup"),
        ],
    )

    restored = PlotSpec.from_json(spec.to_json())

    assert restored == spec
    assert restored.factor_variables[1].column == "InterventionGroup"


def test_a_factor_variable_is_named_after_its_column():
    """Roles, filters and y-scoping all key on the factor's name, and the
    endpoint's `as_table` input delivers the column under that same name — so
    the interactive path and the generated code need no rename on either side.
    """
    from scistackplot import FactorVariable

    assert FactorVariable("Demographics", "InterventionGroup").factor_name == (
        "InterventionGroup"
    )
    assert FactorVariable("Demographics", "InterventionGroup").label == (
        "Demographics.InterventionGroup"
    )
    assert FactorVariable("Condition").factor_name == "Condition"


def test_a_bare_grouping_name_is_refused_with_the_new_spelling():
    """Specs written before groupings could name a column carried a bare string.
    Read as-is it would fail three frames down with `string indices must be
    integers`; say what it is and what to write instead."""
    with pytest.raises(ValueError, match='{"variable": "Condition"}'):
        PlotSpec.from_dict({"measures": ["x"], "factor_variables": ["Condition"]})


def test_spec_round_trip_preserves_role_enum_types():
    spec = PlotSpec(measures=["a"], roles={"f": Role.ITERATE})
    restored = PlotSpec.from_json(spec.to_json())
    assert restored.roles["f"] is Role.ITERATE


# --- role validation -------------------------------------------------------


def test_unknown_measure_is_rejected(scalar_table):
    with pytest.raises(RoleError, match="not in the table"):
        validate(PlotSpec(measures=["Nope"]), scalar_table)


def test_two_factors_may_share_the_x_axis(scalar_table):
    """Nested grouping: "stim and sham side by side, each split by session".

    X used to be single-assignment. It is not: which factor is the OUTER
    grouping is an order (``x_layers``), not a different role — see
    test_x_nesting.py for the composition.
    """
    spec = PlotSpec(
        measures=["StepLength"], roles={"subject": Role.X, "session": Role.X}
    )

    validate(spec, scalar_table)  # must not raise


def test_colour_still_accepts_only_one_factor(scalar_table):
    """The single-assignment rule survives where it still makes sense: two
    factors cannot both be the colour channel."""
    spec = PlotSpec(
        measures=["StepLength"], roles={"subject": Role.COLOR, "session": Role.COLOR}
    )
    with pytest.raises(RoleError, match="accepts one factor"):
        validate(spec, scalar_table)


def test_1d_measure_refuses_a_factor_on_x(series_table):
    spec = PlotSpec(measures=["Signal"], roles={"session": Role.X})
    with pytest.raises(RoleError, match="within-observation index"):
        validate(spec, series_table)


def test_second_measure_owns_x(scalar_frame):
    from scistackplot import LongTable

    frame = scalar_frame.assign(Speed=1.0)
    table = LongTable.from_frame(
        frame, factors=["subject", "session", "trial"], measures=["StepLength", "Speed"]
    )
    spec = PlotSpec(
        measures=["StepLength"], x_measure="Speed", roles={"subject": Role.X}
    )
    with pytest.raises(RoleError, match="already supplies the x axis"):
        validate(spec, table)


def test_variant_factor_cannot_be_pooled_silently(variant_table):
    spec = PlotSpec(measures=["Peak"], roles={"subject": Role.X})
    with pytest.raises(RoleError, match="would be pooled"):
        validate(spec, variant_table)


def test_variant_factor_is_fine_once_assigned(variant_table):
    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.X, "bandpass.low_hz": Role.COLOR},
    )
    validate(spec, variant_table)  # does not raise


def test_explicitly_pooling_a_variant_factor_is_allowed(variant_table):
    """The new spelling of the deleted ``variant_policy='pool'``.

    Pooling variants is legal when ASKED for — five variants of a measure
    averaged into one line, or left as replicates for a mean ± error band. The
    rule is only that it must not happen by default (the test below)."""
    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.X, "bandpass.low_hz": Role.AGGREGATE},
    )
    validate(spec, variant_table)  # does not raise

    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.X, "bandpass.low_hz": Role.FREE},
    )
    validate(spec, variant_table)  # does not raise


def test_a_defaulted_variant_factor_is_still_refused(variant_table):
    """The half that matters: nobody assigned it, so pooling would be silent —
    two pipelines' results read as replicates of one condition."""
    spec = PlotSpec(measures=["Peak"], roles={"subject": Role.X})

    with pytest.raises(RoleError, match="would be pooled"):
        validate(spec, variant_table)


def test_a_named_variant_satisfies_the_no_pooling_rule(variant_table):
    """Selecting one variant is what pinning became: the factor collapses to a
    single level, so there is nothing left to pool."""
    from scistackplot import apply_variant_sets
    from scistackplot.spec import VariantSet

    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.X},
        variant_sets=[VariantSet("20 Hz", {"bandpass.low_hz": "20"})],
    )
    validate(spec, apply_variant_sets(spec, variant_table))  # does not raise


# --- defaults --------------------------------------------------------------


def test_default_roles_put_a_variant_on_colour(variant_table):
    roles = default_roles(variant_table, "Peak")
    assert roles["bandpass.low_hz"] is Role.COLOR


def test_default_roles_for_1d_leave_x_unassigned(series_table):
    roles = default_roles(series_table, "Signal")
    assert Role.X not in roles.values()


def test_default_roles_for_1d_open_on_one_record(series_table):
    """Every schema key separates figures: the first figure is one record's
    data, which is what makes opening a large variable cheap (2026-09-13)."""
    roles = default_roles(series_table, "Signal")
    plain = [f.name for f in series_table.factors if not f.is_variant and not f.is_field]
    assert plain, "the fixture has schema keys"
    assert all(roles[name] is Role.ITERATE for name in plain), roles
    assert Role.FREE not in roles.values()


def test_default_roles_for_scalars_keep_x_and_colour(scalar_table):
    """Scalars are cheap at any size, and one point per figure is no plot."""
    roles = default_roles(scalar_table, "StepLength")
    assert Role.X in roles.values()
    assert Role.ITERATE not in roles.values()


def test_default_spec_scopes_y_limits_to_every_panel_factor(struct_table):
    """Per-panel autoscale from the start — and the cheapest limits to compute."""
    spec = default_spec(struct_table, "RawEMG")
    panel_factors = {n for n, r in spec.roles.items() if r in (Role.ITERATE, Role.FACET)}
    assert panel_factors
    assert set(spec.y_axis.scope) == panel_factors


def test_the_default_y_scope_follows_the_tables_factor_order(struct_table):
    """The TABLE's order, not the roles dict's.

    `default_roles` assigns the struct's fields before the schema keys, so the
    roles dict leads with `ColName` while the table leads with the schema keys.
    Scope order does not change which limits are computed, but it does decide
    the order they are reported and compared in, and dict-insertion order is
    whichever role happened to be assigned first.
    """
    spec = default_spec(struct_table, "RawEMG")
    order = struct_table.factor_names
    assert list(spec.y_axis.scope) == sorted(spec.y_axis.scope, key=order.index)
    assert spec.y_axis.scope[-1] == "ColName", "fields come after the schema keys"


# --- capability ------------------------------------------------------------


def test_distributions_need_replicates():
    without = {"session": Role.X, "subject": Role.COLOR}
    with_free = {"session": Role.X, "subject": Role.COLOR, "trial": Role.FREE}

    assert PlotKind.BOX not in available_plots(Shape.SCALAR, without)
    assert PlotKind.BOX in available_plots(Shape.SCALAR, with_free)


def test_aggregate_role_does_not_supply_replicates():
    """AGGREGATE collapses its factor, so it removes replicates rather than
    providing them — the distinction the design doc insists on."""
    roles = {"session": Role.X, "trial": Role.AGGREGATE}
    assert PlotKind.BOX not in available_plots(Shape.SCALAR, roles)


def test_band_needs_replicates_and_1d():
    assert PlotKind.BAND in available_plots(Shape.SERIES_1D, {"t": Role.FREE})
    assert PlotKind.BAND not in available_plots(Shape.SERIES_1D, {"t": Role.COLOR})
    assert PlotKind.BAND not in available_plots(Shape.SCALAR, {"t": Role.FREE})


@pytest.mark.parametrize(
    "shape,roles,expected",
    [
        (Shape.SCALAR, {"a": Role.X}, PlotKind.SCATTER),
        (Shape.SCALAR, {"a": Role.X, "b": Role.FREE}, PlotKind.BOX),
        (Shape.SERIES_1D, {"a": Role.COLOR}, PlotKind.LINE),
        (Shape.SERIES_1D, {"a": Role.FREE}, PlotKind.BAND),
        (Shape.MATRIX_2D, {}, PlotKind.HEATMAP),
    ],
)
def test_default_plot_table(shape, roles, expected):
    """The whole 'default plot per data type' requirement, as one table."""
    assert default_plot(shape, roles) is expected


def test_2d_only_offers_heatmap():
    assert available_plots(Shape.MATRIX_2D, {"a": Role.FREE}) == [PlotKind.HEATMAP]


def test_capabilities_report_explains_unavailable_kinds(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.AGGREGATE},
    )
    report = capabilities(spec, scalar_table)

    assert report["shape"] == "scalar"
    assert report["has_replicates"] is False
    box = next(k for k in report["kinds"] if k["kind"] == "box")
    assert box["available"] is False
    assert "replicates" in box["reason"]


# --- struct/dict variables: fields become subplots -------------------------


def test_field_factor_defaults_to_one_subplot_each(struct_table):
    """13 muscles overplotted on one axis is not a figure anyone wanted."""
    roles = default_roles(struct_table, "RawEMG")
    assert roles["ColName"] is Role.FACET


def test_field_factor_does_not_steal_a_schema_keys_channel(struct_table):
    roles = default_roles(struct_table, "RawEMG")
    assert roles["ColName"] is not Role.COLOR
    # The schema keys still get their own (per-record) figures.
    assert all(r is Role.ITERATE for n, r in roles.items() if n != "ColName")


def test_default_spec_wraps_many_fields_into_a_grid(struct_table):
    spec = default_spec(struct_table, "RawEMG")
    assert spec.roles["ColName"] is Role.FACET
    # 3 fields stay in a single horizontal row.
    assert spec.facet.n_cols == 3
    # Only the width is pinned; the height follows the panel count.
    assert spec.facet.n_rows is None


def test_default_spec_wraps_a_wide_struct(struct_table):
    import pandas as pd

    from scistackplot import LongTable

    frame = pd.DataFrame(
        {
            "subject": ["01"] * 13,
            "ColName": [f"m{n:02d}" for n in range(13)],
            "RawEMG": [[0.0, 1.0]] * 13,
        }
    )
    table = LongTable.from_frame(
        frame,
        factors=["subject", "ColName"],
        measures=["RawEMG"],
        field_factors=["ColName"],
    )
    assert default_spec(table, "RawEMG").facet.n_cols == 4


def test_field_factor_is_reported_to_the_gui(struct_table):
    described = struct_table.describe()
    field = next(f for f in described["factors"] if f["name"] == "ColName")
    assert field["is_field"] is True
