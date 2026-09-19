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
        roles={"session": Role.GROUP, "limb": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["limb", "session"],
        color="limb",
        kind=PlotKind.BOX,
    )
    restored = PlotSpec.from_json(spec.to_json())
    assert restored == spec


def test_spec_toml_round_trip():
    pytest.importorskip("tomli_w")
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "trial": Role.COLLAPSE},
        color="session",
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


def test_several_factors_may_group(scalar_table):
    """Nested grouping: "stim and sham side by side, each split by session".

    Which factor is the OUTER grouping is an order (``groups``, innermost
    first), not a different role — see test_x_nesting.py for the composition.
    """
    spec = PlotSpec(
        measures=["StepLength"], roles={"subject": Role.GROUP, "session": Role.GROUP}
    )

    validate(spec, scalar_table)  # must not raise


def test_colour_is_one_grouping_layer(scalar_table):
    """Colour is a tag on a grouping layer, not a role: it must name one."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE},
        color="session",
    )
    with pytest.raises(RoleError, match="not a grouping layer"):
        validate(spec, scalar_table)


def test_1d_measure_groups_as_series(series_table):
    """Its x axis is the within-observation index; a grouped factor is one
    line per level rather than a tick."""
    from scistackplot.roles import grouping_layers

    spec = PlotSpec(measures=["Signal"], roles={"session": Role.GROUP})
    validate(spec, series_table)  # must not raise
    layers = grouping_layers(spec, series_table)
    assert layers.ticks == [] and layers.series == ["session"]


def test_second_measure_owns_x(scalar_frame):
    from scistackplot import LongTable
    from scistackplot.roles import grouping_layers

    frame = scalar_frame.assign(Speed=1.0)
    table = LongTable.from_frame(
        frame, factors=["subject", "session", "trial"], measures=["StepLength", "Speed"]
    )
    spec = PlotSpec(
        measures=["StepLength"], x_measure="Speed", roles={"subject": Role.GROUP}
    )
    validate(spec, table)  # a grouped factor is a series, never a tick
    assert grouping_layers(spec, table).ticks == []


def test_an_unassigned_variant_factor_separates_figures(variant_table):
    """Nothing is ever pooled silently: a variant factor the spec never
    named fans out like any other unmentioned factor."""
    from scistackplot.roles import complete_roles

    spec = PlotSpec(measures=["Peak"], roles={"subject": Role.GROUP})
    validate(spec, variant_table)  # does not raise
    assert complete_roles(spec, variant_table)["bandpass.low_hz"] is Role.ITERATE


def test_variant_factor_is_fine_once_assigned(variant_table):
    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP, "bandpass.low_hz": Role.GROUP},
        color="bandpass.low_hz",
    )
    validate(spec, variant_table)  # does not raise


def test_collapsing_a_variant_factor_is_refused(variant_table):
    """Two pipelines' results are never replicates of one condition. The old
    'aggregate'/'free' opt-in went with those roles."""
    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP, "bandpass.low_hz": Role.COLLAPSE},
    )
    with pytest.raises(RoleError, match="cannot be collapsed"):
        validate(spec, variant_table)


def test_a_named_variant_answers_the_factor(variant_table):
    """Selecting one variant is what pinning became: the factor collapses to a
    single level, so there is nothing left to separate."""
    from scistackplot import apply_variant_sets
    from scistackplot.spec import VariantSet

    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP},
        variant_sets=[VariantSet("20 Hz", {"bandpass.low_hz": "20"})],
    )
    validate(spec, apply_variant_sets(spec, variant_table))  # does not raise


# --- defaults --------------------------------------------------------------


def test_default_roles_make_a_variant_the_coloured_layer(variant_table):
    from scistackplot.roles import default_assignment

    opened = default_assignment(variant_table, "Peak")
    assert opened.roles["bandpass.low_hz"] is Role.GROUP
    assert opened.color == "bandpass.low_hz"
    assert opened.groups[0] == "bandpass.low_hz", "innermost"


def test_default_roles_for_1d_group_nothing(series_table):
    from scistackplot.roles import default_assignment

    assert default_assignment(series_table, "Signal").groups == []


def test_default_roles_for_1d_open_on_one_record(series_table):
    """Every schema key separates figures: the first figure is one record's
    data, which is what makes opening a large variable cheap (2026-09-13)."""
    roles = default_roles(series_table, "Signal")
    plain = [f.name for f in series_table.factors if not f.is_variant and not f.is_field]
    assert plain, "the fixture has schema keys"
    assert all(roles[name] is Role.ITERATE for name in plain), roles


def test_default_roles_for_scalars_group_their_own_level(scalar_table):
    """Granular first (2026-09-17): the deepest key is the mark, everything
    above it separates figures, nothing is collapsed."""
    roles = default_roles(scalar_table, "StepLength")
    assert roles == {
        "subject": Role.ITERATE, "session": Role.ITERATE, "trial": Role.GROUP,
    }


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


def test_distributions_need_a_sample():
    without = {"session": Role.GROUP, "subject": Role.GROUP}
    with_sample = {"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.COLLAPSE}

    assert PlotKind.BOX not in available_plots(Shape.SCALAR, without)
    assert PlotKind.BOX in available_plots(Shape.SCALAR, with_sample)


def test_a_bar_needs_no_sample():
    """A bar of single values, no error bar, is a legitimate figure."""
    assert PlotKind.BAR in available_plots(Shape.SCALAR, {"session": Role.GROUP})


def test_spaghetti_needs_two_layers():
    roles = {"session": Role.GROUP, "subject": Role.GROUP}
    assert PlotKind.SPAGHETTI not in available_plots(Shape.SCALAR, roles, n_groups=1)
    assert PlotKind.SPAGHETTI in available_plots(Shape.SCALAR, roles, n_groups=2)


def test_band_needs_a_sample_and_1d():
    assert PlotKind.BAND in available_plots(Shape.SERIES_1D, {"t": Role.COLLAPSE})
    assert PlotKind.BAND not in available_plots(Shape.SERIES_1D, {"t": Role.GROUP})
    assert PlotKind.BAND not in available_plots(Shape.SCALAR, {"t": Role.COLLAPSE})


@pytest.mark.parametrize(
    "shape, roles, expected",
    [
        (Shape.SCALAR, {"a": Role.GROUP}, PlotKind.SCATTER),
        (Shape.SCALAR, {"a": Role.GROUP, "b": Role.COLLAPSE}, PlotKind.BOX),
        (Shape.SERIES_1D, {"a": Role.GROUP}, PlotKind.LINE),
        (Shape.SERIES_1D, {"a": Role.COLLAPSE}, PlotKind.BAND),
        (Shape.MATRIX_2D, {}, PlotKind.HEATMAP),
    ],
)
def test_default_plot_table(shape, roles, expected):
    assert default_plot(shape, roles) is expected


def test_2d_only_offers_heatmap():
    assert available_plots(Shape.MATRIX_2D, {"a": Role.COLLAPSE}) == [PlotKind.HEATMAP]


def test_capabilities_report_explains_unavailable_kinds(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session", "subject"],
    )
    report = capabilities(spec, scalar_table)

    assert report["shape"] == "scalar"
    assert report["has_sample"] is False
    box = next(k for k in report["kinds"] if k["kind"] == "box")
    assert box["available"] is False
    assert "sample" in box["reason"]


# --- struct/dict variables: fields become subplots -------------------------


def test_field_factor_defaults_to_one_subplot_each(struct_table):
    """13 muscles overplotted on one axis is not a figure anyone wanted."""
    roles = default_roles(struct_table, "RawEMG")
    assert roles["ColName"] is Role.FACET


def test_field_factor_does_not_steal_a_schema_keys_channel(struct_table):
    roles = default_roles(struct_table, "RawEMG")
    assert roles["ColName"] is not Role.GROUP
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
