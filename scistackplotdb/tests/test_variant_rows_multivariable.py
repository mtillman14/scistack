"""
A variant row may draw from any variable.

This is how "plot Raw against Filtered" and "plot v1 against v2" became one
mechanism rather than two: a row is one *series*, and a series is a variable
plus a region of that variable's variant space. Rows stack into the ordinary
``Variant`` factor, so the comparison takes a colour or a facet like anything
else, and the export already had the right shape — one ``for_each`` input per
row.

The correctness core is that a row claims only **its own variable's** rows.
``resolve_selection`` deliberately drops selection keys naming a column the
frame lacks (a stale spec must never silently empty a figure), so with two
variables in one frame an unqualified mask would let a row pinning
``Code:filterEMG=v1`` match every row of the *other* variable too — drawing it
once per variant, identically, which looks exactly like real replicates.
"""

from __future__ import annotations

import pytest
from scistackplot import PlotKind, PlotSpec, Role, capabilities, resolve
from scistackplot.spec import VariantSet
from scistackplot.variants import VARIABLE_COLUMN, VARIANT_FACTOR, apply_variant_sets

from scistackplotdb import ScidbSource

pytest.importorskip("seaborn")


def _spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _two_variable_spec() -> PlotSpec:
    return _spec(
        variant_sets=[
            VariantSet(name="Raw", variable="StepLength"),
            VariantSet(name="Scaled", variable="Scaled"),
        ]
    )


@pytest.fixture
def scaled(seeded):
    """A second trial-level scalar, so two variables can stack.

    Produced by a pipeline step rather than saved directly, so it carries the
    provenance columns a real second variable would — the stacked frame then has
    to reconcile two variables' variant columns, which is the interesting case.
    """
    from scidb import for_each

    from conftest import Scaled, StepLength

    def double_it(value):
        return float(value) * 2.0

    for_each(
        double_it,
        inputs={"value": StepLength},
        outputs=[Scaled],
        subject=[],
        session=[],
        trial=[],
    )
    return seeded


def test_two_rows_over_two_variables_become_one_variant_factor(scaled):
    table = ScidbSource(scaled).get_table(["StepLength", "Scaled"])
    spec = _two_variable_spec()

    derived = apply_variant_sets(spec, table)

    assert derived.has_factor(VARIANT_FACTOR)
    assert [str(level) for level in derived.factor(VARIANT_FACTOR).levels] == [
        "Raw",
        "Scaled",
    ]
    # Which variable a row came from is what its NAME says; keeping the column
    # would ask the same question twice and `validate` would refuse it.
    assert not derived.has_factor(VARIABLE_COLUMN)


def test_a_row_claims_only_its_own_variables_rows(scaled):
    """The bug this design exists to prevent, pinned.

    Both variables' values live in one column. If the mask were not qualified,
    each row would claim all 24 and the figure would draw every value twice.
    """
    table = ScidbSource(scaled).get_table(["StepLength", "Scaled"])
    derived = apply_variant_sets(_two_variable_spec(), table)

    counts = derived.frame[VARIANT_FACTOR].value_counts().to_dict()
    assert counts == {"Raw": 12, "Scaled": 12}
    assert len(derived.frame) == 24


def test_values_come_from_the_right_variable(scaled):
    """Not just the row counts — the numbers themselves must not be swapped."""
    table = ScidbSource(scaled).get_table(["StepLength", "Scaled"])
    derived = apply_variant_sets(_two_variable_spec(), table)
    frame = derived.frame

    raw = frame[frame[VARIANT_FACTOR] == "Raw"]["StepLength"].sort_values().to_numpy()
    scaled_values = (
        frame[frame[VARIANT_FACTOR] == "Scaled"]["StepLength"].sort_values().to_numpy()
    )
    assert scaled_values == pytest.approx(raw * 2.0)


def test_the_variant_factor_takes_a_role_like_any_other(scaled):
    table = ScidbSource(scaled).get_table(["StepLength", "Scaled"])
    spec = _two_variable_spec()

    from dataclasses import replace

    coloured = resolve(
        replace(spec.with_roles(**{VARIANT_FACTOR: Role.GROUP}), color=VARIANT_FACTOR), table
    )
    assert len(coloured) == 1

    separate = resolve(spec.with_roles(**{VARIANT_FACTOR: Role.ITERATE}), table)
    assert len(separate) == 2
    assert {figure.figure_key[VARIANT_FACTOR] for figure in separate} == {
        "Raw",
        "Scaled",
    }


def test_row_counts_reported_to_the_gui_match_the_figure(scaled):
    """`variant_summary` and the renderer share one mask (`variants.row_mask`).

    A count the figure disagrees with would be worse than no count.
    """
    table = ScidbSource(scaled).get_table(["StepLength", "Scaled"])
    report = capabilities(_two_variable_spec(), table)

    assert [entry["row_count"] for entry in report["variants"]["sets"]] == [12, 12]
    assert [entry["variable"] for entry in report["variants"]["sets"]] == [
        "StepLength",
        "Scaled",
    ]


def test_a_row_naming_only_a_variable_is_not_inert(scaled):
    """"Also plot Scaled, all of it" is a complete instruction.

    An empty selection alone means the user has not spoken yet; naming a
    variable IS speaking.
    """
    table = ScidbSource(scaled).get_table(["StepLength", "Scaled"])
    spec = _spec(
        variant_sets=[
            VariantSet(variable="StepLength"),
            VariantSet(variable="Scaled"),
        ]
    )
    derived = apply_variant_sets(spec, table)

    # Unnamed rows label themselves with their variable, which reads in a legend.
    assert [str(v) for v in derived.factor(VARIANT_FACTOR).levels] == [
        "StepLength",
        "Scaled",
    ]


# --- dict/struct variables stack too ---------------------------------------


def _emg_spec() -> PlotSpec:
    return PlotSpec(
        measures=["Emg"],
        # One line per record per variant, all in one figure: the schema keys
        # are series layers, the variant the coloured one.
        roles={
            "ColName": Role.FACET,
            "Variant": Role.GROUP,
            "subject": Role.GROUP,
            "session": Role.GROUP,
            "trial": Role.GROUP,
        },
        groups=["Variant", "trial", "session", "subject"],
        color="Variant",
        kind=PlotKind.LINE,
        variant_sets=[
            VariantSet(name="Raw", variable="Emg"),
            VariantSet(name="Filtered", variable="EmgFiltered"),
        ],
    )


def test_two_dict_variables_stack_on_their_shared_fields(seeded):
    """Raw vs Filtered EMG, compared muscle by muscle — the archetypal case.

    Each is melted to the shared fields FIRST, so what stacks is the melted long
    form and nothing downstream can tell this from two scalar variables.
    """
    table = ScidbSource(seeded).get_table(["Emg", "EmgFiltered"])

    assert table.has_factor("ColName")
    assert {str(v) for v in table.factor("ColName").levels} == {
        "RHAM",
        "RTA",
        "LMG",
    }
    # 3 subjects x 2 sessions x 2 trials x 3 muscles x 2 variables
    assert len(table.frame) == 3 * 2 * 2 * 3 * 2


def test_the_muscle_factor_still_defaults_to_one_subplot_each(seeded):
    """13 muscles overplotted on one axis is not a figure anyone wanted, and
    that has to hold whether one dict variable is plotted or two."""
    from scistackplot import default_roles

    table = ScidbSource(seeded).get_table(["Emg", "EmgFiltered"])

    assert default_roles(table, "Emg")["ColName"] is Role.FACET


def test_each_dict_variable_claims_only_its_own_rows(seeded):
    table = ScidbSource(seeded).get_table(["Emg", "EmgFiltered"])

    derived = apply_variant_sets(_emg_spec(), table)

    counts = derived.frame[VARIANT_FACTOR].value_counts().to_dict()
    # 3 subjects x 2 sessions x 2 trials x 3 muscles, per variable.
    assert counts == {"Raw": 36, "Filtered": 36}


def test_two_dict_variables_render_as_one_figure_per_muscle(seeded):
    figures = resolve(_emg_spec(), ScidbSource(seeded).get_table(["Emg", "EmgFiltered"]))

    assert len(figures) == 1
    # One subplot per muscle, two coloured series in each.
    assert len(figures[0].panels) == 3
    assert {str(v) for v in figures[0].color_order} == {"Raw", "Filtered"}


def test_stackable_with_offers_the_other_dict_variable(seeded):
    assert ScidbSource(seeded).stackable_with("Emg") == ["EmgFiltered"]


def test_a_dict_cannot_stack_with_a_plain_value(seeded):
    """No correspondence between one number and a set of fields."""
    source = ScidbSource(seeded)

    assert "Signal" not in source.stackable_with("Emg")
    with pytest.raises(ValueError, match="single value"):
        source.get_table(["Emg", "Signal"])


def test_dicts_with_no_shared_fields_are_refused(seeded):
    """Every field on its own subplot with a single series is not a comparison."""
    source = ScidbSource(seeded)
    # Fabricate the mismatch at the frame level rather than seeding a whole
    # variable: the rule under test is about field SETS, not about storage.
    # It must stay multi-column — a single column would trip the different
    # guard, the one about a dict having no counterpart in a plain value.
    renames = {"RHAM": "LTA", "RTA": "RGAS", "LMG": "LSOL"}
    frame = source._variable_frame("EmgFiltered")
    frame.data_columns = list(renames.values())
    frame.frame = frame.frame.rename(columns=renames)
    source._frames["EmgFiltered"] = frame

    with pytest.raises(ValueError, match="share no fields"):
        source.get_table(["Emg", "EmgFiltered"])


def test_generated_code_melts_both_dict_inputs(seeded):
    """The endpoint gets each variable wide; the fields align across inputs and
    ONE melt turns them into the value column."""
    from scistackplot import generate_plot_function

    table = ScidbSource(seeded).get_table(["Emg", "EmgFiltered"])
    source = generate_plot_function(_emg_spec(), table)

    # No rename: a dict variable's value column is not named after it.
    assert "rename(columns={'EmgFiltered'" not in source
    assert "df.melt(" in source
    compile(source, "<generated>", "exec")


def test_stacking_refuses_mismatched_shapes(seeded):
    source = ScidbSource(seeded)
    with pytest.raises(ValueError, match="same kind of value"):
        source.get_table(["StepLength", "Signal"])


def test_stacking_refuses_mismatched_schema_levels(seeded):
    """Mass is subject-level; broadcasting it across trials would inflate n."""
    source = ScidbSource(seeded)
    with pytest.raises(ValueError, match="same schema level"):
        source.get_table(["StepLength", "Mass"])


def test_stackable_with_offers_only_what_stacks(scaled):
    source = ScidbSource(scaled)
    partners = source.stackable_with("StepLength")

    assert "Scaled" in partners
    assert "Signal" not in partners, "different shape"
    assert "Mass" not in partners, "different schema level"


def test_each_row_loads_its_own_variable_in_the_generated_endpoint(scaled):
    """The export shape was already right; each row just names its variable.

    One `for_each` input per row is what `codegen.variant_params` has always
    produced — `as_table` hands a function schema keys and data columns only,
    never the columns that tell variants apart, so the split has to happen where
    the LOAD happens.
    """
    from scistackplotdb import generate_endpoint

    table = ScidbSource(scaled).get_table(["StepLength", "Scaled"])
    code = generate_endpoint(
        _two_variable_spec(), table, input_variable="StepLength"
    )

    assert '"raw": StepLength,' in code.foreach_source
    assert '"scaled": Scaled,' in code.foreach_source
    assert "as_table=['raw', 'scaled']" in code.foreach_source
    # Both variables' values stack into ONE column, which is what the plot call
    # reads; the Variant label is what keeps them apart.
    assert "rename(columns={'Scaled': 'StepLength'})" in code.function_source
    compile(code.source, "<generated>", "exec")


def test_generated_two_variable_function_runs(scaled):
    """Generated code must execute — the export is literal seaborn, not a call
    back into this package, so running it is the only meaningful check."""
    import matplotlib.pyplot as plt

    from scistackplot import generate_plot_function

    source_obj = ScidbSource(scaled)
    table = source_obj.get_table(["StepLength", "Scaled"])
    spec = _two_variable_spec()
    source = generate_plot_function(spec, table)

    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102

    # The frames an endpoint would receive: one per row, each under its own
    # variable's column name, with the schema keys as columns.
    keys = ["subject", "session", "trial"]
    raw = table.frame[table.frame["Variable"] == "StepLength"][
        [*keys, "StepLength"]
    ].reset_index(drop=True)
    other = (
        table.frame[table.frame["Variable"] == "Scaled"][[*keys, "StepLength"]]
        .rename(columns={"StepLength": "Scaled"})
        .reset_index(drop=True)
    )

    figure = namespace["plot_steplength"](raw, other, "figure.png")
    assert figure.axes
    plt.close(figure)


def test_an_x_measure_cannot_pair_with_stacked_variables(scaled):
    source = ScidbSource(scaled)
    with pytest.raises(ValueError, match="pairs with ONE y measure"):
        source.get_table(["StepLength", "Scaled"], x_measure="Mass")
