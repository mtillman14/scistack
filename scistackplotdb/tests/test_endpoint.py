"""Endpoint generation: spec -> plot_ function + for_each call."""

from __future__ import annotations

import pytest
from scistackplot import PlotKind, PlotSpec, Role

from scistackplotdb import ScidbSource, default_path_template, generate_endpoint

pytest.importorskip("seaborn")


@pytest.fixture
def table(seeded):
    return ScidbSource(seeded).get_table(["StepLength"])


@pytest.fixture
def iterating_spec():
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )


def test_iterate_roles_become_foreach_keywords(table, iterating_spec):
    code = generate_endpoint(iterating_spec, table, input_variable="StepLength")

    assert code.iterate_keys == ["subject"]
    assert "subject=[]," in code.foreach_source
    # Non-iterated keys stay as DataFrame columns (decision D2) — they must NOT
    # appear as iteration keywords.
    assert "session=[]" not in code.foreach_source
    assert "trial=[]" not in code.foreach_source


def test_a_nested_iterated_key_carries_its_ancestors_into_for_each(table):
    """The fan-out decision is made once, in scistackplot, so the pipeline
    inherits it.

    Iterating `trial` alone would run the endpoint once per trial NUMBER,
    pooling every subject into one figure — a different figure set from the one
    the panel previewed.
    """
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.ITERATE})
    code = generate_endpoint(spec, table, input_variable="StepLength")

    assert code.iterate_keys == ["subject", "session", "trial"]
    for key in ("subject", "session", "trial"):
        assert f"{key}=[]," in code.foreach_source
        # Every iterated key must reach the filename, or two figures collide.
        assert f"{{{key}}}" in code.path_template


def test_for_each_keys_run_in_schema_order_not_role_order(table):
    """Dict order is click order; the PathOutput template must not inherit it."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.ITERATE, "subject": Role.ITERATE, "session": Role.GROUP},
    )
    code = generate_endpoint(spec, table, input_variable="StepLength")

    assert code.iterate_keys == ["subject", "trial"]
    assert code.path_template.index("{subject}") < code.path_template.index("{trial}")


def test_plot_input_is_passed_as_a_table(table, iterating_spec):
    """plot_ does not default as_table on (only stat_ does), so say it."""
    code = generate_endpoint(iterating_spec, table, input_variable="StepLength")
    assert "as_table=['df']" in code.foreach_source


def test_generated_call_is_finalized_by_default(table, iterating_spec):
    code = generate_endpoint(iterating_spec, table, input_variable="StepLength")
    assert "finalized=True" in code.foreach_source


def test_draft_mode_is_available(table, iterating_spec):
    code = generate_endpoint(
        iterating_spec, table, input_variable="StepLength", finalized=False
    )
    assert "finalized=False" in code.foreach_source


def test_path_template_names_every_iterate_key():
    """Omitting one would make two figures write the same file."""
    template = default_path_template("plot_steplength", ["subject", "session"])
    assert "{subject}" in template
    assert "{session}" in template


def test_output_variable_defaults_from_the_input(table, iterating_spec):
    code = generate_endpoint(iterating_spec, table, input_variable="StepLength")
    assert code.output_variable == "StepLengthFigure"
    assert "outputs=[StepLengthFigure]" in code.foreach_source


def test_generated_source_compiles(table, iterating_spec):
    code = generate_endpoint(iterating_spec, table, input_variable="StepLength")
    compile(code.source, "<generated>", "exec")


def test_second_measure_is_passed_as_a_second_input(seeded):
    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"], x_measure="Mass")
    spec = PlotSpec(
        measures=["StepLength"],
        x_measure="Mass",
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session", "subject"],
        color="subject",
        kind=PlotKind.SCATTER,
    )

    code = generate_endpoint(
        spec, table, input_variable="StepLength", x_variable="Mass"
    )
    assert '"df_x": Mass,' in code.foreach_source
    assert "as_table=['df', 'df_x']" in code.foreach_source


# --- variants ---------------------------------------------------------------
#
# Everything the GUI can do has to be writable by hand, so a variant figure has
# to survive export. The two shapes below are the whole contract:
#   one variant  -> one `df` input, pinned by a Variant(...) wrapper
#   two or more  -> one input each, concatenated and labelled in the function
# It cannot be one input carrying a variant column: `as_table` hands a function
# schema keys and data columns only, never the branch-param/code columns that
# tell variants apart (scifor's `_extract_data`).


@pytest.fixture
def comparison_spec():
    from scistackplot.spec import VariantSet

    return PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
        variant_sets=[
            VariantSet("baseline", {"Code:bandpass": "v1"}),
            VariantSet("new filter", {"bandpass.low_hz": ["20", "50"]}),
        ],
    )


def test_one_variant_pins_the_single_input(table):
    from scistackplot.spec import VariantSet

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
        variant_sets=[VariantSet("current", {"CodeIsLatest": True})],
    )

    code = generate_endpoint(spec, table, input_variable="StepLength")

    assert '"df": Variant(StepLength, code_version=\'latest\')' in code.foreach_source
    assert "def plot_steplength(df, filename)" in code.function_source


def test_each_variant_becomes_its_own_input(table, comparison_spec):
    code = generate_endpoint(comparison_spec, table, input_variable="StepLength")

    assert '"baseline": Variant(StepLength, fn=\'bandpass\', code_version=\'v1\')' in (
        code.foreach_source
    )
    assert '"new_filter": Variant(StepLength, fn=\'bandpass\', low_hz=[\'20\', \'50\'])' in (
        code.foreach_source
    )
    assert "as_table=['baseline', 'new_filter']" in code.foreach_source


def test_the_generated_function_labels_and_stacks_them(table, comparison_spec):
    code = generate_endpoint(comparison_spec, table, input_variable="StepLength")

    assert "def plot_steplength(baseline, new_filter, filename)" in code.function_source
    assert "baseline.assign(**{'Variant': 'baseline'})" in code.function_source
    assert "new_filter.assign(**{'Variant': 'new filter'})" in code.function_source


def test_variant_input_names_are_identifiers(table):
    """A variant is named for a reader ("20 Hz + latest"); a parameter cannot be."""
    from scistackplot.spec import VariantSet

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
        variant_sets=[
            VariantSet("20 Hz + latest", {"bandpass.low_hz": "20"}),
            VariantSet("50 Hz + latest", {"bandpass.low_hz": "50"}),
        ],
    )

    code = generate_endpoint(spec, table, input_variable="StepLength")

    assert "def plot_steplength(v_20_hz_latest, v_50_hz_latest, filename)" in (
        code.function_source
    )


def test_a_multi_function_selection_nests_variants(table):
    """Two producing functions cannot share one Variant(fn=...), and dotted-string
    kwargs would leak a reserved namespace into code the user is meant to edit."""
    from scistackplot.spec import VariantSet

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
        variant_sets=[
            VariantSet("old load, 20 Hz", {"Code:loadEMG": "v1", "bandpass.low_hz": "20"})
        ],
    )

    code = generate_endpoint(spec, table, input_variable="StepLength")

    assert (
        "Variant(Variant(StepLength, fn='bandpass', low_hz='20'), "
        "fn='loadEMG', code_version='v1')" in code.foreach_source
    )


def test_generated_variant_source_compiles(table, comparison_spec):
    code = generate_endpoint(comparison_spec, table, input_variable="StepLength")

    compile(code.function_source, "<generated>", "exec")
