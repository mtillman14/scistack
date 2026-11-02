"""Code export: the generated stat_ function reproduces run() exactly (Stage 11)."""

from __future__ import annotations

import json

import pytest

from conftest import bar_spec, popcorn_frame, popcorn_table
from scistackplot.spec import FactorVariable, VariantSet
from scistackstats import DV, Analysis, StatsSpec, describe_table, run
from scistackstats.codegen import NotExportable, generate_stats_function, why_not_exportable


def _spec(**design) -> StatsSpec:
    return StatsSpec(design=bar_spec(**design), comparisons=(Analysis(),))


def test_generated_function_equals_run():
    spec = _spec()
    tables = {"Ratings": popcorn_table()}
    fn = generate_stats_function(spec, tables)
    assert fn.name == "stat_ratings" and fn.inputs == {"df": "Ratings"}
    namespace: dict = {}
    exec(compile(fn.source, "<generated>", "exec"), namespace)  # noqa: S102
    produced = namespace[fn.name](popcorn_frame())
    expected = run(spec, tables).to_dict()
    expected["descriptives"] = describe_table(spec, tables).to_dict()
    assert json.dumps(produced, sort_keys=True, default=str) == json.dumps(expected, sort_keys=True, default=str)


def test_the_spec_is_embedded_and_round_trips():
    spec = _spec()
    fn = generate_stats_function(spec, {"Ratings": popcorn_table()})
    assert "StatsSpec.from_dict(" in fn.source
    assert "import csvstats" not in fn.source, "the statistics stay in their owner"
    compile(fn.source, "<generated>", "exec")


def test_unexportable_specs_say_why():
    tables = {"Ratings": popcorn_table()}
    assert "variant rows" in why_not_exportable(_spec(variant_sets=[VariantSet(name="v1")]), tables)
    joined = _spec(factor_variables=[FactorVariable(variable="Demographics", column="Group")])
    assert "joined grouping" in why_not_exportable(joined, tables)
    cols = StatsSpec(design=bar_spec(), dvs=(DV("Ratings", columns=("x",)),))
    assert "multi-column" in why_not_exportable(cols, tables)
    with pytest.raises(NotExportable):
        generate_stats_function(cols, tables)


def test_several_dvs_get_one_input_each():
    spec = StatsSpec(design=bar_spec(), dvs=(DV("Ratings"), DV("Crunch")))
    table = popcorn_table()
    fn = generate_stats_function(spec, {"Ratings": table, "Crunch": table})
    assert fn.inputs == {"df_ratings": "Ratings", "df_crunch": "Crunch"}


def test_the_embedded_spec_reads_back_without_running_the_function():
    # A stat_ node opens back into StatStudio from this (studio-unification Stage 2).
    from scistackstats.codegen import embedded_spec

    spec = _spec()
    fn = generate_stats_function(spec, {"Ratings": popcorn_table()})
    recovered = embedded_spec(fn.source)
    expected = {k: v for k, v in spec.to_dict().items() if k != "guide"}
    assert recovered == expected
    assert StatsSpec.from_dict(recovered).to_dict()["design"] == spec.to_dict()["design"]


def test_a_hand_written_stat_function_has_no_embedded_spec():
    from scistackstats.codegen import embedded_spec

    assert embedded_spec("def stat_mine(df):\n    return {'n': len(df)}\n") is None
    assert embedded_spec("def broken(:\n") is None
    # A non-literal argument is not evaluated.
    assert embedded_spec("def stat_x(df):\n    return StatsSpec.from_dict(load())\n") is None
