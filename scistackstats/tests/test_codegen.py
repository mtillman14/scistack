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
