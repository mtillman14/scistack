"""StatStudio → pipeline: the ``stat_`` function plus its ``for_each`` call.

The function is ``scistackstats.codegen.generate_stats_function``. This
module writes the call around it:

- **no iteration keywords** (P7): one call over the whole fan-out, so a
  correction family that spans strata or DVs is computed in one place. scidb
  stores one record per call (a grand aggregation);
- one input per DV variable, loaded by name;
- ``finalized`` (record mode) by default, so the result is a lineage record
  that goes stale when its inputs change and that ``scidb report`` lists.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from scistacklog import Log
from scistackplot import LongTable

from scistackstats import StatsSpec
from scistackstats.codegen import generate_stats_function

LAYER = "scistackstatsdb"


@dataclass
class StatsEndpointCode:
    function_name: str
    function_source: str
    foreach_source: str
    output_variable: str
    inputs: dict[str, str] = field(default_factory=dict)

    @property
    def source(self) -> str:
        return f"{self.function_source}\n\n{self.foreach_source}"


def default_output_variable(stats_spec: StatsSpec) -> str:
    """``Ratings`` -> ``RatingsStats``."""
    return f"{stats_spec.targets[0].variable}Stats"


def generate_stats_endpoint(
    stats_spec: StatsSpec,
    tables: dict[str, LongTable],
    *,
    function_name: str | None = None,
    output_variable: str | None = None,
    finalized: bool = True,
) -> StatsEndpointCode:
    """The ``stat_`` function and its ``for_each`` call. Raises
    ``scistackstats.codegen.NotExportable`` with the reason when the spec
    cannot be exported yet."""
    fn = generate_stats_function(stats_spec, tables, function_name=function_name)
    output = output_variable or default_output_variable(stats_spec)
    lines = ["for_each(", f"    {fn.name},", "    inputs={"]
    lines += [f'        "{param}": {variable},' for param, variable in fn.inputs.items()]
    lines += [
        "    },",
        f"    outputs=[{output}],",
        f"    as_table={list(fn.inputs)!r},",
        f"    finalized={finalized},",
        ")",
    ]
    Log.info("[stats endpoint] %s: inputs %s -> %s, finalized=%s (no iteration: one record)",
             fn.name, fn.inputs, output, finalized, layer=LAYER)
    return StatsEndpointCode(function_name=fn.name, function_source=fn.source,
                             foreach_source="\n".join(lines), output_variable=output, inputs=fn.inputs)


def required_declarations(code: StatsEndpointCode) -> list[str]:
    """Variable types the call needs that may not exist yet (the output)."""
    return [code.output_variable]
