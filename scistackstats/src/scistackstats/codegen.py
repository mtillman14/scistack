"""Code export: a ``stat_`` function that reproduces a StatStudio table (Stage 11).

The generated function **calls scistackstats** with the spec embedded as a
literal. It does not restate the statistics: design derivation, test
selection, correction families and every csv-stats call stay in their one
owner, so the exported code cannot drift from what StatStudio showed. It
receives the long-format table ``for_each`` hands a ``stat_`` function
(schema keys as columns, ``as_table`` on), rebuilds the ``LongTable`` with
the level order and schema the preview used, and returns the report dict
(stored by scidb's ``stat_`` machinery: lineage, ``finalized``, ``scidb
report``).

First pass: a DV must be a single-column Variable whose factors are its
schema keys. :func:`why_not_exportable` refuses, with the reason, for chosen
``ColName`` columns, joined grouping variables, variant rows or variant
axes, and an x variable. Rebuilding those tables from ``for_each`` inputs
would be a second copy of the source's join and melt logic.
"""

from __future__ import annotations

import keyword
import pprint
import re
from dataclasses import dataclass, field
from typing import Any

from scistacklog import Log

from scistackplot import LongTable

from .spec import StatsSpec

LAYER = "scistackstats"


class NotExportable(ValueError):
    """This stats spec cannot be exported as code yet; the message says why."""


def _ident(text: str) -> str:
    name = re.sub(r"\W+", "_", str(text)).strip("_").lower() or "stats"
    if name[0].isdigit() or keyword.iskeyword(name):
        name = f"v_{name}"
    return name


def default_function_name(stats_spec: StatsSpec) -> str:
    return f"stat_{_ident(stats_spec.targets[0].variable)}"


def input_params(stats_spec: StatsSpec) -> dict[str, str]:
    """``{parameter: variable}``: ``df`` for one variable, ``df_<name>`` for several."""
    variables = list(dict.fromkeys(dv.variable for dv in stats_spec.targets))
    if len(variables) == 1:
        return {"df": variables[0]}
    return {f"df_{_ident(v)}": v for v in variables}


def why_not_exportable(stats_spec: StatsSpec, tables: dict[str, LongTable]) -> str | None:
    design = stats_spec.design
    if any(dv.columns is not None for dv in stats_spec.targets):
        return "chosen columns of a multi-column variable are not exported as code yet"
    if design.factor_variables:
        return "joined grouping variables are not exported as code yet"
    if design.x_measure:
        return "an x variable is not exported as code yet"
    if design.variant_sets:
        return "variant rows are not exported as code yet"
    for dv in stats_spec.targets:
        table = tables.get(dv.variable)
        if table is None:
            return f"no table loaded for {dv.variable}"
        if table.variant_factors:
            names = [f.name for f in table.variant_factors]
            return f"{dv.variable} has variant axes ({', '.join(names)}); they are not exported as code yet"
        if table.field_factors:
            return f"{dv.variable} is a multi-column variable; it is not exported as code yet"
    return None


@dataclass
class StatsFunction:
    name: str
    source: str
    #: ``{parameter: variable}`` the function takes.
    inputs: dict[str, str] = field(default_factory=dict)


def _table_literal(variable: str, param: str, table: LongTable) -> list[str]:
    factors = list(table.factor_names)
    levels = {f: [str(v) for v in table.factor(f).levels] for f in factors}
    return [
        f"    {variable!r}: LongTable.from_frame(",
        f"        {param},",
        f"        factors=[c for c in {factors!r} if c in {param}.columns],",
        f"        measures=[{variable!r}],",
        f"        name={variable!r},",
        f"        schema_levels={list(table.schema_levels)!r},",
        f"        level_order={levels!r},",
        "    ),",
    ]


def generate_stats_function(stats_spec: StatsSpec, tables: dict[str, LongTable], *,
                            function_name: str | None = None) -> StatsFunction:
    """The ``stat_`` function source (module docstring). Raises
    :class:`NotExportable` with the reason when the spec is not exportable."""
    reason = why_not_exportable(stats_spec, tables)
    if reason:
        Log.info("[stats codegen] not exportable: %s", reason, layer=LAYER)
        raise NotExportable(reason)
    name = function_name or default_function_name(stats_spec)
    if not name.isidentifier() or keyword.iskeyword(name):
        raise NotExportable(f"{name!r} is not a valid function name")
    params = input_params(stats_spec)
    spec_literal = pprint.pformat(stats_spec.to_dict(), indent=1, width=96, sort_dicts=True)
    dvs = ", ".join(dv.key for dv in stats_spec.targets)
    analyses = ", ".join(f"{a.id} ({a.family})" for a in stats_spec.comparisons) or "descriptives only"
    lines = [
        f"def {name}({', '.join(params)}):",
        f'    """StatStudio statistics for {dvs}: {analyses}.',
        "",
        "    Generated by scistackstats. The statistics are scistackstats' and",
        "    csv-stats' own (this function calls them with the saved settings), so",
        "    the result is what StatStudio showed for the same rows.",
        '    """',
        "    from scistackplot import LongTable",
        "    from scistackstats import StatsSpec, describe_table, run",
        "",
        "    spec = StatsSpec.from_dict(",
        *("        " + line for line in spec_literal.splitlines()),
        "    )",
        "    tables = {",
    ]
    for param, variable in params.items():
        lines += ["    " + line for line in _table_literal(variable, param, tables[variable])]
    lines += [
        "    }",
        "    report = run(spec, tables).to_dict()",
        '    report["descriptives"] = describe_table(spec, tables).to_dict()',
        "    return report",
        "",
    ]
    source = "\n".join(lines)
    Log.info("[stats codegen] %s: %d input(s), %d analysis(es), %d line(s)", name, len(params),
             len(stats_spec.comparisons), len(lines), layer=LAYER)
    return StatsFunction(name=name, source=source, inputs=params)
