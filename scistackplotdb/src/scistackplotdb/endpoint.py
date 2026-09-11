"""
Turning a ``PlotSpec`` into a pipeline endpoint.

The GUI's "Add to pipeline" produces two things: a ``plot_`` function (generated
by ``scistackplot.codegen`` — literal seaborn, no runtime dependency on this
package) and the ``for_each`` call that runs it. This module owns the second
half, and with it the one translation that has to be exactly right:

    Role.ITERATE  ->  a for_each iteration keyword

Interactively, ITERATE fans out through a pandas ``groupby`` inside
``resolve()``. In the pipeline it fans out through ``for_each`` + ``PathOutput``.
If those two ever disagree, the exported pipeline is not what the user
previewed — the worst failure mode this layer has, and what
``tests/test_fanout_parity.py`` exists to prevent.

Everything else the endpoint needs already exists in scidb: ``finalized``,
artifact stamping, ``skip_computed`` and ``scidb report`` are untouched.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from scistacklog import Log
from scistackplot import LongTable, PlotSpec, default_function_name
from scistackplot import generate_plot_function
from scistackplot.codegen import group_param, variant_params
from scistackplot.roles import fanout_keys
from scistackplot.variants import defined_sets

from .load import LATEST_COLUMN

LAYER = "scistackplotdb"


@dataclass
class EndpointCode:
    """Generated source for one plotting endpoint."""

    function_name: str
    function_source: str
    foreach_source: str
    iterate_keys: list[str] = field(default_factory=list)
    path_template: str = ""
    output_variable: str = ""

    @property
    def source(self) -> str:
        """Function and call together, ready to append to a pipeline module."""
        return f"{self.function_source}\n\n{self.foreach_source}"


def generate_endpoint(
    spec: PlotSpec,
    table: LongTable,
    *,
    input_variable: str,
    function_name: str | None = None,
    output_variable: str | None = None,
    path_template: str | None = None,
    finalized: bool = True,
    x_variable: str | None = None,
) -> EndpointCode:
    """
    Generate the ``plot_`` function and its ``for_each`` call.

    ``input_variable`` is the scidb variable type supplying the data;
    ``x_variable`` is the optional second measure for an x–y scatter.
    """
    name = function_name or default_function_name(spec)
    # NOT spec.iterate_factors: the fan-out includes schema keys promoted
    # because a nested key iterates, and runs in schema order. The preview and
    # the generated for_each have to agree on both — that is what
    # tests/test_fanout_parity.py checks.
    iterate_keys = fanout_keys(spec, table)
    output = output_variable or default_output_variable(input_variable)
    template = path_template or default_path_template(name, iterate_keys)

    function_source = generate_plot_function(spec, table, function_name=name)
    foreach_source = _foreach_call(
        spec=spec,
        function_name=name,
        input_variable=input_variable,
        x_variable=x_variable,
        output_variable=output,
        path_template=template,
        iterate_keys=iterate_keys,
        finalized=finalized,
    )

    Log.info(
        "generated endpoint %s: input=%s output=%s iterate=%s finalized=%s",
        name,
        input_variable,
        output,
        iterate_keys or "none",
        finalized,
        layer=LAYER,
    )
    return EndpointCode(
        function_name=name,
        function_source=function_source,
        foreach_source=foreach_source,
        iterate_keys=iterate_keys,
        path_template=template,
        output_variable=output,
    )


def default_output_variable(input_variable: str) -> str:
    """``StepLength`` -> ``StepLengthFigure``."""
    return f"{input_variable}Figure"


def default_path_template(function_name: str, iterate_keys: list[str]) -> str:
    """
    Build a PathOutput template that cannot collide.

    Every ITERATE key goes into the filename. Omitting one would make two
    figures write the same file; for schema keys scidb treats that as
    pre-existing overwrite behavior (no error), and for variants its collision
    guard raises before anything renders. Including them all avoids both.
    """
    slug = re.sub(r"^plot_", "", function_name)
    parts = "".join(f"_{{{key}}}" for key in iterate_keys)
    return f"plots/{slug}{parts}.png"


def variant_expression(input_variable: str, variant_set) -> str:
    """A ``Variant(...)`` call selecting one named variant's records.

    The inverse of :func:`~scistackplotdb.variants.selection_for`, and the thing
    that makes a variant figure reproducible by hand: what the popup's
    checkboxes and version dropdowns produced comes back out as the same
    wrapper a scientist would have typed.

    Selections spanning two producing functions **nest** rather than resorting
    to dotted-string kwargs::

        Variant(Variant(EMG, fn="loadEMG", code_version="v1"), fn="bandpass", low_hz=20)

    Nesting is documented, merges the two filters (``scidb.variant.Variant``
    handles it explicitly), and keeps every keyword readable — where
    ``**{"__code__.loadEMG": "v1"}`` would leak a reserved namespace into code a
    user is meant to edit.
    """
    from scistackplot import CODE_FACTOR_PREFIX, LATEST

    by_function: dict[str, dict[str, object]] = {}
    for column, value in (variant_set.selection or {}).items():
        if column.startswith(CODE_FACTOR_PREFIX):
            fn_name = column[len(CODE_FACTOR_PREFIX) :]
            by_function.setdefault(fn_name, {})["code_version"] = value
        elif column == LATEST_COLUMN:
            # The source's "these are the current records" recommendation. scidb
            # spells the same thing `code_version="latest"`, resolved per schema
            # location by the same rule — not "the highest ordinal".
            by_function.setdefault(None, {})["code_version"] = LATEST
        else:
            fn_name, _, param = column.rpartition(".")
            by_function.setdefault(fn_name or None, {})[param] = value

    expression = input_variable
    for fn_name in sorted(by_function, key=lambda n: (n is None, n or "")):
        arguments = []
        if fn_name:
            arguments.append(f"fn={fn_name!r}")
        arguments.extend(f"{key}={value!r}" for key, value in by_function[fn_name].items())
        expression = f"Variant({expression}, {', '.join(arguments)})"
    return expression


def _single_variant_expression(input_variable: str, spec) -> str:
    """The lone variant's pin, or the bare variable when nothing is selected.

    The row may name its own variable, in which case that is what the endpoint
    loads — ``input_variable`` is only the default.
    """
    sets = defined_sets(spec.variant_sets)
    if len(sets) != 1:
        return input_variable
    return variant_expression(sets[0].variable or input_variable, sets[0])


def _foreach_call(
    *,
    spec: PlotSpec,
    function_name: str,
    input_variable: str,
    x_variable: str | None,
    output_variable: str,
    path_template: str,
    iterate_keys: list[str],
    finalized: bool,
) -> str:
    variant_inputs = variant_params(spec)
    if variant_inputs:
        # One input per named variant, each loaded through its own pin. See
        # `codegen.variant_params` for why this cannot be a single `df`.
        # Zipped against `defined_sets`, not `spec.variant_sets`: unfilled rows
        # produce no input, so indexing the raw list would pair a parameter with
        # the wrong variant's selection.
        inputs = [
            f'        "{generated.param}": '
            f"{variant_expression(generated.variable, variant)},"
            for generated, variant in zip(
                variant_inputs, defined_sets(spec.variant_sets), strict=True
            )
        ]
        table_inputs = [generated.param for generated in variant_inputs]
    else:
        pinned = _single_variant_expression(input_variable, spec)
        inputs = [f'        "df": {pinned},']
        table_inputs = ["df"]
    if x_variable:
        inputs.append(f'        "df_x": {x_variable},')
        table_inputs.append("df_x")
    for group in spec.factor_variables:
        # A grouping variable arrives as its own input and is merged onto the
        # data inside the function: `as_table` hands a function schema keys and
        # data columns only, so a subject-level Condition cannot ride along on
        # the measure's frame.
        inputs.append(f'        "{group_param(group)}": {group},')
        table_inputs.append(group_param(group))
    inputs.append(f'        "filename": PathOutput("{path_template}"),')

    lines = [
        "for_each(",
        f"    {function_name},",
        "    inputs={",
        *inputs,
        "    },",
        f"    outputs=[{output_variable}],",
        # A plot_ function receives the long-format table (schema keys as
        # columns) — as_table defaults ON only for stat_, so say it explicitly.
        f"    as_table={table_inputs!r},",
        f"    finalized={finalized},",
    ]
    for key in iterate_keys:
        # [] means "every value present" — the same all-values resolution
        # scifor applies to an empty iteration list.
        lines.append(f"    {key}=[],")
    lines.append(")")
    return "\n".join(lines)


def required_declarations(code: EndpointCode) -> list[str]:
    """
    Variable types the generated call needs that may not exist yet.

    The GUI declares these through the normal entity-declaration path before
    writing the code, so a generated endpoint never references an undeclared
    type.
    """
    return [code.output_variable]
