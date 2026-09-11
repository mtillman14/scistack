"""
Variant selection, in scidb's vocabulary.

Two jobs, both translations rather than policy:

* :func:`variant_set` turns a ``scidb.Variant`` — the selector a scientist
  already writes in ``for_each`` — into the column-keyed
  ``scistackplot.VariantSet`` a spec can serialize. Writing the selector once
  and having it mean the same thing in a run and in a figure is the whole point;
  a second, plotting-only dialect for "the v1 records" would be one more thing
  to keep in sync and one more thing to get wrong.
* :func:`variant_graph` answers what a variant picker needs to draw itself:
  which axes exist for a variable, and every version each function in its chain
  has ever run under.

The translation lives here because it needs *both* halves: scidb's namespacing
(``__code__``, ``bandpass.low_hz``) and the plotting layer's column names
(``Code:bandpass``). scistackplot must not import scidb — the CSV path depends
on that — and scidb has no business knowing what a figure calls a column.
"""

from __future__ import annotations

from typing import Any

from scistacklog import Log
from scistackplot import CODE_FACTOR_PREFIX, LongTable, VariantSet, natural_sort_key

from .load import LATEST_COLUMN

LAYER = "scistackplotdb"


def variant_set(
    name: str | None,
    variant: Any,
    table: LongTable | None = None,
) -> VariantSet:
    """
    Build a named :class:`~scistackplot.spec.VariantSet` from a ``scidb.Variant``.

    ::

        table = source.get_table(["StepLength"])
        spec = PlotSpec(
            measures=["StepLength"],
            variant_sets=[
                variant_set("baseline",   Variant(StepLength, code_version="v1"), table),
                variant_set("new filter", Variant(StepLength, fn="bandpass", low_hz=20), table),
            ],
            roles={"Variant": Role.COLOR, "session": Role.X},
        )

    ``table`` is what resolves the *abbreviations* scidb allows — a bare
    ``code_version=`` (which function?) and a bare branch-param name (which
    producing function's ``low_hz``?). Both are resolved exactly as scidb
    resolves them at load time: by looking at what actually exists, and raising
    rather than guessing when more than one candidate matches. Omit ``table``
    only when every key is already unambiguous (``fn=``-qualified, or a dotted
    name), and this raises if one is not — a variant that quietly selected the
    wrong function's version would be undetectable in the figure.
    """
    branch_params = getattr(variant, "branch_params", None)
    if branch_params is None:
        raise TypeError(
            f"variant_set expects a scidb.Variant (or anything carrying "
            f"branch_params); got {type(variant).__name__}. For a selection you "
            f"have already written as columns, build "
            f"scistackplot.VariantSet(name, selection) directly."
        )
    return VariantSet(name=name, selection=selection_for(branch_params, table))


def selection_for(
    branch_params: dict[str, Any], table: LongTable | None = None
) -> dict[str, Any]:
    """``Variant.branch_params`` → a column-keyed selection.

    ``__code__``/``__code__.fn`` become ``Code:fn``; branch params keep their
    namespaced name, which is already the column name.
    """
    from scidb.variant import CODE_PIN_PREFIX

    axes = _axes_of(table)
    code_axes = [a for a in axes if a["kind"] == "code"]

    selection: dict[str, Any] = {}
    for key, value in branch_params.items():
        if key == CODE_PIN_PREFIX:
            selection[_bare_code_column(code_axes)] = value
        elif key.startswith(f"{CODE_PIN_PREFIX}."):
            selection[f"{CODE_FACTOR_PREFIX}{key.split('.', 1)[1]}"] = value
        else:
            selection[_param_column(key, axes)] = value
    return selection


def _axes_of(table: LongTable | None) -> list[dict]:
    if table is None:
        return []
    return [f.origin for f in table.variant_factors if f.origin]


def _bare_code_column(code_axes: list[dict]) -> str:
    """The column a bare ``code_version=`` means, or a message naming the choice.

    Same rule as ``scidb.database._filter_records_by_code_version``: exactly one
    versioned function is unambiguous, several are not.
    """
    if len(code_axes) == 1:
        return code_axes[0]["column"]
    if not code_axes:
        raise ValueError(
            "code_version= needs a table to resolve against (pass one), and "
            "that table must hold at least one versioned function. If you know "
            'the function, name it: Variant(X, fn="bandpass", code_version="v1").'
        )
    names = sorted(a["function"] for a in code_axes)
    raise ValueError(
        f"code_version= is ambiguous: {len(names)} functions in this table hold "
        f"more than one version ({names}). Name one with "
        f'fn="{names[0]}", or add a variant per function.'
    )


def _param_column(key: str, axes: list[dict]) -> str:
    """Resolve a branch-param key to its column, suffix-matching a bare name."""
    columns = [a["column"] for a in axes if a["kind"] == "param"]
    if not columns or key in columns:
        # Already namespaced (or nothing to check it against — the caller passed
        # no table, having promised the key is exact).
        return key
    hits = [c for c in columns if c.rsplit(".", 1)[-1] == key]
    if len(hits) == 1:
        return hits[0]
    if not hits:
        raise ValueError(
            f"No variant axis matches branch param {key!r}. This table's axes "
            f"are {sorted(columns)}."
        )
    raise ValueError(
        f"Branch param {key!r} is ambiguous — {len(hits)} producing functions "
        f"use it ({sorted(hits)}). Disambiguate with fn=, e.g. "
        f'Variant(X, fn="{hits[0].rsplit(".", 1)[0]}", {key}=…).'
    )


def variant_graph(db, variable_frame, functions: list[str] | None = None) -> dict:
    """
    Everything a variant picker needs for one variable.

    Returns ``{"axes": [...], "versions": {fn: [...]}, "latest_column": ...}``:

    ``axes``
        One entry per variant column — ``kind``/``function``/``param`` from
        :func:`~scistackplotdb.load.attach_variants`, plus the levels present.
    ``versions``
        Every recorded version of every function in this variable's upstream
        chain, **including functions that have only ever had one**. Single
        version functions are not an *axis* (they distinguish no two records),
        but they are still a legitimate thing to pick a version of, and a
        dropdown that omitted them would be empty for the common case.
        ``functions`` adds further names to look up — the GUI passes every
        function node on the canvas, so nodes outside this variable's chain can
        still say what they have run.
    """
    from scidb.provenance_query import code_versions_batch, function_versions

    frame = variable_frame.frame
    record_ids = frame["record_id"].tolist() if "record_id" in frame.columns else []
    chain: set[str] = set()
    if record_ids:
        for by_fn in code_versions_batch(db._duck, record_ids).values():
            chain.update(by_fn)

    wanted = sorted(chain | set(functions or ()))
    versions = function_versions(db._duck, wanted) if wanted else {}

    axes = []
    for axis in variable_frame.variant_axes:
        column = axis["column"]
        levels = (
            sorted(
                {str(v) for v in frame[column].dropna().unique()},
                key=natural_sort_key,
            )
            if column in frame.columns
            else []
        )
        axes.append({**axis, "levels": levels})

    Log.info(
        "variant_graph(%s): %d axis/axes, versions for %d function(s) "
        "(%d in this variable's chain)",
        variable_frame.name,
        len(axes),
        len(versions),
        len(chain),
        layer=LAYER,
    )
    # Each axis in the vocabulary the PICKER has to match on. `param` here is the
    # producing function's ARGUMENT name (scidb's `fn.param` namespacing) — not
    # the name of the Parameter entity feeding it, which is what the canvas node
    # is labelled with. The two coincide only until someone renames a Parameter
    # or wires the port from a glue node, and when they stop coinciding the axis
    # silently drops out of the graph. Logged so the popup's binding can be
    # checked against what it was actually given.
    for axis in axes:
        Log.info(
            "  axis %s: kind=%s function=%s param=%s, %d level(s) %s",
            axis["column"],
            axis["kind"],
            axis["function"],
            axis["param"],
            len(axis["levels"]),
            axis["levels"][:10],
            layer=LAYER,
        )
    return {
        "variable": variable_frame.name,
        "axes": axes,
        "versions": versions,
        "chain_functions": sorted(chain),
        "latest_column": variable_frame.latest_column or LATEST_COLUMN,
    }
