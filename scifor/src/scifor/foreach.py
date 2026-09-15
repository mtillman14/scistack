"""Pure for_each loop — works with DataFrames, no DuckDB/database knowledge.

Filesystem access via PathInput/PathOutput is fine and by design; the
boundary this module holds is "no DuckDB knowledge," not "no I/O."

Logging: emits through the scistacklog facade with ``layer="scifor"``.
INFO carries the run narrative (banner, periodic progress, end-of-run
summary with failure reasons); DEBUG carries per-iteration detail
([run]/[skip]/[done] lines, internal flow). Dry-run output goes to stdout
via print() — it is the requested result, not logging.
"""

import time
from collections.abc import Callable
from itertools import product
from typing import TYPE_CHECKING, Any

from scistacklog import Log

if TYPE_CHECKING:
    import pandas as pd

from .colname import ColName
from .column_selection import ColumnSelection
from .each_of import EachOf, require_alternatives
from .fixed import Fixed
from .locations import LocationFilter, filter_combos
from .merge import Merge
from .pathinput import PathInput
from .pathoutput import PathOutput
from .schema import expand_schema_keys, get_schema

# Periodic progress guards (module-level so tests can monkeypatch).
# A progress line is emitted when the outermost iterated key's value changes,
# but never at the very first combo, never within the first
# _PROGRESS_START_DELAY_S of the loop (fast runs emit none), and never more
# often than _PROGRESS_MIN_INTERVAL_S (degenerate shapes: huge or only key).
_PROGRESS_MIN_INTERVAL_S = 2.0
_PROGRESS_START_DELAY_S = 5.0
# Value-list preview length in the banner, and combos listed per failure
# reason in the end-of-run summary.
_VALUES_PREVIEW_MAX = 4
_SUMMARY_COMBOS_MAX = 5


def _format_value_list(values) -> str:
    """``12 values [1, 2, 3, …, 12]`` — truncated preview of an iterable."""
    n = len(values)
    shown = [repr(v) for v in values]
    if n > _VALUES_PREVIEW_MAX:
        shown = shown[: _VALUES_PREVIEW_MAX - 1] + ["…", repr(values[-1])]
    return f"{n} value{'s' if n != 1 else ''} [{', '.join(shown)}]"


def _record_iteration_failure(
    failure_reasons: dict,
    warned_reasons: set,
    exc: Exception,
    metadata_str: str,
    context: str,
) -> None:
    """Track a per-iteration failure for the end-of-run summary.

    Every failure logs a [skip] line at DEBUG; the first occurrence of each
    distinct reason also logs at WARN with a traceback, so the default
    (INFO) log still answers "what failed and why" — except NoDataError,
    which is an expected outcome (this combo has no backing data) rather
    than a bug, so it never escalates to WARN and carries no traceback.
    """
    reason = f"{type(exc).__name__}: {exc}"
    failure_reasons.setdefault(reason, []).append(metadata_str)
    if isinstance(exc, NoDataError):
        Log.debug(f"[skip] {metadata_str}: {context}: {exc}", layer="scifor")
        return
    if reason not in warned_reasons:
        warned_reasons.add(reason)
        Log.warn(
            f"iteration failed: {metadata_str} — {context}: {exc} "
            f"(first occurrence; traceback follows)",
            layer="scifor",
            exc_info=True,
        )
    Log.debug(f"[skip] {metadata_str}: {context}: {exc}", layer="scifor", exc_info=True)


def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    dry_run: bool = False,
    as_table: list[str] | bool | None = None,
    distribute: bool = False,
    where=None,
    locations: "LocationFilter | dict | None" = None,
    output_names: list[str] | int | None = None,
    share_limits: "dict[str, list[str]] | None" = None,
    schema_keys: "list[str] | None" = None,
    _all_combos: list[dict] | None = None,
    _log_fn: "Callable[[str], None] | None" = None,
    _progress_fn: "Callable[[dict], None] | None" = None,
    _cancel_check: "Callable[[], bool] | None" = None,
    _path_input_resolver: "Callable[['PathInput', dict], Any] | None" = None,
    _mapping_inputs: "dict[str, list[str]] | None" = None,
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | None":
    """
    Execute a function for all combinations of metadata, filtering
    DataFrame inputs per iteration.

    This is a pure loop orchestrator — no DuckDB/database knowledge.
    Filesystem access via PathInput/PathOutput inputs is fine and expected.
    All other inputs must be DataFrames or constants.

    Args:
        fn: The function to execute.
        inputs: Dict mapping parameter names to DataFrames, Fixed wrappers,
                Merge wrappers, ColumnSelection wrappers, or constant values.
        dry_run: If True, only print what would happen without executing.
        as_table: Controls which DataFrame inputs keep schema key columns.
                  True = all; list of names = selected; False/None = none.
        distribute: If True, split outputs by element/row and expand them
                    into the result table at the schema level below the
                    deepest iterated key. If no metadata_iterable is a
                    schema key (e.g. a fully static PathInput with no
                    {key} placeholders), distributes to the top of the
                    schema instead.
        where: Optional scifor.ColFilter/CompoundFilter to filter DataFrame
               rows after combo filtering.
        locations: Optional scifor.LocationFilter (or its mapping form) naming
               which schema locations to run: ragged ``include`` prefixes plus
               a standing ``exclude_levels`` rule. Filters COMBOS, where
               ``where=`` filters ROWS within a combo — the two are unrelated
               and compose. An empty/omitted filter is inert. See
               docs/claude/location-filter-semantics.md.
        output_names: Names for result columns. list[str] names them;
                      int N auto-names (output_1, ..., output_N);
                      None defaults to ["output"] (single output).
        share_limits: Optional ``{input_name: [schema_keys_to_hold_fixed]}``.
                      For each named input, computes the global numeric
                      ``(min, max)`` extent of that input's data within each
                      group defined by the held-fixed schema keys (spanning all
                      other iterated keys), and injects it as a
                      ``{input_name}_limits`` keyword argument on each call —
                      but only if the function's signature accepts that name
                      (or ``**kwargs``). Used so e.g. every per-trial plot within
                      a subject shares one y-axis range
                      (``share_limits={"signal": ["subject"]}``).
        schema_keys: Optional list of schema key names to iterate, structural
                    sugar for passing ``key=[]`` for each one by hand (each
                    still auto-resolves via a DataFrame scan of the inputs).
                    Mutually exclusive with explicit **metadata_iterables.
        _all_combos: Pre-built list of metadata dicts; skips itertools.product().
                     Used by DB wrappers that pre-filter schema combinations.
        _log_fn: Deprecated — ignored. scifor now logs through the
                 scistacklog facade (layer="scifor") directly; kept only so
                 existing call sites don't break.
        _path_input_resolver: Optional ``(pathinput, metadata) -> path``
                 override for per-combo PathInput resolution. Used by DB
                 wrappers that need schema-key-type-aware resolution;
                 defaults to plain ``pathinput.load(**metadata)``.
        _mapping_inputs: Optional ``{input_name: [data column names]}``
                 declaring that the named input's rows are records of a
                 *mapping-valued* variable — each row is one dict, spread one
                 column per key — rather than the rows of a table. A single
                 row of such an input is handed to the function as that dict
                 (a struct, from MATLAB), not as a one-row frame. This cannot
                 be inferred here: "one row, N data columns" is also exactly
                 what a genuine table-valued variable looks like, so the
                 caller that knows the storage layout has to say so (scidb's
                 ``_resolve_mapping_inputs`` / ``DatabaseManager
                 .mapping_data_columns``). Ignored for an input passed
                 through ``as_table`` or with a column selection.
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, or None when dry_run=True.
    """
    # Step -1: EachOf expansion — must be first, before any other logic.
    # Each alternative becomes an independent recursive for_each() call;
    # results are concatenated. Mirrors scidb.for_each's own EachOf
    # expansion (which additionally threads save/db/lineage per
    # alternative — concepts this pure layer doesn't have), so a
    # standalone/no-DB pipeline can use EachOf too.
    each_of_axes = []
    for param, val in inputs.items():
        if isinstance(val, EachOf):
            require_alternatives(val, kind="input", param=param)
            each_of_axes.append(("input", param, val.alternatives))
    if isinstance(where, EachOf):
        require_alternatives(where, kind="where")
        each_of_axes.append(("where", None, where.alternatives))

    if each_of_axes:
        from itertools import product as _eachof_product

        import pandas as pd

        results = []
        for combo in _eachof_product(*(axis[2] for axis in each_of_axes)):
            concrete_inputs = dict(inputs)
            concrete_where = where
            for (kind, param, _alts), value in zip(each_of_axes, combo, strict=False):
                if kind == "input":
                    concrete_inputs[param] = value
                elif kind == "where":
                    concrete_where = value
            result = for_each(
                fn,
                concrete_inputs,
                dry_run=dry_run,
                as_table=as_table,
                distribute=distribute,
                where=concrete_where,
                # Every alternative runs over the same locations: a selection
                # is about which data exists, not which variant produced it.
                # (Dropping a kwarg here is a silent, well-worn failure —
                # `glue=` went missing from this same recursion in 2026-09-10.)
                locations=locations,
                output_names=output_names,
                share_limits=share_limits,
                schema_keys=schema_keys,
                _progress_fn=_progress_fn,
                _cancel_check=_cancel_check,
                _path_input_resolver=_path_input_resolver,
                _mapping_inputs=_mapping_inputs,
                **metadata_iterables,
            )
            if result is not None:
                results.append(result)
            # Cooperative cancel: stop iterating across EachOf alternatives
            # as soon as the user cancels — don't start the next concrete run.
            if _cancel_check is not None and _cancel_check():
                break
        return pd.concat(results, ignore_index=True) if results else None

    full_schema_keys = get_schema()

    # Step 0: Forgive a bare ColName class passed without parentheses.
    # `scifor.ColName` (uninstantiated) can only mean the no-arg deferred form,
    # since there is no DataFrame to attach. Normalize it to ColName() so all
    # downstream isinstance(v, ColName) checks treat it uniformly. We remember
    # which inputs arrived this way to give a clearer error if they turn out to
    # lack a for_columns input to resolve against.
    bare_colname_params = [
        name
        for name, v in inputs.items()
        if isinstance(v, type) and issubclass(v, ColName)
    ]
    if bare_colname_params:
        inputs = {
            name: (v() if (isinstance(v, type) and issubclass(v, ColName)) else v)
            for name, v in inputs.items()
        }

    # Step 1: Resolve output_names
    if output_names is None:
        resolved_output_names = ["output"]
    elif isinstance(output_names, int):
        resolved_output_names = [f"output_{i + 1}" for i in range(output_names)]
    else:
        resolved_output_names = list(output_names)
    n_outputs = len(resolved_output_names)
    Log.debug(
        "resolve_output_names: %d output name(s): %s",
        n_outputs,
        resolved_output_names,
        layer="scifor",
    )

    # Step 1.5: schema_keys= structural sugar — seed metadata_iterables with
    # an empty list per requested key, before the empty-list resolver below
    # (Step 2) runs. Shared with scidb.for_each() via expand_schema_keys().
    if schema_keys is not None:
        metadata_iterables = expand_schema_keys(schema_keys, metadata_iterables)
        Log.debug(
            "expand_schema_keys: seeded metadata_iterables for %s",
            schema_keys,
            layer="scifor",
        )

    # Step 2: Resolve empty lists [] in standalone mode. Pass 1 scans
    # DataFrame inputs (non-raising); Pass 2 (Step 2.5) fills whatever's
    # still empty from PathInput filesystem discovery. Keys the user passed
    # with explicit (non-empty) values assert intent and are never
    # overwritten.
    if _all_combos is None:
        user_explicit_keys = {
            k
            for k, v in metadata_iterables.items()
            if not (isinstance(v, list) and len(v) == 0)
        }
        needs_resolve = [
            k
            for k, v in metadata_iterables.items()
            if isinstance(v, list) and len(v) == 0
        ]
        if needs_resolve:
            Log.debug(
                "resolve_empty_lists: scanning DataFrame inputs for %s",
                needs_resolve,
                layer="scifor",
            )
            for key in needs_resolve:
                values = _distinct_values_from_inputs(inputs, key)
                if values:
                    Log.debug(
                        "resolved '%s' to %d values: %s",
                        key,
                        len(values),
                        values,
                        layer="scifor",
                    )
                metadata_iterables[key] = values

        # Step 2.5: PathInput filesystem discovery for keys DataFrames
        # didn't resolve. The Case A/B decision (and whether discovered
        # combos drive iteration directly) is owned by
        # PathInput.apply_discovery so scifor and scidb share one
        # implementation.
        pi = _find_pathinput(inputs)
        if pi is not None and (
            not metadata_iterables
            or any(
                isinstance(v, list) and len(v) == 0
                for v in metadata_iterables.values()
            )
        ):
            metadata_iterables, discovered_combos = resolve_pathinput_discovery(
                pi,
                metadata_iterables,
                user_explicit_keys,
                log=lambda msg: Log.debug(msg, layer="scifor"),
                condense_numeric=True,
            )
            if discovered_combos is not None:
                _all_combos = discovered_combos
                Log.debug(
                    "pathinput_discovery: using %d disk combos",
                    len(discovered_combos),
                    layer="scifor",
                )

        # Any key still unresolved: warn (0 iterations) when a source
        # exists but yields no values; error when no input provides the
        # key at all. (A key a fully static PathInput can never supply was
        # already dropped above by resolve_pathinput_discovery, so it
        # won't reach this loop.)
        for key in list(metadata_iterables.keys()):
            v = metadata_iterables[key]
            if isinstance(v, list) and len(v) == 0:
                if _key_has_source(inputs, key, pi):
                    Log.warn(
                        f"no values found for '{key}' in inputs, 0 iterations",
                        layer="scifor",
                    )
                else:
                    raise ValueError(
                        f"Empty list [] was passed for '{key}', but no input "
                        f"DataFrame has that column and no PathInput template "
                        f"has a {{{key}}} placeholder. Provide values "
                        f"explicitly, add a DataFrame input with a '{key}' "
                        f"column, or use a PathInput with a {{{key}}} "
                        f"placeholder."
                    )

    # Step 3: Validate distribute parameter and resolve target key.
    # Internal discriminator keys (scidb's __rid_* record-id and __vsig_*
    # variant-signature schema extensions) are not experimental LEVELS — they
    # must be invisible to distribute resolution, or an aggregation over a
    # variant-tracked input would see the discriminator as the deepest key
    # and refuse to distribute.
    #
    # The resolved target is reported at INFO, not DEBUG. This line is the one
    # observable difference between "distribute ran" and "distribute was
    # silently dropped somewhere upstream", and the latter fails invisibly:
    # the run still succeeds and still saves, just at the wrong granularity.
    # At DEBUG the two were indistinguishable in scidb.log, which is how a
    # MATLAB run with distribute=true went undiagnosed until the saved columns
    # were inspected by hand (2026-09-14).
    distribute_key = None
    if distribute:
        real_schema_keys = [
            k
            for k in full_schema_keys
            if "__rid_" not in str(k) and "__vsig_" not in str(k)
        ]
        if not real_schema_keys:
            raise ValueError(
                "distribute=True requires a schema. Call set_schema() or "
                "configure_database() first."
            )
        iter_keys_in_schema = [k for k in real_schema_keys if k in metadata_iterables]
        if not iter_keys_in_schema:
            # Nothing is being iterated at a schema level (e.g. a fully
            # static PathInput with no {key} placeholders, or no
            # metadata_iterables at all) — distribute to the top of the
            # schema rather than erroring.
            distribute_key = real_schema_keys[0]
            Log.info(
                "resolve_distribute_target: '%s' (top of schema; nothing iterated)",
                distribute_key,
                layer="scifor",
            )
        else:
            deepest_iterated = iter_keys_in_schema[-1]
            deepest_idx = real_schema_keys.index(deepest_iterated)

            if deepest_idx + 1 >= len(real_schema_keys):
                raise ValueError(
                    f"distribute=True but '{deepest_iterated}' is the deepest schema key. "
                    f"There is no lower level to distribute to. "
                    f"Schema order: {real_schema_keys}"
                )
            distribute_key = real_schema_keys[deepest_idx + 1]
            Log.info(
                "resolve_distribute_target: '%s' (one level below '%s')",
                distribute_key,
                deepest_iterated,
                layer="scifor",
            )

    # Capture input schema-key column dtypes for output round-trip: output
    # metadata columns must come back as EXACTLY the input column's dtype
    # (int stays int, object/str stays object with values verbatim,
    # categorical stays categorical with its categories and orderedness).
    # Restored in _results_to_output_dataframe; mirrors the MATLAB scifor
    # loop's capture_schema_column_types/restore_schema_column_types.
    type_keys = list(
        dict.fromkeys(
            [*full_schema_keys, *metadata_iterables.keys()]
            + ([distribute_key] if distribute_key is not None else [])
        )
    )
    schema_col_dtypes = _capture_schema_column_dtypes(inputs, type_keys)

    # Resolve static ColName(df) wrappers before the data/constant split.
    # Deferred ColName() markers (no DataFrame) are left in place — they resolve
    # per-column inside the for_columns iteration loop (validated below).
    static_count = sum(
        1 for v in inputs.values() if isinstance(v, ColName) and not v.is_deferred
    )
    deferred_count = sum(
        1 for v in inputs.values() if isinstance(v, ColName) and v.is_deferred
    )
    if static_count or deferred_count:
        Log.debug(
            "resolve_colnames: %d static ColName(df) wrapper(s); deferring "
            "%d no-arg ColName() marker(s) to for_columns iteration",
            static_count,
            deferred_count,
            layer="scifor",
        )
    inputs = _resolve_colnames(inputs, full_schema_keys)

    # Step 5: Separate data inputs from constants
    data_inputs = {}
    constant_inputs = {}
    for param_name, var_spec in inputs.items():
        if _is_data_input(var_spec):
            data_inputs[param_name] = var_spec
        else:
            constant_inputs[param_name] = var_spec
    Log.debug(
        "classify_inputs: %d data input(s), %d constant(s)",
        len(data_inputs),
        len(constant_inputs),
        layer="scifor",
    )

    # Check distribute doesn't conflict with a constant input name
    if distribute_key is not None and distribute_key in constant_inputs:
        raise ValueError(
            f"distribute target '{distribute_key}' conflicts with a constant input named '{distribute_key}'."
        )

    # Step 6: Build set of input names to keep as full DataFrames (with schema cols)
    if as_table is True:
        as_table_set = set(data_inputs.keys())
    elif as_table:
        as_table_set = set(as_table)
    else:
        as_table_set = set()
    if as_table_set:
        Log.debug("as_table inputs: %s", sorted(as_table_set), layer="scifor")

    # Step 6.5: Detect iterate-mode ColumnSelection inputs (for_columns).
    # These fan out column-wise: fn runs once per column and the per-column
    # results are reassembled into one wide row per combo. All iterate inputs
    # share a single column axis (zipped by name).
    iterate_params = [
        name
        for name, spec in data_inputs.items()
        if getattr(_unwrap_column_selection(spec), "iterate", False)
    ]

    # Deferred ColName() markers resolve to the current iterated column, so they
    # require at least one for_columns input. (Static ColName(df) was already
    # resolved to a string at Step 4, so only no-arg markers remain here.)
    deferred_colname_params = [
        name
        for name, v in constant_inputs.items()
        if isinstance(v, ColName) and v.is_deferred
    ]
    if deferred_colname_params and not iterate_params:
        bare_here = [p for p in deferred_colname_params if p in bare_colname_params]
        if bare_here:
            hint = (
                f"Input(s) {bare_here} were given the bare ColName class "
                f"(no parentheses), which is treated as the deferred ColName() form. "
                f"If you meant the static single-column form, instantiate it with a "
                f"DataFrame: ColName(df). Otherwise add a for_columns input to "
                f"iterate over."
            )
        else:
            hint = "Use ColName(df) for the static single-column form instead."
        raise ValueError(
            f"ColName() with no argument resolves to the current for_columns "
            f"column, so it requires at least one iterate input "
            f"(for_columns() / ColumnSelection(..., iterate=True)). "
            f"Deferred ColName() input(s): {deferred_colname_params}. "
            f"{hint}"
        )

    # A PathOutput template using {ColName} resolves per-column, so it likewise
    # needs an iterate input. (Metadata-only PathOutput templates are fine
    # without one — they resolve per-combo.)
    path_output_column_params = [
        name
        for name, v in constant_inputs.items()
        if isinstance(v, PathOutput) and v.has_column_token
    ]
    if path_output_column_params and not iterate_params:
        raise ValueError(
            f"PathOutput template uses the {{ColName}} token, which resolves to "
            f"the current for_columns column, so it requires at least one iterate "
            f"input (for_columns() / ColumnSelection(..., iterate=True)). "
            f"PathOutput input(s): {path_output_column_params}."
        )

    iterate_columns: list[str] | None = None
    if iterate_params:
        col_sets = {
            name: _resolve_iterate_columns(data_inputs[name], full_schema_keys)
            for name in iterate_params
        }
        iterate_columns = col_sets[iterate_params[0]]
        for name in iterate_params[1:]:
            if set(col_sets[name]) != set(iterate_columns):
                raise ValueError(
                    f"for_columns inputs must iterate over the same columns "
                    f"(zipped by name). '{iterate_params[0]}' has "
                    f"{iterate_columns} but '{name}' has {col_sets[name]}."
                )
        if n_outputs != 1:
            raise ValueError(
                f"for_columns supports exactly one output; got {n_outputs} "
                f"({resolved_output_names})."
            )
        if distribute_key is not None:
            raise ValueError("for_columns cannot be combined with distribute=True.")
        Log.debug(
            "resolve_iterate_columns: column iteration over %d column(s) %s "
            "for input(s) %s",
            len(iterate_columns),
            iterate_columns,
            iterate_params,
            layer="scifor",
        )

    # Build combo list
    if _all_combos is not None:
        all_combos = _all_combos
        keys = list(metadata_iterables.keys())
        Log.debug(
            "expand_combos: using %d pre-built combos (from DB wrapper)",
            len(all_combos),
            layer="scifor",
        )
    else:
        keys = list(metadata_iterables.keys())
        value_lists = [metadata_iterables[k] for k in keys]
        all_combos = [
            dict(zip(keys, combo, strict=False)) for combo in product(*value_lists)
        ]
        Log.debug(
            "expand_combos: built %d combos from Cartesian product of %s",
            len(all_combos),
            keys,
            layer="scifor",
        )

    fn_name = getattr(fn, "__name__", repr(fn))

    # Step 6.5: locations= — narrow the combo list to the selected schema
    # locations. Applied to BOTH branches above on purpose: a filter that only
    # touched the Cartesian branch would be silently ignored on every
    # database-driven run, which is the path that has one (scidb hands its
    # pre-filtered combos in through _all_combos).
    if locations is not None:
        all_combos = filter_combos(all_combos, locations, context=f"for_each({fn_name})")

    total = len(all_combos)

    # Step 7.5: share_limits prepass — compute per-group numeric extents so all
    # combos in a group (e.g. all trials within a subject) can share axis limits.
    shared_limits_map: dict = {}
    if share_limits:
        shared_limits_map = _compute_shared_limits(
            share_limits, data_inputs, full_schema_keys
        )
        # Param names the function will accept the *_limits kwargs under.
        _limits_accepted = _accepted_param_names(fn)
        Log.debug(
            "compute_shared_limits: %s (fn accepts: %s)",
            list(shared_limits_map.keys()),
            sorted(_limits_accepted)
            if _limits_accepted is not None
            else "any (**kwargs)",
            layer="scifor",
        )
    else:
        _limits_accepted = None

    # Run banner: one INFO line with a truncated per-key value preview; the
    # full value lists and input details follow at DEBUG.
    display_keys = [k for k in keys if not k.startswith("__")]
    meta_summary = (
        ", ".join(
            f"{k}={_format_value_list(metadata_iterables[k])}" for k in display_keys
        )
        if display_keys
        else "no metadata"
    )
    # The per-key previews are each key's FULL value list, so their product can
    # exceed the real iteration count — combos get filtered out beforehand
    # (PathInput discovery dropping combos with no file on disk, where=,
    # skip_computed). Reporting "4 iterations: subject=3 values, session=2
    # values" without saying so reads as an arithmetic error.
    _cartesian = 1
    for _k in display_keys:
        _cartesian *= len(metadata_iterables[_k])
    _pruned_note = ""
    if display_keys and _cartesian > total:
        _pruned_note = (
            f" (of {_cartesian} possible combination(s); "
            f"{_cartesian - total} filtered out before iteration)"
        )
    Log.info(
        f"for_each({fn_name}) — {total} iteration{'s' if total != 1 else ''}"
        f"{_pruned_note}: {meta_summary}",
        layer="scifor",
    )
    # Which combos were dropped is the actual diagnostic question. Only worth
    # materialising the product for a small one.
    if _pruned_note and _cartesian <= 1000:
        _kept = {tuple(str(c.get(k)) for k in display_keys) for c in all_combos}
        _dropped = [
            dict(zip(display_keys, combo, strict=False))
            for combo in product(*(metadata_iterables[k] for k in display_keys))
            if tuple(str(v) for v in combo) not in _kept
        ]
        Log.debug(
            "filtered out %d combination(s): %s",
            len(_dropped),
            _dropped,
            layer="scifor",
        )

    _inputs_str = _format_inputs(inputs)
    Log.info(f"inputs: {_inputs_str}", layer="scifor")

    for k in display_keys:
        vals = metadata_iterables[k]
        formatted = ", ".join(repr(v) for v in vals)
        Log.debug("%s=[%s]", k, formatted, layer="scifor")

    # Non-default options
    _opts_parts = []
    if dry_run:
        _opts_parts.append("dry_run=True")
    if distribute:
        _opts_parts.append("distribute=True")
    if as_table:
        _opts_parts.append(f"as_table={as_table!r}")
    if where is not None:
        _opts_parts.append(f"where={where!r}")
    if locations is not None and not LocationFilter.of(locations).is_empty():
        _location_filter = LocationFilter.of(locations)
        _opts_parts.append(
            f"locations={len(_location_filter.include)} prefix(es), "
            f"excluding {dict(_location_filter.exclude_levels) or '{}'}"
        )
    if _opts_parts:
        Log.info(f"options: {', '.join(_opts_parts)}", layer="scifor")

    if dry_run:
        # Dry-run output is the requested result, not logging: print to stdout.
        print(f"[dry-run] for_each({fn_name})")
        print(f"[dry-run] {total} iterations over: {keys}")
        print(f"[dry-run] inputs: {_format_inputs(inputs)}")
        if distribute_key is not None:
            print(
                f"[dry-run] distribute: '{distribute_key}' (split outputs by element/row, 1-based)"
            )
        print()

    completed = 0
    skipped = 0
    collected_rows: list[tuple[dict, tuple]] = []
    was_cancelled = False
    # Failure aggregation for the end-of-run summary: reason -> combo strings.
    failure_reasons: dict[str, list[str]] = {}
    warned_reasons: set[str] = set()

    # Periodic progress state: a line per outermost-key transition (guarded).
    progress_key = display_keys[0] if display_keys else None
    if progress_key is not None:
        try:
            progress_total = len(dict.fromkeys(c.get(progress_key) for c in all_combos))
        except TypeError:  # unhashable values — fall back to declared list
            progress_total = len(metadata_iterables.get(progress_key, []))
    else:
        progress_total = 0
    _no_value = object()
    progress_last_value = _no_value
    progress_seen = 0
    progress_last_emit = 0.0
    loop_t0 = time.perf_counter()

    for combo_idx, metadata in enumerate(all_combos):
        # Cooperative cancel: check between combos (before any work for this combo).
        if _cancel_check is not None and _cancel_check():
            was_cancelled = True
            _nd = sum(
                len(c)
                for r, c in failure_reasons.items()
                if r.startswith("NoDataError:")
            )
            Log.info(
                f"for_each({fn_name}) cancelled at combo {combo_idx + 1}/{total} "
                f"(completed={completed}, failed={skipped - _nd}, no_data={_nd})",
                layer="scifor",
            )
            if _progress_fn is not None:
                _progress_fn(
                    {
                        "event": "cancelled",
                        "current": combo_idx + 1,
                        "total": total,
                        "completed": completed,
                        "skipped": skipped,
                    }
                )
            break

        metadata_str = ", ".join(f"{k}={v}" for k, v in metadata.items())

        # Periodic progress: emit on outermost-key transitions, but never at
        # the very first combo, never inside the start delay (fast runs stay
        # silent), and never more often than the minimum interval.
        if progress_key is not None and not dry_run:
            progress_value = metadata.get(progress_key, _no_value)
            if progress_value != progress_last_value:
                progress_last_value = progress_value
                progress_seen += 1
                elapsed_now = time.perf_counter() - loop_t0
                if (
                    progress_seen > 1
                    and elapsed_now >= _PROGRESS_START_DELAY_S
                    and elapsed_now - progress_last_emit >= _PROGRESS_MIN_INTERVAL_S
                ):
                    progress_last_emit = elapsed_now
                    _nd = sum(
                        len(c)
                        for r, c in failure_reasons.items()
                        if r.startswith("NoDataError:")
                    )
                    Log.info(
                        f"progress: {progress_key}={progress_value} "
                        f"({progress_seen}/{progress_total}) — "
                        f"{combo_idx}/{total} combos "
                        f"({100.0 * combo_idx / total:.1f}%), "
                        f"completed={completed}, failed={skipped - _nd}, "
                        f"no_data={_nd}, elapsed={elapsed_now:.1f}s",
                        layer="scifor",
                    )

        if _progress_fn is not None:
            _progress_fn(
                {
                    "event": "combo_start",
                    "current": combo_idx + 1,
                    "total": total,
                    "completed": completed,
                    "skipped": skipped,
                    "metadata": metadata,
                }
            )

        if dry_run:
            _print_dry_run_iteration(inputs, metadata, constant_inputs, distribute_key)
            completed += 1
            continue

        # Filter/prepare inputs for this combo
        filtered_inputs = {}
        iterate_dfs: dict[str, Any] = {}
        filter_failed = False

        for param_name, var_spec in data_inputs.items():
            try:
                if param_name in iterate_params:
                    # Keep the full per-combo DataFrame; slice per column below.
                    iterate_dfs[param_name] = _prepare_iterate_df(
                        var_spec, metadata, full_schema_keys, where
                    )
                    continue
                wants_table = param_name in as_table_set
                filtered_inputs[param_name] = _prepare_input(
                    var_spec,
                    metadata,
                    full_schema_keys,
                    wants_table,
                    where,
                    mapping_cols=(_mapping_inputs or {}).get(param_name),
                )
            except Exception as e:
                _record_iteration_failure(
                    failure_reasons,
                    warned_reasons,
                    e,
                    metadata_str,
                    f"failed to filter {param_name}",
                )
                filter_failed = True
                break

        if filter_failed:
            skipped += 1
            if _progress_fn is not None:
                _progress_fn(
                    {
                        "event": "combo_skip",
                        "current": combo_idx + 1,
                        "total": total,
                        "completed": completed,
                        "skipped": skipped,
                        "metadata": metadata,
                        "error": "filter failed",
                    }
                )
            continue

        # Column drift is a hard error (not a per-combo skip): the iterate
        # column set is fixed up front, so a combo missing one of those
        # columns means the stored data is inconsistent and must be surfaced.
        if iterate_params:
            for name in iterate_params:
                missing = [
                    c for c in iterate_columns if c not in iterate_dfs[name].columns
                ]
                if missing:
                    raise ValueError(
                        f"for_columns column drift: column(s) {missing} missing from "
                        f"input '{name}' for combo {metadata_str}. The iterate column "
                        f"set {iterate_columns} must be present in every combo."
                    )

        # Surface empty per-combo inputs explicitly. After upstream existence
        # filtering an empty input usually means this combo has no backing data
        # for that input — the function is still called (it may handle empties),
        # but the emptiness must never be silent.
        _empty_inputs = [
            name
            for name, val in (list(filtered_inputs.items()) + list(iterate_dfs.items()))
            if _input_is_empty(val)
        ]
        if _empty_inputs:
            Log.debug(
                "[empty-combo] %s: input(s) %s had 0 rows",
                metadata_str,
                ", ".join(_empty_inputs),
                layer="scifor",
            )

        # Call the function
        all_param_names = (
            list(filtered_inputs.keys())
            + list(iterate_params)
            + list(constant_inputs.keys())
        )
        if iterate_params:
            msg = (
                f"[run] {metadata_str}: {fn_name} x {len(iterate_columns)} column(s) "
                f"({', '.join(all_param_names)})"
            )
        else:
            msg = f"[run] {metadata_str}: {fn_name}({', '.join(all_param_names)})"
        Log.debug(msg, layer="scifor")

        # Merge constants into function arguments
        filtered_inputs.update(constant_inputs)

        # Inject shared axis limits for this combo's group (share_limits).
        if shared_limits_map:
            for input_name, (group_keys, limits) in shared_limits_map.items():
                param = f"{input_name}_limits"
                if _limits_accepted is not None and param not in _limits_accepted:
                    continue  # fn signature doesn't accept it and has no **kwargs
                gkey = tuple(str(metadata.get(k, "")) for k in group_keys)
                if gkey in limits:
                    filtered_inputs[param] = limits[gkey]

        try:
            fn_t0 = time.perf_counter()
            if iterate_params:
                result = (
                    _run_column_iteration(
                        fn,
                        filtered_inputs,
                        iterate_dfs,
                        iterate_columns,
                        full_schema_keys,
                        as_table_set,
                        metadata,
                        path_input_resolver=_path_input_resolver,
                    ),
                )
            else:
                # PathOutput/PathInput constants resolve to a finished path
                # from this combo's metadata (no column outside for_columns).
                call_inputs = _resolve_path_outputs(filtered_inputs, metadata, None)
                call_inputs = _resolve_path_inputs(
                    call_inputs, metadata, resolver=_path_input_resolver
                )
                result = _call_fn(fn, call_inputs, n_outputs)
            fn_elapsed = time.perf_counter() - fn_t0
            Log.debug(
                "[done] %s: %s completed in %.3fs",
                metadata_str,
                fn_name,
                fn_elapsed,
                layer="scifor",
            )
        except ColumnFunctionError as e:
            # The function failed on specific columns. This is deterministic
            # across combos (a bad column is bad everywhere), so surface it as a
            # hard error naming every offending column rather than silently
            # skipping the whole combo.
            Log.error(f"[error] {metadata_str}: {e}", layer="scifor", exc_info=True)
            raise
        except ForColumnsError:
            # Structural for_columns errors are deterministic across combos and
            # indicate a return-contract bug — surface immediately, don't skip.
            raise
        except Exception as e:
            if getattr(e, "scifor_fatal", False):
                # Deterministic configuration errors marked fatal by a
                # higher layer (e.g. scidb's SchemaKeyTypeError) would fail
                # every combo identically — abort the run instead of
                # skipping through N copies of the same failure.
                Log.error(f"[fatal] {metadata_str}: {e}", layer="scifor", exc_info=True)
                raise
            _record_iteration_failure(
                failure_reasons,
                warned_reasons,
                e,
                metadata_str,
                f"{fn_name} raised",
            )
            skipped += 1
            if _progress_fn is not None:
                _progress_fn(
                    {
                        "event": "combo_skip",
                        "current": combo_idx + 1,
                        "total": total,
                        "completed": completed,
                        "skipped": skipped,
                        "metadata": metadata,
                        "error": str(e),
                    }
                )
            continue

        # Normalize single output to tuple
        if not isinstance(result, tuple):
            result = (result,)

        # Handle distribute: expand result into multiple rows
        if distribute_key is not None:
            for output_value in result:
                try:
                    pieces = _split_for_distribute(output_value)
                except TypeError as e:
                    Log.warn(f"{metadata_str}: cannot distribute: {e}", layer="scifor")
                    continue

                for i, piece in enumerate(pieces):
                    dist_metadata = {**metadata, distribute_key: i + 1}
                    collected_rows.append((dist_metadata, (piece,)))
        else:
            collected_rows.append((metadata, result))

        completed += 1
        if _progress_fn is not None:
            _progress_fn(
                {
                    "event": "combo_done",
                    "current": combo_idx + 1,
                    "total": total,
                    "completed": completed,
                    "skipped": skipped,
                    "metadata": metadata,
                }
            )

    # End-of-run summary
    if dry_run:
        print(f"[dry-run] would process {total} iterations")
        return None

    elapsed = time.perf_counter() - loop_t0
    cancelled_suffix = ", cancelled" if was_cancelled else ""
    # NoDataError means the combo simply has no backing data (expected in a
    # sparse schema-key cross-product) — split it out from genuine `fn`
    # failures so the summary doesn't cry "failed" over missing data.
    no_data_reasons = {
        reason: combos
        for reason, combos in failure_reasons.items()
        if reason.startswith("NoDataError:")
    }
    no_data_count = sum(len(combos) for combos in no_data_reasons.values())
    failed_count = skipped - no_data_count
    Log.info(
        f"for_each({fn_name}) done in {elapsed:.1f}s: completed={completed}, "
        f"failed={failed_count}, no_data={no_data_count}, total={total}"
        f"{cancelled_suffix}",
        layer="scifor",
    )
    # Everything failed: the caller gets an EMPTY DataFrame, which is exactly
    # what "there was nothing to iterate" looks like from the outside. The two
    # have opposite causes and the same symptom, so this one is a WARNING —
    # found twice in one afternoon (2026-09-14), both times read as a discovery
    # or filter bug when in fact every call had raised the same TypeError.
    if total and completed == 0 and failed_count:
        Log.warn(
            f"for_each({fn_name}): every one of the {total} iteration(s) "
            f"failed, so the result is EMPTY — this is not an empty schema. "
            f"Reasons follow.",
            layer="scifor",
        )
    # One line per distinct failure reason, so the default (INFO) log always
    # answers "what failed and why" without per-iteration lines.
    for reason, combos in failure_reasons.items():
        shown = combos[:_SUMMARY_COMBOS_MAX]
        more = (
            f" (+{len(combos) - len(shown)} more)" if len(combos) > len(shown) else ""
        )
        label = "no data" if reason in no_data_reasons else "failed"
        Log.info(
            f'{label}: {len(combos)} × "{reason}" — {"; ".join(shown)}{more}',
            layer="scifor",
        )
    if _progress_fn is not None:
        _progress_fn(
            {
                "event": "summary",
                "current": total,  # keeps positional consumers (GUI) safe
                "total": total,
                "completed": completed,
                "failed": failed_count,
                "no_data": no_data_count,
                "skipped": skipped,  # legacy key: consumers that tally every event
                "cancelled": was_cancelled,
                "failure_reasons": failure_reasons,
            }
        )

    Log.debug(
        "building output DataFrame from %d result row(s)",
        len(collected_rows),
        layer="scifor",
    )
    return _results_to_output_dataframe(
        collected_rows, resolved_output_names, schema_col_dtypes, full_schema_keys
    )


def _call_fn(fn, kwargs, n_outputs):
    """Call fn with the right number of output captures."""
    return fn(**kwargs)


def _resolve_path_outputs(kwargs: dict, metadata: dict, column: "str | None") -> dict:
    """Return a copy of kwargs with any PathOutput resolved to a finished path.

    Substitutes the combo metadata and (inside for_columns) the current column.
    Non-PathOutput entries pass through untouched.
    """
    if not any(isinstance(v, PathOutput) for v in kwargs.values()):
        return kwargs
    return {
        name: (v.resolve(metadata, column) if isinstance(v, PathOutput) else v)
        for name, v in kwargs.items()
    }


def _resolve_path_inputs(kwargs: dict, metadata: dict, resolver=None) -> dict:
    """Return a copy of kwargs with any PathInput resolved to a path.

    Substitutes the combo metadata into the template. Non-PathInput entries
    pass through untouched. ``resolver(pathinput, metadata) -> path``
    overrides the default ``pathinput.load(**metadata)`` — used by DB
    wrappers that need schema-key-type-aware resolution.
    """
    if not any(isinstance(v, PathInput) for v in kwargs.values()):
        return kwargs
    resolve_fn = resolver or (lambda pi, m: pi.load(**m))
    return {
        name: (resolve_fn(v, metadata) if isinstance(v, PathInput) else v)
        for name, v in kwargs.items()
    }


class NoDataError(RuntimeError):
    """A per-combo DataFrame input had no matching rows after combo/where
    filtering. This is expected whenever the schema-key cross-product is
    sparser than the full grid (e.g. not every subject ran every trial), so
    it is reported separately from genuine ``fn`` failures in the end-of-run
    summary rather than as "failed"."""


class ForColumnsError(ValueError):
    """A structural error in for_columns reassembly (e.g. a colliding output
    column or a non-collapsible return). These are deterministic across combos
    and indicate a function/return-contract bug, so they propagate as hard
    errors rather than being swallowed as a per-combo ``[skip]``."""


class ColumnFunctionError(ValueError):
    """The for_columns function raised on one or more iterated columns.

    Rather than skipping the whole combo on the first column that fails, the
    column loop runs every column, collects each failure, and raises this once
    with the complete list. A per-column failure is almost always deterministic
    across combos (e.g. a non-numeric column the function can't process), so the
    full list is the actionable signal: it names exactly which columns to drop
    from the selection or fix upstream. The failing ``(column, message)`` pairs
    are available on the ``failures`` attribute."""

    def __init__(self, message: str, failures: "list[tuple[str, str]]"):
        super().__init__(message)
        self.failures = failures


# Separator joining a source column name to a per-column output key when a
# for_columns function returns multiple named values (dict / Series / 1-row
# DataFrame) instead of a single scalar.
FOR_COLUMNS_OUTPUT_SEP = "__"


def _run_column_iteration(
    fn,
    base_kwargs,
    iterate_dfs,
    iterate_columns,
    schema_keys,
    as_table_set,
    metadata,
    path_input_resolver=None,
):
    """Run fn once per column and reassemble into a single one-row DataFrame.

    For each column, every iterate input is sliced to that column and passed to
    fn; non-iterate inputs/constants in ``base_kwargs`` pass through unchanged.
    An iterate input named in ``as_table_set`` is sliced to a DataFrame holding
    all schema key columns plus that one column (mirroring the non-iterate
    ColumnSelection as_table behavior at ``_prepare_input``); otherwise it is
    sliced to a bare single-column numpy array (the default).

    The per-column return is expanded into output columns by
    ``_expand_column_result``: a scalar yields one column named after the source
    column; a dict / pandas Series / 1-row DataFrame yields one column per key,
    named ``"<col><sep><key>"``. Different source columns may return different
    numbers (and names) of outputs — the reassembled row is the ordered
    concatenation of every produced column, so a single for_each call supports
    an arbitrary, per-column-varying number of outputs.
    """
    import pandas as pd

    ordered: list[tuple[str, Any]] = []
    # Per-column failures, collected across the whole loop so we can report
    # every offending column at once instead of dying on the first one.
    failures: list[tuple[str, str]] = []
    # Constant inputs that are deferred ColName() markers resolve to the name of
    # the column currently being iterated (recomputed each pass below).
    deferred_colname_params = [
        name
        for name, v in base_kwargs.items()
        if isinstance(v, ColName) and v.is_deferred
    ]
    # PathOutput constants resolve per-column (current combo metadata + column).
    path_output_params = [
        name for name, v in base_kwargs.items() if isinstance(v, PathOutput)
    ]
    # PathInput constants have no per-column token (unlike PathOutput's
    # {ColName}) -- the same path applies to every column in this combo, so
    # resolve once up front rather than inside the loop.
    base_kwargs = _resolve_path_inputs(base_kwargs, metadata, resolver=path_input_resolver)
    for col in iterate_columns:
        call_kwargs = dict(base_kwargs)
        for name in deferred_colname_params:
            call_kwargs[name] = col
        for name in path_output_params:
            # PathOutput resolves to a finished path using this combo's metadata
            # and the current column ({ColName}).
            call_kwargs[name] = base_kwargs[name].resolve(metadata, col)
        for name, df in iterate_dfs.items():
            if name in as_table_set:
                # Keep real schema keys + the current column, but never surface
                # internal tracking columns (e.g. scidb's ``__rid_*`` record-id
                # discriminators, which DB wrappers add to the schema for per-combo
                # filtering). They've already done their filtering job upstream.
                keep = [
                    c for c in df.columns if c in schema_keys and not c.startswith("__")
                ] + [col]
                call_kwargs[name] = df[keep]
            else:
                call_kwargs[name] = df[col].values
        try:
            res = fn(**call_kwargs)
        except Exception as e:
            # Record and keep going so the raised error names every bad column.
            failures.append((col, f"{type(e).__name__}: {e}"))
            continue
        if isinstance(res, tuple):
            # for_columns is a single (reassembled) output; a tuple return is
            # collapsed to its first element. Return a dict/Series/1-row frame
            # to emit multiple named values per column.
            res = res[0]
        ordered.extend(_expand_column_result(col, res))

    if failures:
        fn_name = getattr(fn, "__name__", repr(fn))
        detail = "\n".join(f"  - {col}: {msg}" for col, msg in failures)
        message = (
            f"for_columns: {fn_name} raised on {len(failures)} of "
            f"{len(iterate_columns)} iterated column(s). These columns could not "
            f"be processed:\n{detail}\n"
            f"Hint: schema keys are already excluded; restrict the selection to "
            f"data columns (e.g. ColumnSelection(df, columns=[...])) or ensure "
            f"these columns are numeric."
        )
        raise ColumnFunctionError(message, failures)

    # Assemble a one-row DataFrame, preserving encounter order and rejecting
    # collisions (two produced names mapping to the same output column).
    data: dict[str, list] = {}
    for out_name, value in ordered:
        if out_name in data:
            raise ForColumnsError(
                f"for_columns produced a duplicate output column '{out_name}'. "
                f"Per-column output keys must be unique after prefixing with the "
                f"source column name (separator '{FOR_COLUMNS_OUTPUT_SEP}')."
            )
        data[out_name] = [value]
    return pd.DataFrame(data)


def _expand_column_result(col: str, res: Any) -> "list[tuple[str, Any]]":
    """Expand one source column's return into (output_name, value) pairs.

    - scalar (or any non-mapping / non-frame value) -> ``[(col, res)]``
    - dict / pandas Series -> one pair per item, named ``"<col><sep><key>"``
    - 1-row pandas DataFrame -> one pair per column, ``"<col><sep><column>"``

    A multi-row DataFrame is rejected: for_columns reassembles to a single row
    per combo, so each source column must collapse to scalar output value(s).
    """
    import pandas as pd

    sep = FOR_COLUMNS_OUTPUT_SEP
    if isinstance(res, dict):
        return [(f"{col}{sep}{k}", v) for k, v in res.items()]
    if isinstance(res, pd.Series):
        return [(f"{col}{sep}{k}", v) for k, v in res.items()]
    if isinstance(res, pd.DataFrame):
        if len(res) != 1:
            raise ForColumnsError(
                f"for_columns function returned a {len(res)}-row DataFrame for "
                f"column '{col}'; expected a single row (one value per output "
                f"key). Return a scalar, dict, pandas Series, or 1-row DataFrame."
            )
        return [(f"{col}{sep}{c}", res[c].iloc[0]) for c in res.columns]
    return [(col, res)]


def _describe_result(val) -> str:
    """Compact description of a function result value."""
    try:
        import pandas as pd

        if isinstance(val, pd.DataFrame):
            return f"DataFrame {val.shape[0]}x{val.shape[1]}"
    except ImportError:
        pass
    try:
        import numpy as np

        if isinstance(val, np.ndarray):
            return f"ndarray shape={val.shape}"
    except ImportError:
        pass
    if isinstance(val, dict):
        return f"dict ({len(val)} keys)"
    if isinstance(val, (list, tuple)):
        return f"{type(val).__name__} len={len(val)}"
    return type(val).__name__


# ---------------------------------------------------------------------------
# Input classification
# ---------------------------------------------------------------------------


def _is_data_input(var_spec: Any) -> bool:
    """Check if an input is a data input (DataFrame, Fixed, Merge, ColumnSelection)."""
    if _is_dataframe(var_spec):
        return True
    if isinstance(var_spec, (Fixed, Merge, ColumnSelection)):
        return True
    return False


def _is_dataframe(value: Any) -> bool:
    """Return True if value is a pandas DataFrame."""
    try:
        import pandas as pd

        return isinstance(value, pd.DataFrame)
    except ImportError:
        return False


def _input_is_empty(value: Any) -> bool:
    """Return True if a prepared per-combo input carries zero rows.

    Used only for logging — a DataFrame with no rows, or a sized container
    (array/list/Series) of length 0. Scalars and unsized values are never
    considered empty.
    """
    if _is_dataframe(value):
        return value.empty
    if value is None:
        return False
    try:
        return len(value) == 0
    except TypeError:
        return False


# ---------------------------------------------------------------------------
# ColName resolution
# ---------------------------------------------------------------------------


def _resolve_colnames(inputs: dict[str, Any], schema_keys: list[str]) -> dict[str, Any]:
    """Replace static ColName(df) wrappers with the resolved column name string.

    For each static ColName(df) in inputs:
    1. Get the inner DataFrame
    2. Compute data_cols = columns not in schema_keys
    3. If exactly 1 data column -> replace with the string name
    4. Otherwise -> raise ValueError

    Deferred (no-arg) ColName() markers are passed through unchanged — they are
    resolved per-column during for_columns iteration (_run_column_iteration).
    """
    resolved = {}
    for param_name, var_spec in inputs.items():
        if isinstance(var_spec, ColName) and var_spec.is_deferred:
            # No-arg ColName() — leave in place; resolved per-column during
            # for_columns iteration (see _run_column_iteration).
            resolved[param_name] = var_spec
        elif isinstance(var_spec, ColName):
            df = var_spec.data
            if not _is_dataframe(df):
                raise TypeError(
                    f"ColName({param_name}) expected a DataFrame, "
                    f"got {type(df).__name__}"
                )
            data_cols = [c for c in df.columns if c not in schema_keys]
            if len(data_cols) == 1:
                resolved[param_name] = data_cols[0]
            elif len(data_cols) == 0:
                raise ValueError(
                    f"ColName({param_name}): DataFrame has no data columns "
                    f"(all columns are schema keys). "
                    f"Columns: {list(df.columns)}, schema keys: {schema_keys}"
                )
            else:
                raise ValueError(
                    f"ColName({param_name}): DataFrame has {len(data_cols)} "
                    f"data columns ({data_cols}), expected exactly 1. "
                    f"Schema keys: {schema_keys}"
                )
        else:
            resolved[param_name] = var_spec
    return resolved


# ---------------------------------------------------------------------------
# DataFrame filtering
# ---------------------------------------------------------------------------


def _is_per_combo_df(df: "pd.DataFrame", schema_keys: list[str]) -> bool:
    """True if df has at least one column that is a schema key."""
    return bool(set(df.columns) & set(schema_keys))


def _accepted_param_names(fn) -> "set[str] | None":
    """Return the set of keyword names ``fn`` accepts, or None if it takes **kwargs.

    None means "inject anything" (the function has a ``**kwargs`` catch-all).
    Falls back to ``__scidb_params__`` (set by scidb/scilineage wrappers) when
    the signature can't be introspected.
    """
    import inspect

    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        params = getattr(fn, "__scidb_params__", None)
        return set(params) if params is not None else set()
    names = set()
    for p in sig.parameters.values():
        if p.kind == inspect.Parameter.VAR_KEYWORD:
            return None  # **kwargs — accepts any keyword
        if p.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            names.add(p.name)
    return names


def _numeric_extent(df: "pd.DataFrame") -> "tuple[float, float] | tuple[None, None]":
    """Return (min, max) over all numeric values in df, flattening array cells.

    Handles both scalar-valued cells and ndarray/list-valued cells (timeseries).
    Returns (None, None) when no finite numeric values are present.
    """
    import numpy as np

    lo = None
    hi = None
    for col in df.columns:
        for val in df[col].to_numpy():
            arr = (
                np.asarray(val, dtype="float64").ravel()
                if not np.isscalar(val)
                else np.asarray([val], dtype="float64")
            )
            arr = arr[np.isfinite(arr)]
            if arr.size == 0:
                continue
            cmn = float(arr.min())
            cmx = float(arr.max())
            lo = cmn if lo is None else min(lo, cmn)
            hi = cmx if hi is None else max(hi, cmx)
    if lo is None:
        return (None, None)
    return (lo, hi)


def _compute_shared_limits(
    share_limits: dict, data_inputs: dict, schema_keys: list[str]
) -> dict:
    """Compute per-group numeric extents for each input named in share_limits.

    Returns ``{input_name: (group_keys_present, {group_key_tuple: (min, max)})}``
    where ``group_keys_present`` is the subset of the requested held-fixed schema
    keys actually present in the input's DataFrame, and each group spans all rows
    sharing those key values (i.e. across every other iterated key).
    """
    import pandas as pd

    result: dict = {}
    for input_name, group_keys in share_limits.items():
        var_spec = data_inputs.get(input_name)
        if var_spec is None:
            continue
        df, _eff_meta, column_selection = _resolve_data_spec(var_spec, {})
        if not isinstance(df, pd.DataFrame):
            continue
        data_cols = [
            c
            for c in df.columns
            if c not in schema_keys and not str(c).startswith("__")
        ]
        if column_selection:
            data_cols = [c for c in data_cols if c in column_selection]
        if not data_cols:
            continue

        present_group_keys = [k for k in group_keys if k in df.columns]
        limits: dict = {}
        if present_group_keys:
            for gvals, gdf in df.groupby(present_group_keys, sort=False):
                key = gvals if isinstance(gvals, tuple) else (gvals,)
                gkey = tuple(str(v) for v in key)
                mn, mx = _numeric_extent(gdf[data_cols])
                if mn is not None:
                    limits[gkey] = (mn, mx)
        else:
            mn, mx = _numeric_extent(df[data_cols])
            if mn is not None:
                limits[()] = (mn, mx)
        result[input_name] = (present_group_keys, limits)
    return result


def _filter_df_for_combo(
    df: "pd.DataFrame", metadata: dict, schema_keys: list[str]
) -> "pd.DataFrame":
    """Filter df rows to match combo metadata for schema key columns present in df."""
    import pandas as pd

    mask = pd.Series([True] * len(df), index=df.index)
    for key in schema_keys:
        if key in df.columns and key in metadata:
            col_vals = df[key]
            meta_val = metadata[key]
            mask = mask & (col_vals == meta_val)
    return df[mask].reset_index(drop=True)


def _apply_where_filter(df: "pd.DataFrame", where) -> "pd.DataFrame":
    """Apply a scifor Col filter to a DataFrame."""
    if where is None:
        return df
    mask = where.apply(df)
    return df[mask].reset_index(drop=True)


def _extract_data(
    df: "pd.DataFrame",
    schema_keys: list[str],
    as_table: bool,
    mapping_cols: "list[str] | None" = None,
) -> Any:
    """Extract data from a filtered DataFrame.

    If as_table: return full DataFrame (real schema columns + data columns, but
    not internal ``__``-prefixed schema columns such as scidb's ``__rid_*``
    record-id discriminators).
    Otherwise: drop schema key columns; if 1 row + 1 data col -> extract scalar.

    ``mapping_cols`` (see ``for_each``'s ``_mapping_inputs``) says the rows are
    dict records spread one column per key. A single row is then rebuilt into
    that dict, which is the value the caller saved. Without it a dict-valued
    variable came back as a one-row frame, because the 1-row unwrap below only
    fires for a *single* data column and a dict has as many columns as keys.
    """
    if as_table:
        internal = [c for c in df.columns if c in schema_keys and c.startswith("__")]
        return df.drop(columns=internal) if internal else df

    if mapping_cols and len(df) == 1:
        present = [c for c in mapping_cols if c in df.columns]
        if present:
            Log.debug(
                f"_extract_data: rebuilding dict-valued record from "
                f"{len(present)} column(s)",
                layer="scifor",
            )
            return {c: df[c].iloc[0] for c in present}
        # Fall through: the selection/rename upstream left none of the declared
        # columns, so treat it as an ordinary frame rather than an empty dict.

    data_cols = [c for c in df.columns if c not in schema_keys]
    if len(df) == 1 and len(data_cols) == 1:
        return df[data_cols[0]].iloc[0]
    if len(data_cols) == 1:
        # Single data column, multiple rows → 2D numpy column vector
        return df[data_cols].reset_index(drop=True).values
    if data_cols and set(data_cols) != set(df.columns):
        return df[data_cols].reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Input preparation per combo
# ---------------------------------------------------------------------------


def _prepare_input(
    var_spec: Any,
    metadata: dict,
    schema_keys: list[str],
    as_table: bool,
    where=None,
    mapping_cols: "list[str] | None" = None,
) -> Any:
    """Prepare a single data input for the current combo.

    ``mapping_cols`` marks the input as dict-valued (see ``for_each``'s
    ``_mapping_inputs``); it only reaches ``_extract_data``, so a column
    selection or ``as_table`` still wins.
    """
    if isinstance(var_spec, Merge):
        return _prepare_merge(var_spec, metadata, schema_keys, where)

    if isinstance(var_spec, Fixed) and isinstance(var_spec.data, Merge):
        raise TypeError(
            "Fixed cannot wrap a Merge. Use Fixed on individual "
            "constituents inside the Merge instead: "
            "Merge(Fixed(df1, ...), df2)"
        )

    # Resolve the raw DataFrame and effective metadata
    df, effective_metadata, column_selection = _resolve_data_spec(var_spec, metadata)

    excl = _excluded_columns(var_spec)

    if not _is_per_combo_df(df, schema_keys):
        # Constant DataFrame — pass unchanged every iteration
        if column_selection is not None:
            # An empty selection means "all data columns".
            cols = column_selection or _all_data_columns(df, schema_keys)
            cols = _apply_exclusions(cols, excl)
            return _apply_column_selection(df, cols)
        return df

    filtered = _filter_df_for_combo(df, effective_metadata, schema_keys)
    filtered = _apply_where_filter(filtered, where)

    # No matching rows -> skip this combo (unless as_table, where an empty
    # table is valid output).
    if not as_table and len(filtered) == 0:
        raise NoDataError("No data for this combo after filtering.")

    if column_selection is not None:
        # An empty selection means "all data columns".
        cols = column_selection or _all_data_columns(filtered, schema_keys)
        cols = _apply_exclusions(cols, excl)
        if as_table:
            # Keep real schema columns alongside selected data columns, but never
            # surface internal tracking columns (e.g. scidb's ``__rid_*`` record-id
            # discriminators added to the schema for per-combo filtering).
            keep = [
                c
                for c in filtered.columns
                if c in schema_keys and not c.startswith("__")
            ] + cols
            return filtered[keep]
        return _apply_column_selection(filtered, cols)

    return _extract_data(filtered, schema_keys, as_table, mapping_cols=mapping_cols)


def _resolve_data_spec(
    var_spec: Any, metadata: dict
) -> tuple["pd.DataFrame", dict, list[str] | None]:
    """Resolve a var_spec into (DataFrame, effective_metadata, column_selection)."""
    column_selection = None

    if isinstance(var_spec, Fixed):
        effective_metadata = {**metadata, **var_spec.fixed_metadata}
        inner = var_spec.data
        if isinstance(inner, ColumnSelection):
            column_selection = inner.columns
            df = inner.data
        else:
            df = inner
    elif isinstance(var_spec, ColumnSelection):
        effective_metadata = metadata
        column_selection = var_spec.columns
        df = var_spec.data
    else:
        # Plain DataFrame
        effective_metadata = metadata
        df = var_spec

    return df, effective_metadata, column_selection


def _unwrap_column_selection(var_spec: Any) -> Any:
    """Return the ColumnSelection inside a spec (bare or Fixed-wrapped), else the spec."""
    if isinstance(var_spec, Fixed) and isinstance(var_spec.data, ColumnSelection):
        return var_spec.data
    return var_spec


def _excluded_columns(var_spec: Any) -> list[str]:
    """Return the ColumnSelection's ``excl_columns`` (bare or Fixed-wrapped)."""
    cs = _unwrap_column_selection(var_spec)
    if isinstance(cs, ColumnSelection):
        return cs.excl_columns
    return []


def _apply_exclusions(cols: list[str], excl: list[str]) -> list[str]:
    """Drop ``excl`` names from ``cols``, preserving order. No-op if ``excl`` empty."""
    if not excl:
        return cols
    excl_set = set(excl)
    return [c for c in cols if c not in excl_set]


def _all_data_columns(df: "pd.DataFrame", schema_keys: list[str]) -> list[str]:
    """Return a DataFrame's data columns: everything that is not a schema key
    or an internal ``__*`` column. Used to expand an empty (all-columns)
    ColumnSelection to a concrete list."""
    return [
        c for c in df.columns if c not in schema_keys and not str(c).startswith("__")
    ]


def _resolve_iterate_columns(var_spec: Any, schema_keys: list[str]) -> list[str]:
    """Resolve an iterate-mode ColumnSelection's column list.

    An explicit list is returned as-is; an empty list (the all-columns
    sentinel) is expanded to every data column of the underlying DataFrame.
    Any ``excl_columns`` are removed from whichever list is produced.
    """
    cs = _unwrap_column_selection(var_spec)
    excl = _excluded_columns(var_spec)
    cols = list(cs.columns)
    if cols:
        return _apply_exclusions(cols, excl)
    df = cs.data if isinstance(cs, ColumnSelection) else None
    if df is None or not _is_dataframe(df):
        raise ValueError(
            "for_columns(): cannot resolve all columns — the input is not a "
            "DataFrame-backed ColumnSelection. Pass an explicit column list."
        )
    resolved = _apply_exclusions(_all_data_columns(df, schema_keys), excl)
    if not resolved:
        raise ValueError(
            f"for_columns(): no data columns found to iterate over "
            f"(columns were {list(df.columns)}, schema keys {schema_keys}, "
            f"excluded {excl})."
        )
    return resolved


def _prepare_iterate_df(
    var_spec: Any, metadata: dict, schema_keys: list[str], where=None
) -> "pd.DataFrame":
    """Prepare the per-combo DataFrame for an iterate-mode ColumnSelection.

    Returns the combo-filtered DataFrame retaining the iterate columns so the
    caller can slice one column at a time. Unlike ``_prepare_input`` it does
    not collapse to a single column.
    """
    df, effective_metadata, _column_selection = _resolve_data_spec(var_spec, metadata)
    if not _is_per_combo_df(df, schema_keys):
        return df
    filtered = _filter_df_for_combo(df, effective_metadata, schema_keys)
    filtered = _apply_where_filter(filtered, where)
    if len(filtered) == 0:
        raise NoDataError("No data for this combo after filtering.")
    return filtered


def _apply_column_selection(df: "pd.DataFrame", columns: list[str]) -> Any:
    """Extract selected columns from a DataFrame."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(
            f"Column(s) {missing} not found. Available columns: {list(df.columns)}"
        )
    if len(columns) == 1:
        return df[columns[0]].values
    return df[columns]


# ---------------------------------------------------------------------------
# Merge handling
# ---------------------------------------------------------------------------


def _prepare_merge(
    merge_spec: Merge,
    metadata: dict,
    schema_keys: list[str],
    where=None,
) -> "pd.DataFrame":
    """Filter each constituent of a Merge and combine into a single DataFrame."""
    import pandas as pd

    parts = []

    for i, spec in enumerate(merge_spec.tables):
        label = f"merge[{i}]"

        df, effective_metadata, column_selection = _resolve_data_spec(spec, metadata)

        if _is_per_combo_df(df, schema_keys):
            filtered = _filter_df_for_combo(df, effective_metadata, schema_keys)
            filtered = _apply_where_filter(filtered, where)
            # Drop schema key columns for merge
            data_cols = [c for c in filtered.columns if c not in schema_keys]
            if data_cols and set(data_cols) != set(filtered.columns):
                part_df = filtered[data_cols].reset_index(drop=True)
            else:
                part_df = filtered.reset_index(drop=True)
        else:
            part_df = df.reset_index(drop=True)

        if column_selection is not None:
            missing = [c for c in column_selection if c not in part_df.columns]
            if missing:
                raise KeyError(
                    f"Column(s) {missing} not found in {label}. "
                    f"Available: {list(part_df.columns)}"
                )
            if len(column_selection) == 1:
                part_df = pd.DataFrame(
                    {column_selection[0]: part_df[column_selection[0]]}
                )
            else:
                part_df = part_df[column_selection]

        parts.append(part_df)

    return _merge_parts(parts)


def _merge_parts(parts: list["pd.DataFrame"]) -> "pd.DataFrame":
    """Merge multiple DataFrames column-wise."""
    import pandas as pd

    if not parts:
        raise ValueError("Merge has no constituents.")

    # Check for column name conflicts
    seen_columns: set[str] = set()
    for df in parts:
        for col in df.columns:
            if col in seen_columns:
                raise KeyError(
                    f"Column name conflict in Merge: "
                    f"column '{col}' appears in multiple constituents."
                )
            seen_columns.add(col)

    # Check row count compatibility
    row_counts = [(len(df), df) for df in parts if len(df) > 1]
    if row_counts:
        unique_counts = {n for n, _ in row_counts}
        if len(unique_counts) > 1:
            detail = ", ".join(str(n) for n, _ in row_counts)
            raise ValueError(
                f"Cannot merge constituents with different row counts: {detail}."
            )
        target_len = row_counts[0][0]
    else:
        target_len = 1

    expanded = []
    for df in parts:
        if len(df) == 1 and target_len > 1:
            df = pd.concat([df] * target_len, ignore_index=True)
        expanded.append(df)

    return pd.concat(expanded, axis=1)


# ---------------------------------------------------------------------------
# Empty-list resolution from DataFrame inputs
# ---------------------------------------------------------------------------


def _distinct_values_from_inputs(inputs: dict, key: str) -> list:
    """Find distinct values for `key` by scanning DataFrame inputs.

    Non-raising: a PathInput may still supply the key via filesystem
    discovery (handled by the caller), which decides whether an
    unresolved key warns, is dropped, or errors.
    """
    all_values = set()
    for _param_name, var_spec in inputs.items():
        df = _get_raw_df(var_spec)
        if df is not None and key in df.columns:
            all_values.update(df[key].dropna().unique().tolist())
    if not all_values:
        return []
    try:
        return sorted(all_values)
    except TypeError:
        return list(all_values)


def _find_pathinput(inputs: dict) -> "PathInput | None":
    """Return the first bare PathInput in inputs, or None.

    Bare only — ``Fixed(PathInput(...))`` is a scidb-only pattern (scifor's
    ``Fixed`` wraps DataFrames, not loadable specs).
    """
    for var_spec in inputs.values():
        if isinstance(var_spec, PathInput):
            return var_spec
    return None


def resolve_pathinput_discovery(
    pi: "PathInput | None",
    metadata_iterables: dict,
    user_explicit_keys: "set | None" = None,
    log=None,
    condense_numeric: bool = False,
    unsupplyable_keys: "set | None" = None,
) -> "tuple[dict, list[dict] | None]":
    """Fill empty metadata iterables from PathInput filesystem discovery,
    then drop any key a fully static PathInput (no ``{key}`` placeholders)
    can never supply, instead of leaving it as an empty list that would
    zero out the Cartesian product.

    Shared by scifor's own standalone empty-list resolution and scidb's
    DB-backed ``for_each`` (which calls this after its own DB-based
    resolution leaves some keys still empty), so both layers make the
    same discovery/leniency decision.

    Args:
        pi: The PathInput to discover against, or None if no PathInput is
            present (metadata_iterables is returned unchanged, combos=None).
        metadata_iterables: Mutable mapping of key -> list of values.
        user_explicit_keys: Keys the caller passed with explicit non-empty
            values (not delegated to resolution).
        log: Optional ``log(msg)`` callback.
        condense_numeric: Forwarded to ``PathInput.apply_discovery`` — see
            there. Defaults to False so scidb's declared-only
            ``schema_key_types`` contract is unaffected; the standalone
            scifor call site opts in explicitly.
        unsupplyable_keys: Keys that provably have NO source — neither the
            caller, nor the DB, nor this template can fill them — and which
            the caller is therefore going to drop from the iteration anyway.
            They are excluded from the "every iterated key is a placeholder"
            test below, so a key that is about to disappear cannot force the
            Cartesian-product fallback on its way out. Membership must be
            decidable WITHOUT walking the disk (the caller knows what its own
            resolution failed to fill; ``pi.placeholder_keys()`` says what this
            template could ever supply), which is why this is an input rather
            than something computed here after discovery.

    Returns:
        ``(metadata_iterables, discovered_combos | None)``.
    """
    if pi is None:
        return metadata_iterables, None
    metadata_iterables, discovered_combos = pi.apply_discovery(
        metadata_iterables, user_explicit_keys, log=log, condense_numeric=condense_numeric
    )
    placeholder_keys = set(pi.placeholder_keys())
    if any(isinstance(v, list) and len(v) == 0 for v in metadata_iterables.values()):
        pi_is_static = not placeholder_keys
        if pi_is_static:
            for key in list(metadata_iterables.keys()):
                v = metadata_iterables[key]
                if isinstance(v, list) and len(v) == 0:
                    if log is not None:
                        log(
                            f"'{key}' has no source and PathInput "
                            f"{pi.path_template!r} has no template placeholders; "
                            f"ignoring '{key}' (treating it as if it were never "
                            f"requested)"
                        )
                    del metadata_iterables[key]

    # Discovered combos only drive iteration directly when every iterated key
    # is one of this PathInput's own template placeholders -- otherwise a
    # Cartesian product with keys resolved from elsewhere (tables/DB) is
    # still required, and using the bare discovered combos would silently
    # drop that other dimension's iteration. (metadata_iterables has already
    # been filled per-key above, so the Cartesian-product fallback still
    # picks up the disk-discovered values either way.)
    #
    # ``unsupplyable_keys`` is subtracted first. A key nothing can fill is not a
    # real iteration dimension -- the caller drops it moments later -- but while
    # it is still in the dict it used to fail this subset test and force the
    # Cartesian product. That is how a 5-key schema whose template supplies 4
    # turned 419 real files into 952 combos, 533 of which could only fail on a
    # missing file (2026-09-13; see
    # .claude/matlab-run-timing-and-phantom-combos-plan.md). The set is decided
    # before discovery runs, so a key on its way out no longer votes.
    _iterated_keys = set(metadata_iterables.keys()) - set(unsupplyable_keys or ())
    if discovered_combos is not None and not _iterated_keys <= placeholder_keys:
        if log is not None:
            log(
                f"discovery combos dropped: iterated keys "
                f"{sorted(_iterated_keys - placeholder_keys)} are not template "
                f"placeholders of {pi.path_template!r} — Cartesian product of "
                f"iterables will drive combos"
            )
        discovered_combos = None
    return metadata_iterables, discovered_combos


def _get_raw_df(var_spec: Any) -> "pd.DataFrame | None":
    """Extract the DataFrame from a var_spec, if it contains one."""
    if _is_dataframe(var_spec):
        return var_spec
    if isinstance(var_spec, Fixed) and _is_dataframe(var_spec.data):
        return var_spec.data
    if isinstance(var_spec, ColumnSelection) and _is_dataframe(var_spec.data):
        return var_spec.data
    return None


def _key_has_source(inputs: dict, key: str, pi: "PathInput | None") -> bool:
    """True if any DataFrame column or PathInput placeholder provides key."""
    for var_spec in inputs.values():
        df = _get_raw_df(var_spec)
        if df is not None and key in df.columns:
            return True
    if pi is not None and key in pi.placeholder_keys():
        return True
    return False


def _capture_schema_column_dtypes(inputs: dict, keys: list) -> dict:
    """Record each schema-key input column's pandas dtype.

    Scans the raw DataFrame inputs and records, per key, the column dtype
    (a CategoricalDtype carries its categories and orderedness), so output
    metadata columns can round-trip as EXACTLY the input column dtype. A key
    whose input DataFrames disagree on dtype is recorded as None and left at
    the natural output dtype (warned at restore time).
    """
    dtypes: dict = {}
    conflicts: set = set()
    for param_name, var_spec in inputs.items():
        df = _get_raw_df(var_spec)
        if df is None:
            continue
        for key in keys:
            if key not in df.columns:
                continue
            dt = df[key].dtype
            if key not in dtypes:
                dtypes[key] = dt
                Log.debug(
                    "schema key '%s' (input '%s'): input column dtype %s",
                    key,
                    param_name,
                    dt,
                    layer="scifor",
                )
            elif dtypes[key] != dt:
                conflicts.add(key)
    for key in conflicts:
        dtypes[key] = None
    return dtypes


def _restore_schema_column_dtypes(
    result: "pd.DataFrame", col_dtypes: "dict | None"
) -> "pd.DataFrame":
    """Cast output metadata columns back to the captured input dtypes.

    Keys without a captured dtype (explicit iterables with no DataFrame
    column) keep the iterable's natural dtype. A cast that cannot be
    performed losslessly leaves the column unchanged and warns, so identity
    errors are visible rather than silent.
    """
    import pandas as pd

    if not col_dtypes or result.empty:
        return result
    dup_labels = list(result.columns[result.columns.duplicated()].unique())
    if dup_labels:
        # Typically a function returning its input DataFrame with the
        # metadata columns still inside, so combo metadata gets appended a
        # second time under the same label.
        Log.warn(
            "output DataFrame has DUPLICATE column label(s) %s — the "
            "function's returned DataFrame likely already contains "
            "metadata column(s) that for_each appends per combo; dtype "
            "restore skips these columns",
            dup_labels,
            layer="scifor",
        )
    for key, dtype in col_dtypes.items():
        if key not in result.columns:
            continue
        col = result[key]
        if isinstance(col, pd.DataFrame):  # duplicated label — warned above
            continue
        if dtype is None:
            Log.warn(
                "schema key '%s': input DataFrames disagree on column "
                "dtype — leaving the output column as %s",
                key,
                col.dtype,
                layer="scifor",
            )
            continue
        if col.dtype == dtype:
            continue
        converted = None
        try:
            converted = col.astype(dtype)
            same = (converted == col) | (converted.isna() & col.isna())
            lossless = bool(same.all())
        except (ValueError, TypeError):
            lossless = False
        if not lossless and isinstance(dtype, pd.CategoricalDtype):
            # Values outside the captured category set (e.g. distribute
            # indices): fall back to a plain categorical of the values,
            # mirroring the MATLAB categorical(col) fallback.
            converted = col.astype("category")
            lossless = True
        if not lossless:
            Log.warn(
                "schema key '%s': cannot restore input column dtype %s "
                "losslessly — leaving as %s",
                key,
                dtype,
                col.dtype,
                layer="scifor",
            )
            continue
        result[key] = converted
        Log.debug(
            "schema key '%s': output column restored to input dtype %s",
            key,
            dtype,
            layer="scifor",
        )
    return result


# ---------------------------------------------------------------------------
# Result collection
# ---------------------------------------------------------------------------


def _spread_decision(
    collected_rows: list[tuple[dict, tuple]],
    schema_keys: "list | tuple",
) -> tuple[bool, list[str], list[str], int]:
    """Should a DataFrame return value's ROWS become separate records?

    Returns ``(spread, discriminating_keys, pinned_collisions, max_rows)``.

    **Rows spread unless spreading would silently multiply records at one
    address.** A record's address is its schema keys, so rows spread when:

    1. the DataFrame carries a schema key the combination does NOT already
       pin — each row then addresses its own location, and scidb files it
       there (it reads every non-``__`` column of the row as that record's
       metadata). This is what the spread is FOR: a function iterating
       ``subject`` alone returning one row per ``session``; **or**
    2. there is at most one row — spreading cannot multiply anything, so the
       wide row simply becomes the result table's columns. This is the
       ``for_columns`` shape (one reassembled ``1 x N`` row per combo) and
       every ``distribute`` piece (``df.iloc[[i]]``), both of which depend on
       that spread and produce exactly one record per combo either way.

    Everything else — a MULTI-row table with nothing to distinguish where its
    rows go — is one record per combination. That is the only case whose
    behavior changes, and it is the bug: one 322-row CSV became 322 records
    at a single ``(subject, session)``, distinguishable only by their
    contents.

    ``distribute=True`` needs no clause here: it splits upstream of result
    collection and stamps each piece with its own ``distribute_key``, so the
    pieces arrive as pinned single rows and take condition 2.
    """
    real_keys = [
        k
        for k in (schema_keys or [])
        if "__rid_" not in str(k) and "__vsig_" not in str(k)
    ]
    discriminating: set[str] = set()
    collisions: set[str] = set()
    combos_with: int = 0
    max_rows = 0
    for metadata, result_tuple in collected_rows:
        pinned = set(metadata.keys())
        row_disc: set[str] = set()
        for value in result_tuple:
            cols = {str(c) for c in value.columns}
            max_rows = max(max_rows, len(value))
            row_disc |= {k for k in real_keys if k in cols and k not in pinned}
            collisions |= {k for k in real_keys if k in cols and k in pinned}
        if row_disc:
            combos_with += 1
        discriminating |= row_disc

    if discriminating and combos_with != len(collected_rows):
        # Some combos supply the finer address and others don't, so the same
        # output would be filed at two different granularities. Spread (the
        # rows that DO carry the key must reach their own locations) and say
        # so — this is an authoring bug in the function, not a config choice.
        Log.warn(
            "inconsistent output addressing: %d of %d combination(s) return "
            "schema key(s) %s, the rest do not — the records will not all be "
            "filed at the same granularity",
            combos_with,
            len(collected_rows),
            sorted(discriminating),
            layer="scifor",
        )
    spread = bool(discriminating) or max_rows <= 1
    return spread, sorted(discriminating), sorted(collisions), max_rows


def _results_to_output_dataframe(
    collected_rows: list[tuple[dict, tuple]],
    output_names: list[str],
    col_dtypes: "dict | None" = None,
    schema_keys: "list | tuple" = (),
) -> "pd.DataFrame":
    """Build a combined DataFrame from all for_each results."""
    import pandas as pd

    if not collected_rows:
        return pd.DataFrame()

    # Check if all outputs are DataFrames (candidates for row spreading)
    all_dataframes = all(
        isinstance(value, pd.DataFrame)
        for _, result_tuple in collected_rows
        for value in result_tuple
    )

    spread = False
    if all_dataframes:
        spread, disc_keys, collisions, max_rows = _spread_decision(
            collected_rows, schema_keys
        )
        _out = ", ".join(str(n) for n in output_names) or "output"
        if collisions:
            Log.warn(
                "output %s returns schema key column(s) %s that the combination "
                "already pins — the metadata and data columns collide and the "
                "data column silently wins. Rename or drop them.",
                _out,
                collisions,
                layer="scifor",
            )
        # max_rows is the LARGEST row count across the combinations, which each
        # returned their own DataFrame — quoting it bare read as though every
        # combination returned that many rows (examples/vo2max/scidb.log
        # reported "372-row DataFrame" for combinations of 322/372/304/366).
        if spread and disc_keys:
            Log.info(
                "output %s: %d DataFrame(s), up to %d row(s) each, discriminated "
                "by unpinned schema key(s) %s — spreading rows into separate "
                "records",
                _out,
                len(collected_rows),
                max_rows,
                disc_keys,
                layer="scifor",
            )
        elif not spread:
            Log.info(
                "output %s: %d DataFrame(s), up to %d row(s) each, carry no "
                "unpinned schema-key column, so every row shares one address — "
                "saving each whole table as ONE record per combination. To file "
                "rows separately they need a finer address: return a schema-key "
                "column, or pass distribute=True to spread them one level below "
                "the deepest iterated key.",
                _out,
                len(collected_rows),
                max_rows,
                layer="scifor",
            )

    if spread:
        parts = []
        for metadata, result_tuple in collected_rows:
            combined_data = pd.concat(
                [df.reset_index(drop=True) for df in result_tuple], axis=1
            )
            nr = len(combined_data)
            meta_df = pd.DataFrame({k: [v] * nr for k, v in metadata.items()})
            parts.append(
                pd.concat([meta_df.reset_index(drop=True), combined_data], axis=1)
            )
        result = pd.concat(parts, ignore_index=True)
        Log.debug(
            "collect_results (spread mode): DataFrame with %d row(s), %d column(s)",
            len(result),
            len(result.columns),
            layer="scifor",
        )
    else:
        rows = []
        for metadata, result_tuple in collected_rows:
            row = dict(metadata)
            for name, value in zip(output_names, result_tuple, strict=False):
                row[name] = value
            rows.append(row)
        result = pd.DataFrame(rows)
        Log.debug(
            "collect_results (scalar mode): DataFrame with %d row(s), %d column(s)",
            len(result),
            len(result.columns),
            layer="scifor",
        )

    # Round-trip metadata column dtypes: cast each metadata column back to
    # the exact dtype of the input column it was resolved from.
    return _restore_schema_column_dtypes(result, col_dtypes)


# ---------------------------------------------------------------------------
# Distribute
# ---------------------------------------------------------------------------


def _split_for_distribute(data: Any) -> list[Any]:
    """Split data into elements for distribute-style expansion."""
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            return [data.iloc[[i]] for i in range(len(data))]
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(data, np.ndarray):
            if data.ndim == 1:
                return [data[i] for i in range(len(data))]
            elif data.ndim == 2:
                return [data[i, :] for i in range(data.shape[0])]
            else:
                raise TypeError(
                    f"distribute does not support numpy arrays with {data.ndim} dimensions. "
                    f"Only 1D (split by element) and 2D (split by row) are supported."
                )
    except ImportError:
        pass

    if isinstance(data, list):
        return list(data)

    raise TypeError(
        f"distribute does not support type {type(data).__name__}. "
        f"Supported types: numpy 1D/2D array, list, pandas DataFrame."
    )


# ---------------------------------------------------------------------------
# Display / dry-run
# ---------------------------------------------------------------------------


def _format_inputs(inputs: dict[str, Any]) -> str:
    """Format inputs dict for display."""
    parts = []
    for name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            parts.append(f"{name}: {var_spec.__name__}")
        elif isinstance(var_spec, Fixed):
            fixed_str = ", ".join(
                f"{k}={v}" for k, v in var_spec.fixed_metadata.items()
            )
            inner = var_spec.data
            if isinstance(inner, ColumnSelection):
                inner_name = inner.__name__
            elif _is_dataframe(inner):
                inner_name = f"DataFrame{list(inner.columns)}"
            else:
                inner_name = repr(inner)
            parts.append(f"{name}: Fixed({inner_name}, {fixed_str})")
        elif isinstance(var_spec, ColumnSelection):
            parts.append(f"{name}: {var_spec.__name__}")
        elif _is_dataframe(var_spec):
            parts.append(f"{name}: DataFrame{list(var_spec.columns)}")
        elif _is_data_input(var_spec):
            parts.append(f"{name}: {repr(var_spec)}")
        else:
            parts.append(f"{name}: {var_spec!r}")
    return "{" + ", ".join(parts) + "}"


def _print_dry_run_iteration(
    inputs: dict[str, Any],
    metadata: dict[str, Any],
    constant_inputs: dict[str, Any],
    distribute: str | None = None,
) -> None:
    """Print what would happen for one iteration in dry-run mode."""
    metadata_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
    print(f"[dry-run] {metadata_str}:")

    for param_name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            print(f"  merge {param_name}:")
            for i, sub_spec in enumerate(var_spec.tables):
                _print_constituent_filter(sub_spec, metadata, i)
        elif isinstance(var_spec, Fixed):
            filter_metadata = {**metadata, **var_spec.fixed_metadata}
            inner = var_spec.data
            if isinstance(inner, ColumnSelection):
                col_str = ", ".join(inner.columns)
                print(
                    f"  filter {param_name} with {filter_metadata} -> columns: [{col_str}]"
                )
            elif _is_dataframe(inner):
                print(f"  filter {param_name} = DataFrame with {filter_metadata}")
            else:
                print(f"  filter {param_name} with {filter_metadata}")
        elif isinstance(var_spec, ColumnSelection):
            col_str = ", ".join(var_spec.columns)
            print(f"  filter {param_name} with {metadata} -> columns: [{col_str}]")
        elif _is_dataframe(var_spec):
            print(f"  filter {param_name} = DataFrame with {metadata}")
        elif _is_data_input(var_spec):
            print(f"  filter {param_name} with {metadata}")
        else:
            print(f"  constant {param_name} = {var_spec!r}")

    if distribute is not None:
        print(f"  distribute by '{distribute}' (1-based indexing)")


def _print_constituent_filter(spec: Any, metadata: dict[str, Any], index: int) -> None:
    """Print a single Merge constituent's filter line for dry-run display."""
    if isinstance(spec, Fixed):
        filter_metadata = {**metadata, **spec.fixed_metadata}
        inner = spec.data
        if isinstance(inner, ColumnSelection):
            col_str = ", ".join(inner.columns)
            print(
                f"    [{index}] filter with {filter_metadata} -> columns: [{col_str}]"
            )
        else:
            print(f"    [{index}] filter with {filter_metadata}")
    elif isinstance(spec, ColumnSelection):
        col_str = ", ".join(spec.columns)
        print(f"    [{index}] filter with {metadata} -> columns: [{col_str}]")
    else:
        print(f"    [{index}] filter with {metadata}")
