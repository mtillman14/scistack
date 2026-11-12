"""DB-backed for_each wrapper — loads inputs, delegates loop to scifor, saves outputs.

This module holds the entry point (``for_each``), the execute phase, introspect
and the skip_computed hook. The rest lives in sibling modules, each importing
only from the ones listed before it (no cycles):

- ``foreach_common``: shared state types, per-combo resolution, small helpers.
- ``foreach_stamping``: endpoint artifact provenance stamping, column iteration.
- ``foreach_load``: loading inputs and converting them for the function.
- ``foreach_save``: saving results (the save plan, then applying it).
- ``foreach_prepare``: the prepare/save seam shared with the MATLAB bridge.

Every moved name is imported back here, so ``from scidb.run.foreach import X``
keeps working. To monkeypatch a function, patch it in the module whose code
CALLS it.
"""

import hashlib
import json
import logging
import math
import os
import re
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from itertools import product
from pathlib import Path as _Path
from typing import TYPE_CHECKING, Any

from scicanonicalhash import compute_function_hash
from scifor.foreach import resolve_pathinput_discovery as _scifor_resolve_pathinput_discovery
from scifor.pathinput import PathInput
from scifor.column_selection import column_value as _column_value

if TYPE_CHECKING:
    import pandas as pd

    from scidb.store.database import DatabaseManager

import scifor as _scifor
from scifor import ColName, ColumnSelection, EachOf, Fixed, LoopHooks, Merge, require_alternatives
from scifor.options import reject_private_options, reject_removed_options
from scifor import for_each as _scifor_for_each
from scifor import UNSET, stopped_run_of, validate_on_error
from scifor.failures import journaled
from scifor.foreach import bind_path_inputs, spread_output_frame
from scifor.project.root import project_root as current_project_root
from scifor import output_column_name as _output_column_name
from scifor.outputs import is_no_output as _is_no_output

from scidb.run import glue as _glue
from scidb.model.across_variants import AcrossVariants
from scidb.run.bindings import (
    COMBO_KEY,
    EMPTY_SIGNATURE,
    RECORD_ID_COLUMN,
    InputBinding,
    InputKind,
    RecordPool,
    RunBindings,
    Selection,
    VariantGroup,
    merge_branch_params,
    signature_conflicts_with,
    variant_signature,
)
from scidb.model.exceptions import AmbiguousParamError, DatabaseNotConfiguredError
from scidb.store.database import database_or_none, dataset_schema_keys_of
from scidb.model.input_spec import find_pathinput, is_loadable, spec_name, type_name, variable_type
from scidb.project.entities import declared_spelling
from scidb.model.parameter import check_path_input_names, declared_input_names, path_input_specs_of
from scidb.model.roles import endpoint_kind as _roles_endpoint_kind
from scifor import schema_types as _scifor_schema_types

from scidb.run.selection_axis import SELECTION_PARAM, selection_axes_of
from scidb.store.schema_values import schema_str, scidb_key_type_errors
from scidb.model.variant import match_bare_name
from scidb.model.filters import Filter
from scidb.run.foreach_call import ForEachCall, RunContext, RunSelection
from scidb.run.foreach_config import ForEachConfig, RunOptions
from scidb.model.log import Log
from scifor.project.names import function_name as _function_name
from scidb.run.per_combo import PerComboLoader, PerComboLoaderMerge
from scidb.run.pipeline import Pipeline as _Pipeline
from scidb.run.pipeline import Step, active_pipeline
from scidb.provenance.save import GraphRecord as _GraphRecord
from scidb.model.variant import Variant

# Moved into sibling modules (grug follow-up); imported back for this module
# and for code that imports these names from here.
from scidb.run.foreach_common import (  # noqa: F401 — re-exported
    _DryRunMerge,
    _ForEachState,
    _PreresolvedFilter,
    _active_database,
    _apply_per_combo_col_selection,
    _build_run_bindings,
    _capture_function_source,
    _describe_save_data,
    _get_schema_keys,
    _has_pathinput,
    _load_pathinput_checked,
    _merge_constituent_variable_filter,
    _output_type_label,
    _propagate_schema,
    _report_no_output_skips,
    _resolve_per_combo_loader,
    _resolve_per_combo_merge,
    _save_seam_error,
    _to_dataframe,
    pathinput_key_type_offender,
    pathinput_numeric_eligible,
)
from scidb.run.foreach_stamping import (  # noqa: F401 — re-exported
    _build_stamp_blob,
    _endpoint_artifact_path,
    _iterate_column_selection,
    _load_var_type_as_spread,
    _resolve_all_columns,
    _resolve_for_columns,
    _stamp_db_name,
    _stamp_draft_endpoint_artifacts,
    _stamp_inputs_from_edges,
)
from scidb.run.foreach_load import (  # noqa: F401 — re-exported
    _check_merge_filter_coverage,
    _compute_fixed_input_rids,
    _compute_merge_effective_ids,
    _convert_inputs,
    _convert_inputs_for_display,
    _drop_unpopulated_schema_columns,
    _drop_unpopulated_schema_columns_in_place,
    _endpoint_policy,
    _get_loadable_class_from_spec,
    _get_schema_level_idx,
    _jsonify_stat,
    _load_input,
    _log_loaded_input,
    _make_plot_wrapper,
    _make_stat_wrapper,
    _merge_needs_per_combo,
    _resolve_colname_from_db,
    _resolve_mapping_inputs,
    normalize_stat_payload,
)
from scidb.run.foreach_save import (  # noqa: F401 — re-exported
    SaveItem,
    SavePlan,
    SaveReport,
    _collect_row_items,
    _item_metadata,
    _log_input_provenance_sources,
    _log_pinned_outputs,
    _output_class,
    _pooled_params_of,
    _record_provenance,
    _row_save_metadata,
    _save_batches,
    _save_generated,
    _save_one_batch,
    _save_results,
    _stamp_recorded_artifact,
    apply_save_plan,
    build_save_plan,
)
from scidb.run.foreach_prepare import (  # noqa: F401 — re-exported
    _PLACEHOLDER_RE,
    _RidIndex,
    _VARIANT_TOKEN,
    _apply_pre_combo_hook,
    _build_call_identity,
    _build_rid_index,
    _check_path_outputs,
    _dry_run_preview,
    _existing_combos,
    _expand_aggregation,
    _expand_combos,
    _expand_full_iteration,
    _for_each_prepare,
    _for_each_save_resolved,
    _guard_pathoutput_collisions,
    _inject_path_placeholders,
    _iteration_level_of,
    _load_all_inputs,
    _log_input_multiplicity,
    _normalize_glue,
    _normalize_variable_inputs,
    _pathoutput_placeholders,
    _remap_fixed_rids_through_glue,
    _resolve_bp_placeholder,
    _resolve_iterables,
    _sanitize_path_value,
    _track_variants,
)


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scicanonicalhash import canonical_hash as _chash
from scidb.model.filters import SchemaKeyInFilter
from scidb.model.filters import raw_sql
from scidb.provenance import query as provenance_query
from scidb.provenance.graph import compute_pathinput_record_id
from scidb.provenance.graph import normalize_as_table
from scidb.provenance.query import check_recorded_selectors
from scidb.provenance.save import compute_input_selectors
from scidb.provenance.save import compute_input_selectors as _cis
from scidb.run.pipeline import GENERATES_FILE_ATTR
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Main for_each entry point
# ---------------------------------------------------------------------------


@journaled
def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[type],
    dry_run: bool = False,
    save: bool = True,
    as_table: list[str] | bool | None = None,
    db=None,
    distribute: bool = False,
    where=None,
    introspect: bool = False,
    skip_computed: bool = False,
    schema_filter: "dict[str, list] | None" = None,
    schema_keys: "list[str] | None" = None,
    schema_key_types: "dict[str, str] | None" = None,
    on_error: str = "continue",
    locations: "Any" = None,
    share_limits: "dict[str, list[str]] | None" = None,
    project_root: "str | _Path | None" = None,
    glue: "dict[str, Any] | None" = None,
    parameter_names: "dict[str, str] | None" = None,
    step: "str | None" = None,
    pipeline: Any = UNSET,
    hooks: "LoopHooks | None" = None,
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | Step | None":
    """
    Execute a function for all combinations of metadata, loading inputs
    and saving outputs automatically.

    This is the DB-backed wrapper. It:
    1. Resolves empty lists ``[]`` via ``db.distinct_schema_values()``
    2. Pre-filters schema combos via ``db.distinct_schema_combinations()``
    3. Builds ``ForEachConfig`` config keys
    4. Loads all input variables into DataFrames
    5. Converts scidb wrappers → scifor wrappers
    6. Delegates the core loop to ``scifor.for_each``
    7. Saves results from the returned table

    Args:
        fn: The function to execute (plain function handle; for_each handles
            lineage tracking internally).
        inputs: Dict mapping parameter names to variable types, Fixed wrappers,
                Merge wrappers, ColumnSelection wrappers, PathInput, or constants.
        outputs: List of output types/objects with ``.save()``.
        dry_run: If True, only print what would happen without executing.
        save: If True (default), save each function run's output. If False,
                    nothing is written to the database. For an endpoint
                    (``plot_``/``stat_``) that makes a DRAFT: a ``plot_`` figure
                    is still rendered to its PathOutput path and stamped; a
                    ``stat_`` result is pretty-printed and any PathOutput is
                    resolved to None, so e.g. csv-stats skips its PDF report.
        as_table: Controls which inputs are passed as full DataFrames.
        db: Optional database instance.
        distribute: If True, split outputs and save each piece at the schema
                    level below the deepest iterated key.
        where: Optional filter; passed to .load() calls on DB-backed inputs.
        introspect: If True, append introspection columns to the right of the
                    result DataFrame: _record_id_{param} and _branch_params_{param}
                    per DB-backed input, plus _call_id, _config_keys, _where on
                    every row. Does not affect saved outputs.
        skip_computed: If True, skip combos whose outputs already exist and whose
                    full upstream provenance graph is unchanged (function hash,
                    input record_ids, constant hashes). Default False. Requires a
                    database (provenance is always recorded, so there is
                    always a graph to compare against).
        schema_filter: Optional ``{schema_key: [values]}`` value overrides.
                    Cannot be combined with explicit **metadata_iterables.
                    For a key also named in schema_keys (or, if schema_keys
                    is omitted, any schema key at all), the given values
                    replace auto-resolution from the database. For a key
                    NOT being iterated, the values instead constrain which
                    records are loaded (ANDed into where= via
                    SchemaKeyInFilter) without adding an iteration dimension.
        schema_keys: Optional list of schema key names to iterate — structural
                    sugar for passing ``key=[]`` for each one by hand (each
                    auto-resolves to every distinct value in the database).
                    ``[]`` iterates EVERY schema key, the same reading as
                    ``subject=[]`` meaning every subject; ``None`` (the
                    default, with no schema_filter) iterates nothing and
                    pools every row into ONE call — the whole-dataset
                    operation. Cannot be combined with explicit
                    **metadata_iterables. Implemented via scifor's
                    ``expand_schema_keys()``, shared with scifor.for_each().
        schema_key_types: Optional ``{key: "numeric" | "string"}``. The
                    DATABASE owns schema-key types (``scidb.store.schema_types``),
                    so this records them there exactly as
                    ``configure_database(schema_key_types=...)`` does: a key
                    with no stored type gets this one, the same type is a
                    no-op, a different one raises
                    ``SchemaKeyTypeConflictError``. Accepted so a call
                    written for ``scifor.for_each`` runs here unchanged.
                    ``dry_run=True`` checks without writing.
        on_error: ``"continue"`` (default) or ``"stop"``; scifor owns it
                    (``scifor.for_each``). On ``"stop"`` the combos that
                    completed before the failure are SAVED, then the original
                    exception is re-raised (``scifor.stopped_run_of(exc)``
                    carries the record). See docs/claude/error-surfacing.md.
        locations: Optional ``scifor.LocationFilter`` (or its mapping form)
                    naming which schema locations to run: ragged ``include``
                    prefixes plus a standing ``exclude_levels`` rule. Passed
                    to ``scifor.for_each`` for the combos, after this layer has
                    built the full combo list (rid variants included). Its rows
                    are narrowed here, right after loading, because which
                    records a call reads is also what its lineage records, so
                    a key the run does not iterate still decides what each call
                    reads. Distinct from ``schema_filter``, which is a
                    per-key Cartesian narrowing applied while combos are
                    RESOLVED; ``locations`` can express what that cannot
                    (see docs/claude/location-filter-semantics.md).
        project_root: The folder a PathInput without ``root_folder``
                    resolves against; default the process-wide root
                    (``scifor.project.root.project_root``). Passed through to
                    ``scifor.for_each``. Resolution only, never identity.
        glue: Optional ``{param_name: GlueSpec | callable | [GlueSpec, ...]}``.
                    Free-code "glue" nodes applied in memory to that
                    parameter's loaded input before ``fn`` sees it, and never
                    saved. A glue node may change the column space but not the
                    row set; see ``scidb.run.glue`` and
                    ``docs/claude/free-code-glue-nodes.md``.
        parameter_names: Optional ``{argument: declared Parameter name}``.
                    Which declared Parameter fed each constant argument, when
                    the caller knows it and the value does not carry it (a
                    bare value, or a value recorded in history). Recorded on
                    the constant's provenance edge, never part of identity.
                    A named ``Parameter`` input supplies its own; see
                    ``scidb.model.parameter.declared_input_names``.
        step: A name for this pipeline STEP, recorded on the run (`_run_step`).
                    Not identity: renaming it re-runs nothing. The GUI uses it
                    to attribute a script run to the canvas node of the same
                    step name, even when the run's wiring changed (code
                    export writes it). Consequence: ``step`` is a reserved
                    name and cannot be used as a schema key.
        pipeline: Deferred-registration control. Omitted (default): if a
                    Pipeline is active (inside ``with db.pipeline(name):``,
                    or MATLAB's activated scidb.Pipeline), REGISTER this
                    call as a deferred step and return a Step handle — nothing
                    executes until ``pipeline.run_all()/run_until()``; with no
                    active pipeline, execute eagerly as always. Pass ``None``
                    to force an eager call even while a pipeline is active,
                    or a Pipeline instance to register into a non-ambient
                    pipeline. Consequence: ``pipeline`` is a reserved name and
                    cannot be used as a schema key in **metadata_iterables.
        hooks: Optional ``scifor.LoopHooks(progress_fn=..., cancel_check=...)``,
                    the same object ``scifor.for_each`` takes. ``progress_fn``
                    receives one dict per loop event (the GUI's progress bar
                    and the run summary read them); ``cancel_check`` is asked
                    before each combination and True stops the run. Any other
                    LoopHooks field raises: scidb decides those itself.
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, None when dry_run=True, or a Step
        handle when the call was registered into a Pipeline instead of run.
    """
    reject_removed_options(metadata_iterables)
    # A leftover `_progress_fn=` (the spelling before hooks=) would otherwise
    # become a silent metadata axis.
    reject_private_options(metadata_iterables)
    call = ForEachCall.from_options(
        fn,
        inputs,
        outputs,
        metadata_iterables,
        dry_run=dry_run,
        save=save,
        as_table=as_table,
        db=db,
        distribute=distribute,
        where=where,
        introspect=introspect,
        skip_computed=skip_computed,
        schema_filter=schema_filter,
        schema_keys=schema_keys,
        schema_key_types=schema_key_types,
        on_error=on_error,
        locations=locations,
        share_limits=share_limits,
        project_root=project_root,
        glue=glue,
        parameter_names=parameter_names,
        step=step,
        pipeline=pipeline,
        hooks=hooks,
    )
    return _run_for_each(call)


def _run_for_each(call: ForEachCall) -> "pd.DataFrame | Step | None":
    """``for_each`` on a bundled call: registration, the ambient step, the
    call-wide resolution (schema_key_types, where, schema_filter /
    schema_keys, declared Parameter names), then EachOf expansion or one
    concrete run (:func:`_run_concrete`)."""
    validate_on_error(call.on_error)

    # --- Deferred pipeline registration. Must run before ANY other work —
    #     a registered call must have zero side effects, and the spec must
    #     capture the arguments exactly as passed (pristine, un-normalized)
    #     so replay through run_*() is byte-identical to an eager call. ---
    target_pipeline = _target_pipeline(call.context.pipeline)
    if target_pipeline is not None:
        return target_pipeline.register_call(
            fn=call.fn,
            inputs=call.inputs,
            outputs=call.outputs,
            metadata_iterables=dict(call.metadata_iterables),
            options=call.option_values(),
        )

    # --- step: ambient for the whole call (EachOf alternatives included), read
    #     where the run is recorded (provenance_save, `_run_step`). ---
    if call.context.step is not None:
        from scidb.provenance.intent import check_step, current_step, run_step

        step = check_step(call.context.step)
        if current_step() != step:
            with run_step(step):
                return _run_for_each(call.with_context(step=step, pipeline=None))

    # --- schema_key_types: recorded in the DATABASE, which owns it
    #     (scidb.store.schema_types). Done once, here, before the EachOf expansion
    #     below — whose alternatives therefore do not repeat it: every
    #     alternative reads the stored declaration. ---
    if call.schema_key_types:
        decl_db = call.db if call.db is not None else database_or_none()
        if decl_db is None:
            raise DatabaseNotConfiguredError(
                "for_each(schema_key_types=...) records the types in the database, "
                "but no database is configured. Call configure_database() first, "
                "or use scifor.for_each for a run without a database."
            )
        decl_db.declare_schema_key_types(
            call.schema_key_types, dry_run=call.dry_run, source="for_each"
        )
        call = replace(call, schema_key_types=None)

    call = call.with_selection(where=_normalize_where(call.selection.where))

    # --- Resolve the active database + schema_filter/schema_keys. Seeds
    #     metadata_iterables from schema_keys when the caller used
    #     schema_keys/schema_filter instead of explicit iterables; actual DB
    #     resolution of the resulting [] placeholders happens later, in
    #     _for_each_prepare's generic empty-list resolver — this does no DB
    #     querying. ---
    sel = call.selection
    active_db = call.db
    if active_db is None and (
        call.skip_computed or sel.schema_filter is not None or sel.schema_keys is not None
    ):
        active_db = database_or_none()
    if sel.schema_filter is not None or sel.schema_keys is not None:
        call = replace(call, selection=_expand_schema_selection(sel, active_db))

    # --- Declared Parameter names: resolved BEFORE EachOf expansion, which
    #     replaces each Parameter with a bare alternative and so drops its
    #     .name. The alternatives receive the resolved dict. ---
    parameter_names = declared_input_names(call.inputs, call.context.parameter_names)
    # One name, one PathInput: two arguments naming different files alike
    # would be recorded as one input (the name is its identity).
    check_path_input_names(call.inputs)
    if parameter_names:
        Log.debug(f"parameter_names (argument -> declared): {parameter_names}")
    call = call.with_context(parameter_names=parameter_names)

    each_of_axes = _each_of_axes(call.inputs, call.selection.where)
    if each_of_axes:
        return _run_alternatives(call, each_of_axes)
    Log.debug("no EachOf expansion needed")
    return _run_concrete(call, active_db)


def _target_pipeline(pipeline: Any) -> "_Pipeline | None":
    """The Pipeline a call registers into: the one passed, the active one
    when omitted (UNSET), or none when None was passed."""
    if isinstance(pipeline, _Pipeline):
        return pipeline
    if pipeline is UNSET:
        return active_pipeline()
    if pipeline is not None:
        raise TypeError(
            f"pipeline= must be a Pipeline instance, None, or omitted; "
            f"got {type(pipeline).__name__}"
        )
    return None


def _normalize_where(where: Any) -> Any:
    """A string ``where=`` becomes a RawFilter that remembers the original
    string (for the version keys). An EachOf wrapper is left alone — its
    alternatives are normalized in their own runs."""
    if not isinstance(where, str):
        return where

    # Convert Python-style == to SQL =
    raw = raw_sql(where.replace("==", "="))
    # Store original for version_keys serialization
    raw._original_str = where
    return raw


def _expand_schema_selection(sel: RunSelection, active_db: Any) -> RunSelection:
    """Resolve ``schema_keys`` / ``schema_filter`` into the metadata
    iterables and ``where``; both are None in the result, so EachOf
    alternatives never resolve them again."""
    if active_db is None:
        raise ValueError(
            "schema_filter/schema_keys require a database connection, but no db "
            "was provided and no global database is configured."
        )
    schema_keys = sel.schema_keys
    # `schema_keys=[]` means EVERY key — the same reading as `subject=[]`
    # ("all values of subject") — while `None` (with no schema_filter)
    # never reaches here and pools every row into one call. The two are
    # the whole-dataset operation and the full grid; nothing in between
    # is spelled by emptiness.
    if isinstance(schema_keys, str):
        # `list("subject")` is `["s", "u", ...]` — seven unknown keys and
        # an empty grid, silently. The spelling is a list, always.
        raise TypeError(
            f"schema_keys must be a list of key names, not the string "
            f"{schema_keys!r} — write schema_keys=[{schema_keys!r}]"
        )
    if schema_keys:
        unknown = [k for k in schema_keys if k not in active_db.dataset_schema_keys]
        if unknown:
            raise ValueError(
                f"schema_keys {unknown} are not dataset schema keys "
                f"{list(active_db.dataset_schema_keys)}"
            )
    iterate_keys = (
        list(schema_keys)
        if schema_keys is not None and len(list(schema_keys)) > 0
        else active_db.dataset_schema_keys
    )
    # Shared with scifor.for_each(): seeds {key: []} for each iterated key,
    # and raises if metadata_iterables is already populated (mutual
    # exclusivity with explicit **metadata_iterables).
    metadata_iterables = _scifor.expand_schema_keys(iterate_keys, sel.metadata_iterables)
    where = sel.where

    # schema_filter overrides: an iterated key gets its values directly
    # (skipping DB auto-resolution for that key); a key NOT being
    # iterated instead constrains which records are loaded, via a
    # SchemaKeyInFilter ANDed into where= (previously silently ignored —
    # see docs/claude/archive/scidb-for-each-internals.md).
    if sel.schema_filter:
        for key, values in sel.schema_filter.items():
            if key in metadata_iterables:
                metadata_iterables[key] = values
            else:
                constraint = SchemaKeyInFilter(key, values)
                where = constraint if where is None else (where & constraint)
    Log.debug(
        f"expand_schema_keys: seeded metadata_iterables for {list(metadata_iterables.keys())}"
    )
    return replace(
        sel,
        where=where,
        schema_filter=None,
        schema_keys=None,
        metadata_iterables=metadata_iterables,
    )


def _each_of_axes(inputs: dict, where: Any) -> "list[tuple[str, str | None, list]]":
    """``[(kind, param, alternatives)]`` for every EachOf in the call."""
    axes: "list[tuple[str, str | None, list]]" = []
    for param, val in inputs.items():
        if isinstance(val, EachOf):
            require_alternatives(val, kind="input", param=param)
            axes.append(("input", param, val.alternatives))
    if isinstance(where, EachOf):
        require_alternatives(where, kind="where")
        axes.append(("where", None, where.alternatives))
    return axes


def _run_alternatives(
    call: ForEachCall, each_of_axes: "list[tuple[str, str | None, list]]"
) -> "pd.DataFrame | None":
    """One concrete run per EachOf alternative; results concatenated.

    Every alternative is the SAME call with one input (or ``where``)
    replaced — glue, locations, parameter names and every other option ride
    along by construction. on_error="stop" in one alternative raises out of
    this loop; that alternative saved its completed combos, and every
    earlier alternative was saved by its own run.
    """
    import pandas as pd

    Log.debug(
        f"EachOf expansion detected - {len(each_of_axes)} axes, will make recursive calls"
    )
    for kind, param, alts in each_of_axes:
        if kind == "input":
            Log.debug(f"  EachOf axis: input '{param}' with {len(alts)} alternatives")
        else:
            Log.debug(f"  EachOf axis: where with {len(alts)} alternatives")

    results = []
    for combo in product(*(axis[2] for axis in each_of_axes)):
        concrete_inputs = dict(call.inputs)
        concrete_where = call.selection.where
        for (kind, param, _alts), value in zip(each_of_axes, combo, strict=False):
            if kind == "input":
                concrete_inputs[param] = value
            elif kind == "where":
                concrete_where = value
        result = _run_for_each(
            replace(call, inputs=concrete_inputs).with_selection(where=concrete_where)
        )
        if result is not None:
            results.append(result)
        # Cooperative cancel: stop iterating across EachOf alternatives
        # as soon as the user cancels — don't start the next concrete run.
        if call.context.cancelled():
            break
    result_df = pd.concat(results, ignore_index=True) if results else None
    Log.debug(f"EachOf expansion complete - concatenated {len(results)} result(s)")
    return result_df


def _run_concrete(call: ForEachCall, active_db: Any) -> "pd.DataFrame | None":
    """One run with no EachOf left: endpoint policy, generates_file,
    for_columns resolution and the skip hook, then prepare -> execute ->
    save."""
    fn = call.fn
    inputs = call.inputs
    context = call.context
    save = call.save

    # --- Endpoint leaf detection (plot_/stat_ prefixes) ---
    # Policy (detection, PathOutput requirement, stat as_table default, draft
    # logging) is shared with the MATLAB bridge via
    # _endpoint_policy — one source of truth for both paths. Only the fn
    # WRAPPING is language-specific: Python wraps here; MATLAB wraps its own
    # fn in +scidb/for_each.m. See docs/claude/endpoints-viz-and-stats-design.md.
    endpoint_kind, path_param, as_table = _endpoint_policy(
        getattr(fn, "__name__", ""), inputs, save, call.run.as_table
    )
    run = replace(call.run, as_table=as_table)
    if endpoint_kind == "plot":
        fn = _make_plot_wrapper(fn, path_param)
        context = replace(context, inject_combo_metadata=True)
    elif endpoint_kind == "stat":
        fn = _make_stat_wrapper(fn, path_param, save)
        context = replace(context, inject_combo_metadata=True)

    # --- generates_file detection ---
    # No function wrapping: plain functions flow straight through to scifor, which
    # spreads tuple returns natively. Provenance is built from input bindings at
    # save time (the bipartite graph); the execute phase reconstructs BaseVariable
    # inputs. ``generates_file`` is read from the @scistack marker and drives combo-
    # metadata injection + a graph-native lineage-only save (see _save_results).

    is_generates_file = bool(getattr(fn, GENERATES_FILE_ATTR, None)) or bool(
        getattr(fn, "generates_file", False)
    )
    if is_generates_file:
        context = replace(context, inject_combo_metadata=True)
        Log.debug("generates_file=True → combo metadata injection + lineage-only save")

    # --- Resolve for_columns (iterate-mode ColumnSelection) inputs ---
    # Expand empty columns ([] / all) -> all data columns and validate the shared column
    # axis BEFORE version keys are built and before dry-run display,
    # so caching reflects the concrete column set.
    outputs = call.outputs
    inputs = _resolve_for_columns(
        inputs, call.db, [_output_column_name(o) for o in outputs] if outputs else None
    )

    # Build the skip_computed pre-combo hook on the plain function (its
    # function_hash + input bindings are the graph identity; no wrapper needed).
    # AFTER for_columns resolution: the hook compares each input's selector
    # against the recorded edge, and the edge carries the CONCRETE column
    # list — built on the unresolved `for_columns()` it compared `[]` against
    # `["a", "b"]` and recomputed every combo of a per-column step forever.
    # An endpoint draft never skips: looking at the figure or result is its whole
    # point, and an up-to-date record from an earlier run would leave nothing to show.
    is_endpoint_draft = endpoint_kind is not None and not save
    if is_endpoint_draft and call.skip_computed:
        Log.debug(f"skip_computed ignored for draft of {getattr(fn, '__name__', repr(fn))}")
    if (
        call.skip_computed
        and not is_endpoint_draft
        and not call.dry_run
        and outputs
        and active_db is not None
    ):
        context = replace(
            context,
            pre_combo_hook=_build_skip_hook(
                fn,
                outputs,
                active_db,
                inputs,
                as_table=run.as_table,
                distribute=run.distribute,
            ),
        )
        Log.debug(f"built skip_computed hook for {getattr(fn, '__name__', repr(fn))}")

    fn_name = _function_name(fn)

    # Read-side round-trip guard: did a selection this function ran with
    # before fail to reach this run? One query, log-only, at run entry — NOT
    # in _build_skip_hook, which does the same comparison but only exists when
    # skip_computed is on. See docs/claude/input-binding-round-trip.md §5.
    if active_db is not None and not call.dry_run:
        try:
            check_recorded_selectors(active_db._duck, fn_name, _cis(inputs))
        except Exception as exc:  # diagnostics must never fail a run
            Log.debug(f"[selector-check] skipped for {fn_name}: {exc}")

    # --- Pre-loop preparation. Returns None on dry_run shortcut. ---
    with Log.step(f"for_each_prepare({fn_name})"):
        state = _for_each_prepare(
            fn=fn,
            fn_name=fn_name,
            inputs=inputs,
            outputs=outputs,
            dry_run=call.dry_run,
            db=call.db,
            options=run,
            # Prepare narrows the loaded rows by `locations`, and its dry-run
            # shortcut makes its own scifor call with it: a preview that
            # describes a different run than the one that follows is worse
            # than no preview.
            selection=call.selection,
            context=context,
            glue=call.glue,
            # What kind of call this is — the save reads both off the state.
            generates_file=is_generates_file,
            endpoint_kind=endpoint_kind,
            project_root=call.project_root,
        )
    if state is None:
        return None

    # --- Execute: wrap fn for per-combo needs, delegate the loop to scifor,
    #     log the authoritative run summary. ---
    # on_error="stop": scifor re-raises the ORIGINAL exception carrying the
    # completed combos' results; save those first (never drop data), then
    # re-raise below so the caller's traceback still ends in the user's code.
    stop_exc: "BaseException | None" = None
    try:
        result_tbl = _for_each_execute(
            state,
            fn=fn,
            inputs=inputs,
            db=call.db,
            share_limits=call.share_limits,
            locations=call.selection.locations,
            context=context,
            on_error=call.on_error,
        )
    except Exception as e:
        stopped = stopped_run_of(e)
        if stopped is None:
            raise
        stop_exc = e
        result_tbl = stopped.partial_result
        Log.info(
            f"for_each({fn_name}): on_error=stop — saving {stopped.completed} "
            f"completed combo(s) before re-raising {type(e).__name__}"
        )

    # --- Save (+ endpoint artifact stamping) ---
    with Log.step(f"for_each_save({fn_name})"):
        result_tbl = _for_each_save_resolved(
            state=state,
            result_tbl=result_tbl,
            # state.inputs, not the local `inputs`: prepare folded any
            # Parameter-fed glue into the values, and that folded dict is what
            # ForEachConfig hashed.
            inputs=state.inputs if state.inputs is not None else inputs,
            outputs=outputs,
            save=save,
            db=call.db,
        )

    if stop_exc is not None:
        raise stop_exc

    if call.introspect and result_tbl is not None and not result_tbl.empty:
        result_tbl = _apply_introspect(result_tbl, state, call.selection.where)

    return result_tbl


# ---------------------------------------------------------------------------
# Execute phase (Steps 16-17) — between _for_each_prepare and
# _for_each_save_resolved
# ---------------------------------------------------------------------------


def _for_each_execute(
    state: "_ForEachState",
    *,
    fn: Callable,
    inputs: dict,
    db,
    share_limits,
    locations,
    context: RunContext,
    on_error: str = "continue",
):
    """Run scidb.for_each's Steps 16-17: wrap ``fn`` for whatever per-combo
    work the inputs need (PerComboLoader resolution, variable-input
    normalisation, combo-metadata injection, per-combo glue), delegate the
    loop to ``scifor.for_each`` over ``state.full_combos``, and log the one
    authoritative run-summary line. Returns scifor's result table.

    The middle third of ``for_each``: ``_for_each_prepare`` before it, and
    ``_for_each_save_resolved`` after. A pure signature move — every name it
    reads was a local of ``for_each`` at the point the body sat, and it
    still receives them explicitly (``inputs`` is the caller's dict after
    ``for_columns`` resolution, NOT ``state.inputs``, which has had
    Parameter-fed glue folded in; ``fn`` is the endpoint-wrapped function).
    The MATLAB bridge does not call this: its loop runs in MATLAB's
    ``scifor.for_each``, which is why prepare and save were already
    separate functions and this one is the last of the three.

    ``as_table`` / ``distribute`` come from ``state.options``; the cancel
    check, progress callback and combo-metadata injection from ``context``.
    """
    fn_name = state.fn_name
    as_table = state.options.as_table
    distribute = state.options.distribute
    _cancel_check = context.cancel_check
    _progress_fn = context.progress_fn
    _inject_combo_metadata = context.inject_combo_metadata

    # --- Step 16: Wrap fn to resolve PerComboLoader/PerComboLoaderMerge inputs
    #     per-combo, normalize variable inputs to raw data, and/or inject combo
    #     metadata (for generates_file functions). ---
    # Declared schema-key types drive PathInput spelling enforcement in
    # per-combo loads (string keys: exact only; undeclared keys that need a
    # spelling bridge: SchemaKeyTypeError). Hoisted above the wrap-fn guard
    # below because a bare PathInput input no longer needs any of scidb's
    # own per-combo wrapping (its resolution now happens inside scifor's
    # for_each loop via LoopHooks.path_input_resolver) — this must be available even
    # when nothing else triggers that wrapping.
    _kt_db = db
    if _kt_db is None:
        _kt_db = database_or_none()
    _schema_key_types = getattr(_kt_db, "dataset_schema_key_types", {}) or {}
    _schema_keys_for_types = list(getattr(_kt_db, "dataset_schema_keys", []) or [])

    def _path_input_resolver(pi: "PathInput", metadata: dict):
        return _load_pathinput_checked(
            pi, metadata, key_types=_schema_key_types, schema_keys=_schema_keys_for_types
        )

    _per_combo = {
        k: v
        for k, v in state.loaded_inputs.items()
        if isinstance(v, (PerComboLoader, PerComboLoaderMerge))
    }
    _has_variable_inputs = any(is_loadable(v) for v in inputs.values())
    # Params the caller asked to receive as whole DataFrames. Normalization must
    # leave these alone — see `_normalize_variable_inputs`. Same resolution rule
    # as scifor's `as_table_set` (True means every loadable input).
    _as_table_params = (
        {name for name, spec in inputs.items() if is_loadable(spec)}
        if as_table is True
        else set(as_table or ())
    )
    _per_combo_glue = state.per_combo_glue or {}
    if _per_combo or _inject_combo_metadata or _has_variable_inputs or _per_combo_glue:
        wrap_reasons = []
        if _per_combo:
            wrap_reasons.append(f"{len(_per_combo)} PerComboLoader input(s)")
        if _inject_combo_metadata:
            wrap_reasons.append("generates_file metadata injection")
        if _has_variable_inputs:
            wrap_reasons.append("variable input normalization")
        if _per_combo_glue:
            wrap_reasons.append(f"{len(_per_combo_glue)} per-combo glue chain(s)")
        Log.debug(f"wrapping function for {', '.join(wrap_reasons)}")
        _ordered_combos = state.full_combos
        _call_idx = [0]
        _orig_fn = fn
        _loaded_inputs_ref = state.loaded_inputs

        # Get function parameters to check which metadata keys it accepts.
        _fn_params = None
        if _inject_combo_metadata:
            if hasattr(_orig_fn, "__scidb_params__"):
                _fn_params = _orig_fn.__scidb_params__
            else:
                import inspect

                try:
                    sig = inspect.signature(_orig_fn)
                    _fn_params = set(sig.parameters.keys())
                except (ValueError, TypeError):
                    # Couldn't get signature, don't inject metadata
                    _fn_params = set()

        _path_extra = state.path_extra_keys or set()
        _ordered_schema_keys = list(state.current_schema_keys or [])

        def fn(**kwargs):  # noqa: F811 — intentional rebind
            idx = _call_idx[0]
            _call_idx[0] = idx + 1
            current_combo = _ordered_combos[idx] if idx < len(_ordered_combos) else {}
            # Exclude injected PathOutput placeholder keys: as load kwargs they
            # would act as branch_params FILTERS (with sanitized-string values)
            # and silently empty a PerComboLoader load.
            load_kw = {
                k: v
                for k, v in current_combo.items()
                if not k.startswith("__") and k not in _path_extra
            }
            resolved = {}
            for k, v in kwargs.items():
                if isinstance(v, PerComboLoader):
                    resolved[k] = _resolve_per_combo_loader(
                        v,
                        load_kw,
                        key_types=_schema_key_types,
                        schema_keys=_schema_keys_for_types,
                    )
                elif isinstance(v, PerComboLoaderMerge):
                    resolved[k] = _resolve_per_combo_merge(
                        v, load_kw, schema_keys=_schema_keys_for_types
                    )
                else:
                    resolved[k] = v

            # Normalize variable inputs to their raw data (DataFrame → array,
            # dict structure restored) — the form the function expects.
            if _has_variable_inputs:
                resolved = _normalize_variable_inputs(
                    resolved,
                    current_combo,
                    inputs,
                    _loaded_inputs_ref,
                    as_table_params=_as_table_params,
                    bindings=state.bindings,
                    aggregation_mode=state.aggregation_mode,
                )

            # Per-schema-key glue (the D4 opt-in) runs here, on the
            # already-sliced value, immediately before fn sees it — the same
            # code as the bulk site, a different hook.
            for _gp, _gchain in _per_combo_glue.items():
                if _gp in resolved:
                    resolved[_gp] = _glue.apply_glue_chain(
                        resolved[_gp],
                        _gchain,
                        param=_gp,
                        schema_keys=_ordered_schema_keys,
                    )

            if _inject_combo_metadata and _fn_params is not None:
                # Only inject metadata keys that the function signature accepts
                for k, v in load_kw.items():
                    if k not in resolved and k in _fn_params:
                        resolved[k] = v
            return _orig_fn(**resolved)

        # scifor names the function from `__name__` in its logs and failure
        # groups; without this every wrapped run reads "your function fn".
        # Only the name: `functools.wraps` would also set `__wrapped__`, which
        # makes `inspect.signature` see through this **kwargs wrapper.
        fn.__name__ = getattr(_orig_fn, "__name__", repr(_orig_fn))
        fn.__qualname__ = fn.__name__
    else:
        Log.debug("no function wrapping needed")

    # Wrap _progress_fn to track final completed/failed counts for logging.
    # scifor's final "summary" event is authoritative (it also carries the
    # aggregated failure reasons); the per-combo events keep the counts live
    # for GUI consumers.
    _run_summary = {
        "total": 0,
        "completed": 0,
        "skipped": 0,
        "no_data": 0,
        "cancelled": False,
        "stopped": False,
    }

    # Every location that failed or had no data, for _run_outcome
    # (scidb.provenance.outcomes): written in the finally below.
    from scidb.provenance.outcomes import OutcomeCollector, record_outcomes

    _outcomes = OutcomeCollector()

    def _tracking_progress_fn(info: dict):
        _outcomes.observe(info)
        _run_summary["total"] = info.get("total", _run_summary["total"])
        _run_summary["completed"] = info.get("completed", _run_summary["completed"])
        _run_summary["skipped"] = info.get("skipped", _run_summary["skipped"])
        if info.get("event") == "summary":
            _run_summary["no_data"] = info.get("no_data", 0)
            _run_summary["cancelled"] = bool(info.get("cancelled"))
            _run_summary["stopped"] = bool(info.get("stopped"))
        if _progress_fn is not None:
            _progress_fn(info)

    # Which rows of an input a combination reads is its `Selection`; scifor
    # applies it through this hook after its own schema filter. The hook and
    # the graph edges (`RunBindings.edges_for`) read the SAME selection — the
    # frame filter and the recorded lineage cannot disagree by construction.
    # A frame with no `__record_id` (a DataFrame passed directly, a PathInput,
    # a Merge) has no entry and passes through untouched.
    _bindings = state.bindings

    def _select_rows(param: str, frame, combo: dict):
        if RECORD_ID_COLUMN not in frame.columns:
            return frame
        sel = _bindings.selection_of(combo)
        rids = sel.rids.get(param) if sel is not None else None
        if rids is not None:
            frame = frame[frame[RECORD_ID_COLUMN].astype(str).isin([str(r) for r in rids])]
        return frame.drop(columns=[RECORD_ID_COLUMN])

    # Delegate the core loop to scifor
    try:
        with Log.step(f"delegate_to_scifor({fn_name}, {len(state.full_combos)} combos)"):
            result_tbl = _scifor_for_each(
                fn,
                state.loaded_inputs,
                dry_run=False,
                as_table=as_table,
                distribute=distribute,
                outputs=state.output_names,
                share_limits=share_limits,
                locations=locations,
                on_error=on_error,
                project_root=state.project_root,
                hooks=LoopHooks(
                    all_combos=state.full_combos,
                    progress_fn=_tracking_progress_fn,
                    cancel_check=_cancel_check,
                    path_input_resolver=_path_input_resolver,
                    mapping_inputs=state.mapping_inputs,
                    select_rows=_select_rows,
                    locations_applied_to_rows=True,
                    keep_no_output=True,
                ),
                **state.extended_metadata_iterables,
            )
    finally:
        # Authoritative run summary: scifor's counts plus the combos skip_computed
        # removed before the loop (already up to date). This is the one line that
        # documents what ran, what failed, and what was skipped for the whole call.
        _summary_parts = [
            f"completed={_run_summary['completed']}",
            f"failed={_run_summary['skipped'] - _run_summary['no_data']}",
            f"no_data={_run_summary['no_data']}",
        ]
        if state.skip_computed_count:
            _summary_parts.append(
                f"skipped={state.skip_computed_count} (skip_computed, up to date)"
            )
        _summary_parts.append(f"total={_run_summary['total'] + state.skip_computed_count}")
        _journal = Log.current_run()
        if _journal is not None and state.skip_computed_count:
            _journal.count("up_to_date", state.skip_computed_count)
        if _run_summary["cancelled"]:
            _summary_parts.append("cancelled")
        if _run_summary["stopped"]:
            _summary_parts.append("stopped (on_error=stop)")
        Log.info(f"for_each({fn_name}) run summary: {', '.join(_summary_parts)}")
        if _outcomes.events:
            _outcome_db = _kt_db if _kt_db is not None else database_or_none()
            try:
                if _outcome_db is not None:
                    record_outcomes(_outcome_db, fn_name, state.output_types, _outcomes.events)
            except Exception as _e:  # never fail a run over its explanation
                Log.warn(
                    f"[outcomes] {fn_name}: could not store {len(_outcomes.events)} "
                    f"location outcome(s): {type(_e).__name__}: {_e}"
                )

    return result_tbl


# ---------------------------------------------------------------------------
# Introspect helper
# ---------------------------------------------------------------------------


def _apply_introspect(result_tbl, state, where):
    """Append introspection columns to the for_each result DataFrame.

    Column order: existing columns (schema + outputs, minus the combo handle)
    → _record_id_* / _branch_params_* pairs (one per DB-backed input) →
    _call_id, _config_keys, _where.

    Every row names its ``Selection`` (``COMBO_KEY``); the per-input columns
    are read from it. A full-iteration input reads one record, so its
    ``_record_id_*`` is that id; an aggregated input reads several, so its
    ``_record_id_*`` is the list and its ``_branch_params_*`` the variant
    group's (from the signature) — the same two shapes the ``__rid_*`` /
    ``__vsig_*`` row columns used to carry.
    """
    bindings = state.bindings
    selections = [bindings.selection_of(row) for row in result_tbl.to_dict("records")]
    df = result_tbl.drop(columns=[COMBO_KEY], errors="ignore")

    params: list = []
    for sel in selections:
        if sel is None:
            continue
        for p in list(sel.rids) + list(sel.groups):
            if p not in params:
                params.append(p)

    for param_name in params:
        rid_col: list = []
        bp_col: list = []
        for sel in selections:
            rids = tuple(sel.rids.get(param_name, ())) if sel else ()
            sig = sel.groups.get(param_name) if sel else None
            if sig is not None:
                # An aggregated split input: the group's coordinate.
                rid_col.append(list(rids))
                try:
                    bp_col.append(json.loads(sig))
                except (ValueError, TypeError):
                    bp_col.append({})
            elif len(rids) == 1:
                rid_col.append(rids[0])
                bp_col.append(bindings.rid_to_bp.get(rids[0], {}))
            else:
                rid_col.append(list(rids))
                merged, _ = merge_branch_params(bindings.rid_to_bp.get(r, {}) for r in rids)
                bp_col.append(merged)
        df[f"_record_id_{param_name}"] = rid_col
        df[f"_branch_params_{param_name}"] = bp_col

    # Append call-level columns (same value on every row).
    df["_call_id"] = state.call_id
    df["_config_keys"] = json.dumps(state.config_keys)
    df["_where"] = repr(where) if where is not None else None

    return df


# ---------------------------------------------------------------------------
# skip_computed pre-combo hook
# ---------------------------------------------------------------------------


def _find_skip_gate_record(
    db,
    type_name,
    schema_combo,
    fn_name,
    target_const_hashes,
    expected_input_rids=None,
    fixed_rids=frozenset(),
    target_glue_rids=None,
    target_selection=None,
):
    """Latest output record of ``type_name`` at ``schema_combo`` produced by an
    invocation of ``fn_name`` whose constants match ``target_const_hashes``
    (``{param: canonical_hash(value)}``), or None.

    ``target_glue_rids`` (``{param: virtual glue rid}``) are the Parameter-fed
    glued constants, which the graph records as a virtual glue record on the
    consumer's edge (not a constant): a candidate matches only when it bound
    exactly that record — so distinct Parameter values, an edited glue body
    and a changed glued value each disambiguate here, as plain constants do.

    ``target_selection`` is what this call narrowed (``SelectionAxis.to_json``, or
    None). A candidate must agree: the same rows under another axis (a subject that
    only has minute 6, once with no selection and once with one) are another
    variant, and skipping would leave this one without that subject.

    Graph-precise variant lookup for the skip gate: filters by producing function
    (so a re-saved raw input at the same location is not mistaken for the output)
    and by constant hashes (so distinct constant variants are disambiguated).

    ``expected_input_rids`` (aggregation mode): the exact set of upstream
    record_ids this combo's variant group consumes. Aggregation combos carry no
    ``__rid_*`` keys for the staleness comparison, so the GATE must bind the
    group identity instead — a candidate only matches when its invocation's
    consumed variable-input rid set equals the expected set (after removing
    ``fixed_rids`` and the candidate itself, mirroring the full-iteration
    self-referential guard). Without this, a new variant group (or an
    aggregation whose underlying record set grew, e.g. a new session) would
    cross-skip against another group's / the stale record.
    """

    schema_keys = set(db.dataset_schema_keys)
    conds = [
        "inv.function_name = ?",
        "r.type = ?",
        "COALESCE(r.excluded, FALSE) = FALSE",
    ]
    params: list = [fn_name, type_name]
    for k, v in schema_combo.items():
        if k in schema_keys:
            conds.append(f's."{k}" = ?')
            params.append(schema_str(v))
    # LEFT JOIN _schema: a grand-aggregation output (zero iterated schema keys)
    # is saved at the ROOT level with a NULL schema_id — an inner join would
    # silently drop it from the gate, forcing eternal recompute. With no
    # schema_combo conds the record passes; with conds, NULL schema columns
    # fail the equality naturally.
    rows = db._duck.fetchall(
        "SELECT io.output_record_id FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "JOIN _record_save rm ON rm.record_id = io.output_record_id "
        "LEFT JOIN _schema s ON r.schema_id = s.schema_id "
        f"WHERE {' AND '.join(conds)} ORDER BY rm.timestamp DESC",
        params,
    )
    _rejections: list = []
    for (rid,) in rows:
        sig = provenance_query.stored_invocation_signature(db._duck, rid)
        if sig is None:
            _rejections.append(f"{rid[:12]}: no producing invocation")
            continue
        if sig.get("const_hashes", {}) != target_const_hashes:
            _rejections.append(
                f"{rid[:12]}: const_hashes mismatch "
                f"(stored={sig.get('const_hashes', {})}, target={target_const_hashes})"
            )
            continue
        # Parameter-fed glued constants: the stored invocation must have bound
        # exactly these virtual records — and no glue where none is expected.
        stored_glue = sig.get("constant_glue", {})
        if stored_glue != dict(target_glue_rids or {}):
            _rejections.append(
                f"{rid[:12]}: Parameter-fed glue mismatch (stored={stored_glue}, "
                f"target={dict(target_glue_rids or {})}) — the value, glue body "
                f"or glued value differs"
            )
            continue
        if sig.get("selection") != target_selection:
            _rejections.append(
                f"{rid[:12]}: selection mismatch (stored={sig.get('selection')}, "
                f"target={target_selection})"
            )
            continue
        stored_var = sig.get("var_inputs", {})
        if expected_input_rids is not None:
            stored_rids = {
                str(in_rid)
                for edges in stored_var.values()
                for (in_rid, _sel) in edges
                if in_rid is not None
            }
            stored_rids -= {str(r) for r in fixed_rids}
            stored_rids.discard(str(rid))
            if stored_rids != expected_input_rids:
                _extra = sorted(stored_rids - expected_input_rids)[:3]
                _missing = sorted(expected_input_rids - stored_rids)[:3]
                _rejections.append(
                    f"{rid[:12]}: rid-set mismatch (stored {len(stored_rids)} vs "
                    f"expected {len(expected_input_rids)}; stored-only={_extra}, "
                    f"expected-only={_missing})"
                )
                continue
        return rid
    # Gate diagnostics (NOTE 2): a silent None here is indistinguishable from a
    # legitimate first run, so record WHY each candidate was rejected.
    if _rejections:
        Log.debug(
            f"[skip-gate] {type_name} at {schema_combo or '(root)'} for "
            f"{fn_name!r}: {len(rows)} candidate(s), none matched — "
            + " | ".join(_rejections[:5])
        )
    elif expected_input_rids is not None:
        Log.debug(
            f"[skip-gate] {type_name} at {schema_combo or '(root)'} for "
            f"{fn_name!r}: NO candidate records found (query returned 0 rows)"
        )
    return None


def _build_skip_hook(
    fn,
    outputs: list,
    db,
    inputs: dict,
    as_table=None,
    distribute: bool = False,
    fn_hash: "str | None" = None,
    agg_binding_ref: "dict | None" = None,
) -> "Callable[[dict], bool]":
    """Return a pre-combo hook that returns True when a combo can be skipped.

    Rewritten over the bipartite provenance graph (the §11 "port" approach: read
    function_hash + input edges + constant records from the graph, not _lineage).
    The decision splits into:

    1. *Missing vs. variant gate* — ``find_record_id`` on (schema + constants +
       ``__fn`` name) decides whether any output for this variant exists yet.
       No record → the combo has never been run for this variant → compute
       silently (no ``[recompute]`` line), matching the legacy contract.
    2. *Staleness via edge comparison* — load the producing invocation's
       signature for that output (``stored_invocation_signature``) and compare,
       binding by binding, against the combo's *current* inputs: function hash,
       each variable input record_id (+ ColumnSelection selector), and constant
       content hashes. Any mismatch → ``[recompute]``; otherwise ``[skip]``.

    The self-referential guard (``input rid == output rid`` => stable) is what a
    pure membership test cannot express — it stops input==output pipelines
    (variant expansion feeding an output back as input) from recomputing forever.

    Binding parity with the save path holds by construction: the same
    ``full_combo`` whose ``__rid_*`` values feed this hook also feed the save
    path's graph edges, ``compute_function_hash(fn)`` equals the save-side
    ``__fn_hash``, and both sides derive selectors from ``compute_input_selectors``.

    ``fn`` is a plain pipeline function; its identity comes from
    ``compute_function_hash(fn)`` (the same hash for_each writes to the graph).

    ``fn_hash`` overrides that computation. The MATLAB-driven bridge passes a
    no-op sentinel for ``fn`` whose hash would never match what the MATLAB save
    path stored (the MATLAB-computed source hash, written as ``__fn_hash`` →
    graph ``function_hash``). Passing the same MATLAB hash here keeps the
    function-hash comparison meaningful instead of forcing eternal recompute.

    ``agg_binding_ref`` is a MUTABLE holder ``{"bindings": RunBindings | None,
    "aggregation": bool}``. The hook is built BEFORE ``_for_each_prepare`` but
    fires inside prepare's Step 14 combo filter — after Step 13 has built the
    typed bindings and registered every combination's ``Selection`` — so
    prepare fills the holder right before Step 14 via the
    ``_agg_binding_ref`` attribute exposed on the returned hook (one dict is
    auto-created when the caller passes none, which covers both the Python
    and MATLAB-bridge call sites). Each combo names its selection
    (``COMBO_KEY``); in aggregation mode the gate binds the variant group by
    its exact consumed-rid set (see ``_find_skip_gate_record``) instead of
    the per-param rid comparison.
    """


    if agg_binding_ref is None:
        agg_binding_ref = {"bindings": None, "aggregation": False}

    schema_keys: set = set(db.dataset_schema_keys)

    # Pre-compute scalar constant inputs (mirrors ForEachConfig._get_direct_constants
    # so skip-side and save-side constant bindings agree).
    constant_values: dict = {}
    try:
        from scifor import PathInput as _PathInput
    except ImportError:
        _PathInput = None
    try:
        from scifor import PathOutput as _PathOutput
    except ImportError:
        _PathOutput = None
    # PathInput spec record ids, as the save path binds them
    # (provenance_save._pathinput_bindings) — part of identity, so a record
    # produced from a different file is stale.
    path_input_rids: dict = {}
    for name, value in inputs.items():
        if is_loadable(value):
            continue
        if _PathInput is not None and isinstance(value, _PathInput):
            path_input_rids[name] = compute_pathinput_record_id(value.to_key())
            continue
        if _PathOutput is not None and isinstance(value, _PathOutput):
            continue
        if isinstance(value, ColName):
            continue
        constant_values[name] = value

    # Identity hash: use the caller-supplied override (MATLAB path passes its
    # source hash, since the sentinel ``fn`` has no meaningful bytecode hash);
    # otherwise compute from the plain Python function.
    fn_hash = fn_hash if fn_hash is not None else compute_function_hash(fn, truncate=16)
    # Plain function name (``.fcn`` peel kept only for any legacy wrapped input).
    fn_name = _function_name(fn)
    selectors = compute_input_selectors(inputs)
    # The run options THIS call runs under, spelled the way the graph spells
    # them (`run_options_label`), so a record produced under other options —
    # pooled where this call splits, distributed where it does not — is not
    # taken as "already computed" merely because its edge set matches.
    from scidb.provenance.intent import parse_selector

    _loadable_params = [
        p for p, s in inputs.items() if is_loadable(s) or isinstance(s, PathInput)
    ]
    _label_args = (
        bool(distribute),
        normalize_as_table(as_table, _loadable_params),
        [p for p, s in selectors.items() if (parse_selector(s) or {}).get("iterate")],
        [p for p, s in inputs.items() if isinstance(s, AcrossVariants)],
    )
    # The iteration level is only known once _for_each_prepare has resolved
    # the iterables, which is AFTER this hook is built (on both the Python and
    # the MATLAB path). Prepare fills this ref; the label is spelled at
    # compare time. Unfilled, the label has no level, never matches a record
    # saved with one, and the combo recomputes: safe, and logged below.
    level_ref: dict = {"level": None, "set": False}

    def _run_options_now() -> str:
        if not level_ref["set"]:
            Log.debug(
                f"[skip] {fn_name}: iteration level not set on the skip hook; "
                f"comparing run options without it (the combo will recompute)"
            )
        return provenance_query.run_options_label(
            *_label_args, iteration_level=level_ref["level"]
        )

    def _combo_str(schema_combo: dict) -> str:
        return ", ".join(f"{k}={v}" for k, v in sorted(schema_combo.items()))

    def _recompute(combo_str: str, why: str) -> bool:
        Log.debug(f"[recompute] {combo_str} — {why}")
        return False

    Log.debug(
        f"_build_skip_hook: constants={list(constant_values.keys())}, "
        f"fn_hash={fn_hash[:12]}, selectors={ {k: v for k, v in selectors.items() if v} }"
    )

    # Parameter-fed glue: a glued constant's recorded edge binds a virtual glue
    # record, not the constant, and that rid needs the glued value's hash —
    # known only once prepare has run the glue (after this hook is built, on
    # both run paths). Prepare derives ``{param: virtual rid}`` with the save
    # path's own rule and fills this ref; the hook only reads it. Unset, glued
    # params are unknown and compare as plain constants (which then never
    # match: safe, recompute).
    constant_glue_ref: dict = {"rids": {}, "set": False}
    #: invocation id -> whether today's recipe reproduces it (asked once each).
    _recipe_checked: dict[str, bool] = {}

    def _should_skip(combo: dict) -> bool:
        schema_combo = {k: v for k, v in combo.items() if k in schema_keys}
        combo_str = _combo_str(schema_combo)
        # What this combination reads — its `Selection` (filled into the
        # holder at Step 13, before this hook fires at Step 14).
        _bound: RunBindings | None = (agg_binding_ref or {}).get("bindings")
        _sel = _bound.selection_of(combo) if _bound is not None else None
        if _sel is not None and _sel.groups:
            combo_str += " [" + "; ".join(
                f"{p} group {sig}" for p, sig in sorted(_sel.groups.items())
            ) + "]"

        # Aggregation mode: bind the combo's variant group to its exact
        # consumed-rid set. The per-param staleness comparison below compares
        # one record per param, which is not what an aggregating call does,
        # so the group identity must be enforced at the gate — otherwise a
        # NEW variant group (or a grown record set) would cross-skip against
        # another group's output.
        expected_input_rids = None
        _fixed_rids: frozenset = frozenset()
        if _bound is not None and (agg_binding_ref or {}).get("aggregation"):
            _fixed_rids = frozenset(str(r) for r in _bound.pinned_rids.values())
            expected_input_rids = {
                str(r) for rids in _bound.rids_for(_sel).values() for r in rids
            } - _fixed_rids

        # Gate: does an output for THIS variant already exist? Find the latest
        # output record of OutputCls at this schema location whose PRODUCING
        # invocation is ``fn`` with matching constants (graph-precise). Filtering
        # by producing function is essential for input==output (self-referential)
        # pipelines: a re-saved raw input at the same location must NOT be mistaken
        # for the fn's output (it has no producing invocation → would force a
        # spurious recompute).
        glue_rids = constant_glue_ref["rids"]
        target_const_hashes = {
            n: _chash(v) for n, v in constant_values.items() if n not in glue_rids
        }
        output_record_id = None
        for OutputCls in outputs:
            rid = _find_skip_gate_record(
                db,
                OutputCls.__name__,
                schema_combo,
                fn_name,
                target_const_hashes,
                expected_input_rids=expected_input_rids,
                fixed_rids=_fixed_rids,
                target_glue_rids=glue_rids,
                target_selection=(
                    _sel.axis.to_json() if _sel is not None and _sel.axis is not None else None
                ),
            )
            if rid is None:
                Log.debug(
                    f"missing: {combo_str} — no output record for {OutputCls.__name__}"
                )
                return False
            output_record_id = rid

        # Staleness: compare the stored producing invocation's signature against
        # the combo's current inputs.
        sig = provenance_query.stored_invocation_signature(db._duck, output_record_id)
        if sig is None:
            return _recompute(combo_str, "no provenance record")
        if sig["function_hash"] != fn_hash:
            return _recompute(combo_str, "function hash changed")
        # An invocation recorded under an older id recipe is the same
        # computation, but node state can never find it: compute it once more
        # so it is written under today's id (provenance_query.invocation_id_is_current).
        _producer = provenance_query.producing_invocation(db._duck, output_record_id)
        if _producer is not None:
            _inv = _producer[0]
            if _inv not in _recipe_checked:
                _recipe_checked[_inv] = provenance_query.invocation_id_is_current(
                    db._duck, _inv
                )
            if not _recipe_checked[_inv]:
                return _recompute(
                    combo_str, f"invocation {_inv} was recorded under an older id recipe"
                )
        run_options_now = _run_options_now()
        if sig.get("run_options", run_options_now) != run_options_now:
            return _recompute(
                combo_str,
                f"run options changed ({sig['run_options']} -> {run_options_now})",
            )

        stored_var = sig["var_inputs"]  # param -> [(record_id, selector), ...]
        _current = _bound.rids_for(_sel) if _bound is not None else {}
        for param, rid_vals in _current.items():
            for rid_val in rid_vals:
                # Self-referential: the loaded input IS the output record. Stable.
                if str(rid_val) == str(output_record_id):
                    continue
                stored = stored_var.get(param)
                if not stored:
                    return _recompute(combo_str, f"no stored input {param}")
                stored_rids = {str(r) for r, _s in stored}
                if str(rid_val) not in stored_rids:
                    # Naming both rids is what makes an edited *glue*
                    # diagnosable: the binding moves because the glue's
                    # virtual record id moved, and this is the line that
                    # proves the edit propagated.
                    return _recompute(
                        combo_str,
                        f"binding '{param}' changed ({sorted(stored_rids)} -> {rid_val})",
                    )
                stored_sel = next((s for r, s in stored if str(r) == str(rid_val)), None)
                if (stored_sel or None) != (selectors.get(param) or None):
                    return _recompute(combo_str, f"selector for {param} changed")

        # Constants: compare current value hashes to stored content hashes.
        stored_const = sig["const_hashes"]
        for name, value in constant_values.items():
            if name in glue_rids:
                continue  # bound through its virtual glue record (the gate)
            cur_hash = _chash(value)
            stored_hash = stored_const.get(name)
            if stored_hash is None:
                if stored_const:
                    return _recompute(combo_str, f"new constant {name}")
            elif stored_hash != cur_hash:
                return _recompute(combo_str, f"constant {name} changed")

        # PathInputs: the spec that fed the stored record vs the one now.
        stored_pi = sig.get("path_inputs") or {}
        for name, cur_rid in path_input_rids.items():
            if stored_pi.get(name) != cur_rid:
                return _recompute(
                    combo_str,
                    f"PathInput {name} changed ({stored_pi.get(name)} -> {cur_rid})",
                )

        Log.debug(f"[skip] {combo_str}")
        return True

    # Exposed so _for_each_prepare can fill the aggregation binding after
    # Step 12 (variant-group → rid mapping) and before Step 14 (hook firing).
    _should_skip._agg_binding_ref = agg_binding_ref
    # Exposed so _for_each_prepare can hand over the iteration level once the
    # iterables are resolved (see _run_options_now).
    _should_skip._iteration_level_ref = level_ref
    # Exposed so _for_each_prepare can hand over the Parameter-fed glue's
    # identity once the glue has run (see constant_glue_ref).
    _should_skip._constant_glue_ref = constant_glue_ref
    return _should_skip
