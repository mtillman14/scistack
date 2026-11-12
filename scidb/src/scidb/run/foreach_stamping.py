"""for_each: endpoint artifact provenance stamping (D4) and the column-selection iteration it shares with loading.

Moved verbatim out of foreach.py (grug follow-up, docs/claude/grugbrain-principles.md).
"""

import json
import time
from pathlib import Path as _Path
from scidb.model.log import Log
from scidb.store.database import database_or_none
from scidb.store.schema_values import schema_str
from scifor import ColumnSelection
from scifor import Fixed
from typing import Any

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from scidb.run.foreach_common import (
    _ForEachState,
    _get_schema_keys,
    _to_dataframe,
)


# ---------------------------------------------------------------------------
# Endpoint artifact provenance stamping (D4)
# ---------------------------------------------------------------------------


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.artifact_stamp import stamp_artifact
from scidb.model.exceptions import ForColumnsNoColumnsError
from scidb.model.variant import pin_loads_uncollapsed
from scidb.store.database import get_database
from scifor.column_selection import ColumnGroupsError
from scifor.column_selection import resolve_column_groups
def _endpoint_artifact_path(
    endpoint_kind: "str | None", data_value: Any
) -> "str | None":
    """The artifact file a result row points at, or None.

    plot_: the output value IS the path string. stat_: the output value is the
    result JSON; its ``report_path`` (present only in record mode with a
    PathOutput) names the PDF report. Drafts of stat_ resolve PathOutput to
    None, so they yield no path — nothing to stamp.
    """
    if endpoint_kind == "plot":
        return data_value if isinstance(data_value, str) and data_value else None
    if endpoint_kind == "stat" and isinstance(data_value, str):
        try:
            parsed = json.loads(data_value)
        except (ValueError, TypeError):
            return None
        rp = parsed.get("report_path") if isinstance(parsed, dict) else None
        return rp if isinstance(rp, str) and rp else None
    return None


def _stamp_db_name(db: Any) -> "str | None":
    """Basename of the active database file for the stamp blob."""
    if db is None:
        db = database_or_none()
        if db is None:
            return None
    p = getattr(db, "dataset_db_path", None)
    return _Path(p).name if p is not None else None


def _build_stamp_blob(
    *, fn_name: str, inputs_map: dict, schema: dict, db: Any, record_id: "str | None"
) -> dict:
    """The provenance blob embedded in endpoint artifacts (D4).

    ``record_id`` is the primary key (the bipartite graph reaches everything
    else from it); the rest is human-readable redundancy that survives DB
    loss. Drafts carry the FULL blob with ``draft: true`` in place of the
    record_id — a draft figure is fully traceable to its exact input records.
    """
    blob = {
        "scidb_stamp": 1,
        "function": fn_name,
        "inputs": inputs_map,
        "schema": schema,
        "database": _stamp_db_name(db),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    if record_id is not None:
        blob["record_id"] = str(record_id)
    else:
        blob["draft"] = True
    return blob


def _stamp_inputs_from_edges(edges) -> dict:
    """Consumed input rids per param, from the record's typed edges (record
    mode). ``{param: [rid, ...]}``."""
    inputs_map: dict = {}
    for e in edges or []:
        inputs_map.setdefault(e.param, []).append(e.rid)
    return inputs_map


def _stamp_draft_endpoint_artifacts(
    endpoint_kind: str, result_tbl, state: "_ForEachState", db: Any
) -> None:
    """Draft-mode stamping pass: the save phase is suppressed, but draft
    artifacts get the SAME blob a recorded run would embed (decided
    2026-07-06), with ``draft: true`` in place of the record_id. Runs from
    ``_for_each_save_resolved``, which executes in both modes.
    """
    import pandas as pd


    if not state.output_names:
        return
    out_name = state.output_names[0]
    schema_keys = list(state.current_schema_keys or [])
    stamped = 0
    for row in result_tbl.to_dict("records"):
        apath = _endpoint_artifact_path(endpoint_kind, row.get(out_name))
        if not apath:
            continue
        # The same assembly the record-mode save uses, so a draft stamp names
        # exactly the edges a real save would record.
        edges = state.bindings.for_combo(row)
        inputs_map = _stamp_inputs_from_edges(edges)
        schema = {
            k: row[k]
            for k in schema_keys
            if k in row
            and row[k] is not None
            and not (isinstance(row[k], float) and pd.isna(row[k]))
        }
        blob = _build_stamp_blob(
            fn_name=state.fn_name,
            inputs_map=inputs_map,
            schema=schema,
            db=db,
            record_id=None,
        )
        stamp_artifact(apath, blob)
        stamped += 1
    if stamped:
        Log.info(
            f"[artifact-stamp] draft: stamped {stamped} artifact(s) "
            f"for {state.fn_name} (full provenance, no record)"
        )


def _iterate_column_selection(spec: Any) -> "ColumnSelection | None":
    """Return the iterate-mode ColumnSelection inside a spec (bare or Fixed), else None."""
    if isinstance(spec, ColumnSelection) and spec.iterate:
        return spec
    if (
        isinstance(spec, Fixed)
        and isinstance(spec.data, ColumnSelection)
        and spec.data.iterate
    ):
        return spec.data
    return None


def _resolve_all_columns(var_type: Any, db: Any | None) -> list[str]:
    """Resolve ``for_columns()`` (all columns) to the variable's data column names.

    Reads them from storage metadata (``DatabaseManager.data_column_names``)
    and returns them minus schema keys and internal ``__*`` columns. Used so
    that an empty ``columns`` becomes a concrete list before version keys are
    computed.

    It used to load the WHOLE variable to read its column names, and prepare
    then loaded it again for the call (cleanup-audit F28: 1.1 s twice for a
    420-row table). A load is now only the fallback for a type whose metadata
    cannot answer (custom/nested storage, overridden construction).
    """
    import pandas as pd

    var_name = getattr(var_type, "__name__", repr(var_type))
    active_db = db
    if active_db is None:
        active_db = get_database()
    names = None
    if isinstance(var_type, type) and hasattr(active_db, "data_column_names"):
        names = active_db.data_column_names(var_type)
    if names is not None:
        source = "storage metadata"
    else:
        loaded = _load_var_type_as_spread(var_type, db, None)
        if not isinstance(loaded, pd.DataFrame):
            raise ValueError(
                f"for_columns(): could not load '{var_name}' to resolve its columns "
                f"(no DataFrame returned). Pass an explicit column list instead."
            )
        names = list(loaded.columns)
        source = "a full load (metadata could not answer)"
    schema_keys = _get_schema_keys(db)
    cols = [
        c
        for c in names
        if c not in schema_keys and not str(c).startswith("__")
    ]
    Log.info(
        f"[for_columns] {var_name}: every column = {len(cols)} column(s), "
        f"read from {source}"
    )
    if not cols:
        if not names:
            raise ForColumnsNoColumnsError(
                f"for_columns(): '{var_name}' has no data columns to iterate "
                f"because it has no saved records yet. Run the step that "
                f"produces it first, or pass an explicit column list."
            )
        raise ForColumnsNoColumnsError(
            f"for_columns(): no data columns found for '{var_name}': every "
            f"column it has ({names}) is a schema key or internal."
        )
    return cols


def _resolve_for_columns(
    inputs: dict, db: Any | None, output_names: "list[str] | None" = None
) -> dict:
    """Resolve iterate-mode ColumnSelection inputs (``for_columns``).

    Expands empty ``columns`` ([] / all) to all data columns and validates that every
    iterate input shares the same column set (zipped by name), or the same
    column GROUPS when grouped. Returns a new inputs dict with concrete
    ColumnSelections; raises on mismatch. Inputs without any iterate
    selection are returned unchanged.

    ``output_names``: when given, the combine/split decision is checked here,
    before anything loads (``scifor.column_selection.resolve_column_groups``
    owns it; scifor asks again at its Step 6.5). It must be asked HERE for
    ``for_columns()`` (every column): after this expansion scifor sees an
    explicit list and could no longer tell that splitting it across outputs
    would pair them in the data's column order.
    """
    iterate_params = {
        name: cs
        for name, spec in inputs.items()
        if (cs := _iterate_column_selection(spec)) is not None
    }
    if not iterate_params:
        return inputs

    Log.debug(f"resolving for_columns iteration for input(s) {list(iterate_params)}")

    resolved_db = db
    if resolved_db is None:
        resolved_db = database_or_none()

    resolved_cols: dict[str, list[str]] = {}
    for name, cs in iterate_params.items():
        if cs.groups is not None:
            cols = list(cs.columns)  # groups list their columns explicitly
        elif not cs.columns:  # empty [] (or legacy None) -> all data columns
            cols = _resolve_all_columns(cs.data, resolved_db)
            if cs.excl_columns:
                excl = set(cs.excl_columns)
                cols = [c for c in cols if c not in excl]
            Log.debug(
                f"for_columns: resolved '{name}' to all {len(cols)} column(s): {cols}"
            )
        else:
            cols = list(cs.columns)
        resolved_cols[name] = cols

    # Zip: every iterate input must cover the same columns (or groups).
    ref_name = next(iter(resolved_cols))
    ref_cs = iterate_params[ref_name]
    ref_set = set(resolved_cols[ref_name])
    for name, cols in resolved_cols.items():
        cs = iterate_params[name]
        if ref_cs.groups is not None or cs.groups is not None:
            if cs.groups != ref_cs.groups:
                raise ColumnGroupsError(
                    f"for_columns inputs are zipped group by group, so they must "
                    f"declare the same groups. '{ref_name}' has {ref_cs.column_spec} "
                    f"but '{name}' has {cs.column_spec}."
                )
        elif set(cols) != ref_set:
            raise ValueError(
                f"for_columns inputs must iterate over the same columns "
                f"(zipped by name). '{ref_name}' resolves to "
                f"{sorted(ref_set)} but '{name}' resolves to {sorted(cols)}."
            )

    if output_names is not None:
        mode, groups = resolve_column_groups(
            resolved_cols[ref_name],
            ref_cs.groups,
            output_names,
            every_column=ref_cs.groups is None and not ref_cs.columns,
        )
        Log.debug(f"for_columns: {mode} over {len(groups)} group(s)")

    # Rebuild inputs with concrete, iterate-mode ColumnSelections.
    new_inputs = dict(inputs)
    for name, cs in iterate_params.items():
        spec_cols = cs.column_spec if cs.groups is not None else resolved_cols[name]
        new_cs = ColumnSelection(cs.data, spec_cols, iterate=True)
        spec = inputs[name]
        if isinstance(spec, Fixed):
            new_inputs[name] = Fixed(new_cs, **spec.fixed_metadata)
        else:
            new_inputs[name] = new_cs
    return new_inputs


def _load_var_type_as_spread(
    var_type: Any,
    db: Any | None,
    where: Any | None,
    branch_params_filter: dict | None = None,
    use_default: bool = False,
) -> "pd.DataFrame":
    """Bulk load all records for a variable type into a spread DataFrame.

    ``use_default`` applies the variable's pinned default when no variant is
    named (``variant_pins.default_load_args``). It is False here and True only
    from ``_load_input``, so the for_each INPUT path gets the default and
    internal callers (``for_columns`` column discovery) see every variant.

    Routes to ``db.load_all_as_df`` (the fast bulk engine) when a database is
    available.  Falls back to the iterator-based slow path for types that the
    fast engine cannot handle (custom serialisation, subclass overrides, etc.).

    The returned DataFrame has ``__record_id`` and ``__branch_params`` columns
    plus one column per schema/version key and per data column (spread layout).
    """
    import pandas as pd

    _vt_name = getattr(var_type, "__name__", type(var_type).__name__)
    _t0 = time.perf_counter()

    # Resolve the database instance.
    resolved_db = db
    if resolved_db is None:
        resolved_db = database_or_none()

    if resolved_db is not None and hasattr(resolved_db, "load_all_as_df"):
        if use_default:
            # An input naming no variant gets the pinned default. One owner
            # for what that is; it also logs which pin(s) it came from.
            from scidb.store.variant_pins import default_load_args

            branch_params_filter, _ = default_load_args(
                resolved_db, var_type, branch_params_filter, "latest"
            )
        # Fast path: bulk engine with spread layout.
        where_kw = {"where": where} if where is not None else {}
        bp_kw = (
            {"branch_params_filter": branch_params_filter}
            if branch_params_filter
            else {}
        )
        if branch_params_filter:
            Log.debug(
                f"[Variant] _load_var_type_as_spread({_vt_name}): applying "
                f"branch_params_filter={branch_params_filter}"
            )
        # A CODE or RUN-OPTIONS pin has to see superseded records, so it loads
        # uncollapsed. The rule — and why — is `variant.pin_loads_uncollapsed`,
        # shared with `provenance_query.records_for_variant` so a pin traced
        # in the inspector names the records this loader would feed a function.
        # Scoped to exactly this case, so an unpinned load keeps today's
        # behaviour byte for byte.

        has_code_pin = pin_loads_uncollapsed(branch_params_filter)
        if has_code_pin:
            Log.info(
                f"[Variant] {_vt_name}: code/run-options pin present — loading "
                f"uncollapsed (version_id='all') so superseded versions remain "
                f"selectable"
            )
        result = resolved_db.load_all_as_df(
            var_type,
            layout="spread",
            include_rid=True,
            include_bp=True,
            stringify_schema=True,
            version_id="all" if has_code_pin else "latest",
            **where_kw,
            **bp_kw,
        )
        Log.info(
            f"[timing] _load_var_type_as_spread({_vt_name}): "
            f"{len(result)} rows x {len(result.columns) if not result.empty else 0} cols "
            f"in {time.perf_counter() - _t0:.3f}s (fast path)"
        )
        return result

    # Slow fallback: use iterator (no database or database lacks load_all_as_df).
    # BaseVariable.load derives branch_params_filter from its non-schema metadata
    # kwargs (version="latest" path), so pass the pinned branch_params as kwargs.
    db_kwargs = {"db": db} if db is not None else {}
    where_kwargs = {"where": where} if where is not None else {}
    bp_kwargs = dict(branch_params_filter) if branch_params_filter else {}

    _raw = var_type.load(version="latest", **db_kwargs, **where_kwargs, **bp_kwargs)
    loaded = _raw if isinstance(_raw, list) else [_raw]

    if not loaded:
        return pd.DataFrame()

    _schema_keys: set = set()
    if resolved_db is not None and hasattr(resolved_db, "dataset_schema_keys"):
        _schema_keys = set(resolved_db.dataset_schema_keys)

    def _stringify_meta(meta: dict) -> dict:
        const_keys: set = set()
        constants_val = meta.get("__constants")
        if constants_val:
            try:
                if isinstance(constants_val, dict):
                    const_keys = set(constants_val.keys())
                else:
                    const_keys = set(json.loads(constants_val).keys())
            except Exception:
                pass
        return {
            k: schema_str(v) if k in _schema_keys and v is not None else v
            for k, v in meta.items()
            if not k.startswith("__") and k not in const_keys
        }

    first = loaded[0]
    all_have_data = all(hasattr(v, "data") for v in loaded)

    if all_have_data and isinstance(first.data, pd.DataFrame):
        all_data = []
        all_meta_rows = []
        for var in loaded:
            data_df = var.data
            meta = _stringify_meta(
                dict(var.metadata) if hasattr(var, "metadata") and var.metadata else {}
            )
            meta["__record_id"] = getattr(var, "record_id", None)
            meta["__branch_params"] = json.dumps(
                getattr(var, "branch_params", None) or {}
            )
            nr = len(data_df)
            for _ in range(nr):
                all_meta_rows.append(meta)
            all_data.append(data_df.reset_index(drop=True))

        if all_meta_rows:
            combined_meta_df = pd.DataFrame(all_meta_rows)
            combined_data_df = pd.concat(all_data, ignore_index=True)
            result = pd.concat(
                [
                    combined_meta_df.reset_index(drop=True),
                    combined_data_df.reset_index(drop=True),
                ],
                axis=1,
            )
        else:
            result = pd.DataFrame()
    elif all_have_data:
        view_name = (
            var_type.view_name()
            if hasattr(var_type, "view_name")
            else getattr(var_type, "__name__", type(var_type).__name__)
        )
        all_data = []
        all_meta_rows = []
        for var in loaded:
            # Use _to_dataframe so scalars/arrays/lists expand into proper rows
            # (consistent with PerComboLoaderMerge and the DataFrame branch above)
            part_df = _to_dataframe(var.data, view_name)
            meta = _stringify_meta(
                dict(var.metadata) if hasattr(var, "metadata") and var.metadata else {}
            )
            meta["__record_id"] = getattr(var, "record_id", None)
            meta["__branch_params"] = json.dumps(
                getattr(var, "branch_params", None) or {}
            )
            nr = len(part_df)
            for _ in range(nr):
                all_meta_rows.append(dict(meta))
            all_data.append(part_df.reset_index(drop=True))
        if all_meta_rows:
            combined_meta_df = pd.DataFrame(all_meta_rows)
            combined_data_df = pd.concat(all_data, ignore_index=True)
            result = pd.concat(
                [
                    combined_meta_df.reset_index(drop=True),
                    combined_data_df.reset_index(drop=True),
                ],
                axis=1,
            )
        else:
            result = pd.DataFrame()
    else:
        var_name = getattr(var_type, "__name__", type(var_type).__name__)
        rows = []
        for var in loaded:
            rows.append(
                {
                    var_name: var,
                    "__record_id": getattr(var, "record_id", None),
                    "__branch_params": "{}",
                }
            )
        result = pd.DataFrame(rows)

    Log.info(
        f"[timing] _load_var_type_as_spread({_vt_name}): "
        f"{len(result)} rows (slow fallback) in {time.perf_counter() - _t0:.3f}s"
    )
    return result
