"""for_each's shared state types and small helpers: the per-run state, dry-run/filter wrappers, per-combo resolution and the general helpers every phase uses.

Moved verbatim out of foreach.py (grug follow-up, docs/claude/grugbrain-principles.md).
"""

import scifor as _scifor
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path as _Path
from scidb.model.filters import Filter
from scidb.model.input_spec import type_name
from scidb.model.log import Log
from scidb.run.bindings import InputBinding
from scidb.run.bindings import InputKind
from scidb.run.bindings import RunBindings
from scidb.run.foreach_config import RunOptions
from scidb.run.per_combo import PerComboLoader
from scidb.run.per_combo import PerComboLoaderMerge
from scidb.store.database import database_or_none
from scidb.store.database import dataset_schema_keys_of
from scifor import ColumnSelection
from scifor import Fixed
from scifor.column_selection import column_value as _column_value
from scifor.pathinput import PathInput
from typing import Any

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager
    import pandas as pd


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.model.exceptions import SchemaKeyTypeError
from scidb.model.filters import split_schema_key_filters
from scidb.provenance.graph import insert_function_sources
from scidb.provenance.save import compute_input_selectors
from scidb.run.foreach_config import function_sources_for
from scidb.store.database import get_database
from scifor.failures import as_user_value_error
from scifor.failures import describe_value_kinds
from scifor.merge import drop_constant_keys
from scifor.merge import merge_frames
class _DryRunMerge(_scifor.Merge):
    """scifor.Merge subclass used only for dry_run display.

    Has the correct ``__name__`` from the scidb.Merge spec so scifor
    prints ``Merge(GaitData, ForceData)`` instead of a repr string.
    """

    def __init__(self, scidb_merge):
        # Do NOT call super().__init__ — bypass validation for display only
        import pandas as pd

        self._dry_name = scidb_merge.__name__
        # scifor loops over self.tables in _print_dry_run_iteration
        self.tables = [pd.DataFrame() for _ in scidb_merge.tables]

    @property
    def __name__(self) -> str:  # type: ignore[override]
        return self._dry_name


class _PreresolvedFilter(Filter):
    """Wraps a pre-computed set of schema_ids for constituent loading.

    Used in the Merge path so each constituent receives already-resolved,
    already-validated schema_ids directly — no second DB query or coverage
    check needed.

    Also carries the **variable-level portion** of the merge-level where= filter
    (``variable_filter``). ``_load_with_where`` uses it for *semantic* variant
    matching — selecting the single computed variant whose consumed input
    schema_id set the filter describes — exactly as a direct ``.load(where=...)``
    would (§10 "where= redesign"). Constituents with no producing invocation
    (raw/direct-saved data) fall back to their own schema location, restricted to
    the pre-resolved ``schema_ids``.

    ``_schema_ids`` is the **row restriction**: it already encodes the full where=
    filter (variable-level AND any SchemaKey portion), applied on top of the
    variant match via the ``_restrict_to_resolved_ids`` marker so a constituent
    that matches a variant by provenance is still narrowed to the selected rows.
    """

    # Tells DatabaseManager._load_with_where to intersect the variant-matched
    # records with resolve() (the pre-resolved row set) — see class docstring.
    _restrict_to_resolved_ids = True

    def __init__(self, schema_ids: set, variable_filter: "Filter | None" = None):
        self._schema_ids = schema_ids
        self._variable_filter = variable_filter

    def to_key(self) -> str:
        vf = self._variable_filter
        return vf.to_key() if vf is not None else ""

    def resolve(
        self, db, target_variable_class, target_table_name, validate_coverage=True
    ) -> set:
        return self._schema_ids


def _merge_constituent_variable_filter(where: Any) -> "Filter | None":
    """The variable-level portion of a merge-level where= filter (or None).

    Splits off any SchemaKey portion (which selects rows, not variants) and keeps
    the variable-level portion only — the part that semantically identifies which
    computed variant to return — mirroring what ``_load_with_where`` does for a
    direct ``.load(where=...)`` so Merge constituents and direct loads select the
    same variant.
    """

    if isinstance(where, Filter):
        sk_filter, var_filter = split_schema_key_filters(where)
        if sk_filter is not None:
            return var_filter  # None when where is purely SchemaKey
    return where if isinstance(where, Filter) else None


# ---------------------------------------------------------------------------
# Prepared state shared between the prepare / loop / save phases
# ---------------------------------------------------------------------------


@dataclass
class _ForEachState:
    """Everything ``_for_each_prepare`` computes that the inner loop and
    ``_for_each_save_resolved`` consume. Holding it in one place lets the
    same prepare code service both the Python-driven path (the existing
    ``for_each`` orchestration) and the MATLAB-driven bridge entry that
    runs the loop in MATLAB's ``scifor.for_each``.

    Fields are populated by ``_for_each_prepare`` in the order they appear
    in the pre-loop steps; consumers should treat the dataclass as
    read-only once returned.
    """

    fn_name: str
    config_keys: dict
    call_id: str
    output_names: list
    loaded_inputs: dict
    full_combos: list
    extended_metadata_iterables: dict
    aggregation_mode: bool
    current_schema_keys: list
    # What KIND of call this is, decided before prepare and read by the save:
    # a @scistack(generates_file=True) side-effect function saves lineage-only
    # (no data row), and a plot_/stat_ endpoint stamps its artifact. Both were
    # threaded through two signatures as keyword arguments that defaulted to
    # "no" — so a caller that forgot one silently saved the wrong kind.
    generates_file: bool = False
    endpoint_kind: "str | None" = None
    # PathOutput branch_param placeholder keys injected into combos (stripped
    # from the result table before save/return; see _for_each_save_resolved).
    path_extra_keys: Any = None  # set | None
    # Combos removed by the pre-combo hook (skip_computed): already up to
    # date, so they never reach scifor. Folded into the run-summary log line.
    skip_computed_count: int = 0
    # {param_name: [data column names]} for inputs whose records are dicts
    # stored one column per key. scifor rebuilds the mapping per combo; see
    # _resolve_mapping_inputs.
    mapping_inputs: Any = None  # dict | None
    # The inputs spec as prepare left it — identical to the caller's dict
    # except that a Parameter-fed glue chain has been folded into its values
    # (scidb.run.glue.apply_constant_glue) — what the function receives. The save
    # path reads it for selectors and pins (unaffected by constant glue);
    # ForEachConfig hashed this dict with each glued constant put back to its
    # RAW value (_build_call_identity), since the glue is a history hop.
    inputs: Any = None  # dict | None
    # {param_name: [GlueSpec]} the caller attached to this call MINUS anything
    # already applied during prepare — i.e. Parameter-fed chains, which run
    # before fan-out (scidb.run.glue.apply_constant_glue). This is the set MATLAB
    # is told to apply, so leaving those in would run them twice.
    # per_combo_glue is the further subset that must run post-slice rather
    # than on the bulk loaded table (the D4 per-schema-key opt-in, plus
    # whole-table glue on a per-combo input). See scidb.run.glue.
    glue_chains: Any = None  # dict[str, list[GlueSpec]] | None
    per_combo_glue: Any = None  # dict[str, list[GlueSpec]] | None
    # Every input's kind, selector, pinned rid, pooled records and branch
    # params after Step 12 — the typed spine (scidb.run.bindings.RunBindings).
    # `rids_for_combo` is the ONE answer to "what did this call consume" and
    # `for_combo` types it as the graph's edges.
    #
    # It REPLACED three fields that held slices of the same thing:
    # `rid_to_bp` (now `bindings.rid_to_bp`), `fixed_rid_values` (now
    # `bindings.pinned_rids`) and `rid_keys` (now `bindings.tracked_params`).
    # Keeping them beside it is how the Fixed-on-aggregation edge went
    # missing: two containers, one fact, and the save read the wrong one.
    #
    # NEVER None: the save path resolves a late Fixed pin by calling
    # `.pin()` on it, and a `state.bindings or RunBindings()` dance at each
    # reader would pin into a throwaway that the next reader never sees.
    bindings: RunBindings = field(default_factory=RunBindings)
    # {param: (chain_hash, input_set_signature, {source_rid: virtual_rid})}.
    # The provenance half: the consumer's bindings already point at the virtual
    # rids (fuse_glue rewrote __record_id), and the save path writes the
    # matching _record/_invocation/edge rows from this.
    glue_virtual: Any = None  # dict | None
    # The function object itself, carried purely so the save path can capture
    # its SOURCE against the hash it is storing (see _save_results). `fn_name`
    # above is not enough: source has to come from the same object
    # `to_config_keys` hashed, or it lands under a key nothing was stored
    # beneath. Never used for dispatch — the call already happened by then.
    fn: Any = None  # Callable | None
    # {argument: declared Parameter name} (scidb.model.parameter.
    # declared_input_names), carried to record_run so each constant edge
    # names the Parameter the canvas shows. Descriptive, never identity.
    parameter_names: Any = None  # dict[str, str] | None
    # {argument: PathInput.to_spec()} (scidb.model.parameter.path_input_specs_of),
    # carried to record_run so each PathInput record stores WHERE the files
    # were. Descriptive, never identity (identity is the name, `to_key`).
    path_input_specs: Any = None  # dict[str, str] | None
    # The folder rootless PathInputs resolve against, decided once in prepare
    # (bound into `inputs`) and handed to scifor's loop so it does not re-decide.
    project_root: "_Path | None" = None
    # The identity-bearing run options (as_table, distribute) the call ran
    # under; the execute phase hands them to scifor.
    options: RunOptions = field(default_factory=RunOptions)
    # The output VARIABLE TYPES (not result columns), in outputs= order: what
    # a stored run outcome (scidb.provenance.outcomes) is filed under.
    output_types: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Per-combo resolution helpers
# ---------------------------------------------------------------------------


def pathinput_numeric_eligible(keys: "list[str]", key_types: dict) -> "list[str]":
    """THE PathInput key-type policy, half 1 (MATLAB calls it too, through
    +scidb/+internal/load_pathinput_checked.m): the keys whose value may match
    a differently-spelled filename by number. A ``"string"``-declared key is
    never eligible — spelling is identity, "1" never matches "001"."""
    string_keys = {k for k, t in (key_types or {}).items() if t == "string"}
    return [k for k in keys if k not in string_keys]


def pathinput_key_type_offender(
    resolutions: dict, key_types: dict, schema_keys: "list | set"
) -> "tuple[str, str] | None":
    """THE PathInput key-type policy, half 2: the first ``(key, on-disk
    spelling)`` whose bridged spelling makes the dataset's identity ambiguous
    — an UNDECLARED schema key (a ``"numeric"``-declared one is canonical;
    a non-schema key's spelling defines nothing). ``None`` when there is none."""
    numeric_keys = {k for k, t in (key_types or {}).items() if t == "numeric"}
    schema = set(schema_keys or [])
    for key, spelling in dict(resolutions or {}).items():
        if key in schema and key not in numeric_keys:
            return str(key), str(spelling)
    return None


def _load_pathinput_checked(
    pi: "PathInput",
    load_kw: dict,
    key_types: dict,
    schema_keys: "list | set",
):
    """Per-combo PathInput load enforcing declared schema-key types.

    - ``"string"``-declared keys are excluded from the numeric-equivalence
      fallback entirely: spelling is identity, so "1" never matches "001".
    - ``"numeric"``-declared keys may resolve freely (their stored identity
      is already canonical; only the filename lookup bridges spellings).
    - An UNDECLARED schema key whose spelling had to be bridged raises
      SchemaKeyTypeError: the dataset has proven the spelling ambiguous, so
      the user must declare the key's type once.  Non-schema keys keep the
      silent fallback (their spelling does not define dataset identity).
    """

    key_types = key_types or {}
    eligible = set(pathinput_numeric_eligible(list(load_kw), key_types))
    # Numeric bridges only: an alias spelling ("Baseline" for canonical "BL")
    # is a DECLARED equivalence, never the ambiguity this policy guards. scifor
    # owns which kind each bridge was.
    path, resolutions = pi.load_with_captures(
        load_kw, numeric_match=eligible, report="numeric"
    )
    if resolutions:
        offender = pathinput_key_type_offender(resolutions, key_types, schema_keys)
        if offender:
            key, spelling = offender
            raise SchemaKeyTypeError(
                f"PathInput resolved {key}={load_kw[key]!r} to '{spelling}' "
                f"on disk (template {pi.path_template!r}) — the spelling of "
                f"schema key '{key}' is ambiguous (zero-padded filenames). "
                f"Declare its type once to fix its identity (it is stored in "
                f"the database): tick 'Numeric' for '{key}' in the GUI, or "
                f"db.set_schema_key_type('{key}', 'numeric'), to treat values "
                f"as numbers (canonical, no leading zeros); or 'string' to "
                f"make spelling significant (exact matches only)."
            )
        Log.debug(f"pathinput resolved spellings (declared numeric): {resolutions}")
    return path


def _resolve_per_combo_loader(
    pcl: "PerComboLoader",
    load_kw: dict,
    key_types: "dict | None" = None,
    schema_keys: "list | None" = None,
) -> Any:
    """Resolve a PerComboLoader per-combo by calling spec.load(**effective_kw)."""
    spec = pcl.spec

    if isinstance(spec, Fixed):
        effective_kw = {**load_kw, **spec.fixed_metadata}
        inner = spec.data
        columns = None
        if isinstance(inner, ColumnSelection):
            columns = inner.columns
            inner = inner.data
        if isinstance(inner, PathInput):
            return _load_pathinput_checked(
                inner, effective_kw, key_types or {}, schema_keys or []
            )
        lv = inner.load(**effective_kw)
        raw = lv.data if hasattr(lv, "data") else lv
        if columns:
            cls_name = getattr(inner, "__name__", type(inner).__name__)
            raw = _apply_per_combo_col_selection(raw, columns, cls_name)
        return raw

    if isinstance(spec, ColumnSelection):
        lv = spec.data.load(**load_kw)
        raw = lv.data if hasattr(lv, "data") else lv
        cls_name = getattr(spec.data, "__name__", type(spec.data).__name__)
        return _apply_per_combo_col_selection(raw, spec.columns, cls_name)

    # Note: a bare PathInput never reaches this function anymore -- since
    # input_spec.is_loadable excludes it, _convert_inputs passes it through as a
    # constant and its per-combo resolution happens inside scifor's
    # for_each loop (via LoopHooks.path_input_resolver), not here. Only
    # Fixed(PathInput(...)) still routes through PerComboLoader (the
    # isinstance(inner, PathInput) branch above).

    # Plain class
    lv = spec.load(**load_kw)
    return lv.data if hasattr(lv, "data") else lv


def _resolve_per_combo_merge(
    pcl_merge: "PerComboLoaderMerge", load_kw: dict, schema_keys=()
) -> "pd.DataFrame":
    """Resolve a PerComboLoaderMerge per-combo by loading each constituent,
    then combine them by THE Merge rule (``scifor.merge``): constant schema-key
    columns drop, shared varying ones join, a shared data column errors."""

    parts = []
    labels: list[str] = []
    for spec in pcl_merge.merge_spec.tables:
        effective_kw = dict(load_kw)
        columns = None
        actual_spec = spec

        # Unwrap Fixed
        if isinstance(actual_spec, Fixed):
            effective_kw = {**load_kw, **actual_spec.fixed_metadata}
            actual_spec = actual_spec.data

        # Unwrap ColumnSelection
        if isinstance(actual_spec, ColumnSelection):
            columns = actual_spec.columns
            actual_spec = actual_spec.data

        # Load the variable
        lv = actual_spec.load(**effective_kw)
        cls_name = getattr(actual_spec, "__name__", type(actual_spec).__name__)
        if isinstance(lv, list):
            raise ValueError(
                f"{cls_name}.load() returned multiple results (list), expected exactly 1."
            )
        raw = lv.data if hasattr(lv, "data") else lv

        # Convert to DataFrame
        part_df = _to_dataframe(raw, cls_name)

        # Apply column selection
        if columns:
            missing = [c for c in columns if c not in part_df.columns]
            if missing:
                raise KeyError(
                    f"Columns {missing} not found in {cls_name}. "
                    f"Available: {list(part_df.columns)}"
                )
            if len(columns) == 1:
                part_df = part_df[[columns[0]]]
            else:
                part_df = part_df[columns]

        part_df, _dropped = drop_constant_keys(part_df, schema_keys)
        parts.append(part_df)
        labels.append(cls_name)

    return merge_frames(parts, list(schema_keys), labels=labels)


def _to_dataframe(data: Any, cls_name: str) -> "pd.DataFrame":
    """Convert raw data (scalar, array, list, DataFrame) to a named DataFrame."""
    import numpy as np
    import pandas as pd

    if isinstance(data, pd.DataFrame):
        return data.reset_index(drop=True)
    if isinstance(data, np.ndarray):
        if data.ndim == 1:
            return pd.DataFrame({cls_name: data})
        elif data.ndim == 2:
            cols = [f"{cls_name}_{i}" for i in range(data.shape[1])]
            return pd.DataFrame(data, columns=cols)
        else:
            raise ValueError(
                f"Cannot convert {data.ndim}D array from {cls_name} to DataFrame"
            )
    if isinstance(data, (list, tuple)):
        return pd.DataFrame({cls_name: list(data)})
    # Scalar
    return pd.DataFrame({cls_name: [data]})


def _apply_per_combo_col_selection(raw: Any, columns: list, cls_name: str) -> Any:
    """Apply column selection to raw data: one column as scifor delivers it
    (``column_value``: the cell for one row, else an array), several as a
    DataFrame."""
    df = _to_dataframe(raw, cls_name)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(
            f"Columns {missing} not found in {cls_name}. Available: {list(df.columns)}"
        )
    if len(columns) == 1:
        return _column_value(df[columns[0]])
    return df[columns]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _capture_function_source(
    db: "DatabaseManager", fn: Any, fn_name: str, stored_hash: "str | None"
) -> None:
    """Store the SOURCE behind this run's function_hash. Once per run, not per
    record: every record of a run shares one hash.

    Write-time is the only time this is possible. The moment the file is
    edited the old body is gone from disk, so a version whose source was not
    captured as it ran can never be recovered — lazy capture "on first edit"
    would miss the baseline version, which is precisely the one a comparison
    needs. See docs/claude/variant-selection.md §3.

    Reading the source back is best-effort (logged, never raised): losing it
    costs traceability, losing the run costs the user's work. Writing it is
    not: it runs inside the call's save transaction, so a failed INSERT
    raises and fails the save like any other write.
    """

    if fn is None or not stored_hash:
        return
    try:
        derived_hash, entry, units = function_sources_for(fn)
    except Exception as e:
        Log.warn(
            f"[provenance] source capture failed for fn={fn_name}: "
            f"{type(e).__name__}: {e}"
        )
        return
    if not units:
        # A digest with no text: a MATLAB run whose .m text did not reach the
        # bridge (a hash override, or text that did not re-hash to the digest
        # — the bridge warns then).
        Log.debug(f"[provenance] no source captured for fn={fn_name}")
        return
    if derived_hash != stored_hash:
        # Filing source under the wrong key would be worse than not storing
        # it: it reads as captured and returns the wrong code. Refuse, and say
        # so loudly enough to be findable.
        Log.warn(
            f"[provenance] source NOT captured for fn={fn_name}: derived hash "
            f"{derived_hash[:12]} != stored {stored_hash[:12]}; the two recipes "
            f"have drifted"
        )
        return
    written = insert_function_sources(db._duck, stored_hash, units, entry_name=entry)
    Log.info(
        f"[provenance] captured {written} source unit(s) for fn={fn_name} @ "
        f"{stored_hash[:12]}"
    )


def _report_no_output_skips(
    no_output_reasons: "dict[int, list[str]]",
    batch_items: dict,
    outputs: "list[Any]",
    db: Any | None,
) -> None:
    """One line per output that was skipped at some combinations, and an
    empty table for one that saved NOTHING this run.

    The empty table (``DatabaseManager.ensure_variable_table``) is what lets
    SQL on the variable's view return no rows instead of "does not exist";
    scidb's own loads already answer from the record tables either way
    (Stage 0 of .claude/plan-for-columns-groups.md). The first real save
    rebuilds it with its columns (``_check_column_set``).
    """
    if not no_output_reasons:
        return
    saved_slots = {idx for (idx, _path), items in batch_items.items() if items}
    for output_idx, reasons in no_output_reasons.items():
        output_obj = outputs[output_idx]
        label = _output_type_label(output_obj)
        counts = ", ".join(
            f"{reasons.count(r)} {r.replace('_', ' ')}"
            for r in dict.fromkeys(reasons)
        )
        if output_idx in saved_slots:
            Log.info(f"[save] {label}: skipped at {len(reasons)} combination(s) ({counts})")
            continue
        out_cls = output_obj if isinstance(output_obj, type) else type(output_obj)
        active_db = db
        if active_db is None:
            active_db = get_database()
        created = active_db.ensure_variable_table(out_cls)
        Log.info(
            f"[save] {label}: 0 records this run ({counts})"
            + ("; created empty table" if created else "")
        )


def _save_seam_error(
    exc: BaseException, fn_name: str, state: "_ForEachState", output_obj: Any, items: list
) -> BaseException:
    """A save failure as the save seam sees it: an ``ActionableError`` stays
    as it is (``ColumnSetChangedError`` already says what to change);
    anything else is most likely the values the function returned, so it
    becomes a ``UserValueError`` — precise when the batch's values differ in
    kind across combos, hedged otherwise (``scifor.failures.as_user_value_error``).
    """

    output = _output_type_label(output_obj)

    def _diagnose() -> "str | None":
        labelled = [
            (
                ", ".join(f"{k}={v}" for k, v in meta.items() if not str(k).startswith("__"))
                or "the only combo",
                data,
            )
            for data, meta in items
        ]
        return describe_value_kinds(labelled, fn_name=fn_name, output=output)

    return as_user_value_error(
        exc,
        stage="save",
        fn_name=fn_name,
        fn=getattr(state, "fn", None),
        output=output,
        diagnose=_diagnose,
    )


def _get_schema_keys(db: Any | None) -> set:
    """Return the set of dataset_schema_keys from db or the global database."""
    return set(dataset_schema_keys_of(db))


def _has_pathinput(inputs: dict) -> bool:
    """Check if any input is a PathInput, directly or wrapped in Fixed."""
    for v in inputs.values():
        if isinstance(v, PathInput):
            return True
        if isinstance(v, Fixed) and isinstance(v.data, PathInput):
            return True
    return False


def _describe_save_data(val) -> str:
    """Compact description of data being saved."""
    import numpy as np
    import pandas as pd

    if isinstance(val, pd.DataFrame):
        return f"DataFrame {val.shape[0]}x{val.shape[1]}"
    if isinstance(val, np.ndarray):
        return f"ndarray shape={val.shape}"
    if isinstance(val, dict):
        return f"dict, {len(val)} keys"
    if isinstance(val, (list, tuple)):
        return f"{type(val).__name__} len={len(val)}"
    return type(val).__name__


def _output_type_label(output_obj: Any) -> str:
    """The variable type an output saves as, for log lines.

    Deliberately NOT the result column (``scifor.output_column_name``): a
    ``column_name`` override renames the column, not the type being saved.
    """
    if hasattr(output_obj, "view_name"):
        return output_obj.view_name()
    return getattr(output_obj, "__name__", type(output_obj).__name__)


def _propagate_schema(db, distribute: bool) -> None:
    """Propagate dataset_schema_keys from the db into scifor.set_schema()."""
    # If a db was passed explicitly and has schema keys, use them.
    if db is not None and hasattr(db, "dataset_schema_keys"):
        _scifor.set_schema(list(db.dataset_schema_keys))
        return

    # No explicit db: the global one, or None only when none is configured.
    _global_db = database_or_none()

    if _global_db is not None and hasattr(_global_db, "dataset_schema_keys"):
        _scifor.set_schema(list(_global_db.dataset_schema_keys))
    elif distribute:
        raise ValueError(
            "distribute=True requires access to dataset_schema_keys, "
            "but no database is available. Either pass db= to for_each or "
            "call configure_database() first."
        )


def _active_database():
    """The configured database, or ``None`` — for callers holding ``db=None``
    that still need a lookup (the Fixed-rid fallback at save)."""
    return database_or_none()


def _build_run_bindings(
    inputs: dict,
    *,
    tracked_params: list,
    fixed_rid_values: dict,
    colsel_params: list,
    aggregation_mode: bool,
    pools: dict | None = None,
    iterated_keys: list | None = None,
    rid_to_bp: dict | None = None,
) -> RunBindings:
    """Step 11's classification of every input, as one typed structure.

    The kind is decided HERE, once, from what Step 12 already sorted the
    input into — an iteration rid key, a pinned rid, a lineage-only column
    selection — and read everywhere else (`scidb.run.bindings.InputKind`).
    Under aggregation every rid-tracked input is AGGREGATED and carries its
    `RecordPool` (records per iterated location, per variant group).
    """

    selectors = compute_input_selectors(inputs)
    iterate_params = set(tracked_params or [])
    pools = pools or {}
    run = RunBindings(rid_to_bp=dict(rid_to_bp or {}), iterated_keys=tuple(iterated_keys or ()))
    for param, spec in inputs.items():
        if param in fixed_rid_values or hasattr(spec, "fixed_metadata"):
            # PINNED even when Step 12 could not settle the pin on one row:
            # the save path looks the rid up in the database then.
            kind = InputKind.PINNED
        elif param in colsel_params:
            kind = InputKind.AGGREGATED if aggregation_mode else InputKind.LINEAGE_ONLY
        elif param in iterate_params:
            kind = InputKind.AGGREGATED if aggregation_mode else InputKind.ITERATE
        else:
            continue  # a constant, a PathInput, a marker — not a record-bearing input
        run.inputs[param] = InputBinding(
            param=param,
            kind=kind,
            type_name=type_name(spec),
            selector=selectors.get(param),
            pinned_rid=fixed_rid_values.get(param),
            pool=pools.get(param) if kind == InputKind.AGGREGATED else None,
        )
    Log.debug(
        "run bindings: "
        + ", ".join(
            f"{p}={b.kind.value}"
            + (f"[{'split' if b.splits else 'pooled'}]" if b.pool is not None else "")
            for p, b in run.inputs.items()
        )
    )
    return run
