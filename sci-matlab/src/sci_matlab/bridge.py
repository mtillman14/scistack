"""Python bridge for MATLAB-SciStack integration.

Provides proxy classes that satisfy the duck-typing contracts of
scilineage's LineageFcn, LineageFcnInvocation, and LineageFcnResult classes.
This allows MATLAB functions to participate fully in the lineage / caching
system without any changes to existing Python packages.

The key insight is that every Python function that touches these objects
uses duck-typing (attribute access), not isinstance checks on LineageFcn or
LineageFcnInvocation.  LineageFcnResult *is* instantiated directly from
scilineage, so isinstance checks in save_variable() pass naturally.

Duck-typing contracts satisfied
-------------------------------
MatlabLineageFcn provides:
    .hash            str   (64-char hex, same algorithm as LineageFcn.__init__)
    .fcn.__name__    str   (used by extract_lineage)
    .unpack_output   bool
    .unwrap          bool
    .invocations     tuple

MatlabLineageFcnInvocation provides:
    .fcn             MatlabLineageFcn
    .inputs          dict[str, Any]
    .outputs         tuple
    .unwrap          bool
    .hash            property -> compute_lineage_hash()
    .compute_lineage_hash()  str  (reuses classify_inputs from scilineage)
"""

from hashlib import sha256

from scilineage.inputs import classify_inputs

STRING_REPR_DELIMITER = "-"


def _describe_value(val):
    """Return a short type/shape string for logging."""
    import numpy as np
    import pandas as pd

    t = type(val).__name__
    if isinstance(val, pd.DataFrame):
        return f"DataFrame {val.shape[0]}x{val.shape[1]} cols={list(val.columns)}"
    if isinstance(val, np.ndarray):
        return f"ndarray shape={val.shape} dtype={val.dtype}"
    if isinstance(val, (list, tuple)):
        return f"{t} len={len(val)}"
    if isinstance(val, (int, float, str, bool)):
        return f"{t}"
    return f"{t}"


# ---------------------------------------------------------------------------
# Proxy classes
# ---------------------------------------------------------------------------


class _FunctionProxy:
    """Minimal proxy so that ``inv.fcn.fcn.__name__`` works in extract_lineage."""

    def __init__(self, name: str):
        self.__name__ = name


class MatlabLineageFcn:
    """Proxy for a MATLAB function in the scilineage system.

    Satisfies the same duck-typing contract as ``scilineage.core.LineageFcn``
    for every consumer that reads ``.hash``, ``.fcn.__name__``, etc.

    Parameters
    ----------
    source_hash : str
        SHA-256 hex digest of the MATLAB function source code.
    function_name : str
        Human-readable function name (used in lineage records).
    unpack_output : bool
        Whether the function returns multiple outputs.
    """

    def __init__(
        self,
        source_hash: str,
        function_name: str,
        unpack_output: bool = False,
    ):
        self.fcn = _FunctionProxy(function_name)
        self.unpack_output = unpack_output
        self.unwrap = True
        self.invocations: tuple = ()

        # Same algorithm as LineageFcn.__init__
        string_repr = f"{source_hash}{STRING_REPR_DELIMITER}{unpack_output}"
        self.hash: str = sha256(string_repr.encode()).hexdigest()
        self.generates_file = False


class MatlabLineageFcnInvocation:
    """Proxy for a specific MATLAB function invocation.

    Satisfies the same duck-typing contract as
    ``scilineage.core.LineageFcnInvocation``. Reuses ``classify_inputs``
    and the lineage-hash algorithm from scilineage so that cache lookups
    and lineage extraction work unchanged.

    Parameters
    ----------
    matlab_lineage_fcn : MatlabLineageFcn
        The parent lineage function (function identity).
    inputs : dict
        Mapping of argument names (``"arg_0"``, ``"arg_1"``, ...) to
        Python-side values (BaseVariable instances, LineageFcnResults, or
        plain scalars/arrays).
    """

    def __init__(self, matlab_lineage_fcn: MatlabLineageFcn, inputs: dict):
        self.fcn = matlab_lineage_fcn
        self.inputs: dict = dict(inputs)
        self.outputs: tuple = ()
        self.unwrap = True

    def compute_lineage_hash(self) -> str:
        """Compute lineage hash — identical algorithm to LineageFcnInvocation."""
        classified = classify_inputs(self.inputs)
        input_tuples = [c.to_cache_tuple() for c in classified]
        hash_input = f"{self.fcn.hash}{STRING_REPR_DELIMITER}{input_tuples}"
        return sha256(hash_input.encode()).hexdigest()

    @property
    def hash(self) -> str:
        return self.compute_lineage_hash()


# ---------------------------------------------------------------------------
# Helper functions called from MATLAB
# ---------------------------------------------------------------------------


def _reconstruct_input_for_keys(spec):
    """Reconstruct one MATLAB-described input as the Python wrapper that
    ``ForEachConfig._serialize_inputs`` knows how to format.

    ``spec`` is one of:
        - {"kind": "constant", "value": Any}
        - {"kind": "var_type", "type_name": str}
        - {"kind": "column_selection", "type_name": str, "columns": list[str]}
        - {"kind": "fixed", "inner": <spec>, "fixed_metadata": dict}
        - {"kind": "merge", "specs": list[<spec>]}
        - {"kind": "pathinput", "template": str, "root_folder": str}

    Anything else is returned unchanged (treated as a constant value).
    """
    if not isinstance(spec, dict) or "kind" not in spec:
        return spec
    kind = spec["kind"]
    if kind == "constant":
        return spec["value"]
    if kind == "var_type":
        return get_surrogate_class(spec["type_name"])
    if kind == "column_selection":
        from scidb.column_selection import ColumnSelection
        return ColumnSelection(
            get_surrogate_class(spec["type_name"]),
            list(spec["columns"]),
        )
    if kind == "fixed":
        from scidb.fixed import Fixed
        inner = _reconstruct_input_for_keys(spec["inner"])
        fixed_meta = dict(spec.get("fixed_metadata", {}) or {})
        return Fixed(inner, **fixed_meta)
    if kind == "merge":
        from scidb.merge import Merge
        subs = [_reconstruct_input_for_keys(s) for s in spec["specs"]]
        return Merge(*subs)
    if kind == "pathinput":
        from scifor.pathinput import PathInput
        root = spec.get("root_folder") or None
        if root == "":
            root = None
        regex_flag = bool(spec.get("regex", False))
        return PathInput(spec["template"], root_folder=root, regex=regex_flag)
    return spec


def build_for_each_config_keys(
    fn_name: str,
    fn_hash: str,
    inputs_spec: dict,
    where_key=None,
    distribute: bool = False,
    as_table=None,
) -> dict:
    """Return the canonical ``ForEachConfig.to_version_keys()`` dict.

    MATLAB ships a JSON-friendly description of inputs (see
    ``_reconstruct_input_for_keys`` for the spec format) plus the
    pre-computed function name and source hash. We reconstruct the
    Python-side wrappers, run them through ``ForEachConfig`` so the
    serialization logic stays in one place, then replace the sentinel-
    function hash with the MATLAB-provided one.

    The function name uses ``__name__`` so ForEachConfig's ``__fn`` matches
    the MATLAB-visible function name. The MATLAB caller is responsible for
    passing the same name MATLAB's ``functions(fcn).function`` returns.

    Parameters
    ----------
    fn_name : str
        Function name (e.g. "bandpass").
    fn_hash : str
        16- or 64-char hex string from ``compute_matlab_function_hash``.
        Stored in the returned dict's ``__fn_hash`` field unchanged.
    inputs_spec : dict
        ``{param_name: kind-tagged-spec}``. See
        ``_reconstruct_input_for_keys`` for the spec format.
    where_key : str or None
        Already-stringified where-filter key (MATLAB calls
        ``filter.py_filter.to_key()`` before passing).
    distribute : bool
        Per the scidb.for_each ``distribute=`` flag.
    as_table : bool, list, or None
        Per the scidb.for_each ``as_table=`` flag.

    Returns
    -------
    dict
        Canonical version_keys dict (same structure as
        ``ForEachConfig.to_version_keys()``).
    """
    from scidb.foreach_config import ForEachConfig

    inputs = {
        name: _reconstruct_input_for_keys(spec)
        for name, spec in dict(inputs_spec).items()
    }

    # ForEachConfig requires a callable; use a local sentinel so the rest of
    # to_version_keys runs unchanged, then overwrite __fn_hash. The sentinel
    # is not stored anywhere — its hash is discarded.
    def _sentinel():
        pass

    _sentinel.__name__ = fn_name

    # Convert as_table from a py.list/tuple to a Python list for ForEachConfig.
    if as_table is not None and not isinstance(as_table, bool):
        try:
            as_table = list(as_table)
        except TypeError:
            pass

    cfg = ForEachConfig(
        _sentinel,
        inputs,
        where=where_key,
        distribute=bool(distribute),
        as_table=as_table,
    )
    keys = cfg.to_version_keys()
    # Replace the sentinel's auto-computed hash with the MATLAB-provided one.
    keys["__fn"] = fn_name
    keys["__fn_hash"] = fn_hash
    return keys


# ---------------------------------------------------------------------------
# Two-pass for_each bridge: prepare → MATLAB scifor loop → save
#
# The MATLAB embedded Python interpreter only supports unidirectional
# MATLAB→Python calls, so the original "Python drives the loop with a
# MATLAB callback" design is not viable.  Instead, the bridge exposes
# two seam functions:
#
#   1. ``for_each_prepare`` runs scidb's pre-loop work in Python
#      (variant expansion, DB load, branch_params, __upstream prep).
#      Returns an integer ``handle`` that keys server-side state, plus
#      the loaded inputs and expanded combo list that MATLAB needs to
#      run the inner loop with the existing ``+scifor/for_each.m``.
#
#   2. ``for_each_save`` consumes the per-output result tables MATLAB
#      collected from that inner loop, merges them, and runs scidb's
#      save step (lineage-aware, with branch_params and ``__upstream``).
#
# State sits in ``_for_each_state_cache`` between the two calls so
# Python's scidb.foreach._ForEachState (built by the prepare step) is
# the source of truth — MATLAB never has to reconstruct any of it.
# ---------------------------------------------------------------------------


_for_each_state_cache: dict = {}
_for_each_state_id_counter = 0


def _sanitize_rid_key(key: str) -> str:
    """Map ``__rid_x`` → ``x__rid_x`` so MATLAB structs accept it.

    MATLAB struct field names cannot start with ``_``; tables also have
    historically auto-sanitized leading underscores. Prefix with ``x`` so
    every artifact the bridge hands to MATLAB (DataFrame columns, combo
    dict keys, metadata iterable keys, scifor schema list) uses the same
    name and ``+scifor/for_each.m`` can filter and group by them.
    Reversed by ``_unsanitize_rid_key`` before save.
    """
    if key.startswith("__"):
        return "x" + key
    return key


def _unsanitize_rid_key(key: str) -> str:
    if key.startswith("x__"):
        return key[1:]
    return key


def _strip_internal_columns(val, also_strip_record_id_branch_params=False):
    """Drop scidb-internal columns from a DataFrame so MATLAB's scifor
    doesn't trip over them.

    Always drops ``__rid_*`` columns. When
    ``also_strip_record_id_branch_params=True`` (used for Merge
    constituents and for Fixed/ColumnSelection inner data), additionally
    drops ``__record_id`` and ``__branch_params`` — those columns are
    Python-side variant-tracking artifacts that ``MATLAB +scifor/for_each``
    has no concept of and that break ``innerjoin`` inside Merge
    (different record_ids per constituent produce zero-row inner joins).

    Free-standing DataFrames (the rid-expansion case) keep
    ``__record_id`` / ``__branch_params`` so the rest of the prepare
    pipeline can still find them; the per-input rename to ``x__rid_*``
    runs afterward.
    """
    import pandas as pd
    from scifor.fixed import Fixed as _SciforFixed
    from scifor.column_selection import ColumnSelection as _SciforColSel
    from scifor.merge import Merge as _SciforMerge

    if isinstance(val, pd.DataFrame):
        drop_cols = [c for c in val.columns if c.startswith("__rid_")]
        if also_strip_record_id_branch_params:
            for extra in ("__record_id", "__branch_params"):
                if extra in val.columns:
                    drop_cols.append(extra)
        if drop_cols:
            return val.drop(columns=drop_cols)
        return val
    if isinstance(val, _SciforFixed):
        val.data = _strip_internal_columns(
            val.data, also_strip_record_id_branch_params=True
        )
        return val
    if isinstance(val, _SciforColSel):
        val.data = _strip_internal_columns(
            val.data, also_strip_record_id_branch_params=True
        )
        return val
    if isinstance(val, _SciforMerge):
        val.tables = tuple(
            _strip_internal_columns(t, also_strip_record_id_branch_params=True)
            for t in val.tables
        )
        return val
    return val


def _rename_rid_columns_in_value(val, rename_map):
    """Walk a loaded-input value and prep its DataFrames for MATLAB:

    - **Free-standing DataFrame** (variant-iterated input): rename the
      iterated ``__rid_*`` columns to MATLAB-safe form.
    - **Inside ``scifor.Fixed`` / ``scifor.ColumnSelection``**: strip
      every ``__rid_*`` AND ``__record_id`` / ``__branch_params``
      column. See ``_strip_internal_columns`` for why.
    - **``scifor.Merge``**: each constituent is treated as Fixed-like:
      ``__record_id`` / ``__branch_params`` are stripped so MATLAB
      scifor's ``innerjoin`` doesn't see per-constituent record IDs
      as join keys.
    """
    import pandas as pd
    from scifor.fixed import Fixed as _SciforFixed
    from scifor.column_selection import ColumnSelection as _SciforColSel
    from scifor.merge import Merge as _SciforMerge

    if isinstance(val, pd.DataFrame):
        cols_to_rename = {k: v for k, v in rename_map.items() if k in val.columns}
        if cols_to_rename:
            return val.rename(columns=cols_to_rename)
        return val
    if isinstance(val, _SciforFixed):
        val.data = _strip_internal_columns(val.data, True)
        return val
    if isinstance(val, _SciforColSel):
        val.data = _strip_internal_columns(val.data, True)
        return val
    if isinstance(val, _SciforMerge):
        val.tables = tuple(
            _strip_internal_columns(t, True) for t in val.tables
        )
        return val
    return val


def _make_matlab_fn_sentinel(fn_name: str):
    """Build a Python callable that records its name but errors if invoked.

    The MATLAB-driven path runs the user function inside MATLAB's
    ``+scifor/for_each.m`` loop. Python's ``_for_each_prepare`` accepts a
    ``fn`` argument only to read ``fn.__name__`` and build a
    ``ForEachConfig``; the function itself is never called from Python.
    If it ever IS called, that signals a programming error (likely the
    bridge state was reused after ``for_each_save`` freed it).
    """
    def _sentinel(**kwargs):
        raise RuntimeError(
            f"MATLAB for_each sentinel for '{fn_name}' was invoked from "
            f"Python. The MATLAB-driven path runs the loop in MATLAB; "
            f"Python should never call the sentinel directly."
        )
    _sentinel.__name__ = fn_name
    _sentinel.__lineage_wrapper__ = True  # Skip Python's tuple-unpacking wrapper in scidb.for_each
    return _sentinel


def for_each_prepare(
    fn_name: str,
    fn_hash: str,
    inputs_spec,
    output_class_names,
    metadata_iterables,
    where=None,
    distribute: bool = False,
    as_table=None,
    db=None,
    dry_run: bool = False,
):
    """Bridge entry: run scidb.for_each's prepare phase in Python.

    Parameters
    ----------
    fn_name : str
        Function name (e.g. ``"bandpass"``). Used for ``__fn`` and logging.
    fn_hash : str
        MATLAB-computed function source hash. Stored as ``__fn_hash`` in
        the resulting version_keys, overriding the auto-computed hash
        ``ForEachConfig`` derives from the no-op Python sentinel.
    inputs_spec : dict
        ``{param_name: kind-tagged spec}`` (see ``_reconstruct_input_for_keys``).
    output_class_names : list[str]
        MATLAB variable type names for each output, in declaration order.
        Each is resolved via ``get_surrogate_class``.
    metadata_iterables : dict[str, list]
        ``{key: [values...]}`` passed to scidb.for_each as ``**metadata_iterables``.
        Empty lists trigger DB-defaults resolution per scidb's usual rules.
    where : scidb.filters.Filter or None
        Actual Python Filter object (MATLAB passes ``where_filter.py_filter``).
        Python's ``ForEachConfig.to_version_keys`` calls ``.to_key()`` on
        it for ``__where`` stringification; ``_load_input`` consumes the
        same object directly when loading.
    distribute : bool
    as_table : bool, list, or None
    db : DatabaseManager or None

    Returns
    -------
    dict with keys:
        handle : int                — server-side cache id; pass back to for_each_save
        loaded_inputs : dict         — {param_name: DataFrame or constant}
        full_combos : list[dict]     — variant-expanded combos
        output_names : list[str]
        extended_metadata_iterables : dict
        fn_name : str
        fn_hash : str

    Raises
    ------
    NotImplementedError
        If any input resolves to a ``PerComboLoader`` (per-combo loading
        is not yet supported on the MATLAB path).
    """
    from scidb.foreach import (
        PerComboLoader,
        PerComboLoaderMerge,
        _for_each_prepare,
    )

    # Reconstruct Python wrappers from the kind-tagged spec
    inputs = {
        name: _reconstruct_input_for_keys(spec)
        for name, spec in dict(inputs_spec).items()
    }

    # Resolve output class names → surrogate classes
    outputs = [get_surrogate_class(str(n)) for n in list(output_class_names)]

    # Build the no-op Python sentinel for ForEachConfig
    fn = _make_matlab_fn_sentinel(fn_name)

    # Normalize metadata_iterables: each value must be a Python list of
    # values to iterate over.  MATLAB sends scalars (subject=1 →
    # py.float(1.0)), strings, numpy arrays, and lists — wrap scalars
    # and strings as single-element lists so Python's scidb.for_each
    # sees a uniform list-of-values contract regardless of how the user
    # spelled the iterable on the MATLAB side.
    meta = {}
    for key, val in dict(metadata_iterables).items():
        if val is None:
            meta[key] = []
        elif isinstance(val, str):
            meta[key] = [val]
        elif isinstance(val, (bool, int, float)):
            meta[key] = [val]
        elif hasattr(val, "tolist"):
            meta[key] = val.tolist()
        else:
            try:
                meta[key] = list(val)
            except TypeError:
                # Last-resort: treat as a single value
                meta[key] = [val]

    # Normalize as_table: accept True/False/list/None
    if as_table is None or isinstance(as_table, bool):
        as_table_arg = as_table
    else:
        as_table_arg = list(as_table)

    # Where: accept None or a Filter object. (Empty string sometimes
    # arrives from MATLAB when no filter was supplied; treat it as None.)
    where_arg = where if where not in ("", None) else None

    # On dry_run=True, Python's _for_each_prepare runs the dry-run
    # scifor.for_each call and returns None. The MATLAB caller likewise
    # returns early (no save phase). The scifor dry-run output prints
    # planned iterations using DB-resolved metadata values — something
    # MATLAB's scifor.for_each on its own cannot do.
    if dry_run:
        try:
            _for_each_prepare(
                fn=fn,
                fn_name=fn_name,
                inputs=inputs,
                outputs=outputs,
                dry_run=True,
                as_table=as_table_arg,
                db=db if db is not None and not isinstance(db, type(None)) else None,
                distribute=bool(distribute),
                where=where if where not in ("", None) else None,
                _pre_combo_hook=None,
                _cancel_check=None,
                metadata_iterables=meta,
            )
        except Exception:
            raise
        # No state to cache and no save phase needed.
        return {
            "handle": -1,
            "loaded_inputs": {},
            "full_combos": [],
            "output_names": [],
            "extended_metadata_iterables": {},
            "fn_name": fn_name,
            "fn_hash": fn_hash,
            "dry_run": True,
        }

    state = _for_each_prepare(
        fn=fn,
        fn_name=fn_name,
        inputs=inputs,
        outputs=outputs,
        dry_run=False,
        as_table=as_table_arg,
        db=db if db is not None and not isinstance(db, type(None)) else None,
        distribute=bool(distribute),
        where=where_arg,
        _pre_combo_hook=None,
        _cancel_check=None,
        metadata_iterables=meta,
    )

    if state is None:
        raise RuntimeError(
            "for_each_prepare returned None — dry_run is not supported "
            "on the MATLAB-driven path."
        )

    # Override the sentinel's auto-computed hash with the MATLAB hash so
    # the recorded version_keys identify the real MATLAB function source.
    state.config_keys["__fn_hash"] = fn_hash

    # PathInput special case: scidb's _load_input wraps PathInput in a
    # PerComboLoader because PathInput has no load_all (its load() is
    # template substitution, not DB lookup). MATLAB's +scifor/for_each.m
    # natively handles scifor.PathInput when _resolve_pathinput=true is
    # set, so unwrap the sentinel back to the PathInput instance and let
    # MATLAB resolve it per-combo. Other per-combo loader kinds (e.g.
    # ColumnSelection over a no-load_all type) are not yet supported on
    # the MATLAB path; surface a clear error so the user knows.
    from scifor.pathinput import PathInput as _SciforPathInput
    from scidb.fixed import Fixed as _ScidbFixed
    unsupported_per_combo = []
    for k, v in list(state.loaded_inputs.items()):
        if not isinstance(v, (PerComboLoader, PerComboLoaderMerge)):
            continue
        # Unwrap PerComboLoader to its underlying scidb.Fixed / PathInput / etc.
        spec = v.spec if isinstance(v, PerComboLoader) else v.merge_spec
        # Direct PathInput
        if isinstance(spec, _SciforPathInput):
            state.loaded_inputs[k] = spec
            continue
        # scidb.Fixed wrapping a PathInput: build a scifor.Fixed(pathinput,
        # **meta) so MATLAB's resolve_data_spec sees the same kind it expects.
        if isinstance(spec, _ScidbFixed) and isinstance(spec.var_type, _SciforPathInput):
            from scifor.fixed import Fixed as _SciforFixed
            state.loaded_inputs[k] = _SciforFixed(
                spec.var_type, **dict(spec.fixed_metadata)
            )
            continue
        # Anything else (e.g. ColumnSelection over a no-load_all var type)
        # is not yet supported on the MATLAB path.
        unsupported_per_combo.append(k)
    if unsupported_per_combo:
        raise NotImplementedError(
            f"MATLAB-driven scidb.for_each does not yet support per-combo "
            f"loaders for inputs: {unsupported_per_combo}. These arise when "
            f"a Fixed/Merge/ColumnSelection wraps a variable type without "
            f"load_all. Use load_all-capable types, or call from Python."
        )

    # --- Sanitize __rid_* artifacts at the MATLAB boundary ---
    # The internal state cache keeps the original ``__rid_*`` names so
    # Python's save path (_for_each_save_resolved → _save_results) still
    # finds them. Only the MATLAB-facing copies are renamed.
    #
    # Sources of __rid_* names to cover:
    #   - state.rid_keys: var-type inputs that got rid-expanded
    #   - state.fixed_rid_values: Fixed inputs (and inputs that prep
    #     misclassifies as Fixed-like via the .data attribute, e.g.
    #     ColumnSelection — Step 12 adds __rid_{param} for each)
    #   - any other __rid_* key that may appear in combos / DataFrame
    #     columns / extended_metadata_iterables
    rid_rename_map = {k: _sanitize_rid_key(k) for k in state.rid_keys}
    for fixed_param in state.fixed_rid_values:
        rk = f"__rid_{fixed_param}"
        rid_rename_map.setdefault(rk, _sanitize_rid_key(rk))
    # Belt and braces: scan combos + meta_iters for any __rid_* we missed.
    for combo in state.full_combos:
        for k in combo:
            if k.startswith("__rid_") and k not in rid_rename_map:
                rid_rename_map[k] = _sanitize_rid_key(k)
    for k in state.extended_metadata_iterables:
        if k.startswith("__rid_") and k not in rid_rename_map:
            rid_rename_map[k] = _sanitize_rid_key(k)

    # Loaded inputs: rename DataFrame columns (and inside wrappers)
    matlab_loaded_inputs = {
        name: _rename_rid_columns_in_value(val, rid_rename_map)
        for name, val in state.loaded_inputs.items()
    }

    # Full combos: rename keys
    matlab_full_combos = []
    for combo in state.full_combos:
        matlab_full_combos.append({
            rid_rename_map.get(k, k): v for k, v in combo.items()
        })

    # Extended metadata iterables: rename keys
    matlab_meta_iters = {
        rid_rename_map.get(k, k): v
        for k, v in state.extended_metadata_iterables.items()
    }

    # Update Python's scifor schema (which MATLAB's scifor.get_schema reads
    # via Phase 1.3 forwarding) so it sees sanitized __rid_* names. Python's
    # _for_each_save_resolved's Step 18 restores to current_schema_keys
    # regardless of what we set here, so this is per-call only.
    if state.rid_keys_for_schema:
        import scifor as _scifor_local
        sanitized_schema = list(state.current_schema_keys) + [
            _sanitize_rid_key(k) for k in state.rid_keys_for_schema
        ]
        _scifor_local.set_schema(sanitized_schema)
    # If state.rid_keys_for_schema is empty (aggregation mode or Fixed-only
    # inputs), combos may still carry __rid_* keys — but Python's scifor
    # never adds those to its schema so MATLAB's scifor.for_each won't
    # filter on them; no schema update is needed here. The DataFrame /
    # combo / metadata-iterable renames above are sufficient.

    # Cache state for the matching for_each_save call
    global _for_each_state_id_counter
    handle = _for_each_state_id_counter
    _for_each_state_id_counter += 1
    _for_each_state_cache[handle] = {
        "state": state,
        "inputs": inputs,
        "outputs": outputs,
        "db": db,
        "rid_rename_map": rid_rename_map,
    }

    from scidb.log import Log as _BridgeLog
    _BridgeLog.info(
        f"[bridge] for_each_prepare returning: full_combos={len(matlab_full_combos)}, "
        f"extended_metadata_iterables keys={list(matlab_meta_iters.keys())}, "
        f"meta sizes={[len(v) if hasattr(v, '__len__') else 'N/A' for v in matlab_meta_iters.values()]}"
    )
    return {
        "handle": handle,
        "loaded_inputs": matlab_loaded_inputs,
        "full_combos": matlab_full_combos,
        "output_names": state.output_names,
        "extended_metadata_iterables": matlab_meta_iters,
        "fn_name": state.fn_name,
        "fn_hash": fn_hash,
    }


def for_each_describe_loaded_input(val):
    """Describe one ``loaded_inputs`` value so MATLAB can rebuild the
    matching MATLAB-side scifor wrapper.

    Python's ``_for_each_prepare`` returns ``loaded_inputs`` with values
    that may be:
      - pandas DataFrame
      - scifor.Fixed (wrapping a DataFrame plus fixed_metadata)
      - scifor.ColumnSelection (wrapping a DataFrame plus column names)
      - scifor.Merge (wrapping multiple DataFrames or wrappers)
      - PerComboLoader / PerComboLoaderMerge (already rejected upstream)
      - any other constant value

    MATLAB's ``+scifor/for_each.m`` expects the MATLAB classdef versions
    of these wrappers (with MATLAB tables inside). This helper produces a
    kind-tagged dict that MATLAB can switch on, with DataFrames left as
    Python objects (so MATLAB's ``from_python`` converts them to MATLAB
    tables on the other side of the bridge crossing).
    """
    import pandas as pd
    from scifor.fixed import Fixed as _SciforFixed
    from scifor.column_selection import ColumnSelection as _SciforColSel
    from scifor.merge import Merge as _SciforMerge
    from scifor.pathinput import PathInput as _SciforPathInput

    if isinstance(val, pd.DataFrame):
        return {"kind": "dataframe", "data": val}
    if isinstance(val, _SciforFixed):
        return {
            "kind": "fixed",
            "inner": for_each_describe_loaded_input(val.data),
            "fixed_metadata": dict(val.fixed_metadata),
        }
    if isinstance(val, _SciforColSel):
        return {
            "kind": "column_selection",
            "inner": for_each_describe_loaded_input(val.data),
            "columns": list(val.columns),
        }
    if isinstance(val, _SciforMerge):
        return {
            "kind": "merge",
            "tables": [for_each_describe_loaded_input(t) for t in val.tables],
        }
    if isinstance(val, _SciforPathInput):
        # PathInput is resolved per-combo by MATLAB's +scifor/for_each.m
        # via _resolve_pathinput=true. Ship the template + flags so MATLAB
        # can construct a matching MATLAB-side scifor.PathInput.
        return {
            "kind": "pathinput",
            "template": val.path_template,
            "root_folder": (
                str(val.root_folder) if val.root_folder is not None else ""
            ),
            "regex": bool(getattr(val, "regex", False)),
        }
    return {"kind": "raw", "value": val}


def for_each_save(handle, result_dataframes, save: bool = True):
    """Bridge entry: run scidb.for_each's save phase.

    Parameters
    ----------
    handle : int
        Handle returned by ``for_each_prepare``. Consumed (cache entry is
        freed regardless of success or failure).
    result_dataframes : list[pandas.DataFrame]
        One DataFrame per output, in the same order as the
        ``output_class_names`` passed to ``for_each_prepare``. Each carries
        the metadata columns from MATLAB's ``+scifor/for_each.m`` plus
        one column named for that output.  When there is only one output,
        a single DataFrame may be passed (a length-1 list also works).
    save : bool
        If False, run the save phase with ``save=False`` (schema restore
        still happens; no records are written).

    Returns
    -------
    pandas.DataFrame
        The merged result table, with one row per combo and metadata +
        output columns. Same shape Python's ``scidb.for_each`` returns.
    """
    import pandas as pd

    from scidb.foreach import _for_each_save_resolved

    cached = _for_each_state_cache.pop(int(handle), None)
    if cached is None:
        raise ValueError(
            f"for_each_save: handle {handle} not found in cache "
            f"(already freed or never registered)"
        )

    state = cached["state"]
    inputs = cached["inputs"]
    outputs = cached["outputs"]
    db = cached["db"]
    rid_rename_map = cached.get("rid_rename_map", {})

    # Reverse the bridge-boundary sanitization on the way back: MATLAB
    # produced result tables whose columns include the sanitized names
    # (e.g. ``x__rid_x``); Python's save path (state.rid_keys, _save_results)
    # expects the original ``__rid_x`` names.
    reverse_map = {v: k for k, v in rid_rename_map.items()}

    # Merge per-output DataFrames into the single result_tbl shape that
    # _for_each_save_resolved expects (matching what Python's scifor
    # returns when called with output_names).
    if isinstance(result_dataframes, pd.DataFrame):
        dfs = [result_dataframes]
    else:
        dfs = [df for df in list(result_dataframes) if df is not None]

    if reverse_map:
        dfs = [
            df.rename(columns={
                c: reverse_map[c] for c in df.columns if c in reverse_map
            }) if isinstance(df, pd.DataFrame) else df
            for df in dfs
        ]

    if not dfs:
        result_tbl = pd.DataFrame()
    elif len(dfs) == 1:
        result_tbl = dfs[0]
    else:
        # Outer-merge on the shared metadata columns so combos that produced
        # different output counts (e.g. distribute) are preserved.
        result_tbl = dfs[0]
        for df in dfs[1:]:
            shared = [c for c in result_tbl.columns if c in df.columns]
            if shared:
                result_tbl = result_tbl.merge(df, on=shared, how="outer")
            else:
                # No shared columns to merge on; fall back to row-wise concat.
                result_tbl = pd.concat([result_tbl, df], axis=1)

    from scidb.log import Log as _Log
    _Log.info(
        f"[bridge] for_each_save: handle={handle}, "
        f"result_tbl shape={result_tbl.shape}, "
        f"columns={list(result_tbl.columns)}"
    )

    return _for_each_save_resolved(
        state=state,
        result_tbl=result_tbl,
        inputs=inputs,
        outputs=outputs,
        save=bool(save),
        db=db if db is not None and not isinstance(db, type(None)) else None,
        lineage_fixed_rids=None,
    )


def pathinput_project_root() -> str:
    """Return scifor's ``_find_project_root()`` result as a string.

    Public-named wrapper around the underscore-prefixed helper so MATLAB
    can resolve the path through the bridge — MATLAB's parser rejects
    dot-access to identifiers that start with an underscore, so
    ``py.scifor.pathinput._find_project_root()`` is not callable directly
    from MATLAB code.
    """
    from scifor.pathinput import _find_project_root
    return str(_find_project_root())


def discover_pathinput_combos(pi, user_metadata=None):
    """Discover filesystem combos for a PathInput and filter by user values.

    Replaces the MATLAB-side PathInput discovery+filter block in
    ``+scidb/for_each.m``. Returns a dict with both the (filtered) combos
    and the per-key value lists actually present in the filtered result,
    so MATLAB can update its ``meta_values`` to reflect what's on disk
    (dropping invented combos and filling in keys the user passed as []).

    The user-value filter algorithm mirrors what MATLAB used to do:
        - keys not present in combo dicts are ignored
        - empty user-value lists are treated as "no constraint"
        - non-empty user-value lists keep only combos whose value (stringified)
          is in the user-provided set

    Parameters
    ----------
    pi : scifor.pathinput.PathInput
        The Python PathInput instance to discover from. MATLAB's
        ``scifor.PathInput`` exposes its underlying Python instance via
        ``pi.py_obj`` so the same configuration (template, root_folder,
        regex flag) is used for discovery and per-combo load.
    user_metadata : dict[str, list] or None
        ``{key: [user_values...]}``. Empty list means "no constraint".
        Keys absent from combos are ignored.

    Returns
    -------
    dict with keys:
        combos          : list of dicts (filtered)
        original_count  : int (combos before user-value filtering)
        present_keys    : list of str (placeholder keys appearing in combos)
        values_by_key   : dict[str, list[str]] — distinct stringified values
                          per placeholder key in the filtered combos
                          (preserves insertion order, deduplicated)
    """
    combos = pi.discover()
    original_count = len(combos)

    if not combos:
        return {
            'combos': [],
            'original_count': 0,
            'present_keys': [],
            'values_by_key': {},
        }

    present_keys = list(combos[0].keys())

    # Filter by user-supplied metadata values
    user = dict(user_metadata or {})
    if user:
        kept = []
        for combo in combos:
            keep = True
            for key, user_vals in user.items():
                if key not in combo:
                    continue
                uv = list(user_vals or [])
                if not uv:
                    continue  # empty means no constraint
                if str(combo[key]) not in {str(v) for v in uv}:
                    keep = False
                    break
            if keep:
                kept.append(combo)
        combos = kept

    # Compute distinct stringified values per key (insertion-ordered)
    values_by_key: dict = {}
    for key in present_keys:
        seen = {}
        for c in combos:
            v = str(c[key])
            if v not in seen:
                seen[v] = None
        values_by_key[key] = list(seen.keys())

    return {
        'combos': combos,
        'original_count': original_count,
        'present_keys': present_keys,
        'values_by_key': values_by_key,
    }


def compute_matlab_function_hash(source_text: str, name: str = '', unpack_output: bool = False) -> str:
    """SHA-256 hash for a MATLAB function.

    Owned in Python so the format can be tweaked centrally (e.g. to strip
    comments or normalize line endings) without divergence between
    MATLAB-side and GUI-side consumers. Today it hashes the raw UTF-8
    bytes of the source; the ``name`` and ``unpack_output`` parameters
    are accepted for traceability and forward compatibility with future
    format tweaks, but do not affect the returned hash. The proxy hash
    that's actually stored in ``_lineage.function_hash`` is computed by
    ``MatlabLineageFcn.__init__`` from this source hash plus the unpack
    flag — keeping that combining step in one place.

    Parameters
    ----------
    source_text : str
        Source code of the MATLAB function (typically the full ``.m`` file).
    name : str
        Function name, for logging only.
    unpack_output : bool
        Multi-output flag, accepted for forward compatibility.

    Returns
    -------
    str
        64-character lowercase hex SHA-256 digest of ``source_text``.
    """
    return sha256(source_text.encode('utf-8')).hexdigest()


def split_flat_to_lists(flat_array, lengths):
    """Split a flat numpy array into a list of Python lists by lengths.

    Used by MATLAB's to_python cell-column fast path.  Instead of
    N separate MATLAB→Python bridge crossings (one per cell element),
    MATLAB concatenates all cell elements into one flat array, records
    their lengths, and sends both in a single crossing.  This function
    splits the flat array back into per-element Python lists.

    MATLAB's Python bridge preserves the 2-D shape of MATLAB row
    vectors for some dtypes (notably logical) when calling
    ``py.numpy.array(...)``, while flattening for others (double).
    We ravel both inputs defensively so this routine is robust to a
    1-D or 2-D incoming shape and a future bridge change can't break
    the cell-column round trip silently.

    Parameters
    ----------
    flat_array : numpy.ndarray
        Concatenated values (any shape; raveled here).
    lengths : numpy.ndarray
        Integer array where ``lengths[i]`` is the number of elements
        belonging to sub-list *i* (any shape; raveled here).

    Returns
    -------
    list of list
        One Python list per entry in *lengths*, each containing native
        Python scalars (float, int, bool).
    """
    import numpy as np

    flat = np.asarray(flat_array).ravel()
    lens = np.asarray(lengths).ravel().tolist()
    result = []
    pos = 0
    for length in lens:
        result.append(flat[pos:pos + length].tolist())
        pos += length
    return result


def check_cache(invocation: MatlabLineageFcnInvocation):
    """Check if a computation is already cached.

    Returns
    -------
    list or None
        List of cached output values (raw data), or None on miss.
    """
    from scilineage.backend import _get_backend

    _backend = _get_backend()
    if _backend is not None:
        try:
            return _backend.find_by_lineage(invocation)
        except Exception:
            pass
    return None


def make_lineage_fcn_result(invocation: MatlabLineageFcnInvocation, output_num: int, data):
    """Create a real LineageFcnResult backed by a MatlabLineageFcnInvocation.

    The returned object is a genuine ``scilineage.core.LineageFcnResult``
    instance, so ``isinstance`` checks in ``save_variable`` pass.
    """
    from scilineage.core import LineageFcnResult

    return LineageFcnResult(invocation, output_num, True, data)


def register_matlab_variable(type_name: str, schema_version: int = 1):
    """Create a Python surrogate BaseVariable subclass for a MATLAB type.

    The surrogate is auto-registered in ``BaseVariable._all_subclasses``
    via ``__init_subclass__`` and, if a database is configured, registered
    with the ``DatabaseManager`` as well.

    Returns the surrogate class.
    """
    from scidb.variable import BaseVariable

    existing = BaseVariable.get_subclass_by_name(type_name)
    if existing is not None:
        return existing

    surrogate = type(type_name, (BaseVariable,), {"schema_version": schema_version})

    try:
        from scidb.database import get_database
        get_database().register(surrogate)
    except Exception:
        pass  # Database not yet configured; will register on configure_database

    return surrogate


def save_batch_bridge(type_name, data_values, metadata_keys, metadata_columns, common_metadata=None, db=None):
    """Bridge function for MATLAB save_from_table.

    Accepts columnar data (one list per column) from MATLAB and assembles
    the (data_value, metadata_dict) tuples that DatabaseManager.save_batch()
    expects.  This avoids per-row MATLAB↔Python round-trips.

    Parameters
    ----------
    type_name : str
        Variable class name (e.g. "StepLength").
    data_values : list or numpy array
        One data value per row.
    metadata_keys : list of str
        Metadata column names, same order as metadata_columns.
    metadata_columns : list of (list or numpy array)
        One inner list/array per metadata key, each with one value per row.
    common_metadata : dict or None
        Extra metadata applied to every row.
    db : DatabaseManager or None
        Optional database; uses global default when None.

    Returns
    -------
    list of str
        Record IDs for each saved row.
    """
    import time as _time
    from scidb.log import Log
    from scidb.variable import BaseVariable
    from scidb.database import get_database

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        raise ValueError(
            f"Variable type '{type_name}' is not registered. "
            f"Call scidb.register_variable('{type_name}') first."
        )

    t_start = _time.perf_counter()

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()
    common = dict(common_metadata) if common_metadata else {}
    keys = list(metadata_keys)

    # Bulk-convert numpy arrays to native Python lists (one call instead of
    # N per-element .item() calls).  Plain Python lists pass through unchanged.
    if hasattr(data_values, 'tolist'):
        data_list = data_values.tolist()
    else:
        data_list = [v.item() if hasattr(v, 'item') else v for v in data_values]

    meta_lists = []
    for j in range(len(keys)):
        col = metadata_columns[j]
        if isinstance(col, str):
            # Joined string from MATLAB (record-separator delimited)
            meta_lists.append(col.split('\x1e'))
        elif hasattr(col, 'tolist'):
            meta_lists.append(col.tolist())
        else:
            meta_lists.append([v.item() if hasattr(v, 'item') else v for v in col])

    n = len(data_list)
    data_items = []
    for i in range(n):
        meta = dict(common)
        for j, key in enumerate(keys):
            meta[key] = meta_lists[j][i]
        data_items.append((data_list[i], meta))

    t_convert = _time.perf_counter()
    Log.debug(f"save_batch_bridge({type_name}): {n} items, "
              f"data_items construction {t_convert - t_start:.3f}s")

    result = "\n".join(_db.save_batch(cls, data_items))

    Log.info(f"save_batch_bridge({type_name}): {n} items, "
             f"total {_time.perf_counter() - t_start:.3f}s")
    return result


# ---------------------------------------------------------------------------
# Batch cache — keeps data/py_vars in Python so they never cross to MATLAB's
# proxy layer.  MATLAB accesses individual items via get_batch_item().
# ---------------------------------------------------------------------------

_batch_cache = {}
_batch_id_counter = 0


def _cache_batch(data_list, py_vars_list):
    """Store data and py_vars lists server-side, return an integer handle."""
    global _batch_id_counter
    bid = _batch_id_counter
    _batch_id_counter += 1
    _batch_cache[bid] = (data_list, py_vars_list)
    return bid


def get_batch_item(batch_id, index):
    """Return (data, py_var) for one element from a cached batch."""
    data_list, py_vars_list = _batch_cache[int(batch_id)]
    i = int(index)
    return data_list[i], py_vars_list[i]


def get_batch_data_item(batch_id, index):
    """Return just the data for one element from a cached batch."""
    data_list, _ = _batch_cache[int(batch_id)]
    return data_list[int(index)]


def free_batch(batch_id):
    """Release a cached batch."""
    _batch_cache.pop(int(batch_id), None)


def wrap_batch_bridge(py_vars_list):
    """Extract all fields from a list of BaseVariables into bulk format.

    Scalar fields are packed into newline-joined strings and metadata into
    a single JSON string.  The ``py_vars`` list is returned directly so
    MATLAB can convert it to a cell array in one call.  When all data
    values are scalars (int/float), they are packed into a numpy array
    (``scalar_data``) for single-crossing transfer; otherwise data is
    stored in a Python-side cache for per-item access.

    Parameters
    ----------
    py_vars_list : list of BaseVariable
        Python BaseVariable instances to extract.

    Returns
    -------
    dict with keys:
        n              : int
        py_vars        : list  — BaseVariable objects for MATLAB cell() conversion
        batch_id       : int   — handle for get_batch_data_item (non-scalar only)
        record_ids     : str   — newline-joined
        content_hashes : str   — newline-joined
        lineage_hashes : str   — newline-joined ('' for None)
        json_meta      : str   — JSON array of metadata dicts
        scalar_data    : numpy.ndarray (optional) — present when all data are scalars
    """
    import json
    import numpy as np

    py_vars = list(py_vars_list) if not isinstance(py_vars_list, list) else py_vars_list
    n = len(py_vars)

    record_ids = []
    content_hashes = []
    lineage_hashes = []
    meta_dicts = []
    data = []

    for v in py_vars:
        record_ids.append(v.record_id or '')
        content_hashes.append(v.content_hash or '')
        lh = v.lineage_hash
        lineage_hashes.append(lh if lh is not None else '')
        meta = v.metadata
        meta_dicts.append(dict(meta) if meta is not None else {})
        data.append(v.data)

    # Cache data for non-scalar fallback access
    batch_id = _cache_batch(data, py_vars)

    result = {
        'n': n,
        'py_vars': py_vars,
        'batch_id': batch_id,
        'record_ids': '\n'.join(record_ids),
        'content_hashes': '\n'.join(content_hashes),
        'lineage_hashes': '\n'.join(lineage_hashes),
        'json_meta': json.dumps(meta_dicts),
    }

    # Scalar fast path: pack all data into a single numpy array
    if n > 0 and all(isinstance(d, (int, float)) for d in data):
        result['scalar_data'] = np.array(data, dtype=float)

    # DataFrame fast path: concatenate same-schema DataFrames into one
    # so MATLAB converts a single large table instead of N small ones
    if n > 0 and 'scalar_data' not in result:
        import pandas as pd
        if all(isinstance(d, pd.DataFrame) for d in data):
            first_cols = list(data[0].columns)
            if all(list(d.columns) == first_cols for d in data):
                row_counts = [len(d) for d in data]
                concat_df = pd.concat(data, ignore_index=True)
                result['concat_df'] = concat_df
                result['concat_df_row_counts'] = np.array(row_counts, dtype=np.int64)

    return result


def load_and_extract(py_class, metadata_dict, version_id='latest', db=None, where=None):
    """Load all matching variables and extract fields in bulk.

    Combines load_all -> list -> wrap_batch_bridge in one Python call.
    The intermediate BaseVariable list and data arrays stay in Python
    (accessed later via get_batch_item).  Only lightweight strings/JSON
    cross back to MATLAB.

    Parameters
    ----------
    py_class : type
        BaseVariable subclass to load.
    metadata_dict : dict
        Metadata filter (values can be lists for "match any").
    version_id : str or int
        Version filter ('latest', 'all', or an integer).
    db : DatabaseManager or None
        Optional database; uses global default when None.
    where : Filter or None
        Optional where= filter (scidb.filters.Filter instance).

    Returns
    -------
    dict
        Same format as wrap_batch_bridge (with batch_id, no data/py_vars).
    """
    from scidb.database import get_database

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()

    gen = _db.load_all(py_class, dict(metadata_dict), version_id=version_id, where=where)
    py_vars = list(gen)  # materializes entirely in Python
    return wrap_batch_bridge(py_vars)


def load_var_type_all_as_df(py_class, where=None, db=None):
    """Return the assembled DataFrame produced by ``_load_var_type_all``.

    Calls scidb's ``_load_var_type_all`` and surfaces the resulting
    DataFrame (with ``__record_id``, ``__branch_params``, schema columns,
    and data columns) as a single object that crosses the MATLAB↔Python
    bridge in one call. This replaces the MATLAB-side
    ``lineage_results_to_table`` reassembly path and preserves the
    ``__record_id`` / ``__branch_params`` columns needed for variant
    tracking.

    Parameters
    ----------
    py_class : type
        BaseVariable subclass (Python surrogate registered for the MATLAB
        type).
    where : Filter or None
        Optional ``where=`` filter.
    db : DatabaseManager or None
        Optional database; uses global default when None.

    Returns
    -------
    pandas.DataFrame
        Assembled DataFrame. Empty DataFrame when no rows match.
    """
    from scidb.database import get_database
    from scidb.foreach import _load_var_type_all

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()
    return _load_var_type_all(py_class, _db, where)


def get_surrogate_class(type_name: str):
    """Retrieve the Python surrogate class for a MATLAB variable type.

    Raises ValueError if not registered.
    """
    from scidb.variable import BaseVariable

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        raise ValueError(
            f"MATLAB variable type '{type_name}' is not registered. "
            f"Call scidb.register_variable('{type_name}') first."
        )
    return cls


def get_data_column_name(py_class, db=None):
    """Resolve the single data column name for a variable type.

    Used by MATLAB's scidb.ColName to resolve column names via the
    Python bridge.

    Parameters
    ----------
    py_class : type
        BaseVariable subclass to query.
    db : DatabaseManager or None
        Optional database; uses global default when None.

    Returns
    -------
    str
        The single data column name.

    Raises
    ------
    ValueError
        If the variable has 0 or 2+ data columns.
    """
    import json
    from scidb.database import get_database

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()
    var_name = py_class.__name__
    schema_keys = list(_db.dataset_schema_keys)

    row = _db._execute(
        "SELECT dtype FROM _variables WHERE variable_name = ?",
        [var_name],
    ).fetchone()

    if row is None:
        # Variable not yet saved — fall back to view_name
        if hasattr(py_class, 'view_name'):
            return py_class.view_name()
        return var_name

    dtype_meta = json.loads(row[0])
    mode = dtype_meta.get("mode", "single_column")

    if mode == "single_column":
        col_names = list(dtype_meta.get("columns", {}).keys())
        if col_names:
            return col_names[0]
        if hasattr(py_class, 'view_name'):
            return py_class.view_name()
        return var_name

    if mode == "dataframe":
        df_columns = dtype_meta.get("df_columns", list(dtype_meta.get("columns", {}).keys()))
        data_cols = [c for c in df_columns if c not in schema_keys]
        if len(data_cols) == 1:
            return data_cols[0]
        elif len(data_cols) == 0:
            raise ValueError(
                f"ColName({var_name}): variable has no data columns "
                f"(all columns are schema keys). "
                f"Columns: {df_columns}, schema keys: {schema_keys}"
            )
        else:
            raise ValueError(
                f"ColName({var_name}): variable has {len(data_cols)} "
                f"data columns ({data_cols}), expected exactly 1. "
                f"Schema keys: {schema_keys}"
            )

    if mode == "multi_column":
        raise ValueError(
            f"ColName({var_name}): not supported for dict-type (multi_column) variables."
        )

    if hasattr(py_class, 'view_name'):
        return py_class.view_name()
    return var_name
