"""Python bridge for MATLAB-SciStack integration.

Runs scidb's ``for_each`` prepare/save phases on behalf of MATLAB (so MATLAB
pipelines share the exact Python save path + bipartite provenance graph), and
provides :class:`MatlabLineageFcn` — a lightweight MATLAB function handle for
``scidb.check_node_state`` node coloring.

(The former per-call lineage/cache machinery — ``MatlabLineageFcnInvocation``,
``make_lineage_fcn_result``, the rerun cache — was removed with the
``@lineage_fcn`` → ``@scistack`` migration. Real MATLAB ``for_each`` always went
through the batch save path, which is unaffected.)
"""

from hashlib import sha256

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
    """Minimal holder so ``MatlabLineageFcn().fcn.__name__`` yields the function name."""

    def __init__(self, name: str):
        self.__name__ = name


class MatlabLineageFcn:
    """A MATLAB function handle for node-state queries.

    A lightweight identity object: the function name (via ``.fcn.__name__``)
    plus a content ``.hash`` derived from the MATLAB source hash and the
    multi-output flag. Passed to ``scidb.check_node_state`` so a MATLAB pipeline
    function can be colored in the GUI. (It is not a lineage wrapper — there is
    no per-call invocation/result machinery anymore.)

    Parameters
    ----------
    source_hash : str
        SHA-256 hex digest of the MATLAB function source code.
    function_name : str
        Human-readable function name.
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
        #: The digest of the .m source, EXACTLY as the save path stored it in
        #: ``_invocation.function_hash`` (MATLAB's ``scidb.internal.hash_function``
        #: → ``compute_matlab_function_hash``). Read-side hash lookups must use
        #: this: ``fcn`` is a bare name-holder with no source, so AST-hashing it
        #: yields a constant unrelated to the .m file and every comparison
        #: against a stored hash fails. See ``foreach_config.function_hash_for``.
        self.source_hash: str = source_hash
        string_repr = f"{source_hash}{STRING_REPR_DELIMITER}{unpack_output}"
        self.hash: str = sha256(string_repr.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Helper functions called from MATLAB
# ---------------------------------------------------------------------------


def _reconstruct_glue_chains(glue):
    """MATLAB glue structs → ``{param: [GlueSpec]}``.

    Each entry is ``{name, language, source_text, source_file,
    per_schema_key}``.

    **MATLAB glue** (the default, and what a handle in the ``glue`` option
    becomes) carries no callable: a ``.m`` function cannot execute inside
    Python's prepare step, so MATLAB runs the bodies itself in
    ``+scidb/for_each.m``. Python needs only the *source text*, which is what
    the glue's content hash — and so the virtual record id the consumer binds
    to — is derived from.

    **Python glue** reaches a MATLAB run only for a CONSTANT-fed parameter,
    where ``scidb.glue.apply_constant_glue`` applies it during prepare
    (Python's, on both run paths) so the glued value lands in ``__constants``.
    That needs a live callable, and this process has no function registry, so
    the body is loaded from the ``source_file`` the GUI recorded. If it cannot
    be loaded the spec still carries its hash, and the failure is reported
    where the glue is named rather than silently reshaping nothing.

    Returns ``{}`` for None / empty, so callers can pass it through unguarded.
    """
    if glue is None or isinstance(glue, type(None)) or not glue:
        return {}

    from scidb.glue import GlueSpec, resolve_python_glue

    chains: dict = {}
    for param, entries in dict(glue).items():
        specs = []
        for entry in list(entries):
            spec = dict(entry)
            name = str(spec.get("name", "glue"))
            language = str(spec.get("language", "matlab")) or "matlab"
            source_file = str(spec["source_file"]) if spec.get("source_file") else None
            fn = (
                resolve_python_glue(name, source_file)
                if language == "python"
                else None
            )
            specs.append(
                GlueSpec(
                    name=name,
                    fn=fn,
                    language=language,
                    source_text=str(spec.get("source_text", "")),
                    source_file=source_file,
                    per_schema_key=bool(spec.get("per_schema_key", False)),
                )
            )
        if specs:
            chains[str(param)] = specs
    return chains


def _reconstruct_input_for_keys(spec):
    """Reconstruct one MATLAB-described input as the Python wrapper that
    ``ForEachConfig._serialize_inputs`` knows how to format.

    ``spec`` is one of:
        - {"kind": "constant", "value": Any}
        - {"kind": "var_type", "type_name": str}
        - {"kind": "column_selection", "type_name": str, "columns": list[str]}
        - {"kind": "fixed", "inner": <spec>, "fixed_metadata": dict}
        - {"kind": "variant", "inner": <spec>, "branch_params": dict}
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

        cols = spec["columns"]
        cols = list(cols) if cols is not None else None
        return ColumnSelection(
            get_surrogate_class(spec["type_name"]),
            cols,
            iterate=bool(spec.get("iterate", False)),
        )
    if kind == "colname":
        from scifor import ColName

        # Deferred ColName() -> no var_type; _convert_inputs turns it into a
        # scifor.ColName() marker. Static ColName(MyVar) carries type_name and
        # is resolved to a column-name string during prepare.
        if spec.get("deferred", False):
            return ColName()
        return ColName(get_surrogate_class(spec["type_name"]))
    if kind == "fixed":
        from scifor import Fixed

        inner = _reconstruct_input_for_keys(spec["inner"])
        fixed_meta = dict(spec.get("fixed_metadata", {}) or {})
        return Fixed(inner, **fixed_meta)
    if kind == "variant":
        from scidb.variant import Variant

        inner = _reconstruct_input_for_keys(spec["inner"])
        branch_params = dict(spec.get("branch_params", {}) or {})
        return Variant(inner, **branch_params)
    if kind == "merge":
        from scifor import Merge

        subs = [_reconstruct_input_for_keys(s) for s in spec["specs"]]
        return Merge(*subs)
    if kind == "pathinput":
        from scifor.pathinput import PathInput

        root = spec.get("root_folder") or None
        if root == "":
            root = None
        regex_flag = bool(spec.get("regex", False))
        return PathInput(spec["template"], root_folder=root, regex=regex_flag)
    if kind == "path_output":
        from scifor import PathOutput

        return PathOutput(spec["template"])
    if kind == "across_variants":
        from scidb.across_variants import AcrossVariants

        inner = _reconstruct_input_for_keys(spec["inner"])
        return AcrossVariants(inner)
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
    """Map a ``__``-prefixed internal name → ``x__...`` so MATLAB accepts it.

    MATLAB struct field names cannot start with ``_``; tables also have
    historically auto-sanitized leading underscores. Exactly TWO names cross
    this way since 2026-09-20: ``__record_id`` on a loaded frame (the column
    ``for_each.m`` selects rows by, then drops) and ``__combo`` on a
    combination (the index of its row selection). The per-input
    ``__rid_{param}`` / ``__vsig_{param}`` families this used to rename are
    gone — the selection travels as data (``row_selection``) rather than as
    columns. Reversed by the save RPC before Python reads the result table.
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

    When ``also_strip_record_id_branch_params=True`` (used for Merge
    constituents and for Fixed/ColumnSelection inner data), drops
    ``__record_id`` and ``__branch_params`` — those columns are Python-side
    variant-tracking artifacts that ``MATLAB +scifor/for_each`` has no
    concept of and that break ``innerjoin`` inside Merge (different
    record_ids per constituent produce zero-row inner joins). A Fixed or
    ColumnSelection input therefore has no record-id column for the row
    selection to act on in MATLAB — the Fixed pin narrows by its own
    metadata and a ColumnSelection pools its variants, exactly as before.

    A free-standing DataFrame (a plain variable input) keeps ``__record_id``:
    it crosses as ``x__record_id`` and is what ``for_each.m`` selects rows
    by (``_row_selection``) before dropping it.
    """
    import pandas as pd
    from scifor.column_selection import ColumnSelection as _SciforColSel
    from scifor.fixed import Fixed as _SciforFixed
    from scifor.merge import Merge as _SciforMerge

    if isinstance(val, pd.DataFrame):
        drop_cols: list = []
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

    - **Free-standing DataFrame** (a plain variable input): rename
      ``__record_id`` to its MATLAB-safe form (``rename_map``).
    - **Inside ``scifor.Fixed`` / ``scifor.ColumnSelection``**: strip
      ``__record_id`` / ``__branch_params``. See ``_strip_internal_columns``
      for why.
    - **``scifor.Merge``**: each constituent is treated as Fixed-like:
      ``__record_id`` / ``__branch_params`` are stripped so MATLAB
      scifor's ``innerjoin`` doesn't see per-constituent record IDs
      as join keys.
    """
    import pandas as pd
    from scifor.column_selection import ColumnSelection as _SciforColSel
    from scifor.fixed import Fixed as _SciforFixed
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
        val.tables = tuple(_strip_internal_columns(t, True) for t in val.tables)
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
    _sentinel.__lineage_wrapper__ = (
        True  # Skip Python's tuple-unpacking wrapper in scidb.for_each
    )
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
    skip_computed: bool = False,
    finalized: bool = False,
    schema_keys=None,
    schema_filter=None,
    glue=None,
    locations=None,
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
    schema_keys : list[str] or None
        Schema key names to iterate — structural sugar for seeding
        ``metadata_iterables`` with ``key: []`` for each one, via the same
        ``scifor.expand_schema_keys()`` scidb.for_each's pure-Python path
        uses. Mutually exclusive with an already-populated
        ``metadata_iterables``.
    locations : dict or None
        Schema location selection — ragged ``include`` prefixes plus a standing
        ``exclude_levels`` rule (docs/claude/location-filter-semantics.md).
        Applied to ``full_combos`` before they cross to MATLAB, because
        ``+scifor/for_each.m`` runs the loop and never sees Python's
        ``scifor.for_each(locations=)``.
    schema_filter : dict[str, list] or None
        ``{schema_key: [values]}`` overrides. A key also in schema_keys (or,
        if schema_keys is None, any schema key) gets these values instead of
        DB auto-resolution. A key NOT being iterated instead constrains
        which records load, via a SchemaKeyInFilter ANDed into ``where``.
    skip_computed : bool
        If True, build scidb's pre-combo skip hook (the same one
        ``scidb.for_each`` builds in its Step 1.6) and apply it during
        prepare, so combos whose outputs already exist with unchanged
        upstream provenance are dropped from ``full_combos`` and never run
        in MATLAB's loop. The hook is fed the MATLAB-computed ``fn_hash`` so
        its function-hash comparison matches what the save path stored.
        No-op (with a warning) when no database is available.
    finalized : bool
        Endpoint (``plot_``/``stat_`` fn_name prefix) draft/record flag (D3).
        Policy is computed by scidb's ``_endpoint_policy`` (shared with the
        Python path): drafts suppress the save phase in ``for_each_save``;
        stat_ functions default ``as_table`` on. MATLAB does its own fn
        wrapping using the returned ``endpoint_kind``/``path_param``.
    glue : dict or None
        ``{param_name: [{name, source_text, per_schema_key}, ...]}`` — the
        glue chains wired to this call. **A glue node executes in the language
        of the run**, so on this path every chain is MATLAB and its bodies run
        in ``+scidb/for_each.m``, after prepare returns and before the loop (a
        ``.m`` function cannot execute inside Python's prepare step). What
        happens here is the *identity* half: each chain's virtual glue records
        are computed and the consumer's bindings routed through them, exactly
        as on the Python path, so one provenance recipe serves both languages.
        The returned ``glue_chains`` entry tells MATLAB which params to apply
        which nodes to.

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
        endpoint_kind : str          — "plot" | "stat" | "" (non-endpoint)
        path_param : str             — name of the PathOutput input, or ""
        as_table_effective : ...     — as_table after endpoint policy (stat_
                                       defaults it True); MATLAB forwards
                                       this to its scifor loop
        resolved_path_outputs : dict — {param: [path_per_combo]} aligned with
                                       full_combos; ``{ColName}`` tokens are
                                       left in the strings for MATLAB's
                                       for_columns loop to substitute
        glue_chains : dict           — {param: [{name, per_schema_key}]} the
                                       MATLAB side must apply, in order, to
                                       that param's loaded table before the
                                       loop. Empty when no glue is wired.

    Raises
    ------
    NotImplementedError
        If any input resolves to a ``PerComboLoader`` (per-combo loading
        is not yet supported on the MATLAB path).
    """
    from scidb.bindings import COMBO_KEY, RECORD_ID_COLUMN
    from scidb.foreach import (
        _build_skip_hook,
        _for_each_prepare,
        _resolve_for_columns,
    )
    from scidb.log import Log
    from scidb.per_combo import PerComboLoader, PerComboLoaderMerge

    # Reconstruct Python wrappers from the kind-tagged spec
    inputs = {
        name: _reconstruct_input_for_keys(spec)
        for name, spec in dict(inputs_spec).items()
    }

    # Normalize db to None or a live DatabaseManager (MATLAB may pass py.None).
    resolved_db = db if db is not None and not isinstance(db, type(None)) else None

    # Step 1.5: Resolve for_columns (iterate-mode ColumnSelection) inputs.
    # The Python-only path does this in scidb.for_each before _for_each_prepare;
    # the MATLAB path calls _for_each_prepare directly, so resolve here so the
    # concrete column set drives version keys and the per-column MATLAB loop.
    inputs = _resolve_for_columns(inputs, resolved_db)

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

    # Endpoint policy (plot_/stat_ prefixes + finalized): shared with the
    # Python path via scidb's _endpoint_policy — detection, the
    # plot-requires-PathOutput contract, the stat_ as_table default, draft
    # save suppression (applied in for_each_save), and warnings. MATLAB does
    # the language-specific fn wrapping itself from the returned kind.
    from scidb.foreach import _endpoint_policy

    endpoint_kind, endpoint_path_param, as_table_arg, save_suppressed = (
        _endpoint_policy(str(fn_name), inputs, bool(finalized), as_table_arg)
    )

    # Glue chains: MATLAB structs → GlueSpec, language "matlab". Bodies stay
    # unexecuted here (a .m function cannot run in Python); only their source
    # hashes matter to prepare, which is all identity needs.
    glue_arg = _reconstruct_glue_chains(glue)

    # Where: accept None or a Filter object. (Empty string sometimes
    # arrives from MATLAB when no filter was supplied; treat it as None.)
    where_arg = where if where not in ("", None) else None

    # schema_keys/schema_filter: same Step-0-equivalent logic as
    # scidb.foreach.py's pure-Python path (foreach.py:~379), reused here so
    # MATLAB needs no DB-querying code of its own — the resulting []
    # placeholders resolve via _for_each_prepare's existing generic
    # empty-list-from-DB resolver, identical to the pure-Python path.
    py_schema_keys = None if isinstance(schema_keys, type(None)) else schema_keys
    py_schema_filter = None if isinstance(schema_filter, type(None)) else schema_filter
    if py_schema_filter is not None or py_schema_keys is not None:
        schema_db = resolved_db
        if schema_db is None:
            try:
                from scidb.database import get_database

                schema_db = get_database()
            except Exception:
                schema_db = None
        if schema_db is None:
            raise ValueError(
                "schema_filter/schema_keys require a database connection, but no db "
                "was provided and no global database is configured."
            )
        import scifor as _scifor

        iterate_keys = (
            list(py_schema_keys)
            if py_schema_keys is not None
            else schema_db.dataset_schema_keys
        )
        meta = _scifor.expand_schema_keys(iterate_keys, meta)

        if py_schema_filter:
            from scidb.filters import SchemaKeyInFilter

            for key, values in dict(py_schema_filter).items():
                if key in meta:
                    meta[key] = list(values)
                else:
                    constraint = SchemaKeyInFilter(key, list(values))
                    where_arg = constraint if where_arg is None else (where_arg & constraint)

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
                where=where_arg,
                _pre_combo_hook=None,
                _cancel_check=None,
                metadata_iterables=meta,
                glue=glue_arg,
                glue_language="matlab",
                # So the preview counts the combos the real run will do.
                locations=locations,
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

    # skip_computed: build the same pre-combo skip hook scidb.for_each builds
    # in its Step 1.6 (which the MATLAB path bypasses by calling
    # _for_each_prepare directly). Feed it the MATLAB-computed fn_hash so the
    # function-hash comparison matches what the save path stored — the sentinel
    # fn's own hash would never match, forcing eternal recompute. The hook is
    # applied inside _for_each_prepare's Step 14 combo filter, so the returned
    # full_combos already exclude skippable combos.
    pre_combo_hook = None
    if skip_computed and outputs:
        skip_db = resolved_db
        if skip_db is None:
            try:
                from scidb.database import get_database

                skip_db = get_database()
            except Exception:
                skip_db = None
        if skip_db is None:
            Log.warn(
                "[bridge] skip_computed=True ignored: no database available "
                "(pass db= or configure a global database)"
            )
        else:
            pre_combo_hook = _build_skip_hook(
                fn,
                outputs,
                skip_db,
                inputs,
                as_table=as_table_arg,
                distribute=bool(distribute),
                fn_hash=fn_hash,
            )
            Log.info(
                f"[bridge] skip_computed=True: built skip hook for {fn_name} "
                f"(fn_hash={fn_hash[:12] if fn_hash else '<none>'})"
            )

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
        _pre_combo_hook=pre_combo_hook,
        _cancel_check=None,
        metadata_iterables=meta,
        glue=glue_arg,
        glue_language="matlab",
        # The save happens in a SECOND RPC, which reads this off the cached
        # state rather than being told again — one fact, one place.
        endpoint_kind=endpoint_kind or None,
    )

    if state is None:
        raise RuntimeError(
            "for_each_prepare returned None — dry_run is not supported "
            "on the MATLAB-driven path."
        )

    # Override the sentinel's auto-computed hash with the MATLAB hash so
    # the recorded version_keys identify the real MATLAB function source.
    state.config_keys["__fn_hash"] = fn_hash

    # Schema location selection. The Python path hands `locations=` to
    # scifor.for_each, which applies it to the combo list; MATLAB's loop is
    # `+scifor/for_each.m` and never sees that argument, so the SAME filter is
    # applied here to the combos this bridge is about to hand over. Full
    # parity, including the RAGGED half a generated `subject = [...]` value
    # list cannot express, without touching a .m file.
    #
    # Placed before the PathOutput pre-resolution below, which builds one
    # resolved path per combo and asserts the two lists are the same length.
    if locations is not None and not isinstance(locations, type(None)):
        from scifor.locations import filter_combos

        state.full_combos = filter_combos(
            state.full_combos, locations, context=f"matlab for_each({fn_name})"
        )

    # PathInput special case: scidb's _load_input wraps PathInput in a
    # PerComboLoader because PathInput's load() is template substitution,
    # not a DB lookup — it cannot be bulk-loaded. MATLAB's +scifor/for_each.m
    # natively handles scifor.PathInput when _resolve_pathinput=true is
    # set, so unwrap the sentinel back to the PathInput instance and let
    # MATLAB resolve it per-combo. Other per-combo loader kinds are not
    # yet supported on the MATLAB path; surface a clear error so the user knows.
    from scifor import Fixed as _Fixed
    from scifor.pathinput import PathInput as _SciforPathInput

    unsupported_per_combo = []
    for k, v in list(state.loaded_inputs.items()):
        if not isinstance(v, (PerComboLoader, PerComboLoaderMerge)):
            continue
        # Unwrap PerComboLoader to its underlying Fixed / PathInput / etc.
        spec = v.spec if isinstance(v, PerComboLoader) else v.merge_spec
        # Direct PathInput
        if isinstance(spec, _SciforPathInput):
            state.loaded_inputs[k] = spec
            continue
        # Fixed wrapping a PathInput: Fixed is the same class in scifor and
        # scidb now, so the spec already IS what MATLAB's resolve_data_spec
        # expects — pass it through unchanged.
        if isinstance(spec, _Fixed) and isinstance(spec.data, _SciforPathInput):
            state.loaded_inputs[k] = spec
            continue
        # Anything else (e.g. ColumnSelection over a var type with no load support)
        # is not yet supported on the MATLAB path.
        unsupported_per_combo.append(k)
    if unsupported_per_combo:
        raise NotImplementedError(
            f"MATLAB-driven scidb.for_each does not yet support per-combo "
            f"loaders for inputs: {unsupported_per_combo}. These arise when "
            f"a PathInput or Fixed(PathInput) is wrapped in a ColumnSelection "
            f"or Merge, or when a variable type cannot be bulk-loaded. "
            f"Call from Python instead, or restructure to use bulk-loadable types."
        )

    # --- The two internal names that cross the MATLAB boundary ---
    # The state cache keeps the original names so Python's save path
    # (_for_each_save_resolved → _save_results) reads the result table it
    # expects; only the MATLAB-facing copies are renamed. `__record_id` is
    # the frame column `for_each.m` selects rows by; `__combo` is the combo's
    # handle into `row_selection` below (and comes back on every result row,
    # which is how the save finds each row's Selection).
    _bindings = state.bindings
    rid_rename_map = {
        RECORD_ID_COLUMN: _sanitize_rid_key(RECORD_ID_COLUMN),
        COMBO_KEY: _sanitize_rid_key(COMBO_KEY),
    }

    # "Python decides, MATLAB applies": which rows of each input a
    # combination reads is its `Selection`, serialised here aligned with
    # full_combos — `{param: [rid, ...]}` per combo —
    # so `for_each.m` selects rows with ismember on x__record_id and never
    # needs Python per combo. Python's own loop reads the same selections
    # through scifor's `_select_rows` hook: one rule, two runtimes.
    #
    # Aligned by walking `state.full_combos` — NOT by dumping
    # `bindings.selections`: selections are registered for every combination
    # at expansion, and the skip hook removes combinations from
    # `full_combos` AFTERWARDS, so the two lists differ in length whenever
    # skip_computed skipped anything (the first bridge test run, 2026-09-20).
    row_selection = []
    for combo in state.full_combos:
        sel = _bindings.selection_of(combo)
        row_selection.append(
            {param: list(rids) for param, rids in sel.rids.items()} if sel else {}
        )

    # Loaded inputs: rename DataFrame columns (and inside wrappers)
    matlab_loaded_inputs = {
        name: _rename_rid_columns_in_value(val, rid_rename_map)
        for name, val in state.loaded_inputs.items()
    }

    # Pre-resolve PathOutputs per combo Python-side (the PathInput
    # "Python decides, MATLAB applies" pattern): injected branch_param
    # placeholder keys (which may be dotted, e.g. ``bandpass.low_hz``) can
    # never cross as MATLAB struct fields, so the RESOLVED path strings
    # cross instead, aligned with full_combos. ``{ColName}`` tokens are left
    # unresolved for MATLAB's for_columns loop (literal replace composes).
    from scifor import PathOutput as _SciforPathOutput

    resolved_path_outputs = {}
    for _name, _spec in inputs.items():
        if isinstance(_spec, _SciforPathOutput):
            resolved_path_outputs[_name] = [
                str(_spec.resolve(combo, None)) for combo in state.full_combos
            ]
            assert len(resolved_path_outputs[_name]) == len(state.full_combos)

    # Full combos: rename keys (single pass with list comprehension), and
    # DROP injected PathOutput placeholder keys — MATLAB never needs them
    # (paths are pre-resolved above) and dotted names are invalid fields.
    _path_extras = state.path_extra_keys or set()
    matlab_full_combos = [
        {rid_rename_map.get(k, k): v for k, v in combo.items() if k not in _path_extras}
        for combo in state.full_combos
    ]

    # Extended metadata iterables: rename keys
    matlab_meta_iters = {
        rid_rename_map.get(k, k): v
        for k, v in state.extended_metadata_iterables.items()
    }

    # (scifor's schema is no longer extended for a run — MATLAB's
    # scifor.get_schema sees the dataset schema, and row selection travels
    # as `row_selection` instead.)

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
        "where": where,
        "endpoint_kind": endpoint_kind,
        "save_suppressed": save_suppressed,
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
        "row_selection": row_selection,
        "record_id_column": rid_rename_map[RECORD_ID_COLUMN],
        "combo_key": rid_rename_map[COMBO_KEY],
        "output_names": state.output_names,
        "extended_metadata_iterables": matlab_meta_iters,
        "fn_name": state.fn_name,
        "fn_hash": fn_hash,
        "endpoint_kind": endpoint_kind or "",
        "path_param": endpoint_path_param or "",
        "as_table_effective": as_table_arg,
        "resolved_path_outputs": resolved_path_outputs,
        # {param_name: [data column names]} for inputs whose records are
        # MATLAB structs, stored one DuckDB column per field. MATLAB's
        # +scifor/for_each.m rebuilds the struct per combo; without this the
        # spread columns reach the user function as a 1xN table and every
        # field access yields a 1x1 cell. See scidb._resolve_mapping_inputs.
        "mapping_inputs": {k: list(v) for k, v in (state.mapping_inputs or {}).items()},
        # {param: [{name, per_schema_key}]} — the glue nodes MATLAB must apply
        # to that param's table, in order, between prepare and the loop. The
        # provenance side is already done (state.glue_virtual); this is purely
        # "which bodies to call".
        "glue_chains": {
            param: [
                {"name": s.name, "per_schema_key": bool(s.per_schema_key)}
                for s in chain
            ]
            for param, chain in (state.glue_chains or {}).items()
        },
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
    from scifor.colname import ColName as _SciforColName
    from scifor.column_selection import ColumnSelection as _SciforColSel
    from scifor.fixed import Fixed as _SciforFixed
    from scifor.merge import Merge as _SciforMerge
    from scifor.pathinput import PathInput as _SciforPathInput

    if isinstance(val, pd.DataFrame):
        return {"kind": "dataframe", "data": val}
    if isinstance(val, _SciforColName):
        # _convert_inputs only ever leaves a deferred (no-arg) ColName in
        # loaded_inputs (static ColName(MyVar) was resolved to a string).
        # MATLAB rebuilds scifor.ColName() to substitute the current column.
        return {"kind": "colname"}
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
            "iterate": bool(getattr(val, "iterate", False)),
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
    from scifor import PathOutput as _SciforPathOutput

    if isinstance(val, _SciforPathOutput):
        # PathOutput crosses as its template; the ACTUAL per-combo values
        # come from prepare's ``resolved_path_outputs`` (Python pre-resolves
        # so injected branch_param placeholder keys never cross the bridge).
        # MATLAB rebuilds scifor.PathOutput(template) as the fallback for
        # pure-MATLAB resolution.
        return {"kind": "path_output", "template": str(val.template)}
    return {"kind": "raw", "value": val}


def for_each_save(
    handle, result_dataframes, save: bool = True, introspect: bool = False
):
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
    introspect : bool
        If True, append introspection columns to the returned DataFrame
        (_record_id_*, _branch_params_*, _call_id, _config_keys, _where).
        _branch_params_* and _config_keys are JSON strings for MATLAB
        compatibility; all other introspect columns are plain strings.

    Returns
    -------
    pandas.DataFrame
        The merged result table, with one row per combo and metadata +
        output columns. Same shape Python's ``scidb.for_each`` returns.
    """
    import pandas as pd
    from scidb.foreach import _for_each_save_resolved
    from scifor.foreach import spread_nested_results as _spread_nested_results

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
    # Endpoint DRAFT (finalized=False): policy computed once in prepare via
    # _endpoint_policy; the save phase is suppressed here Python-side so
    # MATLAB never re-implements the rule. Draft artifact stamping still
    # runs inside _for_each_save_resolved.
    if cached.get("save_suppressed"):
        save = False

    # Reverse the bridge-boundary sanitization on the way back: MATLAB
    # produced result tables whose columns include the sanitized combo handle
    # (``x__combo``); Python's save path (state.bindings, _save_results)
    # expects ``__combo`` to find each row's Selection.
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
            df.rename(
                columns={c: reverse_map[c] for c in df.columns if c in reverse_map}
            )
            if isinstance(df, pd.DataFrame)
            else df
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

    # MATLAB's loop always runs in nested mode (+scidb/for_each.m passes
    # _nest_table_outputs=true), so each output cell holds the combo's whole
    # return value. Python's loop would now decide whether a returned table's
    # ROWS are separately addressed (it carries a schema key the combo did
    # not pin) — that decision lives in scifor, and applying it here is what
    # keeps the two languages filing a labelled table identically. Before
    # this call a 73-row (subject, session) table saved as ONE dataset-level
    # record on the MATLAB path (scidb.log 2026-09-15 19:16).
    #
    # Single-output runs only. scifor's spread lays every output's columns
    # flat side by side, and _save_results then files ALL data columns under
    # EACH output — Python's multi-table-output limitation. The nested path
    # keeps each MATLAB output's table separate, so it stays in force there
    # rather than importing that limitation.
    nested_shape = result_tbl.shape
    if len(state.output_names) == 1:
        result_tbl = _spread_nested_results(
            result_tbl,
            list(state.output_names),
            list(state.current_schema_keys or []),
        )
    elif len(state.output_names) > 1:
        _Log.info(
            f"[bridge] for_each_save: {len(state.output_names)} outputs — spread "
            f"rule not applied (single-output runs only); each output's table "
            f"is saved whole per combination"
        )

    _Log.info(
        f"[bridge] for_each_save: handle={handle}, "
        f"nested shape={nested_shape}, "
        f"result_tbl shape={result_tbl.shape}, "
        f"columns={list(result_tbl.columns)}"
    )

    result_tbl = _for_each_save_resolved(
        state=state,
        result_tbl=result_tbl,
        # state.inputs, not the cached spec: prepare folded any constant-fed
        # glue chain into the values, and that folded dict is what
        # ForEachConfig hashed. Same rule as scidb.for_each's own save call.
        inputs=state.inputs if state.inputs is not None else inputs,
        outputs=outputs,
        save=bool(save),
        db=db if db is not None and not isinstance(db, type(None)) else None,
    )

    if introspect and result_tbl is not None and not result_tbl.empty:
        import json as _json

        from scidb.foreach import _apply_introspect

        cached_where = cached.get("where")
        result_tbl = _apply_introspect(result_tbl, state, cached_where)
        # Serialize dict-valued columns to JSON strings for MATLAB compatibility.
        for col in list(result_tbl.columns):
            if col.startswith("_branch_params_"):
                result_tbl[col] = result_tbl[col].apply(
                    lambda x: _json.dumps(x) if isinstance(x, dict) else str(x)
                )
        # _where is None when for_each ran without a where= filter. A Python
        # None crosses the bridge as an unconvertible cell, so MATLAB's
        # ``string(result.("_where")(1))`` raises MustBeConvertibleCellArray.
        # Coerce None -> "" so the column is a uniform string the empty-filter
        # introspect tests (and MATLAB consumers) can read directly.
        if "_where" in result_tbl.columns:
            result_tbl["_where"] = result_tbl["_where"].apply(
                lambda x: "" if x is None else str(x)
            )

    return result_tbl


def normalize_stat_result(
    json_str: str,
    report_path: str = "",
    finalized: bool = False,
    fn_name: str = "stat_fn",
) -> str:
    """Bridge entry: canonicalize a MATLAB stat_ result into the stored
    JSON payload — the exact bytes the Python stat_ wrapper would store for
    the same result (``scidb.foreach.normalize_stat_payload``).

    MATLAB's ``jsonencode`` differs from Python's ``json.dumps`` (key order,
    float formatting), so the MATLAB stat_ wrapper jsonencode's the user's
    struct and routes it through here; skip_computed's content identity
    across the two languages depends on this single normalization point.
    ``report_path`` is "" for none (draft or no PathOutput).
    """
    from scidb.foreach import normalize_stat_payload

    return normalize_stat_payload(
        str(json_str),
        str(report_path) or None,
        bool(finalized),
        fn_name=str(fn_name),
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


def set_pathinput_project_root(root) -> str:
    """Bridge entry: pin the directory rootless PathInputs resolve against.

    A ``PathInput`` declared with no ``root_folder`` resolves relative paths
    against scifor's project root, which is found by walking up from the
    **cwd**. Under MATLAB the cwd is wherever the user's MATLAB is sitting --
    for a GUI-generated command, a temp script directory -- so the walk finds
    the wrong project or none at all, and every relative template misses.

    The caller knows the project; the cwd does not. ``+scidb/entities.m``
    calls this with the project root it was given, and the GUI's generated
    command emits it in the preamble so the guarantee holds even for a
    project with no entities file.

    Note this pins *resolution* only -- it does NOT put the root into
    ``root_folder``, which is part of a PathInput's recorded identity. The
    GUI used to rewrite ``root_folder`` here instead, which made a run
    recorded from the GUI un-matchable against its own declaration and grew
    an ``__unresolved__`` ghost node on the canvas; see
    ``scifor.pathinput.set_project_root``.
    """
    from scidb.log import Log
    from scifor.pathinput import set_project_root

    resolved = set_project_root(str(root) if root is not None else None)
    Log.info(
        "[pathinput] MATLAB pinned the PathInput project root to %s",
        resolved,
        layer="matlab",
    )
    return str(resolved) if resolved is not None else ""


def load_entities(project_start=None) -> dict:
    """The project's TOML entities file, flattened for MATLAB marshaling.

    MATLAB has no TOML reader, so ``+scidb/entities.m`` gets the
    declarations through here and rebuilds them as MATLAB
    ``scidb.Parameter``/``scidb.PathInput`` objects. Only plain
    dicts/lists/scalars cross the bridge -- handing MATLAB the constructed
    Python objects would give it Python proxies, not the MATLAB classes
    ``scidb.for_each`` expects.

    ``root_folder`` is ``None`` when unset (MATLAB reads that as ``[]``),
    never ``""``, so "no root" stays distinguishable from "rooted at the
    empty string".

    Returns ``{path, variables, parameters, path_inputs, errors}``. Errors
    are pre-rendered strings: MATLAB shows them as warnings, and a rejected
    entry must be visible there too, not only in the GUI.
    """
    from pathlib import Path

    from scidb import entities
    from scidb.log import Log

    result = entities.load_for_project(project_start)

    # Loading a project's entities is also the moment MATLAB tells Python
    # which project is running, so pin scifor's PathInput resolution base
    # here. Any script that calls scidb.entities(PROJECT_ROOT) -- generated
    # or hand-written -- then resolves a rootless PathInput against that
    # project instead of MATLAB's cwd, with no second call to remember.
    root = entities.project_root(project_start)
    if root is not None:
        set_pathinput_project_root(root)

    def _arms(obj):
        alternatives = getattr(obj, "alternatives", None)
        return list(alternatives) if alternatives is not None else [obj]

    path_inputs = {}
    for name, obj in result.path_inputs.items():
        path_inputs[name] = [
            {
                "template": getattr(arm, "path_template", ""),
                "root_folder": (
                    str(arm.root_folder)
                    if getattr(arm, "root_folder", None) is not None
                    else None
                ),
            }
            for arm in _arms(obj)
        ]

    payload = {
        "path": str(result.path),
        "variables": list(result.variables),
        "parameters": {
            name: list(param.values) for name, param in result.parameters.items()
        },
        "path_inputs": path_inputs,
        "errors": [err.describe() for err in result.errors],
    }
    if not str(result.path):
        # ``EntitiesFile(path=Path())`` -- no project config was found from
        # the starting point, so nothing was loaded. This used to print as
        # "from ." at INFO, which is only legible if you know it is
        # str(Path()), and it is exactly what a generated script that
        # forgot to pass its project root produces.
        Log.warn(
            "[entities] MATLAB load found NO entities file: no project config "
            "in any ancestor of %s (project_start=%r, cwd=%s). Declared "
            "Parameters/PathInputs will not be in scope; pass the project "
            "root as scidb.entities(PROJECT_ROOT).",
            Path(project_start) if project_start is not None else Path.cwd(),
            project_start,
            Path.cwd(),
            layer="matlab",
        )
    else:
        Log.info(
            "[entities] MATLAB load: %d variable(s), %d parameter(s), %d path "
            "input(s), %d rejected, from %s",
            len(payload["variables"]),
            len(payload["parameters"]),
            len(payload["path_inputs"]),
            len(payload["errors"]),
            payload["path"],
            layer="matlab",
        )
    return payload


def ensure_variable_classdefs(names, project_start=None) -> dict:
    """Bridge entry: write MATLAB classdef stubs for declared variables that
    have none, and report where they went.

    ``+scidb/entities.m`` calls this with the names MATLAB itself could not
    resolve (``exist(name, 'class') ~= 8``) -- the decision of *which* names
    need a file is MATLAB's, because MATLAB's path is the only authority on
    what already resolves. See ``scimatlab.stubs``.

    ``names`` arrives from MATLAB as a cell of char/string; each entry is
    stringified here so both marshal correctly.
    """
    from scidb.log import Log
    from scimatlab.stubs import write_variable_classdefs

    raw = list(names) if names is not None else []
    wanted = [str(n) for n in raw]
    # Both sides of the marshalling boundary, because a MATLAB `string` vs
    # `char` mismatch mangles a name *here* and nowhere downstream can tell
    # that from a name that arrived wrong. Only logged when str() actually
    # changed something, so the common case stays quiet.
    changed = [(a, b) for a, b in zip(raw, wanted) if not isinstance(a, str) or a != b]
    if changed:
        Log.debug(
            "[bridge] ensure_variable_classdefs marshalled %s",
            "; ".join(f"{a!r} -> {b!r}" for a, b in changed),
            layer="matlab",
        )
    return write_variable_classdefs(wanted, project_start=project_start)


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
            "combos": [],
            "original_count": 0,
            "present_keys": [],
            "values_by_key": {},
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
        "combos": combos,
        "original_count": original_count,
        "present_keys": present_keys,
        "values_by_key": values_by_key,
    }


def compute_matlab_function_hash(
    source_text: str, name: str = "", unpack_output: bool = False
) -> str:
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
    return sha256(source_text.encode("utf-8")).hexdigest()


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
        result.append(flat[pos : pos + length].tolist())
        pos += length
    return result


def split_df_to_dataframes(df, row_counts):
    """Split a concatenated DataFrame into a list of sub-DataFrames by row counts.

    Used by MATLAB's to_python Strategy 1 (homogeneous table concat) to
    efficiently convert a cell column whose elements are multi-row tables.
    MATLAB concatenates all K tables into one DataFrame (KM rows total),
    sends it across the bridge once, then calls this function to slice it
    back into K DataFrames each with the original number of rows.

    This avoids the per-element bridge cost of Strategy 3 (K separate
    to_python calls) while correctly handling tables with any row count —
    unlike the old to_dict('records') approach which always produced
    single-row DataFrames.

    Parameters
    ----------
    df : pandas.DataFrame
        Concatenated DataFrame (KM rows).
    row_counts : numpy.ndarray or list of int
        Integer array where ``row_counts[i]`` is the row count of the
        i-th original sub-DataFrame.

    Returns
    -------
    list of pandas.DataFrame
        One DataFrame per entry in *row_counts*, sliced from *df* in order.
    """
    import numpy as np

    counts = np.asarray(row_counts).ravel().tolist()
    result = []
    pos = 0
    for n in counts:
        n = int(n)
        result.append(df.iloc[pos : pos + n].reset_index(drop=True))
        pos += n
    return result


# ---------------------------------------------------------------------------
# Bulk array transfer
# ---------------------------------------------------------------------------

# numpy (dtype.kind, itemsize) -> MATLAB class accepted by typecast().
# Deliberately partial: every dtype NOT listed here has no fixed-width raw
# buffer MATLAB can reinterpret (object, str, bytes, datetime, timedelta,
# complex) or no typecast target (float16, float128), and must keep the
# element-by-element path in +scidb/+internal/from_python.m.
_BUFFER_DTYPES = {
    ("f", 4): "single",
    ("f", 8): "double",
    ("i", 1): "int8",
    ("i", 2): "int16",
    ("i", 4): "int32",
    ("i", 8): "int64",
    ("u", 1): "uint8",
    ("u", 2): "uint16",
    ("u", 4): "uint32",
    ("u", 8): "uint64",
    ("b", 1): "logical",
}


def ndarray_to_buffer(arr):
    """Describe a numpy array as ONE raw byte buffer for MATLAB.

    MATLAB's ``from_python`` used to convert every numpy array via
    ``ndarray.tolist()`` + ``cell(py_list)``, which crosses the Python/MATLAB
    boundary once *per element*. At scale that is the whole cost: a 419-record
    EMG variable spread over 10 array columns is 1.74e8 samples, i.e. 1.74e8
    crossings, which reads as a hang rather than as slowness.

    A raw buffer crosses once. MATLAB converts ``py.bytes`` to ``uint8`` in a
    single memcpy and ``typecast``s it back to the original class, so the
    element count stops mattering.

    The buffer is written in **Fortran (column-major) order** so MATLAB can
    ``reshape(vec, shape)`` directly — MATLAB is column-major, numpy is not.
    For the 1-D case (the common one here) the two orders coincide.

    Parameters
    ----------
    arr : numpy.ndarray
        Array to describe. Anything ``numpy.asarray`` accepts also works.

    Returns
    -------
    dict
        ``ok`` (bool), and when ok:
        ``buffer`` (bytes, Fortran order), ``matlab_class`` (str, a typecast
        target or ``"logical"``), ``count`` (int, element count),
        ``itemsize`` (int, bytes per element), ``reason`` (str, empty).
        When not ok, ``reason`` says why and the caller falls back to the
        element-by-element path. Declining is never an error — every dtype
        this refuses still converts correctly, just slowly.

        ``matlab_class`` names the dtype's *typecast* target, not the class
        the user finally sees: MATLAB casts every numeric result to double
        afterwards, because that is what the element-by-element path
        returned and what TestDataRoundTrip pins. Only ``logical`` survives
        as itself.

    Notes
    -----
    ``tobytes`` copies, so this transiently doubles the array's memory in
    Python. That is the price of a single crossing and is still far cheaper
    than the MATLAB-side cell array the old path built (one ~112-byte mxArray
    per element).
    """
    import numpy as np

    empty = {
        "ok": False,
        "buffer": b"",
        "matlab_class": "",
        "count": 0,
        "itemsize": 0,
        "reason": "",
    }

    try:
        a = np.asarray(arr)
    except Exception as exc:  # not array-like at all
        return {**empty, "reason": f"not array-like: {type(exc).__name__}"}

    key = (a.dtype.kind, int(a.dtype.itemsize))
    matlab_class = _BUFFER_DTYPES.get(key)
    if matlab_class is None:
        return {**empty, "reason": f"dtype {a.dtype!s} has no raw-buffer form"}

    try:
        buf = a.tobytes(order="F")
    except Exception as exc:  # e.g. MemoryError on a very large array
        return {**empty, "reason": f"tobytes failed: {type(exc).__name__}"}

    return {
        "ok": True,
        "buffer": buf,
        "matlab_class": matlab_class,
        "count": int(a.size),
        "itemsize": int(a.dtype.itemsize),
        "reason": "",
    }


def flatten_sequences(py_list):
    """Flatten a list of numeric/boolean sequences into a single array with lengths.

    Used by MATLAB's from_python to handle object-dtype columns containing
    lists of varying-length arrays (numeric or boolean). Flattens all sequences
    into one numpy array and records their lengths, enabling a SINGLE Python→MATLAB
    bridge crossing instead of N crossings.

    Parameters
    ----------
    py_list : list
        List of sequences (lists, tuples, or numpy arrays) containing
        homogeneous typed values (numeric or boolean).  Sequences may have
        different lengths.

    Returns
    -------
    tuple of (numpy.ndarray, numpy.ndarray) or (None, None)
        - flat_array: Concatenated array (all sequences flattened)
        - lengths: int64 array where lengths[i] = length of sequence i
        Returns (None, None) if conversion failed (mixed types, non-numeric/bool, etc.)

    Examples
    --------
    >>> flatten_sequences([[1, 2, 3], [4, 5], [6, 7, 8, 9]])
    (array([1, 2, 3, 4, 5, 6, 7, 8, 9]), array([3, 2, 4]))

    >>> flatten_sequences([[True, False], [True], [False, False, True]])
    (array([True, False, True, False, False, True]), array([2, 1, 3]))
    """
    import numpy as np

    if not isinstance(py_list, list) or len(py_list) == 0:
        return None, None

    try:
        # Check if all elements are sequences and convert to arrays
        arrays = []
        for elem in py_list:
            if not isinstance(elem, (list, tuple, np.ndarray)):
                return None, None
            arr = np.asarray(elem)
            # Reject if not typed (object dtype = heterogeneous or non-numeric/bool)
            if arr.dtype.kind == "O":
                return None, None
            # Reject anything that is not a flat sequence. The contract here is
            # "list of variable-length numeric SEQUENCES", and the caller splits
            # the concatenated result back apart with `lengths` — which only
            # describes a 1-D element. For a 2-D element `len(arr)` is its ROW
            # count, so a single 4x3 matrix reports length 4 and MATLAB slices
            # the first 4 elements of a 12-element buffer: a 4x1 column where a
            # 4x3 matrix belongs, silently. Decline and let the element-by-
            # element path convert each array with its own shape intact.
            if arr.ndim != 1:
                return None, None
            arrays.append(arr)

        # Record lengths and concatenate
        lengths = np.array([len(arr) for arr in arrays], dtype="int64")
        flat = np.concatenate(arrays)

        return flat, lengths

    except Exception:
        return None, None


def convert_nested_dicts_arrays_to_lists(py_list):
    """Convert all numpy arrays in nested dicts to Python lists.

    Used by MATLAB's from_python to handle lists of nested dicts containing
    numpy arrays (e.g., structs stored as JSON in DuckDB). Recursively walks
    the structure and converts all numpy arrays to Python lists, avoiding
    expensive numpy→MATLAB bridge crossings for each array.

    Parameters
    ----------
    py_list : list
        List of dicts with potentially nested structure. Leaf values should
        be numpy arrays. Works with arbitrary nesting depth.

    Returns
    -------
    list or None
        Modified list where all numpy arrays have been converted to Python
        lists. Structure and nesting are preserved. Returns None if input
        is not a list of dicts.

    Examples
    --------
    >>> data = [{'a': np.array([1, 2]), 'b': {'c': np.array([3, 4])}}]
    >>> convert_nested_dicts_arrays_to_lists(data)
    [{'a': [1, 2], 'b': {'c': [3, 4]}}]
    """
    import numpy as np

    if not isinstance(py_list, list) or len(py_list) == 0:
        return None

    try:

        def convert_value(val):
            """Recursively convert a value, handling arbitrary nesting."""
            if isinstance(val, np.ndarray):
                # Convert numpy array to Python list
                return val.tolist()
            elif isinstance(val, dict):
                # Recursively convert dict values
                return {k: convert_value(v) for k, v in val.items()}
            elif isinstance(val, (list, tuple)):
                # Recursively convert list/tuple elements
                return [convert_value(item) for item in val]
            else:
                # Native Python types (int, float, str, bool, None) pass through
                return val

        # Check all elements are dicts and convert them
        result = []
        for item in py_list:
            if not isinstance(item, dict):
                # Not all elements are dicts - can't use this fast path
                return None
            result.append(convert_value(item))

        return result

    except Exception:
        return None


def convert_nested_dicts_to_json(py_list):
    """Convert list of nested dicts with numpy arrays to JSON string.

    Combines array-to-list conversion with JSON serialization for efficient
    transfer to MATLAB. Enables a SINGLE bridge crossing (the JSON string)
    instead of N dict conversions.

    Parameters
    ----------
    py_list : list
        List of dicts with potentially nested structure containing numpy arrays.

    Returns
    -------
    str or None
        JSON string representation of the list, or None if conversion failed.

    Examples
    --------
    >>> data = [{'a': np.array([1, 2]), 'b': {'c': np.array([3])}}]
    >>> convert_nested_dicts_to_json(data)
    '[{"a": [1, 2], "b": {"c": [3]}}]'
    """
    import json

    # First convert all numpy arrays to Python lists
    converted = convert_nested_dicts_arrays_to_lists(py_list)
    if converted is None:
        return None

    try:
        # Serialize to JSON string (single string for entire list)
        return json.dumps(converted)
    except Exception:
        return None


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


def save_batch_bridge(
    type_name,
    data_values,
    metadata_keys,
    metadata_columns,
    common_metadata=None,
    db=None,
    row_heights=None,
):
    """Bridge function for MATLAB save_from_table.

    Accepts columnar data (one list per column) from MATLAB and assembles
    the (data_value, metadata_dict) tuples that DatabaseManager.save_batch()
    expects.  This avoids per-row MATLAB↔Python round-trips.

    Parameters
    ----------
    type_name : str
        Variable class name (e.g. "StepLength").
    data_values : list, numpy array, or pandas DataFrame
        One data value per row, or a bulk DataFrame/array when row_heights
        is provided (Strategy A / vertcat mode from MATLAB).
    metadata_keys : list of str
        Metadata column names, same order as metadata_columns.
    metadata_columns : list of (list or numpy array)
        One inner list/array per metadata key, each with one value per row.
    common_metadata : dict or None
        Extra metadata applied to every row.
    db : DatabaseManager or None
        Optional database; uses global default when None.
    row_heights : numpy array or None
        When provided, data_values is a bulk structure (DataFrame or ndarray)
        and row_heights[i] is the number of rows that belong to record i.
        Python splits it back into N per-record slices (zero-copy views).
        None = legacy per-row list mode (backward-compatible).

    Returns
    -------
    list of str
        Record IDs for each saved row.
    """
    import time as _time

    import numpy as np
    import pandas as pd
    from scidb.database import get_database
    from scidb.log import Log
    from scidb.variable import BaseVariable

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

    # Determine how to build data_list from data_values.
    t_split = 0.0
    bulk_mode = "list"

    heights_arr = None
    if row_heights is not None and not isinstance(row_heights, type(None)):
        heights_arr = np.asarray(row_heights, dtype=int).ravel()

    if heights_arr is not None and isinstance(data_values, pd.DataFrame):
        # Strategy A result: bulk DataFrame → split into N per-record DataFrames.
        # reset_index(drop=True) normalises the index to 0..height-1 on every
        # slice so canonical_hash produces the same result as the per-row path
        # (which always received a freshly constructed DataFrame with index [0]).
        offsets = np.concatenate([[0], np.cumsum(heights_arr)])
        _t = _time.perf_counter()
        data_list = [
            data_values.iloc[int(offsets[i]) : int(offsets[i + 1])].reset_index(
                drop=True
            )
            for i in range(len(heights_arr))
        ]
        t_split = _time.perf_counter() - _t
        bulk_mode = "vertcat_df"
    elif heights_arr is not None and isinstance(data_values, np.ndarray):
        # Strategy A result: bulk ndarray → split into N row-slice views.
        offsets = np.concatenate([[0], np.cumsum(heights_arr)])
        _t = _time.perf_counter()
        data_list = [
            data_values[int(offsets[i]) : int(offsets[i + 1])]
            for i in range(len(heights_arr))
        ]
        t_split = _time.perf_counter() - _t
        bulk_mode = "vertcat_arr"
    elif hasattr(data_values, "tolist"):
        # Numpy array (isnumeric fast path or Strategy B flatten result).
        data_list = data_values.tolist()
        bulk_mode = "numpy_list"
    else:
        # Plain Python list (Strategy B split_flat_to_lists, Strategy C, or
        # string/cellstr paths).
        data_list = [v.item() if hasattr(v, "item") else v for v in data_values]

    meta_lists = []
    for j in range(len(keys)):
        col = metadata_columns[j]
        if isinstance(col, str):
            # Joined string from MATLAB (record-separator delimited)
            meta_lists.append(col.split("\x1e"))
        elif hasattr(col, "tolist"):
            meta_lists.append(col.tolist())
        else:
            meta_lists.append([v.item() if hasattr(v, "item") else v for v in col])

    n = len(data_list)
    data_items = []
    for i in range(n):
        meta = dict(common)
        for j, key in enumerate(keys):
            meta[key] = meta_lists[j][i]
        data_items.append((data_list[i], meta))

    t_convert = _time.perf_counter()
    Log.debug(
        f"[timing] save_batch_bridge({type_name}): n={n}, mode={bulk_mode}, "
        f"split={t_split:.3f}s, assembly={t_convert - t_start - t_split:.3f}s"
    )

    # save_batch returns None in the slot of any record it skipped (schema-
    # incompatible). Emit an empty string for those so the newline-delimited
    # result stays row-aligned with the input on the MATLAB side.
    _rids = _db.save_batch(cls, data_items)
    result = "\n".join(r if isinstance(r, str) else "" for r in _rids)

    Log.info(
        f"[timing] save_batch_bridge({type_name}): n={n}, mode={bulk_mode}, "
        f"split={t_split:.3f}s, total={_time.perf_counter() - t_start:.3f}s"
    )
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
        json_meta      : str   — JSON array of metadata dicts
        scalar_data    : numpy.ndarray (optional) — present when all data are scalars
    """
    import json

    import numpy as np

    py_vars = list(py_vars_list) if not isinstance(py_vars_list, list) else py_vars_list
    n = len(py_vars)

    record_ids = []
    content_hashes = []
    meta_dicts = []
    branch_params_list = []
    data = []

    for v in py_vars:
        record_ids.append(v.record_id or "")
        content_hashes.append(v.content_hash or "")
        meta = v.metadata
        meta_dicts.append(dict(meta) if meta is not None else {})
        branch_params_list.append(json.dumps(v.branch_params or {}))
        data.append(v.data)

    # Cache data for non-scalar fallback access
    batch_id = _cache_batch(data, py_vars)

    result = {
        "n": n,
        "py_vars": py_vars,
        "batch_id": batch_id,
        "record_ids": "\n".join(record_ids),
        "content_hashes": "\n".join(content_hashes),
        "json_meta": json.dumps(meta_dicts),
        "json_branch_params": "\n".join(branch_params_list),
    }

    # Scalar fast path: pack all data into a single numpy array
    if n > 0 and all(isinstance(d, (int, float)) for d in data):
        result["scalar_data"] = np.array(data, dtype=float)

    # DataFrame fast path: concatenate same-schema DataFrames into one
    # so MATLAB converts a single large table instead of N small ones
    if n > 0 and "scalar_data" not in result:
        import pandas as pd

        if all(isinstance(d, pd.DataFrame) for d in data):
            first_cols = list(data[0].columns)
            if all(list(d.columns) == first_cols for d in data):
                row_counts = [len(d) for d in data]
                concat_df = pd.concat(data, ignore_index=True)

                # Optimize object-dtype columns containing numpy arrays
                # For variable-length arrays, flatten all data into a single array
                # and send size/offset info separately. This avoids N boundary
                # crossings in MATLAB (one per cell) by doing bulk transfer.
                from scidb.log import Log

                optimized_cols = []
                flattened_data = {}
                json_columns = []
                object_cols = [
                    col for col in concat_df.columns if concat_df[col].dtype == object
                ]
                Log.debug(
                    f"wrap_batch_bridge: found {len(object_cols)} object-dtype columns to check"
                )

                for col_name in concat_df.columns:
                    col = concat_df[col_name]
                    if col.dtype == object and len(col) > 0:
                        # Check if first non-null value is a numpy array
                        first_val = None
                        for val in col:
                            if val is not None:
                                first_val = val
                                break

                        if first_val is not None:
                            first_val_type = type(first_val).__name__
                            Log.debug(
                                f'wrap_batch_bridge: column "{col_name}" first value type: {first_val_type}'
                            )

                            if isinstance(first_val, dict):
                                # JSON/dict columns - serialize to JSON strings for fast transfer
                                # MATLAB will parse these back to structs automatically
                                Log.debug(
                                    f'wrap_batch_bridge: column "{col_name}" contains dicts, serializing to JSON...'
                                )
                                import json

                                concat_df[col_name] = [
                                    json.dumps(val) if isinstance(val, dict) else val
                                    for val in col
                                ]
                                json_columns.append(col_name)
                                optimized_cols.append(col_name + " (JSON)")
                                Log.debug(
                                    f'wrap_batch_bridge: column "{col_name}" serialized to JSON strings'
                                )
                            elif isinstance(first_val, np.ndarray):
                                # Flatten array column: concatenate all arrays and store sizes
                                # This allows MATLAB to convert the entire column in 2 operations
                                # (one for flat data, one for sizes) instead of N operations
                                Log.debug(
                                    f'wrap_batch_bridge: flattening column "{col_name}"...'
                                )
                                arrays = [
                                    arr
                                    if isinstance(arr, np.ndarray)
                                    else np.array(arr)
                                    for arr in col
                                ]

                                # Get array sizes and flatten
                                sizes = np.array(
                                    [len(arr) for arr in arrays], dtype=np.int32
                                )
                                flat_data = (
                                    np.concatenate(arrays)
                                    if len(arrays) > 0
                                    else np.array([])
                                )

                                # Store flattened representation
                                flattened_data[col_name] = {
                                    "flat": flat_data,
                                    "sizes": sizes,
                                    "dtype": str(first_val.dtype),
                                }

                                # Mark column for reconstruction in MATLAB
                                concat_df[col_name] = ["__flattened__"] * len(col)

                                optimized_cols.append(col_name)
                                Log.debug(
                                    f'wrap_batch_bridge: column "{col_name}" flattened: '
                                    f"{len(flat_data)} total elements in {len(sizes)} arrays"
                                )

                if optimized_cols:
                    Log.info(
                        f"wrap_batch_bridge: optimized {len(optimized_cols)} array columns for MATLAB transfer: {optimized_cols[:5]}"
                    )
                    if flattened_data:
                        result["flattened_arrays"] = flattened_data
                    if json_columns:
                        result["json_columns"] = json_columns
                else:
                    Log.debug("wrap_batch_bridge: no array columns found to optimize")

                result["concat_df"] = concat_df
                result["concat_df_row_counts"] = np.array(row_counts, dtype=np.int64)

    return result


def load_and_extract(py_class, metadata_dict, version_id="latest", db=None, where=None):
    """Load all matching variables and extract fields in bulk.

    Combines load -> list -> wrap_batch_bridge in one Python call.
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
    from scidb.exceptions import NotFoundError

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()

    # scidb's load() raises NotFoundError when nothing matches. The MATLAB
    # caller (+scidb/BaseVariable.m) detects the no-match case via the n==0
    # sentinel in the returned dict and raises a clean ``scidb:NotFoundError``
    # itself. If the Python exception is allowed to cross the bridge it surfaces
    # in MATLAB as the opaque, generic ``MATLAB:Python:PyException`` instead,
    # breaking the documented error contract (and every verifyError test that
    # expects ``scidb:NotFoundError``). Convert it back to the empty-result
    # sentinel so MATLAB's n==0 branch fires.
    try:
        gen = _db.load(
            py_class, dict(metadata_dict), version_id=version_id, where=where
        )
        py_vars = list(gen)  # materializes entirely in Python
    except NotFoundError:
        py_vars = []
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


def to_csv_bridge(type_name, filename, **kwargs):
    """Export a variable to CSV on behalf of MATLAB ``BaseVariable.to_csv``.

    MATLAB cannot dispatch the inherited ``to_csv`` *classmethod* on a Python
    surrogate **class object**: ``methods(py_class)`` omits it (it reflects only
    the metaclass's own methods, not classmethods accessible via the MRO) even
    though Python ``hasattr(py_class, 'to_csv')`` is True. So rather than calling
    ``py_class.to_csv(...)`` from MATLAB, MATLAB calls this module-level bridge —
    the same delegation pattern ``save``/``load`` already use (they route through
    ``save_batch_bridge``/``load_and_extract`` instead of the surrogate's
    classmethods).

    ``filename`` is positional; all load options (``where=``, ``version=``,
    ``db=``, and schema-key metadata filters) arrive as ``**kwargs`` and are
    forwarded verbatim to ``to_csv`` → ``export_csv``. Column selection
    (``MyVar["col"].to_csv``) is dispatched on a ``ColumnSelection`` *instance*
    in MATLAB, which works without this bridge.
    """
    cls = get_surrogate_class(type_name)
    cls.to_csv(str(filename), **kwargs)


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

    # Via SciDuck._fetchone so execute+fetch stay under one lock. `_db` is a
    # DatabaseManager (the dataset_schema_keys read above already requires
    # one); the SciDuck it wraps is ._duck. `_db._execute` was an
    # AttributeError — DatabaseManager has no _execute.
    row = _db._duck._fetchone(
        "SELECT dtype FROM _variables WHERE variable_name = ?",
        [var_name],
    )

    if row is None:
        # Variable not yet saved — fall back to view_name
        if hasattr(py_class, "view_name"):
            return py_class.view_name()
        return var_name

    dtype_meta = json.loads(row[0])
    mode = dtype_meta.get("mode", "single_column")

    if mode == "single_column":
        col_names = list(dtype_meta.get("columns", {}).keys())
        if col_names:
            return col_names[0]
        if hasattr(py_class, "view_name"):
            return py_class.view_name()
        return var_name

    if mode == "dataframe":
        df_columns = dtype_meta.get(
            "df_columns", list(dtype_meta.get("columns", {}).keys())
        )
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

    if hasattr(py_class, "view_name"):
        return py_class.view_name()
    return var_name


# ---------------------------------------------------------------------------
# Pipeline registry bridge (endpoint-first stage 4 — MATLAB parity)
#
# Split per the D7 principle: Python owns the graph (registration, topo
# order, bindings, plan); MATLAB owns execution of MATLAB steps, driving
# them through its normal two-pass scidb.for_each. MATLAB function handles
# never cross the bridge — each MATLAB step's StepSpec holds the erroring
# sentinel (fn.__name__ only) plus __matlab__/__matlab_fn_hash__ options,
# and Python's Pipeline.run_* refuses such steps with a pointer back here.
# ---------------------------------------------------------------------------

_pipeline_cache: dict = {}
_pipeline_id_counter: int = 0
_binding_cache: dict = {}
_binding_id_counter: int = 0
_pipeline_run_cache: dict = {}
_pipeline_run_id_counter: int = 0


def _cache_pipeline(pipe) -> int:
    global _pipeline_id_counter
    _pipeline_id_counter += 1
    _pipeline_cache[_pipeline_id_counter] = pipe
    return _pipeline_id_counter


def pipeline_create(name: str, db=None, activate: bool = True) -> int:
    """Create (and by default activate) a Pipeline; returns a cache handle."""
    from scidb.pipeline import Pipeline

    _db = db if db is not None and not isinstance(db, type(None)) else None
    if _db is None:
        try:
            from scidb import get_database

            _db = get_database()
        except Exception:
            _db = None
    pipe = Pipeline(name, db=_db)
    if activate:
        pipe.activate()
    return _cache_pipeline(pipe)


def pipeline_active_name() -> str:
    """Name of the ambient active pipeline, or '' — MATLAB for_each's
    cheap registration-seam check."""
    from scidb.pipeline import active_pipeline

    active = active_pipeline()
    return active.name if active is not None else ""


def pipeline_deactivate(handle: int) -> None:
    _pipeline_cache[int(handle)].deactivate()


def pipeline_register_step(
    handle: int,
    fn_name: str,
    fn_hash: str,
    inputs_spec,
    output_class_names,
    metadata_iterables,
    where=None,
    distribute: bool = False,
    as_table=None,
    save: bool = True,
    finalized: bool = False,
    skip_computed: bool = False,
    schema_keys=None,
    schema_filter=None,
    glue=None,
) -> int:
    """Register one MATLAB for_each call as a deferred step; returns the
    step's index in the pipeline's own step list (MATLAB stores the fn
    handle + raw call args under this index).

    ``glue`` rides along so the registered step's identity matches the eager
    call's: glue names contribute to the call_id, so a step registered
    without them would be a different call site than the same step run
    directly."""
    pipe = _pipeline_cache[int(handle)]
    inputs = {
        name: _reconstruct_input_for_keys(spec)
        for name, spec in dict(inputs_spec).items()
    }
    outputs = [get_surrogate_class(n) for n in list(output_class_names)]
    sentinel = _make_matlab_fn_sentinel(fn_name)
    meta = {k: list(v) for k, v in dict(metadata_iterables).items()}
    schema_keys_arg = list(schema_keys) if schema_keys is not None else None
    schema_filter_arg = (
        {k: list(v) for k, v in dict(schema_filter).items()}
        if schema_filter is not None
        else None
    )
    pipe.register_call(
        fn=sentinel,
        inputs=inputs,
        outputs=outputs,
        metadata_iterables=meta,
        options={
            "where": where,
            "distribute": bool(distribute),
            "as_table": as_table,
            "save": bool(save),
            "finalized": bool(finalized),
            "skip_computed": bool(skip_computed),
            "schema_keys": schema_keys_arg,
            "schema_filter": schema_filter_arg,
            "glue": _reconstruct_glue_chains(glue) or None,
            "__matlab__": True,
            "__matlab_fn_hash__": fn_hash,
        },
    )
    return len(pipe.steps) - 1


def pipeline_bind(handle: int, key_map=None, params=None, iterate=None) -> int:
    """Create a use-edge binding for the pipeline; returns a binding handle.
    Bind-time validation errors (unknown/ambiguous params) raise here."""
    global _binding_id_counter

    pipe = _pipeline_cache[int(handle)]
    binding = pipe.bind(
        key_map=dict(key_map) if key_map else None,
        params=dict(params) if params else None,
        iterate={k: list(v) for k, v in dict(iterate).items()} if iterate else None,
    )
    _binding_id_counter += 1
    _binding_cache[_binding_id_counter] = binding
    return _binding_id_counter


def pipeline_use(
    parent_handle: int, child_handle: int = 0, binding_handle: int = 0
) -> None:
    """Declare a dependency: pass child_handle for an identity edge, or
    binding_handle for a bound edge (exactly one must be nonzero)."""
    parent = _pipeline_cache[int(parent_handle)]
    if int(binding_handle):
        parent.use(_binding_cache[int(binding_handle)])
    else:
        parent.use(_pipeline_cache[int(child_handle)])


def pipeline_plan(handle: int, target_name: str = "") -> list:
    """plan() forwarder; MATLAB-friendly list of dicts (combos summarized
    to a count — full combo dicts don't convert usefully)."""
    pipe = _pipeline_cache[int(handle)]
    entries = pipe.plan(target=target_name or None)
    return [
        {
            "step": e["step"],
            "pipeline": e["pipeline"],
            "endpoint": bool(e["endpoint"]),
            "state": e["state"],
            "n_combos": len(e["combos"]),
        }
        for e in entries
    ]


def pipeline_endpoints(handle: int, include_used: bool = True) -> list:
    return _pipeline_cache[int(handle)].endpoints(include_used=include_used)


def pipeline_execution_order(
    handle: int,
    mode: str = "all",
    target_name: str = "",
    include_used: bool = False,
    finalized=None,
    skip_computed: bool = True,
) -> dict:
    """Resolve the run: topo-ordered descriptors + a run handle.

    MATLAB iterates the descriptors: ``is_matlab`` steps run through
    MATLAB's own scidb.for_each (raw args stored MATLAB-side at the
    descriptor's (pipeline, step_index), overridden by the descriptor's
    post-binding surface); Python steps run via
    ``pipeline_run_python_step(run_handle, position)``. Selection,
    acknowledgment, and deactivation semantics match Python run_*.
    """
    global _pipeline_run_id_counter

    pipe = _pipeline_cache[int(handle)]
    fin = (
        None
        if finalized is None or isinstance(finalized, type(None))
        else bool(finalized)
    )
    descriptors = pipe.execution_order(
        mode=mode,
        target=target_name or None,
        include_used=bool(include_used),
        finalized=fin,
        skip_computed=bool(skip_computed),
    )
    # Re-derive the selection for the run cache (execution_order returns
    # plain data only; _execute_step needs the live pairs/order).
    pairs, order, targets = pipe._select(mode, target_name or None, bool(include_used))
    _pipeline_run_id_counter += 1
    _pipeline_run_cache[_pipeline_run_id_counter] = {
        "pipeline": pipe,
        "pairs": pairs,
        "order": order,
        "targets": targets,
        "skip_computed": bool(skip_computed),
        "finalized": fin,
    }
    # Constants must cross as plain values; drop non-primitive leftovers
    # (MATLAB re-substitutes into its own stored inputs struct).
    for d in descriptors:
        d["constant_inputs"] = {
            k: v
            for k, v in d["constant_inputs"].items()
            if isinstance(v, (int, float, str, bool, list))
        }
    return {"run_handle": _pipeline_run_id_counter, "steps": descriptors}


def pipeline_run_python_step(run_handle: int, position: int):
    """Execute the run's POSITIONth step Python-side (mixed pipelines:
    Python-registered steps run in their home language while MATLAB
    drives the order)."""
    run = _pipeline_run_cache[int(run_handle)]
    i = run["order"][int(position)]
    pipe = run["pipeline"]
    fin = (
        run["finalized"]
        if (run["finalized"] is not None and i in run["targets"])
        else None
    )
    pipe._execute_step(
        run["pairs"],
        i,
        skip_computed=run["skip_computed"],
        finalized=fin,
    )
    return True


def pipeline_run_free(run_handle: int) -> None:
    """Release a finished run's cached state."""
    _pipeline_run_cache.pop(int(run_handle), None)
