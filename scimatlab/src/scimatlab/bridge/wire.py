"""Wiring: MATLAB values and names rebuilt as the Python objects scidb takes.

The input wrappers MATLAB describes as plain specs (``Fixed``, ``Merge``,
column selections, ``ColName``, glue chains, locations, declared Parameter
names) become their Python classes here; the function MATLAB runs is
represented by a sentinel and by :class:`MatlabFunctionIdentity` (its name and
source hash, for node state); MATLAB variable classes get Python surrogates
(``register_matlab_variable``), with their codec when the class has one.
"""

from hashlib import sha256

STRING_REPR_DELIMITER = "-"


# ---------------------------------------------------------------------------
# Proxy classes
# ---------------------------------------------------------------------------


class _FunctionProxy:
    """Minimal holder so ``MatlabFunctionIdentity().fcn.__name__`` yields the function name."""

    def __init__(self, name: str):
        self.__name__ = name


class MatlabFunctionIdentity:
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
        #: → ``scimatlab.source_hash``). Read-side hash lookups must use
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
    where ``scidb.run.glue.apply_constant_glue`` applies it during prepare
    (Python's, on both run paths), recording it as a ``__constant_glue__`` history hop.
    That needs a live callable, and this process has no function registry, so
    the body is loaded from the ``source_file`` the GUI recorded. If it cannot
    be loaded the spec still carries its hash, and the failure is reported
    where the glue is named rather than silently reshaping nothing.

    Returns ``{}`` for None / empty, so callers can pass it through unguarded.
    """
    if glue is None or isinstance(glue, type(None)) or not glue:
        return {}

    from scidb.run.glue import GlueSpec, resolve_python_glue

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
        - {"kind": "pathinput", "spec": PathInput.to_spec() JSON}

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
        from scidb.model.column_selection import ColumnSelection

        from scifor.column_selection import spec_from_wire

        if spec.get("groups"):
            # Column groups ([{"name"?, "columns"}], scifor.groups_to_wire).
            cols = spec_from_wire(list(spec["groups"]))
        else:
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
        from scidb.model.variant import Variant

        inner = _reconstruct_input_for_keys(spec["inner"])
        branch_params = dict(spec.get("branch_params", {}) or {})
        return Variant(inner, **branch_params)
    if kind == "merge":
        from scifor import Merge

        subs = [_reconstruct_input_for_keys(s) for s in spec["specs"]]
        return Merge(*subs)
    if kind == "pathinput":
        from scidb.model.log import Log
        from scifor.pathinput import PathInput

        # The whole to_spec() JSON, rebuilt by its one reader: a field-by-field
        # copy here lost aliases and key_regex until 2026-10-21.
        pi = PathInput.from_spec(spec["spec"])
        Log.info(
            f"[bridge] PathInput {pi.name!r} from MATLAB: template={pi.path_template!r}, "
            f"root_folder={str(pi.root_folder) if pi.root_folder else None!r}, "
            f"regex={pi.regex}, aliases={sorted(pi.aliases)}, "
            f"key_regex={sorted(pi.key_regex)}"
        )
        return pi
    if kind == "path_output":
        from scifor import PathOutput

        return PathOutput(spec["template"])
    if kind == "across_variants":
        from scidb.model.across_variants import AcrossVariants

        inner = _reconstruct_input_for_keys(spec["inner"])
        return AcrossVariants(inner)
    return spec




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


def _make_matlab_fn_sentinel(
    fn_name: str, fn_hash: "str | None" = None, source_text: "str | None" = None
):
    """Build a Python callable that records its name but errors if invoked.

    The MATLAB-driven path runs the user function inside MATLAB's
    ``+scifor/for_each.m`` loop. Python's ``_for_each_prepare`` accepts a
    ``fn`` argument only to read ``fn.__name__`` and build a
    ``ForEachConfig``; the function itself is never called from Python.
    If it ever IS called, that signals a programming error (likely the
    bridge state was reused after ``for_each_save`` freed it).

    It carries the MATLAB digest as ``source_hash`` — the duck-typed marker
    scidb reads for "a MATLAB function" (``function_hash_for``,
    ``function_sources_for``). Without it the save path hashed THIS Python
    body as if it were the user's function, found it never matched the stored
    MATLAB hash, and warned "the two recipes have drifted" on every MATLAB run
    (cleanup-audit F13) — and a match would have filed this sentinel's code
    as the user's source.
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
    if fn_hash:
        _sentinel.source_hash = str(fn_hash)
        # The .m text behind that digest, so the save stores the CODE under
        # the hash (cleanup-audit F35) — duck-typed as ``source_text``, read
        # by scidb's function_sources_for. Attached only when it re-hashes to
        # the digest with the one recipe (scimatlab.source_hash):
        # filing text under a hash it does not produce would return the wrong
        # code for that version later.
        if source_text:
            text = str(source_text)
            from scimatlab.source_hash import text_hashes_to

            if text_hashes_to(text, str(fn_hash)):
                _sentinel.source_text = text
            else:
                from scidb.model.log import Log

                Log.warn(
                    f"[bridge] {fn_name}: source text does not hash to "
                    f"{str(fn_hash)[:12]}; not capturing it"
                )
    if source_text:
        # The signature's output names as MATLAB ran it, recorded beside the
        # run's outputs= (_run_outputs.output_params): which variable each
        # NAMED output saved to, which survives a later reordering of the
        # signature. Owner: scimatlab.source_hash.matlab_output_names.
        from scimatlab.source_hash import matlab_output_names

        names = matlab_output_names(str(source_text), fn_name)
        if names:
            _sentinel.output_params = names
    return _sentinel


def _locations_from_matlab(locations):
    """``+scidb/for_each.m``'s ``locations`` (JSON text, a mapping, or None)
    -> the mapping ``scifor.locations`` reads, or None. Owner:
    ``scifor.locations.locations_from_text`` (shared with ``+scifor/for_each.m``)."""
    from scifor.locations import locations_from_text

    return locations_from_text(locations)


def _parameter_names_from_matlab(names) -> "dict[str, str] | None":
    """``+scidb/for_each.m``'s ``parameter_names`` (a py.dict of str, or
    None) -> ``{input: declared Parameter name}``, merged by the one owner so
    MATLAB and Python can never disagree on the rule."""
    from scidb.model.log import Log
    from scidb.model.parameter import declared_input_names

    if not names:
        return None
    merged = declared_input_names({}, {str(k): str(v) for k, v in dict(names).items()})
    Log.info(f"[bridge] parameter_names (input -> declared Parameter): {merged}")
    return merged or None



#: Type names already warned about a MATLAB codec shadowed by a Python class.
_CODEC_CONFLICT_WARNED: set = set()


def _matlab_codec_to_db(self):
    """Surrogate ``to_db`` for a MATLAB class with its own ``to_db``/``from_db``.

    MATLAB already ran the class's ``to_db`` (``+scidb/+internal/codec.m``), so
    the value arriving here IS the stored table. Anything else means a save
    path reached Python without MATLAB's ``to_db`` applied.
    """
    import pandas as pd

    if not isinstance(self.data, pd.DataFrame):
        raise TypeError(
            f"MATLAB variable {type(self).__name__} has custom to_db/from_db, "
            f"but the value reached Python without MATLAB's to_db() applied "
            f"(got {type(self.data).__name__}, expected a table)"
        )
    return self.data


def _matlab_codec_from_db(cls, df):
    """Surrogate ``from_db``: hand the stored table back unchanged. MATLAB's
    own ``from_db`` rebuilds the object on the MATLAB side."""
    return df


def _sync_matlab_codec(surrogate, codec) -> None:
    """Attach (``codec=True``) or detach (``False``) the pass-through codec on
    a MATLAB surrogate; ``None`` leaves it as it is.

    The flag is what ``DatabaseManager._has_custom_serialization`` reads, so a
    codec surrogate is stored in the columnar ``"custom": True`` form, as a
    Python class with ``to_db``/``from_db`` is. ``None`` exists for callers
    that cannot know (the GUI registers MATLAB variables from discovered
    ``.m`` files) so they never clear a flag MATLAB set.
    """
    if codec is None:
        return
    from scidb.model.log import Log as _BridgeLog

    has = "to_db" in surrogate.__dict__
    if bool(codec) and not has:
        surrogate.to_db = _matlab_codec_to_db
        surrogate.from_db = classmethod(_matlab_codec_from_db)
        _BridgeLog.info(
            f"[codec] MATLAB variable {surrogate.__name__}: custom to_db/from_db on"
        )
    elif not bool(codec) and has:
        del surrogate.to_db
        del surrogate.from_db
        _BridgeLog.info(
            f"[codec] MATLAB variable {surrogate.__name__}: custom to_db/from_db off"
        )


def register_matlab_variable(type_name: str, schema_version: int = 1, codec=None):
    """Create a Python surrogate BaseVariable subclass for a MATLAB type.

    The surrogate is auto-registered in ``BaseVariable._all_subclasses``
    via ``__init_subclass__`` and, if a database is configured, registered
    with the ``DatabaseManager`` as well.

    ``codec`` says whether the MATLAB class overrides ``to_db``/``from_db``
    (decided by ``+scidb/+internal/codec.m``): ``True``/``False`` sets the
    surrogate's pass-through codec, also on an already registered surrogate
    or entities-TOML declaration (``_declared_only``: it names the type but
    says nothing about storage, so the MATLAB class is the definition);
    ``None`` (the default) leaves it unchanged. A Python-defined class of the
    same name is never modified.

    Returns the surrogate class.
    """
    from scidb.model.variable import BaseVariable

    existing = BaseVariable.get_subclass_by_name(type_name)
    if existing is not None:
        if getattr(existing, "_matlab_surrogate", False) or getattr(
            existing, "_declared_only", False
        ):
            _sync_matlab_codec(existing, codec)
        elif codec and type_name not in _CODEC_CONFLICT_WARNED:
            from scidb.model.log import Log as _BridgeLog

            _CODEC_CONFLICT_WARNED.add(type_name)
            _BridgeLog.warn(
                f"[codec] MATLAB variable {type_name} has custom to_db/from_db, "
                f"but a Python class of that name defines its storage; the "
                f"Python class's to_db/from_db apply"
            )
        return existing

    surrogate = type(
        type_name,
        (BaseVariable,),
        {"schema_version": schema_version, "_matlab_surrogate": True},
    )
    _sync_matlab_codec(surrogate, codec)

    try:
        from scidb.store.database import get_database

        get_database().register(surrogate)
    except Exception:
        pass  # Database not yet configured; will register on configure_database

    return surrogate


def sync_output_column_name(type_name: str, column_name=None) -> str:
    """Set (or clear) ``column_name`` on a MATLAB type's Python surrogate.

    ``+scidb/for_each.m`` calls this for every output on every call with the
    MATLAB class's ``column_name`` Constant property (``None`` when the class
    has none), so a property added, changed or removed in MATLAB takes effect
    on the next run. Empty text counts as unset, as in both languages'
    ``output_column_name``. Returns the resulting column name.
    """
    from scidb.model.log import Log as _BridgeLog
    from scifor import output_column_name

    surrogate = register_matlab_variable(type_name)
    # A Python-defined class of the same name owns its own column_name; only
    # a surrogate (which exists to mirror the MATLAB class) is overwritten.
    if getattr(surrogate, "_matlab_surrogate", False) or column_name:
        surrogate.column_name = column_name if column_name else None
    resolved = output_column_name(surrogate)
    _BridgeLog.debug(
        f"[bridge] sync_output_column_name: {type_name} -> column {resolved!r} "
        f"(column_name={surrogate.column_name!r})"
    )
    return resolved



def get_surrogate_class(type_name: str):
    """Retrieve the Python surrogate class for a MATLAB variable type.

    Raises ValueError if not registered.
    """
    from scidb.model.variable import BaseVariable

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        raise ValueError(
            f"MATLAB variable type '{type_name}' is not registered. "
            f"Call scidb.register_variable('{type_name}') first."
        )
    return cls
