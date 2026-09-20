"""Glue nodes — transient, in-memory reshaping between a variable and its consumer.

A **glue node** is free-form user code (a ``glue_``-prefixed function in a
GUI-owned file) that runs *in memory* on the bulk-loaded input table of one
parameter of the consuming function, and is **never saved**. It exists so that
one-off reshaping — rename a column, change a dtype, drop or append a column,
restructure a dict — does not have to be a real pipeline function with a
declared output variable type polluting the database.

Design of record: ``docs/claude/free-code-glue-nodes.md``.

Two rules make everything else work:

**The row-preservation contract.** A glue node may change the column space; it
may not change the row set. Add/drop/rename/retype columns and restructure cell
values freely; filtering, aggregating, exploding or re-indexing is refused. That
is what makes re-attaching the hidden ``__record_id`` / ``__branch_params``
columns *provably* safe, so per-row provenance survives glue without the user
ever knowing those columns exist.

**Schema keys are visible but protected.** A whole-table glue may legitimately
need ``df.groupby("subject")``, and "the loaded table" is the user's mental
model, so schema-key columns are handed in. But scidb stringifies schema values
before combo filtering, so dropping or retyping one makes *every* combo filter
miss and the run "succeeds" having produced nothing. They are therefore verified
present and value-identical on the way out.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .exceptions import SciStackError
from .log import Log
from .per_combo import PerComboLoader, PerComboLoaderMerge

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd


# Columns hidden from glue and re-attached afterwards. Internal bookkeeping;
# the user must never have to think about them.
HIDDEN_PREFIX = "__"

# The name prefix that marks a function as glue. Kept here, next to the
# machinery it governs; ``scidb.roles.function_role`` is the one public
# classifier over it (and over ``plot_``/``stat_``).
GLUE_PREFIX = "glue_"

# "no such input", distinct from an input whose value is legitimately None —
# a Parameter may hold None, and that is still a constant worth gluing.
_MISSING = object()


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class GlueError(SciStackError):
    """Base for glue-node failures.

    ``matlab_identifier`` is the error id the MATLAB side raises for the same
    condition, so the two languages report one vocabulary.
    """

    matlab_identifier = "scidb:glue:error"


class GlueRowsChangedError(GlueError):
    """A glue node changed the row set of the table it was given.

    Row-changing work is a real computation with a real result — use an
    ordinary function node with a saved output.
    """

    matlab_identifier = "scidb:glue:rowsChanged"


class GlueSchemaKeysAlteredError(GlueError):
    """A glue node dropped, reordered or retyped a schema-key column."""

    matlab_identifier = "scidb:glue:schemaKeysAltered"


class GlueLanguageMismatchError(GlueError):
    """A glue chain mixes languages, or its language is not the run's."""

    matlab_identifier = "scidb:glue:languageMismatch"


class GlueUnsupportedInputError(GlueError):
    """Glue was attached to a parameter that has no table to reshape.

    A ``PathInput``-fed parameter is always resolved per combo, so no loaded
    table exists at the fusion point.
    """

    matlab_identifier = "scidb:glue:unsupportedInput"


class GlueChainOrderError(GlueError):
    """A whole-table glue was ordered after a per-schema-key one.

    The two run at different points of the consuming function's execution
    (bulk table vs. post-slice value), so such a chain could not be applied in
    the order it was written.
    """

    matlab_identifier = "scidb:glue:chainOrder"


# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------
@dataclass
class GlueSpec:
    """One glue node: a named, hashable, single-return reshaping function.

    ``fn`` is the live callable for Python glue. MATLAB glue carries
    ``source_text`` instead and is applied on the MATLAB side (a ``.m``
    function cannot execute inside Python's prepare step) — the Python side
    still needs the spec so it can hash it into the consumer's identity and
    tell MATLAB which params carry a chain.

    ``per_schema_key`` is the D4 opt-in: False (default) applies the glue once
    to the whole loaded table, True applies it to each already-sliced value
    just before the consuming function is called.

    **N inputs.** A glue node may take more than one parameter (N edges → N
    params). Exactly one of them is the *piped* parameter — the table flowing
    from the consumer's input, the one the row-preservation contract is about.
    The rest are bound by their own edges and arrive as ``extra_inputs``,
    keyed by the glue's own parameter names. ``pipe_param`` names the piped
    one; omitted, it is the function's first parameter.

    Parameter *names* never bind anything: the caller states the bindings, the
    same edges-only rule ordinary function inputs have followed since
    2026-08-25 (``docs/claude/function-input-resolution.md``). A glue parameter
    called ``emg`` can be fed by any variable.
    """

    name: str
    fn: Callable | None = None
    language: str = "python"
    source_file: str | None = None
    source_text: str | None = None
    per_schema_key: bool = False
    pipe_param: str | None = None
    extra_inputs: dict[str, Any] = field(default_factory=dict)
    _hash: str | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.language not in ("python", "matlab"):
            raise ValueError(
                f"GlueSpec '{self.name}': language must be 'python' or 'matlab', "
                f"got {self.language!r}"
            )
        if (
            self.language == "python"
            and self.fn is None
            and self._hash is None
            and not self.source_text
        ):
            raise ValueError(
                f"GlueSpec '{self.name}': python glue needs a callable, an "
                f"explicit hash, or its source text"
            )

    @property
    def hash(self) -> str:
        """Content-derived identity, 16 hex chars.

        Deliberately never derived from the node's display name: two different
        glue bodies sharing a name must not collide into one provenance node —
        the failure ``entity-editability-model.md`` documents for PathInput.
        """
        if self._hash is None:
            self._hash = _compute_glue_hash(self)
        return self._hash

    def call(self, value: Any) -> Any:
        """Invoke the glue body on ``value``. Python glue only.

        ``value`` lands on the piped parameter; ``extra_inputs`` fills the
        rest by keyword.
        """
        if self.fn is None:
            raise GlueLanguageMismatchError(
                f"glue '{self.name}' ({self.language}) has no Python callable and "
                f"cannot be applied in a Python run"
            )
        if not self.extra_inputs:
            return self.fn(value)
        kwargs = dict(self.extra_inputs)
        kwargs[self.piped_parameter()] = value
        return self.fn(**kwargs)

    def piped_parameter(self) -> str:
        """Name of the parameter the flowing table is bound to."""
        if self.pipe_param:
            return self.pipe_param
        import inspect

        try:
            params = list(inspect.signature(self.fn).parameters)
        except (TypeError, ValueError):  # builtins, C callables
            params = []
        if not params:
            raise GlueError(
                f"glue '{self.name}' takes extra inputs but its piped "
                f"parameter could not be determined; set pipe_param explicitly"
            )
        return params[0]


def _compute_glue_hash(spec: GlueSpec) -> str:
    """16-hex content hash of a glue body.

    Python glue reuses ``compute_function_hash`` (the AST-recursive hash every
    other function identity in the stack uses) so an edit to a helper the glue
    calls invalidates the glue too. MATLAB glue hashes its source text — the
    same formula ``scimatlab.bridge.compute_matlab_function_hash`` uses,
    truncated; it is spelled out here rather than imported because scidb sits
    *below* scimatlab and must not depend on it.
    """
    if spec.language == "python" and spec.fn is not None:
        from scilineage.hashing import compute_function_hash

        return compute_function_hash(spec.fn, truncate=16)
    # Python glue whose body could not be loaded (see resolve_python_glue)
    # falls through to the text hash. It is a DIFFERENT number than the AST
    # hash the same glue gets where its callable is available, so identity is
    # degraded — but resolve_python_glue has already warned, and a wrong-but-
    # stable hash beats crashing the run at spec construction.
    text = spec.source_text or ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def chain_hash(chain: Sequence[GlueSpec]) -> str:
    """16-hex identity of a whole chain, order-sensitive.

    Feeds the virtual record id and the ``__glue_hashes`` version key.
    """
    payload = "|".join(f"{s.name}:{s.hash}" for s in chain)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def input_set_signature(rids: Iterable[str]) -> str:
    """16-hex signature of a glue's whole input record set, order-insensitive.

    A whole-table glue may legitimately read across rows (mean-centering), so
    its output for any one row depends on the entire input set. Folding this
    into the virtual record id makes a *growing* input set force a recompute —
    the same hole that was already closed for aggregation ``skip_computed``.

    Both the save path and the node-state prediction path must compute this the
    same way over the same set, or nodes go falsely red; see
    ``scidb.provenance_query._predict_config_invocations``.
    """
    payload = "|".join(sorted({str(r) for r in rids if r is not None}))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def virtual_rid_map(
    source_rids: Iterable[str], chain: Sequence[GlueSpec]
) -> tuple[str, str, dict[str, str]]:
    """``(chain_hash, input_set_signature, {source_rid: virtual_rid})``.

    The single place the virtual-record identity is derived, so the save path
    and the prediction path cannot drift. See
    :func:`scidb.provenance.compute_glue_record_id` for what each component
    buys.
    """
    from .provenance import compute_glue_record_id

    uniq = sorted({str(r) for r in source_rids if r is not None})
    ch = chain_hash(chain)
    sig = input_set_signature(uniq)
    return ch, sig, {rid: compute_glue_record_id(ch, rid, sig) for rid in uniq}


def chain_names(chain: Sequence[GlueSpec]) -> list[str]:
    """Glue node names in application order — the call-site-bearing half."""
    return [s.name for s in chain]


def chain_hashes(chain: Sequence[GlueSpec]) -> list[str]:
    """Glue body hashes in application order — the version-bearing half."""
    return [s.hash for s in chain]


# ---------------------------------------------------------------------------
# Chain normalization / validation
# ---------------------------------------------------------------------------
GlueChains = dict[str, list[GlueSpec]]


def normalize_glue(glue: Any) -> GlueChains:
    """Coerce a user-supplied ``glue=`` argument into ``{param: [GlueSpec]}``.

    Accepts a bare spec or callable per param as shorthand for a one-element
    chain, since a single reshaper is the overwhelmingly common case.
    """
    if not glue:
        return {}
    result: GlueChains = {}
    for param, value in dict(glue).items():
        if isinstance(value, GlueSpec):
            specs = [value]
        elif callable(value):
            specs = [GlueSpec(name=getattr(value, "__name__", "glue"), fn=value)]
        else:
            specs = []
            for item in value:
                if isinstance(item, GlueSpec):
                    specs.append(item)
                elif callable(item):
                    specs.append(
                        GlueSpec(name=getattr(item, "__name__", "glue"), fn=item)
                    )
                else:
                    raise TypeError(
                        f"glue['{param}'] must contain GlueSpec or callables; "
                        f"got {type(item).__name__}"
                    )
        if specs:
            _validate_chain(param, specs)
            result[param] = specs
    return result


def _validate_chain(param: str, chain: Sequence[GlueSpec]) -> None:
    """Reject mixed-language chains and bulk-after-per-key ordering."""
    languages = {s.language for s in chain}
    if len(languages) > 1:
        raise GlueLanguageMismatchError(
            f"glue chain for '{param}' mixes languages "
            f"({', '.join(sorted(languages))}); a glue node executes in the "
            f"language of the run, so a chain must be single-language"
        )
    seen_per_key = False
    for spec in chain:
        if spec.per_schema_key:
            seen_per_key = True
        elif seen_per_key:
            raise GlueChainOrderError(
                f"glue chain for '{param}': whole-table glue '{spec.name}' is "
                f"ordered after a per-schema-key glue. Whole-table glue runs on "
                f"the bulk loaded table and per-schema-key glue runs after "
                f"slicing, so this chain cannot run in the order written — put "
                f"the whole-table nodes first"
            )


def check_run_language(chains: GlueChains, run_language: str) -> None:
    """Refuse a chain authored in a language other than the run's.

    Python glue in a MATLAB run would technically work for free (it happens
    inside ``for_each_prepare``). It is refused anyway so the exported plain
    script stays faithful and there is one rule instead of an asymmetric one.

    **Constant-fed chains are exempt and must not be passed here** — see
    :func:`apply_constant_glue`, which enforces the opposite rule for them
    (Python always, whatever the run's language) because its application site
    is Python on both run paths. ``_for_each_prepare`` splits the chains before
    calling either.
    """
    for param, chain in chains.items():
        for spec in chain:
            if spec.language != run_language:
                raise GlueLanguageMismatchError(
                    f"glue '{spec.name}' on parameter '{param}' is "
                    f"{spec.language} but this is a {run_language} run; a glue "
                    f"node executes in the language of the run"
                )


def resolve_python_glue(name: str, source_file: str | None) -> Callable | None:
    """Load a Python glue callable from the file that declares it.

    Used when a Python glue chain has to reach a process that has no function
    registry — chiefly a **MATLAB run**, where ``scimatlab.bridge`` runs inside
    MATLAB's own interpreter and only ``scidb``/``scifor`` are configured. The
    GUI knows every glue node's path, so it sends the path and this resolves
    the body at the far end.

    Loading by location rather than by import name is deliberate and matches
    ``registry``'s loose-file handling: ``glue_dir`` is a flat directory of
    one-function files that is not necessarily a package.

    Returns ``None`` (with a warning) rather than raising — the caller still
    has the spec's hash for identity, and a chain that cannot be applied is
    reported by ``apply_glue_chain``/``GlueSpec.call`` with the glue named.
    """
    if not source_file:
        Log.warn(
            f"[glue] '{name}': no source file recorded, so its Python body "
            f"cannot be loaded here"
        )
        return None

    import importlib.util
    from pathlib import Path

    path = Path(source_file)
    if not path.is_file():
        Log.warn(f"[glue] '{name}': source file {path} does not exist")
        return None

    try:
        spec = importlib.util.spec_from_file_location(f"_scistack_glue_{name}", path)
        if spec is None or spec.loader is None:
            Log.warn(f"[glue] '{name}': {path} is not importable")
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001 - a bad glue file must not kill the run
        Log.warn(f"[glue] '{name}': failed to load {path}: {exc}")
        return None

    fn = getattr(module, name, None)
    if not callable(fn):
        Log.warn(f"[glue] '{name}': {path} defines no function called '{name}'")
        return None
    return fn


def is_constant_input(spec: Any) -> bool:
    """Whether an input spec is a plain constant value at prepare time.

    Essentially ``_convert_inputs``' "constant — pass through unchanged"
    branch: not a ``ColName`` marker and not loadable. A ``Parameter``-fed
    input is one of these by the time prepare runs — ``for_each``'s Step 1 has
    already expanded the ``EachOf`` into one recursive call per concrete value.

    ``PathInput`` and an unexpanded ``EachOf`` are excluded explicitly even
    though ``input_spec.is_loadable`` also returns False for both. Neither is a *value*:
    a PathInput resolves per combo (glue on it is refused outright by
    ``refuse_pathinput_glue``, which runs first), and an EachOf is a fan-out
    axis — see :func:`is_constant_axis` for that case. Relying on
    ``input_spec.is_loadable`` happening to say False for them would rest this
    correctness on a docstring about *loading*.
    """
    from scifor import ColName, EachOf, PathInput

    from .input_spec import is_loadable

    if isinstance(spec, (ColName, EachOf, PathInput)):
        return False
    return not is_loadable(spec)


def is_constant_axis(spec: Any) -> bool:
    """Whether an input spec is an unexpanded fan-out over constant values.

    A ``Parameter`` **is** a ``scifor.EachOf``. On the pure-Python path
    ``for_each``'s Step 1 expands it before prepare, so this is normally
    False — but ``scimatlab.bridge`` calls ``_for_each_prepare`` directly, with
    no Step 1 in front of it, so an unexpanded Parameter can arrive there.

    An ``EachOf`` of variable CLASSES is a multi-type input, not a Parameter
    (``build_run_inputs`` builds exactly that shape for a handle with several
    variable edges on it). Its data is loaded, so its glue belongs at the
    fusion point — hence the per-alternative test rather than a bare
    ``isinstance``.
    """
    from scifor import EachOf

    return (
        isinstance(spec, EachOf)
        and bool(spec.alternatives)
        and all(is_constant_input(a) for a in spec.alternatives)
    )


def split_constant_chains(
    inputs: dict[str, Any], chains: GlueChains
) -> tuple[GlueChains, GlueChains]:
    """Split chains into ``(constant_fed, deferred)``.

    ``constant_fed`` is applied here in prepare (:func:`apply_constant_glue`);
    ``deferred`` goes on to the Step-10/11 fusion point, or to MATLAB's loop.
    The two obey **opposite** language rules, so the split has to happen before
    either is validated.

    A chain naming a param that is not an input at all lands in ``deferred``,
    where ``fuse_glue`` reports it as "chain declared but no such input on this
    call" — one place that warning is worded, not two.
    """
    constant_fed: GlueChains = {}
    deferred: GlueChains = {}
    for param, chain in chains.items():
        spec = inputs.get(param, _MISSING)
        if spec is not _MISSING and (
            is_constant_input(spec) or is_constant_axis(spec)
        ):
            constant_fed[param] = chain
        else:
            deferred[param] = chain
    return constant_fed, deferred


def check_constant_glue_language(chains: GlueChains, run_language: str) -> None:
    """Refuse non-Python glue on a constant-fed parameter.

    The inverse of :func:`check_run_language`, for the reason spelled out in
    :func:`apply_constant_glue`: the application site is ``_for_each_prepare``,
    which is Python on both run paths.
    """
    for param in sorted(chains):
        for spec in chains[param]:
            if spec.language != "python":
                raise GlueLanguageMismatchError(
                    f"glue '{spec.name}' on parameter '{param}' is "
                    f"{spec.language}, but '{param}' is fed by a Parameter (or "
                    f"a plain constant). Constant glue is applied while "
                    f"building the version keys, which happens in Python on "
                    f"every run — including a {run_language} one — so it must "
                    f"be Python glue. Either rewrite '{spec.name}' in Python, "
                    f"glue a variable input instead, or reshape the value "
                    f"inside the function."
                )


def apply_constant_glue(inputs: dict[str, Any], chains: GlueChains) -> None:
    """Apply glue attached to a **constant-valued** input, in prepare.

    Mutates ``inputs`` in place, replacing each glued value with the reshaped
    one. ``chains`` must already be the constant-fed half
    (:func:`split_constant_chains`), language-checked by
    :func:`check_constant_glue_language`.

    This is the ``Parameter`` case (and a bare constant passed directly).

    Why here, and not at the Step-10/11 fusion point
    ------------------------------------------------
    A constant never becomes a loaded table, so the fusion point has nothing
    to reshape: it would fall into the "not a table — glue applied to the value
    as-is" branch, *after* ``ForEachConfig`` has already hashed the pre-glue
    value.

    Applying it here — before Step 8 builds the version keys — gets **identity
    right for free**. The value that lands in ``__constants`` IS the glued
    value, so editing a glue body changes the constant's content hash, which is
    one of the bindings ``skip_computed`` compares. Downstream results
    invalidate with **no virtual glue record at all**: §2's staleness hole is
    closed here rather than merely warned about, as it must be for ``Merge`` /
    ``PerComboLoader``.

    A multi-valued ``Parameter`` needs no special handling. ``for_each``'s
    Step 1 expands it into one recursive call per value *before* prepare, so
    each call glues its own concrete value and records it — the fan-out and
    the glue compose without either knowing about the other.

    The language rule (§1) is inverted here, deliberately
    ----------------------------------------------------
    ``_for_each_prepare`` is Python on *both* run paths, so this site can only
    execute Python. A constant-fed chain must therefore be **Python glue even
    in a MATLAB run** — the exact opposite of :func:`check_run_language`'s rule,
    for the exact same reason (a glue node executes where its input exists).
    MATLAB glue on a constant-fed param is refused rather than silently moved
    to a per-combo site, because a per-combo site would record the *pre*-glue
    constant and reintroduce the silent staleness this placement removes.
    """
    from scifor import EachOf

    for param in sorted(chains):
        chain = chains[param]
        before = inputs[param]

        if is_constant_axis(before):
            # An unexpanded fan-out (the MATLAB bridge path, which has no
            # Step 1 in front of it). Map over the alternatives: gluing the
            # EachOf object itself would hand the user's code the axis.
            values = list(before.alternatives)
            inputs[param] = EachOf(
                *(apply_glue_chain(v, chain, param=param) for v in values)
            )
            detail = f"{len(values)} unexpanded value(s)"
        else:
            inputs[param] = apply_glue_chain(before, chain, param=param)
            detail = (
                f"the constant value ({type(before).__name__} -> "
                f"{type(inputs[param]).__name__})"
            )

        Log.info(
            f"[glue] '{param}': applied {len(chain)} node(s) to {detail} "
            f"before the version keys were built, so the glued value is what "
            f"lands in __constants and an edit to a glue body invalidates "
            f"downstream results"
        )


def bulk_chain(chain: Sequence[GlueSpec]) -> list[GlueSpec]:
    """The whole-table part of a chain (applied to the bulk loaded table)."""
    return [s for s in chain if not s.per_schema_key]


def per_combo_chain(chain: Sequence[GlueSpec]) -> list[GlueSpec]:
    """The per-schema-key part of a chain (applied to each sliced value)."""
    return [s for s in chain if s.per_schema_key]


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
def apply_glue_chain(
    value: Any,
    chain: Sequence[GlueSpec],
    *,
    param: str,
    schema_keys: Iterable[str] | None = None,
) -> Any:
    """Run ``chain`` over ``value``, enforcing the glue contracts.

    For a DataFrame: ``__``-prefixed columns are hidden from the glue and
    re-attached by index afterwards; the row set and the schema-key columns are
    verified unchanged. For anything else (a scalar, an array, a dict) the glue
    is simply called — there are no rows to preserve.

    Returns the reshaped value. Raises :class:`GlueRowsChangedError` or
    :class:`GlueSchemaKeysAlteredError` rather than silently re-attaching a
    partial column.
    """
    if not chain:
        return value

    import pandas as pd

    for spec in chain:
        if not isinstance(value, pd.DataFrame):
            t0 = time.perf_counter()
            value = spec.call(value)
            Log.debug(
                f"[glue] '{param}': applied {spec.name} to "
                f"{type(value).__name__} (non-table, no row check) in "
                f"{time.perf_counter() - t0:.3f}s"
            )
            continue
        value = _apply_one_to_frame(value, spec, param=param, schema_keys=schema_keys)
    return value


def _apply_one_to_frame(
    df: "pd.DataFrame",
    spec: GlueSpec,
    *,
    param: str,
    schema_keys: Iterable[str] | None,
) -> "pd.DataFrame":
    import pandas as pd

    hidden_cols = [c for c in df.columns if str(c).startswith(HIDDEN_PREFIX)]
    hidden = df[hidden_cols] if hidden_cols else None
    visible = df.drop(columns=hidden_cols) if hidden_cols else df

    present_keys = [
        k for k in (schema_keys or ()) if k in visible.columns
    ]
    before = {k: visible[k].tolist() for k in present_keys}
    before_cols = list(visible.columns)
    n_before = len(visible)

    t0 = time.perf_counter()
    out = spec.call(visible)
    elapsed = time.perf_counter() - t0

    if not isinstance(out, pd.DataFrame):
        # A glue that collapses a table to a scalar/array has changed the row
        # set by definition; say so in the row-set vocabulary the user knows.
        raise GlueRowsChangedError(
            f"glue '{spec.name}' on parameter '{param}' was given a "
            f"{n_before}-row table and returned {type(out).__name__}. A glue "
            f"node may change the column space but not the row set — use a "
            f"function node with a saved output for anything that reduces rows."
        )

    # The index must come back UNCHANGED, and dtype counts. ``Index.equals``
    # alone is not enough: it compares values across numeric dtypes, so
    # ``df.set_index("signal")`` on a 0.0/1.0/2.0 column reads as "equal" to
    # the original RangeIndex(3) and slips through — a real re-index that the
    # contract refuses, and one that would make the hidden-column re-attach
    # depend on pandas' cross-dtype alignment rules.
    if (
        len(out) != n_before
        or not out.index.equals(visible.index)
        or out.index.dtype != visible.index.dtype
    ):
        raise GlueRowsChangedError(
            f"glue '{spec.name}' on parameter '{param}' changed the row set: "
            f"{n_before} row(s) in, {len(out)} row(s) out"
            + ("" if len(out) != n_before else " (row index changed)")
            + ". A glue node may change the column space but not the row set — "
            "filtering, aggregating, exploding and re-indexing must be an "
            "ordinary function node with a saved output."
        )

    _check_schema_keys(out, before, spec=spec, param=param)

    added = [c for c in out.columns if c not in before_cols]
    dropped = [c for c in before_cols if c not in out.columns]
    retyped = [
        c
        for c in out.columns
        if c in before_cols and str(out[c].dtype) != str(visible[c].dtype)
    ]
    Log.debug(
        f"[glue] '{param}': applied {spec.name} "
        f"(+{added or '-'}, -{dropped or '-'}, dtype {retyped or '-'}) "
        f"in {elapsed:.3f}s"
    )

    if hidden is not None:
        collisions = [c for c in hidden.columns if c in out.columns]
        if collisions:
            # The glue invented a __-prefixed column; dropping ours silently
            # would corrupt provenance, so its copy loses and we say so.
            Log.warn(
                f"[glue] '{param}': {spec.name} produced internal column(s) "
                f"{collisions} — overwritten by scidb's own bookkeeping"
            )
            out = out.drop(columns=collisions)
        # Re-attached POSITIONALLY (raw values, no index alignment), which is
        # exactly what the row-preservation contract just proved safe. Going
        # through pd.concat would re-align on the index instead — fine for the
        # ordinary case, but it turns a duplicate index label into extra rows
        # rather than an error, and provenance must not depend on that.
        out = out.copy()
        for col in hidden.columns:
            out[col] = hidden[col].to_numpy()
    return out


def _check_schema_keys(
    out: "pd.DataFrame",
    before: dict[str, list],
    *,
    spec: GlueSpec,
    param: str,
) -> None:
    """Verify schema-key columns survived value-identical.

    Step 5 stringifies schema values so they match the loaded table's
    stringified columns. An int/str change on a schema column therefore makes
    *every* combo filter miss and the run "succeeds" having produced nothing —
    the documented succeeds-while-doing-no-work failure mode. Compared by
    value, not dtype, so the message names what actually broke.
    """
    for key, values in before.items():
        if key not in out.columns:
            raise GlueSchemaKeysAlteredError(
                f"glue '{spec.name}' on parameter '{param}' dropped schema-key "
                f"column '{key}'. Schema-key columns are visible to glue so it "
                f"can group by them, but they drive the consuming function's "
                f"per-combo slicing and must come back unchanged."
            )
        after = out[key].tolist()
        if after != values:
            raise GlueSchemaKeysAlteredError(
                f"glue '{spec.name}' on parameter '{param}' altered schema-key "
                f"column '{key}' (e.g. {values[:3]!r} -> {after[:3]!r}). Schema "
                f"values are matched as strings during per-combo slicing, so "
                f"even a dtype change makes every combo filter miss."
            )


# ---------------------------------------------------------------------------
# Fusion helpers used by scidb.for_each
# ---------------------------------------------------------------------------
def log_chains(chains: GlueChains) -> None:
    """One INFO line per glued param — the first entry in the diagnostic trail.

    The predicted confusing failure is "I edited my glue and nothing
    recomputed"; this line proves which bodies the run actually picked up.
    """
    for param in sorted(chains):
        chain = chains[param]
        desc = ", ".join(f"{s.name}@{s.hash[:8]}" for s in chain)
        Log.info(f"[glue] '{param}': chain = [{desc}]")


@dataclass
class GlueFusion:
    """What the fusion step produced, for the loop and save phases.

    ``per_combo`` are the chain entries that must run post-slice instead of on
    the bulk table. ``virtual`` maps each glued param to its
    ``(chain_hash, input_set_signature, {source_rid: virtual_rid})`` — the
    provenance half of the feature, consumed by the save path.
    """

    per_combo: dict[str, list[GlueSpec]] = field(default_factory=dict)
    virtual: dict[str, tuple[str, str, dict[str, str]]] = field(default_factory=dict)


def fuse_glue(
    loaded_inputs: dict[str, Any],
    chains: GlueChains,
    *,
    schema_keys: Iterable[str] | None = None,
    apply_bulk: bool = True,
) -> GlueFusion:
    """Apply whole-table glue in place, and route each param's provenance
    through a virtual glue record.

    Runs between Step 10 (``_convert_inputs``) and Step 11 (``__record_id`` →
    ``__rid_{param}`` renaming), which is the placement the whole feature rests
    on: the loaded table already carries schema-key columns so the consumer's
    per-combo slicing is untouched, and it is before rid renaming so provenance
    bookkeeping is untouched.

    Two things happen per glued param:

    1. The whole-table part of the chain is applied to the loaded frame
       (``Fixed`` / ``ColumnSelection`` wrappers are unwrapped and re-wrapped so
       glue sees the frame, never the wrapper).
    2. ``__record_id`` is **rewritten** to the virtual glue rid. Everything
       downstream — Step 11's rename, ``rid_to_bp``, ``__graph_var_bindings``,
       ``invocation_id``, ``skip_computed`` — then works through one extra graph
       hop with no special-casing, which is the only reason an edited glue body
       invalidates the consumer at all (``skip_computed`` compares bindings, and
       never reads version_keys).

    Step 2 happens for the *whole* chain, including its per-schema-key part:
    identity must not depend on where the body happens to run.

    ``apply_bulk=False`` does step 2 only — the MATLAB path, where the bodies
    run in ``+scidb/for_each.m`` but the identity is still Python's to compute.
    """
    import pandas as pd
    import scifor as _scifor

    fusion = GlueFusion()
    for param, chain in chains.items():
        if param not in loaded_inputs:
            Log.warn(
                f"[glue] '{param}': chain declared but no such input on this "
                f"call — glue not applied"
            )
            continue

        per_key = per_combo_chain(chain)
        bulk = bulk_chain(chain)

        data = loaded_inputs[param]
        frame, rewrap = _unwrap_frame(data)

        if frame is None:
            if isinstance(data, (PerComboLoader, PerComboLoaderMerge, _scifor.Merge)):
                # No single table exists yet, so the whole chain has to run
                # post-slice. A Merge is the structural case: it stays a set of
                # CONSTITUENT frames until scifor joins them per combo, and its
                # constituent loading strips every ``__`` column on the way in
                # — so there is no bulk table here and no record id either.
                if bulk:
                    Log.info(
                        f"[glue] '{param}': input has no single loaded table "
                        f"({type(data).__name__}), so its {len(bulk)} "
                        f"whole-table glue node(s) are applied per combo instead"
                    )
                per_key = list(chain)
                bulk = []
            else:
                Log.debug(
                    f"[glue] '{param}': input is "
                    f"{type(data).__name__} (not a table) — glue applied to "
                    f"the value as-is"
                )

        if bulk and apply_bulk:
            result = apply_glue_chain(
                frame if frame is not None else data,
                bulk,
                param=param,
                schema_keys=schema_keys,
            )
            loaded_inputs[param] = rewrap(result) if frame is not None else result
            if isinstance(result, pd.DataFrame):
                frame = result
                Log.info(
                    f"[glue] '{param}': {len(bulk)} whole-table node(s) applied "
                    f"-> {len(result)} rows, {len(result.columns)} cols"
                )
        if per_key:
            fusion.per_combo[param] = per_key

        _route_provenance(
            fusion, loaded_inputs, param, chain, frame, rewrap, source=data
        )

    return fusion


def _route_provenance(
    fusion: GlueFusion,
    loaded_inputs: dict[str, Any],
    param: str,
    chain: Sequence[GlueSpec],
    frame: Any,
    rewrap,
    source: Any = None,
) -> None:
    """Rewrite ``__record_id`` to the virtual glue rid for one param.

    ``source`` is the loaded input as it arrived (before any unwrapping), used
    only to explain *why* identity could not be recorded when it could not.
    """
    import pandas as pd

    if not isinstance(frame, pd.DataFrame) or "__record_id" not in frame.columns:
        # The one thing this feature exists to prevent is an edited glue that
        # silently changes nothing, so the gap is stated rather than logged at
        # DEBUG and forgotten. A Merge gets its own sentence because the
        # reason is structural (see fuse_glue), not an incidental missing
        # column the user could fix by rewiring.
        import scifor as _scifor

        detail = (
            "a Merge input stays a set of constituent frames until scifor "
            "joins them per combo, and its constituents are loaded without "
            "record ids"
            if isinstance(source, _scifor.Merge)
            else "the loaded input carries no __record_id column"
        )
        Log.warn(
            f"[glue] '{param}': {detail}, so this glue cannot be recorded in "
            f"the provenance graph. It WILL reshape the data, but editing its "
            f"body will NOT invalidate downstream results — re-run the "
            f"consuming function explicitly after changing it."
        )
        return

    ch, sig, mapping = virtual_rid_map(frame["__record_id"].dropna(), chain)
    if not mapping:
        return
    frame["__record_id"] = [
        mapping.get(str(r), r) for r in frame["__record_id"].tolist()
    ]
    loaded_inputs[param] = rewrap(frame)
    fusion.virtual[param] = (ch, sig, mapping)
    sample = next(iter(mapping.items()))
    Log.debug(
        f"[glue] virtual record {sample[1]} for input {sample[0]} "
        f"(chain {ch}, input set {sig}, {len(mapping)} record(s))"
    )


def refuse_pathinput_glue(inputs: dict[str, Any], chains: GlueChains) -> None:
    """Refuse glue on a ``PathInput``-fed parameter.

    A PathInput is always resolved per combo (scifor's loop owns it), so there
    is no loaded table at the fusion point and nothing for the whole-table
    contract to mean. Out of scope by design, not an oversight.
    """
    from scifor import EachOf, PathInput

    for param in sorted(chains):
        spec = inputs.get(param)
        is_pi = isinstance(spec, PathInput) or (
            isinstance(spec, EachOf)
            and bool(getattr(spec, "alternatives", None))
            and all(isinstance(a, PathInput) for a in spec.alternatives)
        )
        if is_pi:
            raise GlueUnsupportedInputError(
                f"glue is attached to parameter '{param}', which is fed by a "
                f"PathInput. A PathInput is resolved per combo, so no loaded "
                f"table exists for glue to reshape. Load the data into a "
                f"variable first, then glue that."
            )


def _unwrap_frame(data: Any):
    """Return ``(frame, rewrap)`` for a possibly wrapped loaded input.

    ``Fixed`` and ``ColumnSelection`` both expose ``.data``; the rewrap closure
    puts the reshaped frame back on the same wrapper instance so downstream
    identity checks (``isinstance``) are unaffected.
    """
    import pandas as pd

    import scifor as _scifor

    if isinstance(data, pd.DataFrame):
        return data, lambda df: df
    if isinstance(data, (_scifor.Fixed, _scifor.ColumnSelection)) and isinstance(
        data.data, pd.DataFrame
    ):

        def _rewrap(df, _wrapper=data):
            _wrapper.data = df
            return _wrapper

        return data.data, _rewrap
    return None, lambda v: v
