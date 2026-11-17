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

from scicanonicalhash import compute_function_hash

from scidb.model.exceptions import SciStackError
from scidb.model.log import Log
from scidb.run.per_combo import PerComboLoader, PerComboLoaderMerge

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd


# Columns hidden from glue and re-attached afterwards. Internal bookkeeping;
# the user must never have to think about them.
HIDDEN_PREFIX = "__"

# The name prefix that marks a function as glue. Kept here, next to the
# machinery it governs; ``scidb.model.roles.function_role`` is the one public
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
    """A glue chain mixes languages, or cannot execute in this run.

    The only chain that cannot execute is MATLAB glue in a Python run
    (:func:`check_glue_executable`).
    """

    matlab_identifier = "scidb:glue:languageMismatch"


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
    same recipe ``scimatlab.source_hash`` owns for .m files (SHA-256 of the
    UTF-8 bytes), truncated; it is spelled out here rather than imported because scidb sits
    *below* scimatlab and must not depend on it.
    """
    if spec.language == "python" and spec.fn is not None:
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
    ``scidb.provenance.query._predict_config_invocations``.
    """
    payload = "|".join(sorted({str(r) for r in rids if r is not None}))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def virtual_rid_map(
    source_rids: Iterable[str], chain: Sequence[GlueSpec]
) -> tuple[str, str, dict[str, str]]:
    """``(chain_hash, input_set_signature, {source_rid: virtual_rid})``.

    The single place the virtual-record identity is derived, so the save path
    and the prediction path cannot drift. See
    :func:`scidb.provenance.graph.compute_glue_record_id` for what each component
    buys.
    """
    from scidb.provenance.graph import compute_glue_record_id

    uniq = sorted({str(r) for r in source_rids if r is not None})
    ch = chain_hash(chain)
    sig = input_set_signature(uniq)
    return ch, sig, {rid: compute_glue_record_id(ch, rid, sig) for rid in uniq}


def constant_glue_rid(raw_record_id: str, entry: dict) -> str:
    """The virtual record a Parameter-fed glue's consumer binds — the
    constant-head twin of :func:`virtual_rid_map`, beside it so the two
    derivations of a glue record's identity live in one place.

    ``entry`` is the call's :data:`CONSTANT_GLUE_KEY` entry for the param.
    Same recipe as a variable head, with the glued value's content hash in the
    input-set slot: a constant has no input set, and the glued hash is what
    makes a config FILE edited behind an unchanged path recompute.
    """
    from scidb.provenance.graph import compute_glue_record_id

    return compute_glue_record_id(entry["chain"], raw_record_id, entry["glued"])


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


def chain_language(chain: Sequence[GlueSpec]) -> str:
    """The language of a (single-language, :func:`_validate_chain`) chain."""
    return chain[0].language if chain else "python"


def check_glue_executable(chains: GlueChains, run_language: str) -> None:
    """THE language gate: refuse a chain that cannot execute in this run.

    Glue does not care which language feeds it or which language its
    consumer is written in (docs/claude/free-code-glue-nodes.md §1, "Which
    language runs where"). Python glue runs in either run: MATLAB embeds
    Python, so a MATLAB run calls it in prepare or through the bridge. MATLAB
    glue runs in a MATLAB run, at every head.

    The one refusal is **MATLAB glue in a Python run**: a Python run is a
    Python process, and scidb has no way to execute a ``.m`` file.

    Replaces ``check_run_language`` and ``check_constant_glue_language``
    (2026-11-17), which enforced opposite rules for the two heads.
    """
    for param in sorted(chains):
        for spec in chains[param]:
            if spec.language == "matlab" and run_language != "matlab":
                raise GlueLanguageMismatchError(
                    f"glue '{spec.name}' on parameter '{param}' is MATLAB, but "
                    f"this is a {run_language} run, and a {run_language} run "
                    f"cannot execute a .m file. Rewrite '{spec.name}' in "
                    f"Python, or run this function from MATLAB."
                )
    if chains:
        Log.debug(
            f"[glue] language gate ({run_language} run): "
            + ", ".join(
                f"'{p}'={chain_language(chains[p])}" for p in sorted(chains)
            )
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
    a PathInput resolves per combo (its glue runs per path —
    :func:`is_pathinput_input`), and an EachOf is a fan-out
    axis — see :func:`is_constant_axis` for that case. Relying on
    ``input_spec.is_loadable`` happening to say False for them would rest this
    correctness on a docstring about *loading*.
    """
    from scifor import ColName, EachOf, PathInput

    from scidb.model.input_spec import is_loadable

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


def is_pathinput_input(spec: Any) -> bool:
    """Whether an input spec is fed by a ``PathInput``: bare, pinned
    (``Fixed(PathInput)``), or an ``EachOf`` whose every alternative is one.

    THE predicate for the PathInput head (docs/claude/free-code-glue-nodes.md
    §1, "A PathInput-fed parameter"). A PathInput resolves to a path per
    combo, inside the loop, so its glue runs per path (:class:`PerPathGlue`),
    never at the bulk fusion point.
    """
    from scifor import EachOf, Fixed, PathInput

    if isinstance(spec, PathInput):
        return True
    if isinstance(spec, Fixed) and isinstance(spec.data, PathInput):
        return True
    return (
        isinstance(spec, EachOf)
        and bool(getattr(spec, "alternatives", None))
        and all(is_pathinput_input(a) for a in spec.alternatives)
    )


@dataclass
class GlueSplit:
    """A call's chains by what heads them — the three application sites.

    * ``constant`` — applied in prepare, before the version keys
      (:func:`apply_constant_glue`);
    * ``pathinput`` — applied once per distinct resolved path in the loop
      (:class:`PerPathGlue`);
    * ``table`` — the Step-10/11 fusion point (:func:`fuse_glue`), or MATLAB's
      pre-loop site for MATLAB glue.
    """

    constant: GlueChains = field(default_factory=dict)
    pathinput: GlueChains = field(default_factory=dict)
    table: GlueChains = field(default_factory=dict)


def split_chains(inputs: dict[str, Any], chains: GlueChains) -> GlueSplit:
    """Split chains by head: THE one decision of where each chain runs.

    A chain naming a param that is not an input at all lands in ``table``,
    where ``fuse_glue`` reports it as "chain declared but no such input on this
    call" — one place that warning is worded, not two.
    """
    split = GlueSplit()
    for param, chain in chains.items():
        spec = inputs.get(param, _MISSING)
        if spec is _MISSING:
            split.table[param] = chain
        elif is_pathinput_input(spec):
            split.pathinput[param] = chain
            Log.info(
                f"[glue] '{param}': fed by a PathInput — chain applied once per "
                f"distinct resolved path, in the loop"
                + (
                    " (per_schema_key ignored: there is no table to slice)"
                    if any(s.per_schema_key for s in chain)
                    else ""
                )
            )
        elif is_constant_input(spec) or is_constant_axis(spec):
            split.constant[param] = chain
        else:
            split.table[param] = chain
    Log.debug(
        f"[glue] heads: constant={sorted(split.constant)}, "
        f"pathinput={sorted(split.pathinput)}, table={sorted(split.table)}"
    )
    return split


#: Version key carrying each Parameter-fed glue's identity, ``{param:
#: {"chain": chain_hash, "glued": canonical_hash(glued value)}}``. NOT a
#: call-site key (absent from ``foreach_config._CALL_ID_INCLUDED_KEYS``): like
#: ``__glue_hashes``, an edited body or a changed glued value is a new version
#: at the same call site. Read back by ``provenance_save.constant_bindings``.
CONSTANT_GLUE_KEY = "__constant_glue"


@dataclass(frozen=True)
class ConstantGlue:
    """What :func:`apply_constant_glue` did to one constant-valued param.

    ``raw`` is the value the user declared (what ``__constants`` records and
    the Parameter shows); the function received the glued value, identified
    by ``glued_hash``. Together with ``chain_hash`` they derive the virtual
    glue record the consumer binds — :func:`constant_glue_rid`.
    """

    raw: Any
    chain_hash: str
    glued_hash: str

    def version_entry(self) -> dict:
        """This glue's ``CONSTANT_GLUE_KEY`` entry."""
        return {"chain": self.chain_hash, "glued": self.glued_hash}


def constant_glue_config_keys(applied: "dict[str, ConstantGlue]") -> dict:
    """The ``CONSTANT_GLUE_KEY`` value for a call's applied constant glue."""
    return {p: applied[p].version_entry() for p in sorted(applied)}


#: Version key carrying each PathInput-fed glue's identity, ``{param:
#: {"chain": chain_hash}}``. NOT a call-site key (glue names are, through
#: ``__glue``). Read back by ``provenance.save._pathinput_glue``.
PATHINPUT_GLUE_KEY = "__pathinput_glue"

#: The third slot of a PathInput-glue record id. Fixed: the glued value is
#: produced in the loop, after identity is built, and a plain PathInput does
#: not track file content either (docs/claude/free-code-glue-nodes.md §1).
PATHINPUT_GLUE_SLOT = "pathinput"


def pathinput_glue_config_keys(chains: GlueChains) -> dict:
    """The ``PATHINPUT_GLUE_KEY`` value for a call's PathInput-fed chains."""
    return {p: {"chain": chain_hash(chains[p])} for p in sorted(chains)}


def pathinput_glue_rid(pathinput_record_id: str, entry: dict) -> str:
    """The virtual record a PathInput-fed glue's consumer binds — the
    PathInput-head twin of :func:`constant_glue_rid` (beside it, so every
    derivation of a glue record's identity lives in one place).

    ``entry`` is the call's :data:`PATHINPUT_GLUE_KEY` entry for the param.
    """
    from scidb.provenance.graph import compute_glue_record_id

    return compute_glue_record_id(
        entry["chain"], pathinput_record_id, PATHINPUT_GLUE_SLOT
    )


def apply_constant_glue(
    inputs: dict[str, Any],
    chains: GlueChains,
    preglued: "dict[str, Any] | None" = None,
) -> "dict[str, ConstantGlue]":
    """Apply glue attached to a **constant-valued** input, in prepare.

    Mutates ``inputs`` in place, replacing each glued value with the reshaped
    one — the value the FUNCTION receives. ``chains`` must already be the
    constant-fed head (:func:`split_chains`), passed by
    :func:`check_glue_executable`.

    ``preglued`` is ``{param: glued value}`` for chains whose body ran
    ELSEWHERE — MATLAB glue in a MATLAB run, which ``+scidb/for_each.m``
    applies before prepare because a ``.m`` body cannot run here. Such a
    value is taken as given; everything after (raw value recorded, glued
    value hashed, same history hop) is this one code path, so identity cannot
    tell which language ran the body.

    Returns ``{param: ConstantGlue}`` for every concrete value it glued: the
    caller records the RAW value in ``__constants`` and the glue's identity
    under :data:`CONSTANT_GLUE_KEY`, and history gets the same virtual
    ``__glue__`` hop a variable head gets (docs/claude/free-code-glue-nodes.md
    §1). Until 2026-10-21 the glued value itself was recorded as the constant,
    so history named the Parameter with the glue's OUTPUT (a dict where the
    user declared a path), and the skip gate — built from the raw value —
    never matched, so a Parameter-fed glue step recomputed on every run.

    This is the ``Parameter`` case (and a bare constant passed directly).

    Why here, and not at the Step-10/11 fusion point
    ------------------------------------------------
    A constant never becomes a loaded table, so the fusion point has nothing
    to reshape: it would fall into the "not a table — glue applied to the value
    as-is" branch, *after* ``ForEachConfig`` has already hashed the pre-glue
    value. Here, before Step 8 builds the version keys, the glued value's hash
    is known in time to enter identity: an edited glue body changes
    ``chain_hash``, and a changed glued value (e.g. a config FILE edited behind
    an unchanged path) changes ``glued_hash`` — either moves the virtual rid
    the consumer binds, which ``skip_computed`` compares.

    A multi-valued ``Parameter`` needs no special handling. ``for_each``'s
    Step 1 expands it into one recursive call per value *before* prepare, so
    each call glues its own concrete value and records it — the fan-out and
    the glue compose without either knowing about the other.

    Languages (§1, "Which language runs where")
    ------------------------------------------
    Python glue runs right here, on both run paths. MATLAB glue (MATLAB run
    only — :func:`check_glue_executable` refuses it elsewhere) arrives
    pre-glued through ``preglued``; it is never moved to a per-combo site,
    which would record the *pre*-glue constant and reintroduce the silent
    staleness this placement removes.
    """
    from scicanonicalhash import canonical_hash
    from scifor import EachOf

    from scidb.model.parameter import single_value

    preglued = dict(preglued or {})
    applied: dict[str, ConstantGlue] = {}
    for param in sorted(chains):
        chain = chains[param]
        # A single-valued Parameter that reached prepare unexpanded (the
        # MATLAB bridge has no Step 1) IS its one value.
        before = single_value(inputs[param])

        if chain_language(chain) != "python":
            if param not in preglued:
                raise GlueLanguageMismatchError(
                    f"glue chain on parameter '{param}' is "
                    f"{chain_language(chain)}, but no glued value arrived for "
                    f"it: a {chain_language(chain)} body must be applied by "
                    f"its own runtime before prepare (+scidb/for_each.m does "
                    f"this for MATLAB glue on a constant)"
                )
            glued = preglued[param]
            inputs[param] = glued
            cg = ConstantGlue(
                raw=before,
                chain_hash=chain_hash(chain),
                glued_hash=canonical_hash(glued),
            )
            applied[param] = cg
            Log.info(
                f"[glue] '{param}': {chain_language(chain)} glue was applied to "
                f"the constant before prepare ({type(before).__name__} -> "
                f"{type(glued).__name__}); __constants records the RAW value "
                f"and the glue is a history hop (chain {cg.chain_hash}, glued "
                f"value {cg.glued_hash})"
            )
            continue

        if is_constant_axis(before):
            # A multi-valued fan-out that reached prepare unexpanded. Both run
            # paths expand an EachOf before prepare (for_each's Step 1,
            # for_each.m's Step 0), so this is a defence, not a route: there
            # is no single raw value to record, so no history hop is written
            # and the glued alternatives are recorded as before.
            values = list(before.alternatives)
            inputs[param] = EachOf(
                *(apply_glue_chain(v, chain, param=param) for v in values)
            )
            Log.warn(
                f"[glue] '{param}': {len(values)} unexpanded value(s) glued in "
                f"prepare — no history hop recorded for them (expected every "
                f"EachOf to be expanded before prepare)"
            )
            continue

        glued = apply_glue_chain(before, chain, param=param)
        inputs[param] = glued
        cg = ConstantGlue(
            raw=before,
            chain_hash=chain_hash(chain),
            glued_hash=canonical_hash(glued),
        )
        applied[param] = cg
        Log.info(
            f"[glue] '{param}': applied {len(chain)} node(s) to the constant "
            f"value ({type(before).__name__} -> {type(glued).__name__}) in "
            f"prepare; __constants records the RAW value and the glue is a "
            f"history hop (chain {cg.chain_hash}, glued value {cg.glued_hash})"
        )
    return applied


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

    Step 1 runs only for a **Python** chain, whichever run this is (Python
    glue in a MATLAB run is applied right here, in prepare). A MATLAB chain
    gets step 2 only: its bodies run in ``+scidb/for_each.m``, but the
    identity is still Python's to compute.

    PathInput-fed chains never reach this function (:func:`split_chains`):
    there is no table for them here, and their identity is a different hop.
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

        apply_bulk = chain_language(chain) == "python"
        if bulk and not apply_bulk:
            Log.debug(
                f"[glue] '{param}': {chain_language(chain)} chain — identity "
                f"recorded here, bodies run on the {chain_language(chain)} side"
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


class PerPathGlue:
    """Run one PathInput-fed chain once per distinct resolved path.

    THE owner of the "glue at the PathInput's level" rule
    (docs/claude/free-code-glue-nodes.md §1, "A PathInput-fed parameter"):
    within one run, the chain runs once per distinct ``str(path)``, and every
    later combo that resolves to the same file gets the cached result. A
    ``{subject}`` PathInput under a ``[subject, session, trial]`` function is
    therefore glued once per subject, not once per trial.

    Each call hands back a ``copy.deepcopy`` of the cached value, so a
    function that mutates its input cannot leak into the next combo.

    Used by the Python run's ``fn`` wrapper and, for Python glue in a MATLAB
    run, by ``scimatlab.bridge.apply_path_glue``. MATLAB glue keeps its own
    ``containers.Map`` with the same key in ``+scidb/for_each.m``.
    """

    def __init__(self, param: str, chain: Sequence[GlueSpec]) -> None:
        self.param = param
        self.chain = list(chain)
        self._cache: dict[str, Any] = {}
        self.calls = 0

    def __call__(self, value: Any) -> Any:
        import copy

        self.calls += 1
        key = str(value)
        if key in self._cache:
            Log.debug(f"[glue] '{self.param}': path {key} glued (cached)")
        else:
            t0 = time.perf_counter()
            self._cache[key] = apply_glue_chain(value, self.chain, param=self.param)
            Log.debug(
                f"[glue] '{self.param}': path {key} glued (miss) -> "
                f"{type(self._cache[key]).__name__} in "
                f"{time.perf_counter() - t0:.3f}s"
            )
        return copy.deepcopy(self._cache[key])

    def log_summary(self) -> None:
        """INFO: distinct paths vs combos. N == M on a coarse PathInput means
        the cache is not doing its job."""
        if self.calls:
            Log.info(
                f"[glue] '{self.param}': {len(self._cache)} distinct path(s) "
                f"glued for {self.calls} combo(s)"
            )


def per_path_glue(chains: GlueChains) -> "dict[str, PerPathGlue]":
    """``{param: PerPathGlue}`` for a call's Python PathInput-fed chains.
    MATLAB chains are left to ``+scidb/for_each.m``."""
    return {
        p: PerPathGlue(p, chains[p])
        for p in sorted(chains)
        if chain_language(chains[p]) == "python"
    }


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
