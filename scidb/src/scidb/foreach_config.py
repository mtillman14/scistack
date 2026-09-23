"""ForEachConfig — serializes for_each() computation config into version keys."""

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from scilineage.hashing import (
    compute_function_hash,
    compute_function_hash_with_sources,
)


def _compute_fn_hash(fn: Callable) -> str:
    """Compute a stable hash of the function's bytecode and constants.

    Uses bytecode-based hashing (via scilineage.hashing.compute_function_hash),
    which only changes when the function's actual logic changes. Ignores cosmetic
    changes like whitespace, comments, and formatting.

    Returns 16 hex chars (truncated SHA-256) for version_keys storage.

    Args:
        fn: The function to hash (can be plain function or LineageFcn wrapper).

    Returns:
        16-character hex string hash.
    """
    return compute_function_hash(fn, truncate=16)


def function_hash_for(fn) -> str:
    """The hash the SAVE path recorded in ``_invocation.function_hash`` for ``fn``.

    Read-side counterpart to whatever wrote ``__fn_hash``. Every staleness
    question ("has this function changed since it produced that record?") is a
    comparison against a *stored* hash, so it has to reproduce the stored
    recipe rather than invent its own — that mismatch is a permanent false
    "changed", which reads as a node that can never go green.

    Two recipes exist:

    - **An explicit ``source_hash``** on the function object wins. A MATLAB
      function reaches Python as a ``scimatlab.MatlabLineageFcn`` whose
      ``.fcn`` is a bare name-holder with no source at all, so AST-hashing it
      returns a constant with no relationship to the ``.m`` file. MATLAB
      computes the real digest itself and the bridge stores it verbatim; this
      is that same value. Duck-typed on purpose — scidb does not import
      scimatlab.
    - **Otherwise the AST hash**, unwrapping ``.fcn`` exactly as before so a
      lineage wrapper hashes the function it wraps.

    Deliberately NOT used by ``ForEachConfig.to_version_keys``: that path has
    always hashed the object it was handed without the ``.fcn`` unwrap, and
    changing it would silently alter the hashes stored for existing databases.
    """
    explicit = getattr(fn, "source_hash", None)
    if explicit:
        return str(explicit)
    return _compute_fn_hash(fn.fcn if hasattr(fn, "fcn") else fn)


def function_sources_for(fn) -> tuple[str, str | None, dict]:
    """``(hash, entry_name, {unit_name: source})`` — the code to file under the
    hash the SAVE path stores.

    Mirrors :func:`function_hash_for` in spirit but **not** in recipe, and the
    difference is the entire point of this function existing separately. The
    stored ``__fn_hash`` comes from ``to_version_keys`` →
    ``_compute_fn_hash(self.fn)``, which hashes the object it is handed with no
    ``.fcn`` unwrap. Source therefore has to be collected from that same object,
    or it would be filed under a hash nothing was ever stored beneath — source
    that exists but can never be found, which is worse than none.

    The returned hash is what the caller must **verify** against the stored one
    before writing. It is returned rather than assumed precisely so that check
    is possible.

    MATLAB is handled by duck-typing a ``source_text`` attribute, matching how
    ``function_hash_for`` duck-types ``source_hash`` (scidb does not import
    scimatlab). **Nothing supplies it yet** — ``MatlabLineageFcn`` and the
    bridge's sentinel carry only the digest — so MATLAB functions currently
    capture no source. The hook is here so closing that is a bridge-side change
    rather than a scidb one.

    A ``source_hash`` with NO text means exactly that: nothing to capture,
    returned as no units. The object in hand is then only a stand-in for the
    real (MATLAB) function, and hashing ITS Python body produced a hash that
    never matched the stored one — a false "recipes have drifted" warning on
    every MATLAB run (cleanup-audit F13).
    """
    explicit_text = getattr(fn, "source_text", None)
    inner = getattr(fn, "fcn", fn)
    name = getattr(inner, "__name__", None) or getattr(fn, "__name__", "?")
    if explicit_text:
        return str(getattr(fn, "source_hash", "") or ""), name, {name: explicit_text}
    digest = getattr(fn, "source_hash", None)
    if digest:
        return str(digest), name, {}

    fn_hash, units = compute_function_hash_with_sources(fn, truncate=16)
    # `_hash_source` records the entry point before recursing into callees, and
    # dicts preserve insertion order, so the first unit is the function itself.
    entry = next(iter(units), None)
    return fn_hash, entry, units


# The canonical for_each call-site identity is captured by exactly these
# version_keys fields (see ForEachConfig.to_version_keys()).  Anything else
# in a saved version_keys dict — direct constants unpacked as top-level
# keys, ``__upstream``, ``__output_num``, scihist's lineage extras — is
# per-record bookkeeping and must not affect the call_id.
#
# ``__fn_hash`` is intentionally excluded too, so cosmetic source edits to
# the function body don't fork the call site (see ForEachConfig.to_call_id
# docstring for rationale).
#
# ``__where`` is also excluded: a where= filter's only effect on the computation
# is the surviving input set (already folded into invocation_id / ``__inputs``);
# the where_clause string itself is display-only (§10 where= redesign), so two
# call sites differing only by where= share a call_id. (``to_version_keys`` still
# emits ``__where`` because for_each writes it to ``_run.where_clause`` for display.)
#
# ``__glue`` (the glue node NAMES per param) IS included, following the same
# split: a different glue chain is a different call site, while an edit to a
# glue body is a new version at the same call site — so ``__glue_hashes`` is
# excluded exactly as ``__fn_hash`` is. (An edited body still invalidates
# downstream results, but through the provenance graph, not here: see
# docs/claude/free-code-glue-nodes.md §2.)
_CALL_ID_INCLUDED_KEYS = (
    "__fn",
    "__inputs",
    "__constants",
    "__distribute",
    "__as_table",
    "__across_variants",
    "__glue",
)


def _hash_call_site(version_keys: dict) -> str:
    """The 16-hex-char hash of a call site's keys.

    A strict allow-list of the canonical config keys, so per-record fields
    that ride in the same dict never leak in. Private: the PAYLOAD is
    assembled by :class:`CallSite` and nowhere else, so this is called from
    exactly one place.
    """
    keys = {k: version_keys[k] for k in _CALL_ID_INCLUDED_KEYS if k in version_keys}
    payload = json.dumps(keys, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class RunOptions:
    """HOW a ``for_each`` call is made — the identity-bearing execution modes,
    as one value.

    ``distribute`` (fan the result out over the iterated key), ``as_table``
    (pool an input into one frame) and ``across_variants`` (pool every
    variant group of an input into one call) are not settings: flipping any
    of them names a DIFFERENT run, which writes a second record at the same
    schema location. They are folded into ``invocation_id``, stored as
    columns on ``_invocation``, and reported by
    ``provenance_query.run_options_label``.

    They exist as a value because they were three loose keyword arguments
    threaded through a dozen call sites, and the ones that forgot to pass
    them did not fail — they computed the identity of a call nobody made. Two
    GUI paths hardcoded ``distribute=False, as_table=None`` while the node
    beside them had saved otherwise; ``across_variants`` would have been a
    fourth thing to forget.

    ``as_table`` is kept in its DECLARED form (``True`` = every loadable
    input, or an explicit list) and resolved to names by
    :meth:`resolved_as_table`, because ``True`` only means something against
    a particular parameter list — spelling it out early is what made the
    forward and backward call ids disagree.
    """

    distribute: bool = False
    as_table: Any = None
    across_variants: Sequence[str] = ()

    @classmethod
    def from_config(cls, raw: "Mapping[str, Any] | None") -> "RunOptions":
        """From a stored/GUI options blob (``{"distribute": ..., "as_table":
        ...}``), tolerating a missing or empty one."""
        raw = raw or {}
        return cls(
            distribute=bool(raw.get("distribute", False)),
            as_table=raw.get("as_table") or None,
            across_variants=tuple(raw.get("across_variants") or ()),
        )

    def resolved_as_table(self, loadable_params: "Iterable[str]") -> list[str]:
        """``as_table`` as sorted parameter names — ``True`` means all of
        *loadable_params* (``provenance.normalize_as_table``, the one rule)."""
        from .provenance import normalize_as_table

        return normalize_as_table(self.as_table, list(loadable_params))

    def to_config(self) -> dict:
        """Back to the blob shape the GUI stores and MATLAB renders."""
        out: dict = {"distribute": self.distribute}
        if self.as_table:
            out["as_table"] = self.as_table
        if self.across_variants:
            out["across_variants"] = list(self.across_variants)
        return out


@dataclass(frozen=True)
class CallSite:
    """What is unique to ONE ``for_each`` call site, and the one assembly of
    the payload its ``call_id`` hashes.

    Four readers arrive here from four shapes — live inputs
    (``ForEachConfig.call_site``), a stored variant config
    (``provenance_query.config_call_id``), a ``pipeline_variants`` row, and
    the GUI's binding dicts (``variant_resolver.compute_call_id``) — and
    until 2026-09-20 each re-spelled the rules below by hand: which keys,
    which are omitted when unset, that ``as_table`` is a sorted list. The
    parity suite kept the two scidb spellings equal by TEST; the GUI's was
    not in the suite, and ``as_table=True`` was written as ``True`` forward
    and as the resolved list backward, so that call site never matched. Now
    the spellings are equal by construction: a caller maps its shape onto
    these fields and reads ``call_id``.

    ``inputs`` is the call-site view of each loadable input — the TYPE it
    binds (a ``PathInput`` contributes its ``to_key()``): what narrows WHICH
    records (a Fixed pin, a column selection, a Variant pin) is invocation
    identity, on the edge, not here. ``options`` is the :class:`RunOptions`
    the call runs under. ``glue`` maps a parameter to its glue node NAMES: a
    different chain is a different call site, an edited body is a new version
    at the same one.
    """

    fn_name: str
    inputs: Mapping[str, str] = field(default_factory=dict)
    constants: Mapping[str, Any] = field(default_factory=dict)
    options: RunOptions = field(default_factory=lambda: RunOptions())
    glue: Mapping[str, Sequence[str]] = field(default_factory=dict)

    def version_keys(self) -> dict:
        """The call-site keys, spelled the one way ``call_id`` hashes them."""
        keys: dict = {"__fn": self.fn_name}
        if self.inputs:
            keys["__inputs"] = {k: self.inputs[k] for k in sorted(self.inputs)}
        # Always present, even when empty (a record's version keys carry it).
        keys["__constants"] = dict(self.constants)
        if self.options.distribute:
            keys["__distribute"] = True
        # `True` resolves against THIS call site's own loadable inputs, so
        # `as_table=True` and the explicit list of the same params hash alike.
        as_table = self.options.resolved_as_table(self.inputs)
        if as_table:
            keys["__as_table"] = as_table
        pooled = sorted(str(p) for p in self.options.across_variants)
        if pooled:
            keys["__across_variants"] = pooled
        if self.glue:
            keys["__glue"] = {p: list(self.glue[p]) for p in sorted(self.glue)}
        return keys

    @property
    def call_id(self) -> str:
        return _hash_call_site(self.version_keys())


class ForEachConfig:
    """Serializes for_each() computation config into version keys.

    Captures the parts of a for_each() call that affect the computation's
    identity but are not part of the schema metadata: the function, loadable
    inputs (which variable types / Fixed wrappers are used), where= filter,
    and other behavioral flags.

    These keys are merged into save_metadata so that changing the config
    (e.g. switching smoothing=0.2 to smoothing=0.3, or adding a where= filter)
    creates a new version_keys group rather than silently overwriting existing
    results.
    """

    def __init__(
        self,
        fn: Callable,
        inputs: dict[str, Any],
        where=None,
        distribute: bool = False,
        as_table=None,
        glue: "dict[str, list] | None" = None,
    ):
        self.fn = fn
        self.inputs = inputs
        self.where = where
        self.distribute = distribute
        self.as_table = as_table
        # {param: [GlueSpec, ...]} — normalized glue chains (see scidb.glue).
        self.glue = glue or {}

    @property
    def options(self) -> RunOptions:
        """The run options this call runs under — the two ``for_each`` flags
        plus the inputs it pools across variant groups (a wrapper, not a
        kwarg, but the same kind of fact)."""
        return RunOptions(
            distribute=bool(self.distribute),
            as_table=self.as_table,
            across_variants=tuple(self.across_variants),
        )

    def call_site(self) -> "CallSite":
        """This call as a :class:`CallSite` — the ONE assembly of what is
        unique to a call site. ``inputs`` is the call-site view
        (:meth:`call_site_inputs`)."""
        from .glue import chain_names

        return CallSite(
            fn_name=getattr(self.fn, "__name__", repr(self.fn)),
            inputs=self.call_site_inputs(),
            constants=self._get_direct_constants(),
            options=self.options,
            glue={p: chain_names(c) for p, c in self.glue.items()},
        )

    def to_version_keys(self) -> dict:
        """Return dict of config keys to merge into save_metadata.

        The call-site keys come from :meth:`call_site` (so a version key can
        never spell a run option differently from the call id), with three
        version-only additions: ``__inputs`` as the LOADED view — wrappers
        and all (``Fixed(X, subject=1)``, ``ColumnSelection(...)``), because a
        pin or a column change must fork the record even though it does not
        fork the call site — ``__fn_hash`` (an edit is a new version at the
        same site) and ``__glue_hashes`` (likewise), plus ``__where`` for
        display (``_run.where_clause``; not identity of either kind).

        All values are plain Python objects (dicts, strings, bools, lists).
        Consumed in-memory to build save_metadata / the call_id (no longer a
        stored column).
        """
        keys = self.call_site().version_keys()
        keys["__fn_hash"] = _compute_fn_hash(self.fn)
        inputs_dict = self._serialize_inputs()
        if inputs_dict:
            keys["__inputs"] = inputs_dict
        else:
            keys.pop("__inputs", None)
        if self.where is not None:
            # where can be a string or a Filter object
            # For RawFilter created from string, preserve original string format
            from .filters import RawFilter

            if isinstance(self.where, str):
                keys["__where"] = self.where
            elif isinstance(self.where, RawFilter) and hasattr(
                self.where, "_original_str"
            ):
                # Preserve original string for string-based filters
                keys["__where"] = self.where._original_str
            elif hasattr(self.where, "to_key"):
                keys["__where"] = self.where.to_key()
            else:
                keys["__where"] = str(self.where)
        if self.glue:
            from .glue import chain_hashes

            keys["__glue_hashes"] = {
                p: chain_hashes(c) for p, c in sorted(self.glue.items())
            }
        return keys

    def to_call_id(self) -> str:
        """Stable identifier for this for_each() call site, 16 hex chars —
        :attr:`CallSite.call_id` of :meth:`call_site`.

        Cosmetic edits to the function source do not fork the call site
        (``__fn_hash`` is a version key, not a call-site key). Two for_each()
        calls with the same loadable input TYPES, constants, distribute,
        as_table, across_variants and glue names share an id even if the
        function body was reformatted between runs.

        **Forward must equal backward, by construction.** This id is compared
        against ``provenance_query.config_call_id`` — the same ``CallSite``
        filled from what the graph RECORDED (``param -> record -> variable
        type``) — by ``check_node_state`` when a pipeline step scopes its
        state to its own call site. The graph keeps no trace of a column
        selection or a Fixed pin beyond the edge, so the call-site view of
        an input is the type it binds (:meth:`call_site_inputs`);
        ``to_version_keys`` keeps the wrapper, so a column change still
        re-runs.
        """
        return self.call_site().call_id

    @property
    def across_variants(self) -> list[str]:
        """The parameters wrapped in ``AcrossVariants``, sorted."""
        from .across_variants import AcrossVariants

        return sorted(p for p, s in self.inputs.items() if isinstance(s, AcrossVariants))

    def call_site_inputs(self) -> dict:
        """``{param: identity}`` as the call SITE sees it — the shape
        ``provenance_query.config_from_inputs`` builds from live inputs and
        ``pipeline_variants`` reconstructs from stored edges.

        What is unique to a call SITE is which TYPE feeds each parameter.
        Everything that narrows WHICH records of that type — a
        ``ColumnSelection``'s columns, a ``Fixed`` pin's metadata, a
        ``Variant`` pin's branch params — is invocation identity, on the
        EDGE: the selector and the consumed record ids already say exactly
        which records were read, so folding the wrapper in here would fork
        the call site for a narrowing the graph can reproduce without it
        (the same reasoning that keeps ``where=`` out, §10.1). The canvas
        draws one node for ``Var``, ``Var["a"]``, ``Fixed(Var, subject=1)``
        and ``Variant(Var, low_hz=20)`` alike, and the backward
        reconstruction — which only ever sees ``param -> record -> type`` —
        agrees by construction.

        Every wrapper peels through ONE unwrap (:mod:`scidb.input_spec`).
        Before 2026-09-20 each identity path had its own, handling a
        different subset: a ``Fixed`` pin forked the forward id (a pinned
        step never planned green) and a ``Variant`` pin vanished from the
        predicted config entirely.
        """
        from scifor import PathInput

        from .input_spec import is_loadable, type_name

        result = {}
        for name in sorted(self.inputs):
            spec = self.inputs[name]
            # Every wrapper peels: a Fixed pin, a column selection, an
            # AcrossVariants pooling and a Variant pin all bind the same TYPE
            # (`input_spec`, the ONE unwrap). What each of them narrows is
            # invocation identity — the edge, the selector, the recorded run
            # option — never the call site.
            inner = type_name(spec)
            if inner is not None:
                result[name] = inner
                continue
            # Not a variable type: a PathInput (its template IS the call
            # site), or a Merge / DataFrame that spells itself.
            if is_loadable(spec) or isinstance(spec, PathInput):
                if hasattr(spec, "to_key"):
                    result[name] = spec.to_key()
                else:
                    result[name] = repr(spec)
        return result

    def _get_direct_constants(self) -> dict:
        """Return scalar constant inputs (non-loadable values).

        ColName and PathOutput markers are excluded: they are resolution
        markers, not real constant values, and their effect is determined per
        combo/column at run time rather than being a fixed scalar. ColName
        resolves from the input variable (already captured in ``__inputs``);
        PathOutput resolves a template into an output path, which is write
        bookkeeping rather than computation identity. Including either raw
        marker object would also break version-key hashing (they are not
        JSON-serializable). PathInput is excluded for the same
        JSON-serializability reason -- despite is_loadable now excluding it
        (its per-combo resolution moved to scifor's for_each loop), it still
        belongs in ``__inputs`` via its own ``to_key()``, not here.

        A single-valued :class:`~scidb.parameter.Parameter` is **unwrapped to
        its value**. The wrapper is a discovery/documentation aid, never part
        of computation identity: ``Parameter(30)`` and a bare ``30`` are the
        same input and must hash identically, or the same pipeline forks in
        history depending on how it was declared. Without unwrapping, the
        wrapper reached ``canonical_hash`` as an unknown type and raised
        ``ValueError: Unserializable data type``.

        In normal execution a Parameter never actually gets this far --
        ``for_each`` expands it as an ``EachOf`` first, so ``self.inputs``
        already holds the concrete value. This stays as the defence for a
        ``ForEachConfig`` built directly, and as the thing that makes the
        one-value case provably identical either way.
        """
        from scifor import ColName, PathInput, PathOutput

        from .input_spec import is_loadable
        from .parameter import Parameter

        def _unwrap_parameter(v):
            if isinstance(v, Parameter) and len(v.alternatives) == 1:
                return v.alternatives[0]
            return v

        return {
            k: _unwrap_parameter(v)
            for k, v in self.inputs.items()
            if not is_loadable(v) and not isinstance(v, (ColName, PathOutput, PathInput))
        }

    def _serialize_inputs(self) -> dict:
        """Serialize loadable inputs to a dict.

        Only includes loadable inputs (variable types, Fixed, ColumnSelection,
        Merge) — constants are already included in save_metadata directly.
        PathInput is included too even though is_loadable excludes it (its
        resolution moved to scifor's for_each loop, not scidb's variable
        loader) -- it still needs a stable identity in ``__inputs`` via its
        own ``to_key()``, or two different templates would collapse into the
        same version-key group.

        Returns a dict (not JSON string) so it can be carried in the in-memory
        config keys that build save_metadata.
        """
        from scifor import PathInput

        from .input_spec import is_loadable

        result = {}
        for name in sorted(self.inputs):
            spec = self.inputs[name]
            if is_loadable(spec) or isinstance(spec, PathInput):
                if hasattr(spec, "to_key"):
                    result[name] = spec.to_key()
                elif isinstance(spec, type):
                    result[name] = spec.__name__
                else:
                    result[name] = repr(spec)
        return result
