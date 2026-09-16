"""Variant branch_param pinning wrapper for variable type inputs in for_each (DB-backed)."""

from typing import Any

#: Reserved namespace marking a **code-version** pin inside an otherwise ordinary
#: branch-params filter dict.
#:
#: Code versions ride the existing ``branch_params_filter`` seam rather than a
#: parallel parameter of their own, and that is a design decision, not a
#: shortcut. Since ``Code:<fn>`` became a variant column sitting beside the
#: branch params (docs/claude/variant-selection.md §1), "which variant of this
#: input" is **one** question with two dimensions — so it gets one filter. The
#: alternative was threading a second dict through ~10 call sites that already
#: carry this one, with no gain.
#:
#: The namespace cannot collide with a real branch param: those are always
#: ``{producing_fn}.{param}``, and ``__code__`` is not a producing function.
#: (``__save__.<kwarg>`` is the established precedent for a synthetic namespace
#: in this same dict.)
CODE_PIN_PREFIX = "__code__"

#: Reserved ``branch_params`` key for a **run-options** pin — the third variant
#: dimension after constants and code. ``__run__`` (bare) or ``__run__.<fn>``
#: (disambiguated), valued with a :func:`~scidb.provenance_query.run_options_label`
#: string such as ``"distribute=true"``, or ``"latest"``.
#:
#: Exists because ``distribute``/``as_table`` are folded into ``invocation_id``:
#: re-running unchanged code under a different flag writes a SECOND record at
#: every location, and nothing in constants or code tells the two apart.
#: Same non-collision argument as ``__code__``.
RUN_PIN_PREFIX = "__run__"

#: ``code_version="latest"`` — keep each schema location's own newest code
#: chain, rather than a named ordinal.
#:
#: Deliberately NOT a synonym for "the highest ordinal". A global newest would
#: drop every location never re-run under it, silently removing subjects from a
#: run — the trap documented at ``scistackplotdb/load.py`` for the plotting pin.
#: This resolves per location, so each contributes its own most recent.
LATEST_VERSION = "latest"


#: Display spellings of the two synthetic pin dimensions, as the plotting layer
#: and the GUI picker name their columns (``Code:bandpass``, ``Run:loader``,
#: ``CodeIsLatest``).
#:
#: They live **here**, in the layer that owns the pin vocabulary, even though
#: the columns are a display-layer idea, because the translation has to be
#: available to every consumer that hands scidb a selection — the plotting
#: layer, the GUI RPC, and the ``scidb`` CLI. scidb cannot import the display
#: layer (``scistackplot`` must never import scidb — the CSV path depends on
#: that), so the alternative was the same three-line mapping written once per
#: consumer, which is exactly the scifor/scidb duplication this codebase has
#: already paid for once.
CODE_COLUMN_PREFIX = "Code:"
RUN_COLUMN_PREFIX = "Run:"

#: The per-row "newest at my own schema location" flag a variant table carries.
#: As a *pin* it means ``__code__ = "latest"`` — the same per-location rule,
#: spelled as a column.
LATEST_COLUMN_NAME = "CodeIsLatest"


def is_code_or_run_pin(key: str) -> bool:
    """True for a ``__code__`` / ``__run__`` key (bare or ``.<fn>``-qualified)."""
    return (
        key == CODE_PIN_PREFIX
        or key.startswith(f"{CODE_PIN_PREFIX}.")
        or key == RUN_PIN_PREFIX
        or key.startswith(f"{RUN_PIN_PREFIX}.")
    )


def pin_loads_uncollapsed(branch_params: dict | None) -> bool:
    """Whether a pin must load with ``version_id="all"`` to be satisfiable.

    ``version_id="latest"`` collapses on a variant key of ``(fn_name,
    branch_params, output_num, consumed_locations)``. ``function_hash`` is
    deliberately NOT in it — a body re-run is a newer version of the same
    variant rather than a rival (docs/claude/function-version-variants.md) —
    and since 2026-09-14 the collapse also supersedes an older
    ``distribute``/``as_table`` run of the same code and constants. So two code
    versions, or two run-option sets, merge and the newer wins *before* any
    filter runs: a code or run pin against a collapsed load would always match
    nothing. A branch-param pin is unaffected — ``branch_params`` IS in the
    key, so those variants never merged.

    One rule, consulted by every path that resolves a pin to records — the
    for_each input loader and ``provenance_query.records_for_variant`` — so
    "load this variant" and "introspect this variant" cannot disagree about
    which records exist.
    """
    return any(is_code_or_run_pin(key) for key in (branch_params or {}))


def normalize_pin_key(key: str) -> str:
    """One selection key → its canonical ``branch_params`` spelling.

    ``Code:<fn>`` → ``__code__.<fn>``, ``Run:<fn>`` → ``__run__.<fn>``;
    anything else is already scidb's own namespacing (``fn.param``,
    ``__code__``, a bare suffix-matched name) and passes through untouched.
    """
    if key.startswith(CODE_COLUMN_PREFIX):
        return f"{CODE_PIN_PREFIX}.{key[len(CODE_COLUMN_PREFIX):]}"
    if key.startswith(RUN_COLUMN_PREFIX):
        return f"{RUN_PIN_PREFIX}.{key[len(RUN_COLUMN_PREFIX):]}"
    return key


def normalize_selection(selection: dict | None) -> dict:
    """A column-keyed *or* already-canonical selection → ``branch_params``.

    Idempotent: a dict that is already in ``Variant.branch_params`` form comes
    back unchanged, so a caller never has to know which dialect it is holding.

    ``CodeIsLatest: True`` becomes ``__code__ = "latest"``. ``CodeIsLatest:
    False`` has no scidb spelling — "not the latest" is not a pin — so it is
    dropped with a warning rather than silently inverted.
    """
    out: dict = {}
    for key, value in (selection or {}).items():
        if key == LATEST_COLUMN_NAME:
            if value is False:
                from .log import Log

                Log.warn(
                    f"selection pins {LATEST_COLUMN_NAME}=False, which scidb "
                    f"cannot express (there is no 'not the latest' pin) — "
                    f"ignoring it"
                )
                continue
            out[CODE_PIN_PREFIX] = LATEST_VERSION
        else:
            out[normalize_pin_key(key)] = value
    return out


def branch_param(fn: str, **params: Any) -> dict:
    """Build a namespaced branch-param selector dict without a dotted-string kwarg.

    branch_params are namespaced per producing function (``f"{fn}.{param}"``). A bare
    name is resolved by suffix-match at load time, but collides when two pipeline
    steps share a param name (raising ``AmbiguousParamError``). The disambiguating
    namespaced form can't be written as a kwarg (a ``.`` is not a valid identifier),
    so this helper builds the dict for you — ``**``-unpack it into the load kwargs
    (non-schema kwargs become the branch-params filter)::

        Filtered.load(subject="S01", **branch_param("bandpass", low_hz=30))

    is equivalent to the awkward ``Filtered.load(subject="S01", **{"bandpass.low_hz": 30})``.
    """
    return {f"{fn}.{k}": v for k, v in params.items()}


class Variant:
    """
    Wrapper to pin an input to a specific branch_param variant.

    branch_param pinning is an **orthogonal, load-time filter**: it selects which
    branch_param variant of a variable to load, distinct from the other input
    wrappers' concerns:

    - ``ColumnSelection`` (``MyVar["col"]``) — which columns (after load)
    - ``Fixed(…, session="BL")`` — which schema metadata (per-combo, scifor loop)
    - ``Merge(…)`` — join several inputs (top level)
    - ``Variant(…, low_hz=20)`` — which branch_param variant (load time)

    Because branch_param pinning is threaded as a separate ``branch_params_filter``
    parameter through the loader (exactly like ``where=``), composition with the
    other wrappers is **order-agnostic**::

        Fixed(Variant(X, low_hz=20), session="BL") == Variant(Fixed(X, session="BL"), low_hz=20)

    ``Variant`` may also be a ``Merge`` constituent for **per-constituent** pinning::

        Merge(Variant(A, low_hz=20), B)

    branch_params are namespaced per producing function (e.g. ``bandpass.low_hz``);
    a bare name (``low_hz``) is matched by suffix at load time. When the bare name
    is ambiguous (two pipeline steps share a param name), disambiguate with the
    ``fn=`` keyword instead of a dotted-string kwarg::

        Variant(X, fn="detect_spikes", threshold=0.5)   # → "detect_spikes.threshold"

    This is exactly equivalent to ``Variant(X, **{"detect_spikes.threshold": 0.5})``
    but avoids the ``.``-in-a-kwarg wart.

    Pinning an input to one variant also fixes aggregation-mode variant smushing:
    variant expansion only sees matching records, so an aggregation no longer pools
    multiple distinct variants into one table.

    Example::

        # Run fn over only the low_hz=20 variant of FilteredEMG
        for_each(fn, {"x": Variant(FilteredEMG, low_hz=20)}, [Out], subject=[1, 2])

        # Run once per pinned variant, results concatenated
        EachOf(Variant(FilteredEMG, low_hz=20), Variant(FilteredEMG, low_hz=50))

    **Code versions are a second variant dimension** (``code_version=``), pinned
    the same way and through the same filter. A record's variant identity has
    two parts — the constants upstream of it, and the *code* that produced it
    (docs/claude/variant-selection.md) — and both are things a user may want to
    hold fixed::

        Variant(FilteredEMG, code_version="v1")                 # one upstream
        Variant(FilteredEMG, fn="bandpass", code_version="v1")  # disambiguated
        Variant(FilteredEMG, code_version="latest")             # current code
        Variant(FilteredEMG, low_hz=20, code_version="v2")      # both at once

    A bare ``code_version`` resolves against whichever upstream function has more
    than one recorded version; ``fn=`` names it when several do. Ordinals are the
    per-function ``v1``/``v2``/… of
    :func:`~scidb.provenance_query.code_version_ordinals`, so ``v2`` means the
    same code wherever it appears.

    ``"latest"`` is resolved **per schema location**, not as "the highest
    ordinal" — a location never re-run under the newest code still contributes
    its own newest record instead of vanishing from the run. Pin a named ordinal
    only when you mean it: that *does* drop locations which never ran it, which
    is occasionally what you want and never what you want by accident.

    The declared-axis form composes exactly as it does for branch params::

        EachOf(Variant(EMG, code_version="v1"), Variant(EMG, code_version="v2"))
    """

    def __init__(
        self,
        var_type: Any,
        *,
        fn: str | None = None,
        code_version: str | None = None,
        run_options: str | None = None,
        **branch_params: Any,
    ):
        """
        Args:
            var_type: The variable type to load (must have a ``.load()`` method),
                      or a ``ColumnSelection`` / ``Fixed`` wrapper, or a nested
                      ``Variant``.
            fn: Optional producing-function name used to disambiguate the
                      ``branch_params`` below. When given, each branch_param ``k`` is
                      namespaced to ``f"{fn}.{k}"`` (the namespaced form is then
                      matched exactly at load time, never via the ambiguous suffix
                      path). Use this instead of a dotted-string kwarg.
            code_version: Optional **code**-version pin — ``"v1"``/``"v2"``/… (a
                      per-function ordinal from
                      ``provenance_query.code_version_ordinals``) or
                      ``"latest"``. Bare, it resolves against whichever upstream
                      function has more than one version and raises
                      ``AmbiguousParamError`` when several do; ``fn=`` names one
                      explicitly. Note the asymmetry: a named ordinal drops
                      schema locations that never ran it, while ``"latest"`` is
                      resolved per location and drops none.
            run_options: Optional **run-options** pin — a
                      ``provenance_query.run_options_label`` string such as
                      ``"distribute=true"`` or ``"distribute=false, as_table=[df]"``,
                      or ``"latest"``. Selects records whose upstream function
                      ran under exactly those ``for_each`` flags. Bare, it
                      resolves against whichever upstream function has run more
                      than one way (``AmbiguousParamError`` when several have);
                      ``fn=`` names one. ``"latest"`` is the same per-location
                      rule as ``code_version="latest"`` (one ``is_latest``, run
                      options included).
            **branch_params: branch_param key/value pairs to pin. Bare names are
                      suffix-matched against namespaced branch_params at load time
                      (unless ``fn=`` is given, which namespaces them).

        Raises:
            TypeError: If wrapping a ``Merge`` (pin per constituent instead) or an
                ``EachOf`` (EachOf must stay the outermost wrapper).
            ValueError: If no branch_params are given, or a nested ``Variant``
                supplies a conflicting value for the same key.
        """
        from scifor import EachOf, Merge

        # ``fn=`` namespaces the supplied params under the producing function so the
        # exact-match load path resolves them without the dotted-string kwarg wart.
        if fn is not None:
            branch_params = {f"{fn}.{k}": v for k, v in branch_params.items()}

        # The code pin is extracted BEFORE the namespacing above would reach it:
        # it is not a branch param and must not become "bandpass.code_version".
        # ``fn=`` still disambiguates it, exactly as it does for branch params —
        # one keyword, one meaning ("these pins concern that function").
        if code_version is not None:
            key = f"{CODE_PIN_PREFIX}.{fn}" if fn else CODE_PIN_PREFIX
            branch_params = {**branch_params, key: str(code_version)}
        if run_options is not None:
            key = f"{RUN_PIN_PREFIX}.{fn}" if fn else RUN_PIN_PREFIX
            branch_params = {**branch_params, key: str(run_options)}

        if isinstance(var_type, Merge):
            raise TypeError(
                "Variant cannot wrap a Merge. branch_params are namespaced per "
                "producing function, so one branch_param cannot sensibly broadcast "
                "across Merge constituents. Pin per constituent instead: "
                "Merge(Variant(A, low_hz=20), B)."
            )
        if isinstance(var_type, EachOf):
            raise TypeError(
                "Variant cannot wrap an EachOf. EachOf must stay the outermost "
                "wrapper; nest Variant inside each alternative instead: "
                "EachOf(Variant(A, low_hz=20), Variant(A, low_hz=50))."
            )
        if not branch_params:
            raise ValueError(
                "Variant requires at least one branch_param, code_version or "
                "run_options to pin, e.g. Variant(FilteredEMG, low_hz=20), "
                'Variant(FilteredEMG, code_version="v1") or '
                'Variant(Loaded, run_options="distribute=true").'
            )

        # Nested Variant: merge the dicts; raise on conflicting key values.
        if isinstance(var_type, Variant):
            merged = dict(var_type.branch_params)
            for k, v in branch_params.items():
                if k in merged and merged[k] != v:
                    raise ValueError(
                        f"Conflicting branch_param '{k}' in nested Variant: "
                        f"{merged[k]!r} vs {v!r}."
                    )
                merged[k] = v
            branch_params = merged
            var_type = var_type.var_type

        self.var_type = var_type
        self.branch_params = branch_params

    def to_key(self) -> str:
        """Return a canonical string for use as a version key."""
        if hasattr(self.var_type, "to_key"):
            inner_key = self.var_type.to_key()
        elif isinstance(self.var_type, type):
            inner_key = self.var_type.__name__
        else:
            inner_key = repr(self.var_type)
        sorted_kv = ", ".join(
            f"{k}={v!r}" for k, v in sorted(self.branch_params.items())
        )
        return f"Variant({inner_key}, {sorted_kv})"

    @property
    def __name__(self) -> str:
        """Display name for format_inputs and error messages."""
        from .foreach import _input_type_name

        inner_name = _input_type_name(self.var_type)
        kv = ", ".join(f"{k}={v}" for k, v in sorted(self.branch_params.items()))
        return f"Variant({inner_name}, {kv})"
