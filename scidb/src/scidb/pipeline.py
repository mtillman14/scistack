"""Pipeline concepts: the ``@scistack`` marker and the ``Pipeline`` registry.

Two related pieces live here:

1. **The ``@scistack`` marker** — tags a plain Python function so scistack
   pays attention to it: :mod:`scidb.discover` collects tagged functions
   (GUI step-palette discovery), and :func:`scidb.for_each` reads the
   per-function ``generates_file`` option. The marker does **not** wrap the
   function or change its return value.

2. **The ``Pipeline`` registry** — deferred ``for_each`` registration for
   endpoint-first (pull) execution. Creating a pipeline via
   ``db.pipeline(name)`` activates it as the ambient registration target:
   subsequent ``for_each`` calls register a :class:`StepSpec` instead of
   executing (pass ``pipeline=None`` to force an eager call, or
   ``pipeline=other`` to target a non-ambient pipeline). Dependency edges
   are inferred from variable types — a step that consumes ``Filtered``
   depends on every registered step that produces ``Filtered`` — so
   :meth:`Pipeline.run_until` can walk backward from a target and run only
   its ancestors, and :meth:`Pipeline.plan` can report per-step green/red
   staleness before anything runs.

Design doc: ``docs/claude/endpoint-first-pipelines.md``;
plan: ``.claude/plan-pipeline-registry-stage1.md``.

Usage::

    @scistack
    def bandpass(signal, low_hz):
        return ...

    pipe = db.pipeline("gait_analysis")          # activates

    for_each(bandpass, {"signal": RawSignal, "low_hz": 20},
             [Filtered], subject=["1", "2"])     # registers (deferred)
    for_each(compute_speed, {"filtered": Filtered},
             [Speed], subject=["1", "2"])        # registers (deferred)

    pipe.plan()                                  # dry-run: green/red per step
    pipe.run_until(compute_speed)                # runs ancestors + target
"""

from __future__ import annotations

import atexit
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .log import Log
from .roles import endpoint_kind

# Attribute names stamped onto a tagged function.
SCISTACK_FLAG = "__scistack__"
GENERATES_FILE_ATTR = "__scistack_generates_file__"


def scistack(
    fn: Callable | None = None,
    *,
    generates_file: Any | None = None,
):
    """Mark a plain function so scistack pays attention to it.

    Works bare (``@scistack``) or called
    (``@scistack(generates_file="{subject}/out.csv")``). Returns the
    function unchanged except for marker attributes, so it stays an
    ordinary callable.
    """

    def deco(f: Callable) -> Callable:
        setattr(f, SCISTACK_FLAG, True)
        if generates_file is not None:
            setattr(f, GENERATES_FILE_ATTR, generates_file)
        return f

    return deco(fn) if callable(fn) else deco


def is_scistack_function(obj: Any) -> bool:
    """True if ``obj`` is a callable tagged by :func:`scistack`."""
    return callable(obj) and bool(getattr(obj, SCISTACK_FLAG, False))


# ---------------------------------------------------------------------------
# Deferred-step registry
# ---------------------------------------------------------------------------


def _loadable_classes(spec: Any) -> set[type]:
    """Variable classes a single input spec loads, unwrapping input markers.

    ``PathInput``, constants, and literal DataFrames contribute no producer
    edge (nothing in the pipeline produces them).
    """
    from scifor import ColumnSelection, EachOf, Fixed, Merge

    from .across_variants import AcrossVariants
    from .variant import Variant

    out: set[type] = set()
    if isinstance(spec, type):
        out.add(spec)
    elif isinstance(spec, (Variant, AcrossVariants)):
        out |= _loadable_classes(spec.var_type)
    elif isinstance(spec, (Fixed, ColumnSelection)):
        out |= _loadable_classes(spec.data)
    elif isinstance(spec, EachOf):
        for alt in spec.alternatives:
            out |= _loadable_classes(alt)
    elif isinstance(spec, Merge):
        for sub in spec.tables:
            out |= _loadable_classes(sub)
    return out


@dataclass
class StepSpec:
    """One deferred ``for_each`` call: the exact arguments, execution pending.

    ``options`` holds every ``for_each`` keyword argument other than
    ``fn``/``inputs``/``outputs``/``pipeline`` and is replayed verbatim, so
    a step run through the pipeline produces byte-identical version_keys to
    the same call run eagerly.
    """

    fn: Callable
    inputs: dict[str, Any]
    outputs: list
    metadata_iterables: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return getattr(self.fn, "__name__", repr(self.fn))

    def input_classes(self) -> set[type]:
        classes: set[type] = set()
        for spec in self.inputs.values():
            classes |= _loadable_classes(spec)
        return classes

    def output_classes(self) -> set[type]:
        return {o for o in self.outputs if isinstance(o, type)}

    def to_manifest(self) -> dict:
        """JSON-able projection for display / future GUI use (not replayable)."""
        from .foreach_config import ForEachConfig

        config = ForEachConfig(
            self.fn,
            self.inputs,
            where=self.options.get("where"),
            distribute=self.options.get("distribute", False),
            as_table=self.options.get("as_table"),
        )
        return {
            "fn": self.name,
            "module": getattr(self.fn, "__module__", None),
            "inputs": sorted(c.__name__ for c in self.input_classes()),
            "outputs": sorted(c.__name__ for c in self.output_classes()),
            "iterate": sorted(self.metadata_iterables),
            "version_keys": config.to_version_keys(),
            "call_id": config.to_call_id(),
        }


class Step:
    """Handle returned by a deferred ``for_each`` registration.

    Deliberately NOT data: any attempt to use it like a result DataFrame
    fails fast with a pointer to ``run_until``, so code written against
    eager ``for_each`` cannot silently operate on nothing.
    """

    __slots__ = ("spec", "pipeline_name")

    def __init__(self, spec: StepSpec, pipeline_name: str):
        object.__setattr__(self, "spec", spec)
        object.__setattr__(self, "pipeline_name", pipeline_name)

    def __repr__(self) -> str:
        return (
            f"<Step '{self.spec.name}' (deferred) in pipeline '{self.pipeline_name}'>"
        )

    def __getattr__(self, item):
        raise AttributeError(
            f"'{self.spec.name}' is a deferred pipeline step, not a result — "
            f"nothing has executed. Call "
            f"pipeline.run_until({self.spec.name}) (or run_all()) first."
        )

    def __iter__(self):
        raise TypeError(
            f"'{self.spec.name}' is a deferred pipeline step and not "
            f"iterable — call pipeline.run_until(...) to execute it."
        )


# ---------------------------------------------------------------------------
# Use-edge bindings (cross-project reuse without touching pipeline source)
# ---------------------------------------------------------------------------


def _rename_keys(d: dict, key_map: dict) -> dict:
    return {key_map.get(k, k): v for k, v in d.items()}


def _rewrite_template(template, key_map: dict):
    """Rename ``{key}`` placeholders in a path template (literal replace,
    matching PathOutput/PathInput's own literal resolution)."""
    s = str(template)
    for old, new in key_map.items():
        s = s.replace("{" + old + "}", "{" + new + "}")
    from pathlib import Path as _P

    return _P(s) if isinstance(template, _P) else s


def _rewrite_where(where: Any, key_map: dict, step_name: str) -> Any:
    """Rewrite schema-key names inside a structured Filter tree.

    Raw SQL (strings / RawFilter) cannot be rewritten safely → WARN and
    leave as-is (observability over silent breakage).
    """
    from .filters import (
        CompoundFilter,
        NotFilter,
        RawFilter,
        SchemaKeyCompareFilter,
        SchemaKeyInFilter,
    )

    if where is None:
        return None
    if isinstance(where, (str, RawFilter)):
        Log.warn(
            f"pipeline_binding: step '{step_name}' has a raw-SQL where= "
            f"filter — key_map cannot rewrite raw SQL; the filter is left "
            f"unchanged and may reference the wrong schema keys"
        )
        return where
    if isinstance(where, SchemaKeyCompareFilter):
        return SchemaKeyCompareFilter(
            key_map.get(where.key, where.key), where.op, where.value
        )
    if isinstance(where, SchemaKeyInFilter):
        return SchemaKeyInFilter(key_map.get(where.key, where.key), list(where.values))
    if isinstance(where, CompoundFilter):
        return CompoundFilter(
            _rewrite_where(where.left, key_map, step_name),
            _rewrite_where(where.right, key_map, step_name),
            where.op,
        )
    if isinstance(where, NotFilter):
        return NotFilter(_rewrite_where(where.inner, key_map, step_name))
    Log.warn(
        f"pipeline_binding: step '{step_name}' has a where= filter of "
        f"unrecognized type {type(where).__name__} — left unchanged"
    )
    return where


def _rewrite_input(val: Any, key_map: dict):
    """Rewrite one input spec's schema-key surface (non-mutating: wrappers
    are rebuilt, never modified in place)."""
    from scifor.pathinput import PathInput

    from scifor import Fixed, PathOutput

    if isinstance(val, PathOutput):
        return PathOutput(_rewrite_template(val.template, key_map))
    if isinstance(val, PathInput):
        return PathInput(
            _rewrite_template(val.path_template, key_map),
            root_folder=val.root_folder,
            regex=val.regex,
            aliases=_rename_keys(val.aliases, key_map),
        )
    if isinstance(val, Fixed):
        return Fixed(
            _rewrite_input(val.data, key_map),
            **_rename_keys(val.fixed_metadata, key_map),
        )
    return val


def _constant_input_names(spec: StepSpec) -> set[str]:
    """Names of this spec's scalar constant inputs (the params surface) —
    same classification as ForEachConfig._get_direct_constants."""
    from scifor.pathinput import PathInput

    from scifor import ColName, PathOutput

    from .input_spec import is_loadable

    return {
        k
        for k, v in spec.inputs.items()
        if not is_loadable(v)
        and not isinstance(v, (ColName, PathOutput, PathInput))
        and not isinstance(v, type)
    }


class PipelineBinding:
    """A non-mutating adaptation of a pipeline for one use edge.

    Created via :meth:`Pipeline.bind`. The bound pipeline's own specs are
    never modified — composition materializes rewritten COPIES — so
    different parents can bind the same pipeline differently, and its own
    ``run_all()`` still runs the unbound versions.

    - ``key_map``: native schema key → project schema key. Rewrites the
      declaration surface (iteration kwargs, Path templates, Fixed kwargs,
      structured where= filters, schema_filter/schema_keys, share_limits
      values); records save under the PROJECT's keys.
    - ``params``: constant-input overrides → a different computation
      identity by construction (constants are version keys), i.e. distinct
      variants per binding. Bare names must match exactly one function's
      constant input across the subtree; ``"fn.param"`` disambiguates.
    - ``iterate``: iteration-value overrides, keyed by POST-key_map names.

    Bindings apply transitively to the bound pipeline's own ``uses``
    subtree (the whole subtree was written in the foreign vocabulary).
    """

    def __init__(
        self,
        pipeline: Pipeline,
        key_map: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        iterate: dict[str, Any] | None = None,
    ):
        self.pipeline = pipeline
        self.key_map = dict(key_map or {})
        self.params = dict(params or {})
        self.iterate = dict(iterate or {})
        # original-spec id -> rewritten StepSpec (stable across compositions
        # so dedup-by-identity and repeated runs see the same objects).
        self._rewritten: dict[int, StepSpec] = {}
        # outer-signature -> composed binding (stable objects across closure
        # walks, so THEIR rewrite caches are stable too).
        self._compose_cache: dict[str, PipelineBinding] = {}
        if self.params:
            self._resolved_params = self._resolve_params()

    # -- identity ------------------------------------------------------------

    def signature(self) -> str:
        """Canonical signature for dedup: identical bindings of the same
        pipeline through different parents are ONE computation; different
        params are different variants and must both run."""
        import json as _json

        return _json.dumps(
            {
                "pipeline": id(self.pipeline),
                "key_map": self.key_map,
                "params": {k: repr(v) for k, v in sorted(self.params.items())},
                "iterate": {k: repr(v) for k, v in sorted(self.iterate.items())},
            },
            sort_keys=True,
        )

    def is_identity(self) -> bool:
        return not (self.key_map or self.params or self.iterate)

    # -- params resolution (bind-time validation, E2) --------------------------

    def _resolve_params(self) -> dict[tuple[str, str], Any]:
        """Resolve params targets against the subtree's constant inputs.

        Returns {(fn_name, input_name): value}. Bare names must match
        exactly one function's constant input; ``"fn.param"`` targets one
        function. Ambiguity → AmbiguousParamError; no match → ValueError.
        Both raised HERE, at bind time — not at run.
        """
        from .exceptions import AmbiguousParamError

        available: list[tuple[str, str]] = []  # (fn_name, input_name)
        for p in [self.pipeline] + self.pipeline._uses_pipelines_closure():
            for spec in p.steps:
                for cname in _constant_input_names(spec):
                    available.append((spec.name, cname))

        resolved: dict[tuple[str, str], Any] = {}
        for target, value in self.params.items():
            if "." in target:
                fn_name, input_name = target.rsplit(".", 1)
                matches = [
                    (f, c) for f, c in available if f == fn_name and c == input_name
                ]
            else:
                matches = [(f, c) for f, c in available if c == target]
            if not matches:
                raise ValueError(
                    f"bind(params=...): no constant input matching "
                    f"'{target}' in pipeline '{self.pipeline.name}' or its "
                    f"used pipelines; available: "
                    f"{sorted(f'{f}.{c}' for f, c in available)}"
                )
            fns = sorted({f for f, _ in matches})
            if len(fns) > 1:
                raise AmbiguousParamError(
                    f"bind(params=...): '{target}' matches constant inputs "
                    f"in multiple functions {fns} — disambiguate with "
                    f"'<fn>.{target}'"
                )
            for f, c in matches:
                resolved[(f, c)] = value
        return resolved

    # -- composition ------------------------------------------------------------

    def compose(self, outer: PipelineBinding) -> PipelineBinding:
        """The binding an OUTER edge implies for this inner edge's pipeline
        (outer ∘ inner): key_maps chain (inner then outer), outer params/
        iterate win on conflict, inner iterate keys pass through the outer
        key_map first (they are in the inner post-map vocabulary)."""
        cached = self._compose_cache.get(outer.signature())
        if cached is not None:
            return cached
        composed_key_map = {k: outer.key_map.get(v, v) for k, v in self.key_map.items()}
        for k, v in outer.key_map.items():
            composed_key_map.setdefault(k, v)
        composed_iterate = _rename_keys(self.iterate, outer.key_map)
        composed_iterate.update(outer.iterate)
        composed = PipelineBinding(
            self.pipeline,
            key_map=composed_key_map,
            iterate=composed_iterate,
        )
        # Params were already resolved and VALIDATED at each edge's bind
        # time against its own subtree — carry the resolved maps (outer
        # wins), never re-resolve: outer params may target steps of a
        # shallower pipeline that this deeper subtree doesn't contain.
        composed.params = {**self.params, **outer.params}
        composed._resolved_params = {
            **getattr(self, "_resolved_params", {}),
            **getattr(outer, "_resolved_params", {}),
        }
        self._compose_cache[outer.signature()] = composed
        return composed

    # -- the rewrite ------------------------------------------------------------

    def rewrite(self, spec: StepSpec) -> StepSpec:
        """Materialize the adapted copy of ``spec`` (cached per binding)."""
        if self.is_identity():
            return spec
        cached = self._rewritten.get(id(spec))
        if cached is not None:
            return cached

        km = self.key_map
        new_iterables = _rename_keys(spec.metadata_iterables, km)
        for k, v in self.iterate.items():
            if k in new_iterables:
                new_iterables[k] = v

        new_inputs: dict[str, Any] = {}
        overrides = getattr(self, "_resolved_params", {})
        constant_names = _constant_input_names(spec)
        for name, val in spec.inputs.items():
            if name in constant_names and (spec.name, name) in overrides:
                new_inputs[name] = overrides[(spec.name, name)]
            else:
                new_inputs[name] = _rewrite_input(val, km)

        new_options = dict(spec.options)
        if km:
            if new_options.get("where") is not None:
                new_options["where"] = _rewrite_where(
                    new_options["where"], km, spec.name
                )
            if new_options.get("schema_filter") is not None:
                new_options["schema_filter"] = _rename_keys(
                    new_options["schema_filter"], km
                )
            if new_options.get("schema_keys") is not None:
                new_options["schema_keys"] = [
                    km.get(k, k) for k in new_options["schema_keys"]
                ]
            if new_options.get("share_limits") is not None:
                new_options["share_limits"] = {
                    inp: [km.get(k, k) for k in keys]
                    for inp, keys in new_options["share_limits"].items()
                }

        new_spec = StepSpec(
            fn=spec.fn,
            inputs=new_inputs,
            outputs=list(spec.outputs),
            metadata_iterables=new_iterables,
            options=new_options,
        )
        new_spec.origin = spec  # Step-handle targeting follows rewrites
        self._rewritten[id(spec)] = new_spec
        Log.debug(
            f"pipeline_binding_rewrite: '{spec.name}' adapted for "
            f"'{self.pipeline.name}' binding (key_map={km or '{}'}, "
            f"param overrides="
            f"{sorted(f'{f}.{c}' for (f, c) in overrides if f == spec.name) or '[]'}, "
            f"iterate={sorted(set(self.iterate) & set(new_iterables)) or '[]'})"
        )
        return new_spec


# Module-level session state (pattern-matched to the current-db global):
# the activation stack ambient for_each registration targets, plus every
# pipeline created this session for the never-run atexit check.
_active_stack: list[Pipeline] = []
_all_pipelines: list[Pipeline] = []


def active_pipeline() -> Pipeline | None:
    """The pipeline currently receiving deferred ``for_each`` registrations."""
    return _active_stack[-1] if _active_stack else None


def _reset_pipeline_state() -> None:
    """Clear all session pipeline state (test isolation helper)."""
    _active_stack.clear()
    _all_pipelines.clear()


def _unrun_pipelines() -> list[Pipeline]:
    """Pipelines with registered steps that were never run/planned/deactivated."""
    return [p for p in _all_pipelines if p.steps and not p._acknowledged]


def _warn_unrun_pipelines() -> None:
    for p in _unrun_pipelines():
        Log.warn(
            f"pipeline_never_run: pipeline '{p.name}' registered "
            f"{len(p.steps)} step(s) but none of run_all/run_until/plan/"
            f"deactivate was called — nothing was executed"
        )


atexit.register(_warn_unrun_pipelines)


class Pipeline:
    """A named collection of deferred ``for_each`` steps with pull execution.

    Create via ``db.pipeline(name)`` (binds + activates) or directly
    (``Pipeline("name", db=db)``, activate explicitly). Registration order
    does not matter: execution order is topologically sorted from
    variable-type edges. Multiple producers of one variable type (variant
    branches) all become prerequisites of that type's consumers; variant
    disambiguation stays where it lives today — load-time branch_params.

    Pipelines COMPOSE: ``db.pipeline(name, uses=[other])`` (or
    ``pipe.use(other)``) unions the other pipeline's steps into this
    pipeline's graph, so ``run_until``/``plan`` resolve producers across
    the boundary. ``run_all`` runs own steps + their ancestors only.
    """

    def __init__(
        self,
        name: str,
        db: Any | None = None,
        uses: tuple[Pipeline, ...] | list[Pipeline] = (),
    ):
        self.name = name
        self.db = db
        self.steps: list[StepSpec] = []
        # Use edges, each a PipelineBinding (bare pipelines get identity
        # bindings, so composition has one shape).
        self.uses: list[PipelineBinding] = []
        # True once run_all/run_until/plan/deactivate acknowledged this
        # pipeline; guards the never-run atexit warning.
        self._acknowledged = False
        # Per-step outcome of the most recent _run (run_all/run_until/
        # run_endpoints): [{"step", "label", "pipeline", "completed",
        # "failed", "no_data", "total", "cancelled"}]. for_each's
        # continue-and-report policy means iteration failures never raise, so
        # callers that need an honest verdict (the GUI's run status, CI
        # wrappers) read this instead of inferring success from "the call
        # returned". "no_data" combos (no backing data for that schema-key
        # combination) are expected/benign and excluded from "failed".
        self.last_run_report: list[dict] = []
        _all_pipelines.append(self)
        for other in uses:
            self.use(other)

    def __repr__(self) -> str:
        used = f", uses {len(self.uses)} pipeline(s)" if self.uses else ""
        return f"<Pipeline '{self.name}': {len(self.steps)} step(s){used}>"

    # -- composition -----------------------------------------------------------

    def bind(
        self,
        key_map: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        iterate: dict[str, Any] | None = None,
    ) -> PipelineBinding:
        """Adapt this pipeline for a use edge WITHOUT touching its source:
        ``uses=[loading.bind(key_map={"session": "subject"},
        params={"low_hz": 30})]``. See :class:`PipelineBinding`. Params
        targets are validated here, at bind time."""
        return PipelineBinding(self, key_map=key_map, params=params, iterate=iterate)

    def use(self, other: Pipeline | PipelineBinding) -> Pipeline:
        """Declare ``other`` (a Pipeline or a Pipeline.bind(...) binding) as
        a dependency: its steps join this pipeline's graph (union — nothing
        is copied; bindings materialize adapted copies at composition), so
        ``run_until``/``plan`` resolve producers inside it. Never activates
        anything."""
        from .exceptions import PipelineCycleError

        if isinstance(other, Pipeline):
            binding = PipelineBinding(other)
        elif isinstance(other, PipelineBinding):
            binding = other
        else:
            raise TypeError(
                f"uses= entries must be Pipeline or Pipeline.bind(...) "
                f"instances; got {type(other).__name__}"
            )
        target = binding.pipeline
        if target is self or any(p is self for p in target._uses_pipelines_closure()):
            raise PipelineCycleError(
                f"pipeline '{self.name}' cannot use '{target.name}': "
                f"dependency cycle between pipelines"
            )
        if target.db is not None and self.db is not None and target.db is not self.db:
            raise ValueError(
                f"pipeline '{target.name}' is bound to a different database "
                f"than '{self.name}' — cross-database composition is not "
                f"supported. Rebind one of them (a used pipeline with db=None "
                f"inherits the user's database)."
            )
        self.uses.append(binding)
        if binding.is_identity():
            Log.info(
                f"pipeline_uses: '{self.name}' uses '{target.name}' "
                f"({len(target.steps)} step(s) joined the graph)"
            )
        else:
            Log.info(
                f"pipeline_bound: '{self.name}' uses '{target.name}' with "
                f"key_map={binding.key_map or '{}'}, "
                f"params={sorted(binding.params) or '[]'}, "
                f"iterate={sorted(binding.iterate) or '[]'} "
                f"({len(target.steps)} step(s) joined, adapted)"
            )
        return self

    def _uses_pipelines_closure(self) -> list[Pipeline]:
        """Transitively used pipelines (bindings stripped), deduped by
        identity — for cycle checks and params resolution."""
        ordered: list[Pipeline] = []
        seen: set[int] = set()

        def visit(p: Pipeline) -> None:
            for b in p.uses:
                if id(b.pipeline) not in seen:
                    seen.add(id(b.pipeline))
                    visit(b.pipeline)
                    ordered.append(b.pipeline)

        visit(self)
        return ordered

    def _uses_closure(self) -> list[tuple[Pipeline, PipelineBinding]]:
        """Transitively used pipelines with their EFFECTIVE bindings
        (outer ∘ inner composition down each path), dependencies-first.

        Deduped by (pipeline identity, binding signature): the same
        sub-pipeline reached twice with identical adaptation is one set of
        steps (diamond case); reached with different params it is two
        genuinely different computations — two variants — and both stay.
        """
        ordered: list[tuple[Pipeline, PipelineBinding]] = []
        seen: set[tuple[int, str]] = set()

        def visit(p: Pipeline, outer: PipelineBinding | None) -> None:
            for b in p.uses:
                eff = b if outer is None else b.compose(outer)
                key = (id(eff.pipeline), eff.signature())
                if key not in seen:
                    seen.add(key)
                    visit(eff.pipeline, eff)
                    ordered.append((eff.pipeline, eff))

        visit(self, None)
        return ordered

    def _composed_steps(self) -> list[tuple[Pipeline, StepSpec]]:
        """The full graph's steps as (owner pipeline, spec) pairs: used
        pipelines' steps first (closure order, adapted through their
        effective bindings), then this pipeline's own (never adapted)."""
        pairs: list[tuple[Pipeline, StepSpec]] = []
        for p, binding in self._uses_closure():
            pairs.extend((p, binding.rewrite(s)) for s in p.steps)
        pairs.extend((self, s) for s in self.steps)
        return pairs

    # -- activation ---------------------------------------------------------

    def activate(self) -> Pipeline:
        """Make this pipeline the ambient ``for_each`` registration target."""
        if active_pipeline() is not self:
            _active_stack.append(self)
        return self

    def deactivate(self) -> None:
        """Stop ambient registration without running (explicit escape)."""
        self._acknowledged = True
        while self in _active_stack:
            _active_stack.remove(self)

    def discard(self) -> None:
        """Deactivate AND drop this pipeline from the session registry.

        For transient pipelines (e.g. the GUI compiling its document per
        request): a long-lived process would otherwise accumulate every
        compiled pipeline in the never-run bookkeeping forever.
        """
        self.deactivate()
        while self in _all_pipelines:
            _all_pipelines.remove(self)

    # -- registration --------------------------------------------------------

    def register_call(
        self,
        *,
        fn: Callable,
        inputs: dict,
        outputs: list,
        metadata_iterables: dict,
        options: dict,
    ) -> Step:
        """Record one deferred ``for_each`` call. Zero side effects beyond the log."""
        spec = StepSpec(
            fn=fn,
            inputs=inputs,
            outputs=outputs,
            metadata_iterables=dict(metadata_iterables),
            options=dict(options),
        )
        self.steps.append(spec)
        Log.info(
            f"pipeline_step_registered: '{spec.name}' -> pipeline "
            f"'{self.name}' (deferred; {len(self.steps)} step(s) registered)"
        )
        return Step(spec, self.name)

    # -- graph ----------------------------------------------------------------

    @staticmethod
    def _step_label(owner: Pipeline, spec: StepSpec, via: Pipeline) -> str:
        """Display name for a step; owner-qualified when it lives in a used
        pipeline (``loading:bandpass``)."""
        return spec.name if owner is via else f"{owner.name}:{spec.name}"

    @staticmethod
    def _deps_for(pairs: list[tuple[Pipeline, StepSpec]]) -> dict[int, set[int]]:
        """Pair index -> indices of prerequisite pairs (variable-type edges,
        crossing pipeline boundaries freely)."""
        producers: dict[type, set[int]] = {}
        for i, (_, s) in enumerate(pairs):
            for cls in s.output_classes():
                producers.setdefault(cls, set()).add(i)
        deps: dict[int, set[int]] = {}
        for i, (_, s) in enumerate(pairs):
            needed: set[int] = set()
            for cls in s.input_classes():
                needed |= producers.get(cls, set())
            deps[i] = needed
        return deps

    def _topo_order(
        self,
        pairs: list[tuple[Pipeline, StepSpec]],
        subset: set[int] | None = None,
    ) -> list[int]:
        """Kahn's algorithm over ``subset`` (default: all pairs).

        Deterministic: among ready steps, composed order (used pipelines
        first, then registration order) wins. Raises
        :class:`~scidb.exceptions.PipelineCycleError` if steps remain (a
        step consuming its own output type is the one-step case).
        """
        from .exceptions import PipelineCycleError

        indices = sorted(subset) if subset is not None else list(range(len(pairs)))
        index_set = set(indices)
        all_deps = self._deps_for(pairs)
        deps = {i: all_deps[i] & index_set for i in indices}
        order: list[int] = []
        done: set[int] = set()
        while len(order) < len(indices):
            ready = [i for i in indices if i not in done and deps[i] <= done]
            if not ready:
                stuck = [
                    self._step_label(pairs[i][0], pairs[i][1], self)
                    for i in indices
                    if i not in done
                ]
                raise PipelineCycleError(
                    f"pipeline '{self.name}' has a dependency cycle among "
                    f"steps: {stuck}"
                )
            order.extend(ready)
            done.update(ready)
        return order

    def _resolve_target(
        self, pairs: list[tuple[Pipeline, StepSpec]], target: Any
    ) -> set[int]:
        """Pair indices matching ``target`` (Step handle, callable, or name)
        anywhere in the composed graph — own steps or used pipelines'.

        A function registered multiple times (variant branches) matches all
        of its registrations.
        """
        if isinstance(target, Step):
            matches = {
                i
                for i, (_, s) in enumerate(pairs)
                if s is target.spec or getattr(s, "origin", None) is target.spec
            }
        elif callable(target):
            matches = {i for i, (_, s) in enumerate(pairs) if s.fn is target}
        elif isinstance(target, str):
            matches = {i for i, (_, s) in enumerate(pairs) if s.name == target}
        else:
            raise TypeError(
                f"run_until target must be a Step, callable, or step name; "
                f"got {type(target).__name__}"
            )
        if not matches:
            registered = [self._step_label(o, s, self) for o, s in pairs]
            raise ValueError(
                f"no step matching {target!r} is registered in pipeline "
                f"'{self.name}' or its used pipelines; registered steps: "
                f"{registered}"
            )
        return matches

    def _ancestors(
        self, pairs: list[tuple[Pipeline, StepSpec]], targets: set[int]
    ) -> set[int]:
        deps = self._deps_for(pairs)
        seen: set[int] = set()
        frontier = list(targets)
        while frontier:
            i = frontier.pop()
            for dep in deps[i]:
                if dep not in seen:
                    seen.add(dep)
                    frontier.append(dep)
        return seen

    # -- dry run ----------------------------------------------------------------

    def plan(self, target: Any | None = None) -> list[dict]:
        """Topologically ordered dry-run report over the composed graph;
        nothing executes.

        Each entry: ``{"step", "pipeline", "state", "combos"}`` where
        ``pipeline`` names the step's owner and ``state`` is
        ``check_node_state``'s binary green (computed and current) / red
        (needs attention), or "unknown" when the check itself failed.
        """
        self._acknowledged = True
        pairs = self._composed_steps()
        if target is not None:
            targets = self._resolve_target(pairs, target)
            order = self._topo_order(pairs, targets | self._ancestors(pairs, targets))
        else:
            order = self._topo_order(pairs)


        entries: list[dict] = []
        for i in order:
            owner, spec = pairs[i]
            entry: dict[str, Any] = {
                "step": spec.name,
                "pipeline": owner.name,
                "endpoint": endpoint_kind(spec.name) is not None,
            }
            try:
                from .state import check_node_state

                manifest = spec.to_manifest()
                node = check_node_state(
                    spec.fn,
                    outputs=list(spec.output_classes()),
                    inputs=spec.inputs,
                    db=self._db_for(owner, spec),
                    call_id=manifest["call_id"],
                )
                entry["state"] = node.get("state", "unknown")
                entry["combos"] = node.get("combos", [])
            except Exception as exc:  # staleness check must never block planning
                Log.warn(
                    f"pipeline_plan: state check failed for step '{spec.name}': {exc}"
                )
                entry["state"] = "unknown"
                entry["combos"] = []
            entries.append(entry)
            Log.info(
                f"pipeline_plan: '{self.name}' step '{entry['step']}' state="
                f"{entry['state']} ({len(entry['combos'])} known combo(s))"
            )
        return entries

    def interface(self) -> dict:
        """The pipeline's PORTS: variable types consumed but not produced
        inside the composed graph (``inputs``) and types produced inside
        (``outputs``). Pure graph logic — the GUI renders these as a
        pipeline node's connection ports; MATLAB/CLI can use it too.
        """
        pairs = self._composed_steps()
        produced: set[type] = set()
        consumed: set[type] = set()
        for _, spec in pairs:
            produced |= spec.output_classes()
            consumed |= spec.input_classes()
        return {
            "inputs": sorted(consumed - produced, key=lambda c: c.__name__),
            "outputs": sorted(produced, key=lambda c: c.__name__),
        }

    # -- endpoint verbs -----------------------------------------------------------

    def endpoints(self, include_used: bool = True) -> list[dict]:
        """The composed graph's endpoint steps (``plot_``/``stat_`` per the
        shared ``_endpoint_policy`` prefix detection) — the top-level cards
        an endpoint-first surface renders.

        Each entry: ``{"step", "pipeline", "kind"}`` with kind
        "plot" | "stat".
        """

        pairs = self._composed_steps()
        out = []
        for owner, spec in pairs:
            if not include_used and owner is not self:
                continue
            kind = endpoint_kind(spec.name)
            if kind is not None:
                out.append({"step": spec.name, "pipeline": owner.name, "kind": kind})
        return out

    def run_endpoints(
        self,
        finalized: bool = False,
        skip_computed: bool = True,
        include_used: bool = False,
    ) -> list:
        """Run every endpoint and its ancestry — "make all my figures and
        stats". One topo pass over the union of ancestries; ``finalized``
        applies to the endpoint steps only.

        Default scope is this pipeline's OWN endpoints (consistent with
        run_all's own-steps rule); ``include_used=True`` widens to
        endpoints inside used pipelines.
        """
        pairs, order, targets = self._select("endpoints", include_used=include_used)
        if not order:
            self.deactivate()
            Log.warn(
                f"pipeline_run_skipped: run_endpoints on '{self.name}': no "
                f"endpoint (plot_/stat_) steps"
                + (
                    ""
                    if include_used
                    else " among own steps — pass include_used=True to run "
                    "endpoints inside used pipelines"
                )
            )
            return []
        return self._run(
            pairs,
            order,
            skip_computed=skip_computed,
            finalized=finalized,
            finalized_for=targets,
        )

    def show(self, target: Any, skip_computed: bool = True) -> list:
        """Draft-run one endpoint (+ ancestors) to LOOK at it — the everyday
        verb. Nothing is recorded (finalized=False); returns the endpoint's
        rendered outputs: artifact paths for ``plot_``, result payloads for
        ``stat_``. The caller opens the files.
        """

        pairs = self._composed_steps()
        targets = self._resolve_target(pairs, target)
        non_endpoints = sorted(
            {
                pairs[i][1].name
                for i in targets
                if endpoint_kind(pairs[i][1].name) is None
            }
        )
        if non_endpoints:
            raise ValueError(
                f"show() is for endpoints (plot_/stat_ functions); "
                f"{non_endpoints} are processing steps — use "
                f"run_until(...) for those"
            )
        order = self._topo_order(pairs, targets | self._ancestors(pairs, targets))
        results = self._run(
            pairs,
            order,
            skip_computed=skip_computed,
            finalized=False,
            finalized_for=targets,
        )
        rendered: list = []
        target_positions = {pos for pos, i in enumerate(order) if i in targets}
        for pos in sorted(target_positions):
            result_tbl = results[pos]
            if result_tbl is None:
                continue
            _, spec = pairs[order[pos]]
            for out_cls in spec.output_classes():
                col = out_cls.__name__
                if hasattr(result_tbl, "columns") and col in result_tbl.columns:
                    rendered.extend(result_tbl[col].tolist())
        for path in rendered:
            Log.info(f"pipeline_show: rendered -> {path}")
        return rendered

    # -- execution ----------------------------------------------------------------

    def run_all(self, skip_computed: bool = True) -> list:
        """Run this pipeline's OWN steps plus their ancestors (which may
        live in used pipelines) in dependency order.

        Deliberately NOT the full composed graph: a used pipeline may
        contain steps this pipeline never consumes, and running them here
        would be surprising. Target them with ``run_until`` or run the used
        pipeline directly.
        """
        pairs, order, _ = self._select("all")
        if not order:
            self.deactivate()
            if pairs:
                Log.warn(
                    f"pipeline_run_skipped: run_all on '{self.name}': no own "
                    f"steps registered — the {len(pairs)} step(s) from used "
                    f"pipelines were NOT run (target them with run_until, or "
                    f"run the used pipeline directly)"
                )
            return []
        return self._run(pairs, order, skip_computed=skip_computed)

    def run_until(
        self,
        target: Any,
        finalized: bool | None = None,
        skip_computed: bool = True,
    ) -> list:
        """Run ``target`` and its ancestors only — resolved over the
        composed graph, so both may live in used pipelines.

        ``finalized`` (endpoint draft/record mode) applies to the target
        step(s) only; other steps keep their registered flags.
        """
        pairs, order, targets = self._select("until", target)
        return self._run(
            pairs,
            order,
            skip_computed=skip_computed,
            finalized=finalized,
            finalized_for=targets,
        )

    def _db_for(self, owner: Pipeline, spec: StepSpec):
        """Run-time db resolution: step option → owner pipeline → this
        pipeline (the C3 inheritance rule for db=None sub-pipelines)."""
        return spec.options.get("db") or owner.db or self.db

    def _select(
        self,
        mode: str,
        target: Any | None = None,
        include_used: bool = False,
    ) -> tuple[list, list[int], set[int]]:
        """Shared step selection for run_*/execution_order: returns
        (pairs, topo order, target indices) for ``mode`` ∈
        {"all", "until", "endpoints"}."""

        pairs = self._composed_steps()
        if mode == "until":
            targets = self._resolve_target(pairs, target)
        elif mode == "endpoints":
            targets = {
                i
                for i, (owner, spec) in enumerate(pairs)
                if endpoint_kind(spec.name) is not None
                and (include_used or owner is self)
            }
        elif mode == "all":
            targets = {i for i, (o, _) in enumerate(pairs) if o is self}
        else:
            raise ValueError(f"unknown selection mode {mode!r}")
        if not targets:
            return pairs, [], set()
        order = self._topo_order(pairs, targets | self._ancestors(pairs, targets))
        return pairs, order, targets

    def execution_order(
        self,
        mode: str = "all",
        target: Any | None = None,
        include_used: bool = False,
        finalized: bool | None = None,
        skip_computed: bool = True,
    ) -> list[dict]:
        """The run plan as plain-data descriptors WITHOUT executing —
        the seam an external driver (the MATLAB bridge) runs steps
        through. Same selection + acknowledgment semantics as ``run_*``
        (this pipeline is deactivated and acknowledged), but each step is
        described rather than run.

        Descriptor keys: ``pipeline`` (owner name), ``step`` (fn name),
        ``step_index`` (position in the owner's OWN step list, for
        driver-side lookup of what could not cross a language boundary),
        ``is_matlab``, ``apply_finalized``, ``skip_computed``,
        ``metadata_iterables``/``constant_inputs``/``path_templates``
        (the POST-binding surface as plain data).
        """
        from scifor import PathOutput

        pairs, order, targets = self._select(mode, target, include_used)
        self._acknowledged = True
        self.deactivate()
        descriptors = []
        for i in order:
            owner, spec = pairs[i]
            origin = getattr(spec, "origin", spec)
            opts = dict(spec.options)
            eff_skip = (
                (opts.get("skip_computed") or skip_computed)
                if opts.get("track_lineage", True)
                else False
            )
            descriptors.append(
                {
                    "pipeline": owner.name,
                    "step": spec.name,
                    "step_index": next(
                        idx for idx, s in enumerate(owner.steps) if s is origin
                    ),
                    "is_matlab": bool(opts.get("__matlab__", False)),
                    "apply_finalized": (
                        finalized if (finalized is not None and i in targets) else None
                    ),
                    "skip_computed": eff_skip,
                    "metadata_iterables": dict(spec.metadata_iterables),
                    "constant_inputs": {
                        k: spec.inputs[k] for k in _constant_input_names(spec)
                    },
                    "path_templates": {
                        k: str(v.template)
                        for k, v in spec.inputs.items()
                        if isinstance(v, PathOutput)
                    },
                }
            )
            owner._acknowledged = True
        Log.info(
            f"pipeline_execution_order: '{self.name}' mode={mode} -> "
            f"{[d['step'] for d in descriptors]}"
        )
        return descriptors

    def _run(
        self,
        pairs: list[tuple[Pipeline, StepSpec]],
        order: list[int],
        skip_computed: bool = True,
        finalized: bool | None = None,
        finalized_for: set[int] | None = None,
    ) -> list:

        matlab_steps = sorted(
            {pairs[i][1].name for i in order if pairs[i][1].options.get("__matlab__")}
        )
        if matlab_steps:
            raise RuntimeError(
                f"pipeline '{self.name}' contains MATLAB-registered step(s) "
                f"{matlab_steps} whose function handles live in MATLAB — "
                f"run this pipeline from MATLAB (pipe.run_until(...)), which "
                f"drives execution through Pipeline.execution_order()."
            )
        self._acknowledged = True
        self.deactivate()
        names = [self._step_label(pairs[i][0], pairs[i][1], self) for i in order]
        Log.info(
            f"pipeline_run_started: '{self.name}' {len(order)} step(s) in "
            f"dependency order: {names}"
        )
        results = []
        report: list[dict] = []
        for i in order:
            apply_finalized = (
                finalized
                if (finalized is not None and finalized_for and i in finalized_for)
                else None
            )
            results.append(
                self._execute_step(
                    pairs, i, skip_computed, apply_finalized, report=report
                )
            )
        self.last_run_report = report
        failed_total = sum(e["failed"] for e in report)
        no_data_total = sum(e.get("no_data", 0) for e in report)
        Log.info(
            f"pipeline_run_finished: '{self.name}' ({len(order)} step(s), "
            f"{failed_total} failed combo(s), {no_data_total} no-data combo(s))"
        )
        return results

    def _execute_step(
        self,
        pairs: list[tuple[Pipeline, StepSpec]],
        i: int,
        skip_computed: bool = True,
        finalized: bool | None = None,
        report: list[dict] | None = None,
    ):
        """Execute ONE composed step through eager for_each. Shared by the
        ``_run`` loop and the MATLAB bridge's mixed-pipeline driver (which
        runs Python-registered steps here while MATLAB runs its own).

        When ``report`` is given, a per-step outcome entry is appended to it
        (populated from scifor's authoritative ``summary`` progress event;
        combos removed up-front by skip_computed never enter these counts).
        """
        from .foreach import for_each as _for_each

        owner, spec = pairs[i]
        opts = dict(spec.options)
        opts.pop("__matlab__", None)
        opts.pop("__matlab_fn_hash__", None)
        opts["db"] = self._db_for(owner, spec)
        if report is not None:
            entry = {
                "step": spec.name,
                "label": self._step_label(owner, spec, self),
                "pipeline": owner.name,
                "completed": 0,
                "failed": 0,
                "no_data": 0,
                "total": 0,
                "cancelled": False,
            }
            report.append(entry)
            _caller_progress_fn = opts.get("_progress_fn")

            def _collecting_progress_fn(
                info: dict, _entry=entry, _next=_caller_progress_fn
            ):
                if info.get("event") == "summary":
                    _entry["completed"] = info.get("completed", 0)
                    no_data = info.get("no_data", 0)
                    _entry["failed"] = info.get(
                        "failed", info.get("skipped", 0) - no_data
                    )
                    _entry["no_data"] = no_data
                    _entry["total"] = info.get("total", 0)
                    _entry["cancelled"] = bool(info.get("cancelled"))
                if _next is not None:
                    _next(info)

            opts["_progress_fn"] = _collecting_progress_fn
        # Pull execution defaults to memoized runs; a step's explicitly
        # registered skip_computed=True always wins, and untracked steps
        # are left alone (skip_computed requires lineage).
        if opts.get("track_lineage", True):
            opts["skip_computed"] = opts.get("skip_computed") or skip_computed
        if finalized is not None:
            opts["finalized"] = finalized
        Log.info(
            f"pipeline_step_run: "
            f"'{self._step_label(owner, spec, self)}' (via pipeline "
            f"'{self.name}', skip_computed={opts.get('skip_computed', False)})"
        )
        result = _for_each(
            spec.fn,
            spec.inputs,
            spec.outputs,
            pipeline=None,  # replay is always eager
            **opts,
            **spec.metadata_iterables,
        )
        # An executed step acknowledges its owner: a pipeline that only
        # exists as a dependency should not warn at session end.
        owner._acknowledged = True
        return result
