"""
Source -> GUI pipeline import (Direction 1 of the source-of-truth work —
see docs/claude/code-discovery-categories.md and
.claude/plan-pathinput-sweep-submodule-source-of-truth.md).

A user's Python file can define a reusable, composable pipeline in source:

    from scidb import Pipeline, for_each
    from my_pipeline_fns import bandpass_filter, compute_speed
    from my_variables import RawSignal, Filtered, Speed

    pipe = Pipeline("gait_analysis")   # deliberately no db= -- see below
    pipe.activate()
    for_each(bandpass_filter, {"signal": RawSignal, "low_hz": 20},
             [Filtered], subject=["1", "2"])
    for_each(compute_speed, {"filtered": Filtered}, [Speed],
             subject=["1", "2"])

``Pipeline`` registration is side-effect-free (``StepSpec`` registration is
"zero side effects beyond the log") — the same property that already lets
functions/variables/constants be discovered just by IMPORTING a file.
Importing this file during the normal registry-loading pass registers the
Pipeline into ``scidb.pipeline._all_pipelines`` automatically; this module
seeds that into GUI state (``_pipeline_uses`` / manual nodes+edges) once a
database connection exists — see :func:`discover_and_seed_pipelines`.

Convention: a GUI-discoverable pipeline definition must NOT pass ``db=`` to
``Pipeline()``/``db.pipeline()`` — discovery happens at import time,
potentially before any database is even open, and ``db=None`` is also how
this module tells a genuine user-authored pipeline apart from
``execution_service.build_backend_pipeline``'s own per-request COMPILED
Pipelines (those always set ``db=``, and are short-lived — created and
``.discard()``-ed within a single request, per that module's own "per-
request transients" comment).

"Create once" semantics: a discovered pipeline whose NAME already exists
locally is skipped entirely — never overwritten — matching the same
precedent already established for ``create_variable``/``create_path_input``
(no in-place source-to-GUI overwrite of a named thing; re-running discovery
after editing the source file does not resync hand-edited GUI state, by
design, since there is no reconciliation algorithm here to distinguish a
source-driven change from a deliberate hand-edit).
"""

from __future__ import annotations

import logging
import uuid

from scistack_gui.ids import PARAM_ID_PREFIX as _PARAM_PREFIX

logger = logging.getLogger(__name__)


def discover_and_seed_pipelines(db) -> dict:
    """Seed GUI state from any ``scidb.Pipeline`` objects registered (via
    source-file import) since the last time this ran.

    Every candidate (created or skipped) is discarded from scidb's own
    ``_all_pipelines`` bookkeeping afterward — this is both how
    scistack-gui avoids scidb's "pipeline registered but never run" atexit
    warning for these (discovery never runs them) AND how this function
    knows what's "new" on the next call: nothing is left behind to
    re-process, so a later call only ever sees pipelines a subsequent
    import pass just registered.

    Returns ``{"created": [name, ...], "skipped": [name, ...]}``.
    """
    from scidb.pipeline import _all_pipelines

    candidates = [p for p in _all_pipelines if p.db is None]
    if not candidates:
        return {"created": [], "skipped": []}

    logger.info(
        "[pipeline_discovery] discover_and_seed_pipelines: %d candidate(s): %s",
        len(candidates),
        [p.name for p in candidates],
    )
    seen: dict[int, str] = {}  # id(Pipeline) -> local pipeline_id
    created_names: list[str] = []
    try:
        for pipe in candidates:
            try:
                _seed_pipeline_recursive(db, pipe, seen, created_names)
            except Exception:
                # NOTE: created_names may already contain pipe.name if the
                # failure happened partway through (e.g. a step or two
                # seeded, then add_pipeline_use raised a cycle error) — the
                # partially-created pipeline_id/nodes are real DB rows, not
                # rolled back (no transaction wrapping here). A rare path;
                # not worth the complexity of a rollback for it.
                logger.exception(
                    "[pipeline_discovery] failed to seed pipeline '%s' — skipping it",
                    pipe.name,
                )
    finally:
        for pipe in candidates:
            pipe.discard()

    created = created_names
    skipped = sorted({p.name for p in candidates} - set(created_names))
    if created:
        logger.info("[pipeline_discovery] created: %s", created)
    if skipped:
        logger.info(
            "[pipeline_discovery] skipped (local pipeline already exists, or "
            "seeding failed): %s",
            skipped,
        )
    return {"created": created, "skipped": skipped}


def _seed_pipeline_recursive(
    db, pipe, seen: dict[int, str], created_names: list[str]
) -> str:
    """Seed one Pipeline (and, recursively, everything in ``pipe.uses``)
    into GUI state. Returns the LOCAL ``pipeline_id`` — the existing one if
    a pipeline named ``pipe.name`` already exists locally ("create once",
    see module docstring), otherwise a freshly created one. Appends to
    ``created_names`` only when actually created here (memoized via
    ``seen`` so a diamond-shaped ``uses`` graph — or the same Pipeline
    object appearing both as a top-level candidate and as someone else's
    ``uses`` entry — is only ever processed once).
    """
    from scistack_gui import pipeline_store as ps

    if id(pipe) in seen:
        return seen[id(pipe)]

    existing = {p["name"]: p["pipeline_id"] for p in ps.list_all_pipelines(db)}
    if pipe.name in existing:
        pipeline_id = existing[pipe.name]
        seen[id(pipe)] = pipeline_id
        logger.debug(
            "[pipeline_discovery] '%s' already exists locally (%s) — not touched",
            pipe.name,
            pipeline_id,
        )
        return pipeline_id

    pipeline_id = ps.create_pipeline(db, pipe.name)
    seen[id(pipe)] = pipeline_id
    created_names.append(pipe.name)
    logger.info(
        "[pipeline_discovery] created pipeline '%s' -> %s (%d step(s), %d use(s))",
        pipe.name,
        pipeline_id,
        len(pipe.steps),
        len(pipe.uses),
    )

    # Fresh per PIPELINE (not per discovery pass): a variable/constant
    # referenced by multiple steps of THIS pipeline shares one manual node
    # (looked up by "kind:name" key); a variable/constant referenced by a
    # DIFFERENT discovered pipeline in the same pass gets its OWN
    # independent node with its own arbitrary id — see _get_or_create_node's
    # docstring for why that's what avoids the cross-pipeline collision.
    node_cache: dict[str, str] = {}
    for spec in pipe.steps:
        _seed_step(db, pipeline_id, spec, node_cache)

    for binding in pipe.uses:
        child_id = _seed_pipeline_recursive(db, binding.pipeline, seen, created_names)
        # Raw values (not PipelineBinding.signature()'s repr() form, which
        # is a dedup/cache key, not a storage format) -- the GUI's own
        # bind-editing UI writes/reads params as native JSON types, and
        # this needs to round-trip the same way if the discovered binding
        # is later re-compiled (build_backend_pipeline) or hand-edited.
        binding_dict = {
            "key_map": dict(binding.key_map),
            "params": dict(binding.params),
            "iterate": dict(binding.iterate),
        }
        binding_dict = {k: v for k, v in binding_dict.items() if v}
        try:
            ps.add_pipeline_use(db, pipeline_id, child_id, binding_dict)
        except ValueError as e:
            logger.warning(
                "[pipeline_discovery] could not place '%s' as a submodule of "
                "'%s': %s",
                binding.pipeline.name,
                pipe.name,
                e,
            )

    return pipeline_id


def _registered_name(value, registry_dict: dict) -> "str | None":
    """Which top-level name (if any) in *registry_dict* is bound to the
    exact object *value* — identity comparison, not content-matching: at
    discovery time we hold the LIVE object straight from ``spec.inputs``,
    the same object the registry scan found (same process, same import
    cache), so there's no serialization round-trip to recover from (unlike
    ``graph_builder.resolve_path_input_name``, which reconstructs identity
    from a DB-history string)."""
    for name, obj in registry_dict.items():
        if obj is value:
            return name
    return None


def _get_or_create_node(
    db, pipeline_id: str, node_cache: dict[str, str], node_type: str, label: str
) -> str:
    """Look up (or create) the manual node representing *label* in this
    pipeline's ``node_cache``, returning its id.

    Deliberately an ARBITRARY id (``discovered_{kind}_{uuid}``), never the
    bare canonical form (``var__{label}``/``param__{label}``) — matching
    the same convention function nodes already use here, and how a human
    manually placing a node from the GUI palette works: ``_pipeline_nodes``
    scopes a node to exactly ONE ``pipeline_id`` (last-write-wins on that
    column — see ``_upsert_node``), so writing the bare canonical id
    directly would make it a single, GLOBAL row that two different
    discovered pipelines referencing the same variable/constant would
    fight over — whichever was seeded last silently "steals" it from the
    other's scope. An arbitrary id sidesteps that: ``merge_manual_nodes``
    already matches manual nodes to their real DB-derived counterpart by
    ``(type, label)`` alone, never by id, so this behaves identically once
    the pipeline actually runs and ``var__{label}``/``param__{label}``
    exists for real (graduation) — see
    ``.claude/plan-placement-qualified-node-ids.md``.

    Cached per (pipeline_id-scoped) ``node_cache`` so multiple steps of the
    SAME pipeline referencing the same variable/constant share one node
    (matches what hand-wiring would look like) — a fresh ``node_cache`` per
    pipeline is what keeps two DIFFERENT discovered pipelines from sharing
    one, since each gets its own cache and therefore its own arbitrary id.
    """
    from scistack_gui import pipeline_store as ps

    key = f"{node_type}:{label}"
    if key in node_cache:
        return node_cache[key]
    kind = "var" if node_type == "variableNode" else "const"
    node_id = f"discovered_{kind}_{uuid.uuid4().hex[:12]}"
    ps.write_manual_node(db, node_id, node_type, label, pipeline_id)
    node_cache[key] = node_id
    return node_id


def _seed_step(db, pipeline_id: str, spec, node_cache: dict[str, str]) -> None:
    """One ``StepSpec`` -> one manual functionNode, plus a manual
    variableNode/parameterNode (with edge) for every referenced variable
    class and constant. Manual nodes are required here, not just edges —
    ``build_variable_nodes``/``build_constant_nodes`` only render from DB
    run history, so a genuinely never-run type/constant would otherwise be
    a dangling edge pointing at a node that doesn't exist yet
    (``merge_manual_nodes`` is what makes a manually-placed node show up
    regardless of history — the same path an already-existing "place an
    unwired node by hand" GUI action already goes through).
    PathInput/Sweep nodes are the one exception — always registry-derived
    (see ``graph_builder.seed_undiscovered_path_inputs``), never manual.
    Positions are left at (0, 0) — the frontend dagre-lays-out new nodes on
    first render (see ``api/pipeline.py``'s module docstring)."""
    from scidb import BaseVariable, EachOf, Parameter, PathInput
    from scistack_gui import pipeline_store as ps
    from scistack_gui import registry

    fn_node_id = f"discovered_fn_{uuid.uuid4().hex[:12]}"
    ps.write_manual_node(db, fn_node_id, "functionNode", spec.name, pipeline_id)

    for param, value in spec.inputs.items():
        source_id = None
        if isinstance(value, type) and issubclass(value, BaseVariable):
            source_id = _get_or_create_node(
                db, pipeline_id, node_cache, "variableNode", value.__name__
            )
        elif isinstance(value, Parameter):
            # Parameter/pathInput__ nodes are ALWAYS registry-derived (see
            # graph_builder.seed_undiscovered_path_inputs) -- unlike
            # variables, they never need a manual node here, only the edge.
            # An unregistered (inline, unnamed) Parameter/PathInput falls
            # back to the param name but has no
            # discoverable node either way -- same limitation the DB-
            # history reconstruction path already has for an unnamed
            # PathInput (see graph_builder.resolve_path_input_name's
            # __unresolved__ fallback) -- the edge is still written for
            # when/if the object later gets a real top-level name.
            name = _registered_name(value, registry.get_parameters_registry()) or param
            source_id = f"{_PARAM_PREFIX}{name}"
        elif isinstance(value, PathInput) or (
            isinstance(value, EachOf)
            and all(isinstance(a, PathInput) for a in value.alternatives)
        ):
            name = _registered_name(value, registry.get_path_inputs_registry()) or param
            source_id = f"pathInput__{name}"
        elif isinstance(value, EachOf):
            # Multi-type-variable branch, or some other EachOf shape --
            # only the variable-class case is common enough to wire here;
            # anything else is skipped rather than guessed at.
            if all(isinstance(a, type) and issubclass(a, BaseVariable) for a in value.alternatives):
                # No single node represents "one of several types" -- wire
                # to the FIRST alternative, same simplification already
                # used elsewhere for multi-type display; the other
                # alternatives are still real, just not pre-wired.
                first = value.alternatives[0]
                source_id = _get_or_create_node(
                    db, pipeline_id, node_cache, "variableNode", first.__name__
                )
        else:
            # A plain scalar constant. write_constant registers the name
            # in the parameter palette (idempotent), add_pending_constant
            # stages this discovered value, and the manual parameterNode
            # makes it a real node even with zero DB history -- the same
            # three things a user adding a constant + pending value by
            # hand would trigger. build_parameter_nodes now also renders a
            # source_values row for any name in registry.get_parameters_registry(),
            # but a plain scalar StepSpec constant isn't necessarily a
            # registered scidb.constant() bound to a top-level name, so this
            # manual-node path still covers the case source_values can't.
            from scistack_gui import layout as layout_store

            layout_store.write_constant(param)
            ps.add_pending_constant(db, param, str(value))
            source_id = _get_or_create_node(
                db, pipeline_id, node_cache, "parameterNode", param
            )

        if source_id is None:
            logger.warning(
                "[pipeline_discovery] '%s': input '%s' has an unrepresentable "
                "type (%s) -- left unwired",
                spec.name,
                param,
                type(value).__name__,
            )
            continue

        ps.write_manual_edge(
            db,
            {
                "id": f"discovered_e_{uuid.uuid4().hex[:12]}",
                "source": source_id,
                "target": fn_node_id,
                "targetHandle": f"in__{param}",
            },
        )

    for out_cls in spec.output_classes():
        target_id = _get_or_create_node(
            db, pipeline_id, node_cache, "variableNode", out_cls.__name__
        )
        ps.write_manual_edge(
            db,
            {
                "id": f"discovered_e_{uuid.uuid4().hex[:12]}",
                "source": fn_node_id,
                "target": target_id,
                "sourceHandle": f"out__{out_cls.__name__}",
            },
        )
