"""
Pipeline import/export between SciStack users (to-do #7,
plan-pipeline-import-export.md).

Exports one pipeline's DOCUMENT — recursively including every pipeline it
uses, so the result is self-contained — as a single portable JSON file. The
canvas inside it is a ``canvas_snapshot`` capture: the SAME capture/apply
that duplicate and paste use (one owner, 2026-10-08), so an export carries
exactly what a duplicate would: nodes (history-derived ones as manual
nodes), every intent statement as resolved in the source scope, positions,
edges, uses and their bindings, hidden ports. Sidebar notes about the
exported items travel too. Never exports data/records/run history: a
node's ``label`` is just a name the importing user's OWN registry resolves
locally, same as any manual node already works today. Constants/
PathInputs/Sweeps referenced by name are REUSED locally when that name
already exists there (user-confirmed decision, 2026-08-13) — the same
"shared by name" convention ``scope_service.duplicate_pipeline`` already
established for PathInput. PathInput/Sweep are source-scanned as of
docs/claude/code-discovery-categories.md: a local miss now MATERIALIZES
the bundled value into the importer's own source (via
``path_input_service``) rather than just leaving a dangling reference —
see ``import_pipeline_document``'s ``materialization_errors``.

Deliberately NOT applied on import: hidden edges (captured, because the
snapshot is shared with duplicate, but import passes ``include_hides=False``).
Hidden nodes are absent by construction: the capture reads the visible
graph. Hides apply exclusively
to DB-DERIVED ("graduated") wiring — content that only exists because the
exporting user already ran it locally. A freshly-imported pipeline has no
execution history in the target database, so nothing auto-derives there
yet regardless of whether a hide record is carried over — the exporting
user's past "I cut this auto-derived edge" choice isn't portable data,
it's tied to a run history that doesn't exist on the other end. Hidden
PORTS (``_pipeline_hidden_ports``) DO export: that's a pure wiring-shape
override, present the moment nodes/edges are placed, independent of any
execution history.

Import is ``canvas_snapshot.apply`` (fresh ids), spanning a DATABASE
boundary, plus what only a cross-project copy needs (globals, hypothesis
tag, notes).

**Identity-based reuse (2026-08-14, user-reported):** every pipeline in
the closure — root AND submodules, at any nesting depth — carries a
stable portable identity: its ``pipeline_id``, minted once at creation
and preserved verbatim through export/import (never regenerated), rather
than treated as a fresh-per-import id. On import, each pipeline resolves
against the LOCAL pipeline (if any) already holding that same id:
  - same id, IDENTICAL content (own nodes/edges/hidden-ports, AND
    recursively every submodule it uses) -> REUSED in place, unhidden if
    it was hidden. No new pipeline is created.
  - same id, DIFFERENT content (locally edited/diverged since export) ->
    forked: a fresh id is minted and the name is suffixed
    ("... (imported)"/"(imported 2)"/...) if it collides with any local
    name (hidden included). The existing local pipeline is untouched.
  - id not seen locally at all -> created fresh, PRESERVING the imported
    id; name is only suffixed if it collides with some other local
    pipeline's name.
Pipeline NAMES are consequently not required to be globally unique —
two different users' independently-created, same-named pipelines simply
coexist locally under distinct ids, deduplicated by suffix on display
name only. See ``_resolve_pipeline`` and ``canvas_snapshot.signature``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

#: 2 (2026-10-08): the canvas is a ``canvas_snapshot.CanvasSnapshot`` (every
#: intent statement, settings of run nodes, notes). Version 1 is refused (beta).
FORMAT_VERSION = 2

# Node types whose label names a global (name-shared, not scope-scoped)
# definition that needs bundling alongside the wiring that references it.
#
# Constants and Sweeps are one node type (Parameters, D6), so one referenced
# name can bundle as either — whichever the exporter's source declares. The
# import side materialises whichever it finds.
_GLOBAL_NODE_TYPES = ("parameterNode", "pathInputNode")


def _closure_pipeline_ids(db, root_pipeline_id: str) -> list[str]:
    """``root_pipeline_id`` + every pipeline it uses, recursively (BFS) —
    so the export is self-contained regardless of nesting depth."""
    from scistack_gui import pipeline_store as ps

    seen = [root_pipeline_id]
    seen_set = {root_pipeline_id}
    i = 0
    while i < len(seen):
        pid = seen[i]
        i += 1
        for use in ps.get_pipeline_uses(db, pid):
            child = use["child_pipeline_id"]
            if child not in seen_set:
                seen_set.add(child)
                seen.append(child)
    return seen


def _notes_for(names: set[str], pipeline_ids: list[str]) -> dict[str, str]:
    """The sidebar notes (``layout.read_notes``, keyed ``kind:name``) about
    any of *names* or about a submodule in *pipeline_ids*."""
    from scistack_gui import layout as layout_store

    wanted = set(names) | set(pipeline_ids)
    return {
        key: text
        for key, text in layout_store.read_notes().items()
        if key.partition(":")[2] in wanted
    }


def export_pipeline(db, pipeline_id: str) -> dict:
    """Build the portable document for ``pipeline_id`` + every pipeline it
    (transitively) uses. See module docstring for what is/isn't included."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.services import canvas_snapshot

    pipeline_ids = _closure_pipeline_ids(db, pipeline_id)
    names_by_id = {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(db)}

    # The canvas as data: the same capture duplicate/paste use, so an export
    # carries exactly what a duplicate would (settings of nodes that have
    # run, every intent statement, hidden ports). Hidden EDGES are captured
    # but not applied on import: they hide history-derived connections the
    # target has no history for.
    snap = canvas_snapshot.capture(db, pipeline_ids)

    referenced_names: dict[str, set[str]] = {t: set() for t in _GLOBAL_NODE_TYPES}
    for n in snap.nodes:
        if n.node_type in referenced_names:
            referenced_names[n.node_type].add(n.label)

    # PathInput/Sweep are source-scanned now (see
    # docs/claude/code-discovery-categories.md) — bundling their resolved
    # value here is no longer "the only copy" but an IMPORT-TIME FALLBACK:
    # if the importing user's project doesn't locally define a same-named
    # PathInput/Sweep, import_pipeline_document materializes one from this
    # value via create_path_input/create_parameter.
    from scistack_gui import registry
    from scistack_gui.domain.graph_builder import path_input_display

    path_input_registry = registry.get_path_inputs_registry()
    path_inputs = [
        {"name": name, **path_input_display(path_input_registry[name])}
        for name in sorted(referenced_names["pathInputNode"])
        if name in path_input_registry
    ]

    # Constants and Sweeps are ONE node type on the canvas (Parameters, D6),
    # so one referenced-name set feeds both bundles — PARTITIONED, never
    # duplicated: a Parameter the registry knows as a Sweep bundles its value
    # list; everything else bundles as a constant.
    sweep_registry = registry.get_parameters_registry()
    referenced_params = sorted(referenced_names["parameterNode"])
    sweeps = [
        {"name": name, "values": list(sweep_registry[name].alternatives)}
        for name in referenced_params
        if name in sweep_registry
    ]
    all_pending = ps.get_pending_constants(db)
    constants = {
        name: sorted(all_pending.get(name, set()))
        for name in referenced_params
        if name not in sweep_registry
    }

    hypothesis = None
    hyp_by_id = {h["pipeline_id"]: h for h in ps.list_hypotheses(db)}
    if pipeline_id in hyp_by_id:
        h = hyp_by_id[pipeline_id]
        hypothesis = {
            "research_question": h["research_question"],
            "hypothesis_statement": h["hypothesis_statement"],
            "evidence_for": h["evidence_for"],
            "evidence_against": h["evidence_against"],
        }

    labels = {n.label for n in snap.nodes}
    notes = _notes_for(labels, pipeline_ids)
    # Project-global GUI rows the exported nodes depend on: a manually
    # declared built-in function (no source file to rediscover it from) and a
    # Parameter's generated-set grouping (display only).
    builtin_functions = [b for b in ps.get_builtin_functions(db) if b["name"] in labels]
    value_groups = {
        name: group
        for name, group in ps.get_parameter_value_groups(db).items()
        if name in referenced_names["parameterNode"]
    }

    document = {
        "format_version": FORMAT_VERSION,
        "root_pipeline_id": pipeline_id,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "pipelines": [
            {"pipeline_id": pid, "name": names_by_id.get(pid, pid), "is_root": pid == pipeline_id}
            for pid in pipeline_ids
        ],
        "hypothesis": hypothesis,
        "canvas": snap.to_dict(),
        "constants": constants,
        "path_inputs": path_inputs,
        "sweeps": sweeps,
        "notes": notes,
        "builtin_functions": builtin_functions,
        "parameter_value_groups": value_groups,
    }
    logger.info(
        "[portability] export_pipeline(%s): %d pipeline(s); canvas %s; "
        "%d constant(s), %d path_input(s), %d sweep(s), %d note(s)",
        pipeline_id, len(pipeline_ids), snap.describe(),
        len(constants), len(path_inputs), len(sweeps), len(notes),
    )
    return document


EXPORT_DIRNAME = "exports"


def export_pipeline_to_file(db, pipeline_id: str) -> dict:
    """``export_pipeline`` + write the document to
    ``{project_dir}/exports/`` (mirrors ``endpoint_service.write_report``'s
    "write into the project directory, return the path" pattern) — also
    returns the document itself so the frontend can offer a browser
    download without a second round trip."""
    import json
    import re
    from pathlib import Path

    document = export_pipeline(db, pipeline_id)
    root = next(p for p in document["pipelines"] if p["is_root"])
    safe_name = re.sub(r"[^\w.-]+", "_", root["name"]).strip("_") or "pipeline"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    out_dir = Path(str(db.dataset_db_path)).resolve().parent / EXPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{safe_name}_{timestamp}.json"
    out_path.write_text(json.dumps(document, indent=2, default=str))

    logger.info("[portability] export written: %s", out_path)
    return {"path": str(out_path), "document": document}


def _unique_name(existing: set[str], desired: str) -> str:
    """First name not already in ``existing`` — ``desired``, or
    ``"{desired} (imported)"``, ``"{desired} (imported 2)"``, ... —
    ``create_pipeline`` requires globally-unique names."""
    if desired not in existing:
        return desired
    candidate = f"{desired} (imported)"
    n = 2
    while candidate in existing:
        candidate = f"{desired} (imported {n})"
        n += 1
    return candidate


def _resolve_pipeline(
    db, document: dict, snap, old_pid: str,
    resolution: dict, reused: set, existing_names: set,
) -> str:
    """Post-order resolve ``old_pid`` -> a local pipeline_id, recursing
    into children FIRST (equality can only be judged once every child is
    already resolved — see module docstring's "Identity-based reuse").
    ``old_pid`` IS the portable identity (preserved verbatim through
    export), so resolution is a lookup by id, not by name:
      - a local pipeline already holds this id and its content
        (recursively) matches -> REUSED (unhidden if needed).
      - a local pipeline already holds this id but content diverged ->
        forked: fresh id, name suffixed on collision.
      - no local pipeline holds this id -> created fresh, PRESERVING the
        id; name suffixed only if it collides with some other pipeline.
    Content is compared by ``canvas_snapshot.signature`` on BOTH sides (the
    document's snapshot, and a fresh capture of the local pipeline), so the
    two sides cannot be fingerprinted by different rules. Memoized in
    ``resolution``; reused old_pids are recorded into ``reused`` so the
    caller leaves their content alone (it already exists, verbatim).
    """
    if old_pid in resolution:
        return resolution[old_pid]

    from scistack_gui import pipeline_store as ps
    from scistack_gui.services import canvas_snapshot

    name = next(p["name"] for p in document["pipelines"] if p["pipeline_id"] == old_pid)

    child_resolution: dict[str, str] = {}
    for u in snap.uses:
        if u.parent_pipeline_id == old_pid:
            child_resolution[u.child_pipeline_id] = _resolve_pipeline(
                db, document, snap, u.child_pipeline_id, resolution, reused, existing_names
            )

    local = ps.get_pipeline(db, old_pid)
    if local is not None:
        try:
            doc_sig = canvas_snapshot.signature(snap, old_pid, child_resolution)
            local_snap = canvas_snapshot.capture(db, [old_pid])
            match = doc_sig == canvas_snapshot.signature(local_snap, old_pid, {})
        except Exception:
            # Equality-checking must never block an import — fail safe to
            # "no match" (forks into a fresh, renamed copy, same as any
            # other content mismatch).
            logger.warning(
                "[portability] import: content comparison for '%s' (%s) failed; "
                "treating as diverged", name, old_pid, exc_info=True,
            )
            match = False
        if match:
            if local["hidden"]:
                ps.unhide_pipeline(db, old_pid)
                logger.info(
                    "[portability] import: unhiding local pipeline '%s' (%s) — "
                    "identical content", name, old_pid,
                )
            else:
                logger.info(
                    "[portability] import: reusing local pipeline '%s' (%s) — "
                    "identical content", name, old_pid,
                )
            resolution[old_pid] = old_pid
            reused.add(old_pid)
            return old_pid
        logger.info(
            "[portability] import: local pipeline '%s' (%s) has diverged content — "
            "forking a new copy", name, old_pid,
        )

    new_pid = old_pid if local is None else f"pipe_{uuid.uuid4().hex[:12]}"
    new_name = _unique_name(existing_names, name)
    existing_names.add(new_name)
    created_pid = ps.create_pipeline(db, new_name, pipeline_id=new_pid)
    resolution[old_pid] = created_pid
    return created_pid


def _unresolved_labels(snap) -> list[str]:
    """function/variable labels the import placed a node for that aren't
    in the LOCAL registry yet — informational only (see module docstring:
    nothing blocks the import on this)."""
    from scidb import BaseVariable
    from scistack_gui import matlab_registry, registry

    unresolved: set[str] = set()
    for n in snap.nodes:
        if n.node_type == "functionNode":
            # lookup_function covers library references (pandas.read_csv),
            # which resolve by import rather than living in the registry.
            if registry.lookup_function(n.label) is None and not matlab_registry.is_matlab_function(
                n.label
            ):
                unresolved.add(n.label)
        elif n.node_type == "variableNode":
            if n.label not in BaseVariable._all_subclasses:
                unresolved.add(n.label)
    return sorted(unresolved)


def import_pipeline_document(db, document: dict) -> dict:
    """Recreate an exported document in ``db``. Pipeline ids are preserved
    as the portable identity used for reuse/fork decisions — see module
    docstring's "Identity-based reuse"; node/edge/use ids are always fresh
    (``canvas_snapshot.apply``). Returns ``{"ok", "pipeline_id" (the
    resolved root), "reused": {...}, "unresolved_labels": [...],
    "materialization_errors": [...]}``."""
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.services import canvas_snapshot

    version = document.get("format_version")
    if version != FORMAT_VERSION:
        raise ValueError(
            f"unsupported export format_version: {version!r} (this SciStack "
            f"reads version {FORMAT_VERSION}; re-export from an up-to-date SciStack)"
        )

    snap = canvas_snapshot.CanvasSnapshot.from_dict(document.get("canvas") or {})
    root_old_id = document["root_pipeline_id"]

    # Captured BEFORE any node/position writes below. read_all_constant_
    # names still scans saved positions for canonical param__-prefixed ids
    # as a fallback discovery mechanism, so capturing it AFTER creating
    # nodes would make every freshly-imported constant name look like it
    # "already existed locally".
    from scistack_gui import registry

    local_constant_names = set(layout_store.read_all_constant_names())
    local_path_input_names = set(registry.get_path_inputs_registry())
    local_sweep_names = set(registry.get_parameters_registry())

    # ALL pipelines, hidden included — matches create_pipeline's own
    # uniqueness check, so a name suffix decided here never turns out to
    # collide with a hidden pipeline down the line.
    existing_names = {p["name"] for p in ps.list_all_pipelines(db)}
    resolution: dict[str, str] = {}
    reused_pipelines: set[str] = set()
    new_root_pid = _resolve_pipeline(
        db, document, snap, root_old_id, resolution, reused_pipelines, existing_names
    )
    reused_pipeline_names = sorted(
        next(p["name"] for p in document["pipelines"] if p["pipeline_id"] == old_pid)
        for old_pid in reused_pipelines
    )
    if document.get("hypothesis"):
        h = document["hypothesis"]
        ps.tag_as_hypothesis(db, new_root_pid)
        ps.update_hypothesis(
            db, new_root_pid,
            research_question=h.get("research_question", ""),
            hypothesis_statement=h.get("hypothesis_statement", ""),
            evidence_for=h.get("evidence_for", []),
            evidence_against=h.get("evidence_against", []),
        )

    # A reused pipeline's own content already exists verbatim as part of the
    # local match; only created/forked pipelines receive the snapshot. A use
    # whose child was reused keeps pointing at it (apply falls back to the
    # captured child id, which IS the reused id).
    pipeline_map = {
        old: new for old, new in resolution.items() if old not in reused_pipelines
    }
    old_to_new = canvas_snapshot.apply(db, snap, pipeline_map, include_hides=False)

    reused_constants = []
    for name, values in document.get("constants", {}).items():
        if name in local_constant_names:
            reused_constants.append(name)
            continue
        layout_store.write_constant(name)
        for v in values:
            ps.add_pending_constant(db, name, v)

    # PathInput/Sweep are source-scanned: a name already defined locally is
    # reused UNTOUCHED; only on a local miss is the bundled value
    # MATERIALIZED into the importer's own configured source file, and a
    # failure is surfaced, not silently dropped.
    from scistack_gui.services.parameter_service import create_parameter
    from scistack_gui.services.path_input_service import create_path_input

    materialization_errors: list[dict] = []

    reused_path_inputs = []
    for pi in document.get("path_inputs", []):
        if pi["name"] in local_path_input_names:
            reused_path_inputs.append(pi["name"])
            continue
        result = create_path_input(
            pi["name"],
            pi.get("template", ""),
            pi.get("root_folder"),
            pi.get("alternate_templates"),
        )
        if not result.get("ok"):
            materialization_errors.append(
                {"kind": "path_input", "name": pi["name"], "error": result.get("error")}
            )

    reused_sweeps = []
    for sw in document.get("sweeps", []):
        if sw["name"] in local_sweep_names:
            reused_sweeps.append(sw["name"])
            continue
        result = create_parameter(sw["name"], sw.get("values", []))
        if not result.get("ok"):
            materialization_errors.append(
                {"kind": "parameter", "name": sw["name"], "error": result.get("error")}
            )

    # Notes: added where the target has none for that item; a local note is
    # never overwritten. A submodule's note follows its resolved pipeline id.
    local_notes = layout_store.read_notes()
    n_notes = 0
    for key, text in (document.get("notes") or {}).items():
        kind, _, name = key.partition(":")
        if kind == "submodule" and name in resolution:
            key = f"{kind}:{resolution[name]}"
        if key in local_notes:
            continue
        layout_store.write_note(key, text)
        n_notes += 1

    # Built-in function references and value groups: added where absent,
    # never overwriting a local row.
    local_builtins = {b["name"] for b in ps.get_builtin_functions(db)}
    for b in document.get("builtin_functions") or []:
        if b["name"] not in local_builtins:
            ps.write_builtin_function(db, b["name"], b["language"])
    local_groups = ps.get_parameter_value_groups(db)
    for name, group in (document.get("parameter_value_groups") or {}).items():
        if name not in local_groups:
            ps.set_parameter_value_group(
                db, name, kind=group["kind"], spec=group["spec"], values=group["values"]
            )

    unresolved = _unresolved_labels(snap)

    logger.info(
        "[portability] import_pipeline_document: %d pipeline(s) (%d reused), "
        "%d id(s) written -> root=%s (reused %d constant(s), %d path_input(s), "
        "%d sweep(s); %d note(s) added; %d unresolved label(s), %d "
        "materialization error(s))",
        len(resolution), len(reused_pipelines), len(old_to_new), new_root_pid,
        len(reused_constants), len(reused_path_inputs), len(reused_sweeps),
        n_notes, len(unresolved), len(materialization_errors),
    )
    return {
        "ok": True,
        "pipeline_id": new_root_pid,
        "reused": {
            "pipelines": reused_pipeline_names,
            "constants": reused_constants,
            "path_inputs": reused_path_inputs,
            "sweeps": reused_sweeps,
        },
        "unresolved_labels": unresolved,
        "materialization_errors": materialization_errors,
    }
