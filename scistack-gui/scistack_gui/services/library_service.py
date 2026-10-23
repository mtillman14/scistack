"""
Library pipelines on the canvas: seed, lock, re-sync (portability Stage 10,
D-2026-10-08-10; ``.claude/plan-portability.md``).

A library the project lists (``scidb.names.library_packages``) may ship
pipeline documents (``scidb.library``: a canvas plus the pipelines it
spans). Each is seeded into the canvas tables as LIBRARY-OWNED pipelines:

* ids are DERIVED (``ids.library_pipeline_id`` / ``library_node_id`` /
  ``library_use_id``), so a re-sync writes the same ids and every placement,
  position and node state stays attached;
* ownership is ``pipeline_store._library_pipelines``; the lock that refuses
  edits is ``library_lock``;
* they are not hypotheses, so never tabs: the Libraries list places them.

**Re-sync.** At every discovery pass the document's ``definition_hash`` is
compared with the recorded one. Unchanged: nothing is written. Changed (the
library was upgraded): each of its pipelines' own canvases is cleared
(``pipeline_store.clear_pipeline_content``) and re-applied. Placements OF
the pipeline on the project's canvases are not touched -- they belong to the
project, bindings included.

A pipeline the library no longer ships, or a library no longer listed, is
REPORTED and left as it is (never deleted; the user can hide it).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def sync_libraries(db) -> dict:
    """Seed or re-sync every listed library's pipelines. Returns
    ``{"seeded": [...], "resynced": [...], "unchanged": [...],
    "orphaned": [...], "errors": [...]}`` (entries ``"lib/pipeline"``)."""
    from scidb.library import read_library
    from scidb.names import library_packages

    from scistack_gui import pipeline_store as ps

    report: dict[str, list] = {
        "seeded": [], "resynced": [], "unchanged": [], "orphaned": [], "errors": [],
    }
    shipped: set[tuple[str, str]] = set()
    libraries = sorted(library_packages())
    logger.info("[library_service] sync: listed libraries %s", libraries or "none")
    for lib in libraries:
        info = read_library(lib)
        report["errors"] += [f"{lib}: {e}" for e in info.errors]
        for lp in info.pipelines:
            shipped.add((lib, lp.name))
            try:
                outcome = _sync_one(db, lp)
            except Exception as e:  # one broken document never stops the rest
                logger.exception("[library_service] %s/%s: sync failed", lib, lp.name)
                report["errors"].append(f"{lib}/{lp.name}: {e}")
                continue
            report[outcome].append(f"{lib}/{lp.name}")

    recorded = {(r["library"], r["pipeline_name"]) for r in ps.list_library_pipelines(db)}
    report["orphaned"] = [f"{lib}/{name}" for lib, name in sorted(recorded - shipped)]

    logger.info(
        "[library_service] sync: seeded %s, resynced %s, unchanged %d, orphaned %s, "
        "%d error(s)",
        report["seeded"],
        report["resynced"],
        len(report["unchanged"]),
        report["orphaned"],
        len(report["errors"]),
    )
    for e in report["errors"]:
        logger.warning("[library_service] %s", e)
    return report


def local_pipeline_ids(db, library: str, document: dict) -> dict[str, str]:
    """Document pipeline id -> this project's pipeline id: THE answer (O11).

    The project's own ``_library_pipelines`` rows first -- a library this
    project shared or copied owns the pipelines it already had, under their
    own ids (plan-library-fork-chain B1/B2) -- and DERIVED ids
    (``ids.library_pipeline_id``) only for a document pipeline no row
    claims (a library seeded here for the first time)."""
    from scistack_gui import ids
    from scistack_gui import pipeline_store as ps

    recorded = {
        r["doc_pipeline_id"]: r["pipeline_id"]
        for r in ps.list_library_pipelines(db)
        if r["library"] == library and r["pipeline_name"] == document["name"]
    }
    out = {
        p["pipeline_id"]: recorded.get(p["pipeline_id"])
        or ids.library_pipeline_id(library, document["name"], p["pipeline_id"])
        for p in document["pipelines"]
    }
    logger.debug(
        "[library_service] %s/%s ids: %d recorded, %d derived",
        library, document["name"],
        sum(1 for d in out if d in recorded), sum(1 for d in out if d not in recorded),
    )
    return out


def _sync_one(db, lp) -> str:
    """Seed, re-sync or leave one library pipeline; returns which."""
    from scistack_gui import ids, library_lock
    from scistack_gui import pipeline_store as ps
    from scistack_gui.services import canvas_snapshot

    from scidb.names import authored_libraries

    doc = lp.document
    local = local_pipeline_ids(db, lp.library, doc)
    names = {p["pipeline_id"]: p["name"] for p in doc["pipelines"]}
    owners = {pid: ps.library_owner(db, pid) for pid in local.values()}
    present = {pid: ps.get_pipeline(db, pid) is not None for pid in local.values()}

    if all(present.values()) and all(
        o is not None and o["definition_hash"] == lp.definition_hash for o in owners.values()
    ):
        logger.debug("[library_service] %s/%s unchanged (%s)", lp.library, lp.name, lp.definition_hash[:12])
        return "unchanged"
    # O12: the canvas is the working copy of an authored library's pipeline;
    # its files are what ⇪ Share wrote. Never re-sync the files over it.
    if lp.library in authored_libraries() and all(present.values()) and all(owners.values()):
        logger.info(
            "[library_service] %s/%s: authored here; the canvas is the working copy, not "
            "re-synced from its files (recorded %s, file %s)",
            lp.library, lp.name,
            sorted({(o["definition_hash"] or "")[:12] for o in owners.values()}),
            lp.definition_hash[:12],
        )
        return "unchanged"

    resync = any(present.values())
    old_hash = next((o["definition_hash"] for o in owners.values() if o), None)
    snap = canvas_snapshot.CanvasSnapshot.from_dict(doc["canvas"])

    with library_lock.library_writes():
        for doc_pid, pid in local.items():
            if present[pid]:
                ps.clear_pipeline_content(db, pid)
                if ps.get_pipeline(db, pid)["name"] != names[doc_pid]:
                    ps.rename_pipeline(db, pid, names[doc_pid])
            else:
                ps.create_pipeline(db, names[doc_pid], pipeline_id=pid)
        canvas_snapshot.apply(
            db,
            snap,
            local,
            include_hides=True,
            node_id_for=lambda n: ids.library_node_id(
                lp.library, lp.name, n.node_id, n.node_type, n.label
            ),
            use_id_for=lambda u: ids.library_use_id(lp.library, lp.name, u.use_id),
        )
        for doc_pid, pid in local.items():
            ps.set_library_owner(db, pid, lp.library, lp.name, doc_pid, lp.definition_hash)

    logger.info(
        "[library_service] %s %s/%s: %d pipeline(s) %s; hash %s -> %s; %s",
        "re-synced" if resync else "seeded",
        lp.library,
        lp.name,
        len(local),
        sorted(local.values()),
        (old_hash or "-")[:12],
        lp.definition_hash[:12],
        snap.describe(),
    )
    return "resynced" if resync else "seeded"


def list_libraries(db) -> list[dict]:
    """The Libraries list: every listed library with its schema keys, the
    pipelines it ships (their ROOT pipeline id here, ready to place),
    whether this project AUTHORS it (B4: editable here, ⇪ Share publishes)
    and anything wrong with it. ``[{"library", "schema_keys", "matlab",
    "authored", "derived_from", "pipelines": [{"name", "pipeline_id",
    "seeded"}], "errors"}]``."""
    from scidb.library import read_library
    from scidb.names import authored_libraries, library_packages

    from scistack_gui import pipeline_store as ps

    authored = authored_libraries()
    out = []
    for lib in sorted(library_packages()):
        info = read_library(lib)
        pipelines = []
        for lp in info.pipelines:
            root = local_pipeline_ids(db, lib, lp.document)[lp.document["root"]]
            pipelines.append(
                {
                    "name": lp.name,
                    "pipeline_id": root,
                    "seeded": ps.library_owner(db, root) is not None,
                }
            )
        out.append(
            {
                "library": lib,
                "schema_keys": info.schema_keys,
                "matlab": info.matlab_dir is not None,
                "authored": lib in authored,
                "derived_from": info.derived_from,
                "pipelines": pipelines,
                "errors": info.errors,
            }
        )
    logger.info(
        "[library_service] list_libraries: %d librar(ies), %d pipeline(s)",
        len(out),
        sum(len(x["pipelines"]) for x in out),
    )
    return out


def suggest_key_map(db, pipeline_id: str) -> dict:
    """For placing library pipeline *pipeline_id*: the library's schema keys,
    the project's, and the suggested binding ``key_map`` (library key ->
    project key) from ``scidb.schema_map.KeyMap.auto`` -- the Stage 6 owner
    of matching two schemas. A library that declares no schema keys needs
    no map."""
    from scidb.library import read_library
    from scidb.schema_map import KeyMap

    from scistack_gui import pipeline_store as ps

    owner = ps.library_owner(db, pipeline_id)
    if owner is None:
        raise ValueError(f"{pipeline_id} is not a library pipeline")
    library_keys = read_library(owner["library"]).schema_keys
    project_keys = list(db.dataset_schema_keys)
    if not library_keys:
        return {"library_keys": None, "project_keys": project_keys, "key_map": {}, "unmapped": []}
    km = KeyMap.auto(library_keys, project_keys)
    mapping = km.as_dict()
    return {
        "library_keys": library_keys,
        "project_keys": project_keys,
        # Only the renames and drops; an identity entry needs no binding.
        "key_map": {k: v for k, v in mapping.items() if v is not None and v != k},
        "unmapped": sorted(k for k, v in mapping.items() if v is None),
    }


def _requirements(db, pipeline_id: str) -> "tuple[dict, set[str], set[str]]":
    """``(owner, parameter names, PathInput names)`` the library pipeline
    *pipeline_id* (and the library pipelines it places) refers to. Read from
    the seeded canvas: the nodes are the requirements."""
    from scistack_gui import pipeline_store as ps

    owner = ps.library_owner(db, pipeline_id)
    if owner is None:
        raise ValueError(f"{pipeline_id} is not a library pipeline")
    todo, seen = [pipeline_id], set()
    params: set[str] = set()
    pis: set[str] = set()
    while todo:
        pid = todo.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for meta in ps.get_manual_nodes(db, pid).values():
            if meta["type"] == "parameterNode":
                params.add(meta["label"])
            elif meta["type"] == "pathInputNode":
                pis.add(meta["label"])
        todo += [u["child_pipeline_id"] for u in ps.get_pipeline_uses(db, pid)]
    return owner, params, pis


def placement_check(db, pipeline_id: str) -> dict:
    """Everything placing library pipeline *pipeline_id* needs to ask: the
    suggested key map (:func:`suggest_key_map`) and the Parameters /
    PathInputs it uses that this project does not declare, with the
    library's DEFAULTS (portability Stage 10b: the project declares its own;
    runs resolve those nodes by name in the project's registry)."""
    from scidb.library import read_library

    from scistack_gui import registry
    from scistack_gui.domain.graph_builder import path_input_display

    owner, params, pis = _requirements(db, pipeline_id)
    info = read_library(owner["library"])
    have_params = registry.get_parameters_registry()
    have_pis = registry.get_path_inputs_registry()
    missing_params = [
        {"name": n, "default": info.parameter_defaults.get(n, [])}
        for n in sorted(params - set(have_params))
    ]
    missing_pis = [
        {"name": n, "default": info.path_input_defaults.get(n, [""])}
        for n in sorted(pis - set(have_pis))
    ]
    # D1: a name the project already declares is the PROJECT's value; the
    # library's default is shown beside it, never written over it.
    project_values = [
        {"name": n, "project": list(have_params[n].values),
         "library": info.parameter_defaults.get(n, [])}
        for n in sorted(params & set(have_params))
    ]
    for n in sorted(pis & set(have_pis)):
        d = path_input_display(have_pis[n])
        project_values.append({
            "name": n,
            "project": [d["template"], *[a["template"] for a in d["alternate_templates"]]],
            "library": info.path_input_defaults.get(n, [""]),
        })
    out = {
        **suggest_key_map(db, pipeline_id),
        "library": owner["library"],
        "missing_parameters": missing_params,
        "missing_path_inputs": missing_pis,
        "project_values": project_values,
    }
    logger.info(
        "[library_service] placement_check %s (%s): missing parameters %s, path inputs %s; "
        "project's own values kept for %s",
        pipeline_id,
        owner["library"],
        [p["name"] for p in missing_params],
        [p["name"] for p in missing_pis],
        [p["name"] for p in project_values],
    )
    return out


def declare_requirements(db, pipeline_id: str, names: "list[str] | None" = None) -> dict:
    """Write the library's defaults for the Parameters / PathInputs the
    project lacks (all of them, or just *names*) into the PROJECT's own
    entities file, through the same services the sidebar's "+" uses. A
    PathInput gets the library's template(s) and no root folder: where the
    raw files are is the project's to say."""
    from scistack_gui.services.parameter_service import create_parameter
    from scistack_gui.services.path_input_service import create_path_input

    check = placement_check(db, pipeline_id)
    wanted = None if names is None else set(names)
    declared, failed = [], []
    for p in check["missing_parameters"]:
        if wanted is not None and p["name"] not in wanted:
            continue
        result = create_parameter(p["name"], list(p["default"]))
        (declared if result.get("ok", True) else failed).append(
            p["name"] if result.get("ok", True) else {"name": p["name"], "error": result.get("error")}
        )
    for p in check["missing_path_inputs"]:
        if wanted is not None and p["name"] not in wanted:
            continue
        templates = list(p["default"]) or [""]
        result = create_path_input(
            p["name"], templates[0], None,
            [{"template": t, "root_folder": None} for t in templates[1:]] or None,
        )
        (declared if result.get("ok", True) else failed).append(
            p["name"] if result.get("ok", True) else {"name": p["name"], "error": result.get("error")}
        )
    logger.info(
        "[library_service] declare_requirements %s: declared %s, failed %s",
        pipeline_id, declared, failed,
    )
    return {"ok": not failed, "declared": declared, "failed": failed}


# ---------------------------------------------------------------------------
# Placing, and the tunable values of a placement (plan-library-fork-chain
# Stage 3: D1, D2, E1-E8)
# ---------------------------------------------------------------------------


def on_placed(db, child_pipeline_id: str) -> dict:
    """Placing a library pipeline declares what the project lacks, by itself
    (E7): Parameters with the library's defaults, PathInputs with its
    templates and no root. A name the project has is left as it is (D1).
    ``{}`` for a pipeline no library owns."""
    from scistack_gui import pipeline_store as ps

    if ps.library_owner(db, child_pipeline_id) is None:
        return {}
    result = declare_requirements(db, child_pipeline_id)
    logger.info("[library_service] placed %s: declared %s, failed %s",
                child_pipeline_id, result["declared"], result["failed"])
    return result


def unwired_step_values(db, pipeline_ids) -> list[dict]:
    """The STEP VALUES of the canvases *pipeline_ids*: every argument of a
    Python function node that has a default and that no edge feeds
    (``[{"label", "input", "default", "node_id", "pipeline_id"}]``, in
    canvas order). Read from the stored canvas (rows + drawn edges): the
    document is the canvas. One owner for E1's tunables and E8's report."""
    import inspect as _inspect

    from scistack_gui import matlab_registry, registry
    from scistack_gui import pipeline_store as ps
    from scistack_gui.ids import handle_name, strip_placement

    wired: dict[str, set[str]] = {}
    for e in ps.get_manual_edges(db):
        name = handle_name(e.get("targetHandle"))
        if name:
            wired.setdefault(str(strip_placement(e.get("target") or "")), set()).add(name)
    out = []
    for pid in pipeline_ids:
        for nid, meta in sorted(ps.get_manual_nodes(db, pid).items()):
            label = meta.get("label")
            if meta.get("type") != "functionNode" or matlab_registry.is_matlab_function(label):
                continue
            fn = registry.lookup_function(label)
            if fn is None:
                continue
            try:
                sig = _inspect.signature(getattr(fn, "fcn", fn))
            except (TypeError, ValueError):
                continue
            fed = wired.get(str(strip_placement(nid)), set())
            for arg, p in sig.parameters.items():
                if p.default is _inspect.Parameter.empty or arg in fed:
                    continue
                if p.kind not in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY):
                    continue
                out.append({"label": label, "input": arg, "default": p.default,
                            "node_id": nid, "pipeline_id": pid})
    return out


def parameter_users(db) -> dict[str, list[str]]:
    """``{Parameter name: [libraries whose pipelines use it]}`` (E6), from
    the requirements of every library pipeline here (O18)."""
    from scistack_gui import pipeline_store as ps

    users: dict[str, set[str]] = {}
    for row in ps.list_library_pipelines(db):
        try:
            owner, params, _pis = _requirements(db, row["pipeline_id"])
        except ValueError:
            continue
        for name in params:
            users.setdefault(name, set()).add(owner["library"])
    return {name: sorted(libs) for name, libs in users.items()}


def _use(db, use_id: str) -> dict:
    from scistack_gui import pipeline_store as ps

    use = next((u for u in ps.get_pipeline_uses(db) if u["use_id"] == use_id), None)
    if use is None:
        raise ValueError(f"no placement {use_id!r}")
    return use


def _closure(db, pipeline_id: str) -> list[str]:
    from scistack_gui.services.portability_service import _closure_pipeline_ids

    return _closure_pipeline_ids(db, pipeline_id)


def tunables(db, use_id: str) -> dict:
    """What a recipient can tune on ONE placement of a library pipeline (E1),
    composed from the owners, storing nothing (O23):

    * ``parameters``: the project-level Parameters it needs, with the
      project's value and whether that is still the library default;
    * ``path_inputs``: its PathInputs, with whether a root folder is still
      needed (D2);
    * ``step_values``: function arguments no Parameter feeds (``key`` =
      ``"<fn label>.<input>"``), with this placement's value (its binding,
      O21) or a Parameter it was pointed at (E5);
    * ``key_map``: this placement's schema-key mapping."""
    from scidb.library import read_library

    from scistack_gui import pipeline_store as ps
    from scistack_gui import registry
    from scistack_gui.domain.graph_builder import path_input_display

    use = _use(db, use_id)
    child = use["child_pipeline_id"]
    owner, params, pis = _requirements(db, child)
    info = read_library(owner["library"])
    have_params = registry.get_parameters_registry()
    have_pis = registry.get_path_inputs_registry()
    binding = dict(use.get("binding") or {})
    overrides = dict(binding.get("params") or {})

    parameters = []
    for name in sorted(params):
        p = have_params.get(name)
        value = list(p.values) if p is not None else None
        default = info.parameter_defaults.get(name)
        source = "missing" if p is None else (
            "library default" if default is not None and value == list(default) else "project"
        )
        parameters.append({"name": name, "value": value, "source": source})
    path_inputs = []
    for name in sorted(pis):
        obj = have_pis.get(name)
        d = path_input_display(obj) if obj is not None else {"template": None, "root_folder": None}
        root = d.get("root_folder") or None
        path_inputs.append({"name": name, "template": d.get("template"), "root_folder": root,
                            "needs_root": root is None})
    step_values = []
    for sv in unwired_step_values(db, _closure(db, child)):
        key = f"{sv['label']}.{sv['input']}"
        held = overrides.get(key, sv["default"])
        ref = held.get("parameter") if isinstance(held, dict) else None
        if ref is not None:
            p = have_params.get(ref)
            value = (list(p.values)[0] if p is not None and len(list(p.values)) == 1
                     else (list(p.values) if p is not None else None))
        else:
            value = held
        step_values.append({"key": key, "value": value, "default": sv["default"],
                            "parameter": ref})
    out = {
        "use_id": use_id,
        "pipeline_id": child,
        "library": owner["library"],
        "parameters": parameters,
        "path_inputs": path_inputs,
        "step_values": step_values,
        "key_map": dict(binding.get("key_map") or {}),
    }
    logger.debug("[library_service] tunables %s: %s", use_id, out)
    return out


def project_tunables(db) -> list[dict]:
    """E2: one row per placement of a library pipeline anywhere in the
    project, nested ones included -- ``{"use_id", "path", ...tunables}``,
    ``path`` = pipeline names from the top. View only: edits go through the
    placement (:func:`set_step_value`), the one owner of its values."""
    from scistack_gui import pipeline_store as ps

    pipelines = {p["pipeline_id"]: p["name"] for p in ps.list_all_pipelines(db)}
    uses = ps.get_pipeline_uses(db)
    children = {u["child_pipeline_id"] for u in uses}
    tops = [pid for pid in pipelines if pid not in children]
    path_of: dict[str, list[str]] = {}
    todo = [(pid, [pipelines[pid]]) for pid in sorted(tops)]
    while todo:
        pid, path = todo.pop(0)
        for u in uses:
            if u["parent_pipeline_id"] != pid or u["use_id"] in path_of:
                continue
            child_path = [*path, pipelines.get(u["child_pipeline_id"], u["child_pipeline_id"])]
            path_of[u["use_id"]] = child_path
            todo.append((u["child_pipeline_id"], child_path))
    rows = []
    for u in uses:
        if ps.library_owner(db, u["child_pipeline_id"]) is None:
            continue
        rows.append({**tunables(db, u["use_id"]),
                     "path": path_of.get(u["use_id"], [pipelines.get(u["child_pipeline_id"], "")])})
    rows.sort(key=lambda r: r["path"])
    logger.info("[library_service] project tunables: %d placement(s)", len(rows))
    return rows


def _step_value_keys(db, use: dict) -> set[str]:
    return {f"{sv['label']}.{sv['input']}"
            for sv in unwired_step_values(db, _closure(db, use["child_pipeline_id"]))}


def set_step_value(db, use_id: str, key: str, value) -> dict:
    """E3: this placement's value for step value *key* (``"fn.param"``,
    the scidb ``PipelineBinding`` key, O22), written to its binding (O21).
    The next run of the placement uses it."""
    from scistack_gui import pipeline_store as ps

    use = _use(db, use_id)
    if key not in _step_value_keys(db, use):
        raise ValueError(f"{key!r} is not a step value of this placement")
    binding = dict(use.get("binding") or {})
    params = dict(binding.get("params") or {})
    old = params.get(key)
    params[key] = value
    binding["params"] = params
    ps.update_use_binding(db, use_id, binding)
    logger.info("[library_service] tunables %s: %s %r -> %r", use_id, key, old, value)
    return {"ok": True, **tunables(db, use_id)}


def promote_step_value(db, use_id: str, key: str, parameter: str) -> dict:
    """E5 "Make this a Parameter": a project Parameter *parameter* holding
    the step value's current value, and the placement pointed at it -- a
    REFERENCE in the binding, never a copy of the value (O21). Editing the
    Parameter then changes this placement."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui import registry
    from scistack_gui.services.parameter_service import create_parameter

    use = _use(db, use_id)
    current = next((sv for sv in tunables(db, use_id)["step_values"] if sv["key"] == key), None)
    if current is None:
        raise ValueError(f"{key!r} is not a step value of this placement")
    if parameter not in registry.get_parameters_registry():
        value = current["value"]
        result = create_parameter(parameter, value if isinstance(value, list) else [value])
        if not result.get("ok", True):
            raise ValueError(result.get("error") or f"could not declare {parameter}")
    binding = dict(_use(db, use_id).get("binding") or {})
    params = dict(binding.get("params") or {})
    params[key] = {"parameter": parameter}
    binding["params"] = params
    ps.update_use_binding(db, use_id, binding)
    logger.info("[library_service] tunables %s: %s now follows Parameter %s", use_id, key, parameter)
    return {"ok": True, **tunables(db, use_id)}


def resolved_binding_params(params: "dict | None") -> dict:
    """A binding's ``params`` as the backend binds them: a Parameter
    REFERENCE (``{"parameter": name}``, E5) becomes the project's Parameter
    (an ``EachOf``, so every value runs); plain values pass through."""
    from scistack_gui import registry

    out = {}
    for key, value in (params or {}).items():
        if isinstance(value, dict) and "parameter" in value:
            p = registry.get_parameters_registry().get(value["parameter"])
            if p is None:
                raise ValueError(
                    f"step value {key} follows the Parameter {value['parameter']!r}, "
                    "which this project does not declare"
                )
            out[key] = p
        else:
            out[key] = value
    return out


def missing_roots(db, pipeline_id: str) -> list[str]:
    """D2: the PathInputs a library pipeline in *pipeline_id*'s closure
    reads that have no root folder here yet (a library carries templates,
    never where one project's files are)."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui import registry
    from scistack_gui.domain.graph_builder import path_input_display

    pis = registry.get_path_inputs_registry()
    out = set()
    for pid in _closure(db, pipeline_id):
        if ps.library_owner(db, pid) is None:
            continue
        for meta in ps.get_manual_nodes(db, pid).values():
            if meta.get("type") != "pathInputNode":
                continue
            obj = pis.get(meta.get("label"))
            if obj is not None and not path_input_display(obj).get("root_folder"):
                out.add(meta["label"])
    return sorted(out)
