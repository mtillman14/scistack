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


def local_pipeline_ids(library: str, document: dict) -> dict[str, str]:
    """Document pipeline id -> this project's (derived) pipeline id."""
    from scistack_gui import ids

    return {
        p["pipeline_id"]: ids.library_pipeline_id(library, document["name"], p["pipeline_id"])
        for p in document["pipelines"]
    }


def _sync_one(db, lp) -> str:
    """Seed, re-sync or leave one library pipeline; returns which."""
    from scistack_gui import ids, library_lock
    from scistack_gui import pipeline_store as ps
    from scistack_gui.services import canvas_snapshot

    doc = lp.document
    local = local_pipeline_ids(lp.library, doc)
    names = {p["pipeline_id"]: p["name"] for p in doc["pipelines"]}
    owners = {pid: ps.library_owner(db, pid) for pid in local.values()}
    present = {pid: ps.get_pipeline(db, pid) is not None for pid in local.values()}

    if all(present.values()) and all(
        o is not None and o["definition_hash"] == lp.definition_hash for o in owners.values()
    ):
        logger.debug("[library_service] %s/%s unchanged (%s)", lp.library, lp.name, lp.definition_hash[:12])
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
    pipelines it ships (their ROOT pipeline id here, ready to place) and
    anything wrong with it. ``[{"library", "schema_keys", "matlab",
    "pipelines": [{"name", "pipeline_id", "seeded"}], "errors"}]``."""
    from scidb.library import read_library
    from scidb.names import library_packages

    from scistack_gui import pipeline_store as ps

    out = []
    for lib in sorted(library_packages()):
        info = read_library(lib)
        pipelines = []
        for lp in info.pipelines:
            root = local_pipeline_ids(lib, lp.document)[lp.document["root"]]
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
