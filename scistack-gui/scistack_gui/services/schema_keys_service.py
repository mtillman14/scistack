"""Change the schema keys of an empty dataset: the GUI half (GitHub issue #10).

scidb owns the rules (``scidb.store.schema_keys_change``: validation, the
proposal's union and order, the change itself, and "empty" in
``scidb.store.emptiness``). This module only (1) finds the PathInputs wired
into the failed node and asks scidb for the proposal, and (2) after scidb has
changed the keys and closed the database, reopens it through the normal
startup path (``runtime.db``) so the keys are read fresh.

``.claude/plan-change-schema-keys.md``; docs/claude/changing-schema-keys.md.
"""

from __future__ import annotations

import logging
import time

from scistack_gui.ids import PATH_INPUT_ID_PREFIX, strip_placement

logger = logging.getLogger(__name__)


def _path_input_names_wired_into(db, node_id: str) -> list[str]:
    """Names of the PathInputs with an edge into *node_id* (any scope)."""
    from scistack_gui.domain.edge_view import effective_edges

    target = str(strip_placement(node_id))
    names: list[str] = []
    # The edge view, not the stored rows: a hidden edge or an edge on a hidden
    # node binds nothing, so it must not offer its PathInput's keys.
    view = effective_edges(db, None, caller="schema_keys_proposal")
    for edge in view.wiring_list:
        source = str(strip_placement(edge.get("source") or ""))
        if str(strip_placement(edge.get("target") or "")) == target and source.startswith(
            PATH_INPUT_ID_PREFIX
        ):
            name = source[len(PATH_INPUT_ID_PREFIX):]
            if name not in names:
                names.append(name)
    return names


def _placeholder_keys(path_input) -> list[str]:
    """A PathInput's placeholder keys (an ``EachOf``: each alternative's, in order)."""
    alternatives = getattr(path_input, "alternatives", None)
    if alternatives is not None:
        keys: list[str] = []
        for alt in alternatives:
            for key in _placeholder_keys(alt):
                if key not in keys:
                    keys.append(key)
        return keys
    getter = getattr(path_input, "placeholder_keys", None)
    return list(getter()) if getter else []


def schema_key_proposal(db, node_id: "str | None", data_columns: list[str]) -> dict:
    """``{existing, proposed: [{key, source}], dataset_empty, path_inputs}``.

    *node_id* is the failed node when the dialog opens from a run's remedy
    popup. With no node (the header's "Schema keys…" button) there is
    nothing to guess from, so the proposal is the current keys."""
    from scidb.store.emptiness import is_empty
    from scidb.store.schema_keys_change import propose_schema_keys
    from scistack_gui.project import registry

    started = time.perf_counter()
    names = _path_input_names_wired_into(db, node_id) if node_id else []
    # No node: the current keys are the proposal (and no data columns).
    key_lists = [] if node_id else [list(db.dataset_schema_keys)]
    data_columns = list(data_columns) if node_id else []
    for name in names:
        path_input = registry.get_path_input(name)
        if path_input is None:
            logger.warning("[schema_keys] PathInput %r wired into %s is not registered", name, node_id)
            continue
        key_lists.append(_placeholder_keys(path_input))
    proposed = propose_schema_keys(key_lists, list(data_columns))
    from scidb.store.emptiness import usage_by_branch

    result = {
        "existing": list(db.dataset_schema_keys),
        # The tree as one list per branch, and which branches hold data: the
        # dialog shows the tree read-only and lets only unused keys change.
        "existing_branches": db.schema_tree.paths(),
        "usage": [
            {"branch": branch, "used": used, "records": records}
            for branch, used, records in usage_by_branch(db)
        ],
        "proposed": proposed,
        "dataset_empty": is_empty(db),
        "path_inputs": names,
    }
    logger.info(
        "[schema_keys] proposal for %s: existing=%s proposed=%s (path inputs %s, %d data column(s), %.1f ms)",
        node_id, result["existing"], [p["key"] for p in proposed], names,
        len(data_columns), (time.perf_counter() - started) * 1000,
    )
    return result


def change_schema_keys(db, keys: list, renames: "dict[str, str] | None" = None) -> dict:
    """Change the keys, then reopen the database so this process uses the new
    keys. *keys* is a plain list (one branch) or one list per branch. An empty
    dataset takes any change; one that holds data takes new keys appended (a
    branch, a deeper level) and changes to keys no record or run names; scidb
    (``scidb.store.schema_keys_change``) says which, and a refusal is returned
    as ``{ok: False, error}`` with the database still open."""
    from scidb.store.schema_keys_change import SchemaKeysChangeRefused, change_schema_keys as change
    from scistack_gui.headless import bundle_providers
    from scistack_gui.project import registry
    from scistack_gui.project.config import resolve_project_root
    from scistack_gui.runtime import db as runtime_db

    path = runtime_db.get_db_path()
    # The GUI's one owner of "which folder is the project" (the config the
    # alias / colour editors write is found from here too).
    root = resolve_project_root(None, path)
    logger.info(
        "[schema_keys] change requested: keys=%s renames=%s (project root %s)",
        keys, renames or {}, root,
    )
    try:
        # The bundle sections rename what they own (canvas, saved plots,
        # saved stats); scidb renames the config and entities file.
        change_result = change(
            db, keys, renames=renames, reason="changed from the GUI",
            root=root, providers=bundle_providers(),
        )
    except SchemaKeysChangeRefused as exc:
        logger.warning("[schema_keys] refused: %s", exc)
        return {"ok": False, "error": str(exc)}
    runtime_db.close_db()
    runtime_db.init_db(path)
    logger.info("[schema_keys] database reopened with keys %s", change_result.new_keys)

    references = change_result.references or {}
    if references.get("entities"):
        # The renamed PathInput templates must reach the registry now, not at
        # the next Refresh Code (a run would use the old placeholders).
        error = registry.reload_entities_file()
        if error:
            change_result.leftovers.append(error)
    leftovers = list(change_result.leftovers) + _code_path_inputs_naming(
        list(change_result.renames) + list(change_result.deleted_keys)
    )
    for line in leftovers:
        logger.warning("[schema_keys] leftover: %s", line)
    return {
        "ok": True,
        "old_keys": change_result.old_keys,
        "new_keys": change_result.new_keys,
        "renames": change_result.renames,
        "deleted_keys": change_result.deleted_keys,
        "added_keys": change_result.added_keys,
        "references": {k: v for k, v in references.items() if k not in ("dropped", "flagged")},
        "leftovers": leftovers,
        "leftovers_removed": change_result.leftovers_removed,
    }


def _code_path_inputs_naming(old_keys: list[str]) -> list[str]:
    """One line per PathInput declared in CODE (not the entities file, not a
    library) whose template still names one of *old_keys*. Code is never
    edited, so these are for the user to fix."""
    from scistack_gui.project import registry

    if not old_keys:
        return []
    lines = []
    for name, (pi, source) in sorted(registry.code_declared_path_inputs().items()):
        named = [k for k in _placeholder_keys(pi) if k in old_keys]
        if named:
            lines.append(
                f"PathInput {name} in {source or 'code'} still names "
                f"{', '.join('{' + k + '}' for k in named)}; edit its template there."
            )
    return lines
