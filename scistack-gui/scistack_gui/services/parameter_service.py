"""
Parameter creation and editing — the single source of truth for writing
named ``scidb.Parameter(...)`` declarations to source.

A Parameter is a named thing with one or more values, replacing the former
Constant (one) / Sweep (many) split — see
docs/claude/entity-editability-model.md (D6). One service, because there is
one concept: adding a value is adding an argument, never a change of kind.

A Parameter is source-scanned (see
docs/claude/code-discovery-categories.md), so "create from the GUI" means
appending a real declaration to the configured ``variable_file`` and
refreshing the registry — never writing a bare name to layout.json.

The declaration text is rendered by ``scidb.source_edit.render_parameter``
(Python) / ``matlab_parser.render_matlab_parameter`` (MATLAB), which own the
declaration grammar. :func:`update_parameter` rewrites an existing
declaration in place through ``target_file_service.update_declaration``,
which owns the write policy (entities-file confinement, stale-file guard,
atomic write, rollback).
"""

from __future__ import annotations

import logging

from scistack_gui.services.target_file_service import (
    append_and_refresh,
    validate_entity_name,
)

logger = logging.getLogger(__name__)


def create_parameter(
    name: str, values: "list | None" = None, description: str = ""
) -> dict:
    """Append ``NAME = scidb.Parameter(...)`` to the configured
    ``variable_file`` and refresh the registry.

    An empty *values* writes a Parameter with **no values** — the GUI's "New
    parameter" form only collects a name, and the user fills in the value(s)
    afterwards, in the panel or in source. It used to scaffold a placeholder
    ``0`` instead, which is indistinguishable from a declared value once
    written: it showed as a checked value on the node, fed ``for_each``, and
    any run started before the user noticed stamped ``0`` into records.

    Declared-but-unvalued is legal at rest and refused at execution — see
    ``scidb.parameter`` and ``scifor.require_alternatives``.

    Returns ``{"ok": True, "name": name}`` on success, ``{"ok": False,
    "error": ...}`` on failure (invalid name, name collision, no configured
    target file, or a write/refresh failure).
    """
    from scistack_gui import registry

    err = validate_entity_name(name)
    if err:
        return {"ok": False, "error": err}
    if name in registry.get_parameters_registry():
        return {"ok": False, "error": f"A Parameter named '{name}' already exists."}

    values = list(values or [])
    if not values:
        logger.info(
            "[parameter_service] create_parameter: %r declared with no values "
            "yet — it must be given at least one before anything wired to it "
            "can run",
            name,
        )

    from scistack_gui.services.target_file_service import (
        get_or_create_target_file,
        is_toml_target,
        write_entity,
    )

    target_file, target_err = get_or_create_target_file()
    if target_file is None:
        return {"ok": False, "error": target_err}

    logger.info(
        "[parameter_service] create_parameter: name=%r %d value(s) description=%r "
        "target=%s",
        name,
        len(values),
        description,
        target_file,
    )

    if is_toml_target(target_file):
        from scidb.entities import render_parameter_value

        if description:
            # The TOML format has no home for it (plan D4). Say so rather
            # than dropping it silently -- a description that vanishes with
            # no trace is worse than one that never appeared.
            logger.warning(
                "[parameter_service] Dropping description %r for %r: the TOML "
                "entities file has no description field; declare the Parameter "
                "in Python if you need one",
                description,
                name,
            )
        err = write_entity(
            target_file,
            section="parameters",
            name=name,
            rendered=render_parameter_value(values),
        )
    else:
        from scidb.source_edit import render_parameter

        line = f"\n{name} = {render_parameter(values, description)}\n"
        err = append_and_refresh(line, target_file)

    if err:
        return err
    return {"ok": True, "name": name}


def update_parameter(
    name: str,
    values: list,
    description: str = "",
    group: "dict | None" = None,
) -> dict:
    """Rewrite an existing Parameter's declaration in place, in whichever
    language declared it.

    Adding a value is just a longer *values* list — there is no separate
    "convert" operation, because there is no second kind to convert to
    (D6). An empty *values* is **accepted**: a Parameter's value set may be
    empty at any time, not only at creation, so removing the last value is
    allowed rather than blocked. Values that have already run keep their DB
    history and stay visible on the node as ``history`` rows; a run wired to
    the now-empty Parameter fails loudly at ``for_each`` expansion.

    *group* marks these values as one **generated set** —
    ``{"kind": "range"|"list", "spec": {...}}`` — which is display state
    only: the panel's "Replace values" button sends it, the "Add value"
    button does not, and that is the whole of how the two are told apart.
    It is recorded in the GUI's own store rather than in source, so the
    declaration stays a flat list of values in every language and nothing
    about ``version_keys`` or MATLAB parity changes. Passing ``None`` does
    NOT clear an existing group by itself — adding a value alongside a
    generated set leaves the set intact — it only drops one that has stopped
    describing what source declares (see :func:`_record_value_group`).
    """
    from scidb.entities import render_parameter_value
    from scidb.source_edit import render_parameter

    from scistack_gui.matlab_parser import render_matlab_parameter
    from scistack_gui.services.target_file_service import update_declaration

    if not values:
        # Worth a line in scistack.log: this is the one edit that can take a
        # wired, previously-runnable Parameter back to un-runnable.
        from scistack_gui import registry

        existing = registry.get_parameters_registry().get(name)
        logger.info(
            "[parameter_service] update_parameter: emptying %r (%d declared "
            "value(s) dropped) — anything wired to it cannot run until it has "
            "a value again",
            name,
            len(existing.values) if existing is not None else 0,
        )

    result = update_declaration(
        "parameter",
        name,
        python_expr=render_parameter(values, description),
        matlab_expr=render_matlab_parameter(values, description),
        toml_expr=render_parameter_value(values),
    )
    if result.get("ok"):
        _record_value_group(name, values, group)
    return result


def _record_value_group(name: str, values: list, group: "dict | None") -> None:
    """Persist (or clear) the generated-set marker for *name*.

    Best-effort and never fatal: the values are already written to source by
    the time this runs, and a missing database must not turn a successful
    edit into a reported failure. Losing the marker costs the compact row on
    the canvas, nothing else.
    """
    from scistack_gui import pipeline_store

    try:
        from scistack_gui.db import get_db

        db = get_db()
    except Exception as e:  # pragma: no cover - depends on GUI db state
        logger.warning(
            "[parameter_service] no database while recording the value group "
            "for %r (%s) — values were written, grouping was not",
            name,
            e,
        )
        return
    if db is None:
        return

    if group:
        pipeline_store.set_parameter_value_group(
            db,
            name,
            kind=group.get("kind", "list"),
            spec=group.get("spec") or {},
            values=[str(v) for v in values],
        )
        logger.info(
            "[parameter_service] %r: %d value(s) recorded as one generated "
            "set (kind=%s)",
            name,
            len(values),
            group.get("kind", "list"),
        )
        return

    # No group given: this is an ordinary edit ("Add value", a removal, a
    # rewrite from source). It does NOT clear an existing group by itself —
    # adding a value alongside a generated set leaves the set intact, and the
    # extra value simply renders as its own row. The group only dies when it
    # stops describing what source declares, which is exactly the check
    # ``build_parameter_nodes`` applies on every read; doing it here too just
    # means the dead row is gone at the moment it dies, rather than being
    # ignored until something overwrites it.
    existing = pipeline_store.get_parameter_value_groups(db).get(name)
    if existing is None:
        return
    declared = {str(v) for v in values}
    missing = [m for m in existing["values"] if m not in declared]
    if missing:
        pipeline_store.clear_parameter_value_group(db, name)
        logger.info(
            "[parameter_service] %r: generated set dropped — %d of its %d "
            "value(s) are no longer declared (%s)",
            name,
            len(missing),
            len(existing["values"]),
            missing,
        )
