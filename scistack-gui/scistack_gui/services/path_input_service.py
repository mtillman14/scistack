"""
PathInput/Sweep creation service — single source of truth for writing new
named ``scidb.PathInput``/``scidb.Sweep`` declarations to source.

Mirrors ``variable_service.create_variable``'s exact pattern: PathInput/
Sweep are source-scanned (see docs/claude/code-discovery-categories.md), so
"create from the GUI" means appending a real declaration to the configured
``variable_file`` and refreshing the registry — never writing to
layout.json.

The declaration text itself is rendered by ``scidb.source_edit``
(``render_path_input``/``render_parameter``), which owns the declaration
grammar, so creation and the eventual in-place editing can never drift
apart in what they write. There is no "update" counterpart here yet —
editing an existing PathInput/Sweep's value still means editing source and
hitting Refresh Code; see
``.claude/plan-gui-entity-editing-26-08-24.md`` Stage 5.

This is also what ``portability_service.import_pipeline_document`` calls to
materialize a bundled PathInput/Sweep the importer doesn't already have
locally (see docs/claude/code-discovery-categories.md's portability
section) — one code path serves both callers.
"""

from __future__ import annotations

import logging

from scistack_gui.services.target_file_service import (
    append_and_refresh as _append_and_refresh,
    validate_entity_name as _validate_name,
)

logger = logging.getLogger(__name__)


def create_path_input(
    name: str,
    template: str,
    root_folder: "str | None" = None,
    alternate_templates: "list[dict] | None" = None,
) -> dict:
    """Append ``NAME = scidb.PathInput(...)`` (or, with ``alternate_templates``,
    ``NAME = scidb.EachOf(scidb.PathInput(...), scidb.PathInput(...), ...)``) to the
    configured ``variable_file`` and refresh the registry.

    ``alternate_templates`` is only ever populated by
    ``portability_service.import_pipeline_document`` materializing a
    bundled multi-template PathInput — the GUI's own create action never
    passes it (alternates are a source-code-only concept now, see
    docs/claude/code-discovery-categories.md).

    Returns ``{"ok": True, "name": name}`` on success, ``{"ok": False,
    "error": ...}`` on failure (invalid name, name collision, no configured
    target file, or a write/refresh failure).
    """
    from scistack_gui import registry

    err = _validate_name(name)
    if err:
        return {"ok": False, "error": err}
    if name in registry.get_path_inputs_registry():
        return {"ok": False, "error": f"A PathInput named '{name}' already exists."}

    from scistack_gui.services.target_file_service import (
        get_or_create_target_file,
        is_toml_target,
        write_entity,
    )

    target_file, target_err = get_or_create_target_file()
    if target_file is None:
        return {"ok": False, "error": target_err}

    logger.info(
        "[path_input_service] create_path_input: name=%r template=%r root_folder=%r "
        "%d alternate(s) target=%s",
        name,
        template,
        root_folder,
        len(alternate_templates or []),
        target_file,
    )

    if is_toml_target(target_file):
        from scidb.entities import render_path_input_value

        err = write_entity(
            target_file,
            section="path_inputs",
            name=name,
            rendered=render_path_input_value(
                template, root_folder, alternate_templates
            ),
        )
    else:
        from scidb.source_edit import render_path_input

        line = (
            f"\n{name} = "
            f"{render_path_input(template, root_folder, alternate_templates, name=name)}\n"
        )
        err = _append_and_refresh(line, target_file)

    if err:
        return err
    return {"ok": True, "name": name}


def update_path_input(
    name: str,
    template: str,
    root_folder: "str | None" = None,
    alternate_templates: "list[dict] | None" = None,
) -> dict:
    """Rewrite an existing PathInput declaration in place.

    Note the asymmetry documented in
    ``docs/claude/entity-editability-model.md`` Rule 2: *adding* an alternate
    template is always safe, whereas *replacing* the primary template changes
    what prior runs content-match against. D7's name-history table is what
    keeps that non-destructive.

    An empty template is refused here rather than written: it renders as a
    declaration ``scidb.entities`` rejects ("missing a 'template' string"),
    so the write would go through, fail verification, and be rolled back --
    telling the user their edit "no longer resolves" when the real problem
    is a blank field. Editing only the root folder of a PathInput whose
    template box is empty is exactly how that happens.
    """
    if not (template or "").strip():
        logger.info(
            "[path_input_service] Refusing to update %r: empty template "
            "(root_folder=%r)",
            name,
            root_folder,
        )
        return {
            "ok": False,
            "error": (
                f"'{name}' needs a path template — a root folder alone is not "
                f"a PathInput."
            ),
        }

    from scidb.entities import render_path_input_value
    from scidb.source_edit import render_path_input

    from scistack_gui.matlab_parser import render_matlab_path_input
    from scistack_gui.services.target_file_service import update_declaration

    return update_declaration(
        "path_input",
        name,
        python_expr=render_path_input(
            template, root_folder, alternate_templates, name=name
        ),
        matlab_expr=render_matlab_path_input(
            template, root_folder, alternate_templates, name=name
        ),
        toml_expr=render_path_input_value(
            template, root_folder, alternate_templates
        ),
    )


