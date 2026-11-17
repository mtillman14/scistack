"""
Renaming schema keys everywhere a project NAMES them, in place.

The key tables themselves change in ``scidb.store.schema_keys_change``. This
module is the other half: every place outside those tables that refers to a
key by name, renamed through the same per-shape remaps a bundle import uses
(``scidb.project.schema_map.KeyMap``), so there is no second list of "places
that mention schema keys" (``docs/claude/changing-schema-keys.md``).

=================================  ==========================================
Place                              Who renames it
=================================  ==========================================
``scistack.toml`` [schema_keys],   this module (``KeyMap.config_tables``)
[aliases], [colors]
entities file ``[path_inputs]``    this module (``KeyMap.path_input_arm``)
(template, aliases, key_regex)
entities file ``[library]          this module (``KeyMap.key_list``)
schema_keys``
canvas, saved plots, saved stats   each bundle section's ``rename_keys(ctx)``
                                   (the providers the caller passes)
PathInputs declared in Python      nobody: code is never edited; the caller
                                   reports them
=================================  ==========================================

A RENAMED key's references follow it. A DELETED key is *retired*: every
reference is kept as written and flagged, never dropped
(feedback_never_drop_data).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from scistacklog import Log

from scidb.project.schema_map import KeyMap, MapReport


@dataclass
class KeyRenameContext:
    """What a section's ``rename_keys`` hook receives. The attribute names
    match ``scidb.project.bundle.ImportContext`` (``db``, ``root``,
    ``key_map``, ``map_report``) so a section remaps with the same code on
    both routes."""

    db: Any
    root: Path
    key_map: KeyMap
    map_report: MapReport


def rename_key_references(
    db,
    root: "Path | str",
    renames: "dict[str, str] | None",
    retired: "Sequence[str]" = (),
    *,
    providers: Sequence = (),
) -> dict:
    """Rename *renames* (old -> new) in every reference outside the key
    tables, and flag references to *retired* keys. Returns
    ``{place: what changed, ..., "dropped": [], "flagged": [[where, why]]}``.

    *providers* are bundle sections (``SectionProvider``); each one with a
    ``rename_keys(ctx)`` method renames its own data in place. Nothing is
    done (and nothing logged beyond one line) when there is nothing to
    rename or retire.
    """
    started = time.perf_counter()
    kmap = KeyMap.renaming(renames, retired)
    out: dict = {}
    if kmap.is_identity:
        Log.info("[key_rename] nothing to rename or retire")
        return {"dropped": [], "flagged": []}
    root = Path(root)
    report = MapReport()
    ctx = KeyRenameContext(db=db, root=root, key_map=kmap, map_report=report)

    # Each place on its own: one that fails is flagged and the others still
    # run, so a single malformed entry never leaves the rest of the project
    # naming the old key, and never loses what the others reported.
    places = [
        ("config", lambda: _rename_in_config(root, kmap, report)),
        ("entities", lambda: _rename_in_entities(root, kmap, report)),
    ]
    for provider in providers:
        hook = getattr(provider, "rename_keys", None)
        if hook is not None:
            places.append((provider.name, lambda hook=hook: hook(ctx) or {}))
    for name, run in places:
        t0 = time.perf_counter()
        try:
            out[name] = run()
        except Exception as exc:
            Log.warn(f"[key_rename] {name}: renaming failed: {exc!r}")
            report.flagged.append((name, f"could not be renamed ({exc}); it still names the old key"))
            out[name] = {"error": str(exc)}
            continue
        Log.info(
            f"[key_rename] {name}: {out[name]} ({(time.perf_counter() - t0) * 1000:.0f} ms)"
        )

    out.update(report.to_dict())
    for where, why in report.flagged:
        Log.warn(f"[key_rename] {where}: {why}")
    Log.info(
        f"[key_rename] {kmap.describe()} in {root}: {out} "
        f"({(time.perf_counter() - started) * 1000:.0f} ms)"
    )
    return out


def _rename_in_config(root: Path, kmap: KeyMap, report: MapReport) -> list[str]:
    """``scistack.toml``'s key-named tables renamed; the tables changed.
    The file is written through ``scifor.project.config_file.write`` (which
    tells every cached reader) only when something changed."""
    from scifor.project import config_file
    from scifor.project.config import config_path_at, read_scistack_section

    path = config_path_at(root)
    if not path.is_file():
        Log.info(f"[key_rename] no {path.name} at {root}; no config to rename")
        return []
    section = read_scistack_section(path)
    if section is None:
        report.flagged.append((str(path), "could not be parsed; its key names were not renamed"))
        return []
    new = kmap.config_tables(section, report)
    changed = [name for name in new if new.get(name) != section.get(name)]
    if changed:
        config_file.write(path, new)
        Log.info(f"[key_rename] {path}: renamed keys in {changed}")
    return changed


def _rename_in_entities(root: Path, kmap: KeyMap, report: MapReport) -> list[str]:
    """The entities file's PathInputs (and ``[library] schema_keys``)
    renamed in place, comments and neighbours kept; the entries changed."""
    from scifor.project.entities_file import LIBRARY, upsert_entry

    from scidb.project.bundle import _declared_path_inputs, _map_path_inputs, _toml_loads

    found = _declared_path_inputs(root)
    if found is None:
        Log.info(f"[key_rename] no entities file at {root}; no PathInputs to rename")
        return []
    path, text, _declared = found

    def remap(name: str, arms: "list[dict]") -> "list[dict]":
        return [kmap.path_input_arm(a, f"PathInput {name}", report) for a in arms]

    new_text, changed = _map_path_inputs(text, remap)

    library = (_toml_loads(new_text).get(LIBRARY) or {})
    keys = library.get("schema_keys") if isinstance(library, dict) else None
    if isinstance(keys, list):
        new_keys = kmap.key_list(keys, f"{path.name} [{LIBRARY}] schema_keys", report)
        if new_keys != keys:
            rendered = "[" + ", ".join(f'"{k}"' for k in new_keys) + "]"
            new_text = upsert_entry(new_text, LIBRARY, "schema_keys", rendered)
            changed.append(f"[{LIBRARY}] schema_keys")

    if new_text != text:
        path.write_text(new_text, encoding="utf-8")
        Log.info(f"[key_rename] {path}: renamed keys in {changed}")
    return changed
