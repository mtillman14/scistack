"""
Make my own copy: turn a library the project uses into project code
(portability Stage 10c, D-2026-10-08-11; ``.claude/plan-portability.md``).

All-or-nothing per library (user decision): the installed library's source
is copied into the project and EVERY placement of it switches to the copy.

* Python: the package goes to ``src/<pkg>/<lib>/`` (its imports of
  ``<lib>`` rewritten to ``<pkg>.<lib>``: ``code_rewrite``, the one owner);
  ``copied_libraries`` in scistack.toml makes ``scidb.names`` keep its
  functions named ``<lib>.fn``, so history stays current.
* MATLAB: ``matlab/+<lib>/`` goes to ``<root>/matlab/+<lib>/`` (added to
  the MATLAB sources); the ``+<lib>`` folder keeps the names natively.
* Its Variables are declared in the project's own entities file (they were
  the library's); its Parameter / PathInput defaults are not (the project
  already declares what it uses: Stage 10b).
* Its library-owned pipelines are released: ordinary, editable pipelines,
  with the same ids, content, placements and bindings.
* The library leaves ``packages``; it stays installed (SciStack never
  uninstalls anything).
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

#: Never copied: build/cache noise, and what the copy re-homes elsewhere.
_SKIP_DIRS = {"__pycache__", "pipelines", "matlab"}


@dataclass
class CopyReport:
    library: str
    python_dir: "str | None" = None
    matlab_dir: "str | None" = None
    files: list[str] = field(default_factory=list)
    released_pipelines: list[str] = field(default_factory=list)
    declared_variables: list[str] = field(default_factory=list)
    rewritten_imports: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


class CopyRefused(ValueError):
    """The library cannot be copied as asked; the message says why."""


def _walk(traversable, rel: str = ""):
    for entry in sorted(traversable.iterdir(), key=lambda e: e.name):
        path = f"{rel}{entry.name}"
        if entry.is_dir():
            if entry.name in _SKIP_DIRS and not rel:
                continue
            if entry.name == "__pycache__":
                continue
            yield from _walk(entry, path + "/")
        elif not entry.name.endswith((".pyc", ".pyo")):
            yield path, entry


def make_own_copy(db, library: str) -> CopyReport:
    import importlib.resources

    from scistack_gui import history

    from scidb.entities import DEFAULT_ENTITIES_FILENAME
    from scidb.library import matlab_dir, read_library
    from scidb.names import library_packages
    from scifor.discovery import own_package_dir
    from scifor.pathinput import project_root

    from scistack_gui import pipeline_store as ps
    from scistack_gui.code_rewrite import rewrite_imports
    from scistack_gui.config import add_path, mark_library_copied
    from scistack_gui.db import get_db_path

    library = library.strip()
    root = project_root()
    if library not in library_packages():
        raise CopyRefused(f"'{library}' is not a library of this project (not under packages)")
    own = own_package_dir(root)
    if own is None:
        raise CopyRefused(
            "this project has no package of its own (src/<name>/) to copy the library into; "
            "run `scistack init` in the project folder first"
        )
    own_name, own_dir = own
    py_dest = Path(own_dir) / library
    if py_dest.exists():
        raise CopyRefused(f"{py_dest} already exists; the project already has a '{library}' subpackage")
    m_src = matlab_dir(library)
    m_dest = root / "matlab" / f"+{library}"
    if m_src is not None and (m_src / f"+{library}").is_dir() and m_dest.exists():
        raise CopyRefused(f"{m_dest} already exists")

    report = CopyReport(library=library)
    info = read_library(library)

    # Python: the package, imports re-homed under the project's package.
    def resolve(name: str):
        if name == library or name.startswith(library + "."):
            return f"{own_name}.{name}"
        return None

    top = importlib.resources.files(library)
    for rel, entry in _walk(top):
        if rel == DEFAULT_ENTITIES_FILENAME:
            continue  # its Variables are declared in the project's own file below
        target = py_dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        history.note_write(target)
        if rel.endswith(".py"):
            result = rewrite_imports(entry.read_text(encoding="utf-8"), resolve, where=f"{library}/{rel}")
            target.write_text(result.text, encoding="utf-8")
            report.rewritten_imports += result.changes
            report.warnings += result.warnings
        else:
            target.write_bytes(entry.read_bytes())
        report.files.append(str(target.relative_to(root)))
    if not (py_dest / "__init__.py").exists():
        history.note_write(py_dest / "__init__.py")
        (py_dest / "__init__.py").write_text("")
        report.files.append(str((py_dest / "__init__.py").relative_to(root)))
    report.python_dir = str(py_dest)

    # MATLAB: the +<lib> folder keeps the names natively.
    if m_src is not None and (m_src / f"+{library}").is_dir():
        source = m_src / f"+{library}"
        for src_file in sorted(p for p in source.rglob("*") if p.is_file()):
            target = m_dest / src_file.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            history.note_write(target)
            target.write_bytes(src_file.read_bytes())
            report.files.append(str(target.relative_to(root)))
        report.matlab_dir = str(m_dest)
        add_path(get_db_path(), root / "matlab")

    # Config: unlisted, marked copied (names stay lib.fn).
    mark_library_copied(None, library, project=root)

    # Its pipelines become the project's own.
    for row in ps.list_library_pipelines(db):
        if row["library"] == library:
            ps.release_library_owner(db, row["pipeline_id"])
            report.released_pipelines.append(row["name"])

    # Reload: the copy is discovered as project code; then declare the
    # library's Variables the project does not have yet.
    from scidb import BaseVariable

    from scistack_gui.services.registry_reload_service import reload_registries_from_disk
    from scistack_gui.services.variable_service import create_variable

    library_variables = _library_variables(info, top, DEFAULT_ENTITIES_FILENAME)
    reload_registries_from_disk(get_db_path())
    for name in library_variables:
        if name in BaseVariable._all_subclasses:
            continue
        result = create_variable(name)
        if result.get("ok"):
            report.declared_variables.append(name)
        else:
            report.warnings.append(f"Variable {name}: {result.get('error')}")

    logger.info(
        "[library_copy] %s copied into %s: %d file(s), MATLAB %s, released %s, declared %s, "
        "%d import(s) rewritten, %d warning(s)",
        library,
        py_dest,
        len(report.files),
        report.matlab_dir or "-",
        report.released_pipelines,
        report.declared_variables,
        len(report.rewritten_imports),
        len(report.warnings),
    )
    for w in report.warnings:
        logger.warning("[library_copy] %s", w)
    return report


def _library_variables(info, top, filename: str) -> list[str]:
    """The Variables the library's entities file declares (read as data)."""
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10
        import tomli as tomllib

    entities = top / filename
    if not entities.is_file():
        return []
    try:
        data = tomllib.loads(entities.read_text("utf-8"))
    except Exception as e:
        info.errors.append(f"{filename}: {e}")
        return []
    return [v for v in data.get("variables") or [] if isinstance(v, str)]
