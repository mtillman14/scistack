"""
A SciStack LIBRARY: what an installed package offers beyond its code
(portability Stage 10, ``.claude/plan-portability.md``; decision records
D-2026-10-08-10/11).

A library is an ordinary Python package listed under ``packages`` in the
project's ``scistack.toml`` (``scidb.names.library_packages``). Inside it::

    <lib>/
        __init__.py, *.py            Python functions        -> named lib.fn
        scistack_entities.toml       its Variables, plus [library] schema_keys
        pipelines/<name>.json        one shared pipeline per file (below)
        matlab/+<lib>/*.m            MATLAB functions        -> named lib.fn

This module is the one owner of that layout and of the PIPELINE DOCUMENT
format, and reads an installed library without importing anything but the
package itself (``importlib.resources``) and without constructing its
Variables (``entities.parse_library_table``).

The pipeline document is language-neutral: a canvas, as the GUI's
``canvas_snapshot`` captures it, plus the pipelines it spans. Python,
MATLAB and mixed submodules ship the same way. It carries no location
selections: a library never names a project's subjects. The project picks
its locations and schema when it PLACES the pipeline (the placement
binding's ``iterate`` / ``key_map``).

``definition_hash`` is what the GUI compares at load to decide whether a
seeded library pipeline must be re-synced.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from scistacklog import Log

PIPELINES_DIR = "pipelines"
MATLAB_DIR = "matlab"
DOCUMENT_FORMAT = "scistack-library-pipeline"
#: Bumped on any incompatible change; a mismatch is refused (beta: no migration).
DOCUMENT_VERSION = 1


class LibraryError(ValueError):
    """A library document that cannot be read, with the reason."""


@dataclass
class LibraryPipeline:
    library: str
    name: str
    document: dict
    definition_hash: str


@dataclass
class LibraryInfo:
    name: str
    #: ``[library] schema_keys``, or ``None`` when the library declares none.
    schema_keys: "list[str] | None" = None
    pipelines: list[LibraryPipeline] = field(default_factory=list)
    #: The folder to put on the MATLAB path (holds ``+<lib>/``), or ``None``.
    matlab_dir: "Path | None" = None
    #: Parameters its pipelines need, with DEFAULT values (never registered).
    parameter_defaults: dict = field(default_factory=dict)
    #: PathInputs its pipelines need, with DEFAULT templates (never registered).
    path_input_defaults: dict = field(default_factory=dict)
    #: The Variables its entities file declares.
    variables: list[str] = field(default_factory=list)
    #: ``[library] derived_from``: the libraries it was copied from, newest
    #: first (A8: where it came from, never who).
    derived_from: list[str] = field(default_factory=list)
    #: Its distribution's requirements, as written (:func:`library_dependencies`).
    dependencies: list[str] = field(default_factory=list)
    #: The package folder (``.../src/<lib>/``), when it is a plain folder.
    package_dir: "Path | None" = None
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# The pipeline document
# ---------------------------------------------------------------------------


def make_document(name: str, root: str, pipelines: list[dict], canvas: dict) -> dict:
    """A pipeline document: *root* is the shared pipeline's id inside the
    document, *pipelines* every pipeline the canvas spans
    (``[{"pipeline_id", "name"}]``, the root and its nested submodules), and
    *canvas* a ``canvas_snapshot.CanvasSnapshot.to_dict()``."""
    doc = {
        "format": DOCUMENT_FORMAT,
        "format_version": DOCUMENT_VERSION,
        "name": name,
        "root": root,
        "pipelines": [{"pipeline_id": p["pipeline_id"], "name": p["name"]} for p in pipelines],
        "canvas": canvas,
    }
    problems = validate_document(doc)
    if problems:
        raise LibraryError(f"pipeline document {name!r}: {'; '.join(problems)}")
    return doc


def validate_document(doc: dict) -> list[str]:
    """What is wrong with *doc*, or ``[]``."""
    if not isinstance(doc, dict):
        return ["not a JSON object"]
    problems = []
    if doc.get("format") != DOCUMENT_FORMAT:
        problems.append(f"format is {doc.get('format')!r}, expected {DOCUMENT_FORMAT!r}")
    if doc.get("format_version") != DOCUMENT_VERSION:
        problems.append(
            f"format_version {doc.get('format_version')!r}; this SciStack reads "
            f"{DOCUMENT_VERSION} (re-share the library with an up-to-date SciStack)"
        )
    if not isinstance(doc.get("name"), str) or not doc.get("name"):
        problems.append("no name")
    ids = [p.get("pipeline_id") for p in doc.get("pipelines") or [] if isinstance(p, dict)]
    if doc.get("root") not in ids:
        problems.append(f"root {doc.get('root')!r} is not among its pipelines {ids}")
    if not isinstance(doc.get("canvas"), dict):
        problems.append("no canvas")
    return problems


def document_bytes(doc: dict) -> bytes:
    """The canonical file contents (sorted keys, so the hash is stable)."""
    return (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode("utf-8")


def definition_hash(doc: dict) -> str:
    """SHA-256 of the canonical document."""
    return hashlib.sha256(json.dumps(doc, sort_keys=True).encode("utf-8")).hexdigest()


def read_document(data: bytes, where: str) -> dict:
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise LibraryError(f"{where}: not JSON ({e})") from e
    problems = validate_document(doc)
    if problems:
        raise LibraryError(f"{where}: {'; '.join(problems)}")
    return doc


# ---------------------------------------------------------------------------
# Reading an installed library
# ---------------------------------------------------------------------------


def matlab_dir(package: str) -> "Path | None":
    """*package*'s ``matlab/`` folder (the one to put on MATLAB's path; it
    holds ``+<lib>/``), or ``None``. Located with ``find_spec``, which does
    NOT run the package: the GUI's config loader asks before any code is
    imported."""
    import importlib.util

    try:
        spec = importlib.util.find_spec(package)
    except (ImportError, ValueError) as e:
        Log.debug(f"[library] {package}: no spec ({e})")
        return None
    for location in (spec.submodule_search_locations or []) if spec else []:
        candidate = Path(location) / MATLAB_DIR
        if candidate.is_dir():
            return candidate
    return None


def _defaults(data: dict) -> "tuple[dict, dict]":
    """The raw Parameter values and PathInput templates a library's entities
    file declares -- read as DATA, never constructed (they are defaults a
    project may copy, portability Stage 10b)."""
    from .entities import PARAMETERS, PATH_INPUTS

    params = {}
    for name, raw in (data.get(PARAMETERS) or {}).items():
        params[name] = list(raw) if isinstance(raw, list) else [raw]
    pis = {}
    for name, raw in (data.get(PATH_INPUTS) or {}).items():
        arms = raw if isinstance(raw, list) else [raw]
        pis[name] = [a if isinstance(a, str) else str(a.get("template", "")) for a in arms]
    return params, pis


def read_library(package: str) -> LibraryInfo:
    """Everything *package* offers as a library. A broken document or table
    is recorded in ``errors`` and skipped, never raised: one bad file must
    not hide the rest of the library."""
    import importlib.resources

    from .entities import DEFAULT_ENTITIES_FILENAME, parse_derived_from, parse_library_table

    info = LibraryInfo(name=package)
    try:
        top = importlib.resources.files(package)
    except (ModuleNotFoundError, TypeError) as e:
        info.errors.append(f"{package} is not importable: {e}")
        Log.warn(f"[library] {package}: not importable ({e})")
        return info

    entities = top / DEFAULT_ENTITIES_FILENAME
    if entities.is_file():
        try:
            import tomllib
        except ModuleNotFoundError:  # pragma: no cover - Python 3.10
            import tomli as tomllib
        try:
            data = tomllib.loads(entities.read_text("utf-8"))
            keys, errors = parse_library_table(data)
            info.schema_keys = keys
            info.parameter_defaults, info.path_input_defaults = _defaults(data)
            info.variables = [v for v in data.get("variables") or [] if isinstance(v, str)]
            info.derived_from = parse_derived_from(data)[0]
            info.errors += [f"{DEFAULT_ENTITIES_FILENAME}: {e}" for e in errors]
        except Exception as e:  # invalid TOML: reported, never fatal
            info.errors.append(f"{DEFAULT_ENTITIES_FILENAME}: {e}")

    folder = top / PIPELINES_DIR
    if folder.is_dir():
        for entry in sorted(folder.iterdir(), key=lambda p: p.name):
            if not entry.name.endswith(".json"):
                continue
            where = f"{package}/{PIPELINES_DIR}/{entry.name}"
            try:
                doc = read_document(entry.read_bytes(), where)
            except LibraryError as e:
                info.errors.append(str(e))
                continue
            info.pipelines.append(
                LibraryPipeline(package, doc["name"], doc, definition_hash(doc))
            )
        names = [p.name for p in info.pipelines]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            info.errors.append(f"pipeline name(s) shipped twice: {dupes}; only the first is used")
            seen: set[str] = set()
            info.pipelines = [p for p in info.pipelines if not (p.name in seen or seen.add(p.name))]

    info.matlab_dir = matlab_dir(package)
    try:
        folder = Path(str(top))
        info.package_dir = folder if folder.is_dir() else None
    except (TypeError, ValueError):  # a zip / namespace reader
        info.package_dir = None
    info.dependencies = library_dependencies(package, info.package_dir)

    Log.info(
        f"[library] {package}: schema {info.schema_keys}, "
        f"{len(info.pipelines)} pipeline(s) {[p.name for p in info.pipelines]}, "
        f"MATLAB {'yes' if info.matlab_dir else 'no'}, derived from "
        f"{info.derived_from or '-'}, deps {info.dependencies or '-'}, "
        f"{len(info.errors)} error(s)"
    )
    for e in info.errors:
        Log.warn(f"[library] {package}: {e}")
    return info


def _requirement_name(requirement: str) -> str:
    """The distribution name a requirement string names
    (``"numpy>=1.24"`` -> ``"numpy"``)."""
    import re

    m = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", requirement)
    return m.group(1) if m else requirement.strip()


def library_dependencies(package: str, package_dir: "Path | None" = None) -> list[str]:
    """The requirements of *package*'s distribution, as written: from the
    ``pyproject.toml`` two levels above its folder (the one library layout,
    ``<folder>/src/<lib>/``) when that file names *package*, else from the
    installed distribution's metadata. ``[]`` when neither is found."""
    if package_dir is not None:
        project = _read_pyproject(Path(package_dir).parent.parent)
        if project.get("name") == package:
            return [str(d) for d in project.get("dependencies") or []]
    import importlib.metadata

    try:
        return list(importlib.metadata.requires(package) or [])
    except importlib.metadata.PackageNotFoundError:
        return []


# ---------------------------------------------------------------------------
# Writing a library (Share as library / Make my own copy; plan-library-fork-chain O5)
# ---------------------------------------------------------------------------

_LIB_PYPROJECT = """\
[project]
name = "{name}"
version = "{version}"
description = "A SciStack library: shared pipelines and the code they run."
requires-python = ">=3.10"
dependencies = [
{dependencies}]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/{name}"]
"""

#: First line of the ``__init__.py`` this module generates; a file that
#: starts with it is ours to regenerate whole.
_INIT_MARKER = "# Generated by SciStack (scidb.library)."
#: Starts the re-export block (A9: ``from <lib> import fn``). Lines after it
#: are regenerated; anything before it in a hand-written ``__init__.py`` is kept.
_REEXPORT_MARKER = "# --- re-exports: `from {name} import fn` (rewritten on every share) ---"
_REEXPORT_PREFIX = "# --- re-exports:"

_LIB_INIT = '''{marker}
"""{name}: a SciStack library.

Its shared pipelines are in pipelines/ (one document each), its Variables
and the Parameters / PathInputs its pipelines need (with defaults) in
scistack_entities.toml, and its MATLAB functions, if any, in
matlab/+{name}/. List it under `packages` in a project's scistack.toml to
use it there.
"""
'''


@dataclass
class CreateReport:
    root: Path
    package: str
    created: list[Path] = field(default_factory=list)
    #: Files an ``"update"`` write replaced (documents, declarations, ...).
    updated: list[Path] = field(default_factory=list)


def entities_text(
    variables: list[str],
    parameters: "dict[str, list]",
    path_inputs: "dict[str, list[str]]",
    schema_keys: "list[str] | None",
    derived_from: "list[str]" = (),
) -> str:
    """A library's entities file: its Variables, the Parameters / PathInputs
    its pipelines need WITH DEFAULTS (never registered into a project:
    portability Stage 5), and the ``[library]`` table: schema keys, and
    ``derived_from`` (A8: the libraries it was copied from, newest first).
    PathInputs carry templates only -- a root folder is one project's data
    layout."""
    from .entities import (
        LIBRARY,
        PARAMETERS,
        PATH_INPUTS,
        VARIABLES,
        render_parameter_value,
        render_path_input_value,
        render_value,
    )

    lines = [
        "# Declared by a SciStack library. Parameters and PathInputs here are",
        "# DEFAULTS a project may copy into its own entities file; they are",
        "# never registered into a project from here.",
        f"{VARIABLES} = {render_value(sorted(set(variables)))}",
        "",
        f"[{PARAMETERS}]",
    ]
    for name in sorted(parameters):
        lines.append(f"{name} = {render_parameter_value(list(parameters[name]))}")
    lines += ["", f"[{PATH_INPUTS}]"]
    for name in sorted(path_inputs):
        templates = list(path_inputs[name]) or [""]
        lines.append(
            f"{name} = "
            + render_path_input_value(templates[0], None, [{"template": t} for t in templates[1:]])
        )
    lines += ["", f"[{LIBRARY}]"]
    if schema_keys:
        lines.append(f"schema_keys = {render_value(list(schema_keys))}")
    if derived_from:
        lines.append(f"derived_from = {render_value(list(derived_from))}")
    return "\n".join(lines) + "\n"


def init_text(name: str, reexports: "dict[str, str]", existing: "str | None" = None) -> str:
    """The package's ``__init__.py`` (A9): a docstring, then one
    ``from .<module> import <fn>`` per pipeline function, so a code user
    writes ``from <lib> import fn``. Relative: the file never names the
    package, so it survives a copy under a new name.

    *existing* is the current text: one this module generated is
    regenerated whole; a hand-written one keeps everything above the
    re-export block."""
    if existing is None or existing.startswith(_INIT_MARKER):
        head = _LIB_INIT.format(marker=_INIT_MARKER, name=name)
    else:
        head = existing.split(_REEXPORT_PREFIX)[0].rstrip("\n") + "\n\n"
    lines = [head.rstrip("\n"), "", _REEXPORT_MARKER.format(name=name)]
    for fn, module in sorted(reexports.items()):
        lines.append(f"from .{module} import {fn}  # noqa: F401")
    return "\n".join(lines) + "\n"


def reexports_of(init: str) -> "dict[str, str]":
    """``{fn: module}`` from the re-export block of an ``__init__.py`` this
    module wrote (``{}`` when it has none)."""
    import re

    block = init.split(_REEXPORT_PREFIX, 1)
    if len(block) < 2:
        return {}
    return {
        m.group(2): m.group(1)
        for m in re.finditer(r"^from \.([\w.]+) import (\w+)", block[1], re.M)
    }


def write_library(
    dest: "Path | str",
    name: str,
    *,
    files: "dict[str, bytes]",
    documents: "list[dict]",
    variables: "list[str]" = (),
    parameters: "dict[str, list] | None" = None,
    path_inputs: "dict[str, list[str]] | None" = None,
    schema_keys: "list[str] | None" = None,
    dependencies: "list[str]" = (),
    derived_from: "list[str]" = (),
    reexports: "dict[str, str] | None" = None,
    existing: str = "create",
    version: str = "0.1.0",
) -> CreateReport:
    """Write library *name* at *dest* -- the one owner of its layout.

    *files* are the package's code, keyed by path INSIDE the package
    (``"filters.py"``, ``"matlab/+lib/lowpass.m"``); *documents* the pipeline
    documents (:func:`make_document`); *reexports* ``{fn: module}`` for the
    generated ``__init__.py`` (A9). An ``__init__.py`` in *files* that this
    module did not generate keeps its own text above the re-export block.

    ``existing="create"`` refuses a non-empty *dest* (never overwrites).
    ``existing="update"`` (publishing to a library the project authors)
    requires the library to be there: *files* are ADDED -- one that would
    replace a file with different content is refused, so a code edit made in
    the library is never lost -- *documents* replace their pipelines, and
    the declarations, dependencies and re-exports are MERGED with what the
    library has; the pyproject keeps its version. An invalid *name* or a
    path that would leave the package is refused in both modes. Nothing is
    written when anything is refused."""
    from .entities import DEFAULT_ENTITIES_FILENAME
    from .project import validate_project_name

    if existing not in ("create", "update"):
        raise ValueError(f"existing must be 'create' or 'update', not {existing!r}")
    validate_project_name(name)
    dest = Path(dest).resolve()
    pkg = dest / "src" / name
    if existing == "create" and dest.exists() and any(dest.iterdir()):
        raise LibraryError(f"{dest} is not empty; choose a new or empty folder")
    if existing == "update" and not pkg.is_dir():
        raise LibraryError(f"{pkg} does not exist; there is no library {name!r} to update")
    for doc in documents:
        problems = validate_document(doc)
        if problems:
            raise LibraryError(f"pipeline document {doc.get('name')!r}: {'; '.join(problems)}")
    names = [d["name"] for d in documents]
    if len(set(names)) != len(names):
        raise LibraryError(f"two pipeline documents share a name: {names}")
    for rel in files:
        parts = rel.replace("\\", "/").split("/")
        if rel.startswith("/") or ".." in parts or (len(rel) > 1 and rel[1] == ":"):
            raise LibraryError(f"unsafe path for a library file: {rel!r}")

    variables = set(variables)
    parameters = dict(parameters or {})
    path_inputs = dict(path_inputs or {})
    dependencies = set(dependencies)
    derived = list(derived_from)
    exports = dict(reexports or {})
    given_init = files.get("__init__.py")
    init_existing = given_init.decode("utf-8") if given_init is not None else None
    if existing == "update":
        clashes = sorted(
            rel
            for rel, data in files.items()
            if rel != "__init__.py" and (pkg / rel).is_file() and (pkg / rel).read_bytes() != data
        )
        if clashes:
            raise LibraryError(
                f"{name} already has {clashes} with different content; nothing was "
                "written (rename the module in the project, or edit it in the library)"
            )
        old = _read_folder_declarations(pkg)
        variables |= set(old["variables"])
        parameters = {**old["parameters"], **parameters}
        path_inputs = {**old["path_inputs"], **path_inputs}
        schema_keys = schema_keys or old["schema_keys"]
        derived = derived or old["derived_from"]
        old_project = _read_pyproject(dest)
        dependencies |= {str(d) for d in old_project.get("dependencies") or []}
        version = str(old_project.get("version") or version)
        if (pkg / "__init__.py").is_file():
            init_existing = (pkg / "__init__.py").read_text("utf-8")
            exports = {**reexports_of(init_existing), **exports}

    report = CreateReport(root=dest, package=name)

    def write(path: Path, data: bytes) -> None:
        if path.exists() and path.read_bytes() == data:
            return
        (report.updated if path.exists() else report.created).append(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    deps = "".join(f'    "{d}",\n' for d in sorted(dependencies))
    write(dest / "pyproject.toml",
          _LIB_PYPROJECT.format(name=name, version=version, dependencies=deps).encode())
    for rel, data in sorted(files.items()):
        if rel != "__init__.py":
            write(pkg / rel, data)
    write(pkg / "__init__.py", init_text(name, exports, init_existing).encode())
    for doc in documents:
        write(pkg / PIPELINES_DIR / f"{doc['name']}.json", document_bytes(doc))
    write(
        pkg / DEFAULT_ENTITIES_FILENAME,
        entities_text(sorted(variables), parameters, path_inputs, schema_keys, derived).encode(),
    )
    Log.info(
        f"[library] write_library {name} ({existing}) at {dest}: "
        f"{len(report.created)} file(s) created, {len(report.updated)} updated, "
        f"{len(documents)} pipeline(s) {names}, {len(variables)} variable(s), "
        f"{len(parameters)} parameter default(s), {len(path_inputs)} PathInput "
        f"default(s), deps {sorted(dependencies)}, derived from {derived or '-'}, "
        f"re-exports {sorted(exports)}"
    )
    return report


def create_library(dest: "Path | str", name: str, **kwargs) -> CreateReport:
    """:func:`write_library` in its ``"create"`` mode."""
    return write_library(dest, name, existing="create", **kwargs)


def _read_pyproject(dest: Path) -> dict:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10
        import tomli as tomllib
    path = Path(dest) / "pyproject.toml"
    if not path.is_file():
        return {}
    return tomllib.loads(path.read_text("utf-8")).get("project") or {}


def _read_folder_declarations(pkg: Path) -> dict:
    """The declarations a library FOLDER's entities file holds, read as data
    by path (like :func:`read_library`): the update mode must not depend on
    what this process happens to have imported."""
    from .entities import DEFAULT_ENTITIES_FILENAME, parse_derived_from, parse_library_table

    out = {"variables": [], "parameters": {}, "path_inputs": {}, "schema_keys": None,
           "derived_from": []}
    path = Path(pkg) / DEFAULT_ENTITIES_FILENAME
    if not path.is_file():
        return out
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10
        import tomli as tomllib
    data = tomllib.loads(path.read_text("utf-8"))
    out["variables"] = [v for v in data.get("variables") or [] if isinstance(v, str)]
    out["parameters"], out["path_inputs"] = _defaults(data)
    out["schema_keys"] = parse_library_table(data)[0]
    out["derived_from"] = parse_derived_from(data)[0]
    return out
