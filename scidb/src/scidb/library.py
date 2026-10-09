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

    from .entities import DEFAULT_ENTITIES_FILENAME, parse_library_table

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

    Log.info(
        f"[library] {package}: schema {info.schema_keys}, "
        f"{len(info.pipelines)} pipeline(s) {[p.name for p in info.pipelines]}, "
        f"MATLAB {'yes' if info.matlab_dir else 'no'}, {len(info.errors)} error(s)"
    )
    for e in info.errors:
        Log.warn(f"[library] {package}: {e}")
    return info


# ---------------------------------------------------------------------------
# Writing a new library (Share as library, portability Stage 10b)
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

_LIB_INIT = '''"""{name}: a SciStack library.

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


def entities_text(
    variables: list[str],
    parameters: "dict[str, list]",
    path_inputs: "dict[str, list[str]]",
    schema_keys: "list[str] | None",
) -> str:
    """A library's entities file: its Variables, the Parameters / PathInputs
    its pipelines need WITH DEFAULTS (never registered into a project:
    portability Stage 5), and the ``[library]`` table. PathInputs carry
    templates only -- a root folder is one project's data layout."""
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
    return "\n".join(lines) + "\n"


def create_library(
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
    version: str = "0.1.0",
) -> CreateReport:
    """Write a new library package at *dest* -- the one owner of its layout.

    *files* are the package's code, keyed by path INSIDE the package
    (``"filters.py"``, ``"matlab/+lib/lowpass.m"``); *documents* the pipeline
    documents (:func:`make_document`). An ``__init__.py`` is written when
    *files* has none. Refuses a non-empty *dest* (never overwrites) and an
    invalid *name*; a path in *files* that would leave the package is
    refused too."""
    from .entities import DEFAULT_ENTITIES_FILENAME
    from .project import validate_project_name

    validate_project_name(name)
    dest = Path(dest).resolve()
    if dest.exists() and any(dest.iterdir()):
        raise LibraryError(f"{dest} is not empty; choose a new or empty folder")
    for doc in documents:
        problems = validate_document(doc)
        if problems:
            raise LibraryError(f"pipeline document {doc.get('name')!r}: {'; '.join(problems)}")
    names = [d["name"] for d in documents]
    if len(set(names)) != len(names):
        raise LibraryError(f"two pipeline documents share a name: {names}")

    pkg = dest / "src" / name
    report = CreateReport(root=dest, package=name)

    def write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        report.created.append(path)

    deps = "".join(f'    "{d}",\n' for d in sorted(set(dependencies)))
    write(dest / "pyproject.toml",
          _LIB_PYPROJECT.format(name=name, version=version, dependencies=deps).encode())
    for rel, data in sorted(files.items()):
        parts = rel.replace("\\", "/").split("/")
        if rel.startswith("/") or ".." in parts or (len(rel) > 1 and rel[1] == ":"):
            raise LibraryError(f"unsafe path for a library file: {rel!r}")
        write(pkg / rel, data)
    if "__init__.py" not in files:
        write(pkg / "__init__.py", _LIB_INIT.format(name=name).encode())
    for doc in documents:
        write(pkg / PIPELINES_DIR / f"{doc['name']}.json", document_bytes(doc))
    write(
        pkg / DEFAULT_ENTITIES_FILENAME,
        entities_text(list(variables), parameters or {}, path_inputs or {}, schema_keys).encode(),
    )
    Log.info(
        f"[library] create_library {name} at {dest}: {len(report.created)} file(s), "
        f"{len(documents)} pipeline(s), {len(list(variables))} variable(s), "
        f"{len(parameters or {})} parameter default(s), {len(path_inputs or {})} PathInput "
        f"default(s), deps {sorted(set(dependencies))}"
    )
    return report
