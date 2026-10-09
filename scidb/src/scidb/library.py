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
            keys, errors = parse_library_table(tomllib.loads(entities.read_text("utf-8")))
            info.schema_keys = keys
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
