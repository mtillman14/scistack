"""
The ``.scistack`` bundle: exporting and importing a whole project.

Plan ``.claude/plan-portability.md`` Stage 4; design
``docs/claude/portability.md``. A bundle is an ordinary zip with a different
extension::

    manifest.json        format, versions, project, options, sections + hashes
    config/scistack.toml the project config (this module's own section)
    <section>/...        one folder per section a provider wrote

**One owner for the format and the defaults.** :class:`ExportOptions` holds
the defaults (history on, derived data off, dependency wheels off); a CLI flag
or a GUI checkbox reads its default from here, never restates it. The
manifest, the zip layout and the hash check live only here.

**Sections are providers, passed in explicitly.** scidb never imports the GUI
or the plotting packages: the front end (CLI, GUI) passes the providers it
has -- the GUI's canvas section, the saved-plots section -- to
:func:`export_project` / :func:`import_project`. A provider is any object with
``name``, ``export(ctx) -> {relpath: bytes}`` and ``import_(ctx, files) ->
report``. The manifest lists only the sections actually written, so a bundle
never claims history or data it does not carry.

**Import makes a NEW project.** Into an existing project would be merging
(live collaboration, out of scope). The target folder may exist but must hold
no config and no database. Order: config section written, then
``scidb.project.init_project`` (fills in whatever the bundle did not carry),
then the database is created through the front end's opener (the GUI's
``headless.open_for_import``, which never runs the bundle's code), then each
provider imports.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

from scistacklog import Log

#: Bumped on any incompatible change; a mismatch is refused (beta: no migration).
FORMAT_VERSION = 1

EXTENSION = ".scistack"
MANIFEST = "manifest.json"
CONFIG_SECTION = "config"
ENV_SECTION = "env"
ENV_FILE = "environment.json"

#: A provider's ``phase``: "files" sections are written before the project is
#: initialised and its database opened (the code: the new project's
#: ``src/<pkg>/`` must BE the exporter's); "database" sections (the default)
#: after, with ``ctx.db`` open.
PHASE_FILES = "files"
PHASE_DATABASE = "database"


class BundleError(ValueError):
    """A bundle that cannot be written or read, with the reason."""


@dataclass(frozen=True)
class ExportOptions:
    """What an export includes. THE defaults (decided 2026-10-08): history
    is small and is what a reproduction check compares against; derived data
    can be large, so the user opts in; dependency wheels are for archives."""

    include_history: bool = True
    include_data: bool = False
    include_wheelhouse: bool = False

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: "dict | None") -> "ExportOptions":
        known = {f: (data or {})[f] for f in cls.__dataclass_fields__ if f in (data or {})}
        return cls(**known)


@dataclass
class ExportContext:
    root: Path
    db: Any
    options: ExportOptions
    package: "str | None"


@dataclass
class ImportContext:
    root: Path
    #: ``None`` in the "files" phase (the database is not open yet).
    db: Any
    db_path: Path
    schema_keys: list[str]
    manifest: dict


class SectionProvider(Protocol):
    name: str
    #: Optional; ``PHASE_DATABASE`` when absent.
    phase: str

    def export(self, ctx: ExportContext) -> "dict[str, bytes]": ...

    def import_(self, ctx: ImportContext, files: "dict[str, bytes]") -> dict: ...


@dataclass
class Bundle:
    """A read, hash-checked bundle."""

    manifest: dict
    #: ``{section: {relpath: bytes}}``
    sections: dict[str, dict[str, bytes]] = field(default_factory=dict)


@dataclass
class ImportReport:
    root: Path
    db_path: Path
    package: str
    schema_keys: list[str]
    created: list[Path] = field(default_factory=list)
    sections: dict[str, dict] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _scistack_version() -> str:
    from . import __version__

    return __version__


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def _config_section(root: Path) -> "dict[str, bytes]":
    from scifor.discovery import project_config_at

    config = project_config_at(root)
    if config is None:
        return {}
    return {"scistack.toml": config.read_bytes()}


def _env_section() -> "dict[str, bytes]":
    """What the project ran under: Python, platform, every installed
    distribution with its version. MATLAB is recorded only when a MATLAB
    engine is ALREADY loaded in this process -- an export never starts one."""
    import importlib.metadata
    import platform
    import sys

    dists = {}
    for dist in importlib.metadata.distributions():
        name = dist.metadata.get("Name")
        if name:
            dists[_canonical_dist(name)] = dist.version
    matlab = None
    if "matlab.engine" in sys.modules:
        try:
            matlab = getattr(sys.modules["matlab.engine"], "__version__", "loaded")
        except Exception:  # pragma: no cover - defensive
            matlab = "loaded"
    env = {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "matlab": matlab,
        "distributions": dict(sorted(dists.items())),
    }
    return {ENV_FILE: json.dumps(env, indent=2).encode("utf-8")}


def _canonical_dist(name: str) -> str:
    """A distribution name as PEP 503 compares it (``Foo_Bar`` -> ``foo-bar``)."""
    import re

    return re.sub(r"[-_.]+", "-", name).lower()


def _check_safe(bundle_path: Path, rel: str) -> None:
    """Refuse a member path that would land outside the target folder
    (absolute, a drive, or a ``..`` part): bundle paths become files on disk."""
    parts = rel.replace("\\", "/").split("/")
    if rel.startswith(("/", "\\")) or (len(rel) > 1 and rel[1] == ":") or ".." in parts:
        raise BundleError(f"{bundle_path}: unsafe path in bundle: {rel!r}")


def check_environment(root: Path, env: dict) -> dict:
    """Compare the project's DECLARED dependencies (its ``pyproject.toml``)
    with what is installed here, using the versions the exporter ran.

    Returns ``{"python": {"exported", "here"}, "missing": [...],
    "different": [{"name", "exported", "here"}], "install": "pip ..." | None}``.
    Nothing is installed: SciStack does not manage environments.
    """
    import importlib.metadata
    import platform
    import re

    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10
        import tomli as tomllib

    exported = env.get("distributions") or {}
    pyproject = root / "pyproject.toml"
    declared: list[str] = []
    if pyproject.is_file():
        try:
            deps = tomllib.loads(pyproject.read_text()).get("project", {}).get("dependencies", [])
        except Exception as e:
            Log.warn(f"[bundle] could not read {pyproject}: {e}")
            deps = []
        for req in deps:
            m = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", str(req))
            if m:
                declared.append(_canonical_dist(m.group(1)))

    missing, different, pins = [], [], []
    for name in declared:
        try:
            here = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            here = None
        then = exported.get(name)
        if here is None:
            missing.append(name)
        elif then and here != then:
            different.append({"name": name, "exported": then, "here": here})
        else:
            continue
        pins.append(f"{name}=={then}" if then else name)
    report = {
        "python": {"exported": env.get("python"), "here": platform.python_version()},
        "missing": missing,
        "different": different,
        "install": ("pip install " + " ".join(f'"{p}"' for p in pins)) if pins else None,
    }
    Log.info(
        f"[bundle] environment: {len(declared)} declared dependenc(ies), "
        f"{len(missing)} missing, {len(different)} at another version"
    )
    return report


def export_project(
    root: "Path | str",
    db,
    out_path: "Path | str",
    *,
    options: "ExportOptions | None" = None,
    providers: "list[SectionProvider] | None" = None,
) -> Path:
    """Write the project at *root* (whose database *db* the caller opened) to
    *out_path* (``.scistack`` appended when missing). Returns the path."""
    from scifor.discovery import own_package_dir, read_project_name

    root = Path(root).resolve()
    options = options or ExportOptions()
    out_path = Path(out_path)
    if out_path.suffix != EXTENSION:
        out_path = out_path.with_name(out_path.name + EXTENSION)
    own = own_package_dir(root)
    package = own[0] if own else read_project_name(root)
    Log.info(f"[bundle] export_project: root={root} -> {out_path} options={options.to_dict()}")

    ctx = ExportContext(root=root, db=db, options=options, package=package)
    sections: dict[str, dict[str, bytes]] = {
        CONFIG_SECTION: _config_section(root),
        ENV_SECTION: _env_section(),
    }
    for provider in providers or []:
        if provider.name in sections:
            raise BundleError(f"two sections named {provider.name!r}")
        files = provider.export(ctx) or {}
        sections[provider.name] = files
        Log.info(f"[bundle] section {provider.name}: {len(files)} file(s)")

    manifest = {
        "format_version": FORMAT_VERSION,
        "scistack_version": _scistack_version(),
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "project": {
            "package": package,
            "database": Path(str(db.dataset_db_path)).name,
            "schema_keys": list(db.dataset_schema_keys),
        },
        "options": options.to_dict(),
        "sections": {
            name: {"files": {rel: _sha256(data) for rel, data in sorted(files.items())}}
            for name, files in sections.items()
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".tmp")
    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(MANIFEST, json.dumps(manifest, indent=2, sort_keys=True))
        for name, files in sections.items():
            for rel, data in sorted(files.items()):
                zf.writestr(f"{name}/{rel}", data)
    tmp.replace(out_path)
    Log.info(
        f"[bundle] wrote {out_path}: sections {sorted(sections)}, "
        f"{sum(len(f) for f in sections.values())} file(s)"
    )
    return out_path


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_bundle(path: "Path | str") -> Bundle:
    """Open and check a bundle: format version, every listed file present
    with the hash the manifest recorded, nothing unlisted."""
    path = Path(path)
    try:
        zf = zipfile.ZipFile(path)
    except (zipfile.BadZipFile, FileNotFoundError) as e:
        raise BundleError(f"{path} is not a SciStack bundle: {e}") from e
    with zf:
        names = set(zf.namelist())
        if MANIFEST not in names:
            raise BundleError(f"{path} has no {MANIFEST}")
        manifest = json.loads(zf.read(MANIFEST))
        version = manifest.get("format_version")
        if version != FORMAT_VERSION:
            raise BundleError(
                f"{path} is bundle format {version!r}; this SciStack reads format "
                f"{FORMAT_VERSION}. Re-export it with an up-to-date SciStack."
            )
        sections: dict[str, dict[str, bytes]] = {}
        listed = {MANIFEST}
        for name, info in (manifest.get("sections") or {}).items():
            files: dict[str, bytes] = {}
            _check_safe(path, name)
            for rel, digest in (info.get("files") or {}).items():
                _check_safe(path, rel)
                member = f"{name}/{rel}"
                listed.add(member)
                if member not in names:
                    raise BundleError(f"{path}: {member} is listed but missing")
                data = zf.read(member)
                if _sha256(data) != digest:
                    raise BundleError(f"{path}: {member} does not match its recorded hash")
                files[rel] = data
            sections[name] = files
        unlisted = sorted(n for n in names - listed if not n.endswith("/"))
        if unlisted:
            raise BundleError(f"{path}: files not in the manifest: {unlisted[:5]}")
    Log.info(f"[bundle] read {path}: sections {sorted(sections)}")
    return Bundle(manifest=manifest, sections=sections)


# ---------------------------------------------------------------------------
# Importing
# ---------------------------------------------------------------------------


def _default_open_db(db_path: Path, schema_keys: list[str]):
    from . import configure_database

    return configure_database(db_path, schema_keys)


def import_project(
    bundle_path: "Path | str",
    target_root: "Path | str",
    *,
    providers: "list[SectionProvider] | None" = None,
    schema_keys: "list[str] | None" = None,
    open_db: "Callable[[Path, list[str]], Any] | None" = None,
) -> ImportReport:
    """Make a NEW project at *target_root* from the bundle.

    *schema_keys* overrides the exporter's (default: the manifest's).
    *open_db(db_path, schema_keys)* creates and opens the database; the GUI
    passes ``headless.open_for_import`` (no code discovered). A section the
    bundle carries but no provider was given for is reported, not dropped
    silently.
    """
    from scifor.discovery import config_path_at

    from .project import init_project

    bundle = read_bundle(bundle_path)
    manifest = bundle.manifest
    project = manifest.get("project") or {}
    root = Path(target_root).resolve()
    keys = list(schema_keys or project.get("schema_keys") or [])
    if not keys:
        raise BundleError("no schema keys: the bundle records none and none were given")
    db_name = project.get("database") or f"{project.get('package') or 'project'}.duckdb"
    db_path = root / db_name

    existing = [p for p in (config_path_at(root), db_path) if p.exists()]
    if existing or (root.is_dir() and any(root.glob("*.duckdb"))):
        raise BundleError(
            f"{root} already holds a SciStack project ({existing or 'a database'}); "
            f"import makes a NEW project. Choose an empty or new folder."
        )
    Log.info(f"[bundle] import_project: {bundle_path} -> {root} (schema {keys})")

    report = ImportReport(
        root=root, db_path=db_path, package=project.get("package") or "", schema_keys=keys
    )

    # Config first, so init fills in only what the bundle did not carry.
    root.mkdir(parents=True, exist_ok=True)
    config_files = bundle.sections.get(CONFIG_SECTION, {})
    if "scistack.toml" in config_files:
        config_path_at(root).write_bytes(config_files["scistack.toml"])
        report.created.append(config_path_at(root))

    by_name = {p.name: p for p in providers or []}
    builtin = {CONFIG_SECTION, ENV_SECTION}
    for name, files in bundle.sections.items():
        if name not in builtin and name not in by_name:
            msg = f"section {name!r} ({len(files)} file(s)) has no importer here; not imported"
            report.warnings.append(msg)
            Log.warn(f"[bundle] {msg}")

    def _run(phase: str, ctx: ImportContext) -> None:
        for name, files in bundle.sections.items():
            provider = by_name.get(name)
            if provider is None or getattr(provider, "phase", PHASE_DATABASE) != phase:
                continue
            report.sections[name] = provider.import_(ctx, files) or {}
            Log.info(f"[bundle] imported section {name} ({phase}): {report.sections[name]}")

    # Files: the code, before init, so the new project's package IS the
    # exporter's (a copy) and init only fills in what is missing.
    _run(PHASE_FILES, ImportContext(root=root, db=None, db_path=db_path, schema_keys=keys,
                                    manifest=manifest))

    env_files = bundle.sections.get(ENV_SECTION, {})
    if ENV_FILE in env_files:
        env_report = check_environment(root, json.loads(env_files[ENV_FILE]))
        report.sections[ENV_SECTION] = env_report
        if env_report["install"]:
            report.warnings.append(
                f"dependencies missing or at another version than the exporter's: "
                f"{env_report['install']}"
            )

    init = init_project(root, name=project.get("package") or None)
    report.package = init.package
    report.created += [p for p in init.created if p not in report.created]
    report.warnings += init.warnings

    db = (open_db or _default_open_db)(db_path, keys)
    report.created.append(db_path)

    _run(PHASE_DATABASE, ImportContext(root=root, db=db, db_path=db_path, schema_keys=keys,
                                       manifest=manifest))

    for w in report.warnings:
        Log.warn(f"[bundle] import: {w}")
    Log.info(
        f"[bundle] import_project done: {root} package={report.package} "
        f"sections={sorted(report.sections)} warnings={len(report.warnings)}"
    )
    return report


def describe(bundle: Bundle) -> str:
    """One line per section, for CLI output and logs."""
    out = io.StringIO()
    project = bundle.manifest.get("project") or {}
    out.write(
        f"package {project.get('package')!r}, schema {project.get('schema_keys')}, "
        f"options {bundle.manifest.get('options')}\n"
    )
    for name, files in sorted(bundle.sections.items()):
        out.write(f"  {name}: {len(files)} file(s), {sum(len(d) for d in files.values())} bytes\n")
    return out.getvalue()
