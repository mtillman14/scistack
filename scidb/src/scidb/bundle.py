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
import re
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

HISTORY_SECTION = "history"
DATA_SECTION = "data"
#: Opt-in (``ExportOptions.include_wheelhouse``): a wheel of each library the
#: project lists, so an import can install them offline (scidb.environment).
WHEELHOUSE_SECTION = "wheelhouse"
WHEELHOUSE_INDEX = "wheelhouse.json"

#: What ran, when, with what: the provenance graph and its schema rows, plus
#: the dataset intent that refers to them (exclusions, variant pins,
#: deletion tombstones). Never who (D-2026-10-08-1).
HISTORY_TABLES = (
    "_schema",
    "_record",
    "_constant",
    "_invocation",
    "_function_source",
    "_invocation_input",
    "_invocation_output",
    "_run",
    "_run_invocation",
    "_variant_pin",
    "_variant_tombstone",
    "__scidb_schema_overrides",
)

#: Rows that are dataset INTENT rather than facts: they govern a re-run, so
#: they go live even when the rest of the history is only archived.
INTENT_TABLES = ("__scidb_schema_overrides",)

#: Where an archived (not loaded) history lands in the new project.
ARCHIVE_DIR = ".scistack/archive"


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
    #: Exporter schema key -> recipient key (``scidb.schema_map.KeyMap``);
    #: identity when the schema was kept. Each section remaps its own data.
    key_map: Any = None
    #: Collects what each remap dropped or flagged (``schema_map.MapReport``).
    map_report: Any = None
    #: Whether the bundle's history AND data went into the live database
    #: (verbatim). The GUI section then copies its tables verbatim too.
    history_live: bool = False


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

    def to_dict(self) -> dict:
        """JSON-serialisable: what the CLI's ``--json`` prints and the GUI's
        import command reads back."""
        return json.loads(json.dumps({
            "root": str(self.root),
            "db_path": str(self.db_path),
            "package": self.package,
            "schema_keys": list(self.schema_keys),
            "created": [str(p) for p in self.created],
            "sections": self.sections,
            "warnings": list(self.warnings),
        }, default=str))


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
        "libraries": _library_distributions(),
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "matlab": matlab,
        "distributions": dict(sorted(dists.items())),
    }
    return {ENV_FILE: json.dumps(env, indent=2).encode("utf-8")}


def _library_distributions() -> dict:
    """``{import name: {"distribution", "version"}}`` for every library the
    project lists, so a recipient can install a missing one by its
    DISTRIBUTION name (a library's import name need not match it).
    ``distribution`` is None for a library no installed distribution
    provides (it is on sys.path by other means)."""
    import importlib.metadata

    from .names import library_packages

    try:
        mapping = importlib.metadata.packages_distributions()
    except Exception:  # pragma: no cover - very old importlib.metadata
        mapping = {}
    out = {}
    for lib in sorted(library_packages()):
        dists = mapping.get(lib) or []
        dist = dists[0] if dists else None
        version = None
        if dist:
            try:
                version = importlib.metadata.version(dist)
            except importlib.metadata.PackageNotFoundError:
                version = None
        out[lib] = {"distribution": dist, "version": version}
    return out


def _wheelhouse_section() -> "dict[str, bytes]":
    """A wheel of every listed library (``scidb.environment.library_wheel``),
    plus an index naming the ones that could not be built and why -- a
    missing wheel never fails the export."""
    import tempfile

    from .environment import library_wheel
    from .names import library_packages

    files: dict[str, bytes] = {}
    built, missing = [], []
    with tempfile.TemporaryDirectory() as tmp:
        for lib in sorted(library_packages()):
            try:
                wheel = library_wheel(lib, Path(tmp))
            except Exception as e:  # reported, never fatal
                missing.append({"library": lib, "reason": str(e)})
                Log.warn(f"[bundle] wheelhouse: no wheel for {lib}: {e}")
                continue
            files[wheel.name] = wheel.read_bytes()
            built.append({"library": lib, "wheel": wheel.name})
    files[WHEELHOUSE_INDEX] = json.dumps({"built": built, "missing": missing}, indent=2).encode()
    Log.info(f"[bundle] wheelhouse: {len(built)} wheel(s), {len(missing)} missing")
    return files


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
    "different": [{"name", "exported", "here"}], "install": "pip ..." | None,
    "missing_libraries": [...], "missing_requirements": [...]}``. The last is what
    an install would add (``scidb.environment``): MISSING packages only, at the
    exporter's version, libraries by their distribution name.
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
    # What an install would add: the MISSING ones only, at the exporter's
    # version when known (a version that differs is reported, never changed:
    # scidb.environment). Libraries by their distribution name.
    missing_requirements = [
        f"{name}=={exported[name]}" if exported.get(name) else name for name in missing
    ]
    import importlib.util

    missing_libraries = []
    for lib, info in sorted((env.get("libraries") or {}).items()):
        if importlib.util.find_spec(lib) is not None:
            continue
        dist, ver = (info or {}).get("distribution"), (info or {}).get("version")
        missing_libraries.append(lib)
        if dist:
            req = f"{dist}=={ver}" if ver else dist
            if _canonical_dist(dist) not in {_canonical_dist(m.split("==")[0]) for m in missing_requirements}:
                missing_requirements.append(req)
    report = {
        "python": {"exported": env.get("python"), "here": platform.python_version()},
        "missing": missing,
        "different": different,
        "missing_libraries": missing_libraries,
        "missing_requirements": missing_requirements,
        "install": ("pip install " + " ".join(f'"{p}"' for p in pins)) if pins else None,
    }
    Log.info(
        f"[bundle] environment: {len(declared)} declared dependenc(ies), "
        f"{len(missing)} missing, {len(different)} at another version"
    )
    return report


def _data_tables(duck) -> list[str]:
    """The derived data: every variable's table (``_registered_types``), the
    save events, and the variable metadata tables."""
    tables = ["_registered_types", "_variables", "_variable_groups", "_record_save"]
    try:
        tables += [r[0] for r in duck._fetchall("SELECT table_name FROM _registered_types")]
    except Exception as e:  # no variable was ever saved
        Log.info(f"[bundle] no _registered_types ({e}); no variable tables")
    return tables


def _views(duck) -> list[str]:
    """Every user view (scidb's per-variable views over the data)."""
    return [
        r[0]
        for r in duck._fetchall(
            "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main' AND NOT internal"
        )
    ]


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

    if options.include_data and not options.include_history:
        raise BundleError(
            "derived data needs its history: data without the record of what "
            "produced it cannot be loaded consistently (include_history=True)"
        )
    ctx = ExportContext(root=root, db=db, options=options, package=package)
    sections: dict[str, dict[str, bytes]] = {
        CONFIG_SECTION: _config_section(root),
        ENV_SECTION: _env_section(),
    }
    from . import table_copy

    if options.include_history:
        sections[HISTORY_SECTION] = table_copy.dump(db._duck, list(HISTORY_TABLES))
    if options.include_data:
        sections[DATA_SECTION] = table_copy.dump(
            db._duck, _data_tables(db._duck), _views(db._duck)
        )
    if options.include_wheelhouse:
        sections[WHEELHOUSE_SECTION] = _wheelhouse_section()
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
            # Lets import turn the exporter's absolute in-project config paths
            # back into relative ones (the GUI's first-write seeds).
            "root": root.as_posix(),
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


def _toml_loads(text: str) -> dict:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10
        import tomli as tomllib
    return tomllib.loads(text)


def _relative_to_exporter(value: Any, exporter_root: "str | None") -> Any:
    """An absolute path inside the exporter's project, made relative (``.``
    for the root itself); anything else unchanged. The GUI's first-write
    seeds were absolute, which named the exporter's folders on every other
    machine (Stage 1 notes)."""
    if not exporter_root or not isinstance(value, str):
        return value
    norm = value.replace("\\", "/").rstrip("/")
    er = exporter_root.replace("\\", "/").rstrip("/")
    if norm == er:
        return "."
    if norm.startswith(er + "/"):
        return norm[len(er) + 1:]
    return value


def _write_config(path: Path, data: bytes, kmap, exporter_root, report) -> None:
    """Write the bundle's ``scistack.toml`` into the new project: in-project
    absolute paths made relative, schema-keyed tables (``[schema_keys]``,
    ``[aliases]``, ``[colors]``) remapped. Unchanged -> the bytes verbatim
    (comments kept); changed -> through ``config_file`` (every key kept)."""
    from . import config_file

    section = _toml_loads(data.decode("utf-8"))
    new = json.loads(json.dumps(section, default=str))

    def rel(v):
        return _relative_to_exporter(v, exporter_root)

    if isinstance(new.get("modules"), list):
        new["modules"] = list(dict.fromkeys(rel(m) for m in new["modules"]))
    for key in ("entities_file", "glue_dir", "variable_file"):
        if key in new:
            new[key] = rel(new[key])
    matlab = new.get("matlab")
    if isinstance(matlab, dict):
        for key in ("functions", "variables", "sources"):
            if isinstance(matlab.get(key), list):
                matlab[key] = list(dict.fromkeys(rel(m) for m in matlab[key]))
        for key in ("variable_dir", "entities_file"):
            if key in matlab:
                matlab[key] = rel(matlab[key])
    for table in ("schema_keys", "aliases", "colors"):
        if table in new:
            new[table] = kmap.table(new[table], f"scistack.toml [{table}]", report)

    if new == json.loads(json.dumps(section, default=str)):
        path.write_bytes(data)
        Log.info(f"[bundle] config written verbatim: {path}")
    else:
        config_file.write(path, new)
        Log.info(f"[bundle] config written with paths made relative / keys remapped: {path}")


def _declared_path_inputs(root: Path) -> "tuple[Path, str, dict[str, list[dict]]] | None":
    """The PathInputs the project at *root*'s entities TOML declares, as
    ``(file, text, {name: [{"template", "root_folder"}, ...]})`` (one dict
    per arm), or ``None`` when the project has no entities file. The one
    reader both the import rewrite and :func:`preview` use."""
    from scifor.discovery import project_config_at, read_scistack_section

    from .entities import PATH_INPUTS, resolve_entities_path

    config = project_config_at(root)
    path = resolve_entities_path(root, read_scistack_section(config)) if config else None
    if path is None or not Path(path).is_file():
        return None
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    declared = {}
    for name, raw in (_toml_loads(text).get(PATH_INPUTS) or {}).items():
        raw_arms = raw if isinstance(raw, list) else [raw]
        declared[name] = [
            {"template": a, "root_folder": None} if isinstance(a, str) else
            {"template": a.get("template", ""), "root_folder": a.get("root_folder")}
            for a in raw_arms
        ]
    return path, text, declared


def _rewrite_path_inputs(root: Path, kmap, path_roots: "dict[str, str]", report) -> dict:
    """The PathInputs the new project's entities TOML declares: template
    placeholders renamed through *kmap*; ``root_folder`` set from
    *path_roots* (``{name: folder}``, the recipient's copy of the raw data,
    which is never touched). A PathInput that keeps the exporter's
    ``root_folder`` is reported, as is a root given for an unknown name.
    PathInputs declared in Python source cannot be rewritten here and keep
    their templates."""
    from .entities import PATH_INPUTS, render_path_input_value, upsert_entry

    out = {"rewritten": [], "roots_unchanged": [], "unknown_roots": []}
    found = _declared_path_inputs(root)
    if found is None:
        out["unknown_roots"] = sorted(path_roots)
        return out
    path, text, declared = found

    for name, arms in declared.items():
        where = f"PathInput {name}"
        new_arms = [
            {
                "template": kmap.template(a["template"], where, report),
                "root_folder": path_roots.get(name, a["root_folder"]),
            }
            for a in arms
        ]
        if new_arms != arms:
            first, rest = new_arms[0], new_arms[1:]
            text = upsert_entry(
                text, PATH_INPUTS, name,
                render_path_input_value(first["template"], first["root_folder"], rest),
            )
            out["rewritten"].append(name)
        if name not in path_roots:
            roots = sorted({a["root_folder"] for a in arms if a["root_folder"]})
            if roots:
                out["roots_unchanged"].append({"name": name, "root_folder": roots})
                report.flagged.append(
                    (where, f"root_folder is still the exporter's {roots}; give your own")
                )
    out["unknown_roots"] = sorted(set(path_roots) - set(declared))
    if out["rewritten"]:
        path.write_text(text, encoding="utf-8")
    Log.info(
        f"[bundle] PathInputs: {len(out['rewritten'])} rewritten, "
        f"{len(out['roots_unchanged'])} keep the exporter's root, "
        f"{len(out['unknown_roots'])} root(s) given for unknown names"
    )
    return out


def _import_history_and_data(bundle, db, root: Path, kmap, import_history: bool,
                             manifest: dict, report) -> bool:
    """Put the bundle's history (and data) where it belongs; return whether
    they went LIVE.

    The live database only ever holds history TOGETHER with its data, both
    verbatim (decided 2026-10-08): history whose data is not here would say
    records exist that cannot be loaded. So:

    * data + history, schema kept, history wanted -> both loaded live;
    * history without data (the default bundle), or into another schema ->
      the history is ARCHIVED under ``.scistack/archive/<export time>/`` as
      the bundle's own Parquet files, for audit and ``scistack verify``;
      its dataset intent (``INTENT_TABLES``: exclusions) still goes live
      when the schema was kept, because it governs a re-run;
    * data into another schema -> not imported (it describes a dataset with
      a different shape), reported.
    """
    from . import table_copy

    history = bundle.sections.get(HISTORY_SECTION)
    data = bundle.sections.get(DATA_SECTION)
    out: dict = {"live": False, "archived": None, "intent_loaded": [], "data": None}
    if history is None and data is None:
        return False
    if not import_history:
        out["data"] = "not imported (history opted out)" if data else None
        report.sections[HISTORY_SECTION] = out
        report.warnings.append("the bundle's run history was not imported (opted out)")
        return False

    if data is not None and history is not None and kmap.is_identity:
        out["history"] = table_copy.load(db._duck, history)
        out["data"] = table_copy.load(db._duck, data)
        out["live"] = True
        report.sections[HISTORY_SECTION] = out
        Log.info("[bundle] history and data loaded live (verbatim)")
        return True

    if data is not None:
        out["data"] = (
            "not imported: the schema changed, and the data describes a dataset of "
            "the exporter's shape"
        )
        report.warnings.append(f"derived data {out['data']}")
    if history is not None:
        stamp = re.sub(r"[^0-9A-Za-z]+", "-", str(manifest.get("exported_at") or "export")).strip("-")
        archive = root / ARCHIVE_DIR / stamp
        archive.mkdir(parents=True, exist_ok=True)
        for rel, blob in history.items():
            (archive / rel).write_bytes(blob)
        (archive / "README.txt").write_text(
            "Run history exported with this project, kept read-only for audit and\n"
            "`scistack verify`. It is NOT in the live database: its derived data\n"
            "was not in the bundle (or the schema changed), and the live database\n"
            "only holds history together with its data. tables.json lists the\n"
            "tables; each <table>.parquet holds its rows.\n",
            encoding="utf-8",
        )
        out["archived"] = str(archive)
        if kmap.is_identity:
            spec = json.loads(history[table_copy.TABLES_FILE])
            intent = [t for t in INTENT_TABLES if t in spec.get("tables", {})]
            if intent:
                subset = {
                    table_copy.TABLES_FILE: json.dumps(
                        {"tables": {t: spec["tables"][t] for t in intent}, "views": {}}
                    ).encode("utf-8"),
                    **{f"{t}.parquet": history[f"{t}.parquet"] for t in intent},
                }
                table_copy.load(db._duck, subset)
                out["intent_loaded"] = intent
        Log.info(f"[bundle] history archived at {archive}; intent loaded {out['intent_loaded']}")
    report.sections[HISTORY_SECTION] = out
    return False


def _default_open_db(db_path: Path, schema_keys: list[str]):
    from . import configure_database

    return configure_database(db_path, schema_keys)


def import_project(
    bundle_path: "Path | str",
    target_root: "Path | str",
    *,
    providers: "list[SectionProvider] | None" = None,
    schema_keys: "list[str] | None" = None,
    key_map: "dict[str, str | None] | None" = None,
    path_roots: "dict[str, str] | None" = None,
    import_history: bool = True,
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
    from .schema_map import KeyMap, MapReport

    try:
        kmap = KeyMap.auto(list(project.get("schema_keys") or []), keys, key_map)
    except ValueError as e:
        raise BundleError(str(e)) from e
    map_report = MapReport()
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
        _write_config(
            config_path_at(root), config_files["scistack.toml"], kmap, project.get("root"),
            map_report,
        )
        report.created.append(config_path_at(root))

    by_name = {p.name: p for p in providers or []}
    builtin = {CONFIG_SECTION, ENV_SECTION, HISTORY_SECTION, DATA_SECTION, WHEELHOUSE_SECTION}
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
                                    manifest=manifest, key_map=kmap, map_report=map_report))
    report.sections["path_inputs"] = _rewrite_path_inputs(root, kmap, path_roots or {}, map_report)

    env_files = bundle.sections.get(ENV_SECTION, {})
    from .environment import ENVIRONMENT_FILE, STATE_DIR, WHEELHOUSE_DIR

    if ENV_FILE in env_files:
        # Kept with the project: the install (after trust, a separate step:
        # scidb.environment.install_project_requirements) reads it from here.
        state = root / STATE_DIR
        state.mkdir(parents=True, exist_ok=True)
        (state / ENVIRONMENT_FILE).write_bytes(env_files[ENV_FILE])
        env_report = check_environment(root, json.loads(env_files[ENV_FILE]))
        report.sections[ENV_SECTION] = env_report
        if env_report["install"]:
            report.warnings.append(
                f"dependencies missing or at another version than the exporter's: "
                f"{env_report['install']}"
            )

    wheels = bundle.sections.get(WHEELHOUSE_SECTION, {})
    if wheels:
        house = root / STATE_DIR / WHEELHOUSE_DIR
        house.mkdir(parents=True, exist_ok=True)
        for rel, blob in wheels.items():
            (house / rel).write_bytes(blob)
        index = json.loads(wheels.get(WHEELHOUSE_INDEX, b"{}"))
        report.sections[WHEELHOUSE_SECTION] = {
            "dir": str(house),
            "wheels": sorted(k for k in wheels if k.endswith(".whl")),
            "missing": index.get("missing", []),
        }
        Log.info(f"[bundle] wheelhouse written to {house}: {report.sections[WHEELHOUSE_SECTION]}")

    init = init_project(root, name=project.get("package") or None)
    report.package = init.package
    report.created += [p for p in init.created if p not in report.created]
    report.warnings += init.warnings

    db = (open_db or _default_open_db)(db_path, keys)
    report.created.append(db_path)

    history_live = _import_history_and_data(
        bundle, db, root, kmap, import_history, manifest, report
    )
    _run(PHASE_DATABASE, ImportContext(root=root, db=db, db_path=db_path, schema_keys=keys,
                                       manifest=manifest, key_map=kmap, map_report=map_report,
                                       history_live=history_live))
    report.sections["schema"] = {
        "exporter": list(project.get("schema_keys") or []),
        "recipient": keys,
        "key_map": kmap.as_dict(),
        **map_report.to_dict(),
    }
    if not kmap.is_identity:
        report.warnings.append(
            f"imported into another schema ({kmap.describe()}): "
            f"{len(map_report.dropped)} reference(s) dropped, "
            f"{len(map_report.flagged)} flagged for review"
        )

    for w in report.warnings:
        Log.warn(f"[bundle] import: {w}")
    Log.info(
        f"[bundle] import_project done: {root} package={report.package} "
        f"sections={sorted(report.sections)} warnings={len(report.warnings)}"
    )
    return report


def preview(
    bundle_path: "Path | str", *, providers: "list[SectionProvider] | None" = None
) -> dict:
    """What an import dialog needs to know before asking anything: the
    exporter's package and schema, the export options, the sections, the
    PathInputs (so the user can give their own roots) and the environment
    check. Nothing is written outside a temporary folder and no code runs:
    the config and the "files"-phase sections are unpacked there, exactly as
    :func:`import_project` would unpack them, and read back.

    JSON-serialisable; the CLI's ``bundle-info --json`` and the GUI's import
    command show this dict."""
    import tempfile

    from scifor.discovery import config_path_at

    from .schema_map import KeyMap, MapReport

    bundle = read_bundle(bundle_path)
    manifest = bundle.manifest
    project = manifest.get("project") or {}
    keys = list(project.get("schema_keys") or [])
    kmap = KeyMap.auto(keys, keys)
    out: dict = {
        "path": str(Path(bundle_path).resolve()),
        "package": project.get("package"),
        "schema_keys": keys,
        "database": project.get("database"),
        "exported_root": project.get("root"),
        "options": manifest.get("options") or {},
        "sections": {
            name: {"files": len(files), "bytes": sum(len(d) for d in files.values())}
            for name, files in sorted(bundle.sections.items())
        },
        "has_history": HISTORY_SECTION in bundle.sections,
        "has_data": DATA_SECTION in bundle.sections,
        "path_inputs": [],
        "environment": None,
    }
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        config_files = bundle.sections.get(CONFIG_SECTION, {})
        if "scistack.toml" in config_files:
            _write_config(
                config_path_at(root), config_files["scistack.toml"], kmap, project.get("root"),
                MapReport(),
            )
        ctx = ImportContext(root=root, db=None, db_path=root / "preview.duckdb",
                            schema_keys=keys, manifest=manifest, key_map=kmap,
                            map_report=MapReport())
        for provider in providers or []:
            files = bundle.sections.get(provider.name)
            if files is not None and getattr(provider, "phase", PHASE_DATABASE) == PHASE_FILES:
                provider.import_(ctx, files)
        found = _declared_path_inputs(root)
        if found is not None:
            out["path_inputs"] = [
                {
                    "name": name,
                    "templates": [a["template"] for a in arms],
                    "root_folders": sorted({a["root_folder"] for a in arms if a["root_folder"]}),
                }
                for name, arms in sorted(found[2].items())
            ]
        env_files = bundle.sections.get(ENV_SECTION, {})
        if ENV_FILE in env_files:
            out["environment"] = check_environment(root, json.loads(env_files[ENV_FILE]))
    Log.info(
        f"[bundle] preview {bundle_path}: package={out['package']!r} schema={keys} "
        f"sections={sorted(out['sections'])} path_inputs={[p['name'] for p in out['path_inputs']]}"
    )
    return out


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
