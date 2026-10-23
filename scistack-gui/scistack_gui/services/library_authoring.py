"""
Libraries a project AUTHORS: Share as library, publish, Make my own copy
(``.claude/plan-library-fork-chain.md``, decisions R1/R2/A8/A9).

A library the project makes (⇪ Share a submodule) or copies (✎ Make my own
copy, under a new name) lives in ``<project>/libs/<name>/``
(``scifor.discovery.authored_library_dirs``) with the one layout
``scidb.library.write_library`` writes; the project then USES it (listed
under ``packages``, one copy), and its pipelines stay editable here: the
project's canvas is the working copy, ``pipelines/*.json`` is what ⇪ Share
publishes.

**Three entry points, one sequence** (plan section 3, rule 1). Each only
gathers its SOURCES, then calls the same owners in order:

    check_new_name (O20) -> plan_library (O7) -> write_library (O5) -> switch_project (O10)

* :func:`share` on a project submodule: its closure (``canvas_snapshot``) and
  the project code it runs;
* :func:`share` on a pipeline of a library the project authors (publish):
  the canvas, plus project code the canvas newly uses;
* :func:`copy`: an installed library's package files.

Everything that mentions a new name -- files, document labels, canvas
labels, history, MATLAB calls, the report -- reads it from the ONE
:class:`LibraryPlan` (rule 2). Nothing is written when the plan refuses.
"""

from __future__ import annotations

import ast
import importlib.util
import inspect
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


class AuthoringRefused(ValueError):
    """The share / publish / copy cannot be done as asked; the message says
    why (and names both sides of a collision)."""


# ---------------------------------------------------------------------------
# The report (O24) and the install hint (O25)
# ---------------------------------------------------------------------------


@dataclass
class AuthoringReport:
    """What one share / publish / copy did -- the GUI dialogs, ``scistack
    library --json`` and the logs all read this."""

    action: str  # "share" | "publish" | "copy"
    library: str
    folder: Path
    pipeline: "str | None" = None
    #: The library a copy was made from.
    source: "str | None" = None
    files: list[str] = field(default_factory=list)
    #: Old label -> new label, every function this action renamed.
    functions: dict[str, str] = field(default_factory=dict)
    #: Functions left as they are (another library's, a toolbox's).
    kept_labels: list[str] = field(default_factory=list)
    variables: list[str] = field(default_factory=list)
    parameters: list[str] = field(default_factory=list)
    path_inputs: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    derived_from: list[str] = field(default_factory=list)
    #: E8: function inputs on the shared canvas no Parameter feeds
    #: (``"<new label>.<input>"``): knobs a recipient can only set per step.
    step_values: list[str] = field(default_factory=list)
    rewritten_imports: list[str] = field(default_factory=list)
    qualified_calls: list[str] = field(default_factory=list)
    #: R2: ``{new label: records re-recorded}`` / ``[{"function", "reason"}]``.
    carried: dict[str, int] = field(default_factory=dict)
    not_carried: list[dict] = field(default_factory=list)
    #: What the project switched: packages, pipelines re-owned, nodes relabelled.
    switched: dict = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    install: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["folder"] = str(self.folder)
        return d


def install_hint(folder: Path) -> str:
    """The command a terminal user runs to import the library from their
    own scripts (the GUI does not need it: B3)."""
    return f'"{sys.executable}" -m pip install -e "{folder}"'


# ---------------------------------------------------------------------------
# Names and folders (O20)
# ---------------------------------------------------------------------------


def _project_root() -> Path:
    from scifor.pathinput import project_root

    return Path(project_root())


def library_folder(name: str, root: "Path | None" = None) -> Path:
    """Where library *name* lives when this project authors it (O1)."""
    from scifor.discovery import AUTHORED_LIBRARIES_DIR

    return (root or _project_root()) / AUTHORED_LIBRARIES_DIR / name


def suggest_name(pipeline_name: str) -> str:
    name = re.sub(r"[^a-z0-9_]+", "_", pipeline_name.lower()).strip("_") or "shared"
    return name if name[0].isalpha() else f"lib_{name}"


def check_new_name(name: str, root: "Path | None" = None) -> Path:
    """Refuse a name a NEW library cannot have; return its folder.

    It must be a valid import name, not already importable here (it would
    shadow, or be shadowed by, that package), and its folder
    ``libs/<name>/`` must be free."""
    from scidb.project import validate_project_name

    name = (name or "").strip()
    validate_project_name(name)
    folder = library_folder(name, root)
    if folder.exists() and any(folder.iterdir()):
        raise AuthoringRefused(f"{folder} already exists; choose another library name")
    try:
        importable = importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        importable = False
    if importable:
        raise AuthoringRefused(
            f"a module named {name!r} is already importable here; choose another library name"
        )
    return folder


def share_defaults(db, pipeline_id: str) -> dict:
    """What the Share dialog shows before the user confirms: the suggested
    name, where the library will be written, and whether this is a publish
    (the pipeline already belongs to a library the project authors)."""
    from scidb.names import authored_libraries

    from scistack_gui import pipeline_store as ps

    p = ps.get_pipeline(db, pipeline_id)
    if p is None:
        raise AuthoringRefused(f"unknown pipeline {pipeline_id!r}")
    owner = ps.library_owner(db, pipeline_id)
    publish = owner is not None and owner["library"] in authored_libraries()
    name = owner["library"] if publish else suggest_name(p["name"])
    return {"name": name, "folder": str(library_folder(name)), "pipeline": p["name"],
            "publish": publish}


# ---------------------------------------------------------------------------
# The plan (O7)
# ---------------------------------------------------------------------------


@dataclass
class LibraryPlan:
    """Everything one action will write and change, computed ONCE."""

    action: str
    library: str
    folder: Path
    existing: str  # write_library mode: "create" | "update"
    pipeline: "str | None" = None
    source: "str | None" = None
    #: Old label -> new label (the R2 mapping, the canvas relabel, the report).
    functions: dict[str, str] = field(default_factory=dict)
    files: dict[str, bytes] = field(default_factory=dict)
    documents: list[dict] = field(default_factory=list)
    variables: list[str] = field(default_factory=list)
    parameters: dict = field(default_factory=dict)
    path_inputs: dict = field(default_factory=dict)
    schema_keys: "list[str] | None" = None
    dependencies: list[str] = field(default_factory=list)
    derived_from: list[str] = field(default_factory=list)
    reexports: dict[str, str] = field(default_factory=dict)
    kept_labels: list[str] = field(default_factory=list)
    step_values: list[str] = field(default_factory=list)
    rewritten_imports: list[str] = field(default_factory=list)
    qualified_calls: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    #: The library a copy replaces in ``packages``.
    remove_package: "str | None" = None
    #: ``[(pipeline_id, document name, id inside the document)]`` to own.
    owners: list[tuple[str, str, str]] = field(default_factory=list)
    #: The pipelines whose canvas labels follow :attr:`functions`.
    relabel_pipelines: list[str] = field(default_factory=list)

    def report(self) -> AuthoringReport:
        return AuthoringReport(
            action=self.action,
            library=self.library,
            folder=self.folder,
            pipeline=self.pipeline,
            source=self.source,
            functions=dict(self.functions),
            kept_labels=sorted(self.kept_labels),
            variables=sorted(self.variables),
            parameters=sorted(self.parameters),
            path_inputs=sorted(self.path_inputs),
            dependencies=sorted(self.dependencies),
            derived_from=list(self.derived_from),
            step_values=list(self.step_values),
            rewritten_imports=list(self.rewritten_imports),
            qualified_calls=list(self.qualified_calls),
            warnings=list(self.warnings),
            install=install_hint(self.folder),
        )


def _is_project_file(path: "str | None", root: Path) -> bool:
    """A file of the project's OWN code: under the project folder, outside
    any installed location and outside the libraries it authors (those are
    libraries, ``libs/<name>/``)."""
    from scifor.discovery import AUTHORED_LIBRARIES_DIR

    if not path:
        return False
    p = Path(path).resolve()
    if any(part in ("site-packages", "dist-packages") for part in p.parts):
        return False
    try:
        rel = p.relative_to(root.resolve())
    except ValueError:
        return False
    return not (rel.parts and rel.parts[0] == AUTHORED_LIBRARIES_DIR)


def _rel(path: Path, root: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


@dataclass
class _PyModule:
    name: str  # the project's module name
    path: Path
    is_package: bool
    target: str  # the library's module name


def _absolute(node: ast.ImportFrom, package: str) -> "str | None":
    if node.level == 0:
        return node.module
    parts = package.split(".") if package else []
    if node.level - 1 > len(parts):
        return None
    base = parts[: len(parts) - (node.level - 1)]
    if node.module:
        base.append(node.module)
    return ".".join(base) or None


class _PythonCode:
    """Which project modules a library needs, where they land, and their
    text with imports rewritten (``code_rewrite``, O8)."""

    def __init__(self, lib: str, root: Path):
        from scifor.discovery import own_package_dir

        self.lib = lib
        self.root = root
        own = own_package_dir(root)
        self.own = own[0] if own else None
        self.modules: dict[str, _PyModule] = {}
        self.third_party: set[str] = set()

    def target_of(self, modname: str) -> str:
        if self.own and (modname == self.own or modname.startswith(self.own + ".")):
            rest = modname[len(self.own):].lstrip(".")
            return f"{self.lib}.{rest}" if rest else self.lib
        return f"{self.lib}.{modname}"

    def project_module(self, modname: str) -> "_PyModule | None":
        mod = sys.modules.get(modname)
        path = getattr(mod, "__file__", None) if mod is not None else None
        if mod is None or not _is_project_file(path, self.root):
            return None
        is_pkg = Path(path).name == "__init__.py"
        return _PyModule(modname, Path(path), is_pkg, self.target_of(modname))

    def add(self, modname: str) -> None:
        """Add *modname* and, transitively, the project modules it imports."""
        todo = [modname]
        while todo:
            name = todo.pop()
            if name in self.modules:
                continue
            m = self.project_module(name)
            if m is None:
                continue
            self.modules[name] = m
            for imported in self._imports(m):
                if self.project_module(imported) is not None:
                    todo.append(imported)
                else:
                    top = imported.split(".")[0]
                    if top and top not in sys.stdlib_module_names and top != self.lib:
                        self.third_party.add(top)

    def _imports(self, m: _PyModule) -> list[str]:
        tree = ast.parse(m.path.read_text(encoding="utf-8"))
        package = m.name if m.is_package else m.name.rpartition(".")[0]
        out = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                out += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                base = _absolute(node, package)
                if base is None:
                    continue
                out.append(base)
                # `from pkg import sub` may name a submodule.
                out += [f"{base}.{a.name}" for a in node.names if f"{base}.{a.name}" in sys.modules]
        return out

    @staticmethod
    def rel_path(m: _PyModule) -> str:
        """Path inside the library package."""
        parts = m.target.split(".")[1:]
        if m.is_package:
            return "/".join([*parts, "__init__.py"])
        return "/".join(parts) + ".py"

    def files(self, plan: LibraryPlan) -> dict[str, bytes]:
        from scistack_gui.code_rewrite import rewrite_imports

        def resolve(name: str):
            return self.modules[name].target if name in self.modules else None

        out: dict[str, bytes] = {}
        landed: dict[str, _PyModule] = {}
        for m in sorted(self.modules.values(), key=lambda m: m.name):
            rel = self.rel_path(m)
            if rel in landed:
                other = landed[rel]
                raise AuthoringRefused(
                    f"{_rel(m.path, self.root)} ({m.name}) and {_rel(other.path, self.root)} "
                    f"({other.name}) would both land on {self.lib}/{rel}; rename one of them"
                )
            landed[rel] = m
            result = rewrite_imports(m.path.read_text(encoding="utf-8"), resolve, where=m.name)
            plan.rewritten_imports += result.changes
            plan.warnings += result.warnings
            out[rel] = result.text.encode("utf-8")
        # Intermediate packages need an __init__.py of their own.
        for rel in list(out):
            parts = rel.split("/")[:-1]
            for i in range(1, len(parts) + 1):
                out.setdefault("/".join([*parts[:i], "__init__.py"]), b"")
        return out


class _MatlabCode:
    """Which project ``.m`` files a library needs, and their text with calls
    qualified (``matlab_rewrite``, O9)."""

    def __init__(self, lib: str, root: Path):
        from scistack_gui import matlab_registry

        self.lib = lib
        self.root = root
        self.registry = matlab_registry
        self.functions: dict[str, Path] = {}  # name (maybe a.b.f) -> file

    def project_functions(self) -> dict[str, Path]:
        out = {}
        for name in self.registry.get_all_function_names():
            info = self.registry.get_matlab_function(name)
            if info.file_path is not None and _is_project_file(str(info.file_path), self.root):
                out[name] = Path(info.file_path)
        return out

    def add(self, name: str) -> None:
        from scistack_gui.matlab_rewrite import calls

        known = self.project_functions()
        todo = [name]
        while todo:
            n = todo.pop()
            if n in self.functions or n not in known:
                continue
            self.functions[n] = known[n]
            text = known[n].read_text(encoding="utf-8", errors="replace")
            todo += [other for other in known if other != n and calls(text, other)]

    def files(self, plan: LibraryPlan) -> dict[str, bytes]:
        from scistack_gui.matlab_rewrite import qualify

        out = {}
        for name, path in sorted(self.functions.items()):
            *pkgs, base = name.split(".")
            rel = "/".join(["matlab", f"+{self.lib}", *[f"+{p}" for p in pkgs], f"{base}.m"])
            result = qualify(
                path.read_bytes().decode("utf-8", errors="replace"), self.lib,
                list(self.functions), where=f"{name}.m",
            )
            plan.qualified_calls += result.changes
            out[rel] = result.text.encode("utf-8")
        return out


def _distributions(top_level_modules: set[str]) -> set[str]:
    """Distribution names for top-level import names (stdlib excluded); a
    name no installed distribution provides is left out."""
    import importlib.metadata

    try:
        mapping = importlib.metadata.packages_distributions()
    except Exception:  # pragma: no cover - very old importlib.metadata
        mapping = {}
    out: set[str] = set()
    for top in top_level_modules:
        if top in sys.stdlib_module_names:
            continue
        dists = mapping.get(top) or []
        if dists:
            out.add(dists[0])
    return out


def _strip_locations(snap) -> None:
    """A library never names a project's subjects: drop every location
    selection from the captured canvas."""
    from scidb.intent import ASPECT_SCHEMA_LOCATION

    for n in snap.nodes:
        n.config = {k: v for k, v in (n.config or {}).items() if k != "schemaSelection"}
        statements = []
        for s in n.statements:
            if s.get("aspect") == ASPECT_SCHEMA_LOCATION and isinstance(s.get("value"), dict):
                value = {k: v for k, v in s["value"].items() if k != "schemaSelection"}
                if not value:
                    continue
                s = {**s, "value": value}
            statements.append(s)
        n.statements = statements


def _library_functions(lib: str) -> dict[str, str]:
    """``{label: where}`` of every function library *lib* already has here
    (Python from the registry, MATLAB from its ``+lib`` folder)."""
    from scidb.names import library_of

    from scistack_gui import matlab_registry, registry

    out = {}
    for label, fn in list(registry._functions.items()):
        inner = inspect.unwrap(getattr(fn, "fcn", fn))
        if library_of(inner) == lib:
            out[label] = f"{getattr(inner, '__module__', '?')}.{getattr(inner, '__name__', '?')}"
    for name in matlab_registry.get_all_function_names():
        if name.startswith(f"{lib}."):
            info = matlab_registry.get_matlab_function(name)
            out[name] = str(info.file_path or name)
    return out


def _plan_from_canvas(db, pipeline_id: str, lib: str, existing: str) -> LibraryPlan:
    """Share (``existing="create"``) or publish (``"update"``): the canvas of
    *pipeline_id* and everything it places, plus the project code it runs."""
    from scidb.library import make_document
    from scidb.names import library_of

    from scistack_gui import matlab_registry, registry
    from scistack_gui import pipeline_store as ps
    from scistack_gui.domain.graph_builder import path_input_display
    from scistack_gui.services import canvas_snapshot
    from scistack_gui.services.library_service import unwired_step_values
    from scistack_gui.services.portability_service import _closure_pipeline_ids

    root = _project_root()
    pipe = ps.get_pipeline(db, pipeline_id)
    if pipe is None:
        raise AuthoringRefused(f"unknown pipeline {pipeline_id!r}")
    plan = LibraryPlan(
        action="share" if existing == "create" else "publish",
        library=lib,
        folder=library_folder(lib, root),
        existing=existing,
        pipeline=pipe["name"],
    )

    closure = _closure_pipeline_ids(db, pipeline_id)
    foreign = sorted({
        o["library"] for o in (ps.library_owner(db, p) for p in closure) if o and o["library"] != lib
    })
    if foreign:
        raise AuthoringRefused(
            f"'{pipe['name']}' places pipelines from the librar(ies) {foreign}; another "
            "library's pipeline cannot be shipped inside this one"
        )
    snap = canvas_snapshot.capture(db, closure)
    glue = sorted({n.label for n in snap.nodes if n.node_type == "glueNode"})
    if glue:
        raise AuthoringRefused(f"glue nodes cannot be shared yet: {glue}")

    have = _library_functions(lib) if existing == "update" else {}
    py = _PythonCode(lib, root)
    ml = _MatlabCode(lib, root)
    other_libraries: set[str] = set()
    unresolved: list[str] = []
    sources: dict[str, str] = {}
    for label in sorted({n.label for n in snap.nodes if n.node_type == "functionNode"}):
        if label in have:
            continue  # already this library's (publish)
        if matlab_registry.is_matlab_function(label):
            info = matlab_registry.get_matlab_function(label)
            if info.file_path is None or not _is_project_file(str(info.file_path), root):
                plan.kept_labels.append(label)  # a toolbox / another library's
                if "." in label:
                    other_libraries.add(label.split(".")[0])
                continue
            ml.add(label)
            plan.functions[label] = f"{lib}.{label}"
            sources[label] = _rel(info.file_path, root)
            continue
        fn = registry.lookup_function(label)
        if fn is None:
            unresolved.append(label)
            continue
        inner = inspect.unwrap(getattr(fn, "fcn", fn))
        other = library_of(inner)
        module = getattr(inner, "__module__", "") or ""
        path = getattr(sys.modules.get(module), "__file__", None)
        if other or not _is_project_file(path, root):
            plan.kept_labels.append(label)
            if other:
                other_libraries.add(other)
            elif module:
                py.third_party.add(module.split(".")[0])
            continue
        py.add(module)
        plan.functions[label] = f"{lib}.{inner.__name__}"
        sources[label] = f"{module}.{inner.__name__} ({_rel(path, root)})"
        target = py.target_of(module)
        rel_module = target[len(lib) + 1:] if target.startswith(lib + ".") else ""
        if rel_module:
            plan.reexports[inner.__name__] = rel_module
    if unresolved:
        raise AuthoringRefused(
            f"these functions are on the canvas but their code was not found: {unresolved}"
        )

    # B10: two things that would land on one name refuse, naming both.
    for old, new in sorted(plan.functions.items()):
        if new in have:
            raise AuthoringRefused(
                f"cannot share into {lib}: {old} from {sources[old]} would become {new}, "
                f"which {lib} already has ({have[new]}); rename one of them"
            )
    files = {**py.files(plan), **ml.files(plan)}
    if existing == "update":
        pkg = plan.folder / "src" / lib
        by_rel = {py.rel_path(m): m for m in py.modules.values()}
        for rel, data in sorted(files.items()):
            target = pkg / rel
            if target.is_file() and target.read_bytes() != data and rel in by_rel:
                m = by_rel[rel]
                raise AuthoringRefused(
                    f"cannot share into {lib}: {_rel(m.path, root)} ({m.name}) would land on "
                    f"{_rel(target, root)}, which {lib} already has with other code; rename "
                    "the module in the project"
                )
        # Package __init__ files the library already has are kept as they are.
        files = {rel: d for rel, d in files.items()
                 if not (rel.endswith("__init__.py") and (pkg / rel).is_file())}
    plan.files = files

    # The document: labels from the plan, location selections stripped.
    for n in snap.nodes:
        if n.node_type == "functionNode" and n.label in plan.functions:
            n.label = plan.functions[n.label]
    _strip_locations(snap)
    names = {p["pipeline_id"]: p["name"] for p in ps.list_all_pipelines(db)}
    plan.documents = [make_document(
        pipe["name"], pipeline_id,
        [{"pipeline_id": p, "name": names.get(p, p)} for p in closure],
        snap.to_dict(),
    )]
    plan.owners = [(p, pipe["name"], p) for p in closure]
    plan.relabel_pipelines = list(closure)

    # Declarations: Variables; Parameters and PathInputs as DEFAULTS.
    plan.variables = sorted({n.label for n in snap.nodes if n.node_type == "variableNode"})
    params_reg = registry.get_parameters_registry()
    pis_reg = registry.get_path_inputs_registry()
    for label in sorted({n.label for n in snap.nodes if n.node_type == "parameterNode"}):
        p = params_reg.get(label)
        if p is None:
            plan.warnings.append(f"Parameter {label} is not declared here; shipped without a default")
            plan.parameters[label] = []
            continue
        plan.parameters[label] = list(getattr(p, "alternatives", []) or [])
    for label in sorted({n.label for n in snap.nodes if n.node_type == "pathInputNode"}):
        obj = pis_reg.get(label)
        if obj is None:
            plan.warnings.append(f"PathInput {label} is not declared here; shipped without a template")
            plan.path_inputs[label] = [""]
            continue
        d = path_input_display(obj)
        plan.path_inputs[label] = [d["template"], *[a["template"] for a in d["alternate_templates"]]]
    plan.schema_keys = list(db.dataset_schema_keys) if existing == "create" else None
    plan.dependencies = sorted(
        _distributions(py.third_party) | _distributions(other_libraries - {lib})
    )
    # E8: un-wired inputs, named as the recipient will see them.
    plan.step_values = sorted(
        f"{plan.functions.get(sv['label'], sv['label'])}.{sv['input']}"
        for sv in unwired_step_values(db, closure)
    )
    return plan


def _walk(traversable, rel: str = ""):
    for entry in sorted(traversable.iterdir(), key=lambda e: e.name):
        path = f"{rel}{entry.name}"
        if entry.is_dir():
            if entry.name == "__pycache__":
                continue
            yield from _walk(entry, path + "/")
        elif not entry.name.endswith((".pyc", ".pyo")):
            yield path, entry


def _plan_from_package(db, old: str, new: str) -> LibraryPlan:
    """Make my own copy: library *old*'s package, renamed *new*."""
    import importlib.resources

    from scidb.entities import DEFAULT_ENTITIES_FILENAME
    from scidb.library import (
        MATLAB_DIR,
        PIPELINES_DIR,
        _requirement_name,
        make_document,
        read_document,
        read_library,
        reexports_of,
    )
    from scidb.names import library_packages

    from scistack_gui import pipeline_store as ps
    from scistack_gui.code_rewrite import rewrite_imports
    from scistack_gui.matlab_rewrite import qualify

    if old not in library_packages():
        raise AuthoringRefused(f"'{old}' is not a library of this project (not under packages)")
    info = read_library(old)
    if any("not importable" in e for e in info.errors):
        raise AuthoringRefused(f"'{old}' is not importable here: {info.errors}")
    root = _project_root()
    plan = LibraryPlan(
        action="copy", library=new, folder=library_folder(new, root), existing="create",
        source=old,
    )
    top = importlib.resources.files(old)

    def resolve(name: str):
        if name == old or name.startswith(old + "."):
            return new + name[len(old):]
        return None

    matlab_prefix = f"{MATLAB_DIR}/+{old}/"
    matlab_names = []
    for rel, _entry in _walk(top):
        if rel.startswith(matlab_prefix) and rel.endswith(".m"):
            parts = rel[len(matlab_prefix):-2].split("/")
            matlab_names.append(".".join(p.lstrip("+") for p in parts))

    for rel, entry in _walk(top):
        if rel == DEFAULT_ENTITIES_FILENAME:
            continue  # declarations are re-rendered from read_library's view
        if rel.startswith(f"{PIPELINES_DIR}/") and rel.endswith(".json"):
            continue  # documents are relabelled below
        if rel.endswith(".py"):
            result = rewrite_imports(entry.read_text(encoding="utf-8"), resolve, where=f"{old}/{rel}")
            plan.rewritten_imports += result.changes
            plan.warnings += result.warnings
            plan.files[rel] = result.text.encode("utf-8")
            if rel == "__init__.py":
                plan.reexports = reexports_of(result.text)
        elif rel.startswith(matlab_prefix):
            target = f"{MATLAB_DIR}/+{new}/" + rel[len(matlab_prefix):]
            if rel.endswith(".m"):
                result = qualify(entry.read_bytes().decode("utf-8", errors="replace"), new,
                                 matlab_names, old_packages=[old], where=rel)
                plan.qualified_calls += result.changes
                plan.files[target] = result.text.encode("utf-8")
            else:
                plan.files[target] = entry.read_bytes()
        else:
            plan.files[rel] = entry.read_bytes()

    def relabel(label: str) -> str:
        return new + label[len(old):] if label.startswith(old + ".") else label

    for lp in info.pipelines:
        doc = read_document(json.dumps(lp.document).encode(), f"{old}/{lp.name}")
        for n in doc["canvas"].get("nodes", []):
            if n.get("node_type") == "functionNode" and str(n.get("label", "")).startswith(old + "."):
                plan.functions[n["label"]] = relabel(n["label"])
                n["label"] = relabel(n["label"])
        plan.documents.append(make_document(doc["name"], doc["root"], doc["pipelines"], doc["canvas"]))

    plan.variables = list(info.variables)
    plan.parameters = dict(info.parameter_defaults)
    plan.path_inputs = dict(info.path_input_defaults)
    plan.schema_keys = info.schema_keys
    plan.derived_from = [old, *[d for d in info.derived_from if d != old]]
    lineage = {old, new, *info.derived_from}
    plan.dependencies = [d for d in info.dependencies if _requirement_name(d) not in lineage]
    plan.remove_package = old
    rows = [r for r in ps.list_library_pipelines(db) if r["library"] == old]
    plan.owners = [(r["pipeline_id"], r["pipeline_name"], r["doc_pipeline_id"]) for r in rows]
    plan.relabel_pipelines = [r["pipeline_id"] for r in rows]
    plan.step_values = []
    return plan


def plan_library(db, *, name: str, pipeline_id: "str | None" = None,
                 copy_of: "str | None" = None) -> LibraryPlan:
    """THE plan of one action (O7): a share or publish of *pipeline_id* as
    *name*, or a copy of library *copy_of* as *name*. Refuses (raises
    :class:`AuthoringRefused`) before anything is written."""
    from scidb.names import authored_libraries

    from scistack_gui import pipeline_store as ps

    name = (name or "").strip()
    if copy_of is not None:
        check_new_name(name)
        plan = _plan_from_package(db, copy_of.strip(), name)
    else:
        owner = ps.library_owner(db, pipeline_id) if pipeline_id else None
        if owner is not None and owner["library"] in authored_libraries():
            if name and name != owner["library"]:
                raise AuthoringRefused(
                    f"this pipeline belongs to your library {owner['library']}; ⇪ Share "
                    f"publishes it there (to make another library, use Make my own copy)"
                )
            plan = _plan_from_canvas(db, pipeline_id, owner["library"], "update")
        elif owner is not None:
            raise AuthoringRefused(
                f"this pipeline comes from the library '{owner['library']}'; use Make my own "
                f"copy first (Submodules → Libraries → ✎, or `scistack library copy "
                f"{owner['library']} --as NEW_NAME`)"
            )
        else:
            check_new_name(name)
            plan = _plan_from_canvas(db, pipeline_id, name, "create")
    for old, new in sorted(plan.functions.items()):
        logger.info("[library_authoring] %s %s: %s -> %s", plan.action, plan.library, old, new)
    logger.info(
        "[library_authoring] plan %s %s at %s (%s): %d function(s), %d file(s), %d document(s), "
        "kept %s, step values %s, %d warning(s)",
        plan.action, plan.library, plan.folder, plan.existing, len(plan.functions),
        len(plan.files), len(plan.documents), plan.kept_labels, plan.step_values,
        len(plan.warnings),
    )
    return plan


# ---------------------------------------------------------------------------
# Switching the project to the library (O10)
# ---------------------------------------------------------------------------


def _current_hashes(names) -> dict:
    """``{label: the hash a run of it records now}`` (O15): Python through
    ``scidb.foreach_config.function_hash_for``, MATLAB through the file's
    current source hash. ``None`` when it cannot be resolved here."""
    from scidb.foreach_config import function_hash_for

    from scistack_gui import matlab_registry, registry

    out = {}
    for label in names:
        try:
            if matlab_registry.is_matlab_function(label):
                out[label] = matlab_registry.current_matlab_function(
                    label, purpose="rename").source_hash
                continue
            fn = registry.lookup_function(label)
            out[label] = function_hash_for(fn) if fn is not None else None
        except Exception as e:  # reported as not carried, with the reason
            logger.warning("[library_authoring] cannot hash %s: %s", label, e)
            out[label] = None
    return out


def _relabel_canvas(db, pipeline_ids: list[str], mapping: dict[str, str]) -> dict:
    """Canvas labels follow the plan (O10), through the owners of node
    identity: ``pipeline_store.rebase_node`` moves each renamed function
    node's id (``fn__old__x`` -> ``fn__new__x``: a node id names its
    function) with every piece of state keyed by it, and
    ``layout.rebase_node_positions`` its positions.

    The OLD id is retired, not reused: it keeps the old name's recorded
    wirings (``node_wiring``) and is marked adopted, so the old history is
    still claimed by a node -- and is not drawn again as a new one. The
    renamed node is recorded as running the NEW name's wirings (R2 history),
    which makes it current."""
    from scistack_gui import layout as layout_store
    from scistack_gui import node_wiring
    from scistack_gui import pipeline_store as ps
    from scistack_gui.domain.adoption import node_subject
    from scistack_gui.ids import ROOT_SCOPE, fn_nodes_prefix, strip_placement

    renamed: dict[str, tuple[str, str]] = {}  # old bare id -> (new id, old label)
    scopes: dict[str, str] = {}
    for pid in pipeline_ids:
        for nid, meta in ps.get_manual_nodes(db, pid).items():
            label = meta.get("label")
            if meta.get("type") != "functionNode" or label not in mapping:
                continue
            bare = str(strip_placement(nid))
            if bare in renamed:
                continue
            prefix = fn_nodes_prefix(label)
            suffix = bare[len(prefix):] if bare.startswith(prefix) else bare.rsplit("__", 1)[-1]
            renamed[bare] = (f"{fn_nodes_prefix(mapping[label])}{suffix}", label)
            scopes[bare] = pid
    old_wirings: dict[str, list[dict]] = {}
    for row in node_wiring.associations(db):
        if row["node_id"] in renamed:
            old_wirings.setdefault(row["node_id"], []).append(row)
    for bare, (new_id, label) in renamed.items():
        counts = ps.rebase_node(db, bare, new_id, mapping[label])
        moved = layout_store.rebase_node_positions(bare, new_id)
        for row in old_wirings.get(bare, []):
            node_wiring.record(db, bare, row["wiring_id"], run_id=row.get("run_id"),
                               scope=row.get("scope") or scopes[bare],
                               seen=row.get("last_seen") or None)
        node_wiring.mark_adopted(db, [(node_subject(bare), bare, scopes[bare])])
        # Hidden, never deleted: a hidden node's call sites leave the run-state
        # cascade (graph_builder.hidden_fn_fkeys), so the old name's history --
        # whose function may not even resolve any more -- cannot redden the
        # renamed node's downstream steps.
        for scope in dict.fromkeys((scopes[bare], ROOT_SCOPE)):
            ps.hide_node(db, bare, scope)
        logger.info(
            "[library_authoring] node %s (%s) -> %s (%s): %s, %d position(s); old id retired "
            "with %d wiring(s)", bare, label, new_id, mapping[label], counts, moved,
            len(old_wirings.get(bare, [])),
        )
    recorded = _record_new_wirings(db, renamed, mapping)
    return {"nodes": {b: n for b, (n, _l) in renamed.items()}, "wirings": recorded}


def _record_new_wirings(db, renamed: dict, mapping: dict[str, str]) -> int:
    """Each renamed node runs as its old wirings' NEW-name twins (the
    carried history), as of now -- so the node is green and current."""
    from scistack_gui import node_wiring
    from scistack_gui.api.pipeline import build_aggregate
    from scistack_gui.domain import graph_builder as gb

    if not renamed:
        return 0
    reverse = {new: old for old, new in mapping.items()}
    agg = build_aggregate(db, db.get_aggregated_variants())
    pi = gb.path_input_bindings_by_fkey(agg.path_inputs)
    node_of_old = {}
    for bare, (new_id, _label) in renamed.items():
        for wiring in node_wiring.wirings_for_node(db, bare):
            node_of_old[wiring] = (new_id, bare)
    n = 0
    for fkey, params in agg.fn_input_params.items():
        fn, _cid = fkey
        if fn not in reverse:
            continue
        outputs = agg.fn_outputs.get(fkey, set())
        w_old = gb.wiring_id(reverse[fn], params, outputs, pi.get(fkey, {}))
        if w_old not in node_of_old:
            continue
        new_id, _bare = node_of_old[w_old]
        w_new = gb.wiring_id(fn, params, outputs, pi.get(fkey, {}))
        if node_wiring.record(db, new_id, w_new):
            n += 1
    logger.info("[library_authoring] recorded %d new-name wiring(s) for renamed nodes", n)
    return n


def switch_project(db, plan: LibraryPlan, report: AuthoringReport) -> None:
    """Make the project USE the library it just wrote (O10): list it (and
    unlist a copied original), make it importable and reload the
    registries, own its pipelines with the SAME ids, carry the results of
    every renamed function (R2, ``scidb.rename_functions``) and relabel the
    canvas."""
    from scidb import names, rename_functions
    from scidb.library import definition_hash
    from scifor.discovery import ensure_authored_libraries_importable

    from scistack_gui import pipeline_store as ps
    from scistack_gui.config import add_package, remove_package
    from scistack_gui.db import get_db_path
    from scistack_gui.services.registry_reload_service import reload_registries_from_disk

    root = _project_root()
    db_path = get_db_path()
    if plan.remove_package:
        remove_package(db_path, plan.remove_package, project=root)
    add_package(db_path, plan.library, project=root)
    added = ensure_authored_libraries_importable(root)
    importlib.invalidate_caches()
    names.clear_cache()
    reload_registries_from_disk(db_path)

    hashes = {d["name"]: definition_hash(d) for d in plan.documents}
    owned = []
    for pid, doc_name, doc_pid in plan.owners:
        if doc_name not in hashes or ps.get_pipeline(db, pid) is None:
            continue
        ps.set_library_owner(db, pid, plan.library, doc_name, doc_pid, hashes[doc_name])
        owned.append(pid)

    # Every wiring in history is claimed by its node BEFORE the rename (the
    # one resolution a graph build runs): otherwise the NEW name's history
    # would arrive with no claimant and be minted a second node.
    from scistack_gui.api.pipeline import ensure_node_identities

    ensure_node_identities(db)
    rename = rename_functions(db, plan.functions, _current_hashes(plan.functions.values()))
    report.carried = dict(rename.carried)
    report.not_carried = list(rename.not_carried)
    relabelled = _relabel_canvas(db, plan.relabel_pipelines, plan.functions)
    report.switched = {
        "packages_added": [plan.library],
        "packages_removed": [plan.remove_package] if plan.remove_package else [],
        "importable": added,
        "pipelines_owned": owned,
        "nodes_relabelled": relabelled["nodes"],
        "wirings_recorded": relabelled["wirings"],
    }
    logger.info("[library_authoring] switched to %s: %s", plan.library, report.switched)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def _run(db, plan: LibraryPlan) -> AuthoringReport:
    from scidb.library import write_library

    report = plan.report()
    written = write_library(
        plan.folder,
        plan.library,
        files=plan.files,
        documents=plan.documents,
        variables=plan.variables,
        parameters=plan.parameters,
        path_inputs=plan.path_inputs,
        schema_keys=plan.schema_keys,
        dependencies=plan.dependencies,
        derived_from=plan.derived_from,
        reexports=plan.reexports,
        existing=plan.existing,
    )
    report.files = [str(p.relative_to(plan.folder)) for p in written.created + written.updated]
    report.folder = written.root
    switch_project(db, plan, report)
    logger.info(
        "[library_authoring] %s %s done: %d file(s) written, functions %s, carried %s, "
        "not carried %s, %d warning(s)",
        report.action, report.library, len(report.files), report.functions, report.carried,
        report.not_carried, len(report.warnings),
    )
    for w in report.warnings:
        logger.warning("[library_authoring] %s", w)
    return report


def share(db, pipeline_id: str, name: str) -> AuthoringReport:
    """⇪ Share: a project submodule becomes the new library *name*; a
    pipeline of a library the project authors is published into it."""
    return _run(db, plan_library(db, name=name, pipeline_id=pipeline_id))


def copy(db, library: str, new_name: str) -> AuthoringReport:
    """✎ Make my own copy: installed library *library* becomes the library
    *new_name* this project authors; every placement switches to it."""
    return _run(db, plan_library(db, name=new_name, copy_of=library))
