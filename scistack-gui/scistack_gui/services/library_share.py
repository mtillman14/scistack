"""
Share as library: make a library package out of a submodule (portability
Stage 10b; ``.claude/plan-portability.md``).

The package files are written by ``scidb.library.create_library`` (the one
owner of a library's layout). This module decides WHAT goes in, because only
the GUI knows the canvas and the registry:

* the submodule's closure, captured by ``canvas_snapshot`` (the one owner of
  copying a canvas) into one pipeline document, with function labels made
  ``<lib>.fn`` and location selections stripped (a library never names a
  project's subjects);
* the Python code: each project function's module, plus the project modules
  it imports (transitively, by AST), with imports of the project's own
  package / loose modules rewritten to the library's;
* the MATLAB code: each ``.m`` file into ``matlab/+<lib>/``, plus the project
  helpers it calls, with calls and handles qualified as ``<lib>.fn``;
* the Variables it uses, and its Parameters / PathInputs as DEFAULTS (user
  decision 2026-10-08: the project declares its own; placing offers to copy
  these);
* dependencies: the distributions its third-party imports come from, and the
  other libraries it uses.

Anything it cannot carry -- a function it cannot find, another library's
pipeline placed inside, a glue node -- is refused with the reason; anything
it changed or could not rewrite is in the report.
"""

from __future__ import annotations

import ast
import inspect
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ShareReport:
    library: str
    dest: Path
    pipeline: str
    files: list[str] = field(default_factory=list)
    functions: dict[str, str] = field(default_factory=dict)  # old label -> new label
    kept_labels: list[str] = field(default_factory=list)  # other libraries' functions
    variables: list[str] = field(default_factory=list)
    parameters: list[str] = field(default_factory=list)
    path_inputs: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    rewritten_imports: list[str] = field(default_factory=list)
    qualified_calls: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    install: str = ""

    def to_dict(self) -> dict:
        from dataclasses import asdict

        d = asdict(self)
        d["dest"] = str(self.dest)
        return d


class ShareRefused(ValueError):
    """The submodule cannot be shared as it is; the message says why."""


def _project_root() -> Path:
    from scifor.pathinput import project_root

    return project_root()


def _is_project_file(path: "str | None", root: Path) -> bool:
    if not path:
        return False
    p = Path(path).resolve()
    if any(part in ("site-packages", "dist-packages") for part in p.parts):
        return False
    try:
        p.relative_to(root.resolve())
    except ValueError:
        return False
    return True


# ---------------------------------------------------------------------------
# Python: which modules, where they go, how their imports change
# ---------------------------------------------------------------------------


@dataclass
class _PyModule:
    name: str  # the project's module name
    path: Path
    is_package: bool
    target: str  # the library's module name


class _PythonPlan:
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
                    if top and top not in sys.stdlib_module_names:
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

    def rel_path(self, m: _PyModule) -> str:
        """Path inside the library package."""
        parts = m.target.split(".")[1:]
        if m.is_package:
            return "/".join([*parts, "__init__.py"])
        return "/".join(parts) + ".py"

    def files(self, report: ShareReport) -> dict[str, bytes]:
        out: dict[str, bytes] = {}
        for m in sorted(self.modules.values(), key=lambda m: m.name):
            rel = self.rel_path(m)
            out[rel] = self._rewrite(m, report).encode("utf-8")
        # Intermediate packages need an __init__.py of their own.
        for rel in list(out):
            parts = rel.split("/")[:-1]
            for i in range(1, len(parts) + 1):
                init = "/".join([*parts[:i], "__init__.py"])
                out.setdefault(init, b"")
        return out

    def _rewrite(self, m: _PyModule, report: ShareReport) -> str:
        text = m.path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        edits: list[tuple[int, int, int, int, str]] = []
        for node in ast.walk(tree):
            src = None
            if isinstance(node, ast.Import):
                src = self._rewrite_import(node, report, m)
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                if node.module and node.module in self.modules:
                    src = ast.unparse(ast.ImportFrom(
                        module=self.modules[node.module].target, names=node.names, level=0
                    ))
            if src is not None:
                report.rewritten_imports.append(f"{m.name}: {ast.unparse(node)} -> {src}")
                edits.append((node.lineno, node.col_offset, node.end_lineno, node.end_col_offset, src))
        lines = text.splitlines(keepends=True)
        for l0, c0, l1, c1, src in sorted(edits, reverse=True):
            first, last = lines[l0 - 1], lines[l1 - 1]
            lines[l0 - 1 : l1] = [first[:c0] + src + last[c1:]]
        return "".join(lines)

    def _rewrite_import(self, node: ast.Import, report: ShareReport, m: _PyModule) -> "str | None":
        changed = False
        stmts: list[str] = []
        kept: list[ast.alias] = []
        for a in node.names:
            if a.name not in self.modules:
                kept.append(a)
                continue
            changed = True
            target = self.modules[a.name].target
            head, _, last = target.rpartition(".")
            if "." not in a.name:
                # `import m [as k]` binds m (or k): keep the binding.
                stmts.append(f"from {head} import {last}" + (f" as {a.asname}" if a.asname else ""))
            elif a.asname:
                stmts.append(f"import {target} as {a.asname}")
            else:
                stmts.append(f"import {target}")
                report.warnings.append(
                    f"{m.name}: 'import {a.name}' became 'import {target}'; code that "
                    f"refers to '{a.name.split('.')[0]}.…' must be updated by hand"
                )
        if not changed:
            return None
        if kept:
            stmts.insert(0, "import " + ", ".join(
                a.name + (f" as {a.asname}" if a.asname else "") for a in kept
            ))
        return "; ".join(stmts)


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


# ---------------------------------------------------------------------------
# MATLAB: which files, where they go, which calls get qualified
# ---------------------------------------------------------------------------

_FUNCTION_LINE = re.compile(r"^\s*function\b")


def _code_part(line: str) -> tuple[str, str]:
    """``(code, comment)``: split at the first ``%`` outside a string."""
    in_single = in_double = False
    for i, ch in enumerate(line):
        if ch == "'" and not in_double:
            # A transpose (x') is not a string start; good enough: strings
            # follow an operator, a comma, a paren or whitespace.
            if in_single or i == 0 or line[i - 1] in " =(,;[{+-*/":
                in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "%" and not in_single and not in_double:
            return line[:i], line[i:]
    return line, ""


class _MatlabPlan:
    def __init__(self, lib: str):
        from scistack_gui import matlab_registry

        self.lib = lib
        self.registry = matlab_registry
        self.functions: dict[str, Path] = {}  # name (maybe a.b.f) -> file

    def project_functions(self) -> dict[str, Path]:
        out = {}
        for name in self.registry.get_all_function_names():
            info = self.registry.get_matlab_function(name)
            if info.file_path is not None and not str(name).startswith(f"{self.lib}."):
                out[name] = Path(info.file_path)
        return out

    def add(self, name: str) -> None:
        known = self.project_functions()
        todo = [name]
        while todo:
            n = todo.pop()
            if n in self.functions or n not in known:
                continue
            self.functions[n] = known[n]
            text = known[n].read_text(encoding="utf-8", errors="replace")
            for other in known:
                if other != n and re.search(_call_pattern(other), _code_only(text)):
                    todo.append(other)

    def files(self, report: ShareReport) -> dict[str, bytes]:
        out = {}
        for name, path in sorted(self.functions.items()):
            *pkgs, base = name.split(".")
            rel = "/".join(["matlab", f"+{self.lib}", *[f"+{p}" for p in pkgs], f"{base}.m"])
            out[rel] = self._qualify(name, path.read_bytes(), report)
        return out

    def _qualify(self, name: str, raw: bytes, report: ShareReport) -> bytes:
        text = raw.decode("utf-8", errors="replace")
        newline = "\r\n" if "\r\n" in text else "\n"
        lines = text.split(newline)
        for i, line in enumerate(lines):
            if _FUNCTION_LINE.match(line):
                continue
            code, comment = _code_part(line)
            new = code
            for other in self.functions:
                new = re.sub(_call_pattern(other), f"{self.lib}.{other}", new)
                new = re.sub(rf"@{re.escape(other)}\b", f"@{self.lib}.{other}", new)
            if new != code:
                report.qualified_calls.append(f"{name}.m:{i + 1}: {code.strip()} -> {new.strip()}")
                lines[i] = new + comment
        return newline.join(lines).encode("utf-8")


def _call_pattern(name: str) -> str:
    return rf"(?<![\w.@]){re.escape(name)}(?=\s*\()"


def _code_only(text: str) -> str:
    return "\n".join(
        _code_part(line)[0] for line in text.splitlines() if not _FUNCTION_LINE.match(line)
    )


# ---------------------------------------------------------------------------
# The composition
# ---------------------------------------------------------------------------


def share_defaults(db, pipeline_id: str) -> dict:
    """What the Share dialog suggests: a library name from the pipeline's
    name and a folder beside the project."""
    from scistack_gui import pipeline_store as ps

    p = ps.get_pipeline(db, pipeline_id)
    if p is None:
        raise ShareRefused(f"unknown pipeline {pipeline_id!r}")
    name = re.sub(r"[^a-z0-9_]+", "_", p["name"].lower()).strip("_") or "shared"
    if not name[0].isalpha():
        name = f"lib_{name}"
    root = _project_root()
    return {"name": name, "dest": str(root.parent / name), "pipeline": p["name"]}


def share_as_library(db, pipeline_id: str, dest: "Path | str", name: str) -> ShareReport:
    """Write the submodule *pipeline_id* (and everything it places) as a new
    library *name* at *dest*. Raises :class:`ShareRefused` (or
    ``scidb.library.LibraryError``) with the reason when it cannot."""
    from scidb.library import create_library, make_document
    from scidb.names import library_of
    from scidb.project import validate_project_name

    from scistack_gui import matlab_registry, registry
    from scistack_gui import pipeline_store as ps
    from scistack_gui.services import canvas_snapshot
    from scistack_gui.services.portability_service import _closure_pipeline_ids

    validate_project_name(name)
    import importlib.util

    if importlib.util.find_spec(name) is not None:
        raise ShareRefused(
            f"a module named {name!r} is already importable here; choose another library name"
        )
    root = _project_root()
    pipe = ps.get_pipeline(db, pipeline_id)
    if pipe is None:
        raise ShareRefused(f"unknown pipeline {pipeline_id!r}")
    report = ShareReport(library=name, dest=Path(dest).resolve(), pipeline=pipe["name"])

    closure = _closure_pipeline_ids(db, pipeline_id)
    owned = [ps.library_owner(db, p) for p in closure]
    if any(owned):
        libs = sorted({o["library"] for o in owned if o})
        raise ShareRefused(
            f"'{pipe['name']}' places pipelines from the librar(ies) {libs}; another "
            "library's pipeline cannot be shipped inside this one"
        )
    snap = canvas_snapshot.capture(db, closure)
    glue = sorted({n.label for n in snap.nodes if n.node_type == "glueNode"})
    if glue:
        raise ShareRefused(f"glue nodes cannot be shared yet: {glue}")

    py = _PythonPlan(name, root)
    ml = _MatlabPlan(name)
    other_libraries: set[str] = set()
    unresolved: list[str] = []
    for label in sorted({n.label for n in snap.nodes if n.node_type == "functionNode"}):
        if matlab_registry.is_matlab_function(label):
            info = matlab_registry.get_matlab_function(label)
            if info.file_path is None:  # a MATLAB built-in / toolbox reference
                report.kept_labels.append(label)
                continue
            ml.add(label)
            report.functions[label] = f"{name}.{label}"
            continue
        fn = registry.lookup_function(label)
        if fn is None:
            unresolved.append(label)
            continue
        inner = inspect.unwrap(getattr(fn, "fcn", fn))
        lib = library_of(inner)
        module = getattr(inner, "__module__", "") or ""
        if lib or not _is_project_file(getattr(sys.modules.get(module), "__file__", None), root):
            report.kept_labels.append(label)
            if lib:
                other_libraries.add(lib)
            elif module:
                py.third_party.add(module.split(".")[0])
            continue
        py.add(module)
        report.functions[label] = f"{name}.{inner.__name__}"
    if unresolved:
        raise ShareRefused(
            f"these functions are on the canvas but their code was not found: {unresolved}"
        )

    # The document: labels made lib.fn, location selections stripped.
    from scidb.intent import ASPECT_SCHEMA_LOCATION

    for n in snap.nodes:
        if n.node_type == "functionNode" and n.label in report.functions:
            n.label = report.functions[n.label]
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
    names = {p["pipeline_id"]: p["name"] for p in ps.list_all_pipelines(db)}
    doc = make_document(
        pipe["name"],
        pipeline_id,
        [{"pipeline_id": p, "name": names.get(p, p)} for p in closure],
        snap.to_dict(),
    )

    # Declarations: Variables; Parameters and PathInputs as defaults.
    from scistack_gui.domain.graph_builder import path_input_display

    report.variables = sorted({n.label for n in snap.nodes if n.node_type == "variableNode"})
    params_reg = registry.get_parameters_registry()
    pis_reg = registry.get_path_inputs_registry()
    parameters: dict[str, list] = {}
    for label in sorted({n.label for n in snap.nodes if n.node_type == "parameterNode"}):
        p = params_reg.get(label)
        if p is None:
            report.warnings.append(f"Parameter {label} is not declared here; shipped without a default")
            parameters[label] = []
            continue
        parameters[label] = list(getattr(p, "alternatives", []) or [])
    path_inputs: dict[str, list[str]] = {}
    for label in sorted({n.label for n in snap.nodes if n.node_type == "pathInputNode"}):
        obj = pis_reg.get(label)
        if obj is None:
            report.warnings.append(f"PathInput {label} is not declared here; shipped without a template")
            path_inputs[label] = [""]
            continue
        d = path_input_display(obj)
        path_inputs[label] = [d["template"], *[a["template"] for a in d["alternate_templates"]]]
    report.parameters = sorted(parameters)
    report.path_inputs = sorted(path_inputs)

    files = {**py.files(report), **ml.files(report)}
    report.dependencies = sorted(_distributions(py.third_party) | _distributions(other_libraries))

    created = create_library(
        report.dest,
        name,
        files=files,
        documents=[doc],
        variables=report.variables,
        parameters=parameters,
        path_inputs=path_inputs,
        schema_keys=list(db.dataset_schema_keys),
        dependencies=report.dependencies,
    )
    report.files = [str(p.relative_to(report.dest)) for p in created.created]
    report.install = f'"{sys.executable}" -m pip install -e "{report.dest}"'
    logger.info(
        "[library_share] shared '%s' as %s at %s: %d function(s) %s, %d kept label(s), "
        "%d Python module(s), %d MATLAB file(s), %d import(s) rewritten, %d call(s) "
        "qualified, %d warning(s)",
        pipe["name"],
        name,
        report.dest,
        len(report.functions),
        sorted(report.functions.values()),
        len(report.kept_labels),
        len(py.modules),
        len(ml.functions),
        len(report.rewritten_imports),
        len(report.qualified_calls),
        len(report.warnings),
    )
    for w in report.warnings:
        logger.warning("[library_share] %s", w)
    return report


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
