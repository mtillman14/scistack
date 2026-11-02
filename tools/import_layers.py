"""THE owner of "which module may import which" across the SciStack packages.

Every package that enforces internal layers or a public boundary reads its
imports through this module. It parses source with ``ast`` only (no imports,
no side effects), so the checks stay fast and run without the package's
dependencies installed.

Per-package configuration lives next to that package's test, in two files:

``tests/import_layers.toml``
    ``package``      the importable name (``"scidb"``)
    ``src``          directory holding the package, relative to the TOML file
    ``order``        layers, lowest first; an inner list is a group of peers
                     that may import each other
    ``lazy_baseline`` pinned count of function-level import statements; the
                     test fails when the count rises, and the number is lowered
                     by hand when it drops
    ``[layers]``     layer name -> list of module names. A plain name matches
                     that module only; ``"scidb.inspect.*"`` matches
                     ``scidb.inspect`` and every module under it

``tests/import_allowlist.txt``
    One ``src.module -> dst.module  # reason`` per line. An upward import is
    a violation unless its module pair is listed. Entries that no longer
    match a violation are stale and fail the test, so the list only shrinks.

Bootstrap (or regenerate after a deliberate reorganisation)::

    python -m tools.import_layers scidb/tests/import_layers.toml --write

``--write`` keeps the reason of every pair already listed, adds new pairs
with a ``TODO`` reason (which the test rejects until a human writes one),
drops stale pairs, and updates ``lazy_baseline``.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - 3.10 only
    import tomli as tomllib

TODO_REASON = "TODO"
_ALLOW_LINE = re.compile(r"^\s*(?P<src>[\w.]+)\s*->\s*(?P<dst>[\w.]+)\s*(?:#\s*(?P<reason>.*))?$")


@dataclass(frozen=True)
class Edge:
    """One imported module, as written at one line of one module."""

    src: str
    dst: str
    lineno: int
    lazy: bool  # inside a def/class body
    type_only: bool  # under ``if TYPE_CHECKING:``; never executed


@dataclass(frozen=True)
class LayerConfig:
    """A package's layer declaration, read from ``import_layers.toml``."""

    package: str
    src_root: Path
    order: tuple[str | tuple[str, ...], ...]
    layers: Mapping[str, tuple[str, ...]]
    lazy_baseline: int
    toml_path: Path
    allowlist_path: Path

    def layer_of(self, module: str) -> str | None:
        """The layer of ``module``; the most specific matching entry wins."""
        best: tuple[int, str] | None = None
        for layer, entries in self.layers.items():
            for entry in entries:
                if _matches(entry, module) and (best is None or len(entry) > best[0]):
                    best = (len(entry), layer)
        return best[1] if best else None


def _matches(entry: str, module: str) -> bool:
    """``a.b`` matches only ``a.b``; ``a.b.*`` matches ``a.b`` and below it."""
    if entry.endswith(".*"):
        base = entry[:-2]
        return module == base or module.startswith(base + ".")
    return module == entry


# ---------------------------------------------------------------------------
# Reading source
# ---------------------------------------------------------------------------


def _module_name(root: Path, path: Path) -> str:
    parts = list(path.relative_to(root).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def package_modules(root: Path, package: str) -> dict[str, Path]:
    """Every module of ``package`` under ``root``, by dotted name."""
    return {
        _module_name(root, p): p
        for p in sorted((root / package).rglob("*.py"))
        if "__pycache__" not in p.parts
    }


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _resolve_base(module: str, is_package: bool, node: ast.ImportFrom) -> str:
    if not node.level:
        return node.module or ""
    parts = module.split(".")
    if not is_package:
        parts = parts[:-1]
    if node.level > 1:
        parts = parts[: len(parts) - (node.level - 1)]
    if node.module:
        parts.append(node.module)
    return ".".join(parts)


def _edges_of(module: str, path: Path, known: set[str]) -> list[Edge]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    is_package = path.name == "__init__.py"
    edges: list[Edge] = []

    def visit(node: ast.AST, lazy: bool, type_only: bool) -> None:
        if isinstance(node, ast.Import):
            for alias in node.names:
                edges.append(Edge(module, alias.name, node.lineno, lazy, type_only))
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_base(module, is_package, node)
            targets = {
                f"{base}.{a.name}" if f"{base}.{a.name}" in known else base
                for a in node.names
            }
            for dst in sorted(targets):
                edges.append(Edge(module, dst, node.lineno, lazy, type_only))
        for child in ast.iter_child_nodes(node):
            child_lazy = lazy or isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
            )
            child_type_only = type_only or (
                isinstance(node, ast.If)
                and _is_type_checking(node.test)
                and child in node.body
            )
            visit(child, child_lazy, child_type_only)

    visit(tree, False, False)
    return edges


def module_imports(
    root: Path, package: str, other_modules: Iterable[str] = ()
) -> list[Edge]:
    """Every import statement in every module of ``package`` under ``root``.

    Relative imports are resolved. ``from pkg import name`` resolves to
    ``pkg.name`` when that is a module of ``package`` or in ``other_modules``
    (pass another package's modules to see ``from scidb import intent`` as an
    import of ``scidb.provenance.intent``), else to ``pkg``.
    """
    modules = package_modules(root, package)
    known = set(modules) | set(other_modules)
    edges: list[Edge] = []
    for name, path in modules.items():
        edges.extend(_edges_of(name, path, known))
    return edges


# ---------------------------------------------------------------------------
# Checking
# ---------------------------------------------------------------------------


def layer_rank(order: Sequence[str | Sequence[str]]) -> dict[str, int]:
    """Layer name -> rank (lowest first); peers in a group share a rank."""
    rank: dict[str, int] = {}
    for i, entry in enumerate(order):
        for layer in [entry] if isinstance(entry, str) else entry:
            rank[layer] = i
    return rank


def upward_edges(
    edges: Iterable[Edge],
    layer_of: Callable[[str], str | None],
    order: Sequence[str | Sequence[str]],
) -> list[Edge]:
    """Runtime edges from a lower layer into a higher one."""
    rank = layer_rank(order)
    out = []
    for e in edges:
        if e.type_only:
            continue
        src, dst = layer_of(e.src), layer_of(e.dst)
        if src is None or dst is None:
            continue
        if rank[dst] > rank[src]:
            out.append(e)
    return out


def check(
    edges: Iterable[Edge],
    layer_of: Callable[[str], str | None],
    order: Sequence[str | Sequence[str]],
    allow: set[tuple[str, str]],
) -> list[str]:
    """Violations: upward edges whose ``(src, dst)`` pair is not allowed."""
    return [
        f"{e.src}:{e.lineno} imports {e.dst} "
        f"({layer_of(e.src)} -> {layer_of(e.dst)})"
        for e in upward_edges(edges, layer_of, order)
        if (e.src, e.dst) not in allow
    ]


def stale_allowed(
    edges: Iterable[Edge],
    layer_of: Callable[[str], str | None],
    order: Sequence[str | Sequence[str]],
    allow: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    """Allowed pairs that no longer match any upward edge."""
    live = {(e.src, e.dst) for e in upward_edges(edges, layer_of, order)}
    return allow - live


def lazy_imports(edges: Iterable[Edge]) -> set[tuple[str, int]]:
    """Function-level import statements, as ``(module, lineno)``."""
    return {(e.src, e.lineno) for e in edges if e.lazy}


def unclassified(modules: Iterable[str], layer_of: Callable[[str], str | None]) -> list[str]:
    """Modules that belong to no layer."""
    return sorted(m for m in modules if layer_of(m) is None)


def imports_outside(
    edges: Iterable[Edge], package: str, supported: Iterable[str]
) -> list[str]:
    """Imports of ``package`` that name a module outside ``supported``."""
    allowed = set(supported)
    return [
        f"{e.src}:{e.lineno} imports {e.dst}"
        for e in edges
        if (e.dst == package or e.dst.startswith(package + "."))
        and e.dst not in allowed
    ]


def boundary_violations(
    consumer_root: Path,
    consumer: str,
    provider_root: Path,
    provider: str,
    supported: Iterable[str],
) -> list[str]:
    """Imports from ``consumer`` into ``provider`` outside ``supported``.

    ``from provider import some_module`` counts as importing that module.
    """
    edges = module_imports(
        consumer_root, consumer, other_modules=package_modules(provider_root, provider)
    )
    return imports_outside(edges, provider, supported)


# ---------------------------------------------------------------------------
# Configuration files
# ---------------------------------------------------------------------------


def load_config(toml_path: Path) -> LayerConfig:
    data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    base = toml_path.parent
    return LayerConfig(
        package=data["package"],
        src_root=(base / data["src"]).resolve(),
        order=tuple(o if isinstance(o, str) else tuple(o) for o in data["order"]),
        layers={k: tuple(v) for k, v in data["layers"].items()},
        lazy_baseline=int(data["lazy_baseline"]),
        toml_path=toml_path,
        allowlist_path=base / "import_allowlist.txt",
    )


def read_allowlist(path: Path) -> dict[tuple[str, str], str]:
    """``(src, dst)`` -> reason. Blank and ``#``-only lines are skipped."""
    if not path.exists():
        return {}
    out: dict[tuple[str, str], str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = _ALLOW_LINE.match(line)
        if m is None:
            raise ValueError(f"{path}: cannot parse allow-list line {line!r}")
        out[(m["src"], m["dst"])] = (m["reason"] or "").strip()
    return out


def write_allowlist(path: Path, entries: Mapping[tuple[str, str], str]) -> None:
    header = (
        "# Upward imports allowed today. This list may only shrink:\n"
        "# delete a line when its import is gone. Format: src -> dst  # reason\n"
        "# Owner: tools/import_layers.py\n"
    )
    body = "".join(
        f"{src} -> {dst}  # {reason or TODO_REASON}\n"
        for (src, dst), reason in sorted(entries.items())
    )
    path.write_text(header + body, encoding="utf-8")


def _write(cfg: LayerConfig) -> None:
    edges = module_imports(cfg.src_root, cfg.package)
    old = read_allowlist(cfg.allowlist_path)
    live = {(e.src, e.dst) for e in upward_edges(edges, cfg.layer_of, cfg.order)}
    write_allowlist(cfg.allowlist_path, {pair: old.get(pair, "") for pair in live})
    n_lazy = len(lazy_imports(edges))
    text = cfg.toml_path.read_text(encoding="utf-8")
    cfg.toml_path.write_text(
        re.sub(r"(?m)^lazy_baseline\s*=.*$", f"lazy_baseline = {n_lazy}", text),
        encoding="utf-8",
    )
    print(
        f"{cfg.allowlist_path}: {len(live)} pairs "
        f"({len(live - old.keys())} new, {len(old.keys() - live)} dropped); "
        f"lazy_baseline = {n_lazy}"
    )


@dataclass(frozen=True)
class LayerReport:
    """Everything a package's layer test asserts, computed in one pass."""

    violations: list[str]  # upward imports not on the allow-list
    stale: list[str]  # allow-list pairs with no matching upward import
    unclassified: list[str]  # modules in no layer
    missing_reason: list[str]  # allow-list pairs whose reason is still TODO
    lazy_count: int
    lazy_baseline: int


def layer_report(cfg: LayerConfig) -> LayerReport:
    edges = module_imports(cfg.src_root, cfg.package)
    allowed = read_allowlist(cfg.allowlist_path)
    allow = set(allowed)
    return LayerReport(
        violations=check(edges, cfg.layer_of, cfg.order, allow),
        stale=[
            f"{src} -> {dst}"
            for src, dst in sorted(stale_allowed(edges, cfg.layer_of, cfg.order, allow))
        ],
        unclassified=unclassified(package_modules(cfg.src_root, cfg.package), cfg.layer_of),
        missing_reason=[
            f"{src} -> {dst}"
            for (src, dst), reason in sorted(allowed.items())
            if reason in ("", TODO_REASON)
        ],
        lazy_count=len(lazy_imports(edges)),
        lazy_baseline=cfg.lazy_baseline,
    )


def _report(cfg: LayerConfig) -> None:
    report = layer_report(cfg)
    for label, lines in [
        ("VIOLATION", report.violations),
        ("STALE", report.stale),
        ("UNCLASSIFIED", report.unclassified),
        ("NO REASON", report.missing_reason),
    ]:
        for line in lines:
            print(label, line)
    print(f"lazy imports: {report.lazy_count} (baseline {report.lazy_baseline})")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m tools.import_layers")
    parser.add_argument("config", type=Path, help="a package's import_layers.toml")
    parser.add_argument(
        "--write", action="store_true", help="regenerate the allow-list and baseline"
    )
    args = parser.parse_args(argv)
    cfg = load_config(args.config.resolve())
    (_write if args.write else _report)(cfg)


if __name__ == "__main__":
    main()
