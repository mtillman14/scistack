"""THE owner of "which of a provider's modules may a consumer package import".

Each consumer's ``test_scidb_boundary.py`` reads its imports through
``boundary_violations`` and checks them against ``scidb.SUPPORTED_MODULES``. It
parses source with ``ast`` only (no imports, no side effects), so the check
stays fast and runs without the package's dependencies installed.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Edge:
    """One imported module, as written at one line of one module."""

    src: str
    dst: str
    lineno: int


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
    """Every import in one file, at any depth: a function-level or
    ``if TYPE_CHECKING:`` import counts like a top-level one."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    is_package = path.name == "__init__.py"
    edges: list[Edge] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                edges.append(Edge(module, alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_base(module, is_package, node)
            targets = {
                f"{base}.{a.name}" if f"{base}.{a.name}" in known else base
                for a in node.names
            }
            for dst in sorted(targets):
                edges.append(Edge(module, dst, node.lineno))
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
