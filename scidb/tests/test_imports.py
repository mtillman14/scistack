"""Import structure is a fact of the package, guarded like any other.

Two tests:

1. **Every module imports first.** ``import scidb.X`` in a fresh interpreter,
   for every X, in isolation. A cycle that only bites in one import order
   (``foreach`` before ``database`` but not after) is invisible to a suite
   whose ``conftest`` has already imported everything; a subprocess per
   module is the only honest probe.

2. **No new cycle-dodging lazy imports.** A function-level import of a
   sibling module that could NOT be hoisted to the top of the file without
   creating an import cycle is a cycle-dodge. Each surviving one is on the
   allow-list below WITH its reason, so the next one added has to be argued
   for here rather than slipped in. (A lazy import of a module that COULD
   be hoisted is a style matter, not a cycle, and is not tested.)

The 2026-09-20 sweep (architecture plan, Stage 3) removed the rest: the
role classifier went to ``roles``, the spec helpers to ``input_spec``, the
schema-string rule to ``schema_values``, the per-combo sentinels to
``per_combo``, and ``database`` now imports ``provenance_query`` at the top.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

PKG_DIR = Path(__import__("scidb").__file__).parent
PKG = "scidb"


def _module_names() -> list[str]:
    names = []
    for p in sorted(PKG_DIR.rglob("*.py")):
        rel = p.relative_to(PKG_DIR).with_suffix("")
        parts = list(rel.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if "__pycache__" in parts:
            continue
        names.append(".".join([PKG, *parts]) if parts else PKG)
    return names


MODULES = _module_names()

# ``{importer: {target: reason}}`` — the cycle-dodging lazies we keep.
# A target here is the top-level sibling module (``scidb.<target>``).
ALLOWED_CYCLE_DODGES: dict[str, dict[str, str]] = {
    "scidb.input_spec": {
        "variant": "the leaf that names wrapper stacks must know the wrapper "
        "types; the wrappers import it for their display names, so the "
        "types are imported at call time (documented in the module)",
        "across_variants": "same as variant",
    },
    "scidb.variable": {
        "database": "BaseVariable.load/save reach the ambient database at "
        "CALL time; database imports the variable registry at import time",
    },
    "scidb.pipeline": {
        "foreach": "a Pipeline RUNS for_each; for_each consults the active "
        "pipeline at import time (registry direction), the run direction "
        "is resolved when a step executes",
    },
}


@pytest.mark.parametrize("module", MODULES)
def test_every_module_imports_first(module):
    """``import scidb.<module>`` succeeds in a fresh interpreter, before any
    other scidb module has been imported."""
    proc = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, (
        f"import {module} failed in a fresh interpreter:\n{proc.stderr[-3000:]}"
    )


# --- static analysis -------------------------------------------------------


def _sibling(node: ast.ImportFrom, importer: str) -> list[str]:
    """Top-level sibling module names an import statement pulls in, or []."""
    out = []
    if node.level == 1 and node.module is None:
        # ``from . import x, y``
        out.extend(alias.name for alias in node.names)
    elif node.level == 1 and node.module is not None:
        out.append(node.module.split(".")[0])
    elif node.level == 0 and node.module and node.module.split(".")[0] == PKG:
        parts = node.module.split(".")
        if len(parts) >= 2:
            out.append(parts[1])
        else:
            out.extend(alias.name for alias in node.names)
    elif node.level == 2 and importer.count(".") >= 2:
        # ``from ..x import`` inside a subpackage
        out.append(node.module.split(".")[0] if node.module else "")
    return [o for o in out if (PKG_DIR / f"{o}.py").exists() or (PKG_DIR / o).is_dir()]


def _imports_of(module: str) -> tuple[set[str], set[str]]:
    """(top-level sibling imports, function-level sibling imports)."""
    rel = module.split(".")[1:]
    path = PKG_DIR.joinpath(*rel)
    path = path / "__init__.py" if path.is_dir() else path.with_suffix(".py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    top: set[str] = set()
    lazy: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            top.update(_sibling(node, module))
        elif isinstance(node, ast.If):
            # ``if TYPE_CHECKING:`` blocks do not import at runtime
            continue
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for sub in ast.walk(node):
                if isinstance(sub, ast.ImportFrom):
                    lazy.update(_sibling(sub, module))
    return top, lazy


def _top_level_graph() -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {}
    for m in MODULES:
        if m == PKG or m.count(".") > 1:
            continue  # the package __init__ and subpackages are not nodes
        name = m.split(".")[1]
        graph[name] = _imports_of(m)[0]
    return graph


def _reaches(graph, a, b, seen=None) -> bool:
    if a == b:
        return True
    seen = seen or set()
    if a in seen:
        return False
    seen.add(a)
    return any(_reaches(graph, n, b, seen) for n in graph.get(a, ()))


def test_no_top_level_import_cycles():
    graph = _top_level_graph()
    cycles = sorted(
        f"{a} -> {b} -> ... -> {a}"
        for a, targets in graph.items()
        for b in targets
        if b != a and _reaches(graph, b, a)
    )
    assert not cycles, "top-level import cycles:\n" + "\n".join(cycles)


def test_cycle_dodging_lazy_imports_are_all_on_the_allow_list():
    graph = _top_level_graph()
    found: dict[str, set[str]] = {}
    for m in MODULES:
        if m == PKG or m.count(".") > 1:
            continue
        name = m.split(".")[1]
        _, lazy = _imports_of(m)
        for target in lazy:
            if target == name:
                continue
            if _reaches(graph, target, name):
                found.setdefault(m, set()).add(target)
    unexpected = {
        m: sorted(t for t in targets if t not in ALLOWED_CYCLE_DODGES.get(m, {}))
        for m, targets in found.items()
    }
    unexpected = {m: t for m, t in unexpected.items() if t}
    assert not unexpected, (
        "function-level imports that dodge an import cycle (hoisting them "
        "would create one). Move the shared thing to a leaf module, or add "
        f"the dodge to ALLOWED_CYCLE_DODGES with its reason:\n{unexpected}"
    )
    stale = {
        m: sorted(t for t in allowed if t not in found.get(m, set()))
        for m, allowed in ALLOWED_CYCLE_DODGES.items()
    }
    stale = {m: t for m, t in stale.items() if t}
    assert not stale, f"allow-list entries no longer needed — remove them:\n{stale}"
