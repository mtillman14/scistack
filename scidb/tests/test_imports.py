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

import subprocess
import sys
from pathlib import Path

import pytest

# Appended, not prepended: the repo root holds a directory per package, and
# those must not shadow the installed packages.
sys.path.append(str(Path(__file__).resolve().parents[2]))

from tools.import_layers import module_imports  # noqa: E402

PKG_DIR = Path(__import__("scidb").__file__).parent
SRC = PKG_DIR.parent
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

# ``{importer: {target: reason}}`` — the cycle-dodging lazies we keep, by
# full module name.
ALLOWED_CYCLE_DODGES: dict[str, dict[str, str]] = {
    "scidb.model.input_spec": {
        "scidb.model.variant": "the leaf that names wrapper stacks must know the wrapper "
        "types; the wrappers import it for their display names, so the "
        "types are imported at call time (documented in the module)",
        "scidb.model.across_variants": "same as variant",
    },
    "scidb.model.variable": {
        "scidb.store.database": "BaseVariable.load/save reach the ambient database at "
        "CALL time; database imports the variable registry at import time",
    },
    "scidb.run.pipeline": {
        "scidb.run.foreach": "a Pipeline RUNS for_each; for_each consults the active "
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
#
# Nodes are scidb's modules outside the ``inspect`` and ``project``
# subpackages (the scope this test has always had: those two are checked by
# the layer test, ``test_import_layers.py``). The package ``__init__`` files
# are not nodes: the facade imports everything, and the cluster packages are
# empty. Edges come from ``tools.import_layers`` (relative imports resolved,
# ``if TYPE_CHECKING:`` blocks marked).


def _in_scope(module: str) -> bool:
    if module == PKG or not module.startswith(PKG + "."):
        return False
    if module.split(".")[1] in ("inspect", "project"):
        return False
    path = PKG_DIR.joinpath(*module.split(".")[1:])
    return not path.is_dir()


def _edges():
    return [
        e
        for e in module_imports(SRC, PKG)
        if _in_scope(e.src) and _in_scope(e.dst) and e.dst != e.src and not e.type_only
    ]


def _top_level_graph() -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {m: set() for m in MODULES if _in_scope(m)}
    for e in _edges():
        if not e.lazy:
            graph[e.src].add(e.dst)
    return graph


def _reaches(graph, a, b, seen=None) -> bool:
    if a == b:
        return True
    seen = seen or set()
    if a in seen:
        return False
    seen.add(a)
    return any(_reaches(graph, n, b, seen) for n in graph.get(a, ()))


def test_scope_is_not_empty():
    """The checks below are vacuous if the module walk finds nothing."""
    graph = _top_level_graph()
    assert "scidb.run.foreach" in graph and "scidb.store.database" in graph


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
    for e in _edges():
        if e.lazy and _reaches(graph, e.dst, e.src):
            found.setdefault(e.src, set()).add(e.dst)
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
