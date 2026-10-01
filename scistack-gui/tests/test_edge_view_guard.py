"""Only domain.edge_view reads stored edges for a reader.

Step 1 of the unified edge model (.claude/plan-unified-edge-model.md,
D-2026-10-01-1). Every reader that asks "which edges does this canvas draw"
(graph build, run targets, the disconnected check, the pipeline-run skip
report, the MATLAB command, submodule interfaces) goes through
``edge_view.effective_edges``. A new reader calling
``pipeline_store.get_manual_edges`` or ``get_hidden_edge_ids`` directly is how
the five 2026-10-01 divergences started, so it fails here.

Edge MUTATORS read raw rows legitimately: they change edges, they do not
interpret them. Each is listed with its reason.
"""

from __future__ import annotations

import ast
from pathlib import Path

PKG = Path(__file__).resolve().parents[1] / "scistack_gui"

RAW_READS = {"get_manual_edges", "get_hidden_edge_ids"}

#: (module path relative to scistack_gui/, enclosing function) -> reason.
ALLOWED: dict[tuple[str, str], str] = {
    ("domain/edge_view.py", "effective_edges"): "the one owner",
    ("services/layout_service.py", "put_edge"): (
        "mutator: unhide-on-redraw checks the scope's hidden ids, and cycle "
        "detection sees every stored edge"
    ),
    ("services/layout_service.py", "delete_edge"): (
        "mutator: decides whether the id is a stored manual edge to delete"
    ),
    ("services/scope_service.py", "extract_to_submodule"): (
        "mutator: moves stored edges into the new submodule"
    ),
    ("layout.py", "read_layout"): (
        "API passthrough of stored rows (get_layout); the frontend draws "
        "get_pipeline's edges, not these"
    ),
}

#: Modules allowed anywhere: the store that defines the readers.
ALLOWED_MODULES = {"pipeline_store.py"}


class _Finder(ast.NodeVisitor):
    """Each raw read, attributed to its INNERMOST enclosing function."""

    def __init__(self, rel):
        self.rel, self.stack, self.found = rel, ["<module>"], []

    def _visit_fn(self, node):
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = visit_AsyncFunctionDef = _visit_fn

    def visit_Call(self, node):
        func = node.func
        name = (
            func.attr
            if isinstance(func, ast.Attribute)
            else func.id
            if isinstance(func, ast.Name)
            else None
        )
        if name in RAW_READS:
            self.found.append((self.rel, self.stack[-1], name, node.lineno))
        self.generic_visit(node)


def _raw_reads():
    found = []
    for path in sorted(PKG.rglob("*.py")):
        rel = path.relative_to(PKG).as_posix()
        if rel in ALLOWED_MODULES:
            continue
        finder = _Finder(rel)
        finder.visit(ast.parse(path.read_text(encoding="utf-8")))
        found.extend(finder.found)
    return found


def test_only_edge_view_and_mutators_read_stored_edges():
    unexpected = [
        f"{rel}:{line} {fn}() calls {name}"
        for rel, fn, name, line in _raw_reads()
        if (rel, fn) not in ALLOWED
    ]
    assert not unexpected, (
        "a reader of stored edges bypasses domain.edge_view.effective_edges. "
        "Use the view, or, for a mutator, add it to ALLOWED with its reason:\n"
        + "\n".join(unexpected)
    )


def test_the_allow_list_has_no_stale_entries():
    used = {(rel, fn) for rel, fn, _name, _line in _raw_reads()}
    stale = sorted(set(ALLOWED) - used)
    assert not stale, f"allow-list entries no longer needed, remove them: {stale}"
