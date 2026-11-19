"""Only domain.visibility interprets hides; readers ask it.

docs/claude/visibility-model.md. Before it, about fifteen readers each chose
whose hides applied (the asking canvas with a ``strip_placement`` fallback,
every canvas unioned, the function node's canvas), and a hide inside a
submodule cut the parent's lines into its ports (scistack-debug.log
2026-11-19 12:38:43). A reader calling a raw hide read directly is how that
starts again, so it fails here. An owner guard, not an import pin.

Raw per-canvas reads are allowed in the stores (they define them) and in the
owner. Listing one canvas's own hides for display (the restore panel, the
Parameter checkboxes, copying a canvas) is listed with its reason.
"""

from __future__ import annotations

import ast
from pathlib import Path

PKG = Path(__file__).resolve().parents[1] / "scistack_gui"

RAW_HIDE_READS = {
    "get_hidden_node_ids",
    "get_hidden_edge_ids",
    "list_hidden_parameter_values",
    "hidden_node_ids",
    "hidden_edges",
    "hidden_parameter_values",
    "hides_by_scope",
}

#: Modules allowed anywhere: the stores that define the reads, and the owner.
ALLOWED_MODULES = {
    "stores/pipeline_store.py",
    "stores/intent_store.py",
    "domain/visibility.py",
}

#: (module, enclosing function) -> reason.
ALLOWED: dict[tuple[str, str], str] = {
    ("services/layout_service.py", "get_hidden_constant_values"): (
        "lists one canvas's own unticked values for its Parameter checkboxes"
    ),
}


class _Finder(ast.NodeVisitor):
    def __init__(self, rel):
        self.rel, self.stack, self.found = rel, ["<module>"], []

    def _visit_fn(self, node):
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = visit_AsyncFunctionDef = _visit_fn

    def visit_Call(self, node):
        func = node.func
        if isinstance(func, ast.Attribute):
            name = func.attr
            # `vis.hidden_node_ids(canvas)` is the owner's own API: only a
            # module-level store read counts.
            owner = func.value
            if not (isinstance(owner, ast.Name) and owner.id in _STORE_NAMES):
                name = None
        elif isinstance(func, ast.Name):
            name = func.id
        else:
            name = None
        if name in RAW_HIDE_READS:
            self.found.append((self.rel, self.stack[-1], name, node.lineno))
        self.generic_visit(node)


#: The names the stores are imported under in this package.
_STORE_NAMES = {"pipeline_store", "intent_store", "ps", "_ps", "_ps_hidden", "_store"}


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


def test_only_the_visibility_owner_reads_hides():
    unexpected = [
        f"{rel}:{line} {fn}() calls {name}"
        for rel, fn, name, line in _raw_reads()
        if (rel, fn) not in ALLOWED
    ]
    assert not unexpected, (
        "a reader interprets hides itself instead of asking "
        "domain.visibility.Visibility (docs/claude/visibility-model.md). Ask the "
        "owner, or, for a display listing of one canvas, add it to ALLOWED with "
        "its reason:\n" + "\n".join(unexpected)
    )


def test_the_allow_list_has_no_stale_entries():
    used = {(rel, fn) for rel, fn, _name, _line in _raw_reads()}
    stale = sorted(set(ALLOWED) - used)
    assert not stale, f"allow-list entries no longer needed, remove them: {stale}"
