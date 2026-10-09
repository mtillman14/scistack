"""
Rewriting the absolute imports of Python source when code moves between
packages -- the one owner (portability Stage 10).

Used when a submodule's code is shared as a library (``services/
library_share``: the project's package -> the library's) and when a library
is copied into a project (``services/library_copy``: the library -> a
subpackage of the project). The caller says where each module goes
(*resolve*: old module name -> new one, or ``None`` to leave it); this module
finds every ``import`` / ``from ... import`` that names one (by AST, so
strings and comments are never touched) and rewrites that statement in
place, keeping the names the code binds:

* ``import m`` / ``import m as k`` -> ``from <new parent> import <new last>``
  [``as k``]: ``m`` (or ``k``) is still bound;
* ``import a.b as k`` -> ``import <new> as k``;
* ``import a.b`` -> ``import <new>`` -- the bound top-level name changes, so
  it is reported as a warning for the user to finish by hand;
* ``from a.b import x`` -> ``from <new> import x``.

Relative imports are left alone: they stay valid when a package moves as a
whole.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class RewriteResult:
    text: str
    #: ``"old statement -> new statement"`` per rewritten import.
    changes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def rewrite_imports(text: str, resolve: "Callable[[str], str | None]", *, where: str = "") -> RewriteResult:
    tree = ast.parse(text)
    result = RewriteResult(text)
    edits: list[tuple[int, int, int, int, str]] = []
    for node in ast.walk(tree):
        src = None
        if isinstance(node, ast.Import):
            src = _import(node, resolve, result, where)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            new = resolve(node.module)
            if new is not None and new != node.module:
                src = ast.unparse(ast.ImportFrom(module=new, names=node.names, level=0))
        if src is not None:
            result.changes.append(f"{where}: {ast.unparse(node)} -> {src}")
            edits.append((node.lineno, node.col_offset, node.end_lineno, node.end_col_offset, src))
    if not edits:
        return result
    lines = text.splitlines(keepends=True)
    for l0, c0, l1, c1, src in sorted(edits, reverse=True):
        first, last = lines[l0 - 1], lines[l1 - 1]
        lines[l0 - 1 : l1] = [first[:c0] + src + last[c1:]]
    result.text = "".join(lines)
    return result


def _import(node: ast.Import, resolve, result: RewriteResult, where: str) -> "str | None":
    stmts: list[str] = []
    kept: list[ast.alias] = []
    changed = False
    for a in node.names:
        new = resolve(a.name)
        if new is None or new == a.name:
            kept.append(a)
            continue
        changed = True
        head, _, last = new.rpartition(".")
        if "." not in a.name and head:
            stmts.append(f"from {head} import {last}" + (f" as {a.asname}" if a.asname else ""))
        elif a.asname:
            stmts.append(f"import {new} as {a.asname}")
        else:
            stmts.append(f"import {new}")
            result.warnings.append(
                f"{where}: 'import {a.name}' became 'import {new}'; code that refers to "
                f"'{a.name.split('.')[0]}.…' must be updated by hand"
            )
    if not changed:
        return None
    if kept:
        stmts.insert(0, "import " + ", ".join(a.name + (f" as {a.asname}" if a.asname else "") for a in kept))
    return "; ".join(stmts)
