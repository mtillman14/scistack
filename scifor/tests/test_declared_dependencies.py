"""Everything scifor's source imports must be declared in its ``pyproject.toml``.

numpy and pandas were once listed only under the ``dev`` extra. Development
installs always had them, so nothing failed there, while ``pip install scifor``
alone produced a package that raised ``ImportError`` on ``import scifor``.

The scan covers imports inside functions too: ``for_each`` imports pandas
lazily, and a run needs it just the same. Fix a failure by adding the missing
distribution to ``[project] dependencies``.
"""

import ast
import re
import sys
from collections.abc import Iterator
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib

PACKAGE = Path(__file__).resolve().parents[1]
SOURCE = PACKAGE / "src" / "scifor"

#: Modules that are not distributions to declare, with the reason.
NOT_DISTRIBUTIONS = {
    "scifor": "the package itself",
    "sre_parse": "standard library name of re._parser before Python 3.11; "
    "pathinput.py only falls back to it there, and Python 3.13 removed it",
}


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _imported_modules(node: ast.AST) -> Iterator[tuple[str, int]]:
    """Yield ``(top-level module, line)`` for each absolute import under ``node``,
    at any depth, leaving out ``if TYPE_CHECKING:`` bodies (never run)."""
    if isinstance(node, ast.If) and _is_type_checking(node.test):
        for child in node.orelse:
            yield from _imported_modules(child)
        return
    if isinstance(node, ast.Import):
        for alias in node.names:
            yield alias.name.split(".")[0], node.lineno
    elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
        yield node.module.split(".")[0], node.lineno
    for child in ast.iter_child_nodes(node):
        yield from _imported_modules(child)


def _declared_dependencies() -> set[str]:
    """The distribution names in ``[project] dependencies``, without versions
    or environment markers, normalized to their import spelling."""
    with open(PACKAGE / "pyproject.toml", "rb") as f:
        requirements = tomllib.load(f)["project"]["dependencies"]
    return {
        re.split(r"[\s;\[<>=!~]", req)[0].lower().replace("-", "_")
        for req in requirements
    }


def test_every_import_in_scifor_source_is_declared():
    declared = _declared_dependencies()
    undeclared = []
    for path in sorted(SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for module, line in _imported_modules(tree):
            if (
                module not in sys.stdlib_module_names
                and module not in NOT_DISTRIBUTIONS
                and module.lower() not in declared
            ):
                undeclared.append(f"{path.relative_to(PACKAGE)}:{line}: {module}")
    assert not undeclared, (
        "scifor imports these, but pyproject.toml [project] dependencies does "
        "not declare them:\n" + "\n".join(undeclared)
    )


def test_scan_finds_lazy_imports_and_skips_type_checking_and_relative():
    source = (
        "from typing import TYPE_CHECKING\n"
        "from .sibling import thing\n"
        "if TYPE_CHECKING:\n"
        "    import only_for_types\n"
        "def run():\n"
        "    import lazy_module.sub\n"
    )
    found = {module for module, _ in _imported_modules(ast.parse(source))}
    assert found == {"typing", "lazy_module"}
