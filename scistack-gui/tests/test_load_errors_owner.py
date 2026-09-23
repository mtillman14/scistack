"""``registry.all_load_errors`` is the one answer to "what failed to load".

Four consumers (the registry payload, the project scan, and two in the
target-file service) each built it as ``[*registry.get_load_errors(),
*matlab_registry.get_load_errors()]``. A fifth that forgot the MATLAB half
would hide every MATLAB parse error without a trace, so production code may
reach the per-language getters only through ``all_load_errors``.
"""

import ast
from pathlib import Path

from scistack_gui import matlab_registry, registry

PACKAGE = Path(__file__).resolve().parent.parent / "scistack_gui"


def test_all_load_errors_carries_both_languages():
    registry._load_errors.clear()
    matlab_registry._load_errors.clear()
    registry._record_load_error("a.py", "boom")
    matlab_registry._record_load_error("b.m", "bang")

    assert [e["source"] for e in registry.all_load_errors()] == ["a.py", "b.m"]


def test_only_the_aggregator_reads_the_per_language_lists():
    callers = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if path.name == "registry.py" and path.parent == PACKAGE:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "get_load_errors":
                callers.append(f"{path.relative_to(PACKAGE)}:{node.lineno}")
    assert callers == [], (
        "read discovery failures through registry.all_load_errors():\n"
        + "\n".join(callers)
    )
