"""The MATLAB bridge uses scidb only through public names (cleanup-audit F2).

It used to import six private ``scidb.foreach`` functions, so a rename inside
``foreach.py`` could break every MATLAB run with nothing in scidb marking them
as a contract. The contract is now ``scidb.external_loop``.
"""

import ast
from pathlib import Path

BRIDGE = Path(__file__).resolve().parent.parent / "src" / "scimatlab" / "bridge.py"


def test_the_bridge_imports_nothing_private_from_scidb():
    tree = ast.parse(BRIDGE.read_text(encoding="utf-8"))
    offenders = [
        f"line {node.lineno}: from {node.module} import {alias.name}"
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and (node.module or "").split(".")[0] in {"scidb", "sciduckdb", "scifor"}
        for alias in node.names
        if alias.name.startswith("_")
    ]
    assert not offenders, offenders


def test_the_seam_exports_what_the_bridge_calls():
    from scidb import external_loop

    for name in (
        "prepare",
        "save_resolved",
        "build_skip_hook",
        "resolve_for_columns",
        "endpoint_policy",
        "apply_introspect",
    ):
        assert callable(getattr(external_loop, name)), name
