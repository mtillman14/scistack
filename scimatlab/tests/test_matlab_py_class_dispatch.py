"""Guard: MATLAB may only name a Python class by its *defining* module.

MATLAB resolves a static call ``py.<module>.<Class>.<method>(...)`` only when
``Class`` is defined in ``<module>`` — that is, when ``Class.__module__``
equals the dotted path written in the MATLAB source. If the module merely
*re-exports* the class, MATLAB stops recognizing the path as a class, falls
back to constructor semantics, and fails at runtime with::

    Dot indexing into the result of a function call requires parentheses
    after the function name. The supported syntax is
    'py.scidb.log.Log().info'.

That is exactly what broke ``scidb.Log`` when the ``Log`` implementation moved
out of ``scidb/log.py`` into the ``scistacklog`` package: ``scidb.log`` became a
re-export shim, so ``py.scidb.log.Log.info(...)`` in ``+scidb/Log.m`` started
raising on the first MATLAB-originated log line of every run (``scidb.entities``
in the reported case). ``+scifor/Log.m`` was migrated to ``py.scistacklog.Log``
at the time; ``+scidb/Log.m`` was not.

The MATLAB tests cover the runtime behaviour but are skipped without a MATLAB
licence, so this pytest-level guard re-checks every ``py.<module>.<Class>.<attr>``
call site in the MATLAB sources on every Python test run.

Heuristic notes: whole-line and block ``%`` comments are stripped first (doc
comments legitimately quote the broken form), and only chains that resolve to a
*class* followed by a further attribute are checked — plain constructor calls
such as ``py.scidb.filters.ColumnFilter(...)`` work through a re-export and are
deliberately not flagged.
"""

import importlib
import re
import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))
sys.path.insert(0, str(_root / "scistacklog" / "src"))

import pytest

_MATLAB_ROOTS = [
    _root / "scimatlab" / "src" / "scimatlab" / "matlab",
    _root / "scimatlab" / "tests" / "matlab",
]

# py.<seg>.<seg>...(  — the full dotted chain up to the opening paren.
_PY_CHAIN = re.compile(r"\bpy\.((?:[A-Za-z_]\w*)(?:\.[A-Za-z_]\w*)+)\s*\(")

_BLOCK_COMMENT = re.compile(r"^\s*%[{}]\s*$")


def _strip_comments(text: str) -> str:
    """Drop whole-line and %{ %} block comments from MATLAB source."""
    out = []
    in_block = False
    for line in text.splitlines():
        if _BLOCK_COMMENT.match(line):
            in_block = "{" in line
            continue
        if in_block or line.lstrip().startswith("%"):
            continue
        out.append(line)
    return "\n".join(out)


def _matlab_files():
    files = []
    for root in _MATLAB_ROOTS:
        files.extend(sorted(root.rglob("*.m")))
    return files


def _split_module(chain: str):
    """Longest importable module prefix of ``chain`` → (module, [attrs])."""
    parts = chain.split(".")
    for cut in range(len(parts) - 1, 0, -1):
        name = ".".join(parts[:cut])
        try:
            module = importlib.import_module(name)
        except Exception:
            continue
        return name, module, parts[cut:]
    return None, None, None


def _class_attr_call_sites():
    """Yield (file, lineno, module_path, class_name, attr) for static calls."""
    for path in _matlab_files():
        source = _strip_comments(path.read_text(encoding="utf-8", errors="replace"))
        for lineno, line in enumerate(source.splitlines(), start=1):
            for match in _PY_CHAIN.finditer(line):
                module_path, module, attrs = _split_module(match.group(1))
                if module is None or len(attrs) < 2:
                    continue
                obj = getattr(module, attrs[0], None)
                if not isinstance(obj, type):
                    continue
                yield path, lineno, module_path, attrs[0], attrs[1]


def test_matlab_files_exist():
    assert _matlab_files(), "no MATLAB sources found — check _MATLAB_ROOTS"


def test_py_class_static_calls_name_the_defining_module():
    violations = []
    for path, lineno, module_path, cls_name, attr in _class_attr_call_sites():
        cls = getattr(importlib.import_module(module_path), cls_name)
        if cls.__module__ != module_path:
            violations.append(
                f"{path.relative_to(_root)}:{lineno}: "
                f"py.{module_path}.{cls_name}.{attr}(...) — {cls_name} is defined in "
                f"'{cls.__module__}', not '{module_path}'. MATLAB cannot dispatch a "
                f"static call through a re-export; write "
                f"py.{cls.__module__}.{cls_name}.{attr}(...) instead."
            )
    assert not violations, "MATLAB static calls through re-exported classes:\n" + "\n".join(
        violations
    )


def test_py_class_static_calls_target_existing_attributes():
    missing = []
    for path, lineno, module_path, cls_name, attr in _class_attr_call_sites():
        cls = getattr(importlib.import_module(module_path), cls_name)
        if not hasattr(cls, attr):
            missing.append(
                f"{path.relative_to(_root)}:{lineno}: "
                f"py.{module_path}.{cls_name}.{attr} does not exist"
            )
    assert not missing, "MATLAB calls a Python attribute that is gone:\n" + "\n".join(missing)


@pytest.mark.parametrize("package", ["+scidb", "+scifor"])
def test_matlab_log_delegates_to_scistacklog(package):
    """Pin the specific regression: neither Log.m may route through scistack.log."""
    src = (_MATLAB_ROOTS[0] / package / "Log.m").read_text(encoding="utf-8")
    code = _strip_comments(src)
    assert "py.scistacklog.Log" in code, f"{package}/Log.m must delegate to py.scistacklog.Log"
    assert "py.scidb.log.Log" not in code, (
        f"{package}/Log.m routes logging through the scidb.log re-export shim; "
        "MATLAB static dispatch requires the defining module (scistacklog)"
    )
