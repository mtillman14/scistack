"""Guard: every ``scidb.Log.<name>`` / ``scifor.Log.<name>`` call site exists.

MATLAB resolves static-method names at *call* time, so a typo or a drifted name
survives until the line actually executes — and logging lines execute least
often, typically inside a ``catch`` block. That is the worst place for it:

    catch scistack_err__
        scidb.Log.error('MATLAB: for_each FAILED: %s', scistack_err__.message);
        ...
        rethrow(scistack_err__);
    end

``scidb.Log`` has no ``error`` method (it is named ``err`` to stay clear of
MATLAB's builtin ``error``), so the handler itself threw "The class scidb.Log
has no Constant property or Static method named 'error'", the ``rethrow`` never
ran, and the *real* for_each failure was replaced by a logging bug. Hit
2026-09-01 on a GUI-generated run script.

This scans both hand-written MATLAB sources and the MATLAB text that
``scistack_gui`` emits as Python string literals, and checks every referenced
name against the public surface parsed out of ``+scidb/Log.m`` / ``+scifor/Log.m``.
"""

import re
from pathlib import Path

import pytest

_root = Path(__file__).parent.parent.parent
_MATLAB_SRC = _root / "scimatlab" / "src" / "scimatlab" / "matlab"

# Roots whose sources may reference the Log classes. GUI code is included
# because it generates MATLAB text; missing roots are skipped, not failed.
_SCAN_ROOTS = [
    _MATLAB_SRC,
    _root / "scimatlab" / "tests" / "matlab",
    _root / "scistack-gui" / "scistack_gui",
    _root / "examples",
]

_LOG_REF = re.compile(r"\b(scidb|scifor)\.Log\.(\w+)")
_STATIC_BLOCK = re.compile(r"^\s*methods\s*\(\s*Static\s*\)", re.MULTILINE)
_PRIVATE_BLOCK = re.compile(r"^\s*methods\s*\([^)]*Access\s*=\s*private", re.MULTILINE)
_FUNCTION_DEF = re.compile(
    r"^\s*function\s+(?:\[[^\]]*\]\s*=\s*|[\w~]+\s*=\s*)?(\w+)\s*\(", re.MULTILINE
)
_CONSTANT_BLOCK = re.compile(r"^\s*properties\s*\(\s*Constant\s*\)(.*?)^\s*end", re.MULTILINE | re.DOTALL)
_CONSTANT_DEF = re.compile(r"^\s*(\w+)\s*=", re.MULTILINE)

_BLOCK_COMMENT = re.compile(r"^\s*%[{}]\s*$")


def _strip_matlab_comments(text: str) -> str:
    """Drop whole-line and %{ %} block comments (doc comments quote bad forms)."""
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


_SUBCLASS = re.compile(r"^\s*classdef\s+Log\s*<\s*(\w+)\.Log\b", re.MULTILINE)


def _public_surface(package: str) -> set[str]:
    """Public static methods + constant properties of <package>/Log.m.

    A ``classdef Log < other.Log`` file inherits its parent's surface
    (``+scidb/Log.m`` is a member-free subclass of ``scifor.Log``).
    """
    src = (_MATLAB_SRC / package / "Log.m").read_text(encoding="utf-8")
    code = _strip_matlab_comments(src)
    parent = _SUBCLASS.search(code)
    if parent:
        return _public_surface("+" + parent.group(1))

    names: set[str] = set()
    for block in _CONSTANT_BLOCK.finditer(code):
        names.update(_CONSTANT_DEF.findall(block.group(1)))

    static = _STATIC_BLOCK.search(code)
    assert static, f"{package}/Log.m: no 'methods (Static)' block found"
    private = _PRIVATE_BLOCK.search(code)
    end = private.start() if private else len(code)
    names.update(_FUNCTION_DEF.findall(code[static.end() : end]))

    assert names, f"{package}/Log.m: parsed an empty public surface"
    return names


def _scan_files():
    for root in _SCAN_ROOTS:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if path.suffix in {".m", ".py"} and "node_modules" not in path.parts:
                yield path


def _references():
    """Yield (path, lineno, package, name) for every Log reference found."""
    for path in _scan_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        if path.suffix == ".m":
            text = _strip_matlab_comments(text)
        for lineno, line in enumerate(text.splitlines(), start=1):
            for package, name in _LOG_REF.findall(line):
                yield path, lineno, package, name


@pytest.mark.parametrize("package", ["+scidb", "+scifor"])
def test_public_surface_is_parseable(package):
    surface = _public_surface(package)
    # The documented API — if any of these vanish, callers break silently.
    assert {"debug", "info", "warn", "err", "set_level", "get_level"} <= surface


def test_every_log_call_site_resolves():
    surface = {"scidb": _public_surface("+scidb"), "scifor": _public_surface("+scifor")}
    bad = []
    for path, lineno, package, name in _references():
        if name not in surface[package]:
            hint = ""
            if name == "error":
                hint = " — use 'err' (named to stay clear of MATLAB's builtin error)"
            bad.append(
                f"{path.relative_to(_root)}:{lineno}: {package}.Log.{name} "
                f"is not a static method or constant property of {package}/Log.m{hint}"
            )
    assert not bad, (
        "MATLAB code references a Log member that does not exist (fails only at "
        "runtime, often inside a catch block):\n" + "\n".join(bad)
    )


def test_generated_matlab_error_handlers_use_err():
    """Pin the exact regression: GUI-generated catch blocks must call Log.err."""
    src = (
        _root / "scistack-gui" / "scistack_gui" / "services" / "matlab_command_format.py"
    ).read_text(encoding="utf-8")
    assert "scidb.Log.err(" in src, "generated MATLAB should log failures via Log.err"
    assert "scidb.Log.error(" not in src, (
        "generated MATLAB calls scidb.Log.error, which does not exist — the catch "
        "block throws and swallows the original error before rethrow()"
    )


@pytest.mark.parametrize("package", ["+scifor"])  # scidb.Log inherits these
def test_level_methods_guard_against_double_sprintf(package):
    """``Log.<level>`` must not re-format an already-formatted message.

    Callers legitimately pre-build a message —
    ``scidb.Log.info(sprintf('... %s', p))`` in ``+scidb/entities.m`` — and a
    second ``sprintf`` pass then interprets the result's backslashes as
    escapes. A Windows path did exactly that on 2026-09-01:

        Warning: Escaped character '\\L' is not valid.
        [matlab] [entities] Materialized 1 MATLAB classdef(s) in y:

    — the message was truncated at ``y:``, losing ``\\LabMembers\\...``.

    Verified structurally: exercising it needs MATLAB, which is not available
    in this test environment.
    """
    src = (_MATLAB_SRC / package / "Log.m").read_text(encoding="utf-8")
    code = _strip_matlab_comments(src)

    for level in ("debug", "info", "warn", "err"):
        m = re.search(
            rf"function {level}\(fmt, varargin\)(.*?)^        end",
            code,
            re.DOTALL | re.MULTILINE,
        )
        assert m, f"{package}/Log.m: could not locate the {level}() body"
        body = m.group(1)
        assert "nargin == 1" in body, (
            f"{package}/Log.m: {level}() calls sprintf unconditionally — a "
            "pre-formatted message containing a Windows path will be "
            "silently truncated at the first backslash escape"
        )
