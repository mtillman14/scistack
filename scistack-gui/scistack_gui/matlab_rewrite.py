"""
Qualifying MATLAB calls when code moves into a library package -- the one
owner (``.claude/plan-library-fork-chain.md`` O9; moved out of
``services/library_share._MatlabPlan``).

A MATLAB library keeps its functions in ``matlab/+<lib>/``, so inside it a
call must name the package: ``smooth_emg(x)`` becomes ``<lib>.smooth_emg(x)``
and ``@smooth_emg`` becomes ``@<lib>.smooth_emg``. Copying a library under a
new name requalifies the old package too: ``old.smooth_emg(x)`` ->
``new.smooth_emg(x)``. ``function`` declaration lines, comments and strings
are never touched.

The Python counterpart is ``code_rewrite.rewrite_imports``. Both are
consumed only by the authoring plan (``services/library_authoring``), which
decides which names move where.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_FUNCTION_LINE = re.compile(r"^\s*function\b")


@dataclass
class QualifyResult:
    text: str
    #: ``"file:line: old -> new"`` per changed line.
    changes: list[str] = field(default_factory=list)


def code_part(line: str) -> "tuple[str, str]":
    """``(code, comment)``: split at the first ``%`` outside a string."""
    in_single = in_double = False
    for i, ch in enumerate(line):
        if ch == "'" and not in_double:
            # A transpose (x') is not a string start; good enough: strings
            # follow an operator, a comma, a paren or whitespace.
            if in_single or i == 0 or line[i - 1] in " =(,;[{+-*/":
                in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "%" and not in_single and not in_double:
            return line[:i], line[i:]
    return line, ""


def call_pattern(name: str) -> str:
    """A bare call of *name* (not a member, not a handle, not a longer name)."""
    return rf"(?<![\w.@]){re.escape(name)}(?=\s*\()"


def code_only(text: str) -> str:
    """*text* without comments and ``function`` lines (for call scans)."""
    return "\n".join(
        code_part(line)[0] for line in text.splitlines() if not _FUNCTION_LINE.match(line)
    )


def calls(text: str, name: str) -> bool:
    """Whether *text* calls *name* bare (a dependency to carry along)."""
    return re.search(call_pattern(name), code_only(text)) is not None


def qualify(
    text: str, lib: str, names, *, old_packages=(), where: str = ""
) -> QualifyResult:
    """Qualify every call / handle of *names* (bare function names, possibly
    ``sub.f`` inside ``+sub``) as ``<lib>.<name>``, and requalify
    ``<old>.<name>`` for each package in *old_packages*."""
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.split(newline)
    result = QualifyResult(text)
    names = sorted(set(names), key=len, reverse=True)
    for i, line in enumerate(lines):
        if _FUNCTION_LINE.match(line):
            continue
        code, comment = code_part(line)
        new = code
        for old in old_packages:
            for name in names:
                new = re.sub(
                    rf"(?<![\w.]){re.escape(old)}\.{re.escape(name)}\b", f"{lib}.{name}", new
                )
        for name in names:
            new = re.sub(call_pattern(name), f"{lib}.{name}", new)
            new = re.sub(rf"@{re.escape(name)}\b", f"@{lib}.{name}", new)
        if new != code:
            result.changes.append(f"{where}:{i + 1}: {code.strip()} -> {new.strip()}")
            lines[i] = new + comment
    result.text = newline.join(lines)
    return result
