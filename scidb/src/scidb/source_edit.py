"""
Locating and rewriting entity declarations in source files.

This is the **write** half of source-declared entities; ``discover.py`` and
``scistack_gui.registry`` are the read half. It lives in ``scidb`` because
``scidb`` owns the declaration grammar -- it defines ``constant()``,
``Sweep`` and ``PathInput``, so it is the layer that knows what a
declaration of one looks like (CLAUDE.md NOTE 3). Everything here is pure:
no file I/O, no GUI imports, no registry access. Policy -- which file may
be written, staleness checks, atomic writes, re-scan verification -- belongs
to the caller (``scistack_gui.services.target_file_service``).

Two capabilities:

1. :func:`find_binding_span` -- where in a source file is the value of the
   top-level binding ``NAME = ...``? Returned as character offsets over the
   whole RHS *expression*, which is deliberately wider than "the literal":
   swapping ``scidb.Constant(2)`` for ``scidb.Sweep(2, 5)`` is then the
   same splice as changing a value (see
   ``docs/claude/entity-editability-model.md``, D4/D5).
2. ``render_*`` -- emit the canonical declaration text for each kind. These
   were previously open-coded in ``constant_service`` and
   ``path_input_service``; both now call in here so creation and editing
   can never drift apart in what they write.

See ``.claude/plan-gui-entity-editing-26-08-24.md`` Stage 1.
"""

from __future__ import annotations

import ast
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

DEFAULT_QUALIFIER = "scidb."


@dataclass(frozen=True)
class Span:
    """A half-open ``[start, end)`` range of **character** offsets into a
    source string (not bytes -- see ``_char_offset`` for why that
    distinction needs care)."""

    start: int
    end: int

    def extract(self, text: str) -> str:
        """The substring this span covers -- the caller's ``old`` value when
        logging an edit."""
        return text[self.start : self.end]


# ---------------------------------------------------------------------------
# Locating a declaration
# ---------------------------------------------------------------------------


def _char_offset(lines: list[str], line_starts: list[int], lineno: int, col: int) -> int:
    """Convert an ``ast`` ``(lineno, col_offset)`` pair to a character
    offset into the original text.

    ``col_offset`` is a **UTF-8 byte** offset within its line, not a
    character offset -- they diverge the moment a declaration contains a
    non-ASCII character (``PathInput('{sujet}/données.csv')`` is entirely
    plausible). Re-encoding the line and decoding the prefix converts one
    to the other without assuming ASCII.
    """
    line = lines[lineno - 1]
    char_col = len(line.encode("utf-8")[:col].decode("utf-8", errors="ignore"))
    return line_starts[lineno - 1] + char_col


def find_binding_span(text: str, name: str) -> "Span | None":
    """Span of the RHS expression of the top-level binding ``name = <expr>``
    in *text*, or ``None`` if there is no such binding.

    Deliberate rules:

    - **Top-level only.** A binding nested in a function or ``if`` block is
      not what the registry scanners pick up (they read ``vars(module)``
      after import), so it is not editable either.
    - **Last binding wins.** If a name is bound more than once at module
      level, the final one is what the module ends up holding after
      execution -- the same value discovery sees.
    - **Simple targets only.** ``a = b = ...`` and ``a, b = ...`` return
      ``None``: there is no single RHS that belongs to *this* name, so
      there is nothing safe to splice.
    - ``x: T = v`` (``AnnAssign``) is supported; a bare ``x: T`` with no
      value is not (nothing to edit).

    Returns ``None`` rather than raising on a syntax error, so a caller
    handling a half-written file degrades to "not editable" instead of
    crashing.
    """
    try:
        tree = ast.parse(text)
    except SyntaxError as e:
        logger.warning(
            "[source_edit] Cannot parse source while looking for %r: %s", name, e
        )
        return None

    lines = text.splitlines(keepends=True)
    line_starts: list[int] = []
    offset = 0
    for line in lines:
        line_starts.append(offset)
        offset += len(line)

    found: "Span | None" = None
    for node in tree.body:
        value = None
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1:
                continue
            target = node.targets[0]
            if not isinstance(target, ast.Name) or target.id != name:
                continue
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            if not isinstance(node.target, ast.Name) or node.target.id != name:
                continue
            if node.value is None:
                continue
            value = node.value
        else:
            continue

        if value.end_lineno is None or value.end_col_offset is None:
            continue
        found = Span(
            _char_offset(lines, line_starts, value.lineno, value.col_offset),
            _char_offset(lines, line_starts, value.end_lineno, value.end_col_offset),
        )

    if found is None:
        logger.debug("[source_edit] No top-level binding named %r found", name)
    return found


def line_number(text: str, offset: int) -> int:
    """1-based line containing *offset*, for user-facing "declared in
    ``foo.py:42``" messages. Language-agnostic — the MATLAB side reports
    getter locations through this too."""
    return text.count("\n", 0, max(0, offset)) + 1


def splice(text: str, span: Span, replacement: str) -> str:
    """*text* with *span* replaced by *replacement*. Everything outside the
    span -- comments, formatting, other declarations -- is preserved byte
    for byte, which is the whole reason this is a span splice rather than
    an ``ast.unparse`` round-trip."""
    return text[: span.start] + replacement + text[span.end :]


# ---------------------------------------------------------------------------
# Rendering a declaration
# ---------------------------------------------------------------------------
#
# ``qualifier`` exists because the same expression is written in two
# contexts: an entities file, which uses the qualified ``scidb.X(...)`` form
# so it never depends on what the file happens to import
# (target_file_service.ensure_scidb_import only guarantees a bare
# ``import scidb``), and a generated standalone script, which imports names
# directly. Callers in the first context take the default.


def render_parameter(
    values, description: str = "", *, qualifier: str = DEFAULT_QUALIFIER
) -> str:
    """``scidb.Parameter(2, description='')`` or ``scidb.Parameter(0, 1, 2)``.

    One positional argument per value, so adding a value is adding an
    argument -- never a change of form (D6). ``description`` is always
    emitted, even when empty, so a round-tripped declaration is textually
    stable.
    """
    args = [repr(v) for v in values]
    args.append(f"description={description!r}")
    return f"{qualifier}Parameter({', '.join(args)})"


def render_path_input(
    template: str,
    root_folder: "str | None" = None,
    alternates: "list[dict] | None" = None,
    *,
    name: str,
    qualifier: str = DEFAULT_QUALIFIER,
) -> str:
    """``scidb.PathInput('{subject}/x.csv', name='RawData')``, or, with *alternates*,
    ``scidb.EachOf(scidb.PathInput(...), scidb.PathInput(...))``.

    The ``EachOf``-of-``PathInput``\\ s form is how alternate templates are
    expressed -- there is no separate concept (see
    ``docs/claude/code-discovery-categories.md`` §4). Each alternate is a
    ``{"template", "root_folder"}`` dict.

    ``root_folder`` is omitted when falsy rather than written as
    ``root_folder=None``, so the common single-argument declaration stays
    readable.
    """
    calls = [_path_input_call(template, root_folder, name, qualifier)]
    calls.extend(
        _path_input_call(alt.get("template", ""), alt.get("root_folder"), name, qualifier)
        for alt in (alternates or [])
    )
    if len(calls) == 1:
        return calls[0]
    return f"{qualifier}EachOf({', '.join(calls)})"


def _path_input_call(
    template: str, root_folder: "str | None", name: str, qualifier: str
) -> str:
    args = [repr(template)]
    if root_folder:
        args.append(f"root_folder={root_folder!r}")
    # Required: the name is the PathInput's identity, and it must equal the
    # binding the declaration is written under (scidb.parameter.
    # path_input_declaration).
    args.append(f"name={name!r}")
    return f"{qualifier}PathInput({', '.join(args)})"
