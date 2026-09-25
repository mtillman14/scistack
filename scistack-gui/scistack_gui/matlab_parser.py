"""
Static parser for MATLAB .m files.

Extracts function signatures and classdef declarations without running MATLAB.
Used by the MATLAB registry to discover functions and variable types from
configured .m file paths.
"""

import logging
import re
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path

from scidb.source_edit import Span

logger = logging.getLogger(__name__)

# Regex for MATLAB function declaration:
#   function [out1, out2] = name(in1, in2, ...)
#   function out = name(in1, ...)
#   function name(in1, ...)
_FUNCTION_RE = re.compile(
    r"^\s*function\s+"
    r"(?:"
    r"(?:\[([^\]]*)\]\s*=\s*)"  # [out1, out2] = ...
    r"|"
    r"(?:(\w+)\s*=\s*)"  # out = ...
    r")?"
    r"(\w+)"  # function name
    r"\s*\(([^)]*)\)",  # (param1, param2, ...)
    re.MULTILINE,
)

# Regex for MATLAB classdef: classdef ClassName < ParentClass
_CLASSDEF_RE = re.compile(
    r"^\s*classdef\s+(\w+)\s*<\s*([\w.]+)",
    re.MULTILINE,
)

# MATLAB block comments: a line containing only `%{`, everything up to a
# line containing only `%}`. Stripped before either regex above runs, so
# example code inside a block comment (a common way scientists leave
# "here's how to call this" notes) never false-positives as a real
# function/classdef declaration.
_BLOCK_COMMENT_RE = re.compile(
    r"^[ \t]*%\{[ \t]*$.*?^[ \t]*%\}[ \t]*$\n?",
    re.DOTALL | re.MULTILINE,
)

# MATLAB line continuation (`...` through end of line, including any
# trailing comment) followed by the leading whitespace of the next line.
# Collapsed to a single space before matching so a signature split across
# multiple lines doesn't leak "...\n    " into a captured parameter name.
_LINE_CONTINUATION_RE = re.compile(r"\.\.\.[^\n]*\n[ \t]*")


def _blank_out(text: str, *, keep_newlines: bool) -> str:
    """*text* with every character replaced by a space, optionally keeping
    newlines so line structure and line numbering survive."""
    if keep_newlines:
        return "".join("\n" if c == "\n" else " " for c in text)
    return " " * len(text)


def _preprocess_for_parsing(text: str) -> str:
    """Neutralise block comments and line continuations before any parser
    regex runs. Never applied to the bytes used for ``source_hash`` — only
    to the text used to locate declarations.

    **Length-preserving**: masked regions are overwritten with spaces rather
    than deleted, so an offset into the returned text is also a valid offset
    into the original. That is what makes span-based rewriting possible —
    without it, a splice computed from parsed output would land at the wrong
    place in the real file and corrupt it (see
    ``.claude/plan-gui-entity-editing-26-08-24.md`` Stage 2). Every regex in
    this module only ever searches for declarations and construction calls,
    so a run of spaces is inert to all of them.

    Block comments keep their newlines (the region becomes blank lines, so
    line numbers still match the original file). Line continuations do not:
    the newline is part of what gets collapsed, which is the whole point —
    it joins a split signature back onto one logical line so ``...`` and the
    next line's indentation never leak into a captured parameter name.
    """
    text = _BLOCK_COMMENT_RE.sub(
        lambda m: _blank_out(m.group(0), keep_newlines=True), text
    )
    text = _LINE_CONTINUATION_RE.sub(
        lambda m: _blank_out(m.group(0), keep_newlines=False), text
    )
    return text


def _extract_docstring(text: str, after_pos: int) -> str | None:
    """Collect MATLAB help text: the run of ``%``-prefixed comment lines
    starting immediately after ``after_pos`` (the end of the function
    declaration), stopping at the first line that isn't a comment —
    including a blank line, matching how MATLAB's own ``help`` command
    finds the H1/help block. Returns ``None`` if there is no such block.
    """
    line_end = text.find("\n", after_pos)
    remainder = text[line_end + 1 :] if line_end != -1 else ""
    doc_lines: list[str] = []
    for line in remainder.splitlines():
        stripped = line.strip()
        if not stripped.startswith("%"):
            break
        # Drop the leading '%' and at most one following space.
        content = stripped[1:]
        if content.startswith(" "):
            content = content[1:]
        doc_lines.append(content)
    if not doc_lines:
        return None
    return "\n".join(doc_lines)


@dataclass
class MatlabFunctionInfo:
    """Parsed metadata for a MATLAB function file."""

    name: str
    """Function name (from the function declaration)."""

    file_path: Path | None
    """Absolute path to the .m file. ``None`` for a manually-declared
    reference to a MATLAB built-in/toolbox function — there is no backing
    .m file to point at (see scistack_gui.api.builtin_functions)."""

    params: list[str]
    """Parameter names (input arguments)."""

    source_hash: str
    """SHA-256 hex digest of the file contents (for lineage)."""

    n_outputs: int = 0
    """Number of declared output arguments (0 = void, 1 = scalar, 2+ = multi)."""

    output_names: list[str] = field(default_factory=list)
    """Names of declared output arguments, in order."""

    docstring: str | None = None
    """Help text: the contiguous ``%``-comment lines immediately following
    the function declaration (MATLAB's own ``help``/H1-line convention).
    ``None`` if the function has no such comment block."""

    language: str = "matlab"


def parse_matlab_function(path: Path) -> MatlabFunctionInfo | None:
    """Parse a MATLAB function file and extract its signature.

    Returns ``None`` if the file cannot be read, does not contain a valid
    function declaration, or contains a ``classdef`` declaration (any
    ``function`` inside such a file is a method belonging to that class,
    never a standalone pipeline function).
    """
    logger.debug("[matlab_parser] Parsing function file: %s", path)
    try:
        raw = path.read_bytes()
    except OSError as e:
        logger.warning(
            "[matlab_parser] Cannot read MATLAB function file %s: %s", path, e
        )
        return None

    # Hash raw bytes so the digest matches MATLAB's fileread() which
    # preserves \r\n line endings (read_text would normalise them away).
    logger.debug("[matlab_parser] Computing source hash")
    source_hash = sha256(raw).hexdigest()
    text = _preprocess_for_parsing(raw.decode("utf-8", errors="replace"))

    # A file containing ANY classdef declaration — not just a BaseVariable
    # one — never also defines a standalone pipeline function: MATLAB's
    # one-file-one-entity rule means a `function` match inside such a file
    # is always a method (constructor, a TestMethodSetup/Test method, ...),
    # not top-level callable code. Without this check, folder-scan discovery
    # over e.g. a `matlab.unittest.TestCase` test suite mis-registers each
    # test class's local setup helper (often named identically across many
    # files, e.g. `resetSchema`/`addPaths`) as if it were real pipeline
    # code, silently overwriting same-named registrations from other files
    # (see matlab_registry._register_matlab_function's "shadows previous
    # definition" warning — regression test in test_matlab.py).
    if _CLASSDEF_RE.search(text):
        logger.debug(
            "[matlab_parser] %s contains a classdef; skipping function "
            "extraction (any 'function' inside it belongs to that class)",
            path,
        )
        return None

    logger.debug("[matlab_parser] Searching for function declaration")
    m = _FUNCTION_RE.search(text)
    if m is None:
        logger.debug("[matlab_parser] No function declaration found in %s", path)
        return None

    # Group 3 is always the function name.
    fn_name = m.group(3)
    logger.debug("[matlab_parser] Found function: %s", fn_name)
    # Group 4 is the parameter list.
    raw_params = m.group(4).strip()
    params = (
        [p.strip() for p in raw_params.split(",") if p.strip()] if raw_params else []
    )
    logger.debug("[matlab_parser] Function has %d parameters", len(params))

    # Count output arguments and extract names from the declaration.
    #   Group 1: "[out1, out2]" → count comma-separated names
    #   Group 2: "out"          → single output
    #   Neither:                → void (0 outputs)
    if m.group(1) is not None:
        output_names = [o.strip() for o in m.group(1).split(",") if o.strip()]
        n_outputs = len(output_names)
    elif m.group(2) is not None:
        output_names = [m.group(2).strip()]
        n_outputs = 1
    else:
        output_names = []
        n_outputs = 0
    logger.debug("[matlab_parser] Function has %d outputs", n_outputs)

    docstring = _extract_docstring(text, m.end())
    logger.debug(
        "[matlab_parser] Function %s has docstring: %s",
        fn_name,
        docstring is not None,
    )

    logger.debug("[matlab_parser] Successfully parsed function: %s", fn_name)
    return MatlabFunctionInfo(
        name=fn_name,
        # The caller (config._resolve_glob_paths) already normalizes paths
        # to an absolute, non-symlink-followed form. We deliberately do NOT
        # call .resolve() here because on Windows it canonicalizes mapped
        # drives (y:\...) to UNC (\\server\share\...), which VS Code refuses
        # to open without security.allowedUNCHosts — breaking the GUI's
        # reveal_in_editor feature.
        file_path=path,
        params=params,
        source_hash=source_hash,
        n_outputs=n_outputs,
        output_names=output_names,
        docstring=docstring,
    )


def parse_matlab_variable(path: Path) -> str | None:
    """Parse a MATLAB classdef file for a BaseVariable subclass.

    Looks for ``classdef Foo < scidb.BaseVariable`` (or any parent path
    ending in ``BaseVariable``). Returns the class name or ``None``.
    """
    logger.debug("[matlab_parser] Parsing variable classdef file: %s", path)
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        logger.warning(
            "[matlab_parser] Cannot read MATLAB variable file %s: %s", path, e
        )
        return None
    text = _preprocess_for_parsing(text)

    logger.debug("[matlab_parser] Searching for classdef declaration")
    m = _CLASSDEF_RE.search(text)
    if m is None:
        logger.debug("[matlab_parser] No classdef declaration found in %s", path)
        return None

    class_name = m.group(1)
    parent = m.group(2)
    logger.debug("[matlab_parser] Found classdef: %s < %s", class_name, parent)

    # Accept any parent that ends with "BaseVariable" (e.g. scidb.BaseVariable).
    if parent.endswith("BaseVariable"):
        logger.debug("[matlab_parser] Class %s is a BaseVariable subclass", class_name)
        return class_name

    logger.debug(
        "[matlab_parser] Class %s does not inherit from BaseVariable", class_name
    )
    return None


# ---------------------------------------------------------------------------
# Best-effort literal extraction — construct a REAL PathInput/Sweep object
# from an entities-script declaration, when its arguments are simple
# literals (quoted strings / numbers). Still purely static text (no MATLAB
# run): a construction whose arguments reference a MATLAB variable or
# expression returns None, and the caller falls back to name-only tracking
# (see matlab_registry.py). See
# docs/claude/code-discovery-categories.md.
# ---------------------------------------------------------------------------

_MATLAB_NUMBER_RE = re.compile(r"^[-+]?\d+\.?\d*(?:[eE][-+]?\d+)?$")


def _matching_paren(text: str, start: int) -> "int | None":
    """Index of the ``)`` closing an opening ``(`` that has already been
    consumed (i.e. *start* is the offset just after it), or ``None`` if the
    parens never balance.

    Quoted strings are skipped whole — both ``'...'`` and ``"..."``, with
    MATLAB's doubled-quote escaping — so a paren inside a path template
    never throws off the depth count. Takes an offset rather than a slice so
    callers scanning many calls in one file don't copy the remainder of the
    text per call.
    """
    depth = 1
    i = start
    n = len(text)
    while i < n and depth > 0:
        c = text[i]
        if c in "'\"":
            quote = c
            i += 1
            while i < n:
                if text[i] == quote:
                    if i + 1 < n and text[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        i += 1
    if depth != 0:
        return None
    return i - 1


def _split_matlab_args(args_text: str) -> list[str]:
    """Split a MATLAB argument list on top-level commas, respecting quoted
    strings (both ``'...'`` and ``"..."``, each with MATLAB's doubled-quote
    escaping) and nested brackets, so a comma inside a string literal or a
    nested ``[...]``/``{...}`` never splits there."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    i = 0
    n = len(args_text)
    while i < n:
        c = args_text[i]
        if c in "'\"":
            quote = c
            current.append(c)
            i += 1
            while i < n:
                current.append(args_text[i])
                if args_text[i] == quote:
                    if i + 1 < n and args_text[i + 1] == quote:
                        current.append(args_text[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if c in "([{":
            depth += 1
            current.append(c)
            i += 1
            continue
        if c in ")]}":
            depth -= 1
            current.append(c)
            i += 1
            continue
        if c == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            i += 1
            continue
        current.append(c)
        i += 1
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


_MATLAB_IDENT_RE = re.compile(r"^[A-Za-z]\w*$")

# Calls that join their arguments into one string. `fullfile` is by far the
# most common way a MATLAB path template gets built from a base directory.
_MATLAB_JOIN_CALL_RE = re.compile(r"^(fullfile|strcat|horzcat)\s*\(", re.IGNORECASE)

# MATLAB's canonical separator is platform-dependent, but forward slashes
# resolve on Windows too, and PathInput templates are matched against
# discovered paths rather than opened literally.
_FULLFILE_SEP = "/"


def _split_matlab_concat_parts(inner: str) -> list[str]:
    """Split the inside of a ``[...]`` on top-level commas *and* whitespace.

    Bracket concatenation accepts either (``[a b]`` and ``[a, b]`` are the
    same), unlike an argument list, which only splits on commas.
    """
    parts: list[str] = []
    for comma_part in _split_matlab_args(inner):
        current: list[str] = []
        depth = 0
        i = 0
        n = len(comma_part)
        while i < n:
            c = comma_part[i]
            if c in "'\"":
                quote = c
                current.append(c)
                i += 1
                while i < n:
                    current.append(comma_part[i])
                    if comma_part[i] == quote:
                        if i + 1 < n and comma_part[i + 1] == quote:
                            current.append(comma_part[i + 1])
                            i += 2
                            continue
                        i += 1
                        break
                    i += 1
                continue
            if c in "([{":
                depth += 1
            elif c in ")]}":
                depth -= 1
            if c.isspace() and depth == 0:
                if current:
                    parts.append("".join(current).strip())
                    current = []
                i += 1
                continue
            current.append(c)
            i += 1
        if current:
            parts.append("".join(current).strip())
    return [p for p in parts if p]


def _parse_matlab_literal_token(
    token: str, scope: "dict[str, object] | None" = None
) -> "tuple[bool, object]":
    """``(True, value)`` if *token* can be statically reduced to a MATLAB
    literal, else ``(False, None)``. This never runs MATLAB.

    Without *scope* only quoted strings and numbers reduce. With one — a map
    of earlier ``name = <literal>`` bindings in the same file, from
    :func:`collect_matlab_literal_scope` — three more forms fold:

    - a bare variable reference (``emgTemplate``)
    - bracket concatenation (``[baseDir '\\6MWT.csv']``, comma or space)
    - ``fullfile`` / ``strcat`` / ``horzcat`` of foldable parts

    These cover how a path template is usually assembled from a base
    directory. Anything else (``sprintf``, arithmetic, a function call) stays
    unresolvable on purpose: guessing a value would put a wrong path on the
    canvas, which is worse than the honest "can't extract this" warning.
    """
    token = token.strip()
    if len(token) >= 2 and token[0] == token[-1] and token[0] in "'\"":
        quote = token[0]
        inner = token[1:-1]
        return True, inner.replace(quote * 2, quote)
    if _MATLAB_NUMBER_RE.match(token):
        return True, (
            float(token) if ("." in token or "e" in token.lower()) else int(token)
        )

    if not scope:
        return False, None

    if _MATLAB_IDENT_RE.match(token):
        if token in scope:
            return True, scope[token]
        return False, None

    if token.startswith("[") and token.endswith("]"):
        return _fold_parts(_split_matlab_concat_parts(token[1:-1]), "", scope)

    join = _MATLAB_JOIN_CALL_RE.match(token)
    if join is not None:
        args_end = _matching_paren(token, join.end())
        # Trailing text after the call (``fullfile(a,b) + x``) means this is a
        # larger expression that isn't a plain join.
        if args_end is not None and token[args_end + 1 :].strip() == "":
            sep = _FULLFILE_SEP if join.group(1).lower() == "fullfile" else ""
            return _fold_parts(
                _split_matlab_args(token[join.end() : args_end]), sep, scope
            )

    return False, None


def _fold_parts(
    parts: list[str], sep: str, scope: "dict[str, object]"
) -> "tuple[bool, object]":
    """Resolve every part and join with *sep*; all-or-nothing."""
    if not parts:
        return False, None
    resolved: list[str] = []
    for part in parts:
        ok, value = _parse_matlab_literal_token(part, scope)
        if not ok:
            return False, None
        resolved.append(str(value))
    return True, sep.join(resolved)


def collect_matlab_literal_scope(text: str) -> "dict[str, object]":
    """Statically foldable ``name = <expr>`` bindings in an entities script.

    Walks bindings in file order, folding each against everything already
    resolved, so a chain (``root = 'Y:\\data'; sub = [root '\\emg']``)
    resolves in one pass. Later bindings of a name overwrite earlier ones,
    matching MATLAB's sequential execution and the parser's
    last-binding-wins rule.

    Entity constructor calls are skipped -- they are the things being
    resolved, not inputs to the fold.

    Text is preprocessed exactly as :func:`parse_matlab_entities_script`
    preprocesses it, so a binding sitting inside a block comment can't slip
    into scope and silently redefine a real one.
    """
    parsed = _preprocess_for_parsing(text)
    scope: dict[str, object] = {}
    for m in _BINDING_RE.finditer(parsed):
        rhs_start = m.end()
        while rhs_start < len(parsed) and parsed[rhs_start] in " \t":
            rhs_start += 1
        if _ENTITY_CTOR_RE.match(parsed, rhs_start):
            continue
        rhs = parsed[rhs_start : _statement_end(parsed, rhs_start)].strip()
        ok, value = _parse_matlab_literal_token(rhs, scope)
        if ok:
            scope[m.group(1)] = value
    if scope:
        logger.debug(
            "[matlab_parser] Folded %d literal binding(s) for resolution: %s",
            len(scope),
            sorted(scope),
        )
    return scope


def read_source_text(path: Path) -> "str | None":
    """The file's decoded text, decoded exactly as the parsers decode it —
    so a caller can turn a ``Span`` into a substring or a line number
    against the same string the span was computed for."""
    try:
        return path.read_bytes().decode("utf-8", errors="replace")
    except OSError as e:
        logger.warning("[matlab_parser] Cannot read MATLAB file %s: %s", path, e)
        return None


# ---------------------------------------------------------------------------
# Entities script — the MATLAB analogue of src/scistack_entities.py.
#
# A plain script (no `function`, no `classdef`) of top-level bindings:
#
#     test_sweep = scidb.Sweep(0, 1, 2);
#
# Parsed exactly like Python's `find_binding_span`: locate `NAME = <expr>`
# at the start of a line and return the span of the whole RHS construction
# call, so changing a value and changing the constructor are the same
# splice (see docs/claude/entity-editability-model.md, D1/D4/D5).
# ---------------------------------------------------------------------------

# `NAME = ` at the start of a line. `=` must not be `==`/`<=`/`>=`/`~=`, or a
# comparison inside a script would read as a binding.
_BINDING_RE = re.compile(r"^[ \t]*([A-Za-z]\w*)[ \t]*=(?![=])", re.MULTILINE)

# The constructors an entities script may declare, mapped to the entity kind
# they produce. `Constant` is accepted here from the start even though
# +scidb/Constant.m arrives in Stage 4 — parsing is harmless without it, and
# it keeps the grammar in one place.
# Deliberately unanchored: matched with ``.match(text, pos)``, which anchors
# at *pos* — a ``^`` would additionally demand the start of the whole string.
_ENTITY_CTOR_RE = re.compile(r"(?:scifor\.|scidb\.)?(PathInput|Parameter|EachOf)\s*\(")


@dataclass(frozen=True)
class MatlabBinding:
    """One ``NAME = <ctor>(...)`` declaration in an entities script."""

    name: str
    kind: str
    """``"path_input"``, ``"sweep"``, ``"constant"`` or ``"each_of"``."""
    expr: str
    """The full RHS construction call text, e.g. ``scidb.Sweep(0, 1)``."""
    expr_span: Span
    """Absolute offsets of ``expr`` in the file's decoded text."""
    args_span: Span
    """Absolute offsets of just the argument text inside the call."""


_CTOR_KINDS = {
    "PathInput": "path_input",
    "Parameter": "parameter",
    "EachOf": "each_of",
}


def _statement_end(text: str, start: int) -> int:
    """Offset of the end of the statement beginning at *start* — the first
    top-level ``;`` or newline outside quotes/brackets. MATLAB statements
    have no reliable terminator (the ``;`` is optional and only suppresses
    echo), so a continued or bracketed expression has to be scanned rather
    than split on."""
    depth = 0
    i = start
    n = len(text)
    while i < n:
        c = text[i]
        if c in "'\"":
            quote = c
            i += 1
            while i < n:
                if text[i] == quote:
                    if i + 1 < n and text[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif depth == 0 and (c == ";" or c == "\n"):
            return i
        i += 1
    return n


def parse_matlab_entities_script(path: Path) -> "list[MatlabBinding]":
    """Every entity declaration in a MATLAB entities script, in file order.

    Returns ``[]`` for an unreadable file, or one that declares nothing
    recognisable — never raises. A binding whose RHS is not one of the known
    constructors (a plain ``x = 5;`` helper line) is skipped silently: an
    entities script is allowed to contain ordinary MATLAB.

    Later bindings of the same name are returned too; callers wanting
    "what the script ends up holding" should take the last (matching
    ``scidb.source_edit.find_binding_span``'s last-binding-wins rule).
    """
    text = read_source_text(path)
    if text is None:
        return []
    parsed = _preprocess_for_parsing(text)

    bindings: list[MatlabBinding] = []
    for m in _BINDING_RE.finditer(parsed):
        rhs_start = m.end()
        while rhs_start < len(parsed) and parsed[rhs_start] in " \t":
            rhs_start += 1
        ctor = _ENTITY_CTOR_RE.match(parsed, rhs_start)
        if ctor is None:
            continue
        args_end = _matching_paren(parsed, ctor.end())
        if args_end is None:
            logger.warning(
                "[matlab_parser] Unbalanced parens in '%s' declaration in %s",
                m.group(1),
                path,
            )
            continue
        end = _statement_end(parsed, rhs_start)
        bindings.append(
            MatlabBinding(
                name=m.group(1),
                kind=_CTOR_KINDS[ctor.group(1)],
                expr=text[rhs_start:end].rstrip(),
                expr_span=Span(rhs_start, end),
                args_span=Span(ctor.end(), args_end),
            )
        )

    logger.debug(
        "[matlab_parser] Parsed %d entity declaration(s) from %s",
        len(bindings),
        path,
    )
    return bindings


def binding_path_input_literal(
    binding: MatlabBinding, text: str, scope: "dict[str, object] | None" = None
) -> "tuple[str, str | None] | None":
    """``(template, root_folder)`` from a ``PathInput`` binding, or ``None``
    if any needed argument isn't a simple literal — the same all-or-nothing
    all-or-nothing rule as :func:`binding_parameter_literal`.

    Pass *scope* (from :func:`collect_matlab_literal_scope`) to additionally
    fold templates built from earlier variables, e.g.
    ``PathInput([baseDir '/6MWT.csv'])``.
    """
    args = _split_matlab_args(binding.args_span.extract(text))
    return _path_input_args_to_literal(args, scope)


# `name=value` as a single argument token — MATLAB R2021b+ syntax, which is
# what the GUI's own generator emits (api.matlab_command._format_path_input)
# and what scimatlab's README requires R2021b for. Deliberately rejects `==`
# so a comparison expression is never read as a named argument.
_NAMED_ARG_RE = re.compile(r"^([A-Za-z]\w*)\s*=(?!=)\s*(.+)$", re.DOTALL)


def _parse_named_arg(token: str) -> "tuple[str, str] | None":
    """``(name, value_token)`` if *token* is a ``name=value`` argument, else
    ``None``. A quoted literal containing ``=`` (``'a=b.mat'``) can't match:
    the pattern requires a bare identifier before the ``=``."""
    m = _NAMED_ARG_RE.match(token.strip())
    return (m.group(1), m.group(2).strip()) if m else None


# ---------------------------------------------------------------------------
# Rendering MATLAB declarations — the counterpart of scidb.source_edit's
# render_* for the entities script. Kept here, next to the parser that reads
# them back, so the two can never disagree about the grammar.
# ---------------------------------------------------------------------------


def render_matlab_value(value) -> str:
    """A Python value as a MATLAB literal.

    Single-quoted strings (MATLAB's char-array form, with ``''`` escaping)
    rather than double-quoted, so a declaration reads the way MATLAB users
    write one and parses back through ``_parse_matlab_literal_token``.
    ``bool`` is checked before ``int`` — in Python ``True`` IS an ``int``, so
    the obvious ordering silently renders ``1`` instead of ``true``.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def render_matlab_parameter(values, description: str = "") -> str:
    """``scidb.Parameter(30, description='...')`` or ``scidb.Parameter(0, 1, 2)``.

    One constructor whatever the value count -- adding a value is adding an
    argument, never a change of form (D6).
    """
    args = [render_matlab_value(v) for v in values]
    if description:
        args.append(f"description={render_matlab_value(description)}")
    return f"scidb.Parameter({', '.join(args)})"


def render_matlab_path_input(
    template: str,
    root_folder: "str | None" = None,
    alternates: "list[dict] | None" = None,
    *,
    name: str,
) -> str:
    """``scidb.PathInput('{s}/x.csv', root_folder='/d', name='X')``, or an
    ``scidb.EachOf(...)`` of them when *alternates* is given — the same
    shape :func:`scidb.source_edit.render_path_input` produces for Python."""
    calls = [_matlab_path_input_call(template, root_folder, name)]
    calls.extend(
        _matlab_path_input_call(alt.get("template", ""), alt.get("root_folder"), name)
        for alt in (alternates or [])
    )
    if len(calls) == 1:
        return calls[0]
    return f"scidb.EachOf({', '.join(calls)})"


def _matlab_path_input_call(template: str, root_folder: "str | None", name: str) -> str:
    args = [render_matlab_value(template)]
    if root_folder:
        args.append(f"root_folder={render_matlab_value(root_folder)}")
    # Required — the name is the PathInput's identity (+scifor/PathInput.m).
    args.append(f"name={render_matlab_value(name)}")
    return f"scidb.PathInput({', '.join(args)})"


def find_entities_binding(path: Path, name: str) -> "MatlabBinding | None":
    """The LAST top-level binding of *name* in an entities script, or
    ``None``.

    Last-wins matches how a MATLAB script actually executes, and mirrors
    :func:`scidb.source_edit.find_binding_span`'s rule for Python.
    """
    found = None
    for binding in parse_matlab_entities_script(path):
        if binding.name == name:
            found = binding
    return found


def binding_parameter_literal(
    binding: MatlabBinding, text: str
) -> "tuple[list, str] | None":
    """``(values, description)`` from a ``Parameter`` binding, or ``None`` if
    any positional value isn't a simple literal (all-or-nothing: one
    non-literal invalidates the whole list, so a partially-read Parameter
    can never silently run with fewer values than declared).

    ``description`` is accepted in either MATLAB syntax
    (``description='x'`` or ``'description', 'x'``), mirroring
    :func:`_path_input_args_to_literal`'s handling of ``root_folder``.
    """
    args = _split_matlab_args(binding.args_span.extract(text))
    if not args:
        return None

    values: list = []
    description = ""
    i = 0
    while i < len(args):
        named = _parse_named_arg(args[i])
        if named is not None:
            key, value_token = named
            if key == "description":
                ok_val, val = _parse_matlab_literal_token(value_token)
                if ok_val and isinstance(val, str):
                    description = val
            i += 1
            continue
        if i + 1 < len(args):
            ok_key, key = _parse_matlab_literal_token(args[i])
            if ok_key and key == "description":
                ok_val, val = _parse_matlab_literal_token(args[i + 1])
                if ok_val and isinstance(val, str):
                    description = val
                i += 2
                continue
        ok_val, val = _parse_matlab_literal_token(args[i])
        if not ok_val:
            return None
        values.append(val)
        i += 1
    return (values, description) if values else None


def _path_input_args_to_literal(
    args: list[str], scope: "dict[str, object] | None" = None
) -> "tuple[str, str | None] | None":
    """``(template, root_folder)`` from an already-split PathInput argument
    list.

    Accepts BOTH ways MATLAB can pass ``root_folder`` — ``root_folder='/d'``
    (name=value, R2021b+) and ``'root_folder', '/d'`` (name-value pair) —
    because ``+scifor/PathInput.m``'s ``arguments`` block accepts both, so a
    user (or the GUI's own command generator) may legitimately have written
    either.
    """
    if not args:
        return None
    ok, template = _parse_matlab_literal_token(args[0], scope)
    if not ok or not isinstance(template, str):
        return None

    root_folder: str | None = None
    i = 1
    while i < len(args):
        named = _parse_named_arg(args[i])
        if named is not None:
            key, value_token = named
            if key == "root_folder":
                ok_val, val = _parse_matlab_literal_token(value_token, scope)
                if ok_val and isinstance(val, str):
                    root_folder = val
            i += 1
            continue
        if i + 1 < len(args):
            ok_key, key = _parse_matlab_literal_token(args[i], scope)
            if ok_key and key == "root_folder":
                ok_val, val = _parse_matlab_literal_token(args[i + 1], scope)
                if ok_val and isinstance(val, str):
                    root_folder = val
                i += 2
                continue
        i += 1
    return template, root_folder




def is_matlab_entities_script(path: Path) -> bool:
    """True if *path* is a plain script (no ``function``, no ``classdef``)
    that declares at least one entity.

    The no-function/no-classdef check is what keeps this from colliding
    with the existing "any classdef file is never scanned for functions"
    rule — an entities script is exactly the shape ``classify_matlab_file``
    previously had no answer for.
    """
    text = read_source_text(path)
    if text is None:
        return False
    parsed = _preprocess_for_parsing(text)
    if _CLASSDEF_RE.search(parsed) or _FUNCTION_RE.search(parsed):
        return False
    return bool(parse_matlab_entities_script(path))


# {str(path): ((mtime_ns, size), result)} — see classify_matlab_file.
_CLASSIFY_CACHE: dict[str, tuple[tuple[int, int], object]] = {}
_classify_hits = 0
_classify_misses = 0


def _file_stamp(path: Path) -> "tuple[int, int] | None":
    """``(mtime_ns, size)`` for *path*, or ``None`` if it cannot be stat'd.

    Deliberately NOT a content hash: hashing means reading, and reading is
    the cost this key exists to avoid.
    """
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def invalidate_classify_cache(path: "Path | str | None" = None) -> None:
    """Drop *path* from the classification cache, or all of it when ``None``.

    Call this whenever a file is written by code that may then re-read it in
    the same filesystem timestamp tick — a same-size edit within one tick is
    the only way the ``(mtime_ns, size)`` key can go stale, and an editor
    that writes and immediately reloads is exactly that case.
    """
    global _classify_hits, _classify_misses
    if path is None:
        _CLASSIFY_CACHE.clear()
        _classify_hits = 0
        _classify_misses = 0
        logger.info("[matlab_parser] classification cache cleared")
        return
    if _CLASSIFY_CACHE.pop(str(path), None) is not None:
        logger.debug("[matlab_parser] classification cache invalidated for %s", path)


def classify_cache_stats() -> dict:
    """``{"hits", "misses", "entries"}`` since the last full clear."""
    return {
        "hits": _classify_hits,
        "misses": _classify_misses,
        "entries": len(_CLASSIFY_CACHE),
    }


def classify_matlab_file(
    path: Path,
) -> "tuple[str, str] | tuple[str, MatlabFunctionInfo] | None":
    """Classify a single .m file, reusing the last result while the file is
    unchanged.

    Classification is the hot path of every ``refresh_all()``: it runs once
    per configured .m file, and the uncached body below opens the file up to
    three times (``parse_matlab_variable``, ``parse_matlab_function``, then
    ``is_matlab_entities_script``). On a project whose libraries live on an
    SMB share that measured ~90 ms/file — 12-16 s for 141 files, repeated in
    full on every refresh even when nothing had changed.

    The cache is keyed on ``(mtime_ns, size)`` so a hit costs one ``stat``
    instead of three reads plus three parses, and a changed file still
    re-parses. That preserves semantics exactly: every caller still gets the
    classification the file's current bytes imply. ``MatlabFunctionInfo``
    carries a ``source_hash`` of those bytes, and it stays correct for the
    same reason — a hit means the bytes did not change.

    An unstattable path is never cached (it re-parses and reports its error
    each time, as before).
    """
    global _classify_hits, _classify_misses
    stamp = _file_stamp(path)
    key = str(path)
    if stamp is not None:
        cached = _CLASSIFY_CACHE.get(key)
        if cached is not None and cached[0] == stamp:
            _classify_hits += 1
            logger.debug("[matlab_parser] classification cache hit: %s", path)
            return cached[1]  # type: ignore[return-value]

    _classify_misses += 1
    result = _classify_matlab_file_uncached(path)
    if stamp is not None:
        _CLASSIFY_CACHE[key] = (stamp, result)
    return result


def _classify_matlab_file_uncached(
    path: Path,
) -> tuple[str, str] | tuple[str, MatlabFunctionInfo] | None:
    """Classify a single .m file as a variable, a function, or an entities
    script, for folder-scan discovery (no explicit
    ``matlab.functions``/``matlab.variables`` split available).

    Mirrors how Python's ``_scan_module_functions``/``_scan_module_constants``
    classify each imported object dynamically rather than requiring the
    config to pre-sort files. Tries the classdef parse first (cheaper, and a
    ``BaseVariable`` classdef with methods would otherwise also match the
    function regex).

    Returns ``("variable", class_name)``, ``("function", MatlabFunctionInfo)``,
    ``("entities_script", str(path))``, or ``None`` if the file is none of
    those (e.g. a script that declares nothing, or unreadable).

    The entities-script check comes LAST: it is the only classification that
    requires the file to have neither a ``function`` nor a ``classdef``, so
    everything above has already been ruled out by the time it runs, and it
    cannot steal a file from any existing category.
    """
    var_name = parse_matlab_variable(path)
    if var_name is not None:
        return ("variable", var_name)

    fn_info = parse_matlab_function(path)
    if fn_info is not None:
        return ("function", fn_info)

    if is_matlab_entities_script(path):
        return ("entities_script", str(path))

    return None
