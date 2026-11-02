"""Stable hashing of Python functions by source.

AST-based, and recursive through user-defined callees, so mutating a helper
changes its callers' hashes too. Function hashes are stored as
``_invocation.function_hash``, so any change to the recipe here re-runs every
stored pipeline step; ``tests/test_function_hash.py`` pins one value.
"""

import ast
import hashlib
import inspect
import logging
import os
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["compute_function_hash", "compute_function_hash_with_sources"]


# Determine stdlib directory for user-code detection.
# In some environments (e.g., MATLAB's in-process Python), os.__file__ may not exist.
try:
    _STDLIB_DIR = os.path.realpath(os.path.dirname(os.__file__))
except AttributeError:
    # When running in environments like MATLAB's in-process Python,
    # built-in modules don't have __file__. In this case, we can't
    # reliably detect stdlib functions, so we skip that check.
    _STDLIB_DIR = None
    logger.debug(
        "os.__file__ unavailable (e.g., MATLAB in-process Python). "
        "Stdlib detection disabled in function hashing."
    )


def _is_user_defined(fn: Any) -> bool:
    """True if ``fn``'s source is in user code, not stdlib or an installed package.

    Editable installs (``pip install -e .``) correctly classify as user
    code: pip writes a ``.pth`` pointer into ``site-packages`` but does
    not copy files there, so ``inspect.getfile`` returns the actual
    working-tree path.
    """
    try:
        file = inspect.getfile(fn)
    except TypeError:
        return False
    if not file.endswith(".py"):
        return False

    real = os.path.realpath(file)
    if _STDLIB_DIR is not None and real.startswith(_STDLIB_DIR + os.sep):
        return False
    parts = real.split(os.sep)
    if "site-packages" in parts or "dist-packages" in parts:
        return False
    return True


def _unwrap(ref: Any) -> Any:
    """Peel ``functools.wraps`` decorator layers to reach the original function."""
    seen_ids: set[int] = set()
    while hasattr(ref, "__wrapped__"):
        if id(ref) in seen_ids:
            break
        seen_ids.add(id(ref))
        ref = ref.__wrapped__
    return ref


def _normalize_ast(tree: ast.AST) -> ast.AST:
    """Strip docstrings so cosmetic edits don't perturb the hash.

    Line/column info is removed at ``ast.dump`` time via
    ``include_attributes=False``.
    """
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:] or [ast.Pass()]
    return tree


def _resolve_call_target(call: ast.Call, globals_ns: dict) -> Any:
    """Best-effort static resolution of ``ast.Call.func`` to a callable.

    Returns ``None`` for indirect / unresolvable calls (dispatch tables,
    method calls on locals, ``getattr``-style lookups). Those are an
    accepted limitation — see ``.claude/recursive-function-hashing.md``.
    """
    fn = call.func
    if isinstance(fn, ast.Name):
        return globals_ns.get(fn.id)
    if isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name):
        obj = globals_ns.get(fn.value.id)
        if obj is not None:
            return getattr(obj, fn.attr, None)
    return None


def _hash_source(fn: Any, seen: dict, collect: dict | None = None) -> str:
    """AST-based recursive hash of a callable's source plus its callees.

    ``collect``, when given, receives ``{qualname: source_text}`` for every unit
    this walk hashes — the function itself and each user-defined callee it
    recursed into. Gathering it *here*, rather than in a second walk that
    reproduces the same traversal rules, is deliberate: the whole point of
    storing source is to be able to reconstitute the code behind a stored hash,
    and a separate walk that drifted from this one would store a set of units
    that never corresponded to the hash it was filed under. One walk, one
    answer. Units whose source is unavailable (the bytecode fallback below) are
    simply absent, so a caller can tell "not captured" from "captured as empty".
    """
    fn = _unwrap(fn)
    key = (
        getattr(fn, "__module__", None),
        getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn))),
    )
    if key in seen:
        return seen[key]
    seen[key] = "<cycle>"

    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        result = _hash_bytecode_only(fn)
        seen[key] = result
        return result

    if collect is not None:
        collect[key[1]] = src

    src = inspect.cleandoc("\n" + src)
    try:
        tree = ast.parse(src)
    except SyntaxError:
        result = _hash_bytecode_only(fn)
        seen[key] = result
        return result

    _normalize_ast(tree)
    own = ast.dump(tree, include_attributes=False)

    globals_ns = getattr(fn, "__globals__", {}) or {}
    callee_hashes: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = _resolve_call_target(node, globals_ns)
        if target is None:
            continue
        target = _unwrap(target)
        if isinstance(target, type):
            init = target.__init__
            if init is object.__init__:
                continue
            target = init
        if not callable(target) or not _is_user_defined(target):
            continue
        name = getattr(target, "__qualname__", getattr(target, "__name__", "?"))
        callee_hashes.append((name, _hash_source(target, seen, collect)))

    callee_hashes.sort()
    payload = own + "||" + repr(callee_hashes)
    h = hashlib.sha256(payload.encode()).hexdigest()
    seen[key] = h
    return h


def _hash_bytecode_only(fn: Any) -> str:
    """Non-recursive bytecode-based hash. Fallback when source is unavailable."""
    actual = _unwrap(fn)
    try:
        code = actual.__code__.co_code
        consts = str(actual.__code__.co_consts).encode()
        return hashlib.sha256(code + consts).hexdigest()
    except AttributeError:
        name = getattr(actual, "__name__", repr(actual))
        return hashlib.sha256(name.encode()).hexdigest()


def compute_function_hash(
    fn: Callable, *, truncate: int = 16, recursive: bool = True
) -> str:
    """Compute a stable hash of a function.

    By default uses AST-based recursive hashing: the hash of a function
    depends on the hashes of every user-defined function it transitively
    calls, so mutating a helper invalidates its callers' hashes too.
    External callees (stdlib, ``site-packages``) are treated as opaque.

    Ignores:
    - Whitespace and formatting changes (AST-based)
    - Comments and docstrings (stripped during normalization)
    - Variable name changes that don't affect AST structure (only when the
      changed names aren't referenced elsewhere — same name = same node)

    Args:
        fn: The function to hash: a regular function or any
            ``functools.wraps``-decorated callable.
        truncate: Number of hex characters to return.
        recursive: If False, use the non-recursive bytecode fallback only
            (useful for debugging and for callers that need the legacy
            behavior).

    Returns:
        Hex string hash of the function, truncated to ``truncate`` chars.
    """
    if recursive:
        full = _hash_source(fn, {})
    else:
        full = _hash_bytecode_only(fn)
    logger.debug(
        "compute_function_hash(%s, recursive=%s) = %s",
        getattr(_unwrap(fn), "__qualname__", repr(fn)),
        recursive,
        full[:12],
    )
    return full[:truncate]


def compute_function_hash_with_sources(
    fn: Callable, *, truncate: int = 16
) -> tuple[str, dict[str, str]]:
    """``(hash, {qualname: source_text})`` — the hash plus the code behind it.

    The hash is **identical** to :func:`compute_function_hash`'s, by
    construction: it is the same walk, run once, with collection switched on.
    Callers storing source against a hash must use this rather than hashing and
    reading source separately, or the two can disagree.

    The returned mapping is the *closure*, not one function: because the hash is
    recursive over user-defined callees, a helper's body is part of what the
    hash identifies, and reconstituting the entry point without its helpers
    would not reproduce the hashed code. Always recursive — there is no source
    to collect on the bytecode-only path.
    """
    collected: dict[str, str] = {}
    full = _hash_source(fn, {}, collected)
    return full[:truncate], collected
