"""Deterministic hashing for arbitrary Python objects.

This module re-exports the canonical hashing functionality from the
canonicalhash package for convenience, and provides function hashing
utilities. Function hashing is AST-based and recurses through
user-defined callees so that mutating a helper changes its callers'
hashes too.
"""

import ast
import hashlib
import inspect
import logging
import os
from typing import Any, Callable

from canonicalhash import canonical_hash

logger = logging.getLogger(__name__)

__all__ = ["canonical_hash", "compute_function_hash"]


_STDLIB_DIR = os.path.realpath(os.path.dirname(os.__file__))


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
    if real.startswith(_STDLIB_DIR + os.sep):
        return False
    parts = real.split(os.sep)
    if "site-packages" in parts or "dist-packages" in parts:
        return False
    return True


def _unwrap(ref: Any) -> Any:
    """Peel LineageFcn wrappers and ``functools.wraps`` decorator layers."""
    if hasattr(ref, "fcn"):
        ref = ref.fcn
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
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
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


def _hash_source(fn: Any, seen: dict) -> str:
    """AST-based recursive hash of a callable's source plus its callees."""
    fn = _unwrap(fn)
    key = (getattr(fn, "__module__", None), getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn))))
    if key in seen:
        return seen[key]
    seen[key] = "<cycle>"

    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        result = _hash_bytecode_only(fn)
        seen[key] = result
        return result

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
        callee_hashes.append((name, _hash_source(target, seen)))

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


def compute_function_hash(fn: Callable, *, truncate: int = 16, recursive: bool = True) -> str:
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
        fn: The function to hash. Can be a regular function, a
            ``LineageFcn`` wrapper, or any ``functools.wraps``-decorated
            callable.
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
