---
name: Recursive function hashing in scilineage (AST-based)
status: design locked — ready to implement on approval
---

# Recursive function hashing in scilineage (AST-based)

## Problem

`scilineage.hashing.compute_function_hash(fn)` today hashes only `fn`'s own
bytecode + constants. The bytecode of `foo` contains a `LOAD_GLOBAL "bar"`
opcode, not bar's implementation, so mutating `bar` doesn't propagate to
`foo`'s hash. We want hashes to recurse through user-defined callees.

## Approach: AST-based recursive hashing

Switch from bytecode to **AST**:
- Stable across Python minor versions (bytecode opcodes drift).
- Source is always available for user code (the only thing we recurse into).
- Easier to identify callees precisely (`ast.Call` nodes vs. all of `co_names`).

Bytecode hashing is kept as a fallback when `inspect.getsource()` fails
(REPL, `exec`-generated code, C extensions).

## Decisions locked

| # | Question | Decision |
|---|---|---|
| 1 | "User-defined" boundary | Pure file-path heuristic, no manual lists. A callee is user code iff its source file is NOT under the stdlib directory AND NOT under any `site-packages` / `dist-packages` directory. Editable installs (`pip install -e .`) correctly classify as user code because `inspect.getfile` returns the working-tree path, not the site-packages pointer. The check applies only to transitive callees discovered during the AST walk — the entry-point function passed to `compute_function_hash` is always hashed regardless of where it lives. |
| 2 | Mutual recursion | Simple `seen[id(code)] = placeholder` cycle-break in v1. Hash of a cycle is entry-dependent. Documented limitation. |
| 3 | Indirect calls | Skipped, documented. Covers `dispatch[k](x)`, `obj.method()`, `getattr(m, n)()`, `eval`/`exec`, and **closure-captured callables** (see edge case G). |
| 4 | Classes | Recurse only into what is statically a call. `MyClass(args)` → recurse into `MyClass.__init__`. `instance.method()` → indirect call, skipped. No enumeration of class methods. |
| 5 | Decorators | Probe `getattr(ref, "__wrapped__", None)` before hashing. `functools.wraps` (used by most decorators) sets this. AST hashing also helps because `inspect.getsource(wrapper)` typically returns wrapped-source via `__wrapped__`. |

## Design

### Module: `scilineage/src/scilineage/hashing.py`

```python
import ast
import hashlib
import inspect
import os
import sys
from typing import Callable

_STDLIB_DIR = os.path.realpath(os.path.dirname(os.__file__))


def _is_user_defined(fn: Callable) -> bool:
    """Recurse into anything whose source isn't a stdlib or installed-package file.

    Editable installs (`pip install -e .`) correctly classify as user code:
    pip writes a .pth pointer into site-packages but does not copy files there,
    so `inspect.getfile` returns the actual working-tree path.
    """
    try:
        file = inspect.getfile(fn)
    except TypeError:
        return False                              # builtins / C extensions
    if not file.endswith(".py"):
        return False                              # .so / .pyd / frozen / <string>

    real = os.path.realpath(file)                 # resolve symlinks (Homebrew, venvs)
    if real.startswith(_STDLIB_DIR + os.sep):
        return False                              # stdlib

    parts = real.split(os.sep)
    if "site-packages" in parts or "dist-packages" in parts:
        return False                              # pip / system installed

    return True


def _unwrap(ref):
    """Peel decorators and LineageFcn wrappers off."""
    # LineageFcn → original
    if hasattr(ref, "fcn"):
        ref = ref.fcn
    # functools.wraps trail
    while hasattr(ref, "__wrapped__"):
        ref = ref.__wrapped__
    return ref


def _normalize_ast(tree: ast.AST) -> ast.AST:
    """Strip docstrings + line/col info so cosmetic changes don't perturb the hash."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
               and isinstance(body[0].value.value, str):
                node.body = body[1:] or [ast.Pass()]
    return tree


def _resolve_call_target(call: ast.Call, globals_ns: dict):
    """Best-effort static resolution of an `ast.Call` to a callable object.
    Returns None for indirect / unresolvable calls."""
    fn = call.func
    if isinstance(fn, ast.Name):
        return globals_ns.get(fn.id)
    if isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name):
        obj = globals_ns.get(fn.value.id)
        if obj is not None:
            return getattr(obj, fn.attr, None)
    return None  # method calls on locals, dispatch tables, etc.


def _hash_source(fn: Callable, seen: dict) -> str:
    """AST-based recursive hash."""
    fn = _unwrap(fn)
    key = (fn.__module__, getattr(fn, "__qualname__", fn.__name__))
    if key in seen:
        return seen[key]              # cycle placeholder or memoized
    seen[key] = "<cycle>"              # see decision #2

    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        # Fall back to bytecode hash for source-less callables.
        return _hash_bytecode_only(fn)

    tree = ast.parse(src)
    _normalize_ast(tree)
    own = ast.dump(tree, include_attributes=False)

    # Walk Call nodes; recurse into user-defined targets.
    globals_ns = getattr(fn, "__globals__", {})
    callee_hashes: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = _resolve_call_target(node, globals_ns)
        if target is None:
            continue
        target = _unwrap(target)
        # MyClass(...) → recurse into __init__ only  (decision #4)
        if isinstance(target, type):
            target = target.__init__
            if target is object.__init__:
                continue
        if not callable(target) or not _is_user_defined(target):
            continue
        name = getattr(target, "__qualname__", getattr(target, "__name__", "?"))
        callee_hashes.append((name, _hash_source(target, seen)))

    callee_hashes.sort()
    payload = own + "||" + repr(callee_hashes)
    h = hashlib.sha256(payload.encode()).hexdigest()
    seen[key] = h
    return h


def _hash_bytecode_only(fn) -> str:
    """Fallback when source is unavailable. Equivalent to today's behavior."""
    actual = _unwrap(fn)
    try:
        code = actual.__code__.co_code
        consts = str(actual.__code__.co_consts).encode()
        return hashlib.sha256(code + consts).hexdigest()
    except AttributeError:
        return hashlib.sha256(getattr(actual, "__name__", repr(actual)).encode()).hexdigest()


def compute_function_hash(fn: Callable, *, truncate: int = 16, recursive: bool = True) -> str:
    if recursive:
        return _hash_source(fn, {})[:truncate]
    return _hash_bytecode_only(fn)[:truncate]
```

### Callers (unchanged signatures)

- `scilineage/core.py:82` — `compute_function_hash(fcn, truncate=64)` keeps working.
- `scidb/foreach_config.py:25` — `compute_function_hash(fn, truncate=16)` keeps working.

## Documented limitations (per decisions above)

- **Indirect calls** are invisible: dispatch tables, `obj.method()`,
  `getattr(mod, name)()`, `eval`, `exec`. Mutating a callee reached only
  through these patterns will NOT change the parent's hash.
- **Closure-captured callees** are a special case of indirect calls. Two
  functions produced by the same factory have the same AST and therefore
  the same hash, even if they capture different helpers. Group with
  indirect-call limitation.
- **Cycles** produce entry-dependent hashes. `foo↔bar` hashes differently
  depending on which is computed first. Rare in practice.
- **Decorators that don't set `__wrapped__`** evade unwrap; we hash the
  wrapper's source instead of the wrapped function's. All stdlib /
  `functools.wraps`-based decorators are fine.
- **`ast.parse` failures** (syntax-invalid sources, native code) fall
  back to bytecode hashing, which is non-recursive.

## Tests to add (`scilineage/tests/test_hashing.py`)

1. **Callee mutation propagates** — `foo` calls `bar`; redefining `bar` changes `foo`'s hash.
2. **Numpy callee ignored** — adding/removing `np.array(...)` calls doesn't pull numpy bytecode into the hash; hash stays stable across numpy versions (simulated).
3. **Cosmetic changes ignored** — whitespace, comments, docstring text don't change the hash (preserves current invariant).
4. **Mutual recursion terminates** — `foo↔bar` doesn't infinite-loop.
5. **Self-recursion** — `fact(n)` hashes deterministically.
6. **Class constructor recursion** — `def f(): return MyClass(1)`; mutating `MyClass.__init__` changes `f`'s hash; mutating an unrelated method on `MyClass` does NOT.
7. **Decorator transparency** — `@lru_cache`-decorated callee: changing its body changes the parent hash.
8. **Cross-process stability** — same function, same hash in two separate Python processes (regression for the today-`str(co_consts)` address-in-repr bug).
9. **Source unavailable fallback** — function defined via `exec()` still hashes (via bytecode) without crashing.
10. **Editable-install classification** — a function defined in the monorepo (e.g., a `scidb` helper called from a `@lineage_fcn` user function) is recursed into; a function in `site-packages/numpy/...` is not. Verifies the path heuristic at the boundary that matters most for this codebase.
11. **Symlinked stdlib** — `os.path.realpath` correctly resolves stdlib symlinks; functions like `os.path.join` are skipped regardless of how Python is installed.

## Open follow-ups (not v1)

- Tarjan SCC for cycle stability.
- Explicit `@lineage_fcn(also_depends_on=[fn1, fn2])` escape hatch for indirect/closure cases.
- Canonicalize non-primitive `co_consts` / AST `Constant` values via `canonical_hash` for better stability on regex objects, large literals, etc.
- Cross-call hash caching keyed on `(module, qualname, source-hash)`.
