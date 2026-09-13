"""
Library function references — resolution by explicit import.

A "library function" is a function the user did NOT write: ``numpy.mean``,
``pandas.read_csv``, a Python stdlib call. The GUI lets them be pinned to
the canvas as ordinary function nodes alongside auto-discovered functions
from the user's own code.

**They are deliberately NOT stored in** :mod:`scistack_gui.registry`.
``registry._functions`` holds functions discovered by scanning the user's
source, and every ``load_from_config`` / ``refresh_all`` / ``refresh_module``
clears it. A library function has no file on disk to be rediscovered from,
so it used to be re-registered by replaying the persisted list — but only
three call sites replayed, while several others (``api/project``,
``variable_service``, ``target_file_service``, ``registry_reload_service``)
cleared the registry and did not. The observable bug: adding a Parameter
value or creating a Variable silently evicted ``pandas.read_csv``, and the
next run failed with "not found in registry".

So the callable is never cached in the registry. ``_pipeline_builtin_functions``
records *which* references the user added; this module imports the callable
on demand at every use site (:func:`resolve`). ``importlib`` serves from
``sys.modules`` after the first call, so the hot path is a dict lookup.
Nothing to replay means no refresh path can break it again.

Scope is deliberately narrow: numpy, pandas, and the standard library. Not
a general arbitrary-import backdoor.

One more thing this module owns: the resolved callable is handed out
**name-qualified**, so ``__name__`` is the canonical reference rather than
the bare attribute name. See :func:`with_qualified_name` — that identity is
what keeps a library function to one node on the canvas.
"""

from __future__ import annotations

import functools
import importlib
import importlib.util
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Importable package roots this feature is scoped to (plus the standard
# library itself, checked separately by _is_stdlib_module).
ALLOWED_PACKAGE_ROOTS = frozenset({"numpy", "pandas"})

# Conventional import aliases. Users write `pd.read_csv` because that is
# what their own code says; `pd` is not an importable module, so the
# reference is canonicalized to `pandas.read_csv` at the boundary and the
# alias never reaches persistence, node labels or generated code.
ALIASES = {
    "np": "numpy",
    "pd": "pandas",
}

# Qualified-name wrappers, keyed by canonical reference. Not a resolution
# cache — resolve() still imports on every call (see the module docstring on
# why nothing is cached across registry refreshes). This only keeps ONE
# wrapper object per reference so repeat lookups of `pandas.read_csv` return
# the same callable rather than a fresh identity each time.
_QUALIFIED: dict[str, object] = {}


def canonical_reference(reference: str) -> str:
    """Expand a conventional import alias in *reference*'s root segment.

    ``pd.read_csv`` -> ``pandas.read_csv``; anything else is returned
    unchanged (including a bare ``pd``, which has no attribute to resolve
    and is rejected downstream on its own merits).
    """
    reference = (reference or "").strip()
    root, sep, rest = reference.partition(".")
    if not sep:
        return reference
    expanded = ALIASES.get(root)
    if expanded is None:
        return reference
    logger.info(
        "[library_functions] Expanded import alias %r -> %r", reference, f"{expanded}.{rest}"
    )
    return f"{expanded}.{rest}"


def split_reference(reference: str) -> tuple[str, str]:
    """Split a canonical reference into ``(module_path, attr_name)``.

    A reference with no dot is a Python builtin (``len``), so the module
    path is ``builtins``.
    """
    if "." in reference:
        module_path, _, attr_name = reference.rpartition(".")
        return module_path, attr_name
    return "builtins", reference


def is_library_reference(name: str) -> bool:
    """True if *name* could name a library function, by shape alone.

    A cheap gate so a plain "function not found" doesn't pay for an import
    attempt: the root must be ``builtins``-implied (no dot), an allowed
    package, or an alias for one. Says nothing about whether the function
    actually exists — that is :func:`resolve`'s job.
    """
    if not name:
        return False
    module_path, attr_name = split_reference(canonical_reference(name))
    if not attr_name.isidentifier():
        return False
    if module_path == "builtins":
        return True
    root = module_path.split(".")[0]
    return root.isidentifier()


def with_qualified_name(fn, canonical: str):
    """Return *fn* wrapped so its ``__name__`` is the canonical reference.

    **Why this exists.** The GUI names a library function by its qualified
    reference (``pandas.read_csv``) — that is the node label, the persisted
    builtin-function entry, and the ``function_name`` a run request carries.
    scifor and scidb name a function by ``getattr(fn, "__name__")``
    (``scifor/foreach.py``, ``scidb/foreach.py``, ``scidb/state.py``), and
    ``pandas.read_csv.__name__`` is the bare ``"read_csv"``.

    Handing the raw callable to ``for_each`` therefore recorded the run
    under ``read_csv`` while the canvas node was ``pandas.read_csv``. The
    two never reconciled: ``graph_builder.merge_manual_nodes`` graduates a
    manual node onto its DB counterpart by matching ``(type, label)``, so
    the next graph build produced a SECOND function node — the DB-derived
    ``read_csv`` alongside the user's ``pandas.read_csv`` — and
    ``derive_fn_targets(db, "pandas.read_csv")`` could not see that run's
    history at all.

    Setting ``__name__`` on the pandas function itself is not an option: it
    is a shared global that other code (and the user's own scripts) import.
    So the reference gets its own thin wrapper. ``functools.wraps`` sets
    ``__wrapped__``, so ``inspect.signature`` and ``inspect.getdoc`` read
    through to the real function — the settings panel still sees
    ``read_csv``'s 44 real parameters. Only the NAME differs, which is the
    whole point.

    **One consumer must unwrap by hand**: ``inspect.getsourcefile`` reads
    ``__code__.co_filename`` and does NOT unwrap, while
    ``inspect.getsourcelines`` does — pairing them naively yields this
    file's path with pandas' line number. ``pipeline_service
    .get_function_source`` calls ``inspect.unwrap`` first for that reason.
    (``scilineage.hashing`` already unwraps, so the function hash is
    unaffected.)

    Precedent: ``scidb.foreach``'s Merge wrapper exists for the same reason
    — to give scifor the ``__name__`` it should record.
    """
    if getattr(fn, "__name__", None) == canonical:
        return fn
    cached = _QUALIFIED.get(canonical)
    if cached is not None and getattr(cached, "__wrapped__", None) is fn:
        return cached

    @functools.wraps(fn)
    def qualified(*args, **kwargs):
        return fn(*args, **kwargs)

    qualified.__name__ = canonical
    qualified.__qualname__ = canonical
    _QUALIFIED[canonical] = qualified
    logger.debug(
        "[library_functions] Qualified %r as %r for downstream recording",
        getattr(fn, "__name__", fn),
        canonical,
    )
    return qualified


def resolve(reference: str):
    """Import *reference* and return the callable, or ``None``.

    The returned callable is name-qualified (see :func:`with_qualified_name`):
    its ``__name__`` is the canonical reference, so scifor/scidb record the
    run under the same name the GUI shows on the node.

    The hot path: called on every function lookup that misses the registry,
    on every node-parameter read, and at run time. Silent on failure —
    callers turn ``None`` into their own error. Use :func:`validate` for
    the user-facing, reason-carrying check.
    """
    if not reference:
        return None
    canonical = canonical_reference(reference)
    module_path, attr_name = split_reference(canonical)
    if not attr_name.isidentifier():
        return None
    if module_path != "builtins" and not _root_allowed(module_path):
        return None
    try:
        mod = importlib.import_module(module_path)
    except ImportError:
        logger.debug("[library_functions] Could not import %r for %r", module_path, reference)
        return None
    fn = getattr(mod, attr_name, None)
    if fn is None or not callable(fn):
        return None
    return with_qualified_name(fn, canonical)


def validate(reference: str) -> tuple[dict | None, str | None, object]:
    """Validate *reference* and resolve it to a real callable.

    Returns ``(error_response, None, None)`` on failure, or
    ``(None, canonical_name, fn)`` on success. The error shape matches what
    ``builtin_function_service.create_builtin_function`` returns to the
    frontend.
    """
    canonical = canonical_reference(reference)
    module_path, attr_name = split_reference(canonical)

    if not attr_name.isidentifier():
        return (
            {"ok": False, "error": f"'{reference}' is not a valid Python reference."},
            None,
            None,
        )

    if module_path != "builtins":
        root = module_path.split(".")[0]
        if root not in ALLOWED_PACKAGE_ROOTS:
            try:
                root_mod = importlib.import_module(root)
            except ImportError:
                return (
                    {"ok": False, "error": f"'{root}' is not installed or not importable."},
                    None,
                    None,
                )
            if not _is_stdlib_module(root_mod):
                return (
                    {
                        "ok": False,
                        "error": (
                            f"'{root}' is not allowed — library function references "
                            "are restricted to the Python standard library, numpy, "
                            "and pandas."
                        ),
                    },
                    None,
                    None,
                )

    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        return {"ok": False, "error": f"Could not import '{module_path}': {e}"}, None, None

    fn = getattr(mod, attr_name, None)
    if fn is None:
        return (
            {"ok": False, "error": f"'{attr_name}' not found in '{module_path}'."},
            None,
            None,
        )
    if not callable(fn):
        return {"ok": False, "error": f"'{canonical}' is not callable."}, None, None

    # Same qualified identity :func:`resolve` hands out, so a caller that
    # uses validate()'s callable can never record a run under the bare name.
    return None, canonical, with_qualified_name(fn, canonical)


def _in_site_packages(path: str) -> bool:
    """True if *path* lives in an installed-packages directory."""
    parts = Path(path).resolve().parts
    return "site-packages" in parts or "dist-packages" in parts


def _is_stdlib_root(root: str, origin: str | None) -> bool:
    """True if *root* names a standard-library module.

    ``sys.stdlib_module_names`` is the authoritative answer and is checked
    first. *origin* (a spec origin or a module ``__file__``) only guards the
    one case the name list cannot see: a third-party distribution installed
    under a stdlib name, which must not inherit stdlib's trust.

    This replaced a "is the file under ``sysconfig.get_paths()['stdlib']``?"
    test that was wrong outside a virtualenv. In a venv, site-packages sits
    beside the base interpreter's stdlib, so the test held. Run against a
    plain interpreter — CI's ``setup-python``, a system or conda Python —
    site-packages is a *subdirectory* of the stdlib path, so every installed
    third-party package answered "stdlib" and became resolvable. That is the
    arbitrary-import backdoor this module's docstring says it is not, and it
    opened based on nothing but how the interpreter was laid out.
    """
    if root not in sys.stdlib_module_names:
        return False
    # No file at all: a built-in or frozen module (sys, itertools).
    if origin is None or origin in ("built-in", "frozen"):
        return True
    if _in_site_packages(origin):
        logger.debug(
            "[library_functions] %r shadows a stdlib name from %s — not treated as stdlib",
            root,
            origin,
        )
        return False
    return True


def _root_allowed(module_path: str) -> bool:
    """True if *module_path*'s root is numpy/pandas or a stdlib module.

    :func:`resolve`'s counterpart to :func:`validate`'s allow-list check —
    same rule, but classified from the module's **spec** rather than by
    importing it. That matters because :func:`resolve` runs on every
    function lookup that misses the registry, including names that are
    neither library references nor registered functions: importing an
    arbitrary root just to reject it would execute a user module as a side
    effect of a failed lookup. ``find_spec`` locates without executing.

    (:func:`validate` may import freely — the user explicitly asked for
    that reference to be added.)
    """
    root = module_path.split(".")[0]
    if root in ALLOWED_PACKAGE_ROOTS:
        return True
    if root not in sys.stdlib_module_names:
        # Not a stdlib name, so no spec lookup is needed to reject it.
        logger.debug("[library_functions] root %r is not allowed", root)
        return False
    try:
        spec = importlib.util.find_spec(root)
    except (ImportError, ValueError):
        return False
    if spec is None:
        return False
    return _is_stdlib_root(root, spec.origin)


def _is_stdlib_module(mod) -> bool:
    """True if *mod* lives in the standard library.

    :func:`validate`'s counterpart to :func:`_root_allowed` — same rule,
    applied to an already-imported module.
    """
    root = mod.__name__.split(".")[0]
    return _is_stdlib_root(root, getattr(mod, "__file__", None))
