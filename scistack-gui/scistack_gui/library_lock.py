"""
Library-owned pipelines are read-only: the one owner of that rule
(portability Stage 10, D-2026-10-08-10; ``.claude/plan-portability.md``).

A library's pipeline is seeded from the installed library and re-synced
when the library changes (``services/library_service``), so an edit made in
the project would be silently overwritten on the next upgrade. Instead, an
edit is refused with :class:`LibraryPipelineReadOnly`, whose message says
which library owns it and that "Make my own copy" makes it editable.

**When the rule applies.** Only during a USER EDIT:
``api/handlers.Handler.invoke`` runs every undoable handler (the set of user
edits, by the undo system's own definition) inside :func:`user_edit`.
Internal writes are never refused: graduation of seeded nodes onto their
history twins during a graph build, seeding itself (inside
:func:`library_writes`), scripts and tests calling the stores directly.

**Where it is checked.** In the store functions every edit goes through:
``intent_store.put_statements`` / ``delete_statements`` / ``clear_aspect``
(settings, edges, hides: one choke point) and the ``pipeline_store``
writes that bypass the intent store (``GUARDED_WRITES`` there; the guard
test ``test_every_guarded_write_checks_the_lock`` holds them to it).

**What the project may still change** around a library pipeline: the
placement on ITS canvas (the use row's parent is the project's pipeline),
its binding (``key_map`` / ``params`` / ``iterate``), node positions
(cosmetic), and dataset exclusions (scidb, not a canvas).
"""

from __future__ import annotations

import contextvars
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class LibraryPipelineReadOnly(ValueError):
    """An edit inside a library-owned pipeline. A ValueError, so both
    transports already report it (HTTP 400 where mapped, the RPC error
    frame) with its message intact."""

    def __init__(self, library: str, pipeline: str, what: str):
        self.library = library
        self.pipeline = pipeline
        self.what = what
        super().__init__(
            f"'{pipeline}' comes from the library '{library}' and is read-only "
            f"({what} refused). To edit it in this project, use Make my own copy "
            f"(Submodules → Libraries → ✎, or `scistack library copy {library}`)."
        )


_user_edit: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "scistack_library_user_edit", default=None
)
_permit: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "scistack_library_permit", default=False
)


@contextmanager
def user_edit(method: str):
    """Mark the enclosed calls as one user edit (*method*: the handler)."""
    token = _user_edit.set(method)
    try:
        yield
    finally:
        _user_edit.reset(token)


@contextmanager
def library_writes():
    """Allow writes inside library pipelines (seeding and re-sync only)."""
    token = _permit.set(True)
    try:
        yield
    finally:
        _permit.reset(token)


def internal(fn):
    """Mark a store operation as INTERNAL bookkeeping (graduating a node onto
    its history twin, rebasing ids, migrating config between ids): it runs
    under :func:`library_writes`, so it is never refused even when a graph
    build performs it inside some unrelated user edit."""
    import functools

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with library_writes():
            return fn(*args, **kwargs)

    return wrapper


def _active() -> bool:
    return _user_edit.get() is not None and not _permit.get()


def check_scope(db, pipeline_id: "str | None", what: str) -> None:
    """Refuse *what* in *pipeline_id* when it is library-owned (during a
    user edit only)."""
    if not pipeline_id or not _active():
        return
    from scistack_gui import pipeline_store as ps

    owner = ps.library_owner(db, pipeline_id)
    if owner is None:
        return
    logger.warning(
        "[library_lock] refused %s in %s (library %s, pipeline %s) during %s",
        what,
        pipeline_id,
        owner["library"],
        owner["pipeline_name"],
        _user_edit.get(),
    )
    raise LibraryPipelineReadOnly(owner["library"], owner["pipeline_name"], what)


def check_node(db, node_id: "str | None", what: str) -> None:
    """Refuse *what* on a node that sits in a library-owned pipeline."""
    if not node_id or not _active():
        return
    from scistack_gui import intent_store

    check_scope(db, intent_store.scope_of_node(db, node_id), what)


def check_statement(db, subject_kind: str, subject_ref: str, scope: str, value, what: str) -> None:
    """Refuse a statement written or deleted inside a library pipeline: one
    made AT its scope, about one of its nodes, or (a drawn edge, made at the
    global scope) touching one of its nodes."""
    if not _active():
        return
    from scistack_gui import intent_store

    if scope and scope != intent_store.GLOBAL_SCOPE:
        check_scope(db, scope, what)
    if subject_kind == intent_store.SUBJECT_EDGE:
        if isinstance(value, dict):
            check_node(db, value.get("source"), what)
            check_node(db, value.get("target"), what)
        return
    # Any other subject: a node id resolves to its canvas; a non-node ref (a
    # variable type's name, a parameter's) resolves to the root canvas,
    # which is never library-owned, so it is never refused here.
    check_node(db, subject_ref, what)
