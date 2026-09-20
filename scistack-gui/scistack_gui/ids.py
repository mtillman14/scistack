"""Node ids: what a string on the canvas IS, made into a type.

A canvas node id names one of two things, and until 2026-09-20 the
difference was a substring test at each of ~40 call sites:

* a **bare** (canonical) id — ``var__{Type}``, ``fn__{fn}__{wiring_id}``,
  ``param__{name}``, ``pathInput__{name}``, or a manual node's random
  suffix — naming a piece of shared, scope-independent data;
* a **placed** id — ``{bare}::{pipeline_id}`` — naming one scope's
  independent placement of that data (a duplicated hypothesis re-running
  identical wiring gets its own placement; see
  ``docs/claude/hypothesis-tabs-and-submodules.md``).

Every id-keyed store (node config, hidden ports, column selections, layout
positions) is keyed by the BARE id, while the canvas hands back PLACED ids
after ``resolve_scope_view``. A lookup that forgets to strip finds nothing,
and the DuckDB watcher's ``dag_updated`` then reverts the UI — the
"placement-id lookup trap" (fixed 2026-09-14, and fixed again on the next
seam that forgot). :class:`BareNodeId` and :class:`PlacedNodeId` make the
two spellings two types: a function that takes a ``BareNodeId`` has said
which one it wants, and ``strip_placement`` at its seam is the conversion,
not a defensive habit.

Both are ``str`` subclasses, so JSON, DuckDB parameters and every
``startswith`` keep working; the type is information, not a wrapper to
unwrap. A LEAF: nothing of ``scistack_gui`` is imported here, so the
stores, the domain modules and the services can all take their ids from
one place.
"""

from __future__ import annotations

#: ``{bare}::{pipeline_id}``. ``::`` never appears in a pipeline_id (``main``
#: or ``pipe_{hex}``) or in a function/variable/constant label, so it is a
#: safe, unambiguous separator.
PLACEMENT_SEP = "::"

#: The reserved root scope: pre-scoping documents live here after migration,
#: and a node whose manual row names no ``pipeline_id`` is placed here.
#: ONE spelling — it was ``scope_filter.ROOT``, ``pipeline_store.
#: ROOT_PIPELINE_ID`` and ``graph_builder._ROOT_PIPELINE_ID`` until 2026-09-20.
ROOT_SCOPE = "main"

VAR_ID_PREFIX = "var__"
FN_ID_PREFIX = "fn__"

PARAM_ID_PREFIX = "param__"
"""Node-id prefix for every **Parameter** — Constants and Sweeps alike.

Replaces the old ``const__`` and ``sweep__`` prefixes outright (clean break,
beta — no migration). One prefix is what lets a Parameter keep its identity
when a second value turns its declaration from a Constant into a Sweep: the
id no longer encodes which form the source currently uses.

The prefix is load-bearing beyond display — it appears in ``*.layout.json``
positions, ``_pipeline_hidden_nodes`` rows, synthesised edge ids and
``targetHandle``s, and ``edge_resolver``'s manual-edge resolution — so it is
defined once here and referenced everywhere rather than spelled inline.

See docs/claude/entity-editability-model.md (D6).
"""

PATH_INPUT_ID_PREFIX = "pathInput__"
"""Node-id prefix for every **PathInput**.

Named for the same reason as ``PARAM_ID_PREFIX``: ``edge_resolver`` has to
recognise a PathInput source to bind it to the parameter its edge names, and
that recognition should not be a bare string literal repeated across layers.
"""

#: Every prefix a DB-derived (non-manual) canonical id can start with —
#: shared by the layout.json migration and anything else that needs to
#: distinguish "this id names real DB data" from a manual/opaque id.
DB_DERIVED_PREFIXES = (VAR_ID_PREFIX, FN_ID_PREFIX, PARAM_ID_PREFIX, PATH_INPUT_ID_PREFIX)


class BareNodeId(str):
    """A canonical node id with no placement suffix."""

    __slots__ = ()

    def __new__(cls, value: str) -> "BareNodeId":
        if PLACEMENT_SEP in value:
            raise ValueError(
                f"{value!r} carries a placement suffix — not a bare id "
                f"(strip_placement() first, or ask for a PlacedNodeId)"
            )
        return super().__new__(cls, value)

    def place(self, scope: str) -> "PlacedNodeId":
        """This node's placement in *scope*."""
        return PlacedNodeId(f"{self}{PLACEMENT_SEP}{scope}")

    @property
    def is_db_derived(self) -> bool:
        return self.startswith(DB_DERIVED_PREFIXES)


class PlacedNodeId(str):
    """``{bare}::{pipeline_id}`` — one scope's placement of a canonical node."""

    __slots__ = ()

    def __new__(cls, value: str) -> "PlacedNodeId":
        bare, sep, scope = value.rpartition(PLACEMENT_SEP)
        if not sep or not bare or not scope:
            raise ValueError(f"{value!r} is not a placement-qualified id")
        return super().__new__(cls, value)

    @property
    def bare(self) -> BareNodeId:
        return BareNodeId(self.rpartition(PLACEMENT_SEP)[0])

    @property
    def scope(self) -> str:
        return self.rpartition(PLACEMENT_SEP)[2]


NodeId = BareNodeId | PlacedNodeId


def node_id(value: str) -> NodeId:
    """*value* as the type it is."""
    if isinstance(value, (BareNodeId, PlacedNodeId)):
        return value
    return PlacedNodeId(value) if parse_placement_id(value) else BareNodeId(value)


def placement_id(canonical_id: str, pipeline_id: str) -> PlacedNodeId:
    """The id for one scope's independent placement of a canonical node."""
    return BareNodeId(canonical_id).place(pipeline_id)


def parse_placement_id(node_id: str) -> tuple[BareNodeId, str] | None:
    """Split a placement-qualified id into (canonical_id, pipeline_id).

    Returns None for a bare id with no placement suffix.
    """
    if PLACEMENT_SEP not in node_id:
        return None
    bare, _, scope = node_id.rpartition(PLACEMENT_SEP)
    return (BareNodeId(bare), scope) if bare else None


def strip_placement(node_id: str) -> BareNodeId:
    """The bare canonical id, with any placement suffix removed (a no-op
    if there wasn't one). This is the conversion at a seam that receives
    ids from the canvas; inside a store, take a ``BareNodeId`` instead and
    let the caller strip.
    """
    parsed = parse_placement_id(node_id)
    return parsed[0] if parsed else BareNodeId(node_id)


# ---------------------------------------------------------------------------
# Function-node ids
# ---------------------------------------------------------------------------
#
# DB-derived function nodes use composite ids:
#     fn__{fn_name}__{wiring_id}
# where wiring_id is a 16-hex-char hash of the function's wiring
# (scidb.provenance.compute_wiring_id, the one owner).
#
# Manual function nodes (dragged in by the user) use a different suffix:
#     fn__{fn_name}__{6-char-random}
# These graduate to a canonical DB-derived id once a matching for_each call
# has been recorded.


def fn_node_id(fn_name: str, call_id: str) -> BareNodeId:
    """Compose a DB-derived function-node id from (fn_name, wiring/call id)."""
    return BareNodeId(f"{FN_ID_PREFIX}{fn_name}__{call_id}")


def parse_fn_node_id(node_id: str) -> tuple[str, str] | None:
    """Parse a composite fn node id into (fn_name, call_id).

    Returns None for legacy/manual ids that don't match the composite
    pattern (e.g. ``fn__bandpass`` or ``fn__bandpass__abc123`` where
    ``abc123`` is a random 6-char manual suffix rather than a 16-hex
    call_id). Strips a placement suffix (``::{pipeline_id}``) first, if
    present — callers only ever want the bare (fn_name, call_id), never
    the placement scope, so this is transparent to every consumer.
    """
    bare = strip_placement(node_id)
    if not bare.startswith(FN_ID_PREFIX):
        return None
    body = bare[len(FN_ID_PREFIX) :]
    # Split from the right: the last 16-hex segment is call_id, rest is fn_name.
    if "__" not in body:
        return None
    fn_name, _, suffix = body.rpartition("__")
    if not fn_name:
        return None
    if len(suffix) != 16 or not all(c in "0123456789abcdef" for c in suffix):
        return None
    return fn_name, suffix
