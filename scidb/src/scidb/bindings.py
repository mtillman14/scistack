"""How a record id travels through ``for_each`` — the typed spine.

Stage 2a of ``.claude/plan-architecture-2026-09-20.md``. A *rid* is a
``record_id``: the 16-hex, content-addressed id of one saved record. That is
the simple part. The hard part is that "which records fed this call" has to
survive five hand-offs, and until this module each was spelled differently:

* ``__record_id`` — a column on the loaded frame;
* ``__rid_{param}`` — the same rid renamed per parameter (Step 12), then a
  key on each combination (so scifor filters the frame to that record per
  call), then a meta column on the result row;
* ``__rid_{param}_{i}`` — the aggregation path's ``__upstream`` dict, which
  needs unique keys;
* a bare string — the skip gate's rid sets;
* ``(param, rid, selector)`` — the graph edge.

Sixteen differently-named containers in ``foreach.py`` held rids keyed by
one of those spellings with nothing saying which. The ``Fixed``-on-
aggregation edge was missing for as long as the graph existed because the
rid sat in ``fixed_rid_values`` (bare keys) while the save read
``lineage_fixed_rids`` (prefixed keys, a different source) — three separate
gaps, found only by ``test_identity_parity.py`` on 2026-09-20.

This module is the one place the spellings are named, and it is a LEAF: no
scidb imports, so anything may import it (scifor's bridge will).

What a newtype cannot do: reach inside a pandas column name. ``__rid_x`` on
a frame stays a string at the scifor boundary; :func:`rid_column` and
:func:`param_of` make that boundary ONE place instead of fifteen.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Mapping

# ---------------------------------------------------------------------------
# The column spelling — the ONLY two functions that know the prefix
# ---------------------------------------------------------------------------

#: Prefix of the per-parameter record-id column / combo key.
RID_PREFIX = "__rid_"

#: The record-id column a loaded frame carries BEFORE it is attributed to a
#: parameter (``load_all_as_df`` writes it; Step 12 renames it).
RECORD_ID_COLUMN = "__record_id"


def rid_column(param: str) -> str:
    """``__rid_{param}`` — the combo key / frame column carrying *param*'s
    bound record id."""
    return f"{RID_PREFIX}{param}"


def param_of(column: Any) -> str:
    """The parameter a rid column names; a bare parameter name passes
    through unchanged, so callers holding either spelling can normalise
    without first asking which they hold."""
    s = str(column)
    return s[len(RID_PREFIX) :] if s.startswith(RID_PREFIX) else s


def is_rid_column(column: Any) -> bool:
    return str(column).startswith(RID_PREFIX)


def rid_columns(columns: Iterable[Any]) -> list[str]:
    """The rid columns among *columns*, in order."""
    return [str(c) for c in columns if is_rid_column(c)]


# ---------------------------------------------------------------------------
# What kind of input this is — decided once, carried everywhere
# ---------------------------------------------------------------------------


class InputKind(str, Enum):
    """How a parameter's records take part in iteration.

    Decided ONCE at Step 12 and carried on the :class:`InputBinding`, so the
    save path, the skip gate and the expected-invocation predictor read a
    field rather than each re-deriving "is this Fixed?" with ``isinstance``
    and ``hasattr("fixed_metadata")`` (the predictor re-derived it a fourth
    way, from schema levels).
    """

    #: A plain variable: its rid is an ITERATION AXIS — several records at
    #: one location are variants, each its own call.
    ITERATE = "iterate"
    #: ``Fixed``: ONE pinned rid, injected into every combination, never an
    #: axis.
    PINNED = "pinned"
    #: ``ColumnSelection``: prunes combinations that have no data and binds
    #: for the edge, never expands.
    LINEAGE_ONLY = "lineage_only"
    #: Records BELOW the iterated level: pooled into one call per location,
    #: every record an edge of that one invocation.
    AGGREGATED = "aggregated"


# ---------------------------------------------------------------------------
# One edge, one input, one call's worth
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Binding:
    """One input edge of an invocation: *param* read *rid*, through
    *selector* (the column selection's stored JSON, or ``None``).

    Replaces the ``(param, rid)`` / ``(param, rid, selector)`` tuples that
    ``compute_invocation_id`` accepted in two arities. Iterating a
    ``Binding`` yields exactly the three-tuple, so the hash it produces is
    the same bytes as before — the parity suite depends on that.
    """

    param: str
    rid: str
    selector: str | None = None

    def __iter__(self):
        yield self.param
        yield self.rid
        yield self.selector

    @classmethod
    def coerce(cls, value: Any) -> "Binding":
        """A ``Binding`` from a ``Binding`` or either tuple arity."""
        if isinstance(value, Binding):
            return value
        parts = tuple(value)
        if len(parts) == 3:
            param, rid, selector = parts
        elif len(parts) == 2:
            (param, rid), selector = parts, None
        else:
            raise ValueError(f"not a binding: {value!r}")
        return cls(str(param), str(rid), None if selector is None else str(selector))


@dataclass
class InputBinding:
    """One input's whole story after Step 12.

    ``rids`` is every record this input can bind — one for ``PINNED``, the
    per-location candidates for ``ITERATE`` / ``LINEAGE_ONLY``, the pooled
    set for ``AGGREGATED`` — keyed by the (stringified) schema location so
    a combination can ask for its own. ``rid_to_bp`` is each rid's branch
    params, the variant identity the save path stamps on the output.
    """

    param: str
    kind: InputKind
    type_name: str | None = None
    selector: str | None = None
    rids_by_location: dict[tuple, list[str]] = field(default_factory=dict)
    pinned_rid: str | None = None
    rid_to_bp: dict[str, dict] = field(default_factory=dict)

    @property
    def column(self) -> str:
        return rid_column(self.param)

    def all_rids(self) -> list[str]:
        out: list[str] = []
        if self.pinned_rid:
            out.append(self.pinned_rid)
        for rids in self.rids_by_location.values():
            out.extend(rids)
        return out


@dataclass
class RunBindings:
    """Every input's :class:`InputBinding`, and the ONE row→edges assembly.

    :meth:`for_combo` is the only place an invocation's edge list is built:
    full iteration reads ``__rid_*`` off the combination, aggregation reads
    the pooled set for the combination's location, pinned rids are appended
    — and nothing else assembles edges. ``__graph_var_bindings`` is written
    from its result.
    """

    inputs: dict[str, InputBinding] = field(default_factory=dict)

    def __getitem__(self, param: str) -> InputBinding:
        return self.inputs[param]

    def __contains__(self, param: str) -> bool:
        return param in self.inputs

    def of_kind(self, kind: InputKind) -> list[InputBinding]:
        return [b for b in self.inputs.values() if b.kind == kind]

    @property
    def selectors(self) -> dict[str, str | None]:
        return {p: b.selector for p, b in self.inputs.items()}

    def for_combo(
        self,
        combo: Mapping[str, Any],
        pooled: Mapping[str, Iterable[str]] | None = None,
    ) -> list[Binding]:
        """The edges of the invocation that *combo* runs.

        *combo* carries ``__rid_{param}`` for every ``ITERATE`` /
        ``LINEAGE_ONLY`` / ``PINNED`` input in full iteration; an aggregating
        call passes the location's pooled rids per parameter in *pooled*
        (keyed by param or by its rid column — both are read). A pinned rid
        missing from both is appended from the binding itself, so a ``Fixed``
        input is an edge in every mode.
        """
        edges: list[Binding] = []
        bound: set[str] = set()
        for key, value in combo.items():
            if not is_rid_column(key) or value is None:
                continue
            if isinstance(value, float) and value != value:  # NaN
                continue
            param = param_of(key)
            edges.append(Binding(param, str(value), self.selectors.get(param)))
            bound.add(param)
        for key, rids in (pooled or {}).items():
            param = param_of(key)
            for rid in rids:
                if rid is None:
                    continue
                edges.append(Binding(param, str(rid), self.selectors.get(param)))
                bound.add(param)
        for param, binding in self.inputs.items():
            if binding.pinned_rid and param not in bound:
                edges.append(Binding(param, binding.pinned_rid, binding.selector))
                bound.add(param)
        return edges
