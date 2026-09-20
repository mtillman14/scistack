"""How a record id travels through ``for_each`` — the typed spine.

Stage 2a of ``.claude/plan-architecture-2026-09-20.md``. A *rid* is a
``record_id``: the 16-hex, content-addressed id of one saved record. That is
the simple part. The hard part is that "which records fed this call" has to
survive five hand-offs, and until this module each was spelled differently:

* ``__record_id`` — a column on the loaded frame;
* ``__rid_{param}`` — the same rid renamed per parameter (Step 12), then a
  key on each combination (so scifor filters the frame to that record per
  call), then a meta column on the result row;
* ``__vsig_{param}`` — under aggregation, not a rid but the SIGNATURE of a
  variant group (the JSON of its records' branch params), riding on the
  frame and the combo exactly like a rid, and the only link from a combo
  to the set of records it pooled (see "Variant groups" below);
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


# ---------------------------------------------------------------------------
# Variant groups — the aggregation-mode selection
# ---------------------------------------------------------------------------
# Full iteration selects ONE record per input per call and carries it as
# ``__rid_{param}``. Aggregation selects a SET: every record of the input
# below the iterated location — split by *variant group*, so cycles filtered
# with ``low_hz=20`` and cycles filtered with ``low_hz=50`` never pool into one
# aggregate (that double-counts and destroys variant identity; decision D1 in
# ``endpoints-viz-and-stats-design.md``). A group is identified by its
# SIGNATURE: the canonical JSON of the records' derived branch params
# (``{"bandpass.low_hz": 20}``), and it rides on the frame and the combo as
# ``__vsig_{param}`` exactly the way a rid rides as ``__rid_{param}`` — the
# same transport, the same seam, and the same reason it is a column: scifor's
# per-combo filter only knows how to filter on columns. ``AcrossVariants``
# opts an input out of the split (pool everything, attach the branch params
# as ordinary columns); a ``ColumnSelection`` input never splits.
#
# ``__save__.<key>`` entries in a signature are save-time kwargs that are BOTH
# a discriminator and a loaded data column scifor row-filters by when
# ``<key>`` is iterated; a group whose value contradicts the combo's own is
# not paired with it (:func:`signature_conflicts_with`).

VSIG_PREFIX = "__vsig_"
SAVE_KWARG_PREFIX = "__save__."


def vsig_column(param: str) -> str:
    """``__vsig_{param}`` — the variant-group signature column / combo key."""
    return f"{VSIG_PREFIX}{param}"


def is_vsig_column(column: Any) -> bool:
    return str(column).startswith(VSIG_PREFIX)


def param_of_vsig(column: Any) -> str:
    s = str(column)
    return s[len(VSIG_PREFIX) :] if s.startswith(VSIG_PREFIX) else s


def is_internal_column(column: Any) -> bool:
    """A ``__rid_*`` or ``__vsig_*`` column: scidb's own transport, filtered
    on by scifor and hidden from the user function."""
    return is_rid_column(column) or is_vsig_column(column)


def variant_signature(branch_params: Mapping[str, Any] | None) -> str:
    """The canonical identity of a point in variant space: a record's derived
    branch params as sorted JSON.

    THE recipe — every consumer that asks "are these two records the same
    variant?" routes through here (``docs/claude/variant-space.md`` §4):
    the aggregation auto-split grouping loaded records, the
    expected-invocation predictor grouping current records straight from the
    graph, the ``latest`` collapse key, the PathOutput ``{variant}`` digest
    and the GUI's variant summary. They were five hand-rolled
    ``json.dumps(..., sort_keys=True)`` calls and had already drifted.

    ``sort_keys`` also sorts nested dicts, and a tuple and a list both emit
    ``[1, 2]``, so a value read back from a JSON column and the same value
    read from the graph sign identically. ``default=str`` keeps a
    non-serialisable value (a datetime, say) from raising in the middle of a
    load.
    """
    import json

    return json.dumps(dict(branch_params or {}), sort_keys=True, default=str)


#: The signature of a record with no derived branch params — and the value a
#: split input takes at a combination where it has no data, so the combo
#: still flows through and skips gracefully.
EMPTY_SIGNATURE = variant_signature({})


def signature_conflicts_with(signature: str, location: Mapping[str, Any]) -> bool:
    """True when a ``__save__.<key>`` entry contradicts *location*'s own
    value for ``<key>`` — pairing that group with that combination would call
    the function on rows the combination's own filter excludes."""
    import json

    if signature == EMPTY_SIGNATURE:
        return False
    for k, v in json.loads(signature).items():
        if not str(k).startswith(SAVE_KWARG_PREFIX):
            continue
        bare = str(k)[len(SAVE_KWARG_PREFIX) :]
        if bare in location and str(location[bare]) != str(v):
            return True
    return False


def merge_branch_params(
    dicts: Iterable[Mapping[str, Any]],
) -> tuple[dict, dict[str, list]]:
    """Fold several records' branch params into one point, last write wins.

    Returns the merged dict and ``{key: [every value seen]}`` for each key
    that changed value on the way. A conflict means two variant groups
    reached one call — which the aggregation auto-split exists to prevent,
    and which a PathOutput ``{placeholder}`` cannot represent — so both
    callers need the keys, and one needs the values for its message.
    """
    merged: dict = {}
    seen: dict[str, list] = {}
    for bp in dicts:
        for k, v in (bp or {}).items():
            if k in merged and merged[k] != v:
                seen.setdefault(k, [merged[k]]).append(v)
            merged[k] = v
    return merged, seen


@dataclass(frozen=True)
class VariantGroup:
    """One variant group of one input at one iterated location: the records
    that pool into a single aggregating call."""

    signature: str
    rids: tuple[str, ...]

    @property
    def branch_params(self) -> dict:
        import json

        return dict(json.loads(self.signature))


@dataclass
class RecordPool:
    """What an ``AGGREGATED`` input pools: its records per iterated location,
    per variant group.

    ``location_keys`` are the iterated schema keys THIS input populates — an
    input coarser than the iterated level (a subject-level table under a
    per-session aggregation) is keyed by the subset it has, so it is found
    at every location beneath it instead of at none. ``split`` says whether
    the groups are separate calls (one ``__vsig_{param}`` value per call) or
    pooled into one (``AcrossVariants``, ``ColumnSelection``).
    """

    location_keys: tuple[str, ...] = ()
    groups_by_location: dict[tuple, dict[str, VariantGroup]] = field(default_factory=dict)
    split: bool = True

    def location_of(self, combo: Mapping[str, Any]) -> tuple:
        return tuple(str(combo.get(k, "")) for k in self.location_keys)

    def groups_at(self, combo: Mapping[str, Any]) -> dict[str, VariantGroup]:
        return self.groups_by_location.get(self.location_of(combo), {})

    def signatures(self) -> list[str]:
        """Every signature seen at any location, first-seen order."""
        return list(
            dict.fromkeys(sig for groups in self.groups_by_location.values() for sig in groups)
        )

    def rids_at(self, combo: Mapping[str, Any], param: str) -> list[str]:
        """The records this input feeds the call that *combo* runs: the
        group the combo names in ``__vsig_{param}`` when split, every
        group's records when pooled."""
        groups = self.groups_at(combo)
        if not self.split:
            return [rid for g in groups.values() for rid in g.rids]
        group = groups.get(str(combo.get(vsig_column(param), EMPTY_SIGNATURE)))
        return list(group.rids) if group else []


@dataclass
class InputBinding:
    """One input's whole story after Step 12.

    ``pinned_rid`` is the one record a ``PINNED`` input reads; ``pool`` is
    what an ``AGGREGATED`` input reads (per location, per variant group);
    an ``ITERATE`` / ``LINEAGE_ONLY`` input's record rides on the
    combination itself as ``__rid_{param}``.
    """

    param: str
    kind: InputKind
    type_name: str | None = None
    selector: str | None = None
    pinned_rid: str | None = None
    pool: RecordPool | None = None

    @property
    def column(self) -> str:
        return rid_column(self.param)

    @property
    def vsig_column(self) -> str:
        return vsig_column(self.param)

    @property
    def splits(self) -> bool:
        """One call per variant group (carries a ``__vsig_`` column)."""
        return self.pool is not None and self.pool.split

    def rids_for(self, combo: Mapping[str, Any]) -> list[str]:
        """The records this input binds in the call *combo* runs."""
        if self.pool is not None:
            return self.pool.rids_at(combo, self.param)
        value = combo.get(self.column)
        if value is not None and not (isinstance(value, float) and value != value):
            return [str(value)]
        return [self.pinned_rid] if self.pinned_rid else []


@dataclass
class RunBindings:
    """Every input's :class:`InputBinding`, and the ONE row→edges assembly.

    :meth:`rids_for_combo` is the only place "which records does this call
    consume" is answered — the save path's edges, the skip gate's expected
    rid set and the draft endpoint stamp all read it — and
    :meth:`for_combo` types that answer as the graph's edge list. Its result
    travels TYPED on the ``GraphRecord`` the save hands to ``record_run``,
    and its invocation id is stamped into the record's version keys as
    ``__invocation_id``.

    ``rid_to_bp`` is every loaded record's derived branch params (the
    variant identity the save path checks for conflicts); ``iterated_keys``
    are the schema keys an aggregating run iterated, in combo order (empty
    for a grand aggregation and for full iteration).
    """

    inputs: dict[str, InputBinding] = field(default_factory=dict)
    rid_to_bp: dict[str, dict] = field(default_factory=dict)
    iterated_keys: tuple[str, ...] = ()

    def __getitem__(self, param: str) -> InputBinding:
        return self.inputs[param]

    def __contains__(self, param: str) -> bool:
        return param in self.inputs

    def of_kind(self, kind: InputKind) -> list[InputBinding]:
        return [b for b in self.inputs.values() if b.kind == kind]

    @property
    def selectors(self) -> dict[str, str | None]:
        return {p: b.selector for p, b in self.inputs.items()}

    @property
    def pinned_rids(self) -> dict[str, str]:
        return {p: b.pinned_rid for p, b in self.inputs.items() if b.pinned_rid}

    @property
    def tracked_columns(self) -> list[str]:
        """The ``__rid_{param}`` columns Step 12 registered on a loaded
        frame — the plain variable inputs, whether they went on to be an
        iteration axis (``ITERATE``) or to pool (``AGGREGATED``).

        Deliberately not "the axes": a ``LINEAGE_ONLY`` (ColumnSelection)
        input also carries a rid column but never expands, and a ``PINNED``
        one has no column at all. Telling those three apart is what
        ``InputKind`` is for; this is the set the MATLAB bridge must rename
        and the set the save-path diagnostic reports.
        """
        return [
            b.column
            for b in self.inputs.values()
            if b.kind in (InputKind.ITERATE, InputKind.AGGREGATED)
        ]

    @property
    def split_params(self) -> list[str]:
        """Inputs that expand one call per variant group, in input order."""
        return [p for p, b in self.inputs.items() if b.splits]

    @property
    def vsig_columns(self) -> list[str]:
        return [vsig_column(p) for p in self.split_params]

    def pin(self, param: str, rid: str) -> None:
        """Bind *param* to one record (a ``Fixed`` input whose rid was
        resolved after Step 12, or remapped through a glue chain)."""
        binding = self.inputs.get(param)
        if binding is None:
            self.inputs[param] = InputBinding(param=param, kind=InputKind.PINNED, pinned_rid=rid)
        else:
            binding.pinned_rid = rid

    def rids_for_combo(self, combo: Mapping[str, Any]) -> dict[str, list[str]]:
        """``{param: [rid, ...]}`` — every record the call *combo* runs
        consumes, in every mode."""
        out: dict[str, list[str]] = {}
        for param, binding in self.inputs.items():
            rids = binding.rids_for(combo)
            if rids:
                out[param] = rids
        return out

    def for_combo(self, combo: Mapping[str, Any]) -> list[Binding]:
        """The edges of the invocation that *combo* runs."""
        return [
            Binding(param, str(rid), self.inputs[param].selector)
            for param, rids in self.rids_for_combo(combo).items()
            for rid in rids
        ]

    def branch_params_for(self, edges: Iterable[Binding]) -> tuple[dict, dict[str, list]]:
        """The branch params an output built from *edges* inherits, merged
        across every consumed record, with any conflict named."""
        return merge_branch_params(self.rid_to_bp.get(e.rid, {}) for e in edges)
