"""How a record id travels through ``for_each`` — the typed spine.

Stages 2a–2c of ``.claude/plan-architecture-2026-09-20.md``. A *rid* is a
``record_id``: the 16-hex, content-addressed id of one saved record. That is
the simple part. The hard part is that "which records fed this call" has to
survive the hand-off to scifor and back, and until this module it was
spelled by hand at every step:

* ``__record_id`` — the column on the loaded frame (still the one column);
* ``__rid_{param}`` — the same rid renamed per parameter, then a key on
  each combination, then a meta column on the result row, then a key
  pushed into scifor's global schema so its filter would match on it;
* ``__vsig_{param}`` — under aggregation, the SIGNATURE of a variant group
  riding on the frame and the combo the same way;
* a bare string — the skip gate's rid sets;
* ``(param, rid, selector)`` — the graph edge.

Sixteen differently-named containers in ``foreach.py`` held rids keyed by
one of those spellings with nothing saying which; the ``Fixed``-on-
aggregation edge was missing for as long as the graph existed because the
rid sat in one container while the save read another.

Now: a combination's reads are a :class:`Selection` (per input, the record
ids — and, for a split aggregated input, the variant group's signature),
built once at combo expansion and held on :class:`RunBindings`; the combo
carries ONE key, :data:`COMBO_KEY`, its index. scifor selects rows by it
through the ``_select_rows`` hook, the save writes edges from it
(:meth:`RunBindings.edges_for`), the skip gate compares it. The frame keeps
``__record_id``; scifor's schema is never extended; nothing parses a
prefix off a combo or a row. The per-input spellings
(:func:`rid_column`, :func:`vsig_column`) survive only as the
``introspect=True`` view's column names and ``RecordPool``'s internals.

This module is a LEAF: no scidb imports, so anything may import it.
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

    def rids_at(self, location: Mapping[str, Any], signature: str | None = None) -> list[str]:
        """The records this input feeds a call at *location*: the group
        *signature* names when split, every group's records when pooled."""
        groups = self.groups_at(location)
        if not self.split:
            return [rid for g in groups.values() for rid in g.rids]
        group = groups.get(EMPTY_SIGNATURE if signature is None else str(signature))
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
    def splits(self) -> bool:
        """One call per variant group."""
        return self.pool is not None and self.pool.split


# ---------------------------------------------------------------------------
# What ONE combination reads — and the one key that names it on the combo
# ---------------------------------------------------------------------------

#: The single key scidb adds to a combination it hands scifor: the index of
#: that combination's :class:`Selection` in ``RunBindings.selections``. One
#: reserved key with one meaning, spelled here and nowhere else. It replaced
#: a FAMILY of keys — ``__rid_{param}`` and ``__vsig_{param}`` for every
#: input — whose prefixes encoded what each value was, which every reader
#: then had to parse back out (2026-09-20).
COMBO_KEY = "__combo"


@dataclass(frozen=True)
class Selection:
    """Which records one combination reads: per input, the record ids —
    one for an iterated, pinned or lineage-only input, several for an
    aggregated one — and, for a split aggregated input, the variant group's
    signature.

    Built once at combo expansion and read by three things that used to each
    re-derive it from the combo's prefixed keys: scifor's frame filter
    (through ``_select_rows``), the graph edges (:meth:`RunBindings.edges_for`)
    and the skip gate. The location half of a combination (schema keys,
    iterables) is scifor's and stays on the combo dict; this half is scidb's
    and lives here, with :data:`COMBO_KEY` as the link.
    """

    rids: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    groups: Mapping[str, str] = field(default_factory=dict)

    def all_rids(self) -> set[str]:
        return {rid for rids in self.rids.values() for rid in rids}

    def with_rid(self, param: str, rid: str) -> "Selection":
        """A copy binding *param* to one more record (a pin, a lineage-only
        column selection)."""
        rids = dict(self.rids)
        rids[param] = tuple(rids.get(param, ())) + (str(rid),)
        return Selection(rids, dict(self.groups))


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
    for a grand aggregation and for full iteration). ``selections`` is one
    :class:`Selection` per combination scifor will run, in combo order; the
    combo carries its index as :data:`COMBO_KEY`, and so does every result
    row scifor writes from it.
    """

    inputs: dict[str, InputBinding] = field(default_factory=dict)
    rid_to_bp: dict[str, dict] = field(default_factory=dict)
    iterated_keys: tuple[str, ...] = ()
    selections: list[Selection] = field(default_factory=list)

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

    def add_selection(self, selection: Selection) -> int:
        """Register what one combination reads; returns the handle to put
        on the combo under :data:`COMBO_KEY`."""
        self.selections.append(selection)
        return len(self.selections) - 1

    def selection_of(self, combo: Mapping[str, Any]) -> Selection | None:
        """The :class:`Selection` a combo or result row names, or ``None``
        for one this run did not build (a caller-supplied row)."""
        handle = combo.get(COMBO_KEY)
        if handle is None or (isinstance(handle, float) and handle != handle):
            return None
        try:
            return self.selections[int(handle)]
        except (TypeError, ValueError, IndexError):
            return None

    def pin(self, param: str, rid: str) -> None:
        """Bind *param* to one record (a ``Fixed`` input whose rid was
        resolved after Step 12, or remapped through a glue chain)."""
        binding = self.inputs.get(param)
        if binding is None:
            self.inputs[param] = InputBinding(param=param, kind=InputKind.PINNED, pinned_rid=rid)
        else:
            binding.pinned_rid = rid

    def rids_for(self, selection: Selection | None) -> dict[str, list[str]]:
        """``{param: [rid, ...]}`` — every record a call with *selection*
        consumes. A ``PINNED`` input's rid is appended when the selection
        did not carry it: a ``Fixed`` pin Step 12 could not settle on one
        row is resolved from the database at save time (``pin``), after the
        selections were built, and it is an edge in every mode."""
        out: dict[str, list[str]] = {}
        if selection is not None:
            for param, rids in selection.rids.items():
                if rids:
                    out[param] = [str(r) for r in rids]
        for param, binding in self.inputs.items():
            if binding.pinned_rid and param not in out:
                out[param] = [binding.pinned_rid]
        return out

    def rids_for_combo(self, combo: Mapping[str, Any]) -> dict[str, list[str]]:
        """:meth:`rids_for` of the selection *combo* (or a result row) names."""
        return self.rids_for(self.selection_of(combo))

    def edges_for(self, selection: Selection | None) -> list[Binding]:
        """The edges of the invocation that runs with *selection*."""
        return [
            Binding(param, rid, self.selectors.get(param))
            for param, rids in self.rids_for(selection).items()
            for rid in rids
        ]

    def for_combo(self, combo: Mapping[str, Any]) -> list[Binding]:
        """The edges of the invocation that *combo* (or a result row) runs —
        the ONE row→edges assembly the save path and the draft stamp use."""
        return self.edges_for(self.selection_of(combo))

    def branch_params_for(self, edges: Iterable[Binding]) -> tuple[dict, dict[str, list]]:
        """The branch params an output built from *edges* inherits, merged
        across every consumed record, with any conflict named."""
        return merge_branch_params(self.rid_to_bp.get(e.rid, {}) for e in edges)
